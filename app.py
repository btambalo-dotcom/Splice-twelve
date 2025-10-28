from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime
import os, uuid
from pathlib import Path
from io import BytesIO

BASE_DIR = Path(__file__).parent.resolve()
EXPORT_DIR = BASE_DIR / 'exports'
EXPORT_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY','dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{(BASE_DIR / 'data.db').as_posix()}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app); login_manager.login_view = 'login'

# ---------- Models ----------
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=True)
    def set_password(self,p): self.password_hash = generate_password_hash(p)
    def check_password(self,p): return check_password_hash(self.password_hash,p)

class DeviceType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    value_usd = db.Column(db.Float, default=0.0, nullable=False)

class SpliceTier(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    min_splices = db.Column(db.Integer, nullable=False)
    max_splices = db.Column(db.Integer, nullable=True)
    price_per_splice_usd = db.Column(db.Float, default=0.0, nullable=False)

class MapMaster(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), unique=True, nullable=False)

class Record(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sheet = db.Column(db.String(120))
    map = db.Column(db.String(200))
    type = db.Column(db.String(120))
    splices = db.Column(db.Integer)
    device = db.Column(db.String(120))
    created_date = db.Column(db.DateTime, nullable=True)
    splicer = db.Column(db.String(120))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    price_splices_usd = db.Column(db.Float, default=0.0)
    price_device_usd = db.Column(db.Float, default=0.0)
    total_usd = db.Column(db.Float, default=0.0)

@login_manager.user_loader
def load_user(uid): return User.query.get(int(uid))

with app.app_context():
    db.create_all()

# ---------- Helpers ----------
ALLOWED={'xlsx'}
REQUIRED = ['Type','Map','Splices','Device','Splicer','Created']

def parse_excel(upload):
    raw = upload.read()
    xls = pd.ExcelFile(BytesIO(raw))
    if 'Sheet1' not in xls.sheet_names:
        raise ValueError('A aba "Sheet1" não foi encontrada')
    df = xls.parse('Sheet1')
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError('Colunas ausentes: ' + ', '.join(missing))
    out = df[REQUIRED].copy()
    out['Splices'] = pd.to_numeric(out['Splices'], errors='coerce').fillna(0).astype(int)
    out['Created'] = pd.to_datetime(out['Created'], errors='coerce')
    out['__sheet__'] = 'Sheet1'
    return out

def tier_price_for(count):
    tier = SpliceTier.query.filter((SpliceTier.min_splices <= count) & ((SpliceTier.max_splices >= count) | (SpliceTier.max_splices.is_(None)))).order_by(SpliceTier.min_splices.desc()).first()
    return tier.price_per_splice_usd if tier else 0.0

def device_value_for(type_name):
    if not type_name: return 0.0
    dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
    return dt.value_usd if dt else 0.0

def apply_prices(df):
    def row_calc(splices, type_name):
        charge = max(int(splices) - 1, 0)
        return charge * tier_price_for(charge), device_value_for(type_name)
    prices = df.apply(lambda r: row_calc(r['Splices'], str(r['Type'])), axis=1, result_type='expand')
    df['price_splices_usd'] = prices[0]
    df['price_device_usd'] = prices[1]
    df['total_usd'] = (df['price_splices_usd'] + df['price_device_usd']).round(2)
    return df

def persist(df):
    rows=[]
    for _, r in df.iterrows():
        rows.append(Record(sheet=str(r.get('__sheet__','')), map=str(r.get('Map') or ''), type=str(r.get('Type') or ''),
                           splices=int(r.get('Splices') or 0), device=str(r.get('Device') or ''),
                           created_date=r.get('Created') if pd.notna(r.get('Created')) else None,
                           splicer=str(r.get('Splicer') or ''),
                           price_splices_usd=float(r.get('price_splices_usd') or 0.0), price_device_usd=float(r.get('price_device_usd') or 0.0),
                           total_usd=float(r.get('total_usd') or 0.0)))
    if rows:
        db.session.bulk_save_objects(rows); db.session.commit()

# ---------- Auth ----------
@app.route('/init-admin')
def init_admin():
    user = os.environ.get('ADMIN_USERNAME','admin'); pwd = os.environ.get('ADMIN_PASSWORD','admin123')
    if not User.query.filter_by(username=user).first():
        u=User(username=user, is_admin=True); u.set_password(pwd); db.session.add(u); db.session.commit()
        return f'Admin criado: {user} / {pwd}'
    return 'Admin já existe.'

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method=='POST':
        u=User.query.filter_by(username=request.form.get('username','')).first()
        if u and u.check_password(request.form.get('password','')):
            login_user(u); return redirect(url_for('index'))
        flash('Usuário ou senha inválidos.','error')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))

# ---------- Upload / Dashboard ----------
@app.route('/', methods=['GET','POST'])
@login_required
def index():
    if request.method=='POST':
        f = request.files.get('file')
        if not f or f.filename=='':
            flash('Envie um arquivo .xlsx','error'); return redirect(url_for('index'))
        ext = f.filename.rsplit('.',1)[1].lower()
        if ext not in ALLOWED:
            flash('Somente .xlsx é permitido','error'); return redirect(url_for('index'))
        try:
            df = parse_excel(f)
            df = apply_prices(df)
            persist(df)
        except Exception as e:
            flash(f'Erro ao ler planilha: {e}','error'); return redirect(url_for('index'))
        token = uuid.uuid4().hex
        csv_path = EXPORT_DIR / f'clean_{token}.csv'
        xlsx_path = EXPORT_DIR / f'clean_{token}.xlsx'
        df.to_csv(csv_path, index=False)
        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as w:
            df.to_excel(w, index=False, sheet_name='dados')
        table_html = df.head(100).to_html(index=False, classes='table table-striped table-sm')
        return render_template('results.html', table_html=table_html, token=token)
    from sqlalchemy import func
    totals = db.session.query(func.count(Record.id), func.sum(Record.total_usd)).first()
    return render_template('upload.html', total_rows=totals[0] or 0, total_amount=totals[1] or 0.0)

@app.route('/download/csv/<token>')
@login_required
def download_csv(token):
    path = EXPORT_DIR / f'clean_{token}.csv'
    return send_file(path, as_attachment=True, download_name='dados_limpos.csv')

@app.route('/download/xlsx/<token>')
@login_required
def download_xlsx(token):
    path = EXPORT_DIR / f'clean_{token}.xlsx'
    return send_file(path, as_attachment=True, download_name='dados_limpos.xlsx')

# ---------- Settings (Devices + Tiers) ----------
@app.route('/settings')
@login_required
def settings_home():
    types = DeviceType.query.order_by(DeviceType.name).all()
    tiers = SpliceTier.query.order_by(SpliceTier.min_splices).all()
    return render_template('settings.html', types=types, tiers=tiers)

@app.route('/settings/devices/add', methods=['POST'])
@login_required
def settings_devices_add():
    name = request.form.get('name','').strip()
    val = float(request.form.get('value_usd','0') or 0)
    if not name:
        flash('Nome obrigatório.','error'); return redirect(url_for('settings_home'))
    obj = DeviceType.query.filter(DeviceType.name.ilike(name)).first()
    if obj: obj.value_usd = val
    else: db.session.add(DeviceType(name=name, value_usd=val))
    db.session.commit(); flash('Dispositivo salvo.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/devices/delete/<int:tid>')
@login_required
def settings_devices_del(tid):
    obj = DeviceType.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash('Dispositivo removido.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/tiers/add', methods=['POST'])
@login_required
def settings_tiers_add():
    min_s = int(request.form.get('min_splices','0') or 0)
    max_raw = request.form.get('max_splices','').strip()
    max_s = int(max_raw) if max_raw else None
    price = float(request.form.get('price_per_splice_usd','0') or 0)
    db.session.add(SpliceTier(min_splices=min_s, max_splices=max_s, price_per_splice_usd=price))
    db.session.commit(); flash('Faixa salva.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/tiers/delete/<int:tid>')
@login_required
def settings_tiers_del(tid):
    obj = SpliceTier.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash('Faixa removida.','success'); return redirect(url_for('settings_home'))

# ---------- Maps ----------
@app.route('/maps')
@login_required
def maps_home():
    return render_template('maps.html', maps=MapMaster.query.order_by(MapMaster.name).all())

@app.route('/maps/add', methods=['POST'])
@login_required
def maps_add():
    name = request.form.get('name','').strip()
    if name and not MapMaster.query.filter_by(name=name).first():
        db.session.add(MapMaster(name=name)); db.session.commit(); flash('Mapa adicionado.','success')
    else:
        flash('Nome vazio ou já existe.','error')
    return redirect(url_for('maps_home'))

@app.route('/maps/delete/<int:mid>')
@login_required
def maps_delete(mid):
    m = MapMaster.query.get_or_404(mid); db.session.delete(m); db.session.commit()
    flash('Mapa removido.','success')
    return redirect(url_for('maps_home'))

# ---------- Manual Entry ----------
@app.route('/manual', methods=['GET','POST'])
@login_required
def manual_entry():
    maps = MapMaster.query.order_by(MapMaster.name).all()
    types = DeviceType.query.order_by(DeviceType.name).all()
    if request.method == 'POST':
        from datetime import datetime as _dt
        map_name = request.form.get('map','').strip()
        type_name = request.form.get('type','').strip()
        device = request.form.get('device','').strip()
        splices = int(request.form.get('splices','0') or 0)
        splicer = request.form.get('splicer','').strip()
        created_raw = request.form.get('created','').strip()
        created_date = _dt.strptime(created_raw, '%Y-%m-%d') if created_raw else None

        import pandas as pd
        df = pd.DataFrame([{'Type': type_name, 'Map': map_name, 'Splices': splices, 'Device': device, 'Splicer': splicer, 'Created': created_date, '__sheet__': 'manual'}])
        df = apply_prices(df)
        r = Record(sheet='manual', map=map_name, type=type_name, splices=splices, device=device,
                   created_date=created_date, splicer=splicer,
                   price_splices_usd=float(df.iloc[0]['price_splices_usd']), price_device_usd=float(df.iloc[0]['price_device_usd']),
                   total_usd=float(df.iloc[0]['total_usd']))
        db.session.add(r); db.session.commit()
        flash('Lançamento salvo.','success')
        return redirect(url_for('manual_entry'))
    recent = Record.query.order_by(Record.id.desc()).limit(25).all()
    return render_template('manual_entry.html', maps=maps, types=types, recent=recent)

# ---------- Reports ----------
@app.route('/reports')
@login_required
def reports():
    # Date filters via querystring (?start=YYYY-MM-DD&end=YYYY-MM-DD)
    start_raw = request.args.get('start','').strip()
    end_raw = request.args.get('end','').strip()

    q = Record.query
    start_dt = end_dt = None
    if start_raw:
        try:
            start_dt = datetime.strptime(start_raw, '%Y-%m-%d')
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date >= start_dt)
        except Exception:
            flash('Data inicial inválida. Use YYYY-MM-DD.','error')
    if end_raw:
        try:
            # fim do dia 23:59:59
            end_dt = datetime.strptime(end_raw, '%Y-%m-%d')
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date <= end_dt)
        except Exception:
            flash('Data final inválida. Use YYYY-MM-DD.','error')

    rows = q.all()

    from collections import defaultdict
    agg_map = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    agg_type = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    for r in rows:
        m = r.map or '—'; t = r.type or '—'
        agg_map[m]['rows'] += 1; agg_map[m]['splices'] += int(r.splices or 0); agg_map[m]['total'] += float(r.total_usd or 0.0)
        agg_type[t]['rows'] += 1; agg_type[t]['splices'] += int(r.splices or 0); agg_type[t]['total'] += float(r.total_usd or 0.0)
    # Totais gerais do filtro
    total_rows = len(rows)
    total_splices = sum(int(r.splices or 0) for r in rows)
    total_usd = sum(float(r.total_usd or 0.0) for r in rows)

    return render_template('reports.html',
                           map_rows=sorted(agg_map.items()),
                           type_rows=sorted(agg_type.items()),
                           start=start_raw, end=end_raw,
                           total_rows=total_rows, total_splices=total_splices, total_usd=total_usd)

if __name__=='__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
