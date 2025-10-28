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
SQLITE_PATH = Path('/tmp/app.db')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY','dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{SQLITE_PATH}'
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
    value = db.Column(db.Float, default=0.0, nullable=False)

class SpliceTier(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    min_splices = db.Column(db.Integer, nullable=False)
    max_splices = db.Column(db.Integer, nullable=True)
    price_per_splice = db.Column(db.Float, default=0.0, nullable=False)

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
    price_splices = db.Column(db.Float, default=0.0)
    price_device = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)

@login_manager.user_loader
def load_user(uid): return User.query.get(int(uid))

with app.app_context():
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    db.create_all()

ALLOWED={'xlsx'}
REQUIRED = ['Type','Map','Splices','Device','Splicer','Created']

def parse_excel_exact(upload):
    raw = upload.read()
    xls = pd.ExcelFile(BytesIO(raw))
    sheet = 'Sheet1'
    if sheet not in xls.sheet_names:
        raise ValueError('A aba "Sheet1" não foi encontrada')
    df = xls.parse(sheet)
    df.columns = [str(c).strip() for c in df.columns]
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError('Colunas ausentes: ' + ', '.join(missing))
    out = df[REQUIRED].copy()
    out['Splices'] = pd.to_numeric(out['Splices'], errors='coerce').fillna(0).astype(int)
    out['Created'] = pd.to_datetime(out['Created'], errors='coerce')
    out['__sheet__'] = sheet
    return out

def price_for_splices(n):
    tier = SpliceTier.query.filter((SpliceTier.min_splices <= n) & ((SpliceTier.max_splices >= n) | (SpliceTier.max_splices.is_(None)))).order_by(SpliceTier.min_splices.desc()).first()
    return (n * tier.price_per_splice) if tier else 0.0
def price_for_device_type(type_name):
    if not type_name: return 0.0
    dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
    return dt.value if dt else 0.0

def apply_prices(df):
    df['price_splices'] = df['Splices'].apply(lambda n: price_for_splices(int(n)))
    df['price_device'] = df['Type'].astype(str).apply(lambda t: price_for_device_type(str(t)))
    df['total'] = df['price_splices'] + df['price_device']
    return df

def persist(df):
    rows=[]
    for _, r in df.iterrows():
        rows.append(Record(sheet=str(r.get('__sheet__','')), map=str(r.get('Map') or ''), type=str(r.get('Type') or ''),
                           splices=int(r.get('Splices') or 0), device=str(r.get('Device') or ''),
                           created_date=r.get('Created') if pd.notna(r.get('Created')) else None,
                           splicer=str(r.get('Splicer') or ''),
                           price_splices=float(r.get('price_splices') or 0.0), price_device=float(r.get('price_device') or 0.0),
                           total=float(r.get('total') or 0.0)))
    if rows:
        db.session.bulk_save_objects(rows); db.session.commit()

@app.route('/init-admin')
def init_admin():
    user = os.environ.get('ADMIN_USERNAME','admin'); pwd = os.environ.get('ADMIN_PASSWORD','admin123')
    if not User.query.filter_by(username=user).first():
        u=User(username=user, is_admin=True); u.set_password(pwd); db.session.add(u); db.session.commit()
        return f'Admin criado: {user} / {pwd}'
    return 'Admin já existe.'

from flask import render_template_string

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
            df = parse_excel_exact(f)
            df = apply_prices(df)
            persist(df)
        except Exception as e:
            flash(f'Erro ao ler planilha: {e}','error'); return redirect(url_for('index'))
        token = uuid.uuid4().hex
        (EXPORT_DIR/f'clean_{token}.csv').write_text(df.to_csv(index=False), encoding='utf-8')
        with pd.ExcelWriter(EXPORT_DIR/f'clean_{token}.xlsx', engine='openpyxl') as w:
            df.to_excel(w, index=False, sheet_name='dados')
        table_html = df.head(100).to_html(index=False, classes='table table-striped table-sm')
        return render_template('results.html', table_html=table_html, token=token)
    from sqlalchemy import func
    totals = db.session.query(func.count(Record.id), func.sum(Record.total)).first()
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

@app.route('/settings')
@login_required
def settings_home():
    return render_template('settings.html',
        types=DeviceType.query.order_by(DeviceType.name).all(),
        tiers=SpliceTier.query.order_by(SpliceTier.min_splices).all())

@app.route('/settings/device-type', methods=['POST'])
@login_required
def add_device_type():
    name = request.form.get('name','').strip()
    value = float(request.form.get('value','0') or 0)
    obj = DeviceType.query.filter(DeviceType.name.ilike(name)).first()
    if obj: obj.value = value
    else: db.session.add(DeviceType(name=name, value=value))
    db.session.commit(); flash('Tipo salvo.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/device-type/delete/<int:tid>')
@login_required
def del_device_type(tid):
    obj = DeviceType.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash('Tipo removido.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/splice-tier', methods=['POST'])
@login_required
def add_splice_tier():
    min_s = int(request.form.get('min_splices','0') or 0)
    max_raw = request.form.get('max_splices','').strip()
    max_s = int(max_raw) if max_raw else None
    price = float(request.form.get('price_per_splice','0') or 0)
    db.session.add(SpliceTier(min_splices=min_s, max_splices=max_s, price_per_splice=price))
    db.session.commit(); flash('Faixa salva.','success'); return redirect(url_for('settings_home'))

@app.route('/settings/splice-tier/delete/<int:tid>')
@login_required
def del_splice_tier(tid):
    obj = SpliceTier.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash('Faixa removida.','success'); return redirect(url_for('settings_home'))

@app.route('/reports')
@login_required
def reports():
    rows = Record.query.all()
    from collections import defaultdict
    agg_map = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    agg_type = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    for r in rows:
        m = r.map or '—'; t = r.type or '—'
        agg_map[m]['rows'] += 1; agg_map[m]['splices'] += int(r.splices or 0); agg_map[m]['total'] += float(r.total or 0.0)
        agg_type[t]['rows'] += 1; agg_type[t]['splices'] += int(r.splices or 0); agg_type[t]['total'] += float(r.total or 0.0)
    return render_template('reports.html', map_rows=sorted(agg_map.items()), type_rows=sorted(agg_type.items()))

if __name__=='__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
