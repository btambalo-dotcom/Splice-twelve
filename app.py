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
    from sqlalchemy import or_
    tier = SpliceTier.query.filter((SpliceTier.min_splices <= count) & (or_(SpliceTier.max_splices == None, SpliceTier.max_splices >= count))).order_by(SpliceTier.min_splices.desc()).first()
    return tier.price_per_splice_usd if tier else 0.0

def device_value_for(type_name):
    if not type_name: return 0.0
    dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
    return dt.value_usd if dt else 0.0

def apply_prices(df):
    def row_calc(splices, type_name):
        charge = max(int(splices) - 1, 0)  # desconsidera a 1ª fusão
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

# ---------- Records Management ----------
@app.route('/records')
@login_required
def records_home():
    start_raw = request.args.get('start','').strip()
    end_raw = request.args.get('end','').strip()
    map_q = request.args.get('map','').strip()
    type_q = request.args.get('type','').strip()
    device_q = request.args.get('device','').strip()

    q = Record.query
    if start_raw:
        try:
            sdt = datetime.strptime(start_raw, '%Y-%m-%d')
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date >= sdt)
        except: flash('Data inicial inválida.','error')
    if end_raw:
        try:
            edt = datetime.strptime(end_raw, '%Y-%m-%d').replace(hour=23,minute=59,second=59,microsecond=999999)
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date <= edt)
        except: flash('Data final inválida.','error')
    if map_q: q = q.filter(Record.map.ilike(f"%{map_q}%"))
    if type_q: q = q.filter(Record.type.ilike(f"%{type_q}%"))
    if device_q: q = q.filter(Record.device.ilike(f"%{device_q}%"))
    rows = q.order_by(Record.id.desc()).limit(300).all()
    return render_template('records.html', rows=rows, start=start_raw, end=end_raw, map=map_q, type=type_q, device=device_q)

@app.route('/record/delete/<int:rid>')
@login_required
def record_delete(rid):
    obj = Record.query.get_or_404(rid)
    db.session.delete(obj); db.session.commit()
    where = request.args.get('next','') or 'manual'
    flash('Registro removido.','success')
    return redirect(url_for('manual_entry') if where=='manual' else url_for('records_home'))

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
    total_rows = len(rows)
    total_splices = sum(int(r.splices or 0) for r in rows)
    total_usd = sum(float(r.total_usd or 0.0) for r in rows)

    return render_template('reports.html',
                           map_rows=sorted(agg_map.items()),
                           type_rows=sorted(agg_type.items()),
                           start=start_raw, end=end_raw,
                           total_rows=total_rows, total_splices=total_splices, total_usd=total_usd)

# ---------- Report Exports ----------
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

def _filtered_rows_for_report(args):
    start_raw = (args.get('start') or '').strip()
    end_raw = (args.get('end') or '').strip()
    q = Record.query
    if start_raw:
        try:
            sdt = datetime.strptime(start_raw, '%Y-%m-%d')
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date >= sdt)
        except: pass
    if end_raw:
        try:
            edt = datetime.strptime(end_raw, '%Y-%m-%d').replace(hour=23,minute=59,second=59,microsecond=999999)
            q = q.filter(Record.created_date.isnot(None)).filter(Record.created_date <= edt)
        except: pass
    return q.all(), start_raw, end_raw

@app.route('/export/report/xlsx')
@login_required
def export_report_xlsx():
    rows, start_raw, end_raw = _filtered_rows_for_report(request.args)
    import pandas as pd
    raw = [{
        'ID': r.id, 'Created': r.created_date, 'Map': r.map, 'Type': r.type, 'Device': r.device, 'Splices': r.splices,
        'Price Splices ($)': r.price_splices_usd, 'Price Device ($)': r.price_device_usd, 'Total ($)': r.total_usd
    } for r in rows]
    df = pd.DataFrame(raw)

    # Aggregations
    map_agg = df.groupby('Map', dropna=False).agg({'ID':'count','Splices':'sum','Total ($)':'sum'}).reset_index().rename(columns={'ID':'Linhas'}) if not df.empty else pd.DataFrame(columns=['Map','Linhas','Splices','Total ($)'])
    type_agg = df.groupby('Type', dropna=False).agg({'ID':'count','Splices':'sum','Total ($)':'sum'}).reset_index().rename(columns={'ID':'Linhas'}) if not df.empty else pd.DataFrame(columns=['Type','Linhas','Splices','Total ($)'])

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        kpi = pd.DataFrame({'Métrica':['Linhas','Fusões','Total ($)'],
                            'Valor':[int(df.shape[0]), int(df['Splices'].sum()) if not df.empty else 0, float(df['Total ($)'].sum()) if not df.empty else 0.0]})
        kpi.to_excel(w, index=False, sheet_name='KPIs')
        map_agg.to_excel(w, index=False, sheet_name='Por MAP')
        type_agg.to_excel(w, index=False, sheet_name='Por TYPE')
        df.to_excel(w, index=False, sheet_name='Registros')
    buf.seek(0)
    name = f"relatorio_{start_raw or 'inicio'}_{end_raw or 'fim'}.xlsx"
    return send_file(buf, as_attachment=True, download_name=name)

@app.route('/export/report/pdf')
@login_required
def export_report_pdf():
    rows, start_raw, end_raw = _filtered_rows_for_report(request.args)
    data_raw = [['ID','Data','Map','Type','Device','Splices','Total ($)']]
    total_usd = 0.0; total_splices = 0; total_rows = 0
    for r in rows:
        total_rows += 1; total_splices += int(r.splices or 0); total_usd += float(r.total_usd or 0)
        data_raw.append([r.id, (r.created_date.date().isoformat() if r.created_date else ''), r.map, r.type, r.device, r.splices, f"$ {float(r.total_usd or 0):.2f}"])

    from collections import defaultdict
    by_map = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    by_type = defaultdict(lambda: {'rows':0,'splices':0,'total':0.0})
    for r in rows:
        m = r.map or '—'; t = r.type or '—'
        by_map[m]['rows'] += 1; by_map[m]['splices'] += int(r.splices or 0); by_map[m]['total'] += float(r.total_usd or 0)
        by_type[t]['rows'] += 1; by_type[t]['splices'] += int(r.splices or 0); by_type[t]['total'] += float(r.total_usd or 0)

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, title="Relatório Twelve Tech")
    styles = getSampleStyleSheet()
    story = []

    periodo = f"Período: {start_raw or 'início'} até {end_raw or 'fim'}"
    story.append(Paragraph("<b>Relatório Twelve Tech</b>", styles['Title']))
    story.append(Paragraph(periodo, styles['Normal']))
    story.append(Spacer(1, 8))
    story.append(Paragraph(f"Linhas: <b>{total_rows}</b> &nbsp;&nbsp; Fusões: <b>{total_splices}</b> &nbsp;&nbsp; Total: <b>$ {total_usd:.2f}</b>", styles['Normal']))
    story.append(Spacer(1, 12))

    def make_table(title, header, rows_dict):
        story.append(Paragraph(f"<b>{title}</b>", styles['Heading3']))
        data = [header] + [[k, v['rows'], v['splices'], f"$ {v['total']:.2f}"] for k,v in sorted(rows_dict.items())]
        tbl = Table(data, hAlign='LEFT')
        tbl.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#0e1726')),
                                 ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                                 ('GRID',(0,0),(-1,-1),0.25,colors.HexColor('#334155')),
                                 ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#0b1220'), colors.HexColor('#111827')]),
                                 ('TEXTCOLOR',(0,1),(-1,-1),colors.HexColor('#e5e7eb'))]))
        story.append(tbl); story.append(Spacer(1, 10))

    make_table('Por MAP', ['Map','Linhas','Fusões','Total ($)'], by_map)
    make_table('Por TYPE', ['Type','Linhas','Fusões','Total ($)'], by_type)

    story.append(Paragraph('<b>Registros (amostra, até 40)</b>', styles['Heading3']))
    sample = data_raw[:41]  # header + 40 rows
    tbl2 = Table(sample, hAlign='LEFT')
    tbl2.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#0e1726')),
                              ('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                              ('GRID',(0,0),(-1,-1),0.25,colors.HexColor('#334155')),
                              ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#0b1220'), colors.HexColor('#111827')]),
                              ('TEXTCOLOR',(0,1),(-1,-1),colors.HexColor('#e5e7eb'))]))
    story.append(tbl2)

    doc.build(story)
    buf.seek(0)
    name = f"relatorio_{start_raw or 'inicio'}_{end_raw or 'fim'}.pdf"
    return send_file(buf, as_attachment=True, download_name=name, mimetype='application/pdf')

if __name__=='__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
