from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime
from collections import defaultdict
import os, re, uuid
from pathlib import Path

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

# ---------- Helpers ----------
ALLOWED={'xlsx','xls','csv'}
def allowed_file(n): return '.' in n and n.rsplit('.',1)[1].lower() in ALLOWED

def _norm(s):
    base = str(s or '').strip().strip('"').strip("'").lower()
    return (base.replace('á','a').replace('à','a').replace('â','a').replace('ã','a')
                .replace('é','e').replace('ê','e').replace('í','i')
                .replace('ó','o').replace('ô','o').replace('õ','o')
                .replace('ú','u').replace('ç','c'))

def make_unique(cols):
    seen = {}
    out = []
    for c in cols:
        base = str(c)
        if base not in seen:
            seen[base]=1; out.append(base)
        else:
            seen[base]+=1; out.append(f"{base}_{seen[base]}")
    return out

HEADER_ALIASES = {
    'type': {'type','tipo','category','classe'},
    'map': {'map','mapa','map name','nome do mapa'},
    'splices': {'splices','splice','fusoes','fusao','fusões','fusão','qtd fusoes','splice count','splice qty'},
    'device': {'device','dispositivo','equipamento','serial'},
    'created_date': {'created','created_at','data','date','created date'},
    'splicer': {'splicer','tecnico','técnico','technician'}
}

def sanitize_columns(df):
    # strip quotes/whitespace
    df.columns = [str(c).strip().strip('"').strip("'") for c in df.columns]
    # drop exact duplicates (keeps first)
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]
    # ensure uniqueness
    df.columns = make_unique(df.columns)
    return df

def rename_headers(df):
    df = sanitize_columns(df)
    ren = {}
    for c in df.columns:
        base = _norm(c)
        for target, aliases in HEADER_ALIASES.items():
            if any((base == _norm(a)) or (_norm(a) in base) for a in aliases):
                t = target if target not in df.columns else f"{target}_2"
                ren[c] = t; break
    df = df.rename(columns=ren)
    dropcols = [c for c in df.columns if c.endswith('_2') and c[:-2] in df.columns]
    if dropcols: df.drop(columns=dropcols, inplace=True)
    return df

def guess_missing(df):
    nunique = df.nunique(dropna=False)
    if 'type' not in df.columns:
        tokens={'splice','test','placement','service','splicing'}
        for c in df.columns:
            s = df[c].astype(str).str.strip().str.lower()
            if (s.isin(tokens)).mean() > 0.25: df.rename(columns={c:'type'}, inplace=True); break
    if 'splices' not in df.columns:
        best,score=None,-1
        for c in df.columns:
            nums = pd.to_numeric(df[c], errors='coerce')
            if nums.notna().mean() < 0.4: continue
            ints = (nums.dropna()%1==0).mean()
            med  = nums.dropna().median() if not nums.dropna().empty else 0
            sc   = ints + (1.0 if med<=300 else 0.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:'splices'}, inplace=True)
    if 'device' not in df.columns:
        import re
        pat = re.compile(r'^[A-Za-z0-9][A-Za-z0-9\-_/\.\s]{3,}$')
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str).str.strip()
            sc = s.apply(lambda x: bool(pat.match(x)) and not x.isdigit()).mean()
            if sc>score: best,score=c,sc
        if score>=0.25: df.rename(columns={best:'device'}, inplace=True)
    if 'map' not in df.columns:
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str)
            sc = s.str.contains(',', na=False).mean()*1.5 + (s.str.len().median()/80.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:'map'}, inplace=True)
    return df

def finalize(df, sheet_name):
    if hasattr(df.columns, 'levels'):
        df.columns = [' '.join([str(x) for x in t if str(x)!='']) for t in df.columns]
    df = sanitize_columns(df)
    df = rename_headers(df)
    df = guess_missing(df)
    wanted = ['type','map','splices','device','created_date','splicer']
    # Keep only known/wanted columns to avoid reindex error paths
    keep = [c for c in df.columns if c in wanted]
    df = df[keep] if keep else pd.DataFrame(columns=wanted)
    out = df.copy()
    if 'splices' not in out.columns: out['splices'] = 0
    if 'type' not in out.columns: out['type'] = None
    if 'map' not in out.columns: out['map'] = None
    if 'device' not in out.columns: out['device'] = None
    if 'created_date' in out.columns: out['created_date'] = pd.to_datetime(out['created_date'], errors='coerce')
    out['splices'] = pd.to_numeric(out['splices'], errors='coerce').fillna(0).astype(int)
    out['__sheet__'] = sheet_name
    return out[['type','map','splices','device','created_date','splicer','__sheet__']]

def read_table(upload):
    ext = upload.filename.rsplit('.',1)[1].lower()
    if ext=='csv':
        df = pd.read_csv(upload, sep=None, engine='python', quotechar='"', skipinitialspace=True)
        return finalize(df, 'CSV')
    xl = pd.ExcelFile(upload)
    frames = [finalize(xl.parse(n), n) for n in xl.sheet_names]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=['type','map','splices','device','created_date','splicer','__sheet__'])

# ---------- Pricing ----------
class PricingMixin:
    @staticmethod
    def price_for_splices(n):
        tier = SpliceTier.query.filter((SpliceTier.min_splices <= n) & ((SpliceTier.max_splices >= n) | (SpliceTier.max_splices.is_(None)))).order_by(SpliceTier.min_splices.desc()).first()
        return (n * tier.price_per_splice) if tier else 0.0
    @staticmethod
    def price_for_device_type(type_name):
        if not type_name: return 0.0
        dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
        return dt.value if dt else 0.0

def apply_prices(df):
    df['price_splices'] = df['splices'].apply(lambda n: PricingMixin.price_for_splices(int(n) if pd.notna(n) else 0))
    df['price_device'] = df['type'].apply(lambda t: PricingMixin.price_for_device_type(str(t)) if pd.notna(t) else 0.0)
    df['total'] = df['price_splices'] + df['price_device']
    return df

def persist(df):
    rows=[]
    for _, r in df.iterrows():
        rows.append(Record(sheet=str(r.get('__sheet__','')), map=str(r.get('map') or ''), type=str(r.get('type') or ''),
                           splices=int(r.get('splices') or 0), device=str(r.get('device') or ''),
                           created_date=r.get('created_date') if isinstance(r.get('created_date'), pd.Timestamp) else None,
                           splicer=str(r.get('splicer') or ''),
                           price_splices=float(r.get('price_splices') or 0.0), price_device=float(r.get('price_device') or 0.0),
                           total=float(r.get('total') or 0.0)))
    if rows:
        db.session.bulk_save_objects(rows); db.session.commit()

# ---------- Routes ----------
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

@app.route('/', methods=['GET','POST'])
@login_required
def index():
    if request.method=='POST':
        f = request.files.get('file')
        if not f or f.filename=='': flash('Envie um arquivo .csv/.xlsx/.xls','error'); return redirect(request.url)
        if not allowed_file(f.filename): flash('Formato não suportado.','error'); return redirect(request.url)
        try:
            df = read_table(f)
        except Exception as e:
            flash(f'Erro ao ler arquivo: {e}','error'); return redirect(request.url)
        df = apply_prices(df); persist(df)

        token = uuid.uuid4().hex
        csv_path = EXPORT_DIR / f'clean_{token}.csv'
        xlsx_path = EXPORT_DIR / f'clean_{token}.xlsx'
        df.to_csv(csv_path, index=False)
        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='dados')

        session['last_token'] = token
        table_html = df.head(100).to_html(index=False, classes='table table-striped table-sm')
        return render_template('results.html', table_html=table_html, token=token)

    from sqlalchemy import func
    totals = db.session.query(func.count(Record.id), func.sum(Record.total)).first()
    return render_template('upload.html', total_rows=totals[0] or 0, total_amount=totals[1] or 0.0)

@app.route('/download/csv/<token>')
@login_required
def download_csv(token):
    path = EXPORT_DIR / f'clean_{token}.csv'
    if not path.exists():
        flash('Arquivo não encontrado. Envie novamente o arquivo para processar.','error')
        return redirect(url_for('index'))
    return send_file(path, as_attachment=True, download_name='dados_limpos.csv')

@app.route('/download/xlsx/<token>')
@login_required
def download_xlsx(token):
    path = EXPORT_DIR / f'clean_{token}.xlsx'
    if not path.exists():
        flash('Arquivo não encontrado. Envie novamente o arquivo para processar.','error')
        return redirect(url_for('index'))
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
    if not name:
        flash('Nome do tipo é obrigatório.','error'); return redirect(url_for('settings_home'))
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
