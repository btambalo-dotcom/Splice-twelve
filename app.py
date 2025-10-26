from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime
import os, re, uuid
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
EXPORT_DIR = BASE_DIR / 'exports'
EXPORT_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY','dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL','sqlite:///app.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app); login_manager.login_view = 'login'

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=True)
    def set_password(self,p): self.password_hash = generate_password_hash(p)
    def check_password(self,p): return check_password_hash(self.password_hash,p)

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

@login_manager.user_loader
def load_user(uid): return User.query.get(int(uid))

with app.app_context():
    db.create_all()

# ---------- Helpers ----------
ALLOWED={'xlsx','xls','csv'}
def allowed_file(n): return '.' in n and n.rsplit('.',1)[1].lower() in ALLOWED

def _norm(s):
    base = str(s or '').strip().lower()
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

def rename_headers(df):
    # First, deduplicate any duplicate names coming from source
    df.columns = make_unique(df.columns)
    ren = {}
    for c in df.columns:
        base = _norm(c)
        for target, aliases in HEADER_ALIASES.items():
            if any(a == base or a in base for a in aliases):
                # avoid overwriting an existing mapped target: keep unique by suffix
                target_name = target if target not in ren.values() and target not in df.columns else f"{target}_2"
                ren[c] = target_name; break
    df = df.rename(columns=ren)
    # If we created *_2 due to duplicates, prefer the first one
    prefer = {}
    for c in df.columns:
        if c.endswith('_2') and c[:-2] in df.columns:
            # drop the *_2 since base exists
            df.drop(columns=[c], inplace=True)
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
    df.columns = make_unique(df.columns)
    df = rename_headers(df)
    df = guess_missing(df)
    wanted = ['type','map','splices','device','created_date','splicer']
    out = df.reindex(columns=[c for c in wanted if c in df.columns]).copy()
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
        df = pd.read_csv(upload, sep=None, engine='python')
        return finalize(df, 'CSV')
    xl = pd.ExcelFile(upload)
    frames = [finalize(xl.parse(n), n) for n in xl.sheet_names]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=['type','map','splices','device','created_date','splicer','__sheet__'])

# ---------- Routes ----------
@login_manager.user_loader
def user_loader(uid): return User.query.get(int(uid))

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

        token = uuid.uuid4().hex
        csv_path = EXPORT_DIR / f'clean_{token}.csv'
        xlsx_path = EXPORT_DIR / f'clean_{token}.xlsx'
        df.to_csv(csv_path, index=False)
        with pd.ExcelWriter(xlsx_path, engine='openpyxl') as w:
            df.to_excel(w, index=False, sheet_name='dados')

        session['last_token'] = token
        table_html = df.head(100).to_html(index=False, classes='table table-striped table-sm')
        return render_template('results.html', table_html=table_html, token=token)

    # summary (not computing totals here)
    total = db.session.query(db.func.count(Record.id)).scalar() or 0
    return render_template('upload.html', total_rows=total, total_amount=0)

@app.route('/download/csv/<token>')
@login_required
def download_csv(token):
    path = EXPORT_DIR / f'clean_{token}.csv'
    if not path.exists():
        flash('Arquivo não encontrado. Envie novamente.','error'); return redirect(url_for('index'))
    return send_file(path, as_attachment=True, download_name='dados_limpos.csv')

@app.route('/download/xlsx/<token>')
@login_required
def download_xlsx(token):
    path = EXPORT_DIR / f'clean_{token}.xlsx'
    if not path.exists():
        flash('Arquivo não encontrado. Envie novamente.','error'); return redirect(url_for('index'))
    return send_file(path, as_attachment=True, download_name='dados_limpos.xlsx')

if __name__=='__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
