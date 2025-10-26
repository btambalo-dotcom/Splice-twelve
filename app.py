
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime
import os, re

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY","dev")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL","sqlite:///app.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app); login_manager.login_view = "login"

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    def set_password(self, p): self.password_hash = generate_password_hash(p)
    def check_password(self, p): return check_password_hash(self.password_hash, p)

class Record(db.Model):
    id = db.Column(db.Integer, primary_key=True)
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

# ---------------- Parsing patched ----------------
ALLOWED = {"xlsx","xls","csv"}
def allowed_file(fn): 
    return "." in fn and fn.rsplit(".",1)[1].lower() in ALLOWED

def _norm(s: str) -> str:
    base = str(s or "").strip().lower()
    base = (base.replace("á","a").replace("à","a").replace("â","a").replace("ã","a")
                 .replace("é","e").replace("ê","e")
                 .replace("í","i")
                 .replace("ó","o").replace("ô","o").replace("õ","o")
                 .replace("ú","u")
                 .replace("ç","c"))
    return base

HEADER_ALIASES = {
    "type": {"type","tipo","category","classe"},
    "map": {"map","mapa","map name","nome do mapa"},
    "splices": {"splices","splice","fusoes","fusao","fusões","fusão","qtd fusoes","splice count","splice qty"},
    "device": {"device","dispositivo","equipamento","serial"},
    "created_date": {"created","created_at","data","date","created date"},
    "splicer": {"splicer","tecnico","técnico","technician"}
}

def _rename_by_header(df: pd.DataFrame) -> pd.DataFrame:
    ren = {}
    for c in df.columns:
        base = _norm(c)
        for target, aliases in HEADER_ALIASES.items():
            if any(a in base for a in aliases):
                ren[c] = target
                break
    return df.rename(columns=ren)

def _guess_missing(df: pd.DataFrame) -> pd.DataFrame:
    nunique = df.nunique(dropna=False)
    # type
    if "type" not in df.columns:
        tokens = {"splice","test","placement","service","splicing"}
        for c in df.columns:
            s = df[c].astype(str).str.strip().str.lower()
            if (s.isin(tokens)).mean() > 0.25:
                df.rename(columns={c:"type"}, inplace=True); break
    # splices
    if "splices" not in df.columns:
        best,score=None,-1
        for c in df.columns:
            nums = pd.to_numeric(df[c], errors="coerce")
            if nums.notna().mean() < 0.4: continue
            ints = (nums.dropna()%1==0).mean()
            med  = nums.dropna().median() if not nums.dropna().empty else 0
            sc   = ints + (1.0 if med<=300 else 0.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:"splices"}, inplace=True)
    # device
    if "device" not in df.columns:
        pat = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_/\.\s]{3,}$")
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str).str.strip()
            sc = s.apply(lambda x: bool(pat.match(x)) and not x.isdigit()).mean()
            if sc>score: best,score=c,sc
        if score>=0.25: df.rename(columns={best:"device"}, inplace=True)
    # map
    if "map" not in df.columns:
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str)
            sc = s.str.contains(",", na=False).mean()*1.5 + (s.str.len().median()/80.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:"map"}, inplace=True)
    return df

def read_table(upload) -> pd.DataFrame:
    ext = upload.filename.rsplit(".",1)[1].lower()
    if ext == "csv":
        df = pd.read_csv(upload)
        return _guess_missing(_rename_by_header(df))
    xl = pd.ExcelFile(upload)
    frames = []
    for nm in xl.sheet_names:
        df0 = xl.parse(nm)
        frames.append(_guess_missing(_rename_by_header(df0)))
    return pd.concat(frames, ignore_index=True)

# ---------------- Routes ----------------
@app.route("/init-admin")
def init_admin():
    user = os.environ.get("ADMIN_USERNAME","admin")
    pwd  = os.environ.get("ADMIN_PASSWORD","admin123")
    if not User.query.filter_by(username=user).first():
        u = User(username=user); u.set_password(pwd)
        db.session.add(u); db.session.commit()
        return f"Admin criado: {user} / {pwd}"
    return "Admin já existe."

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method=="POST":
        u = User.query.filter_by(username=request.form.get("username","")).first()
        if u and u.check_password(request.form.get("password","")):
            login_user(u); return redirect(url_for("index"))
        flash("Usuário ou senha inválidos.","error")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout(): logout_user(); return redirect(url_for("login"))

@app.route("/", methods=["GET","POST"])
@login_required
def index():
    if request.method=="POST":
        f = request.files.get("file")
        if not f or f.filename=="":
            flash("Envie um arquivo .xlsx/.xls/.csv","error"); return redirect(request.url)
        if not allowed_file(f.filename):
            flash("Formato não suportado.","error"); return redirect(request.url)
        try:
            df = read_table(f)
        except Exception as e:
            flash(f"Erro ao ler arquivo: {e}","error"); return redirect(request.url)
        # persist
        for _, r in df.iterrows():
            created = r.get("created_date")
            if isinstance(created, pd.Timestamp):
                created = created.to_pydatetime()
            db.session.add(Record(
                map=str(r.get("map") or ""),
                type=str(r.get("type") or ""),
                splices=int(r.get("splices") or 0),
                device=str(r.get("device") or ""),
                created_date=created,
                splicer=str(r.get("splicer") or "")
            ))
        db.session.commit()
        return render_template("results.html", table_html=df.to_html(index=False, classes="table table-striped table-sm"))
    count = Record.query.count()
    return render_template("upload.html", total_rows=count)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
