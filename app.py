from flask import Flask, render_template, request, redirect, url_for, send_file, flash, Response
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime
import os, re

# ===== Flask/Gunicorn =====
app = Flask(__name__)  # Gunicorn looks for this "app"
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY","dev-key")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL","sqlite:///app.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app); login_manager.login_view = "login"

# ===== Models =====
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=True)
    def set_password(self, p): self.password_hash = generate_password_hash(p)
    def check_password(self, p): return check_password_hash(self.password_hash, p)

class DeviceType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    value = db.Column(db.Float, default=0.0, nullable=False)

class SpliceTier(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    min_splices = db.Column(db.Integer, nullable=False)
    max_splices = db.Column(db.Integer, nullable=True)  # None = ∞
    price_per_splice = db.Column(db.Float, default=0.0, nullable=False)

class Record(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sheet = db.Column(db.String(120))
    map = db.Column(db.String(200))
    type = db.Column(db.String(120))
    splices = db.Column(db.Integer)
    device = db.Column(db.String(120))
    created_date = db.Column(db.DateTime, nullable=True)  # from file if available
    created_at = db.Column(db.DateTime, default=datetime.utcnow)  # ingestion time
    price_splices = db.Column(db.Float, default=0.0)
    price_device = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)

@login_manager.user_loader
def load_user(uid): return User.query.get(int(uid))

with app.app_context():
    db.create_all()  # Auto-create DB on boot

# ===== Excel parsing helpers =====
ALLOWED = {"xlsx","xls","csv"}
def allowed_file(name): return "." in name and name.rsplit(".",1)[1].lower() in ALLOWED
def _norm(s): return re.sub(r"\s+"," ",str(s or "")).strip().lower()

HEADER_MAP = {
    "type": [r"\btype\b", r"\btipo\b", r"\bcategory\b"],
    "map": [r"\bmap\b", r"\bmapa\b", r"map name"],
    "splices": [r"splic", r"fus"],
    "device": [r"\bdevice\b", r"serial", r"equip", r"aparelho"],
    "created_date": [r"created", r"data", r"date"]
}

def _drop_unnamed(df):
    return df[[c for c in df.columns if not _norm(c).startswith("unnamed")]]

def _rename_by_header(df):
    ren = {}
    for c in df.columns:
        n = _norm(c)
        for target, pats in HEADER_MAP.items():
            if any(re.search(p, n) for p in pats):
                ren[c] = target; break
    return df.rename(columns=ren)

def _guess_by_content(df):
    nunique = df.nunique(dropna=False)

    # type
    if "type" not in df.columns:
        like = {"splice","test","placement","service","splicing"}
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str).str.strip().str.lower()
            if (s.isin(like)).mean() > 0.25:
                df.rename(columns={c:"type"}, inplace=True); break

    # splices
    if "splices" not in df.columns:
        best,score=None,-1
        for c in df.columns:
            nums = pd.to_numeric(df[c], errors="coerce")
            if nums.notna().mean() < 0.4: continue
            ints = (nums.dropna()%1==0).mean()
            med = nums.dropna().median() if not nums.dropna().empty else 0
            sc = ints + (1.0 if med <= 300 else 0.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:"splices"}, inplace=True)

    # device
    if "device" not in df.columns:
        pat = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_/\.]{3,}$")
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str).str.strip()
            sc = s.apply(lambda x: bool(pat.match(x)) and not x.isdigit()).mean()
            if sc>score: best,score=c,sc
        if score >= 0.25: df.rename(columns={best:"device"}, inplace=True)

    # map
    if "map" not in df.columns:
        best,score=None,-1
        for c in df.columns:
            if nunique[c] <= 1: continue
            s = df[c].astype(str)
            sc = s.str.contains(",").mean()*1.5 + (s.str.len().median()/80.0)
            if sc>score: best,score=c,sc
        if best: df.rename(columns={best:"map"}, inplace=True)

    return df

def _finalize(df, sheet_name):
    keep = [c for c in ["type","map","splices","device","created_date"] if c in df.columns]
    out = df[keep].copy()
    if "splices" in out.columns:
        out["splices"] = pd.to_numeric(out["splices"], errors="coerce").fillna(0).astype(int)
    if "created_date" in out.columns:
        out["created_date"] = pd.to_datetime(out["created_date"], errors="coerce")
    out["__sheet__"] = sheet_name
    return out

def parse_df(df, sheet_name):
    df = _drop_unnamed(df.copy())
    df = _rename_by_header(df)
    df = _guess_by_content(df)
    return _finalize(df, sheet_name)

def read_table(upload):
    ext = upload.filename.rsplit(".",1)[1].lower()
    if ext=="csv":
        return parse_df(pd.read_csv(upload), "CSV")
    xl = pd.ExcelFile(upload)
    frames = []
    for nm in xl.sheet_names:
        # try header=0, fallback others handled in guess
        frames.append(parse_df(xl.parse(nm), nm))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["type","map","splices","device","created_date","__sheet__"])

# ===== Pricing =====
def price_for_splices(n):
    tier = SpliceTier.query.filter(
        (SpliceTier.min_splices <= n) &
        ((SpliceTier.max_splices >= n) | (SpliceTier.max_splices.is_(None)))
    ).order_by(SpliceTier.min_splices.desc()).first()
    return (n * tier.price_per_splice) if tier else 0.0

def price_for_device_type(type_name):
    if not type_name: return 0.0
    dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
    return dt.value if dt else 0.0

def apply_prices(df):
    if "splices" not in df.columns: df["splices"] = 0
    if "type" not in df.columns: df["type"] = None
    df["price_splices"] = df["splices"].apply(lambda n: price_for_splices(int(n) if pd.notna(n) else 0))
    df["price_device"] = df["type"].apply(lambda t: price_for_device_type(str(t)) if pd.notna(t) else 0.0)
    df["total"] = df["price_splices"] + df["price_device"]
    return df

def persist(df):
    rows = []
    for _, r in df.iterrows():
        rows.append(Record(
            sheet=str(r.get("__sheet__", "")),
            map=str(r.get("map") or ""),
            type=str(r.get("type") or ""),
            splices=int(r.get("splices") or 0),
            device=str(r.get("device") or ""),
            created_date=r.get("created_date") if isinstance(r.get("created_date"), pd.Timestamp) else None,
            price_splices=float(r.get("price_splices") or 0.0),
            price_device=float(r.get("price_device") or 0.0),
            total=float(r.get("total") or 0.0),
        ))
    if rows:
        db.session.bulk_save_objects(rows); db.session.commit()

# ===== Routes =====
@app.route("/init-admin")
def init_admin():
    user = os.environ.get("ADMIN_USERNAME","admin")
    pwd = os.environ.get("ADMIN_PASSWORD","admin123")
    if not User.query.filter_by(username=user).first():
        u = User(username=user, is_admin=True); u.set_password(pwd)
        db.session.add(u); db.session.commit()
        return f"Admin criado: {user} / {pwd}"
    return "Admin já existe."

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username","").strip()
        password = request.form.get("password","")
        u = User.query.filter_by(username=username).first()
        if u and u.check_password(password):
            login_user(u); return redirect(url_for("index"))
        flash("Usuário ou senha inválidos.", "error")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout(): logout_user(); return redirect(url_for("login"))

@app.route("/", methods=["GET","POST"])
@login_required
def index():
    if request.method == "POST":
        if "file" not in request.files or request.files["file"].filename == "":
            flash("Envie um arquivo .xlsx/.xls/.csv.", "error"); return redirect(request.url)
        f = request.files["file"]
        if not allowed_file(f.filename):
            flash("Formato não suportado.", "error"); return redirect(request.url)
        try:
            df = read_table(f)
        except Exception as e:
            flash(f"Erro ao ler arquivo: {e}", "error"); return redirect(request.url)
        df = apply_prices(df)
        persist(df)
        html = df.to_html(index=False, classes="table table-striped table-sm")
        return render_template("results.html", table_html=html)
    totals = db.session.query(db.func.count(Record.id), db.func.sum(Record.total)).first()
    return render_template("upload.html", total_rows=totals[0] or 0, total_amount=totals[1] or 0.0)

# Settings
from functools import wraps
def admin_required(fn):
    @wraps(fn)
    def w(*a, **k):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("Acesso restrito ao administrador.", "error")
            return redirect(url_for("index"))
        return fn(*a, **k)
    return w

@app.route("/settings")
@login_required
@admin_required
def settings_home():
    return render_template("settings.html",
                           types=DeviceType.query.order_by(DeviceType.name).all(),
                           tiers=SpliceTier.query.order_by(SpliceTier.min_splices).all())

@app.route("/settings/device-type", methods=["POST"])
@login_required
@admin_required
def add_device_type():
    name = request.form.get("name","").strip()
    value = float(request.form.get("value","0") or 0)
    if not name: flash("Nome do tipo é obrigatório.", "error"); return redirect(url_for("settings_home"))
    obj = DeviceType.query.filter(DeviceType.name.ilike(name)).first()
    if obj: obj.value = value
    else: db.session.add(DeviceType(name=name, value=value))
    db.session.commit(); flash("Tipo salvo.", "success"); return redirect(url_for("settings_home"))

@app.route("/settings/device-type/delete/<int:tid>")
@login_required
@admin_required
def del_device_type(tid):
    obj = DeviceType.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash("Tipo removido.", "success"); return redirect(url_for("settings_home"))

@app.route("/settings/splice-tier", methods=["POST"])
@login_required
@admin_required
def add_splice_tier():
    min_s = int(request.form.get("min_splices","0") or 0)
    max_raw = request.form.get("max_splices","").strip()
    max_s = int(max_raw) if max_raw else None
    price = float(request.form.get("price_per_splice","0") or 0)
    db.session.add(SpliceTier(min_splices=min_s, max_splices=max_s, price_per_splice=price))
    db.session.commit(); flash("Faixa salva.", "success"); return redirect(url_for("settings_home"))

@app.route("/settings/splice-tier/delete/<int:tid>")
@login_required
@admin_required
def del_splice_tier(tid):
    obj = SpliceTier.query.get_or_404(tid); db.session.delete(obj); db.session.commit()
    flash("Faixa removida.", "success"); return redirect(url_for("settings_home"))

# Reports
@app.route("/reports")
@login_required
def reports():
    start = request.args.get("start","").strip()
    end = request.args.get("end","").strip()
    map_f = request.args.get("map","").strip()
    type_f = request.args.get("type","").strip()

    q = Record.query
    if start:
        try: q = q.filter( (Record.created_date >= datetime.fromisoformat(start)) | (Record.created_date.is_(None) & (Record.created_at >= datetime.fromisoformat(start))) )
        except Exception: flash("Data inicial inválida.", "error")
    if end:
        try: q = q.filter( (Record.created_date <= datetime.fromisoformat(end+" 23:59:59")) | (Record.created_date.is_(None) & (Record.created_at <= datetime.fromisoformat(end+" 23:59:59"))) )
        except Exception: flash("Data final inválida.", "error")
    if map_f: q = q.filter(Record.map.ilike(f"%{map_f}%"))
    if type_f: q = q.filter(Record.type.ilike(f"%{type_f}%"))

    rows = q.all()
    from collections import defaultdict
    def rec_date(r): return (r.created_date or r.created_at).date() if (r.created_date or r.created_at) else None

    agg_day = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})
    agg_map = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})
    agg_type = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})

    for r in rows:
        d = rec_date(r); dk = d.isoformat() if d else "—"
        agg_day[dk]["rows"] += 1; agg_day[dk]["splices"] += int(r.splices or 0)
        agg_day[dk]["price_splices"] += float(r.price_splices or 0.0); agg_day[dk]["price_device"] += float(r.price_device or 0.0)
        agg_day[dk]["total"] += float(r.total or 0.0)

        m = r.map or "—"
        agg_map[m]["rows"] += 1; agg_map[m]["splices"] += int(r.splices or 0)
        agg_map[m]["price_splices"] += float(r.price_splices or 0.0); agg_map[m]["price_device"] += float(r.price_device or 0.0)
        agg_map[m]["total"] += float(r.total or 0.0)

        t = r.type or "—"
        agg_type[t]["rows"] += 1; agg_type[t]["splices"] += int(r.splices or 0)
        agg_type[t]["price_splices"] += float(r.price_splices or 0.0); agg_type[t]["price_device"] += float(r.price_device or 0.0)
        agg_type[t]["total"] += float(r.total or 0.0)

    return render_template("reports.html",
                           day_rows=sorted(agg_day.items()),
                           map_rows=sorted(agg_map.items()),
                           type_rows=sorted(agg_type.items()),
                           start=start, end=end, map_filter=map_f, type_filter=type_f)

@app.route("/reports/export.csv")
@login_required
def reports_export():
    import csv
    from io import StringIO
    q = Record.query.order_by(Record.id.asc()).all()
    sio = StringIO()
    w = csv.writer(sio)
    w.writerow(["id","sheet","type","map","splices","device","created_date","created_at","price_splices","price_device","total"])
    for r in q:
        w.writerow([r.id, r.sheet, r.type, r.map, r.splices, r.device,
                    r.created_date.isoformat() if r.created_date else "",
                    r.created_at.isoformat() if r.created_at else "",
                    f"{r.price_splices:.2f}", f"{r.price_device:.2f}", f"{r.total:.2f}"])
    return Response(sio.getvalue().encode("utf-8-sig"), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment; filename=records_export.csv"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
