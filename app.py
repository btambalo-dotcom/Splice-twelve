from flask import Flask, render_template, request, redirect, url_for, send_file, flash, Response
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, login_user, login_required, logout_user, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd
from datetime import datetime
import os, re

# -------- Config --------
ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv"}
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-key")
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///" + os.path.join(BASE_DIR, "app.db"))
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"

# -------- Models --------
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_admin = db.Column(db.Boolean, default=True)
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class DeviceType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    value = db.Column(db.Float, nullable=False, default=0.0)

class SpliceTier(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    min_splices = db.Column(db.Integer, nullable=False)
    max_splices = db.Column(db.Integer, nullable=True)  # None = ∞
    price_per_splice = db.Column(db.Float, nullable=False, default=0.0)

class Record(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sheet = db.Column(db.String(120))
    type = db.Column(db.String(120))
    map = db.Column(db.String(120))
    splices = db.Column(db.Integer)
    device = db.Column(db.String(120))
    created_date = db.Column(db.DateTime, nullable=True)
    price_splices = db.Column(db.Float, default=0.0)
    price_device = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)

@login_manager.user_loader
def load_user(user_id): return User.query.get(int(user_id))

# -------- Helpers --------
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

HEADER_MAP = {
    "type": [r"^type$", r"^tipo$", r"^category$", r"^classe$"],
    "map": [r"^map$", r"^mapa$", r"^map_id$", r"^id_mapa$", r"^map name$", r"^nome do mapa$"],
    "splices": [r"^splices?$", r"^fus(?:ões|oes)$", r"^qtd\s*fus(?:ões|oes)$", r"^splice count$", r"^n[úu]mero\s*de\s*fus(?:ões|oes)$"],
    "device": [r"^device$", r"^dispositivo$", r"^aparelho$", r"^equip(?:amento)?$", r"^serial$", r"^id\s*device$"],
    "created_date": [r"^created(_| )?at$", r"^data\s*de\s*cria[çc][aã]o$", r"^data$", r"^date$", r"^created$"]
}

def normalize_col(col: str):
    if col is None: return ""
    return re.sub(r"\s+", " ", str(col).strip()).lower()

def match_target(col: str):
    c = normalize_col(col)
    for target, patterns in HEADER_MAP.items():
        for pat in patterns:
            if re.match(pat, c):
                return target
    return None

def parse_dataframe(df: pd.DataFrame, sheet_name: str):
    # rename by header
    renamed = {}
    for col in df.columns:
        target = match_target(col)
        if target and target not in renamed.values():
            renamed[col] = target
    work = df.rename(columns=renamed).copy()

    # best-effort ensure
    def ensure(field, cands):
        nonlocal work
        if field not in work.columns:
            for cand in work.columns:
                cn = normalize_col(cand)
                for pat in cands:
                    if re.search(pat, cn):
                        work = work.rename(columns={cand: field})
                        return
    ensure("type", [r"type", r"tipo", r"category"])
    ensure("map", [r"map", r"mapa"])
    ensure("splices", [r"splice", r"fus"])
    ensure("device", [r"device", r"dispositivo", r"aparelho", r"equip"])
    ensure("created_date", [r"created", r"data", r"date"])

    keep = [c for c in ["type","map","splices","device","created_date"] if c in work.columns]
    sub = work[keep].copy()
    if "created_date" in sub.columns:
        sub["created_date"] = pd.to_datetime(sub["created_date"], errors="coerce")
    sub["__sheet__"] = sheet_name
    if "splices" in sub.columns:
        sub["splices"] = pd.to_numeric(sub["splices"], errors="coerce").fillna(0).astype(int)
    return sub

# ---- SMART EXCEL READER (fix for 2-row headers) ----
def combine_multiindex_columns(df):
    try:
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for tup in df.columns:
                parts = [str(x) for x in tup if x is not None and str(x).strip() != "" and not str(x).startswith("Unnamed")]
                if not parts:
                    new_cols.append("")
                else:
                    cand = parts[-1].strip()
                    if len(parts) > 1 and not re.search(r"^unnamed", parts[0], re.I):
                        name = " ".join(parts).strip()
                    else:
                        name = cand
                    new_cols.append(name)
            df.columns = new_cols
    except Exception:
        pass
    return df

def guess_fields_by_content(df):
    candidates = {"type": None, "map": None, "splices": None, "device": None}
    type_like = set(["splice","test","placement","service","splicing"])
    for c in df.columns:
        try:
            series = df[c].astype(str).str.strip().str.lower()
            if (series.isin(type_like)).mean() > 0.3:
                candidates["type"] = c; break
        except Exception:
            continue
    # splices numeric
    best_num, best_score = None, -1
    for c in df.columns:
        try:
            nums = pd.to_numeric(df[c], errors="coerce")
            if nums.notna().mean() < 0.5: continue
            ints = (nums.dropna() % 1 == 0).mean()
            median = nums.dropna().median() if not nums.dropna().empty else 0
            score = ints + (1.0 if median <= 200 else 0.0)
            if score > best_score: best_score, best_num = score, c
        except Exception: continue
    if best_num: candidates["splices"] = best_num
    # device alfanumérico
    device_pattern = re.compile(r"^[A-Za-z0-9\-_/]{4,}$")
    best_dev, best_dev_score = None, -1
    for c in df.columns:
        try:
            s = df[c].astype(str).str.strip()
            score = s.apply(lambda x: bool(device_pattern.match(x)) and not x.isdigit()).mean()
            if score > best_dev_score: best_dev_score, best_dev = score, c
        except Exception: continue
    if best_dev_score >= 0.3: candidates["device"] = best_dev
    # map: textos longos com vírgula
    best_map, best_map_score = None, -1
    for c in df.columns:
        try:
            s = df[c].astype(str)
            score = s.str.contains(",").mean() + (s.str.len().median() / 100.0)
            if score > best_map_score: best_map_score, best_map = score, c
        except Exception: continue
    candidates["map"] = best_map

    for k, src in candidates.items():
        if src and k not in df.columns:
            df.rename(columns={src: k}, inplace=True)
    return df

def read_excel_smart(file):
    # try normal header
    try:
        xl = pd.ExcelFile(file)
        frames = []
        for nm in xl.sheet_names:
            df0 = xl.parse(nm)
            df0 = parse_dataframe(df0, nm)
            frames.append(df0)
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if set(["type","map","splices","device"]).issubset(set(df.columns)): return df
    except Exception: pass
    # try multi-row header
    try:
        xl = pd.ExcelFile(file)
        frames = []
        for nm in xl.sheet_names:
            df1 = xl.parse(nm, header=[0,1])
            df1 = combine_multiindex_columns(df1)
            df1 = parse_dataframe(df1, nm)
            if not set(["type","map","splices","device"]).issubset(set(df1.columns)):
                df1 = guess_fields_by_content(df1)
            frames.append(df1)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception: pass
    # try using row 2 as header
    try:
        xl = pd.ExcelFile(file)
        frames = []
        for nm in xl.sheet_names:
            df2 = xl.parse(nm, header=1)
            df2 = parse_dataframe(df2, nm)
            if not set(["type","map","splices","device"]).issubset(set(df2.columns)):
                df2 = guess_fields_by_content(df2)
            frames.append(df2)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception: pass
    # last resort: guess only
    try:
        xl = pd.ExcelFile(file)
        frames = []
        for nm in xl.sheet_names:
            df3 = xl.parse(nm)
            df3 = guess_fields_by_content(df3)
            df3 = parse_dataframe(df3, nm)
            frames.append(df3)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception as e:
        raise e

def price_for_splices(n: int):
    tier = SpliceTier.query.filter(
        (SpliceTier.min_splices <= n) &
        ((SpliceTier.max_splices >= n) | (SpliceTier.max_splices.is_(None)))
    ).order_by(SpliceTier.min_splices.desc()).first()
    return (n * tier.price_per_splice) if tier else 0.0

def price_for_device_type(type_name: str):
    if not type_name: return 0.0
    dt = DeviceType.query.filter(DeviceType.name.ilike(str(type_name))).first()
    return dt.value if dt else 0.0

def dataframe_with_prices(df: pd.DataFrame):
    for col in ["type","splices"]:
        if col not in df.columns: df[col] = None if col=="type" else 0
    df["price_splices"] = df["splices"].apply(lambda n: price_for_splices(int(n) if pd.notna(n) else 0))
    df["price_device"] = df["type"].apply(lambda t: price_for_device_type(str(t)) if pd.notna(t) else 0.0)
    df["total"] = df["price_splices"] + df["price_device"]
    return df

def persist_records(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        rec = Record(
            sheet=str(r.get("__sheet__", "")),
            type=str(r.get("type", "")) if pd.notna(r.get("type", "")) else None,
            map=str(r.get("map", "")) if pd.notna(r.get("map", "")) else None,
            splices=int(r.get("splices", 0)) if pd.notna(r.get("splices", 0)) else 0,
            device=str(r.get("device","")) if pd.notna(r.get("device","")) else None,
            created_date=r.get("created_date") if isinstance(r.get("created_date"), pd.Timestamp) else None,
            price_splices=float(r.get("price_splices", 0.0) or 0.0),
            price_device=float(r.get("price_device", 0.0) or 0.0),
            total=float(r.get("total", 0.0) or 0.0)
        )
        rows.append(rec)
    if rows:
        db.session.bulk_save_objects(rows)
        db.session.commit()

# -------- Routes --------
@app.route("/init-admin")
def init_admin():
    username = os.environ.get("ADMIN_USERNAME", "admin")
    pwd = os.environ.get("ADMIN_PASSWORD", "admin123")
    if User.query.filter_by(username=username).first():
        return "Admin já existe."
    u = User(username=username, is_admin=True)
    u.set_password(pwd)
    db.session.add(u)
    db.session.commit()
    return f"Admin criado: {username} / {pwd}"

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username","").strip()
        password = request.form.get("password","")
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for("index"))
        flash("Usuário ou senha inválidos.", "error")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

@app.route("/", methods=["GET","POST"])
@login_required
def index():
    if request.method == "POST":
        if "file" not in request.files:
            flash("Nenhum arquivo enviado.", "error")
            return redirect(request.url)
        file = request.files["file"]
        if file.filename == "":
            flash("Selecione um arquivo.", "error")
            return redirect(request.url)
        if not allowed_file(file.filename):
            flash("Formato não suportado. Use .xlsx, .xls ou .csv", "error")
            return redirect(request.url)
        try:
            ext = file.filename.rsplit(".",1)[1].lower()
            if ext == "csv":
                df = pd.read_csv(file)
                raw = parse_dataframe(df, "CSV")
            else:
                raw = read_excel_smart(file)
        except Exception as e:
            flash(f"Erro ao ler o arquivo: {e}", "error")
            return redirect(request.url)

        priced = dataframe_with_prices(raw.copy())
        persist_records(priced)

        disp = priced.copy()
        if "created_date" in disp.columns:
            disp["created_date"] = pd.to_datetime(disp["created_date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        table_html = disp.to_html(index=False, classes="table table-striped table-sm")

        os.makedirs("tmp", exist_ok=True)
        fname = f"resultado_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.csv"
        path = os.path.join("tmp", fname)
        disp.to_csv(path, index=False, encoding="utf-8")
        return render_template("results.html", table_html=table_html, download_name=fname)

    totals = db.session.query(db.func.count(Record.id), db.func.sum(Record.total)).first()
    total_rows = totals[0] or 0
    total_amount = totals[1] or 0.0
    return render_template("upload.html", total_rows=total_rows, total_amount=total_amount)

@app.route("/download/<path:fname>")
@login_required
def download(fname):
    path = os.path.join("tmp", fname)
    if not os.path.exists(path):
        flash("Arquivo não encontrado ou expirado. Reenvie o arquivo.", "error")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True, download_name="resultado_twelve_tech.csv")

def admin_required(func):
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("Acesso restrito ao administrador.", "error")
            return redirect(url_for("index"))
        return func(*args, **kwargs)
    return wrapper

@app.route("/settings")
@login_required
@admin_required
def settings_home():
    types = DeviceType.query.order_by(DeviceType.name.asc()).all()
    tiers = SpliceTier.query.order_by(SpliceTier.min_splices.asc()).all()
    return render_template("settings.html", types=types, tiers=tiers)

@app.route("/settings/device-type", methods=["POST"])
@login_required
@admin_required
def add_device_type():
    name = request.form.get("name","").strip()
    value = float(request.form.get("value","0") or 0)
    if not name:
        flash("Nome do tipo é obrigatório.", "error")
        return redirect(url_for("settings_home"))
    existing = DeviceType.query.filter(DeviceType.name.ilike(name)).first()
    if existing:
        existing.value = value
    else:
        db.session.add(DeviceType(name=name, value=value))
    db.session.commit()
    flash("Tipo de device salvo.", "success")
    return redirect(url_for("settings_home"))

@app.route("/settings/device-type/delete/<int:tid>")
@login_required
@admin_required
def delete_device_type(tid):
    obj = DeviceType.query.get_or_404(tid)
    db.session.delete(obj)
    db.session.commit()
    flash("Tipo de device removido.", "success")
    return redirect(url_for("settings_home"))

@app.route("/settings/splice-tier", methods=["POST"])
@login_required
@admin_required
def add_splice_tier():
    try:
        min_s = int(request.form.get("min_splices","0") or 0)
        max_s_raw = request.form.get("max_splices","")
        max_s = int(max_s_raw) if max_s_raw.strip() != "" else None
        price = float(request.form.get("price_per_splice","0") or 0)
    except ValueError:
        flash("Valores inválidos no tier.", "error")
        return redirect(url_for("settings_home"))
    tier = SpliceTier(min_splices=min_s, max_splices=max_s, price_per_splice=price)
    db.session.add(tier)
    db.session.commit()
    flash("Faixa de fusões salva.", "success")
    return redirect(url_for("settings_home"))

@app.route("/settings/splice-tier/delete/<int:tid>")
@login_required
@admin_required
def delete_splice_tier(tid):
    obj = SpliceTier.query.get_or_404(tid)
    db.session.delete(obj)
    db.session.commit()
    flash("Faixa de fusões removida.", "success")
    return redirect(url_for("settings_home"))

# ----- Reports -----
@app.route("/reports", methods=["GET"])
@login_required
def reports():
    start = request.args.get("start","").strip()
    end = request.args.get("end","").strip()
    map_filter = request.args.get("map","").strip()
    type_filter = request.args.get("type","").strip()

    q = Record.query
    if start:
        try:
            dt = datetime.fromisoformat(start)
            q = q.filter( (Record.created_date >= dt) | (Record.created_date.is_(None) & (Record.created_at >= dt)) )
        except Exception:
            flash("Data inicial inválida. Use YYYY-MM-DD.", "error")
    if end:
        try:
            dt2 = datetime.fromisoformat(end + " 23:59:59")
            q = q.filter( (Record.created_date <= dt2) | (Record.created_date.is_(None) & (Record.created_at <= dt2)) )
        except Exception:
            flash("Data final inválida. Use YYYY-MM-DD.", "error")
    if map_filter:
        q = q.filter(Record.map.ilike(f"%{map_filter}%"))
    if type_filter:
        q = q.filter(Record.type.ilike(f"%{type_filter}%"))

    rows = q.all()

    from collections import defaultdict
    def rec_date(r):
        return (r.created_date or r.created_at).date() if (r.created_date or r.created_at) else None

    agg_day = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})
    agg_map = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})
    agg_type = defaultdict(lambda: {"rows":0,"splices":0,"price_splices":0.0,"price_device":0.0,"total":0.0})

    for r in rows:
        d = rec_date(r)
        key_day = d.isoformat() if d else "—"
        agg_day[key_day]["rows"] += 1
        agg_day[key_day]["splices"] += int(r.splices or 0)
        agg_day[key_day]["price_splices"] += float(r.price_splices or 0.0)
        agg_day[key_day]["price_device"] += float(r.price_device or 0.0)
        agg_day[key_day]["total"] += float(r.total or 0.0)

        m = r.map or "—"
        agg_map[m]["rows"] += 1
        agg_map[m]["splices"] += int(r.splices or 0)
        agg_map[m]["price_splices"] += float(r.price_splices or 0.0)
        agg_map[m]["price_device"] += float(r.price_device or 0.0)
        agg_map[m]["total"] += float(r.total or 0.0)

        t = r.type or "—"
        agg_type[t]["rows"] += 1
        agg_type[t]["splices"] += int(r.splices or 0)
        agg_type[t]["price_splices"] += float(r.price_splices or 0.0)
        agg_type[t]["price_device"] += float(r.price_device or 0.0)
        agg_type[t]["total"] += float(r.total or 0.0)

    day_rows = sorted( (k,v) for k,v in agg_day.items() )
    map_rows = sorted( (k,v) for k,v in agg_map.items() )
    type_rows = sorted( (k,v) for k,v in agg_type.items() )

    return render_template("reports.html",
                           start=start, end=end, map_filter=map_filter, type_filter=type_filter,
                           day_rows=day_rows, map_rows=map_rows, type_rows=type_rows)

@app.route("/reports/export.csv")
@login_required
def reports_export():
    start = request.args.get("start","").strip()
    end = request.args.get("end","").strip()
    map_filter = request.args.get("map","").strip()
    type_filter = request.args.get("type","").strip()

    q = Record.query
    if start:
        try:
            dt = datetime.fromisoformat(start)
            q = q.filter( (Record.created_date >= dt) | (Record.created_date.is_(None) & (Record.created_at >= dt)) )
        except Exception:
            pass
    if end:
        try:
            dt2 = datetime.fromisoformat(end + " 23:59:59")
            q = q.filter( (Record.created_date <= dt2) | (Record.created_date.is_(None) & (Record.created_at <= dt2)) )
        except Exception:
            pass
    if map_filter:
        q = q.filter(Record.map.ilike(f"%{map_filter}%"))
    if type_filter:
        q = q.filter(Record.type.ilike(f"%{type_filter}%"))

    import csv
    from io import StringIO
    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerow(["id","sheet","type","map","splices","device","created_date","created_at","price_splices","price_device","total"])
    for r in q.order_by(Record.id.asc()).all():
        writer.writerow([r.id, r.sheet, r.type, r.map, r.splices, r.device,
                         r.created_date.isoformat() if r.created_date else "",
                         r.created_at.isoformat() if r.created_at else "",
                         f"{r.price_splices:.2f}", f"{r.price_device:.2f}", f"{r.total:.2f}"])
    csv_bytes = sio.getvalue().encode("utf-8-sig")
    return Response(csv_bytes, mimetype="text/csv",
                    headers={"Content-Disposition":"attachment; filename=records_export.csv"})

@app.cli.command("init-db")
def init_db():
    db.create_all()
    print("DB criado. Use /init-admin para criar o admin.")

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(host="0.0.0.0", port=8000, debug=True)
