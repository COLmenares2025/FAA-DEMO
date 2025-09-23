import os
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Response
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import sqlite3
from .importer import import_csv_bytes

from .db import connect, DB_PATH
from .schema_sql import SCHEMA_SQL
from .utils import normalize_payload, fingerprint_from_row, diff_rows
from .auth import (
    ensure_default_users,
    create_session,
    delete_session,
    verify_password,
    require_role,
    require_user,
)

# Static files (serve /static/*)
## STATIC MOUNT MOVED BELOW

app = FastAPI(title="Air Audit v1")

# CORS configurable por variables de entorno
def _parse_csv_env(s: str) -> list[str]:
    parts = [p.strip() for p in (s or "").split(",")]
    return [p for p in parts if p]

_origins_env = os.getenv("ALLOWED_ORIGINS", "*")
_methods_env = os.getenv("CORS_ALLOW_METHODS", "*")
_headers_env = os.getenv("CORS_ALLOW_HEADERS", "*")
_creds_env = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() in ("1", "true", "yes")

_allow_origins = ["*"] if _origins_env.strip() == "*" else _parse_csv_env(_origins_env)
_allow_methods = ["*"] if _methods_env.strip() == "*" else _parse_csv_env(_methods_env)
_allow_headers = ["*"] if _headers_env.strip() == "*" else _parse_csv_env(_headers_env)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_credentials=_creds_env,
    allow_methods=_allow_methods,
    allow_headers=_allow_headers,
)

# Static files (serve /static/*)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.on_event("startup")
def startup():
    # Ensure schema
    with connect() as con:
        cur = con.cursor()
        cur.executescript(SCHEMA_SQL)
        # --- MIGRACIÓN BORRADO LÓGICO (si no existen las columnas) ---
        cur.execute("PRAGMA table_info(aircraft)")
        cols = {r[1] for r in cur.fetchall()}
        if "is_active" not in cols:
            cur.execute("ALTER TABLE aircraft ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "archived_at" not in cols:
            cur.execute("ALTER TABLE aircraft ADD COLUMN archived_at TEXT")
        # índice útil
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aircraft_active ON aircraft(is_active)")
        ensure_default_users(cur)
        con.commit()

    # Migration: add actor columns to data_ledger (idempotent)
    with connect() as con:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(data_ledger)")
        dcols = {r[1] for r in cur.fetchall()}
        if "actor_user_id" not in dcols:
            cur.execute("ALTER TABLE data_ledger ADD COLUMN actor_user_id INTEGER")
        if "actor_username" not in dcols:
            cur.execute("ALTER TABLE data_ledger ADD COLUMN actor_username TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ledger_actor ON data_ledger(actor_user_id)")
        con.commit()


# ---------------------- Auth ----------------------


@app.post("/auth/login")
def login(payload: Dict[str, str], response: Response):
    username = (payload.get("username") or "").strip().lower()
    password = payload.get("password") or ""
    if not username or not password:
        raise HTTPException(status_code=400, detail="Usuario y contraseña requeridos")
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT id, username, password_hash, password_salt, role FROM users WHERE username=?",
            (username,),
        ).fetchone()
        if not row or not verify_password(password, row["password_salt"], row["password_hash"]):
            raise HTTPException(status_code=401, detail="Credenciales inválidas")
        session = create_session(cur, row["id"])
        con.commit()
        # Set cookie httpOnly con el token de sesión
        cookie_params = {
            "httponly": True,
            "samesite": "lax",
            "path": "/",
        }
        # Overrides por entorno
        samesite_env = (os.getenv("COOKIE_SAMESITE", "lax") or "").strip().lower()
        if samesite_env in ("lax", "strict", "none"):
            cookie_params["samesite"] = samesite_env
        domain_env = (os.getenv("COOKIE_DOMAIN", "") or "").strip()
        if domain_env:
            cookie_params["domain"] = domain_env
        secure_env = os.getenv("COOKIE_SECURE", "false").lower() in ("1", "true", "yes")
        if cookie_params.get("samesite") == "none" or secure_env:
            cookie_params["secure"] = True
        if os.getenv("COOKIE_SECURE", "false").lower() in ("1", "true", "yes"):
            cookie_params["secure"] = True
        response.set_cookie(key="session", value=session["token"], **cookie_params)
        return {
            "token": session["token"],
            "expires_at": session["expires_at"],
            "user": {"id": row["id"], "username": row["username"], "role": row["role"]},
        }


@app.post("/auth/logout")
def logout(response: Response, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        cur = con.cursor()
        delete_session(cur, current_user["session_token"])
        con.commit()
    response.delete_cookie("session", path="/")
    return {"ok": True}


@app.get("/auth/session")
def session(current_user: Dict[str, Any] = Depends(require_user)):
    return {
        "active": True,
        "user": {
            "id": current_user["id"],
            "username": current_user["username"],
            "role": current_user["role"],
        },
        "expires_at": current_user["session_expires_at"],
    }

@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# ---------------------- Aircraft ----------------------
@app.post("/aircraft")
def create_aircraft(
    name: str = Form(...),
    model: str = Form("N/A"),
    # TIME & CYCLES (opcionales, sugeridos por UI)
    aircraft_hours: Optional[float] = Form(None),
    aircraft_landings: Optional[int] = Form(None),
    apu_hours: Optional[float] = Form(None),
    apu_cycles: Optional[int] = Form(None),
    engine_1_hours: Optional[float] = Form(None),
    engine_1_cycles: Optional[int] = Form(None),
    engine_2_hours: Optional[float] = Form(None),
    engine_2_cycles: Optional[int] = Form(None),
    current_user: Dict[str, Any] = Depends(require_role("admin")),
):
    name = name.strip()
    model = (model or "N/A").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nombre requerido")
    with connect() as con:
        cur = con.cursor()
        cur.execute("INSERT INTO aircraft(name, model) VALUES (?,?)", (name, model))
        aircraft_id = cur.lastrowid
        # Guardar TIME & CYCLES si fueron provistos
        def _nn(x):
            return x if x is None else (float(x) if isinstance(x, (int, float, str)) else None)
        tc_vals = {
            "aircraft_hours": aircraft_hours,
            "aircraft_landings": aircraft_landings,
            "apu_hours": apu_hours,
            "apu_cycles": apu_cycles,
            "engine_1_hours": engine_1_hours,
            "engine_1_cycles": engine_1_cycles,
            "engine_2_hours": engine_2_hours,
            "engine_2_cycles": engine_2_cycles,
        }
        # Validaciones básicas (no negativos)
        for k, v in list(tc_vals.items()):
            if v is None:
                continue
            try:
                if k.endswith("_hours"):
                    v2 = float(v)
                else:
                    v2 = int(v)
                if v2 < 0:
                    raise ValueError
                tc_vals[k] = v2
            except Exception:
                raise HTTPException(status_code=400, detail=f"{k} inválido (no negativo)")
        if any(v is not None for v in tc_vals.values()):
            cur.execute(
                """
                INSERT INTO aircraft_time_cycles(
                    aircraft_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles,
                    engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?, datetime('now'))
                """,
                (
                    aircraft_id,
                    tc_vals["aircraft_hours"], tc_vals["aircraft_landings"], tc_vals["apu_hours"], tc_vals["apu_cycles"],
                    tc_vals["engine_1_hours"], tc_vals["engine_1_cycles"], tc_vals["engine_2_hours"], tc_vals["engine_2_cycles"],
                ),
            )
        # Ledger: creación de avión
        details = {
            "action": "CREATE",
            "table": "aircraft",
            "aircraft_id": aircraft_id,
            "values": {"name": name, "model": model}
        }
        log_ledger(cur, "aircraft", "CREATE", aircraft_id, None, details, current_user)
        con.commit()
        return {"id": aircraft_id, "name": name, "model": model}

"""@app.get("/aircraft")
def list_aircraft():
    with connect() as con:
        rows = con.execute("SELECT id, name, model, created_at FROM aircraft ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]"""

@app.post("/aircraft/{aircraft_id}/archive")
def archive_aircraft(aircraft_id: int, current_user: Dict[str, Any] = Depends(require_role("admin"))):
    with connect() as con:
        cur = con.cursor()
        r = cur.execute("SELECT id, name, model, is_active, archived_at FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        if r["is_active"] == 0:
            return {"id": aircraft_id, "status": "already_archived"}
        before = {"is_active": r["is_active"], "archived_at": r["archived_at"]}
        cur.execute("UPDATE aircraft SET is_active=0, archived_at=datetime('now') WHERE id=?", (aircraft_id,))
        after_row = cur.execute("SELECT is_active, archived_at FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        after = {"is_active": after_row["is_active"], "archived_at": after_row["archived_at"]}
        # Ledger
        details = {
            "action": "ARCHIVE",
            "table": "aircraft",
            "aircraft_id": aircraft_id,
            "name": r["name"],
            "model": r["model"],
            "before": before,
            "after": after
        }
        log_ledger(cur, "aircraft", "ARCHIVE", aircraft_id, None, details, current_user)
        con.commit()
        return {"id": aircraft_id, "status": "archived"}

@app.post("/aircraft/{aircraft_id}/restore")
def restore_aircraft(aircraft_id: int, current_user: Dict[str, Any] = Depends(require_role("admin"))):
    with connect() as con:
        cur = con.cursor()
        r = cur.execute("SELECT id, name, model, is_active, archived_at FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        if r["is_active"] == 1:
            return {"id": aircraft_id, "status": "already_active"}
        before = {"is_active": r["is_active"], "archived_at": r["archived_at"]}
        cur.execute("UPDATE aircraft SET is_active=1, archived_at=NULL WHERE id=?", (aircraft_id,))
        after_row = cur.execute("SELECT is_active, archived_at FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        after = {"is_active": after_row["is_active"], "archived_at": after_row["archived_at"]}
        # Ledger
        details = {
            "action": "RESTORE",
            "table": "aircraft",
            "aircraft_id": aircraft_id,
            "name": r["name"],
            "model": r["model"],
            "before": before,
            "after": after
        }
        log_ledger(cur, "aircraft", "RESTORE", aircraft_id, None, details, current_user)
        con.commit()
        return {"id": aircraft_id, "status": "restored"}

@app.get("/aircraft")
def list_aircraft(include_archived: int = 0, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        if include_archived:
            rows = con.execute(
                "SELECT id, name, model, created_at, is_active, archived_at FROM aircraft ORDER BY id DESC"
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, name, model, created_at, is_active, archived_at FROM aircraft WHERE is_active=1 ORDER BY id DESC"
            ).fetchall()
        return [dict(r) for r in rows]


# ---------------------- Items (published) ----------------------
@app.get("/aircraft/{aircraft_id}/items")
def list_items(
    aircraft_id: int,
    limit: int = 50,
    offset: int = 0,
    search: Optional[str] = None,
    current_user: Dict[str, Any] = Depends(require_user),
):
    q = """
        SELECT mi.*
        FROM v_items_loaded mi
        WHERE mi.aircraft_id = :aid
    """
    params = {"aid": aircraft_id, "limit": limit, "offset": offset}
    if search:
        q += " AND (mi.description LIKE :kw OR mi.item_code LIKE :kw)"
        params["kw"] = f"%{search}%"
    q += " ORDER BY mi.id LIMIT :limit OFFSET :offset"
    with connect() as con:
        rows = con.execute(q, params).fetchall()
        return [dict(r) for r in rows]

@app.get("/aircraft/{aircraft_id}/items/count")
def count_items(
    aircraft_id: int,
    search: Optional[str] = None,
    current_user: Dict[str, Any] = Depends(require_user),
):
    q = """
        SELECT COUNT(*) AS n
        FROM v_items_loaded mi
        WHERE mi.aircraft_id = :aid
    """
    params = {"aid": aircraft_id}
    if search:
        q += " AND (mi.description LIKE :kw OR mi.item_code LIKE :kw)"
        params["kw"] = f"%{search}%"
    with connect() as con:
        n = con.execute(q, params).fetchone()["n"]
        return {"count": int(n)}

# ---------------------- Item detail & update ----------------------
@app.get("/items/{item_id}")
def get_item(item_id: int, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        r = con.execute("SELECT * FROM maintenance_item WHERE id=?", (item_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Item no encontrado")
        return dict(r)

@app.put("/items/{item_id}")
def update_item(
    item_id: int,
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role("admin")),
):
    # Normalize input
    update = normalize_payload(payload)
    if not update:
        raise HTTPException(status_code=400, detail="Sin cambios")
    # Fetch current row
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("SELECT * FROM maintenance_item WHERE id=?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Item no encontrado")
        current = dict(row)
        # Merge values
        new_values = current.copy()
        new_values.update(update)
        # Recompute fingerprint (may change if key fields change)
        new_values["fingerprint"] = fingerprint_from_row(new_values)
        # Build dynamic SQL
        sets = [f"{k}=:{k}" for k in update.keys()]
        sets.append("fingerprint=:fingerprint")
        sql = "UPDATE maintenance_item SET " + ", ".join(sets) + " WHERE id=:id"
        try:
            cur.execute(sql, {**new_values, "id": item_id})
        except sqlite3.IntegrityError as e:
            # Likely UNIQUE(import_batch_id, fingerprint) conflict
            raise HTTPException(status_code=409, detail=f"Conflicto de duplicado dentro del lote: {e}")
        # Ledger
        changed = diff_rows(current, new_values)
        if changed:
            details = {"action":"UPDATE","table":"maintenance_item","item_id": item_id,"diff": changed}
            log_ledger(cur, "maintenance_item", "UPDATE", item_id, current["import_batch_id"], details, current_user)
        con.commit()
        return {"id": item_id, "updated_fields": list(update.keys())}

# ---------------------- Create manual item ----------------------
def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)

def log_ledger(cur, table_name: str, action: str, row_id: Optional[int], import_batch_id: Optional[int], details: Dict[str, Any], actor: Optional[Dict[str, Any]] = None) -> None:
    actor_user_id = None
    actor_username = None
    if actor:
        try:
            actor_user_id = int(actor.get("id"))
        except Exception:
            actor_user_id = None
        actor_username = actor.get("username")
    cur.execute(
        "INSERT INTO data_ledger(table_name, action, row_id, import_batch_id, actor_user_id, actor_username, details) VALUES (?,?,?,?,?,?,?)",
        (table_name, action, row_id, import_batch_id, actor_user_id, actor_username, json_dumps(details))
    )

@app.post("/aircraft/{aircraft_id}/items")
def create_item(
    aircraft_id: int,
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic")),
):
    data = normalize_payload(payload)
    # Required
    if not data.get("description"):
        raise HTTPException(status_code=400, detail="description es requerido")
    # Find or create a manual import batch
    manual_sig = f"manual::{aircraft_id}"
    with connect() as con:
        cur = con.cursor()
        # verify aircraft exists
        a = cur.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        b = cur.execute("SELECT id FROM import_batch WHERE aircraft_id=? AND file_sha256=?", (aircraft_id, manual_sig)).fetchone()
        if not b:
            cur.execute("""
                INSERT INTO import_batch(aircraft_id, file_name, file_sha256, total_rows, inserted_rows, error_rows, status)
                VALUES (?, 'manual-api', ?, 0, 0, 0, 'loaded')
            """, (aircraft_id, manual_sig))
            batch_id = cur.lastrowid
        else:
            batch_id = b["id"]
        # Prepare insert
        data_full = {
            "aircraft_id": aircraft_id,
            "import_batch_id": batch_id,
            **data
        }
        data_full["fingerprint"] = fingerprint_from_row(data_full)
        cols = ",".join(data_full.keys())
        vals = ":" + ",:".join(data_full.keys())
        sql = f"INSERT INTO maintenance_item({cols}) VALUES ({vals})"
        try:
            cur.execute(sql, data_full)
        except sqlite3.IntegrityError as e:
            # Duplicate inside manual batch
            raise HTTPException(status_code=409, detail=f"Ítem duplicado dentro del lote manual: {e}")
        # Update batch counters (optional)
        cur.execute("UPDATE import_batch SET inserted_rows = inserted_rows + 1 WHERE id=?", (batch_id,))
        # Ledger
        details = {"action":"INSERT","table":"maintenance_item","item_id": cur.lastrowid,"source":"manual","values": data}
        log_ledger(cur, "maintenance_item", "INSERT", cur.lastrowid, batch_id, details, current_user)
        con.commit()
        return {"id": cur.lastrowid, "import_batch_id": batch_id}

@app.get("/aircraft/{aircraft_id}/audit")
def get_aircraft_audit(
    aircraft_id: int,
    limit: int = 50,
    offset: int = 0,
    current_user: Dict[str, Any] = Depends(require_user),
):
    """
    Devuelve eventos del ledger relacionados al avión:
      - Eventos directos de 'aircraft' (CREATE/ARCHIVE/RESTORE)
      - Eventos de 'maintenance_item' asociados a este aircraft (INSERT/UPDATE)
      - (Opcional) Eventos de 'import_batch' del aircraft (UPDATE de estado)
    """
    with connect() as con:
        cur = con.cursor()
        q = """
        SELECT l.id, l.ts, l.table_name, l.action, l.row_id, l.import_batch_id, l.actor_user_id, l.actor_username, l.details
        FROM data_ledger l
        WHERE l.table_name = 'aircraft' AND l.row_id = :aid

        UNION ALL

        SELECT l.id, l.ts, l.table_name, l.action, l.row_id, l.import_batch_id, l.actor_user_id, l.actor_username, l.details
        FROM data_ledger l
        JOIN maintenance_item mi ON mi.id = l.row_id
        WHERE l.table_name = 'maintenance_item' AND mi.aircraft_id = :aid

        UNION ALL

        SELECT l.id, l.ts, l.table_name, l.action, l.row_id, l.import_batch_id, l.actor_user_id, l.actor_username, l.details
        FROM data_ledger l
        JOIN import_batch b ON b.id = l.row_id
        WHERE l.table_name = 'import_batch' AND b.aircraft_id = :aid

        ORDER BY 2 DESC, 1 DESC
        LIMIT :limit OFFSET :offset
        """
        rows = cur.execute(q, {"aid": aircraft_id, "limit": limit, "offset": offset}).fetchall()

        # Intentar parsear details a JSON para comodidad del front
        import json
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["details"] = json.loads(d["details"]) if d["details"] else {}
            except Exception:
                d["details"] = {"raw": d["details"]}
            out.append(d)
        return out

@app.get("/audit")
def get_all_audit(
    limit: int = 50,
    offset: int = 0,
    current_user: Dict[str, Any] = Depends(require_user),
):
    """
    Devuelve eventos del ledger de TODOS los aviones.
    Incluye aircraft_id en la salida para identificar cada evento.
    """
    with connect() as con:
        cur = con.cursor()
        q = """
        SELECT
          l.id, l.ts, l.table_name, l.action, l.row_id, l.import_batch_id,
          COALESCE(mi.aircraft_id, b.aircraft_id,
                   CASE WHEN l.table_name='aircraft' THEN l.row_id END) AS aircraft_id,
          l.actor_user_id, l.actor_username,
          l.details
        FROM data_ledger l
        LEFT JOIN maintenance_item mi
          ON l.table_name='maintenance_item' AND mi.id = l.row_id
        LEFT JOIN import_batch b
          ON l.table_name='import_batch' AND b.id = l.row_id
        ORDER BY 2 DESC, 1 DESC
        LIMIT :limit OFFSET :offset
        """
        rows = cur.execute(q, {"limit": limit, "offset": offset}).fetchall()

        import json
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["details"] = json.loads(d["details"]) if d["details"] else {}
            except Exception:
                d["details"] = {"raw": d["details"]}
            out.append(d)
        return out


@app.post("/aircraft/{aircraft_id}/imports")
async def upload_import(
    aircraft_id: int,
    publish_mode: str = "quarantine",
    file: UploadFile = File(...),
    current_user: Dict[str, Any] = Depends(require_role("admin")),
):
    content = await file.read()
    with connect() as con:
        # verificar que el avión existe
        r = con.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        try:
            result = import_csv_bytes(con, aircraft_id, file.filename, content, publish_mode=publish_mode)
            # Registrar en ledger el resultado del import
            try:
                batch_id = int(result.get("import_batch_id")) if isinstance(result, dict) else None
            except Exception:
                batch_id = None
            det = {
                "action": "UPDATE",
                "table": "import_batch",
                "file_name": file.filename,
                "publish_mode": publish_mode,
                "status": result.get("status") if isinstance(result, dict) else None,
                "inserted_rows": result.get("inserted_rows") if isinstance(result, dict) else None,
                "errors": result.get("errors") if isinstance(result, dict) else None,
                "quarantined": result.get("quarantined") if isinstance(result, dict) else None,
            }
            log_ledger(con.cursor(), "import_batch", "UPDATE", batch_id, batch_id, det, current_user)
            con.commit()
            return result
        except sqlite3.IntegrityError as e:
            con.rollback()
            # mismo archivo importado antes para ese avión
            if "UNIQUE(file_sha256, aircraft_id)" in str(e):
                raise HTTPException(status_code=409, detail="Ese archivo ya fue importado para este avión.")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            con.rollback()
            raise HTTPException(status_code=400, detail=str(e))



# ===================== MTRs API =====================

def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

def _ensure_today_or_past(d: Optional[date], field: str):
    if not d:
        raise HTTPException(status_code=400, detail=f"{field} es requerido en formato YYYY-MM-DD")
    if d > date.today():
        raise HTTPException(status_code=400, detail=f"{field} no puede ser futuro")

def _validate_tc(v: Any, allow_float: bool) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        if allow_float:
            x = float(v)
            if x < 0:
                raise ValueError
            return x
        else:
            x = int(v)
            if x < 0:
                raise ValueError
            return x
    except Exception:
        raise HTTPException(status_code=400, detail="Valores de TIME & CYCLES inválidos")

def _validate_city(code: str) -> str:
    code = (code or "").strip().upper()
    if not code or len(code) not in (3,4) or not code.isalpha():
        raise HTTPException(status_code=400, detail="Ciudad debe ser código IATA/ICAO (3–4 letras)")
    return code

def _get_mtr(cur, mtr_id: int):
    m = cur.execute("SELECT * FROM mtr WHERE id=?", (mtr_id,)).fetchone()
    if not m:
        raise HTTPException(status_code=404, detail="MTR no encontrado")
    return m

@app.get("/mtrs")
def list_mtrs(limit: int = 50, offset: int = 0, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        rows = con.execute(
            "SELECT * FROM v_mtr_list ORDER BY id DESC LIMIT :limit OFFSET :offset",
            {"limit": limit, "offset": offset},
        ).fetchall()
        return [dict(r) for r in rows]

@app.get("/aircraft/{aircraft_id}/time-cycles")
def get_aircraft_time_cycles(aircraft_id: int, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        cur = con.cursor()
        a = cur.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        r = cur.execute(
            "SELECT aircraft_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles, engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles, updated_at FROM aircraft_time_cycles WHERE aircraft_id=?",
            (aircraft_id,)
        ).fetchone()
        return dict(r) if r else None

@app.put("/aircraft/{aircraft_id}/time-cycles")
def set_aircraft_time_cycles(
    aircraft_id: int,
    payload: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(require_role("admin")),
):
    def _nonneg(name: str, val: Any, allow_float: bool):
        if val is None or val == "":
            return None
        try:
            x = float(val) if allow_float else int(val)
            if x < 0:
                raise ValueError
            return x
        except Exception:
            raise HTTPException(status_code=400, detail=f"{name} inválido (no negativo)")

    with connect() as con:
        cur = con.cursor()
        a = cur.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        vals = {
            "aircraft_hours": _nonneg("aircraft_hours", payload.get("aircraft_hours"), True),
            "aircraft_landings": _nonneg("aircraft_landings", payload.get("aircraft_landings"), False),
            "apu_hours": _nonneg("apu_hours", payload.get("apu_hours"), True),
            "apu_cycles": _nonneg("apu_cycles", payload.get("apu_cycles"), False),
            "engine_1_hours": _nonneg("engine_1_hours", payload.get("engine_1_hours"), True),
            "engine_1_cycles": _nonneg("engine_1_cycles", payload.get("engine_1_cycles"), False),
            "engine_2_hours": _nonneg("engine_2_hours", payload.get("engine_2_hours"), True),
            "engine_2_cycles": _nonneg("engine_2_cycles", payload.get("engine_2_cycles"), False),
        }
        cur.execute(
            """
            INSERT INTO aircraft_time_cycles(
              aircraft_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles,
              engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?, datetime('now'))
            ON CONFLICT(aircraft_id) DO UPDATE SET
              aircraft_hours=excluded.aircraft_hours,
              aircraft_landings=excluded.aircraft_landings,
              apu_hours=excluded.apu_hours,
              apu_cycles=excluded.apu_cycles,
              engine_1_hours=excluded.engine_1_hours,
              engine_1_cycles=excluded.engine_1_cycles,
              engine_2_hours=excluded.engine_2_hours,
              engine_2_cycles=excluded.engine_2_cycles,
              updated_at=datetime('now')
            """,
            (aircraft_id, vals["aircraft_hours"], vals["aircraft_landings"], vals["apu_hours"], vals["apu_cycles"],
             vals["engine_1_hours"], vals["engine_1_cycles"], vals["engine_2_hours"], vals["engine_2_cycles"]),
        )
        # Ledger opcional
        log_ledger(cur, "aircraft", "UPDATE", aircraft_id, None, {"action":"SET_TIME_CYCLES","values": vals}, current_user)
        con.commit()
        return {"ok": True}

@app.post("/mtrs")
def create_mtr(payload: Dict[str, Any], current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    aid = payload.get("aircraft_id")
    wcd = _parse_iso_date(payload.get("work_complete_date"))
    _ensure_today_or_past(wcd, "work_complete_date")
    serial = (payload.get("aircraft_serial_no") or "").strip()
    reg = (payload.get("aircraft_reg_no") or "").strip()
    city = _validate_city(payload.get("work_complete_city") or "")
    if not (aid and serial and reg):
        raise HTTPException(status_code=400, detail="Campos requeridos: aircraft_id, aircraft_serial_no, aircraft_reg_no")
    tc = payload.get("time_cycles") or {}
    tc_norm = {
        "aircraft_hours": _validate_tc(tc.get("aircraft_hours"), True),
        "aircraft_landings": _validate_tc(tc.get("aircraft_landings"), False),
        "apu_hours": _validate_tc(tc.get("apu_hours"), True),
        "apu_cycles": _validate_tc(tc.get("apu_cycles"), False),
        "engine_1_hours": _validate_tc(tc.get("engine_1_hours"), True),
        "engine_1_cycles": _validate_tc(tc.get("engine_1_cycles"), False),
        "engine_2_hours": _validate_tc(tc.get("engine_2_hours"), True),
        "engine_2_cycles": _validate_tc(tc.get("engine_2_cycles"), False),
    }
    upd_air_tc = bool(payload.get("update_aircraft_time_cycles"))
    with connect() as con:
        cur = con.cursor()
        a = cur.execute("SELECT id, name, model FROM aircraft WHERE id=?", (aid,)).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        # Crear MTR borrador
        cur.execute(
            """
            INSERT INTO mtr(aircraft_id, status, work_complete_date, aircraft_serial_no, aircraft_reg_no, work_complete_city, created_by)
            VALUES (?, 'borrador', ?, ?, ?, ? , ?)
            """,
            (aid, wcd.isoformat(), serial, reg, city, current_user.get("id")),
        )
        mtr_id = cur.lastrowid
        # Snapshot TC
        cur.execute(
            """
            INSERT INTO mtr_time_cycles_snapshot(
              mtr_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles,
              engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (mtr_id, tc_norm["aircraft_hours"], tc_norm["aircraft_landings"], tc_norm["apu_hours"], tc_norm["apu_cycles"],
             tc_norm["engine_1_hours"], tc_norm["engine_1_cycles"], tc_norm["engine_2_hours"], tc_norm["engine_2_cycles"]),
        )
        # Upsert TC del avión si corresponde
        if upd_air_tc:
            cur.execute(
                """
                INSERT INTO aircraft_time_cycles(
                  aircraft_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles,
                  engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?, datetime('now'))
                ON CONFLICT(aircraft_id) DO UPDATE SET
                  aircraft_hours=excluded.aircraft_hours,
                  aircraft_landings=excluded.aircraft_landings,
                  apu_hours=excluded.apu_hours,
                  apu_cycles=excluded.apu_cycles,
                  engine_1_hours=excluded.engine_1_hours,
                  engine_1_cycles=excluded.engine_1_cycles,
                  engine_2_hours=excluded.engine_2_hours,
                  engine_2_cycles=excluded.engine_2_cycles,
                  updated_at=datetime('now')
                """,
                (aid, tc_norm["aircraft_hours"], tc_norm["aircraft_landings"], tc_norm["apu_hours"], tc_norm["apu_cycles"],
                 tc_norm["engine_1_hours"], tc_norm["engine_1_cycles"], tc_norm["engine_2_hours"], tc_norm["engine_2_cycles"]),
            )
        # Ledger
        det = {
            "action": "CREATE",
            "table": "mtr",
            "mtr_id": mtr_id,
            "values": {
                "aircraft_id": aid,
                "work_complete_date": wcd.isoformat(),
                "aircraft_serial_no": serial,
                "aircraft_reg_no": reg,
                "work_complete_city": city,
                "time_cycles": tc_norm,
                "update_aircraft_time_cycles": upd_air_tc,
            }
        }
        log_ledger(cur, "mtr", "CREATE", mtr_id, None, det, current_user)
        con.commit()
        return {"id": mtr_id}

@app.get("/mtrs/{mtr_id}")
def get_mtr_detail(mtr_id: int, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        items = cur.execute("SELECT * FROM mtr_item WHERE mtr_id=? ORDER BY id", (mtr_id,)).fetchall()
        snap = cur.execute("SELECT * FROM mtr_time_cycles_snapshot WHERE mtr_id=?", (mtr_id,)).fetchone()
        return {"mtr": dict(m), "items": [dict(r) for r in items], "time_cycles": dict(snap) if snap else None}

@app.put("/mtrs/{mtr_id}")
def update_mtr(mtr_id: int, payload: Dict[str, Any], current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede editar un MTR en borrador")
        before = dict(m)
        fields = {}
        if "work_complete_date" in payload:
            d = _parse_iso_date(payload.get("work_complete_date"))
            _ensure_today_or_past(d, "work_complete_date")
            fields["work_complete_date"] = d.isoformat()
        for k in ["aircraft_serial_no","aircraft_reg_no"]:
            if k in payload:
                v = (payload.get(k) or "").strip()
                if not v:
                    raise HTTPException(status_code=400, detail=f"{k} requerido")
                fields[k] = v
        if "work_complete_city" in payload:
            fields["work_complete_city"] = _validate_city(payload.get("work_complete_city"))
        # Repair facility & inspection (opcionales en borrador)
        for k in [
            "repair_facility","facility_certificate","work_order_number","work_performed_by","performer_certificate_number","repair_date",
            "additional_certification_statement","work_inspected_by","inspector_certificate_number","inspection_date"
        ]:
            if k in payload:
                fields[k] = payload.get(k)
        if fields:
            sets = ", ".join([f"{k}=:{k}" for k in fields.keys()])
            cur.execute(f"UPDATE mtr SET {sets}, updated_at=datetime('now') WHERE id=:id", {**fields, "id": mtr_id})
        # Update snapshot TC si viene
        if "time_cycles" in payload:
            tc = payload.get("time_cycles") or {}
            tc_norm = {
                "aircraft_hours": _validate_tc(tc.get("aircraft_hours"), True),
                "aircraft_landings": _validate_tc(tc.get("aircraft_landings"), False),
                "apu_hours": _validate_tc(tc.get("apu_hours"), True),
                "apu_cycles": _validate_tc(tc.get("apu_cycles"), False),
                "engine_1_hours": _validate_tc(tc.get("engine_1_hours"), True),
                "engine_1_cycles": _validate_tc(tc.get("engine_1_cycles"), False),
                "engine_2_hours": _validate_tc(tc.get("engine_2_hours"), True),
                "engine_2_cycles": _validate_tc(tc.get("engine_2_cycles"), False),
            }
            cur.execute(
                """
                UPDATE mtr_time_cycles_snapshot SET
                  aircraft_hours=:aircraft_hours,
                  aircraft_landings=:aircraft_landings,
                  apu_hours=:apu_hours,
                  apu_cycles=:apu_cycles,
                  engine_1_hours=:engine_1_hours,
                  engine_1_cycles=:engine_1_cycles,
                  engine_2_hours=:engine_2_hours,
                  engine_2_cycles=:engine_2_cycles
                WHERE mtr_id=:mtr_id
                """,
                {**tc_norm, "mtr_id": mtr_id},
            )
            if payload.get("update_aircraft_time_cycles"):
                cur.execute(
                    """
                    INSERT INTO aircraft_time_cycles(
                      aircraft_id, aircraft_hours, aircraft_landings, apu_hours, apu_cycles,
                      engine_1_hours, engine_1_cycles, engine_2_hours, engine_2_cycles, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?, datetime('now'))
                    ON CONFLICT(aircraft_id) DO UPDATE SET
                      aircraft_hours=excluded.aircraft_hours,
                      aircraft_landings=excluded.aircraft_landings,
                      apu_hours=excluded.apu_hours,
                      apu_cycles=excluded.apu_cycles,
                      engine_1_hours=excluded.engine_1_hours,
                      engine_1_cycles=excluded.engine_1_cycles,
                      engine_2_hours=excluded.engine_2_hours,
                      engine_2_cycles=excluded.engine_2_cycles,
                      updated_at=datetime('now')
                    """,
                    (m["aircraft_id"], tc_norm["aircraft_hours"], tc_norm["aircraft_landings"], tc_norm["apu_hours"], tc_norm["apu_cycles"],
                     tc_norm["engine_1_hours"], tc_norm["engine_1_cycles"], tc_norm["engine_2_hours"], tc_norm["engine_2_cycles"]),
                )
        after = dict(cur.execute("SELECT * FROM mtr WHERE id=?", (mtr_id,)).fetchone())
        changes = diff_rows(before, after)
        if changes:
            det = {"action":"UPDATE","table":"mtr","mtr_id": mtr_id, "diff": changes}
            log_ledger(cur, "mtr", "UPDATE", mtr_id, None, det, current_user)
        con.commit()
        return {"id": mtr_id, "updated": list(fields.keys())}

@app.get("/mtrs/{mtr_id}/items")
def list_mtr_items(mtr_id: int, current_user: Dict[str, Any] = Depends(require_user)):
    with connect() as con:
        cur = con.cursor()
        _get_mtr(cur, mtr_id)
        rows = cur.execute("SELECT * FROM mtr_item WHERE mtr_id=? ORDER BY id", (mtr_id,)).fetchall()
        return [dict(r) for r in rows]

@app.post("/mtrs/{mtr_id}/items")
def add_mtr_item(mtr_id: int, payload: Dict[str, Any], current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    item_code = (payload.get("item_code") or "").strip()
    description = (payload.get("description") or None)
    maintenance_item_id = payload.get("maintenance_item_id")
    if not item_code:
        raise HTTPException(status_code=400, detail="item_code requerido")
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede editar un MTR en borrador")
        # Validar item_code pertenece al avión
        aid = m["aircraft_id"]
        r = cur.execute(
            "SELECT id FROM v_items_loaded WHERE aircraft_id=? AND item_code=? LIMIT 1",
            (aid, item_code),
        ).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="item_code no pertenece al avión o no está publicado")
        try:
            cur.execute(
                "INSERT INTO mtr_item(mtr_id, item_code, description, maintenance_item_id) VALUES (?,?,?,?)",
                (mtr_id, item_code, description, maintenance_item_id),
            )
        except sqlite3.IntegrityError as e:
            raise HTTPException(status_code=409, detail="Ítem ya agregado al MTR")
        log_ledger(cur, "mtr_item", "INSERT", cur.lastrowid, None, {"mtr_id": mtr_id, "item_code": item_code}, current_user)
        con.commit()
        return {"id": cur.lastrowid}

@app.put("/mtrs/{mtr_id}/items/{item_id}")
def update_mtr_item(mtr_id: int, item_id: int, payload: Dict[str, Any], current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    desc = payload.get("description")
    if desc is None:
        raise HTTPException(status_code=400, detail="description requerido")
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede editar un MTR en borrador")
        r = cur.execute("SELECT * FROM mtr_item WHERE id=? AND mtr_id=?", (item_id, mtr_id)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Ítem no encontrado")
        cur.execute("UPDATE mtr_item SET description=? WHERE id=?", (desc, item_id))
        log_ledger(cur, "mtr_item", "UPDATE", item_id, None, {"mtr_id": mtr_id, "fields": ["description"]}, current_user)
        con.commit()
        return {"id": item_id}

@app.delete("/mtrs/{mtr_id}/items/{item_id}")
def delete_mtr_item(mtr_id: int, item_id: int, current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede editar un MTR en borrador")
        r = cur.execute("SELECT id FROM mtr_item WHERE id=? AND mtr_id=?", (item_id, mtr_id)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Ítem no encontrado")
        cur.execute("DELETE FROM mtr_item WHERE id=?", (item_id,))
        log_ledger(cur, "mtr_item", "UPDATE", item_id, None, {"mtr_id": mtr_id, "action": "DELETE"}, current_user)
        con.commit()
        return {"ok": True}

@app.post("/mtrs/{mtr_id}/confirm-items")
def confirm_mtr_items(mtr_id: int, current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede confirmar en borrador")
        n = cur.execute("SELECT COUNT(*) AS n FROM mtr_item WHERE mtr_id=?", (mtr_id,)).fetchone()["n"]
        if n <= 0:
            raise HTTPException(status_code=400, detail="Debe agregar al menos un ítem")
        cur.execute("UPDATE mtr SET items_confirmed_at=datetime('now') WHERE id=?", (mtr_id,))
        log_ledger(cur, "mtr", "UPDATE", mtr_id, None, {"action": "CONFIRM_ITEMS"}, current_user)
        con.commit()
        return {"ok": True}

@app.post("/mtrs/{mtr_id}/submit")
def submit_mtr(mtr_id: int, current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="MTR ya enviado")
        # Validar snapshots y secciones
        snap = cur.execute("SELECT * FROM mtr_time_cycles_snapshot WHERE mtr_id=?", (mtr_id,)).fetchone()
        if not snap:
            raise HTTPException(status_code=400, detail="Falta snapshot de TIME & CYCLES")
        # Ítems y descripciones
        bad = cur.execute("SELECT COUNT(*) AS n FROM mtr_item WHERE mtr_id=? AND (description IS NULL OR TRIM(description)='')", (mtr_id,)).fetchone()["n"]
        total = cur.execute("SELECT COUNT(*) AS n FROM mtr_item WHERE mtr_id=?", (mtr_id,)).fetchone()["n"]
        if total <= 0 or bad > 0:
            raise HTTPException(status_code=400, detail="Todos los ítems deben tener descripción y existir al menos uno")
        # Campos requeridos finales
        for k in ["work_complete_date", "aircraft_serial_no", "aircraft_reg_no", "work_complete_city"]:
            if not (m[k]):
                raise HTTPException(status_code=400, detail=f"Campo requerido faltante: {k}")
        # Validar fechas finales
        for fld in ["work_complete_date", "repair_date", "inspection_date"]:
            d = _parse_iso_date(m[fld]) if m[fld] else None
            if fld != "repair_date" and not d:
                raise HTTPException(status_code=400, detail=f"{fld} requerido en formato YYYY-MM-DD")
            if d and d > date.today():
                raise HTTPException(status_code=400, detail=f"{fld} no puede ser futuro")
        # REPAIR FACILITY e INSPECTION requeridos para enviar
        for k in [
            "repair_facility","facility_certificate","work_order_number","work_performed_by","performer_certificate_number",
            "work_inspected_by","inspector_certificate_number"
        ]:
            if not (m[k] and str(m[k]).strip()):
                raise HTTPException(status_code=400, detail=f"Campo requerido faltante: {k}")
        cur.execute("UPDATE mtr SET status='enviado', submitted_at=datetime('now') WHERE id=?", (mtr_id,))
        log_ledger(cur, "mtr", "UPDATE", mtr_id, None, {"action": "SUBMIT"}, current_user)
        con.commit()
        return {"id": mtr_id, "status": "enviado"}

@app.delete("/mtrs/{mtr_id}")
def delete_mtr(mtr_id: int, current_user: Dict[str, Any] = Depends(require_role("admin", "mechanic"))):
    with connect() as con:
        cur = con.cursor()
        m = _get_mtr(cur, mtr_id)
        if m["status"] != "borrador":
            raise HTTPException(status_code=400, detail="Solo se puede borrar un MTR en borrador")
        cur.execute("DELETE FROM mtr WHERE id=?", (mtr_id,))
        log_ledger(cur, "mtr", "UPDATE", mtr_id, None, {"action": "DELETE"}, current_user)
        con.commit()
        return {"ok": True}
