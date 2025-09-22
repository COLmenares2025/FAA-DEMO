import os
import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable

from fastapi import HTTPException, Header, Depends, Cookie

from .db import connect

SESSION_DURATION_SECONDS = int(os.getenv("SESSION_DURATION_SECONDS", "28800"))

ROLE_LABELS = {"admin", "mechanic", "auditor"}


def hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def verify_password(password: str, salt: str, password_hash: str) -> bool:
    return hash_password(password, salt) == password_hash


def create_user(cur, username: str, password: str, role: str) -> None:
    username = username.strip().lower()
    if role not in ROLE_LABELS:
        raise ValueError("Rol inválido")
    salt = secrets.token_hex(16)
    pwd_hash = hash_password(password, salt)
    cur.execute(
        "INSERT INTO users(username, password_hash, password_salt, role) VALUES (?,?,?,?)",
        (username, pwd_hash, salt, role),
    )


def ensure_default_users(cur) -> None:
    count = cur.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    if count:
        return
    defaults = [
        ("admin", os.getenv("ADMIN_DEFAULT_PASSWORD", "admin123"), "admin"),
        ("mechanic", os.getenv("MECHANIC_DEFAULT_PASSWORD", "mechanic123"), "mechanic"),
        ("auditor", os.getenv("AUDITOR_DEFAULT_PASSWORD", "auditor123"), "auditor"),
    ]
    for username, password, role in defaults:
        create_user(cur, username, password, role)


def create_session(cur, user_id: int) -> Dict[str, str]:
    token = secrets.token_hex(32)
    expires_at = (datetime.utcnow() + timedelta(seconds=SESSION_DURATION_SECONDS)).isoformat()
    cur.execute(
        "INSERT INTO user_session(token, user_id, expires_at) VALUES (?,?,?)",
        (token, user_id, expires_at),
    )
    return {"token": token, "expires_at": expires_at}


def delete_session(cur, token: str) -> None:
    cur.execute("DELETE FROM user_session WHERE token=?", (token,))


def _extract_token(authorization: Optional[str], cookie_token: Optional[str] = None) -> str:
    if not authorization:
        # Usar cookie de sesión si está presente
        if cookie_token:
            return cookie_token
        raise HTTPException(status_code=401, detail="Token requerido")
    prefix = "Bearer "
    if not authorization.startswith(prefix):
        raise HTTPException(status_code=401, detail="Formato de token inválido")
    token = authorization[len(prefix):].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token requerido")
    return token


def get_current_user(
    authorization: Optional[str] = Header(None),
    session: Optional[str] = Cookie(default=None),
) -> Dict[str, str]:
    token = _extract_token(authorization, session)
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT s.token, s.expires_at, u.id AS user_id, u.username, u.role
            FROM user_session s
            JOIN users u ON u.id = s.user_id
            WHERE s.token=?
            """,
            (token,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Sesión no válida")
        try:
            expires_at = datetime.fromisoformat(row["expires_at"])
        except Exception:
            expires_at = datetime.utcnow() - timedelta(seconds=1)
        if expires_at < datetime.utcnow():
            cur.execute("DELETE FROM user_session WHERE token=?", (token,))
            con.commit()
            raise HTTPException(status_code=401, detail="Sesión expirada")
        return {
            "id": row["user_id"],
            "username": row["username"],
            "role": row["role"],
            "session_token": row["token"],
            "session_expires_at": row["expires_at"],
        }


def require_user(user=Depends(get_current_user)) -> Dict[str, str]:
    return user


def require_role(*roles: str) -> Callable:
    allowed = {r.lower() for r in roles if r}

    def dependency(user=Depends(get_current_user)) -> Dict[str, str]:
        if allowed and user["role"].lower() not in allowed:
            raise HTTPException(status_code=403, detail="No tienes permisos para esta operación")
        return user

    return dependency
