import sqlite3, bcrypt, os
from pathlib import Path
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta

DB_PATH = Path("data/tasks.db")


def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def _ensure_schema():
    """Ensure the users table exists in the shared SQLite database.

    Uses `src/init_users.sql` if available; otherwise creates a minimal schema.
    """
    try:
        schema_file = Path(__file__).with_name("init_users.sql")
        sql = None
        if schema_file.exists():
            sql = schema_file.read_text(encoding="utf-8")
        else:
            sql = (
                "CREATE TABLE IF NOT EXISTS users (\n"
                "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                "    email TEXT UNIQUE NOT NULL,\n"
                "    password_hash TEXT NOT NULL,\n"
                "    role TEXT NOT NULL DEFAULT 'viewer',\n"
                "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
                ");"
            )
        with _get_conn() as conn:
            conn.executescript(sql)
            # Also ensure OTP table exists
            conn.execute(
                "CREATE TABLE IF NOT EXISTS user_otps (\n"
                " id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                " email TEXT NOT NULL,\n"
                " purpose TEXT NOT NULL,\n"
                " code TEXT NOT NULL,\n"
                " expires_at INTEGER NOT NULL,\n"
                " consumed INTEGER NOT NULL DEFAULT 0,\n"
                " created_at INTEGER NOT NULL\n"
                ")"
            )
    except Exception:
        # Fail silently to avoid blocking app startup; create minimal table
        with _get_conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS users (\n"
                " id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                " email TEXT UNIQUE NOT NULL,\n"
                " password_hash TEXT NOT NULL,\n"
                " role TEXT NOT NULL DEFAULT 'viewer',\n"
                " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
                ")"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS user_otps (\n"
                " id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                " email TEXT NOT NULL,\n"
                " purpose TEXT NOT NULL,\n"
                " code TEXT NOT NULL,\n"
                " expires_at INTEGER NOT NULL,\n"
                " consumed INTEGER NOT NULL DEFAULT 0,\n"
                " created_at INTEGER NOT NULL\n"
                ")"
            )


# Ensure schema on import
_ensure_schema()


def create_user(email: str, password: str, role: str = "viewer") -> bool:
    """Đăng ký user mới"""
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    with _get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO users (email, password_hash, role) VALUES (?, ?, ?)",
                (email, pw_hash, role),
            )
            return True
        except sqlite3.IntegrityError:
            return False  # email đã tồn tại


def get_user(email: str) -> Optional[Dict]:
    """Lấy thông tin user theo email"""
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, email, password_hash, role FROM users WHERE email=?",
            (email,),
        ).fetchone()
        if row:
            return {
                "id": row[0],
                "email": row[1],
                "password_hash": row[2],
                "role": row[3],
            }
    return None


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode(), password_hash.encode())


def update_password(email: str, new_password: str) -> bool:
    """Update the user's password hash.

    Returns True if the user exists and the password was updated.
    """
    pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    with _get_conn() as conn:
        cur = conn.execute(
            "UPDATE users SET password_hash=? WHERE email=?",
            (pw_hash, email),
        )
        return cur.rowcount > 0


def create_otp(email: str, purpose: str, code: str, ttl_minutes: int = 10) -> None:
    """Create an OTP for a given user/purpose with expiration."""
    expires_at = int((datetime.utcnow() + timedelta(minutes=ttl_minutes)).timestamp())
    created_at = int(datetime.utcnow().timestamp())
    with _get_conn() as conn:
        # Invalidate previous unused OTPs for same purpose
        conn.execute(
            "UPDATE user_otps SET consumed=1 WHERE email=? AND purpose=? AND consumed=0",
            (email, purpose),
        )
        conn.execute(
            "INSERT INTO user_otps (email, purpose, code, expires_at, consumed, created_at)\n"
            "VALUES (?, ?, ?, ?, 0, ?)",
            (email, purpose, code, expires_at, created_at),
        )


def verify_and_consume_otp(email: str, purpose: str, code: str) -> Tuple[bool, str]:
    """Verify OTP and consume it if valid. Returns (ok, reason)."""
    now_ts = int(datetime.utcnow().timestamp())
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT id, expires_at, consumed FROM user_otps WHERE email=? AND purpose=? AND code=? ORDER BY id DESC LIMIT 1",
            (email, purpose, code),
        ).fetchone()
        if not row:
            return False, "not_found"
        _id, expires_at, consumed = row
        if consumed:
            return False, "consumed"
        if expires_at < now_ts:
            return False, "expired"
        conn.execute("UPDATE user_otps SET consumed=1 WHERE id=?", (_id,))
        return True, "ok"
