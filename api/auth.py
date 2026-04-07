from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Optional

import bcrypt

_SECRET = os.getenv("SECRET_KEY", "vigatec_dev_secret_changeme")


def hash_password(plain: str) -> str:
    """Genera el hash bcrypt de una contraseña en texto plano."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Verifica una contraseña contra su hash bcrypt."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


def create_session_token(user_id: str) -> str:
    """
    Crea un token de sesión firmado con HMAC-SHA256.
    Formato: user_id|timestamp|firma
    """
    ts = str(int(time.time()))
    payload = f"{user_id}|{ts}"
    sig = hmac.new(_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}|{sig}"


def verify_session_token(token: str) -> Optional[str]:
    """
    Verifica el token de sesión.
    Retorna el user_id si es válido y no ha expirado (8 horas).
    Retorna None si el token es inválido o expiró.
    """
    try:
        parts = token.split("|")
        if len(parts) != 3:
            return None

        user_id, ts, sig = parts
        payload = f"{user_id}|{ts}"
        expected = hmac.new(_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(sig, expected):
            return None

        # Expira a las 8 horas
        if int(time.time()) - int(ts) > 28800:
            return None

        return user_id
    except Exception:
        return None
