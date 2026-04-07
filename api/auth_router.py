from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from api.auth import create_session_token, verify_password
from api.db import get_db
from api.rbac_models import User

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
router = APIRouter(tags=["auth"])


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/ui/home"):
    """Renderiza la página de login."""
    return TEMPLATES.TemplateResponse("login.html", {
        "request": request,
        "error": "",
        "next": next,
    })


@router.post("/login")
def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form(default="/ui/home"),
    db: Session = Depends(get_db),
):
    """Verifica credenciales y crea la sesión."""
    user: User | None = db.query(User).filter(
        User.email == email.strip().lower(),
        User.is_active == True,
    ).first()

    # Credenciales incorrectas o usuario sin contraseña configurada
    if not user or not user.password_hash or not verify_password(password, user.password_hash):
        return TEMPLATES.TemplateResponse("login.html", {
            "request": request,
            "error": "Email o contraseña incorrectos.",
            "next": next,
        }, status_code=401)

    # Crear token de sesión firmado
    token = create_session_token(str(user.id))

    # Redirigir al destino y setear cookies
    redirect_url = next if next.startswith("/") else "/ui/home"
    resp = RedirectResponse(url=redirect_url, status_code=303)

    # session_token: httponly, no accesible desde JS (seguridad)
    resp.set_cookie(
        "session_token",
        token,
        httponly=True,
        samesite="lax",
        max_age=28800,  # 8 horas
    )
    # ctx_user_id: necesario para el sistema RBAC existente
    resp.set_cookie(
        "ctx_user_id",
        str(user.id),
        httponly=False,
        samesite="lax",
        max_age=28800,
    )
    return resp


@router.get("/logout")
@router.post("/logout")
def logout():
    """Cierra la sesión y limpia todas las cookies."""
    resp = RedirectResponse(url="/login", status_code=303)
    resp.delete_cookie("session_token")
    resp.delete_cookie("ctx_user_id")
    resp.delete_cookie("ctx_company_id")
    resp.delete_cookie("ctx_branch_id")
    resp.delete_cookie("ctx_org_unit_id")
    return resp
