# worker/tasks.py
from __future__ import annotations

import os
import uuid
import time
import traceback
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
import urllib.request
import urllib.error

from sqlalchemy import text as sql_text

from worker.celery_app import celery_app
from api.db import SessionLocal
from api.models import Run


def _utcnow():
    return datetime.now(timezone.utc)


def _safe_uuid(v: str):
    try:
        return uuid.UUID(str(v))
    except Exception:
        return None


def _write_text(path: str | None, content: str):
    if not path:
        return
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    except Exception:
        pass


def _run_solver(case_path: str, out_dir: str) -> tuple[int, str]:
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    candidates = [
    ["python", "-u", "/app/solver_core/solve.py", "--case", case_path, "--out", out_dir],
    ["python", "-u", "/app/solver_core/solve.py", "--input", case_path, "--output", out_dir],
    ["python", "-u", "/app/solver_core/solve.py", "--case_path", case_path, "--out_dir", out_dir],
    ["python", "-u", "/app/solver_core/solve.py", case_path],
    ]

    last = None
    for cmd in candidates:
        try:
            cp = subprocess.run(
                cmd,
                cwd=str(out_dir_path),
                capture_output=True,
                text=True,
            )
            combined = (cp.stdout or "") + "\n" + (cp.stderr or "")
            last = (cp.returncode, combined)

            if cp.returncode == 0:
                return cp.returncode, combined

            if "unrecognized arguments" in combined.lower() or "usage:" in combined.lower():
                continue

            continue
        except FileNotFoundError as e:
            last = (127, f"FileNotFoundError: {e}")
            continue
        except Exception as e:
            last = (1, f"Exception: {e}\n{traceback.format_exc()}")
            continue

    if last is None:
        return 1, "No se pudo ejecutar solver: no hubo candidatos ejecutables."
    return last


@celery_app.task(name="execute_run")
def execute_run(run_id: str):
    db = SessionLocal()
    try:
        rid = _safe_uuid(run_id)
        if not rid:
            return

        run = db.get(Run, rid)
        if not run:
            return

        run.status = "running"
        run.started_at = _utcnow()
        run.finished_at = None
        run.error_message = None
        db.commit()

        case_path = str(run.case_path)
        out_dir = str(run.out_dir)

        if not case_path or not Path(case_path).exists():
            msg = f"case.xlsx no existe en path: {case_path}"
            run.status = "failed"
            run.finished_at = _utcnow()
            run.error_message = msg
            db.commit()
            _write_text(run.log_path, msg)
            return

        rc, log_txt = _run_solver(case_path=case_path, out_dir=out_dir)
        _write_text(run.log_path, log_txt)

        if rc == 0:
            run.status = "success"
            run.finished_at = _utcnow()
            db.commit()
        else:
            run.status = "failed"
            run.finished_at = _utcnow()
            run.error_message = f"Solver retornó rc={rc}"
            db.commit()

    except Exception as e:
        db.rollback()
        err = f"{e}\n{traceback.format_exc()}"
        try:
            rid = _safe_uuid(run_id)
            if rid:
                run = db.get(Run, rid)
                if run:
                    run.status = "failed"
                    run.finished_at = _utcnow()
                    run.error_message = str(e)
                    db.commit()
                    _write_text(run.log_path, err)
        except Exception:
            pass
    finally:
        db.close()


@celery_app.task(name="request_turnos")
def request_turnos(*args, **kwargs):
    # compat
    return


@celery_app.task(name="publish.send_employee_pdf")
def publish_send_employee_pdf(publication_id: str, employee_key: str):
    db = SessionLocal()
    try:
        pid = _safe_uuid(publication_id)
        if not pid:
            return

        pub = db.execute(
            sql_text(
                """
                SELECT id, company_id, run_id, month, org_unit_id, mode, test_email
                FROM run_publications
                WHERE id=:id
                """
            ),
            {"id": str(pid)},
        ).mappings().first()

        if not pub:
            return

        rec = db.execute(
            sql_text(
                """
                SELECT employee_key, email, status
                FROM run_publication_recipients
                WHERE publication_id=:pid AND employee_key=:ek
                """
            ),
            {"pid": str(pid), "ek": employee_key},
        ).mappings().first()

        if not rec:
            return

        to_email = (rec.get("email") or "").strip()
        if not to_email:
            db.execute(
                sql_text(
                    """
                    UPDATE run_publication_recipients
                    SET status='no_email', last_error='missing email'
                    WHERE publication_id=:pid AND employee_key=:ek
                    """
                ),
                {"pid": str(pid), "ek": employee_key},
            )
            db.commit()
            return

        # ===== 1) PDF desde API interna (con token interno) =====
        base = os.getenv("API_INTERNAL_BASE_URL", "http://api:8000")
        token = os.getenv("INTERNAL_API_TOKEN", "")

        params = {
            "company_id": str(pub["company_id"]),
            "run_id": str(pub["run_id"]),
            "month": str(pub["month"]),
            "employee_id": str(employee_key),
        }
        url = f"{base}/ui/exports/employee.pdf?{urlencode(params)}"

        headers = {"User-Agent": "vigatec-worker/0.1"}
        if token:
            headers["X-Internal-Token"] = token

        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=60) as resp:
                pdf_bytes = resp.read()
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            raise RuntimeError(f"PDF HTTPError {e.code}: {body[:500]}") from e

        if not pdf_bytes or len(pdf_bytes) < 500:
            raise RuntimeError("PDF generado vacío o demasiado pequeño.")

        # ===== 2) Enviar email por Resend =====
        from worker.resend_client import send_email_resend, ResendAttachment

        from_email = os.getenv("RESEND_FROM", "no-reply@dotaciones.cl").strip().strip("<>")
        subject = f"Turnos {pub['month']} — Publicación"
        html = (
            f"<p>Adjuntamos tu calendario de turnos del mes <b>{pub['month']}</b>.</p>"
            f"<p>Saludos,<br/>Vigatec</p>"
        )

        att = ResendAttachment(
            filename=f"turnos_{pub['month']}_{employee_key}.pdf",
            content_type="application/pdf",
            content_bytes=pdf_bytes,
        )

        time.sleep(0.6)  # Resend rate limit: max 2 req/s
        send_email_resend(
            from_email=from_email,
            to_emails=[to_email],
            subject=subject,
            html=html,
            attachments=[att],
        )

        # ===== 3) Marcar enviado =====
        db.execute(
            sql_text(
                """
                UPDATE run_publication_recipients
                SET status='sent', sent_at=NOW(), last_error=NULL
                WHERE publication_id=:pid AND employee_key=:ek
                """
            ),
            {"pid": str(pid), "ek": employee_key},
        )
        db.commit()

    except Exception as e:
        db.rollback()
        err = f"{e}\n{traceback.format_exc()}"
        try:
            pid = _safe_uuid(publication_id)
            if pid:
                db.execute(
                    sql_text(
                        """
                        UPDATE run_publication_recipients
                        SET status='failed', last_error=:err
                        WHERE publication_id=:pid AND employee_key=:ek
                        """
                    ),
                    {"pid": str(pid), "ek": employee_key, "err": err[:4000]},
                )
                db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()


# ============================================================
# NOTIFICACIÓN DE CAMBIO DE TURNO CON ACEPTACIÓN/RECHAZO
# ============================================================

def _send_email(to_email: str, subject: str, html: str):
    """Helper para enviar email via Resend."""
    from worker.resend_client import send_email_resend
    from_email = os.getenv("RESEND_FROM", "no-reply@dotaciones.cl").strip().strip("<>")
    time.sleep(0.6)  # Resend rate limit
    send_email_resend(from_email=from_email, to_emails=[to_email], subject=subject, html=html)


@celery_app.task(name="notify_override_change")
def notify_override_change(
    override_id: str,
    run_id: str,
    company_id: str,
    employee_id: str,
    fecha: str,
    shift_id_old: str,
    shift_id_new: str,
    employee_email: str,
    employee_nombre: str,
    supervisor_email: str = "",
    hours: int = 24,
):
    """Envía email al colaborador con botones Aceptar/Rechazar y crea registro en BD."""
    db = SessionLocal()
    try:
        if not employee_email or not employee_email.strip():
            print(f"[notify_override_change] Sin email para {employee_id}, saltando.")
            return

        # Generar token único para respuesta
        token = str(uuid.uuid4())
        fecha_limite = datetime.now(timezone.utc) + __import__('datetime').timedelta(hours=hours)

        # Insertar registro de respuesta pendiente
        db.execute(
            sql_text("""
                INSERT INTO plan_override_responses 
                (token, override_id, run_id, company_id, employee_id, fecha, 
                 shift_id_old, shift_id_new, estado, fecha_limite, created_at)
                VALUES (:token, :oid, :rid, :cid, :eid, :fecha, 
                        :old, :new, 'pending', :limite, NOW())
                ON CONFLICT (override_id) DO UPDATE SET
                    token = EXCLUDED.token,
                    estado = 'pending',
                    fecha_limite = EXCLUDED.fecha_limite,
                    fecha_respuesta = NULL,
                    notificado_supervisor = FALSE
            """),
            {
                "token": token,
                "oid": override_id,
                "rid": run_id,
                "cid": company_id,
                "eid": employee_id,
                "fecha": fecha,
                "old": shift_id_old,
                "new": shift_id_new,
                "limite": fecha_limite,
            },
        )
        db.commit()

        # Construir URLs de respuesta
        base_url = os.getenv("APP_BASE_URL", "https://turnos.vigatec.cl")
        accept_url = f"{base_url}/api/override-response?token={token}&action=accept"
        reject_url = f"{base_url}/api/override-response?token={token}&action=reject"

        # Email HTML
        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#1e293b;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="color:#f8fafc;margin:0;">📅 Cambio de turno programado</h2>
          </div>
          <div style="background:#fff;padding:24px;border:1px solid #e2e8f0;border-top:none;">
            <p>Hola <b>{employee_nombre}</b>,</p>
            <p>Se ha programado un cambio en tu turno del día <b>{fecha}</b>:</p>
            <table style="width:100%;border-collapse:collapse;margin:16px 0;">
              <tr>
                <td style="padding:10px;background:#fef2f2;border:1px solid #e2e8f0;font-weight:600;">Turno anterior</td>
                <td style="padding:10px;border:1px solid #e2e8f0;font-family:monospace;text-decoration:line-through;color:#991b1b;">{shift_id_old}</td>
              </tr>
              <tr>
                <td style="padding:10px;background:#f0fdf4;border:1px solid #e2e8f0;font-weight:600;">Turno nuevo</td>
                <td style="padding:10px;border:1px solid #e2e8f0;font-family:monospace;color:#166534;"><b>{shift_id_new}</b></td>
              </tr>
            </table>
            <p>Tienes <b>{hours} horas</b> para confirmar o rechazar este cambio.</p>
            <p style="color:#64748b;font-size:13px;">Si no respondes, el cambio se aceptará automáticamente.</p>
            <div style="text-align:center;margin:28px 0;">
              <a href="{accept_url}" style="display:inline-block;background:#22c55e;color:#fff;padding:12px 32px;border-radius:8px;text-decoration:none;font-weight:700;margin-right:12px;">✅ Aceptar</a>
              <a href="{reject_url}" style="display:inline-block;background:#ef4444;color:#fff;padding:12px 32px;border-radius:8px;text-decoration:none;font-weight:700;">❌ Rechazar</a>
            </div>
            <p style="color:#94a3b8;font-size:11px;text-align:center;">Vigatec · Sistema de Turnos</p>
          </div>
        </div>
        """

        _send_email(employee_email, f"Cambio de turno {fecha} — Por confirmar", html)
        print(f"[notify_override_change] Email enviado a {employee_email} para {employee_id} fecha {fecha}")

    except Exception as e:
        db.rollback()
        print(f"[notify_override_change] Error: {e}\n{traceback.format_exc()}")
    finally:
        db.close()


@celery_app.task(name="notify_supervisor_override_response")
def notify_supervisor_override_response(token: str):
    """Notifica al supervisor cuando un colaborador acepta o rechaza un cambio."""
    db = SessionLocal()
    try:
        row = db.execute(
            sql_text("SELECT * FROM plan_override_responses WHERE token=:t"),
            {"t": token},
        ).mappings().first()

        if not row or row.get("notificado_supervisor"):
            return

        estado = row["estado"]
        employee_id = row["employee_id"]
        fecha = row["fecha"]
        shift_old = row["shift_id_old"]
        shift_new = row["shift_id_new"]

        # Buscar email del supervisor (primer user activo de la empresa)
        sup = db.execute(
            sql_text("""
                SELECT u.email, u.full_name FROM users u
                JOIN user_scopes us ON us.user_id = u.id
                WHERE us.company_id = :cid AND u.is_active = TRUE AND u.email IS NOT NULL
                ORDER BY u.created_at ASC LIMIT 1
            """),
            {"cid": row["company_id"]},
        ).mappings().first()

        if not sup or not sup.get("email"):
            return

        # Buscar nombre del colaborador
        emp = db.execute(
            sql_text("SELECT nombre FROM employees WHERE employee_key=:ek LIMIT 1"),
            {"ek": employee_id},
        ).mappings().first()
        emp_nombre = emp["nombre"] if emp else employee_id

        emoji = "✅" if estado == "accepted" else "❌"
        accion = "aceptó" if estado == "accepted" else "rechazó"
        color = "#22c55e" if estado == "accepted" else "#ef4444"

        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
          <div style="background:#1e293b;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="color:#f8fafc;margin:0;">{emoji} Respuesta a cambio de turno</h2>
          </div>
          <div style="background:#fff;padding:24px;border:1px solid #e2e8f0;border-top:none;">
            <p>Hola <b>{sup["full_name"]}</b>,</p>
            <p><b>{emp_nombre}</b> <span style="color:{color};font-weight:700;">{accion}</span> el cambio de turno del día <b>{fecha}</b>.</p>
            <table style="width:100%;border-collapse:collapse;margin:16px 0;">
              <tr>
                <td style="padding:10px;background:#f8fafc;border:1px solid #e2e8f0;font-weight:600;">Turno anterior</td>
                <td style="padding:10px;border:1px solid #e2e8f0;font-family:monospace;">{shift_old}</td>
              </tr>
              <tr>
                <td style="padding:10px;background:#f8fafc;border:1px solid #e2e8f0;font-weight:600;">Turno nuevo</td>
                <td style="padding:10px;border:1px solid #e2e8f0;font-family:monospace;color:#1d4ed8;"><b>{shift_new}</b></td>
              </tr>
            </table>
            <p style="color:#94a3b8;font-size:11px;text-align:center;">Vigatec · Sistema de Turnos</p>
          </div>
        </div>
        """

        _send_email(sup["email"], f"{emoji} {emp_nombre} {accion} cambio de turno {fecha}", html)

        db.execute(
            sql_text("UPDATE plan_override_responses SET notificado_supervisor=TRUE WHERE token=:t"),
            {"t": token},
        )
        db.commit()

    except Exception as e:
        db.rollback()
        print(f"[notify_supervisor_override_response] Error: {e}\n{traceback.format_exc()}")
    finally:
        db.close()


@celery_app.task(name="auto_accept_expired_overrides")
def auto_accept_expired_overrides():
    """Marca como aceptados los overrides sin respuesta que ya vencieron. Ejecutar cada hora."""
    db = SessionLocal()
    try:
        rows = db.execute(
            sql_text("""
                UPDATE plan_override_responses
                SET estado='accepted', fecha_respuesta=NOW()
                WHERE estado='pending' AND fecha_limite < NOW()
                RETURNING token
            """),
        ).fetchall()
        db.commit()

        for (token,) in rows:
            notify_supervisor_override_response.delay(token)

        print(f"[auto_accept] {len(rows)} overrides expirados marcados como aceptados.")

    except Exception as e:
        db.rollback()
        print(f"[auto_accept_expired_overrides] Error: {e}\n{traceback.format_exc()}")
    finally:
        db.close()