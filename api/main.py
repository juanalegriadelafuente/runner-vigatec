from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from api.db import get_db, init_db_with_retry
from api.models import Run
from api.schemas import ArtifactItem, ArtifactListResponse, RunCreateResponse, RunStatusResponse
from api.storage import ensure_dirs, file_mtime_utc, get_run_paths, safe_resolve_under

from api.qa import load_qa, qa_message, qa_status, qa_summary


# Import routers (UI)
from api.ui import router as ui_router

# Celery task
from worker.tasks import execute_run

# Ensure SQLAlchemy models are imported (tables created)
import api.masterdata_models  # noqa: F401
import api.demand_models  # noqa: F401
import api.vocab_models  # noqa: F401
import api.case_data_models  # noqa: F401
import api.rbac_models  # noqa: F401
import api.holiday_models  # noqa: F401


app = FastAPI(
    title="Vigatec Runner SaaS (Fase 0)",
    version="0.1.0",
    description="Runner que recibe case.xlsx, ejecuta solver_core como caja negra y expone artefactos.",
)

# ✅ STATIC (CSS)
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)

# ✅ UI router
app.include_router(ui_router)


@app.on_event("startup")
def on_startup():
    init_db_with_retry()


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.post("/runs", response_model=RunCreateResponse)
async def create_run(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Expected an .xlsx file (case.xlsx)")

    run_id = uuid.uuid4()
    paths = get_run_paths(run_id)
    ensure_dirs(paths)

    contents = await file.read()
    paths.case_path.write_bytes(contents)

    run = Run(
        id=run_id,
        status="queued",
        original_filename=file.filename,
        case_path=str(paths.case_path),
        out_dir=str(paths.out_dir),
        log_path=str(paths.log_path),
    )
    db.add(run)
    db.commit()

    execute_run.delay(str(run_id))
    return RunCreateResponse(id=run_id, status="queued")


@app.get("/runs/{run_id}", response_model=RunStatusResponse)
def get_run(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run: Run | None = db.get(Run, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    duration = None
    if run.started_at and run.finished_at:
        duration = (run.finished_at - run.started_at).total_seconds()

    qa = load_qa(str(run.id))

    return RunStatusResponse(
        id=run.id,
        status=run.status,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        original_filename=run.original_filename,
        duration_seconds=duration,
        error_message=run.error_message,
        qa_status=qa_status(qa),
        qa_message=qa_message(qa),
        qa_summary=qa_summary(qa),
    )


@app.get("/runs/{run_id}/qa")
def get_run_qa(run_id: uuid.UUID):
    qa = load_qa(str(run_id))
    if qa is None:
        raise HTTPException(status_code=404, detail="qa_plan.json not found for this run yet")
    return qa


@app.get("/runs/{run_id}/artifacts", response_model=ArtifactListResponse)
def list_artifacts(run_id: uuid.UUID, db: Session = Depends(get_db)):
    run: Run | None = db.get(Run, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    out_dir = Path(run.out_dir)
    log_path = Path(run.log_path)

    artifacts: list[ArtifactItem] = []

    if out_dir.exists():
        for p in sorted(out_dir.glob("**/*")):
            if p.is_file():
                rel = p.relative_to(out_dir).as_posix()
                artifacts.append(
                    ArtifactItem(
                        name=rel,
                        size_bytes=p.stat().st_size,
                        modified_at=file_mtime_utc(p).isoformat(),
                    )
                )

    if log_path.exists() and log_path.is_file():
        artifacts.append(
            ArtifactItem(
                name="logs/solver.log",
                size_bytes=log_path.stat().st_size,
                modified_at=file_mtime_utc(log_path).isoformat(),
            )
        )

    return ArtifactListResponse(id=run.id, artifacts=artifacts)


@app.get("/runs/{run_id}/artifacts/{name:path}")
def download_artifact(run_id: uuid.UUID, name: str, db: Session = Depends(get_db)):
    run: Run | None = db.get(Run, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    out_dir = Path(run.out_dir).resolve()
    run_base = out_dir.parent.resolve()
    logs_dir = (run_base / "logs").resolve()

    if name.startswith("logs/"):
        candidate = (logs_dir / name.replace("logs/", "", 1))
        try:
            path = safe_resolve_under(logs_dir, candidate)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid artifact path")
    else:
        candidate = out_dir / name
        try:
            path = safe_resolve_under(out_dir, candidate)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid artifact path")

    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact not found")

    return FileResponse(str(path), filename=path.name, media_type="application/octet-stream")