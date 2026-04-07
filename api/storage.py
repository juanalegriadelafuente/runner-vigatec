from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID
from datetime import datetime, timezone

from api.config import settings


@dataclass(frozen=True)
class RunPaths:
    run_dir: Path
    input_dir: Path
    out_dir: Path
    logs_dir: Path
    case_path: Path
    log_path: Path


def get_run_paths(run_id: UUID) -> RunPaths:
    base = Path(settings.STORAGE_PATH).resolve()
    run_dir = (base / "runs" / str(run_id)).resolve()
    input_dir = run_dir / "input"
    out_dir = run_dir / "out"
    logs_dir = run_dir / "logs"
    case_path = input_dir / "case.xlsx"
    log_path = logs_dir / "solver.log"
    return RunPaths(run_dir, input_dir, out_dir, logs_dir, case_path, log_path)


def ensure_dirs(paths: RunPaths) -> None:
    paths.input_dir.mkdir(parents=True, exist_ok=True)
    paths.out_dir.mkdir(parents=True, exist_ok=True)
    paths.logs_dir.mkdir(parents=True, exist_ok=True)


def safe_resolve_under(base: Path, candidate: Path) -> Path:
    base_resolved = base.resolve()
    cand_resolved = candidate.resolve()
    if base_resolved not in cand_resolved.parents and base_resolved != cand_resolved:
        raise ValueError("Invalid artifact path.")
    return cand_resolved


def file_mtime_utc(path: Path) -> datetime:
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc)
