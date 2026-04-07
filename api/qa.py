from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _storage_root() -> Path:
    """
    Same base used by API/worker (mounted as ./storage:/app/storage in docker-compose).
    """
    return Path(os.getenv("STORAGE_PATH", "/app/storage"))


def _qa_path(run_id: str) -> Path:
    """
    We store QA under:
      storage/runs/<run_id>/out/qa_plan.json
    """
    return _storage_root() / "runs" / run_id / "out" / "qa_plan.json"


def load_qa(run_id: str) -> dict[str, Any] | None:
    p = _qa_path(run_id)
    if not p.exists() or not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        # If file is corrupted, treat as missing for now
        return None


def qa_summary(qa: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Return a stable subset for UI/API. If missing, return None.
    """
    if not qa:
        return None

    out: dict[str, Any] = {}

    # Core fields (most important)
    for k in [
        "rows_total",
        "employees_unique",
        "date_min",
        "date_max",
        "duplicates_employee_fecha_rows",
        "missing_turno_rows",
        "missing_name_employees",
    ]:
        if k in qa:
            out[k] = qa.get(k)

    # Optional “libres ratio” fields (added later)
    for k in [
        "max_weekday_libre_ratio",
        "max_weekday_libre_date",
        "max_weekday_libres",
        "max_weekday_total_rows",
    ]:
        if k in qa:
            out[k] = qa.get(k)

    return out


def qa_status(qa: dict[str, Any] | None) -> str:
    """
    Returns:
      - "N/A" if no QA yet
      - "FAIL" if critical issues
      - "WARN" if suspicious but not blocking
      - "OK" otherwise
    """
    if not qa:
        return "N/A"

    dup = int(qa.get("duplicates_employee_fecha_rows", 0) or 0)
    missing_turno = int(qa.get("missing_turno_rows", 0) or 0)
    missing_name = int(qa.get("missing_name_employees", 0) or 0)

    # Critical issues -> FAIL
    if missing_turno > 0:
        return "FAIL"
    if dup > 0:
        return "FAIL"

    # Threshold: max ratio of LIBRE on weekdays (Mon-Fri)
    # You told me you moved it to 60%. We'll default to 0.60 and allow env override.
    warn_threshold = float(os.getenv("QA_MAX_WEEKDAY_LIBRE_RATIO_WARN", "0.60"))
    ratio = qa.get("max_weekday_libre_ratio", None)

    if ratio is not None:
        try:
            ratio_f = float(ratio)
            if ratio_f > warn_threshold:
                return "WARN"
        except Exception:
            # If ratio can't be parsed, ignore it
            pass

    # Missing names is usually a WARN (data issue, not solver issue)
    if missing_name > 0:
        return "WARN"

    return "OK"


def qa_message(qa: dict[str, Any] | None) -> str:
    """
    Human-friendly message to show in UI quickly.
    """
    st = qa_status(qa)
    if st == "N/A":
        return "QA N/A (todavía no se genera qa_plan.json)"
    if st == "OK":
        return "QA OK"
    if st == "FAIL":
        dup = int((qa or {}).get("duplicates_employee_fecha_rows", 0) or 0)
        missing_turno = int((qa or {}).get("missing_turno_rows", 0) or 0)
        reasons = []
        if missing_turno > 0:
            reasons.append(f"missing_turno_rows={missing_turno}")
        if dup > 0:
            reasons.append(f"duplicates_employee_fecha_rows={dup}")
        if reasons:
            return "QA FAIL: " + ", ".join(reasons)
        return "QA FAIL"
    if st == "WARN":
        ratio = (qa or {}).get("max_weekday_libre_ratio", None)
        date = (qa or {}).get("max_weekday_libre_date", None)
        if ratio is not None and date is not None:
            return f"QA WARN: libre_ratio alto en weekday ({ratio:.2f}) en {date}"
        return "QA WARN"
    return f"QA {st}"
