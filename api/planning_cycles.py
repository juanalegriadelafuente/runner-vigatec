# api/planning_cycles.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


def _parse_month(month: str) -> Tuple[int, int]:
    if not MONTH_RE.match(month):
        raise ValueError("month must be YYYY-MM")
    y, m = month.split("-")
    return int(y), int(m)


def _month_start_end(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    # next month start
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    end = nxt - timedelta(days=1)
    return start, end


def sundays_in_month(year: int, month: int) -> List[date]:
    start, end = _month_start_end(year, month)
    d = start
    out = []
    while d <= end:
        if d.weekday() == 6:  # Monday=0 ... Sunday=6
            out.append(d)
        d += timedelta(days=1)
    return out


def compute_cycle(month: str) -> Dict[str, Any]:
    """
    Regla Vigatec (según lo que definiste):
    - La cantidad de semanas a planificar = cantidad de domingos del mes.
    - El horizonte del mes = desde el LUN previo al primer DOM del mes, hasta el último DOM del mes.
      Ej: Abril 2026 → primer domingo 2026-04-05 → inicio 2026-03-30 (LUN) → fin 2026-04-26 (DOM).
    """
    year, m = _parse_month(month)
    sundays = sundays_in_month(year, m)
    if not sundays:
        raise ValueError(f"month has no sundays? impossible: {month}")

    first_sun = sundays[0]
    last_sun = sundays[-1]
    start_date = first_sun - timedelta(days=6)  # Monday before
    end_date = last_sun

    cycles = []
    for idx, sun in enumerate(sundays, start=1):
        cycles.append(
            {
                "cycle_index": idx,
                "start_date": (sun - timedelta(days=6)).isoformat(),
                "end_date": sun.isoformat(),
                "sunday": sun.isoformat(),
            }
        )

    return {
        "month": month,
        "weeks": len(sundays),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "sundays": [d.isoformat() for d in sundays],
        "cycles": cycles,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _planning_dir(storage_path: Path) -> Path:
    d = storage_path / "planning"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cycles_path(storage_path: Path) -> Path:
    return _planning_dir(storage_path) / "cycles.json"


def load_cycles(storage_path: Path) -> Dict[str, Any]:
    p = _cycles_path(storage_path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def save_cycles(storage_path: Path, data: Dict[str, Any]) -> None:
    p = _cycles_path(storage_path)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def month_add(year: int, month: int, add: int) -> Tuple[int, int]:
    # month 1..12
    total = (year * 12 + (month - 1)) + add
    y = total // 12
    m = (total % 12) + 1
    return y, m


def precompute(storage_path: Path, start_month: str, months: int) -> Dict[str, Any]:
    y, m = _parse_month(start_month)
    db = load_cycles(storage_path)

    for i in range(months):
        yy, mm = month_add(y, m, i)
        key = f"{yy:04d}-{mm:02d}"
        db[key] = compute_cycle(key)

    save_cycles(storage_path, db)
    return {"ok": True, "count": len(db), "start_month": start_month, "months": months}
