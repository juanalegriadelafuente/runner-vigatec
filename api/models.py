from __future__ import annotations

import uuid
from datetime import datetime, timezone, date

from sqlalchemy import Column, String, DateTime, Text, Integer, Date, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from api.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Run(Base):
    __tablename__ = "runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    status = Column(String, nullable=False, default="queued")  # queued|running|success|failed
    original_filename = Column(String, nullable=True)

    case_path = Column(Text, nullable=False)
    out_dir = Column(Text, nullable=False)
    log_path = Column(Text, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    error_message = Column(Text, nullable=True)


class PlanningCycle(Base):
    __tablename__ = "planning_cycles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    tenant_id = Column(String, nullable=False, default="default")
    # Always YYYY-MM-01 (first day of target calendar month)
    target_month = Column(Date, nullable=False)

    # This is what goes to Parametros.fecha_inicio_mes
    cycle_start = Column(Date, nullable=False)
    cycle_end = Column(Date, nullable=False)

    # semanas = number of Sundays in the calendar month (4 or 5)
    weeks = Column(Integer, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        UniqueConstraint("tenant_id", "target_month", name="uq_planning_cycles_tenant_month"),
    )
