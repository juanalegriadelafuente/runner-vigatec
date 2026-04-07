from __future__ import annotations

import uuid
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from api.db import Base


class PlanOverride(Base):
    """
    Override de una celda del plan para un Run específico.
    Guarda el shift_id final que debe mostrarse/exportarse.
    """
    __tablename__ = "plan_overrides"
    __table_args__ = (
        UniqueConstraint("run_id", "employee_id", "fecha", name="uq_plan_override_cell"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    run_id = Column(UUID(as_uuid=True), ForeignKey("runs.id", ondelete="CASCADE"), nullable=False)

    employee_id = Column(String(80), nullable=False)  # employee_key (lo mismo que lee el solver)
    fecha = Column(String(32), nullable=False)        # YYYY-MM-DD

    shift_id = Column(String(80), nullable=False)     # nuevo turno/código (LIBRE/LM/etc)

    # opcional (para auditoría futura)
    source = Column(String(32), nullable=False, server_default="UI")  # UI/IMPORT/etc
    is_valid = Column(Boolean, nullable=False, server_default="true")
    error_message = Column(String(500), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)