from __future__ import annotations

import uuid
from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from api.db import Base


class Holiday(Base):
    """
    Feriado por sucursal.

    - branch_id      → FK a branches.id
    - fecha          → date del feriado
    - nombre         → descripción (ej: "Año Nuevo")
    - irrenunciable  → si True: el solver no puede asignar turno trabajado ese día
                       y el validador de edición manual lo bloquea
    """

    __tablename__ = "holidays"
    __table_args__ = (
        UniqueConstraint("branch_id", "fecha", name="uq_holiday_branch_fecha"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_id = Column(UUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), nullable=False, index=True)
    fecha = Column(Date, nullable=False)
    nombre = Column(String(200), nullable=False, default="")
    irrenunciable = Column(Boolean, nullable=False, default=False)


class HolidayCl(Base):
    """Catálogo nacional de feriados de Chile. Independiente de empresa/sucursal."""
    __tablename__ = "holidays_cl"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    fecha = Column(Date, nullable=False)
    nombre = Column(String(200), nullable=False, default="")
    irrenunciable = Column(Boolean, nullable=False, default=False)
    nacional = Column(Boolean, nullable=False, default=True)
    region = Column(String(200), nullable=True, default=None)

    __table_args__ = (
        UniqueConstraint("fecha", "region", name="uq_holidays_cl_fecha_region"),
    )
