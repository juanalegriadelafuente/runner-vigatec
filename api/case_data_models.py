from __future__ import annotations

import uuid
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from api.db import Base


class RestriccionEmpleado(Base):
    """
    Replica hoja RestriccionesEmpleado.
    employee_id puede ser NULL para reglas globales.
    """
    __tablename__ = "restricciones_empleado"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)

    employee_id = Column(String(80), nullable=True)  # rut/employee_id o vacío para global
    tipo = Column(String(80), nullable=False)

    valor1 = Column(String(200), nullable=True)
    valor2 = Column(String(200), nullable=True)

    dia_semana = Column(String(3), nullable=True)   # LUN..DOM
    fecha = Column(String(32), nullable=True)       # YYYY-MM-DD o ISO

    hard = Column(Boolean, nullable=False, server_default="false")
    penalizacion = Column(Integer, nullable=False, server_default="0")
    detalle = Column(String(500), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class AusentismoEmpleado(Base):
    """
    Replica hoja AusentismoEmpleado.
    """
    __tablename__ = "ausentismo_empleado"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)

    employee_id = Column(String(80), nullable=False)

    fecha_inicio = Column(String(32), nullable=True)  # YYYY-MM-DD o ISO
    fecha_fin = Column(String(32), nullable=True)

    ausentismo = Column(String(80), nullable=True)    # LM, VAC, etc (DEBE venir de vocab)
    detalle = Column(String(500), nullable=True)

    hard = Column(Boolean, nullable=False, server_default="true")
    penalizacion = Column(Integer, nullable=False, server_default="0")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)