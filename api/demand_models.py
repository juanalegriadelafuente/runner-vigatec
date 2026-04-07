from __future__ import annotations

import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

try:
    from api.db import Base  # type: ignore
except Exception:  # pragma: no cover
    from api.models import Base  # type: ignore


class DemandUnit(Base):
    """
    DemandaUnidad por org_unit (no por cargo) en tramos horarios.
    Se exporta 1:1 a la hoja DemandaUnidad del case.xlsx
    
    Dos curvas de demanda:
    - requeridos (Mínimo Operativo): El piso absoluto para que la OU funcione.
      Sin esto, la unidad no puede abrir. Restricción DURA del solver.
    - requeridos_ideal (Demanda Real): Lo que realmente se necesita para operar bien.
      El solver intenta maximizar cobertura sobre esto como objetivo BLANDO.
    """
    __tablename__ = "demand_unit"
    __table_args__ = (
        UniqueConstraint("org_unit_id", "dia_semana", "inicio", "fin", name="uq_demand_unit_ou_dow_range"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_unit_id = Column(UUID(as_uuid=True), ForeignKey("org_units.id", ondelete="CASCADE"), nullable=False)

    # LUN..DOM
    dia_semana = Column(String(3), nullable=False)

    # Guardamos como string normalizada "HH:MM:SS" para export fácil a Excel
    inicio = Column(String(8), nullable=False)
    fin = Column(String(8), nullable=False)

    # CURVA 1: Mínimo Operativo (restricción dura)
    # Sin esto, la OU no puede abrir/operar
    requeridos = Column(Integer, nullable=False, default=0)

    # CURVA 2: Demanda Real / Ideal (objetivo blando) — NUEVO
    # Lo que realmente se necesita para operar bien
    # Si es NULL, se asume igual a requeridos (sin holgura)
    requeridos_ideal = Column(Integer, nullable=True)

    active = Column(Boolean, nullable=False, server_default="true")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    @property
    def ideal_efectivo(self) -> int:
        """Retorna el ideal efectivo: si no está definido, usa el mínimo."""
        return self.requeridos_ideal if self.requeridos_ideal is not None else self.requeridos

    @property
    def tiene_holgura(self) -> bool:
        """Indica si hay holgura definida (ideal > mínimo)."""
        return self.requeridos_ideal is not None and self.requeridos_ideal > self.requeridos


class PoolTurno(Base):
    """
    PoolTurnos por org_unit + cargo + dia_semana + shift_id
    """
    __tablename__ = "pool_turnos"
    __table_args__ = (
        UniqueConstraint("org_unit_id", "cargo_id", "dia_semana", "shift_id", name="uq_pool_ou_cargo_dow_shift"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_unit_id = Column(UUID(as_uuid=True), ForeignKey("org_units.id", ondelete="CASCADE"), nullable=False)

    cargo_id = Column(String(200), nullable=False)
    dia_semana = Column(String(3), nullable=False)

    shift_id = Column(String(120), nullable=False)

    habilitado = Column(Boolean, nullable=False, server_default="true")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)