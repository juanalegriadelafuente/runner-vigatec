from __future__ import annotations

import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

# Base puede vivir en api.db o api.models según tu versión.
try:
    from api.db import Base  # type: ignore
except Exception:  # pragma: no cover
    from api.models import Base  # type: ignore


class Company(Base):
    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(200), unique=True, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    branches = relationship("Branch", back_populates="company", cascade="all, delete-orphan")


class Branch(Base):
    __tablename__ = "branches"
    __table_args__ = (
        UniqueConstraint("company_id", "code", name="uq_branches_company_code"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)

    code = Column(String(80), nullable=False)
    name = Column(String(200), nullable=False)

    # Si True: el solver puede asignar turnos trabajados en feriados (demanda normal).
    # Si False: la demanda baja a 0 en feriados (nadie trabaja).
    opera_en_feriados = Column(Boolean, nullable=False, server_default="false")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    company = relationship("Company", back_populates="branches")
    org_units = relationship("OrgUnit", back_populates="branch", cascade="all, delete-orphan")


class OrgUnit(Base):
    __tablename__ = "org_units"
    __table_args__ = (
        UniqueConstraint("branch_id", "org_unit_key", name="uq_org_units_branch_key"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    branch_id = Column(UUID(as_uuid=True), ForeignKey("branches.id", ondelete="CASCADE"), nullable=False)

    org_unit_key = Column(String(120), nullable=False)
    name = Column(String(200), nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    branch = relationship("Branch", back_populates="org_units")
    employees = relationship("Employee", back_populates="org_unit", cascade="all, delete-orphan")


class Employee(Base):
    __tablename__ = "employees"
    __table_args__ = (
        UniqueConstraint("org_unit_id", "employee_key", name="uq_employees_org_unit_employee_key"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_unit_id = Column(UUID(as_uuid=True), ForeignKey("org_units.id", ondelete="CASCADE"), nullable=False)

    employee_key = Column(String(80), nullable=False)
    rut = Column(String(80), nullable=True)
    nombre = Column(String(200), nullable=False)

    cargo_id = Column(String(200), nullable=False)
    jornada_id = Column(String(80), nullable=False)
    contrato_max_min_semana = Column(Integer, nullable=False)

    expertise = Column(String(40), nullable=True)
    email = Column(String(254), nullable=True)

    active = Column(Boolean, nullable=False, server_default="true")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    org_unit = relationship("OrgUnit", back_populates="employees")
