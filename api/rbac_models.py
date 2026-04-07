from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from api.db import Base


def _utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Identidad
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Autenticación (nullable para no romper usuarios existentes sin contraseña)
    password_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Rol alto nivel
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="SUPER")
    # SUPER | COMPANY_ADMIN | ZONAL_ADMIN | BRANCH_ADMIN | COLLABORATOR

    # Permisos por feature
    can_manage_catalogs: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    can_manage_companies: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    can_edit_employees: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_edit_demand: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_edit_pool: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_edit_restrictions: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_edit_absences: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_request_turnos: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    can_edit_turnos: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    can_view_all_runs: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class UserScope(Base):
    """
    Define el alcance del usuario:
    - COMPANY_ADMIN: scopes con company_id
    - ZONAL_ADMIN:   scopes con branch_id (muchos)
    - BRANCH_ADMIN:  scope con branch_id
    - COLLABORATOR:  scope con org_unit_id (o empleado en futuro)
    """
    __tablename__ = "user_scopes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)

    company_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("companies.id"), nullable=True)
    branch_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("branches.id"), nullable=True)
    org_unit_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("org_units.id"), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
