from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict


# -----------------------
# Company
# -----------------------
class CompanyCreate(BaseModel):
    name: str = Field(min_length=2, max_length=200)


class CompanyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    created_at: datetime


# -----------------------
# Branch
# -----------------------
class BranchCreate(BaseModel):
    code: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=2, max_length=200)


class BranchOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    company_id: uuid.UUID
    code: str
    name: str
    created_at: datetime


# -----------------------
# OrgUnit
# -----------------------
class OrgUnitCreate(BaseModel):
    org_unit_key: str = Field(min_length=1, max_length=120)  # ej: "PERU 805"
    name: str = Field(min_length=2, max_length=200)


class OrgUnitOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    branch_id: uuid.UUID
    org_unit_key: str
    name: str
    created_at: datetime


# -----------------------
# Employee
# -----------------------
class EmployeeCreate(BaseModel):
    employee_key: str = Field(min_length=1, max_length=80)  # ej: rut
    rut: Optional[str] = Field(default=None, max_length=80)
    nombre: str = Field(min_length=2, max_length=200)

    cargo_id: str = Field(min_length=1, max_length=200)
    jornada_id: str = Field(min_length=1, max_length=80)
    contrato_max_min_semana: int = Field(ge=0)

    expertise: Optional[str] = Field(default=None, max_length=40)
    active: bool = True


class EmployeeUpdate(BaseModel):
    rut: Optional[str] = Field(default=None, max_length=80)
    nombre: Optional[str] = Field(default=None, max_length=200)

    cargo_id: Optional[str] = Field(default=None, max_length=200)
    jornada_id: Optional[str] = Field(default=None, max_length=80)
    contrato_max_min_semana: Optional[int] = Field(default=None, ge=0)

    expertise: Optional[str] = Field(default=None, max_length=40)
    active: Optional[bool] = None


class EmployeeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_unit_id: uuid.UUID

    employee_key: str
    rut: Optional[str]
    nombre: str

    cargo_id: str
    jornada_id: str
    contrato_max_min_semana: int

    expertise: Optional[str]
    active: bool
    created_at: datetime