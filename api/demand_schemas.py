from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator

DOW_ALLOWED = {"LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"}


def normalize_time_str(x: str) -> str:
    """
    Acepta: '7:30', '07:30', '07:30:00' -> devuelve 'HH:MM:SS'
    """
    s = str(x).strip()
    if not s:
        raise ValueError("hora vacía")

    parts = s.split(":")
    if len(parts) == 2:
        hh, mm = parts
        ss = "00"
    elif len(parts) == 3:
        hh, mm, ss = parts
    else:
        raise ValueError("Formato inválido. Usa HH:MM o HH:MM:SS")

    if not (hh.isdigit() and mm.isdigit() and ss.isdigit()):
        raise ValueError("Hora debe ser numérica")

    hh_i = int(hh)
    mm_i = int(mm)
    ss_i = int(ss)
    if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59 and 0 <= ss_i <= 59):
        raise ValueError("Hora fuera de rango")

    return f"{hh_i:02d}:{mm_i:02d}:{ss_i:02d}"


# -----------------------
# DemandUnit
# -----------------------
class DemandUnitCreate(BaseModel):
    dia_semana: str = Field(min_length=3, max_length=3)
    inicio: str = Field(min_length=1, max_length=16)
    fin: str = Field(min_length=1, max_length=16)
    requeridos: int = Field(ge=0)
    active: bool = True

    @field_validator("dia_semana")
    @classmethod
    def v_dow(cls, v: str) -> str:
        s = v.strip().upper()
        s = s.replace("MIÉ", "MIE").replace("SÁB", "SAB")
        if s not in DOW_ALLOWED:
            raise ValueError(f"dia_semana inválido. Usa {sorted(DOW_ALLOWED)}")
        return s

    @field_validator("inicio", "fin")
    @classmethod
    def v_time(cls, v: str) -> str:
        return normalize_time_str(v)


class DemandUnitUpdate(BaseModel):
    inicio: Optional[str] = None
    fin: Optional[str] = None
    requeridos: Optional[int] = Field(default=None, ge=0)
    active: Optional[bool] = None

    @field_validator("inicio", "fin")
    @classmethod
    def v_time_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return normalize_time_str(v)


class DemandUnitOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_unit_id: uuid.UUID
    dia_semana: str
    inicio: str
    fin: str
    requeridos: int
    active: bool
    created_at: datetime
    updated_at: datetime


# -----------------------
# PoolTurnos
# -----------------------
class PoolTurnoCreate(BaseModel):
    cargo_id: str = Field(min_length=1, max_length=200)
    dia_semana: str = Field(min_length=3, max_length=3)
    shift_id: str = Field(min_length=1, max_length=120)
    habilitado: bool = True

    @field_validator("dia_semana")
    @classmethod
    def v_dow(cls, v: str) -> str:
        s = v.strip().upper()
        s = s.replace("MIÉ", "MIE").replace("SÁB", "SAB")
        if s not in DOW_ALLOWED:
            raise ValueError(f"dia_semana inválido. Usa {sorted(DOW_ALLOWED)}")
        return s

    @field_validator("cargo_id", "shift_id")
    @classmethod
    def v_trim(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("no puede ser vacío")
        return s


class PoolTurnoUpdate(BaseModel):
    habilitado: Optional[bool] = None


class PoolTurnoOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    org_unit_id: uuid.UUID
    cargo_id: str
    dia_semana: str
    shift_id: str
    habilitado: bool
    created_at: datetime
    updated_at: datetime


# -----------------------
# Coverage
# -----------------------
class OrgUnitCoverage(BaseModel):
    org_unit_id: uuid.UUID
    org_unit_key: Optional[str] = None

    demand_rows: int
    demand_missing_days: List[str]

    pool_rows: int
    employee_cargos: List[str]
    cargos_without_pool: List[str]

    warnings: List[str]


class CompanyCoverage(BaseModel):
    company_id: uuid.UUID
    org_units: List[OrgUnitCoverage]
    warnings: List[str]