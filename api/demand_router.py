from __future__ import annotations

import uuid
from typing import List, Set, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.db import get_db
from api.demand_models import DemandUnit, PoolTurno
from api.demand_schemas import (
    DemandUnitCreate, DemandUnitUpdate, DemandUnitOut,
    PoolTurnoCreate, PoolTurnoUpdate, PoolTurnoOut,
    OrgUnitCoverage, CompanyCoverage,
)
from api.masterdata_models import OrgUnit, Branch, Company, Employee  # usamos tus maestros existentes

DOW_ORDER = ["LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]

router = APIRouter(tags=["admin-demand"])


# -----------------------
# DemandUnit (OU demand)
# -----------------------
@router.post("/org-units/{org_unit_id}/demand", response_model=DemandUnitOut)
def upsert_demand_unit(org_unit_id: uuid.UUID, payload: DemandUnitCreate, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    # Upsert por clave (ou, dow, inicio, fin)
    row = (
        db.query(DemandUnit)
        .filter(
            DemandUnit.org_unit_id == org_unit_id,
            DemandUnit.dia_semana == payload.dia_semana,
            DemandUnit.inicio == payload.inicio,
            DemandUnit.fin == payload.fin,
        )
        .first()
    )

    if row:
        row.requeridos = payload.requeridos
        row.active = payload.active
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    row = DemandUnit(
        org_unit_id=org_unit_id,
        dia_semana=payload.dia_semana,
        inicio=payload.inicio,
        fin=payload.fin,
        requeridos=payload.requeridos,
        active=payload.active,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/org-units/{org_unit_id}/demand", response_model=List[DemandUnitOut])
def list_demand_unit(org_unit_id: uuid.UUID, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    return (
        db.query(DemandUnit)
        .filter(DemandUnit.org_unit_id == org_unit_id)
        .order_by(DemandUnit.dia_semana.asc(), DemandUnit.inicio.asc(), DemandUnit.fin.asc())
        .all()
    )


@router.patch("/demand/{demand_id}", response_model=DemandUnitOut)
def update_demand_unit(demand_id: uuid.UUID, payload: DemandUnitUpdate, db: Session = Depends(get_db)):
    row = db.get(DemandUnit, demand_id)
    if not row:
        raise HTTPException(status_code=404, detail="Demand row not found")

    data = payload.model_dump(exclude_unset=True)
    # si cambia inicio/fin, podría violar unique constraint; dejamos que DB lo rechace si choca
    for k, v in data.items():
        setattr(row, k, v)

    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/demand/{demand_id}")
def delete_demand_unit(demand_id: uuid.UUID, db: Session = Depends(get_db)):
    row = db.get(DemandUnit, demand_id)
    if not row:
        raise HTTPException(status_code=404, detail="Demand row not found")
    db.delete(row)
    db.commit()
    return {"ok": True, "deleted": str(demand_id)}


# -----------------------
# PoolTurnos
# -----------------------
@router.post("/org-units/{org_unit_id}/pool-turnos", response_model=PoolTurnoOut)
def upsert_pool_turno(org_unit_id: uuid.UUID, payload: PoolTurnoCreate, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    row = (
        db.query(PoolTurno)
        .filter(
            PoolTurno.org_unit_id == org_unit_id,
            PoolTurno.cargo_id == payload.cargo_id,
            PoolTurno.dia_semana == payload.dia_semana,
            PoolTurno.shift_id == payload.shift_id,
        )
        .first()
    )

    if row:
        row.habilitado = payload.habilitado
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    row = PoolTurno(
        org_unit_id=org_unit_id,
        cargo_id=payload.cargo_id,
        dia_semana=payload.dia_semana,
        shift_id=payload.shift_id,
        habilitado=payload.habilitado,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.get("/org-units/{org_unit_id}/pool-turnos", response_model=List[PoolTurnoOut])
def list_pool_turnos(org_unit_id: uuid.UUID, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    return (
        db.query(PoolTurno)
        .filter(PoolTurno.org_unit_id == org_unit_id)
        .order_by(PoolTurno.cargo_id.asc(), PoolTurno.dia_semana.asc(), PoolTurno.shift_id.asc())
        .all()
    )


@router.patch("/pool-turnos/{pool_id}", response_model=PoolTurnoOut)
def update_pool_turno(pool_id: uuid.UUID, payload: PoolTurnoUpdate, db: Session = Depends(get_db)):
    row = db.get(PoolTurno, pool_id)
    if not row:
        raise HTTPException(status_code=404, detail="Pool row not found")

    data = payload.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(row, k, v)

    db.add(row)
    db.commit()
    db.refresh(row)
    return row


@router.delete("/pool-turnos/{pool_id}")
def delete_pool_turno(pool_id: uuid.UUID, db: Session = Depends(get_db)):
    row = db.get(PoolTurno, pool_id)
    if not row:
        raise HTTPException(status_code=404, detail="Pool row not found")
    db.delete(row)
    db.commit()
    return {"ok": True, "deleted": str(pool_id)}


# -----------------------
# Coverage checks (anti LIBRE masivo)
# -----------------------
def _demand_missing_days(rows: List[DemandUnit]) -> List[str]:
    present = {r.dia_semana for r in rows if r.active}
    return [d for d in DOW_ORDER if d not in present]


def _employee_cargos_in_ou(db: Session, org_unit_id: uuid.UUID) -> List[str]:
    cargos = (
        db.query(Employee.cargo_id)
        .filter(Employee.org_unit_id == org_unit_id, Employee.active == True)  # noqa: E712
        .distinct()
        .all()
    )
    return sorted({c[0] for c in cargos if c and c[0]})


def _cargos_without_pool(db: Session, org_unit_id: uuid.UUID, cargos: List[str]) -> List[str]:
    if not cargos:
        return []
    pool_cargos = (
        db.query(PoolTurno.cargo_id)
        .filter(PoolTurno.org_unit_id == org_unit_id, PoolTurno.habilitado == True)  # noqa: E712
        .distinct()
        .all()
    )
    has = {c[0] for c in pool_cargos if c and c[0]}
    return sorted([c for c in cargos if c not in has])


@router.get("/coverage/org-units/{org_unit_id}", response_model=OrgUnitCoverage)
def coverage_org_unit(org_unit_id: uuid.UUID, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    demand_rows = db.query(DemandUnit).filter(DemandUnit.org_unit_id == org_unit_id).all()
    pool_rows = db.query(PoolTurno).filter(PoolTurno.org_unit_id == org_unit_id).all()

    missing_days = _demand_missing_days(demand_rows)

    cargos = _employee_cargos_in_ou(db, org_unit_id)
    cargos_wo_pool = _cargos_without_pool(db, org_unit_id, cargos)

    warnings: List[str] = []
    if len(demand_rows) == 0:
        warnings.append("OU no tiene DemandaUnidad cargada (demanda=0 => muchos LIBRE).")
    if missing_days:
        warnings.append(f"DemandaUnidad no cubre días: {missing_days}")
    if cargos and cargos_wo_pool:
        warnings.append(f"Hay cargos en dotación sin pool habilitado: {cargos_wo_pool}")

    return OrgUnitCoverage(
        org_unit_id=org_unit_id,
        org_unit_key=ou.org_unit_key,
        demand_rows=len(demand_rows),
        demand_missing_days=missing_days,
        pool_rows=len(pool_rows),
        employee_cargos=cargos,
        cargos_without_pool=cargos_wo_pool,
        warnings=warnings,
    )


@router.get("/coverage/companies/{company_id}", response_model=CompanyCoverage)
def coverage_company(company_id: uuid.UUID, db: Session = Depends(get_db)):
    c = db.get(Company, company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Company not found")

    # org_units de la empresa: companies -> branches -> org_units
    ous = (
        db.query(OrgUnit)
        .join(Branch, OrgUnit.branch_id == Branch.id)
        .filter(Branch.company_id == company_id)
        .all()
    )

    results: List[OrgUnitCoverage] = []
    all_warnings: List[str] = []

    for ou in ous:
        cov = coverage_org_unit(ou.id, db)
        results.append(cov)
        all_warnings.extend(cov.warnings)

    if not ous:
        all_warnings.append("La empresa no tiene unidades organizativas (OU).")

    return CompanyCoverage(company_id=company_id, org_units=results, warnings=sorted(set(all_warnings)))