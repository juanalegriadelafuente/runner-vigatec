from __future__ import annotations

import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.db import get_db
from api.masterdata_models import Company, Branch, OrgUnit, Employee
from api.masterdata_schemas import (
    CompanyCreate, CompanyOut,
    BranchCreate, BranchOut,
    OrgUnitCreate, OrgUnitOut,
    EmployeeCreate, EmployeeUpdate, EmployeeOut,
)

router = APIRouter(tags=["masterdata"])


# -----------------------
# Companies
# -----------------------
@router.post("/companies", response_model=CompanyOut)
def create_company(payload: CompanyCreate, db: Session = Depends(get_db)):
    exists = db.query(Company).filter(Company.name == payload.name).first()
    if exists:
        raise HTTPException(status_code=409, detail="Company name already exists")
    c = Company(name=payload.name)
    db.add(c)
    db.commit()
    db.refresh(c)
    return c


@router.get("/companies", response_model=List[CompanyOut])
def list_companies(db: Session = Depends(get_db)):
    return db.query(Company).order_by(Company.created_at.desc()).all()


@router.get("/companies/{company_id}", response_model=CompanyOut)
def get_company(company_id: uuid.UUID, db: Session = Depends(get_db)):
    c = db.get(Company, company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Company not found")
    return c


# -----------------------
# Branches
# -----------------------
@router.post("/companies/{company_id}/branches", response_model=BranchOut)
def create_branch(company_id: uuid.UUID, payload: BranchCreate, db: Session = Depends(get_db)):
    c = db.get(Company, company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Company not found")

    exists = db.query(Branch).filter(Branch.company_id == company_id, Branch.code == payload.code).first()
    if exists:
        raise HTTPException(status_code=409, detail="Branch code already exists for this company")

    b = Branch(company_id=company_id, code=payload.code, name=payload.name)
    db.add(b)
    db.commit()
    db.refresh(b)
    return b


@router.get("/companies/{company_id}/branches", response_model=List[BranchOut])
def list_branches(company_id: uuid.UUID, db: Session = Depends(get_db)):
    c = db.get(Company, company_id)
    if not c:
        raise HTTPException(status_code=404, detail="Company not found")
    return db.query(Branch).filter(Branch.company_id == company_id).order_by(Branch.created_at.desc()).all()


@router.get("/branches/{branch_id}", response_model=BranchOut)
def get_branch(branch_id: uuid.UUID, db: Session = Depends(get_db)):
    b = db.get(Branch, branch_id)
    if not b:
        raise HTTPException(status_code=404, detail="Branch not found")
    return b


# -----------------------
# Org Units
# -----------------------
@router.post("/branches/{branch_id}/org-units", response_model=OrgUnitOut)
def create_org_unit(branch_id: uuid.UUID, payload: OrgUnitCreate, db: Session = Depends(get_db)):
    b = db.get(Branch, branch_id)
    if not b:
        raise HTTPException(status_code=404, detail="Branch not found")

    exists = db.query(OrgUnit).filter(OrgUnit.branch_id == branch_id, OrgUnit.org_unit_key == payload.org_unit_key).first()
    if exists:
        raise HTTPException(status_code=409, detail="org_unit_key already exists for this branch")

    ou = OrgUnit(branch_id=branch_id, org_unit_key=payload.org_unit_key, name=payload.name)
    db.add(ou)
    db.commit()
    db.refresh(ou)
    return ou


@router.get("/branches/{branch_id}/org-units", response_model=List[OrgUnitOut])
def list_org_units(branch_id: uuid.UUID, db: Session = Depends(get_db)):
    b = db.get(Branch, branch_id)
    if not b:
        raise HTTPException(status_code=404, detail="Branch not found")
    return db.query(OrgUnit).filter(OrgUnit.branch_id == branch_id).order_by(OrgUnit.created_at.desc()).all()


@router.get("/org-units/{org_unit_id}", response_model=OrgUnitOut)
def get_org_unit(org_unit_id: uuid.UUID, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")
    return ou


# -----------------------
# Employees
# -----------------------
@router.post("/org-units/{org_unit_id}/employees", response_model=EmployeeOut)
def create_employee(org_unit_id: uuid.UUID, payload: EmployeeCreate, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")

    exists = db.query(Employee).filter(Employee.org_unit_id == org_unit_id, Employee.employee_key == payload.employee_key).first()
    if exists:
        raise HTTPException(status_code=409, detail="Employee key already exists in this org unit")

    e = Employee(
        org_unit_id=org_unit_id,
        employee_key=payload.employee_key,
        rut=payload.rut,
        nombre=payload.nombre,
        cargo_id=payload.cargo_id,
        jornada_id=payload.jornada_id,
        contrato_max_min_semana=payload.contrato_max_min_semana,
        expertise=payload.expertise,
        active=payload.active,
    )
    db.add(e)
    db.commit()
    db.refresh(e)
    return e


@router.get("/org-units/{org_unit_id}/employees", response_model=List[EmployeeOut])
def list_employees(org_unit_id: uuid.UUID, db: Session = Depends(get_db)):
    ou = db.get(OrgUnit, org_unit_id)
    if not ou:
        raise HTTPException(status_code=404, detail="OrgUnit not found")
    return db.query(Employee).filter(Employee.org_unit_id == org_unit_id).order_by(Employee.created_at.desc()).all()


@router.get("/employees/{employee_id}", response_model=EmployeeOut)
def get_employee(employee_id: uuid.UUID, db: Session = Depends(get_db)):
    e = db.get(Employee, employee_id)
    if not e:
        raise HTTPException(status_code=404, detail="Employee not found")
    return e


@router.patch("/employees/{employee_id}", response_model=EmployeeOut)
def update_employee(employee_id: uuid.UUID, payload: EmployeeUpdate, db: Session = Depends(get_db)):
    e = db.get(Employee, employee_id)
    if not e:
        raise HTTPException(status_code=404, detail="Employee not found")

    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(e, k, v)

    db.add(e)
    db.commit()
    db.refresh(e)
    return e