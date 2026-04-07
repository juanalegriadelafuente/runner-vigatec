from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from sqlalchemy.orm import Session

from api.masterdata_models import Company, Branch, OrgUnit


@dataclass
class UiCtx:
    company_id: Optional[uuid.UUID] = None
    branch_id: Optional[uuid.UUID] = None
    org_unit_id: Optional[uuid.UUID] = None


def load_ctx_from_cookie(request) -> UiCtx:
    def _get_uuid(name: str) -> Optional[uuid.UUID]:
      v = request.cookies.get(name)
      if not v:
        return None
      try:
        return uuid.UUID(v)
      except Exception:
        return None

    return UiCtx(
      company_id=_get_uuid("ctx_company_id"),
      branch_id=_get_uuid("ctx_branch_id"),
      org_unit_id=_get_uuid("ctx_org_unit_id"),
    )


def build_ctx_lists(db: Session, ctx: UiCtx):
    companies = db.query(Company).order_by(Company.name.asc()).all()

    branches: List[Branch] = []
    org_units: List[OrgUnit] = []

    if ctx.company_id:
        branches = db.query(Branch).filter(Branch.company_id == ctx.company_id).order_by(Branch.code.asc()).all()
        if ctx.branch_id:
            org_units = db.query(OrgUnit).filter(OrgUnit.branch_id == ctx.branch_id).order_by(OrgUnit.org_unit_key.asc()).all()
        else:
            # todas las OU de la empresa
            org_units = (
                db.query(OrgUnit)
                .join(Branch, OrgUnit.branch_id == Branch.id)
                .filter(Branch.company_id == ctx.company_id)
                .order_by(OrgUnit.org_unit_key.asc())
                .all()
            )

    return companies, branches, org_units


def enrich_template_context(db: Session, request, data: Dict[str, Any]) -> Dict[str, Any]:
    ctx = load_ctx_from_cookie(request)
    companies, branches, org_units = build_ctx_lists(db, ctx)
    data.update(
        {
            "ctx": ctx,
            "ctx_companies": companies,
            "ctx_branches": branches,
            "ctx_org_units": org_units,
        }
    )
    return data