from __future__ import annotations

import uuid
from typing import Iterable, Optional, Set, Tuple

from sqlalchemy.orm import Session

from api.masterdata_models import Branch, Company, OrgUnit
from api.rbac_models import User, UserScope


ROLE_SUPER = "SUPER"
ROLE_COMPANY_ADMIN = "COMPANY_ADMIN"
ROLE_ZONAL_ADMIN = "ZONAL_ADMIN"
ROLE_BRANCH_ADMIN = "BRANCH_ADMIN"
ROLE_COLLABORATOR = "COLLABORATOR"


def get_current_user(db: Session, user_id: Optional[uuid.UUID]) -> Optional[User]:
    if not user_id:
        return None
    u = db.get(User, user_id)
    if u and u.is_active:
        return u
    return None


def get_user_scopes(db: Session, user_id: uuid.UUID) -> list[UserScope]:
    return db.query(UserScope).filter(UserScope.user_id == user_id).all()


def _allowed_company_ids(db: Session, u: User) -> Set[uuid.UUID]:
    if u.role == ROLE_SUPER:
        # Super puede ver todas
        return set([c.id for c in db.query(Company).all()])

    scopes = get_user_scopes(db, u.id)
    company_ids: Set[uuid.UUID] = set()

    for s in scopes:
        if s.company_id:
            company_ids.add(s.company_id)
        if s.branch_id:
            b = db.get(Branch, s.branch_id)
            if b:
                company_ids.add(b.company_id)
        if s.org_unit_id:
            ou = db.get(OrgUnit, s.org_unit_id)
            if ou:
                b = db.get(Branch, ou.branch_id)
                if b:
                    company_ids.add(b.company_id)

    return company_ids


def _allowed_branch_ids(db: Session, u: User) -> Set[uuid.UUID]:
    if u.role == ROLE_SUPER:
        return set([b.id for b in db.query(Branch).all()])

    scopes = get_user_scopes(db, u.id)
    branch_ids: Set[uuid.UUID] = set()

    for s in scopes:
        if s.branch_id:
            branch_ids.add(s.branch_id)
        if s.org_unit_id:
            ou = db.get(OrgUnit, s.org_unit_id)
            if ou:
                branch_ids.add(ou.branch_id)
        if s.company_id:
            bs = db.query(Branch).filter(Branch.company_id == s.company_id).all()
            for b in bs:
                branch_ids.add(b.id)

    return branch_ids


def _allowed_org_unit_ids(db: Session, u: User) -> Set[uuid.UUID]:
    if u.role == ROLE_SUPER:
        return set([ou.id for ou in db.query(OrgUnit).all()])

    scopes = get_user_scopes(db, u.id)
    ou_ids: Set[uuid.UUID] = set()

    for s in scopes:
        if s.org_unit_id:
            ou_ids.add(s.org_unit_id)
        if s.branch_id:
            ous = db.query(OrgUnit).filter(OrgUnit.branch_id == s.branch_id).all()
            for ou in ous:
                ou_ids.add(ou.id)
        if s.company_id:
            ous = (
                db.query(OrgUnit)
                .join(Branch, OrgUnit.branch_id == Branch.id)
                .filter(Branch.company_id == s.company_id)
                .all()
            )
            for ou in ous:
                ou_ids.add(ou.id)

    return ou_ids


def filter_companies(db: Session, u: Optional[User]) -> list[Company]:
    if not u:
        return []
    ids = _allowed_company_ids(db, u)
    return db.query(Company).filter(Company.id.in_(ids)).order_by(Company.name.asc()).all()


def filter_branches(db: Session, u: Optional[User], company_id: Optional[uuid.UUID]) -> list[Branch]:
    if not u or not company_id:
        return []
    allowed = _allowed_branch_ids(db, u)
    return (
        db.query(Branch)
        .filter(Branch.company_id == company_id, Branch.id.in_(allowed))
        .order_by(Branch.code.asc())
        .all()
    )


def filter_org_units(db: Session, u: Optional[User], branch_id: Optional[uuid.UUID], company_id: Optional[uuid.UUID]) -> list[OrgUnit]:
    if not u:
        return []
    allowed = _allowed_org_unit_ids(db, u)

    q = db.query(OrgUnit).filter(OrgUnit.id.in_(allowed))

    if branch_id:
        q = q.filter(OrgUnit.branch_id == branch_id)
    elif company_id:
        q = q.join(Branch, OrgUnit.branch_id == Branch.id).filter(Branch.company_id == company_id)

    return q.order_by(OrgUnit.org_unit_key.asc()).all()


def require(u: Optional[User], attr: str) -> None:
    if not u:
        raise PermissionError("No user selected")
    if u.role == ROLE_SUPER:
        return
    ok = getattr(u, attr, False)
    if not ok:
        raise PermissionError(f"Permission denied: {attr}")


def can_see_company(db: Session, u: Optional[User], company_id: uuid.UUID) -> bool:
    if not u:
        return False
    if u.role == ROLE_SUPER:
        return True
    return company_id in _allowed_company_ids(db, u)