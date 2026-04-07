# Corre desde la raiz del proyecto: python append_endpoints.py

content = '''

# =========================
# SUCURSALES
# =========================

@router.post("/companies/{company_id}/branches")
def ui_branch_create(
    company_id: uuid.UUID,
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    db: Session = Depends(get_db),
):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    if not can_see_company(db, current_user, company_id):
        raise HTTPException(403, "Forbidden")
    code = code.strip()
    name = name.strip()
    if not code or not name:
        return _redirect(f"/ui/companies/{company_id}?err=Código+y+nombre+son+requeridos")
    exists = db.query(Branch).filter(Branch.company_id == company_id, Branch.code == code).first()
    if exists:
        return _redirect(f"/ui/companies/{company_id}?err=Ya+existe+una+sucursal+con+código+{code}")
    db.add(Branch(company_id=company_id, code=code, name=name))
    db.commit()
    return _redirect(f"/ui/companies/{company_id}?ok=Sucursal+creada")


@router.get("/branches/{branch_id}", response_class=HTMLResponse)
def ui_branch_detail(branch_id: uuid.UUID, request: Request, db: Session = Depends(get_db)):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    branch = db.get(Branch, branch_id)
    if not branch:
        raise HTTPException(404, "Sucursal no encontrada")
    if not can_see_company(db, current_user, branch.company_id):
        raise HTTPException(403, "Forbidden")
    org_units = (
        db.query(OrgUnit)
        .filter(OrgUnit.branch_id == branch_id)
        .order_by(OrgUnit.org_unit_key.asc())
        .all()
    )
    return TEMPLATES.TemplateResponse(
        "branch_detail.html",
        _enrich(db, request, {
            "request": request,
            "branch": branch,
            "company_id": str(branch.company_id),
            "org_units": org_units,
            "ok": request.query_params.get("ok"),
            "err": request.query_params.get("err"),
        }),
    )


@router.post("/branches/{branch_id}/settings")
def ui_branch_settings(
    branch_id: uuid.UUID,
    request: Request,
    company_id: str = Form(""),
    opera_en_feriados: str = Form(""),
    db: Session = Depends(get_db),
):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    branch = db.get(Branch, branch_id)
    if not branch:
        raise HTTPException(404, "Sucursal no encontrada")
    if not can_see_company(db, current_user, branch.company_id):
        raise HTTPException(403, "Forbidden")
    branch.opera_en_feriados = bool(opera_en_feriados)
    db.commit()
    return _redirect(f"/ui/branches/{branch_id}?ok=Configuración+guardada")


@router.post("/branches/{branch_id}/org-units")
def ui_org_unit_create(
    branch_id: uuid.UUID,
    request: Request,
    org_unit_key: str = Form(...),
    name: str = Form(...),
    db: Session = Depends(get_db),
):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    branch = db.get(Branch, branch_id)
    if not branch:
        raise HTTPException(404, "Sucursal no encontrada")
    if not can_see_company(db, current_user, branch.company_id):
        raise HTTPException(403, "Forbidden")
    org_unit_key = org_unit_key.strip()
    name = name.strip()
    if not org_unit_key or not name:
        return _redirect(f"/ui/branches/{branch_id}?err=org_unit_key+y+nombre+son+requeridos")
    exists = db.query(OrgUnit).filter(
        OrgUnit.branch_id == branch_id,
        OrgUnit.org_unit_key == org_unit_key
    ).first()
    if exists:
        return _redirect(f"/ui/branches/{branch_id}?err=Ya+existe+una+OU+con+key+{org_unit_key}")
    db.add(OrgUnit(branch_id=branch_id, org_unit_key=org_unit_key, name=name))
    db.commit()
    return _redirect(f"/ui/branches/{branch_id}?ok=OU+creada")


# =========================
# FERIADOS (por sucursal)
# =========================

_DOW_ES = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]


@router.get("/holidays", response_class=HTMLResponse)
def ui_holidays(request: Request, db: Session = Depends(get_db)):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    companies = filter_companies(db, current_user)
    q_company_id = request.query_params.get("company_id") or (str(ctx.company_id) if ctx.company_id else "")
    q_branch_id  = request.query_params.get("branch_id") or ""
    try:
        selected_year = int(request.query_params.get("year") or datetime.now(timezone.utc).year)
    except ValueError:
        selected_year = datetime.now(timezone.utc).year
    years = list(range(selected_year - 1, selected_year + 3))
    branches = []
    holidays_out = []
    if q_company_id:
        cid = _parse_uuid(q_company_id)
        if cid and can_see_company(db, current_user, cid):
            branches = (
                db.query(Branch)
                .filter(Branch.company_id == cid)
                .order_by(Branch.code.asc())
                .all()
            )
    if q_branch_id:
        bid = _parse_uuid(q_branch_id)
        if bid:
            raw = (
                db.query(Holiday)
                .filter(
                    Holiday.branch_id == bid,
                    Holiday.fecha >= date(selected_year, 1, 1),
                    Holiday.fecha <= date(selected_year, 12, 31),
                )
                .order_by(Holiday.fecha.asc())
                .all()
            )
            for h in raw:
                d = h.fecha if isinstance(h.fecha, date) else date.fromisoformat(str(h.fecha))
                holidays_out.append({
                    "id": h.id,
                    "fecha": d.isoformat(),
                    "dow": _DOW_ES[d.weekday()],
                    "nombre": h.nombre,
                    "irrenunciable": h.irrenunciable,
                })
    return TEMPLATES.TemplateResponse(
        "holidays.html",
        _enrich(db, request, {
            "request": request,
            "companies": companies,
            "branches": branches,
            "holidays": holidays_out,
            "selected_company_id": q_company_id,
            "selected_branch_id": q_branch_id,
            "selected_year": selected_year,
            "years": years,
            "err": request.query_params.get("err"),
            "ok":  request.query_params.get("ok"),
        }),
    )


@router.post("/holidays")
def ui_holidays_create(
    request: Request,
    company_id: str = Form(...),
    branch_id: str = Form(...),
    fecha: str = Form(...),
    nombre: str = Form(...),
    irrenunciable: str = Form(""),
    year: str = Form(""),
    db: Session = Depends(get_db),
):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    cid = _parse_uuid(company_id)
    bid = _parse_uuid(branch_id)
    if not cid or not bid:
        return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&err=Datos+invalidos")
    if not can_see_company(db, current_user, cid):
        raise HTTPException(403, "Forbidden")
    try:
        d = date.fromisoformat(fecha.strip())
    except ValueError:
        return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&err=Fecha+invalida")
    exists = db.query(Holiday).filter(Holiday.branch_id == bid, Holiday.fecha == d).first()
    if exists:
        return _redirect(
            f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}"
            f"&err=Ya+existe+un+feriado+el+{d.isoformat()}+para+esta+sucursal"
        )
    db.add(Holiday(branch_id=bid, fecha=d, nombre=nombre.strip(), irrenunciable=bool(irrenunciable)))
    db.commit()
    y = year or str(d.year)
    return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={y}&ok=Feriado+agregado")


@router.post("/holidays/{holiday_id}/delete")
def ui_holidays_delete(
    holiday_id: str,
    request: Request,
    company_id: str = Form(""),
    branch_id: str = Form(""),
    year: str = Form(""),
    db: Session = Depends(get_db),
):
    ctx = _load_ctx(request)
    current_user = get_current_user(db, ctx.user_id)
    if not current_user:
        return _redirect("/ui/system/users")
    hid = _parse_uuid(holiday_id)
    if not hid:
        return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&err=ID+invalido")
    h = db.get(Holiday, hid)
    if not h:
        return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&err=Feriado+no+existe")
    branch = db.get(Branch, h.branch_id)
    if branch:
        cid = _parse_uuid(company_id) or branch.company_id
        if not can_see_company(db, current_user, cid):
            raise HTTPException(403, "Forbidden")
    db.delete(h)
    db.commit()
    return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&ok=Feriado+eliminado")
'''

with open("api/ui.py", "a", encoding="utf-8") as f:
    f.write(content)

print("OK - endpoints agregados")
