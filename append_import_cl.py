# Corre desde la raiz del proyecto: python append_import_cl.py
# Agrega el endpoint /holidays/import-cl y el modelo HolidayCl a ui.py

endpoint = '''

# =========================
# IMPORTAR FERIADOS CHILE
# =========================

@router.post("/holidays/import-cl")
def ui_holidays_import_cl(
    request: Request,
    company_id: str = Form(...),
    branch_id: str = Form(...),
    year: str = Form(...),
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
        yr = int(year)
    except ValueError:
        return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&err=Año+invalido")

    # Cargar desde catalogo nacional
    from api.holiday_models import HolidayCl
    nacionales = (
        db.query(HolidayCl)
        .filter(
            HolidayCl.nacional == True,
            HolidayCl.fecha >= date(yr, 1, 1),
            HolidayCl.fecha <= date(yr, 12, 31),
        )
        .all()
    )
    added = 0
    skipped = 0
    for h in nacionales:
        exists = db.query(Holiday).filter(Holiday.branch_id == bid, Holiday.fecha == h.fecha).first()
        if exists:
            skipped += 1
            continue
        db.add(Holiday(
            branch_id=bid,
            fecha=h.fecha,
            nombre=h.nombre,
            irrenunciable=h.irrenunciable,
        ))
        added += 1
    db.commit()
    msg = f"Importados+{added}+feriados"
    if skipped:
        msg += f"+({skipped}+ya+existian)"
    return _redirect(f"/ui/holidays?company_id={company_id}&branch_id={branch_id}&year={year}&ok={msg}")
'''

with open("api/ui.py", "a", encoding="utf-8") as f:
    f.write(endpoint)

print("OK - endpoint import-cl agregado")
