from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

DOW_ORDER = ["LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"]


def _read_excel_sheet(xls: pd.ExcelFile, name: str) -> pd.DataFrame | None:
    if name not in xls.sheet_names:
        return None
    df = pd.read_excel(xls, sheet_name=name)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def load_plan(plan_path: Path) -> pd.DataFrame:
    if not plan_path.exists():
        raise FileNotFoundError(f"Plan file not found: {plan_path}")

    if plan_path.suffix.lower() == ".csv":
        df = pd.read_csv(plan_path)
    else:
        df = pd.read_excel(plan_path)

    df.columns = [str(c).strip() for c in df.columns]

    if "fecha" not in df.columns or "shift_id" not in df.columns:
        raise ValueError("Plan must have at least columns: fecha, shift_id")

    df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()
    df["shift_id"] = df["shift_id"].astype(str)
    df["is_libre"] = df["shift_id"].str.upper().eq("LIBRE")
    return df


def inspect_case(case_path: Path) -> Dict[str, pd.DataFrame]:
    if not case_path.exists():
        raise FileNotFoundError(f"Case file not found: {case_path}")

    xls = pd.ExcelFile(case_path)

    dfs: Dict[str, pd.DataFrame] = {}

    for sheet in [
        "Dotacion",
        "PoolTurnos",
        "DemandaUnidad",
        "NecesidadMinimos",
        "ExcepcionesDemanda",
        "Jornadas",
    ]:
        df = _read_excel_sheet(xls, sheet)
        if df is not None:
            dfs[sheet] = df

    return dfs


def summarize_plan(df_plan: pd.DataFrame) -> Dict:
    out: Dict = {}
    out["rows_total"] = int(len(df_plan))
    out["employees_unique"] = int(df_plan["employee_id"].nunique()) if "employee_id" in df_plan.columns else None
    out["date_min"] = df_plan["fecha"].min().date().isoformat()
    out["date_max"] = df_plan["fecha"].max().date().isoformat()
    out["libre_ratio_total"] = float(df_plan["is_libre"].mean())

    if "dia_semana" in df_plan.columns:
        by_dow = (
            df_plan.groupby("dia_semana")["is_libre"]
            .agg(total="count", libres="sum", ratio="mean")
            .reset_index()
        )
        by_dow["dia_semana"] = pd.Categorical(by_dow["dia_semana"], categories=DOW_ORDER, ordered=True)
        by_dow = by_dow.sort_values("dia_semana")
        out["libres_por_dia_semana"] = [
            {
                "dia_semana": str(r["dia_semana"]),
                "total": int(r["total"]),
                "libres": int(r["libres"]),
                "ratio": float(r["ratio"]),
            }
            for _, r in by_dow.iterrows()
        ]

    if "cargo" in df_plan.columns:
        by_cargo = (
            df_plan.groupby("cargo")["is_libre"]
            .agg(total="count", libres="sum", ratio="mean")
            .reset_index()
            .sort_values("ratio", ascending=False)
        )
        out["libres_por_cargo"] = [
            {
                "cargo": str(r["cargo"]),
                "total": int(r["total"]),
                "libres": int(r["libres"]),
                "ratio": float(r["ratio"]),
            }
            for _, r in by_cargo.iterrows()
        ]

    if "employee_id" in df_plan.columns:
        by_emp = (
            df_plan.groupby("employee_id")["is_libre"]
            .agg(total="count", libres="sum", ratio="mean")
            .reset_index()
            .sort_values("ratio", ascending=False)
        )
        out["empleados_100_libre"] = [
            {
                "employee_id": str(r["employee_id"]),
                "total": int(r["total"]),
                "libres": int(r["libres"]),
                "ratio": float(r["ratio"]),
            }
            for _, r in by_emp[by_emp["ratio"] >= 0.999999].iterrows()
        ]

    return out


def find_pool_gaps(dfs: Dict[str, pd.DataFrame]) -> Dict:
    if "Dotacion" not in dfs or "PoolTurnos" not in dfs:
        return {"available": False, "reason": "Missing Dotacion and/or PoolTurnos sheets"}

    dot = dfs["Dotacion"].copy()
    pool = dfs["PoolTurnos"].copy()

    # Normalize
    for c in ["org_unit_id", "cargo_id", "cargo"]:
        if c in dot.columns:
            dot[c] = dot[c].astype(str).str.strip()
        if c in pool.columns:
            pool[c] = pool[c].astype(str).str.strip()

    if "habilitado" in pool.columns:
        pool = pool[pool["habilitado"].fillna(0).astype(int) == 1]

    # Expected keys: prefer cargo_id, fallback to cargo
    use_cargo_id = "cargo_id" in dot.columns and "cargo_id" in pool.columns

    missing: List[Dict] = []
    if use_cargo_id:
        keys = dot[["employee_id", "org_unit_id", "cargo_id"]].dropna()
        for _, r in keys.iterrows():
            emp = str(r["employee_id"])
            ou = str(r["org_unit_id"])
            cid = str(r["cargo_id"])
            has_any = len(pool[(pool["org_unit_id"] == ou) & (pool["cargo_id"] == cid)]) > 0
            if not has_any:
                missing.append({"employee_id": emp, "org_unit_id": ou, "cargo_id": cid})
    else:
        keys = dot[["employee_id", "org_unit_id", "cargo"]].dropna()
        for _, r in keys.iterrows():
            emp = str(r["employee_id"])
            ou = str(r["org_unit_id"])
            cargo = str(r["cargo"])
            has_any = len(pool[(pool["org_unit_id"] == ou) & (pool["cargo"] == cargo)]) > 0
            if not has_any:
                missing.append({"employee_id": emp, "org_unit_id": ou, "cargo": cargo})

    return {"available": True, "employees_without_pool": missing[:200], "count": len(missing)}


def find_demand_gaps(dfs: Dict[str, pd.DataFrame]) -> Dict:
    # Demand can be in DemandaUnidad or NecesidadMinimos
    demand_sources = []
    if "DemandaUnidad" in dfs:
        demand_sources.append(("DemandaUnidad", dfs["DemandaUnidad"]))
    if "NecesidadMinimos" in dfs:
        demand_sources.append(("NecesidadMinimos", dfs["NecesidadMinimos"]))

    if not demand_sources or "Dotacion" not in dfs:
        return {"available": False, "reason": "Missing demand sheets and/or Dotacion"}

    dot = dfs["Dotacion"].copy()
    dot["org_unit_id"] = dot["org_unit_id"].astype(str).str.strip()
    org_units = sorted(dot["org_unit_id"].dropna().unique().tolist())

    gaps: List[Dict] = []

    for ou in org_units:
        for dow in DOW_ORDER:
            has = False
            for name, df in demand_sources:
                tmp = df.copy()
                if "org_unit_id" not in tmp.columns or "dia_semana" not in tmp.columns:
                    continue
                tmp["org_unit_id"] = tmp["org_unit_id"].astype(str).str.strip()
                tmp["dia_semana"] = tmp["dia_semana"].astype(str).str.strip()

                if "requeridos" in tmp.columns:
                    sub = tmp[(tmp["org_unit_id"] == ou) & (tmp["dia_semana"] == dow)]
                    # consider demand defined if any row exists (even 0), but flag if it's completely absent
                    if len(sub) > 0:
                        has = True
                else:
                    # if no requeridos column, presence of any row counts
                    sub = tmp[(tmp["org_unit_id"] == ou) & (tmp["dia_semana"] == dow)]
                    if len(sub) > 0:
                        has = True

            if not has:
                gaps.append({"org_unit_id": ou, "dia_semana": dow})

    return {"available": True, "missing_demand_weekdays": gaps[:200], "count": len(gaps)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True, help="Path to plan_mensual.(csv|xlsx)")
    ap.add_argument("--case", required=False, help="Path to case.xlsx (optional but recommended)")
    ap.add_argument("--out", required=False, default="diagnostico_libres.json", help="Output json file")
    args = ap.parse_args()

    plan_path = Path(args.plan)
    df_plan = load_plan(plan_path)

    report: Dict = {"plan_path": str(plan_path)}
    report["plan_summary"] = summarize_plan(df_plan)

    if args.case:
        case_path = Path(args.case)
        dfs = inspect_case(case_path)
        report["case_path"] = str(case_path)
        report["pool_gaps"] = find_pool_gaps(dfs)
        report["demand_gaps"] = find_demand_gaps(dfs)
    else:
        report["case_path"] = None
        report["pool_gaps"] = {"available": False, "reason": "No case.xlsx provided"}
        report["demand_gaps"] = {"available": False, "reason": "No case.xlsx provided"}

    out_path = Path(args.out)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()