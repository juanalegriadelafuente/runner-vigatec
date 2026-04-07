# src/solve.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set
import argparse
import math
import re
import datetime as dt

import pandas as pd
from ortools.sat.python import cp_model

SOLVER_VERSION = "v10.6"

# Cambios v10.3:
# - REDISEÑO: Colaciones v5 - basado en excedente real de brechas del solver.
#   Ya no reconstruye demanda internamente. Usa directamente cubiertos_personas
#   y requeridos_min_personas del reporte de brechas para saber EXACTAMENTE
#   dónde sobra gente (excedente >= 1 = alguien puede salir a colación).
# - Tracking de colaciones en curso + anti-solapamiento (100M penalty).
# - Si excedente < 1 en todos los slots, elige el de menor daño (forzado legal).
# - Elimina toda la lógica de "demanda interpolada" y "piso móvil".

# Cambios v10.2:
# - FIX CRÍTICO: El mínimo operativo NUNCA puede ser 0 (siempre al menos 1 persona)
#   Esto corrige el bug donde dos personas podían tomar colación al mismo tiempo
#   dejando sin cobertura la operación
# - MEJORA: Logging mejorado para diagnóstico de asignaciones forzadas

# Cambios v10.1:
# - MEJORA: Colaciones ahora usan demanda interpolada si no hay slot exacto
# - MEJORA: Si no hay demanda definida, usa dotacion - 1 como mínimo (siempre queda alguien)
# - MEJORA: Orden de asignación por hora de entrada (primero en entrar, primero en comer)
# - MEJORA: Más logging para diagnóstico

# Cambios v10.0:
# - FEATURE: Asignación inteligente de colaciones post-optimización (AUTOMÁTICO).
#   Se detecta automáticamente si hay turnos con colación (ej: S_0730_1600_60).
#   Las colaciones se asignan DESPUÉS de resolver, respetando:
#   - Art. 34 CT Chile: colación entre hora 3 y 5 desde inicio del turno
#   - Mínimo operativo: NUNCA baja de la dotación mínima requerida
#   - Coordinación: escalona automáticamente cuando la dotación está justa
#   - Preferencia: prioriza horarios cercanos a 12:00-13:00
#   No requiere parámetro en el Excel. Se activa solo si hay turnos con _XX al final.

# Cambios v9.9:
# - FIX: Reporte de brechas ahora cuenta cubiertos correctamente cuando cargo == "__ALL__" (demanda_unidad mode).
#   Antes filtraba plan_day por cargo="__ALL__" que nunca existe en el plan, resultando en cubiertos=0 siempre.

# Cambios v9.8:
# - FIX: Etapa 2c ahora se ejecuta correctamente (indentación corregida).
# - FIX: Etapa 1c ahora usa 'horizon_dates' en vez de 'dates' (variable inexistente).
# - FIX: Descanso mínimo en frontera PlanPrevio ya no se multiplica x60 dos veces.

# Cambios v9.7-fix:
# - FIX: cerrar_dias_sin_demanda ahora default=0 (permisivo). Antes era 1 y cerraba días como LIBRE
#   si la demanda no estaba perfectamente configurada para cada (ou, cargo, fecha).
# - FIX: Type hint de solve_case corregido (retorna 3 DataFrames, no 2).

# Cambios v9.5:
# - Etapa 2a ahora usa objetivo lexicográfico en UNA sola corrida: minimiza (semanas-persona con déficit, luego minutos bajo contrato).
#   Esto reduce UNKNOWN en 2b/3 y empuja a cumplir contrato sin depender de sufijos de turno.
# - Etapa 2b se deja como no-ejecutada (se reporta vacía) porque el refinamiento ya ocurre en 2a.

# Cambios v9:
#
# Cambios v9.1:
# - Fix: evita evaluar expresiones CP-SAT en un `if` (RecursionError). Etapa 2c ahora usa `if deficit_terms:`.
#
# - Etapa 2b ahora es opcional: si 2a logra 0 shortfalls, se fuerza under==0 y se salta 2b (evita UNKNOWN).
# - Si 2b queda UNKNOWN, se usa la mejor cota observable desde 2a y se continúa (no infeasible).
# - Diagnóstico: 2b se reporta como vacío cuando se salta.

# ═══════════════════════════════════════════════════════════════════════════════
# MÓDULO DE COLACIONES INTELIGENTES v7
# ═══════════════════════════════════════════════════════════════════════════════
# POST-PROCESO basado en BRECHAS del solver.
#
# v6 FIXES:
# - Agrega brechas por (fecha, OU, slot) sumando cubiertos y req_min.
#   Antes usaba min() del excedente entre cargos, lo que bloqueaba colaciones
#   cuando un cargo tenía exc=0 aunque el total de personas sobrara.
# - Lookup por OU del empleado: cada persona evalúa el excedente de SU OU.
# - Scoring 100% lexicográfico con levels separados por 1B/1M/1K:
#   Level 0: Anti-solapamiento (1B * col_concurrentes)
#   Level 1: Anti-déficit (1M * max(0, 1 - exc_disponible))
#   Level 2: Preferir más excedente (-1K * exc_disponible)
#   Level 3: Cercanía al mediodía (dist_minutos / 60)
#   Noon distance NUNCA puede vencer anti-solapamiento ni anti-déficit.

from collections import defaultdict
from dataclasses import dataclass

_COLACION_SLOT_MIN = 30
_COLACION_PREF_INICIO = 12 * 60
# Art. 34 CT Chile: "La jornada de trabajo se dividirá en dos partes,
# dejándose entre ellas, a lo menos, el tiempo de media hora para la colación."
# No restringe cuándo dentro de la jornada. El empleador decide.
# Margen: 30 min desde inicio y 30 min antes del fin (operativo, no legal).
_COLACION_MARGEN_INICIO = 30   # minutos desde inicio del turno
_COLACION_MARGEN_FIN = 30      # minutos antes del fin del turno


@dataclass
class _TurnoColacion:
    employee_id: str
    fecha: str
    shift_id: str
    org_unit_id: str
    inicio_min: int
    fin_min: int
    duracion_colacion: int
    ventana_inicio: int
    ventana_fin: int


def _col_to_min(hhmm: str) -> Optional[int]:
    if not hhmm or not isinstance(hhmm, str):
        return None
    hhmm = hhmm.strip()
    if ":" not in hhmm:
        return None
    parts = hhmm.split(":")
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]) * 60 + int(parts[1])
    except ValueError:
        return None


def _col_min_to_hhmm(minutes: int) -> str:
    if minutes is None:
        return ""
    minutes = int(minutes) % 1440
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _col_parse_break_minutes(shift_id: str) -> int:
    if not shift_id or not isinstance(shift_id, str):
        return 0
    shift_id = shift_id.strip().upper()
    if not shift_id.startswith("S_"):
        return 0
    parts = shift_id.split("_")
    if len(parts) >= 4 and parts[3].isdigit():
        return int(parts[3])
    return 0


def _col_calcular_ventana(inicio: int, fin: int, dur_col: int) -> Tuple[int, int]:
    """
    Ventana de colación = toda la jornada con margen operativo.
    Art. 34 CT Chile no restringe cuándo, solo que exista.
    """
    fin_cmp = fin if fin >= inicio else fin + 1440
    vi = inicio + _COLACION_MARGEN_INICIO
    vf = fin_cmp - dur_col - _COLACION_MARGEN_FIN
    vf = max(vf, vi)  # Asegurar que vf >= vi
    return vi % 1440, vf % 1440


def _col_slots_en_ventana(vi: int, vf: int, sm: int) -> List[int]:
    slots = []
    if vf >= vi:
        t = vi
        while t <= vf:
            slots.append(t % 1440)
            t += sm
    else:
        t = vi
        while t < 1440:
            slots.append(t % 1440)
            t += sm
        t = 0
        while t <= vf:
            slots.append(t % 1440)
            t += sm
    return slots


def _col_build_excedente_map(
    brechas_df: pd.DataFrame,
) -> Dict[Tuple[str, str, int], Tuple[int, int]]:
    """
    Construye mapa de (cubiertos_total, req_min_total) por (fecha, OU, slot).
    
    CLAVE v6: SUMA cubiertos y req_min por OU (across cargos).
    Esto refleja el total REAL de personas en la tienda vs el mínimo total.
    Un admin en la tienda ES una persona que mantiene la operación abierta,
    aunque su cargo específico tenga su propio requerimiento.
    
    Returns: Dict[(fecha, ou, slot_min)] = (total_cubiertos, total_req_min)
    """
    # Acumular por (fecha, ou, slot)
    agg: Dict[Tuple[str, str, int], List[int]] = defaultdict(lambda: [0, 0])
    
    if brechas_df is None or brechas_df.empty:
        return {}
    
    for _, row in brechas_df.iterrows():
        fecha = str(row.get("fecha", ""))[:10]
        ou = str(row.get("org_unit_id", "")).strip().upper()
        tramo = str(row.get("tramo_inicio", "")).strip()
        cubiertos = int(row.get("cubiertos_personas", 0) or 0)
        req_min = int(row.get("requeridos_min_personas", 0) or 0)
        
        sm = _col_to_min(tramo)
        if sm is None:
            continue
        
        key = (fecha, ou, sm)
        agg[key][0] += cubiertos
        agg[key][1] += req_min
    
    return {k: tuple(v) for k, v in agg.items()}


def _asignar_colaciones_inteligente(
    plan: pd.DataFrame,
    shift_times: Dict[str, Tuple[str, str]],
    brechas_df: pd.DataFrame,
    slot_min: int = _COLACION_SLOT_MIN,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Asigna colaciones v6 basándose en excedente real de brechas, por OU.

    ALGORITMO v6:
    1. Desde brechas_df: SUM cubiertos y req_min por (fecha, OU, slot).
       excedente_base = sum_cubiertos - sum_req_min.
    2. Dotación BASE como fallback (slots sin brechas).
    3. Tracking: col_asignadas[fecha, ou, slot] cuenta colaciones ya dadas.
       excedente_disponible = excedente_base - col_asignadas.
    4. Scoring LEXICOGRÁFICO ESTRICTO:
       1B * solapamiento + 1M * deficit + (-1K * excedente) + dist_noon
       → Nunca la preferencia horaria vence la seguridad operativa.
    """
    plan = plan.copy()
    if 'colacion_inicio' not in plan.columns:
        plan['colacion_inicio'] = ''

    # ─── Excedente desde brechas (sumado por OU) ─────────────────────────────
    brecha_map = _col_build_excedente_map(brechas_df)
    # brecha_map[(fecha, ou, slot)] = (total_cubiertos, total_req_min)
    
    if verbose:
        print(f"[COLACIONES v7] Brechas mapeadas: {len(brecha_map)} (fecha,OU,slot)")
        # Muestra
        for (f, ou, s), (cub, req) in list(brecha_map.items())[:5]:
            print(f"  {f} {ou} {_col_min_to_hhmm(s)}: cub={cub} req={req} exc={cub-req}")

    # ─── Dotación BASE como fallback ──────────────────────────────────────────
    dotacion_base: Dict[Tuple[str, str, int], int] = defaultdict(int)
    for _, row in plan.iterrows():
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        ou = str(row.get('org_unit_id', '')).strip().upper()
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
        st = shift_times.get(sid)
        if not st:
            continue
        ss, ee = _col_to_min(st[0]), _col_to_min(st[1])
        if ss is None or ee is None:
            continue
        ee_cmp = ee if ee >= ss else ee + 1440
        t = ss
        while t < ee_cmp:
            dotacion_base[(fecha, ou, t % 1440)] += 1
            t += slot_min

    # ─── Tracking de colaciones asignadas por (fecha, OU, slot) ───────────────
    col_asignadas: Dict[Tuple[str, str, int], int] = defaultdict(int)

    def _get_excedente(fecha: str, ou: str, slot: int) -> int:
        """Excedente disponible = (cubiertos - req_min) - col_ya_asignadas."""
        key3 = (fecha, ou, slot)
        if key3 in brecha_map:
            cub, req = brecha_map[key3]
            exc_base = cub - req
        else:
            # Fallback: dotacion_base - 1
            exc_base = max(0, dotacion_base.get(key3, 0) - 1)
        return exc_base - col_asignadas.get(key3, 0)

    # ─── Identificar empleados ────────────────────────────────────────────────
    turnos: List[Tuple[int, _TurnoColacion]] = []
    for idx, row in plan.iterrows():
        if row.get('colacion_inicio', ''):
            continue
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        emp = str(row.get('employee_id', ''))
        ou = str(row.get('org_unit_id', '')).strip().upper()
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
        dur = _col_parse_break_minutes(sid)
        if dur <= 0:
            continue
        st = shift_times.get(sid)
        if not st:
            continue
        ss, ee = _col_to_min(st[0]), _col_to_min(st[1])
        if ss is None or ee is None:
            continue
        ee_cmp = ee if ee >= ss else ee + 1440
        vi, vf = _col_calcular_ventana(ss, ee_cmp, dur)
        if vi > vf:
            vi = vf = (ss + _COLACION_MARGEN_INICIO) % 1440
        turnos.append((idx, _TurnoColacion(
            employee_id=emp, fecha=fecha, shift_id=sid, org_unit_id=ou,
            inicio_min=ss, fin_min=ee_cmp, duracion_colacion=dur,
            ventana_inicio=vi, ventana_fin=vf,
        )))

    if not turnos:
        return plan
    if verbose:
        print(f"[COLACIONES v7] {len(turnos)} turnos necesitan colación")

    # ─── Ordenar: ventana estrecha primero ────────────────────────────────────
    def _sort_key(item):
        _, i = item
        sz = (i.ventana_fin - i.ventana_inicio + 1440) % 1440
        return (max(sz, 1), i.inicio_min)
    turnos.sort(key=_sort_key)

    # ─── Evaluar y asignar ────────────────────────────────────────────────────

    def _evaluar(fecha: str, ou: str, slot_ini: int, dur: int):
        """
        Returns: (min_exc_disponible, max_col_en_slot, min_dotacion, dist_noon)
        
        min_dotacion = mínima cantidad de personas trabajando en los sub-slots
        de la colación (ANTES de descontar esta colación). Esto permite al
        scoring preferir horarios donde hay MÁS gente → mayor colchón.
        """
        min_exc = float('inf')
        max_col = 0
        min_dot = float('inf')
        tt = slot_ini
        for _ in range(dur // slot_min):
            s = tt % 1440
            exc = _get_excedente(fecha, ou, s)
            min_exc = min(min_exc, exc)
            max_col = max(max_col, col_asignadas.get((fecha, ou, s), 0))
            # Dotación base en este slot (personas trabajando, sin descontar cols)
            dot = dotacion_base.get((fecha, ou, s), 0)
            min_dot = min(min_dot, dot)
            tt += slot_min
        dist = abs(slot_ini - _COLACION_PREF_INICIO)
        if dist > 720:
            dist = 1440 - dist
        e = int(min_exc) if min_exc != float('inf') else -999
        d = int(min_dot) if min_dot != float('inf') else 0
        return e, max_col, d, dist / 60.0

    def _score(exc: int, col: int, dotacion: int, dist: float) -> float:
        """
        Score LEXICOGRÁFICO ESTRICTO v7. Menor = mejor.
        
        Level 0: Anti-solapamiento      → 1B * col_concurrentes
                 JAMÁS poner 2 colaciones al mismo tiempo.
        Level 1: Anti-déficit           → 1M * max(0, 1 - exc)
                 JAMÁS bajar del mínimo operativo.
        Level 2: Máxima dotación        → -100K * dotacion     ★ NUEVO
                 PREFERIR horarios con MÁS gente trabajando.
                 Esto empuja colaciones al período de overlap
                 (mañana+tarde) donde hay máximo personal.
        Level 3: Más excedente          → -1K * exc
        Level 4: Cercanía al mediodía   → dist (0 a ~12)
                 Desempate suave, NUNCA vence niveles superiores.
        """
        return (
            col     * 1_000_000_000 +          # L0: NUNCA solapar
            max(0, 1 - exc) * 1_000_000 +      # L1: NUNCA bajar del mínimo
            (-dotacion) * 100_000 +             # L2: MÁS gente = mejor momento ★
            (-exc)  * 1_000 +                   # L3: preferir más margen
            dist                                # L4: mediodía (desempate)
        )

    def _aplicar(fecha: str, ou: str, slot_ini: int, dur: int):
        tt = slot_ini
        for _ in range(dur // slot_min):
            s = tt % 1440
            col_asignadas[(fecha, ou, s)] += 1
            tt += slot_min

    ok = 0
    forzadas = 0

    for idx, info in turnos:
        slots = _col_slots_en_ventana(info.ventana_inicio, info.ventana_fin, slot_min)
        best_slot, best_sc = None, float('inf')
        best_exc, best_col = -999, 0

        for s in slots:
            exc, col, dot, dist = _evaluar(info.fecha, info.org_unit_id, s, info.duracion_colacion)
            sc = _score(exc, col, dot, dist)
            if sc < best_sc:
                best_sc, best_slot, best_exc, best_col = sc, s, exc, col

        if best_slot is None:
            best_slot = info.ventana_inicio

        plan.at[idx, 'colacion_inicio'] = _col_min_to_hhmm(best_slot)
        _aplicar(info.fecha, info.org_unit_id, best_slot, info.duracion_colacion)

        if best_exc >= 1 and best_col == 0:
            ok += 1
            if verbose:
                print(f"[COLACIONES v7] ✓ {info.employee_id[:15]}: "
                      f"{_col_min_to_hhmm(best_slot)} (exc={best_exc})")
        else:
            forzadas += 1
            if verbose:
                motivo = []
                if best_exc < 1:
                    motivo.append(f"exc={best_exc}")
                if best_col > 0:
                    motivo.append(f"overlap={best_col}")
                print(f"[COLACIONES v7] ⚠️ {info.employee_id[:15]}: "
                      f"{_col_min_to_hhmm(best_slot)} ({', '.join(motivo)})")

    if verbose:
        print(f"[COLACIONES v7] Resultado: {ok} OK, {forzadas} forzadas "
              f"(total: {len(turnos)})")

    return plan


def _generar_diagnostico_colaciones(
    plan: pd.DataFrame,
    shift_times: Dict[str, Tuple[str, str]],
    brechas_df: pd.DataFrame,
    slot_min: int = _COLACION_SLOT_MIN,
) -> pd.DataFrame:
    """Diagnóstico v6: excedente por OU post-colaciones."""
    brecha_map = _col_build_excedente_map(brechas_df)

    dotacion_base: Dict[Tuple[str, str, int], int] = defaultdict(int)
    for _, row in plan.iterrows():
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        ou = str(row.get('org_unit_id', '')).strip().upper()
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
        st = shift_times.get(sid)
        if not st:
            continue
        ss, ee = _col_to_min(st[0]), _col_to_min(st[1])
        if ss is None or ee is None:
            continue
        ee_cmp = ee if ee >= ss else ee + 1440
        t = ss
        while t < ee_cmp:
            dotacion_base[(fecha, ou, t % 1440)] += 1
            t += slot_min

    en_colacion: Dict[Tuple[str, str, int], int] = defaultdict(int)
    for _, row in plan.iterrows():
        ci = row.get('colacion_inicio', '')
        if not ci:
            continue
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        ou = str(row.get('org_unit_id', '')).strip().upper()
        dur = _col_parse_break_minutes(sid)
        if dur <= 0:
            continue
        cm = _col_to_min(ci)
        if cm is None:
            continue
        t = cm
        for _ in range(dur // slot_min):
            en_colacion[(fecha, ou, t % 1440)] += 1
            t += slot_min

    rows = []
    all_keys = set(dotacion_base.keys()) | set(k for k in brecha_map.keys())
    for (fecha, ou, slot) in sorted(all_keys):
        base = dotacion_base.get((fecha, ou, slot), 0)
        ec = en_colacion.get((fecha, ou, slot), 0)
        real = base - ec
        if (fecha, ou, slot) in brecha_map:
            cub, req = brecha_map[(fecha, ou, slot)]
        else:
            cub, req = base, max(1, base - 1)
        exc_base = cub - req
        exc_post = exc_base - ec
        rows.append({
            'fecha': fecha, 'org_unit': ou,
            'slot': _col_min_to_hhmm(slot),
            'requeridos_min': req, 'cubiertos_brechas': cub,
            'dotacion_base': base, 'en_colacion': ec,
            'dotacion_real': real,
            'excedente_brechas': exc_base,
            'excedente_post_colacion': exc_post,
            'alerta': 'BAJO_MINIMO' if real < req else '',
        })
    return pd.DataFrame(rows)


DOW_MAP = {
    0: "LUN",
    1: "MAR",
    2: "MIE",
    3: "JUE",
    4: "VIE",
    5: "SAB",
    6: "DOM",
}

ANY_CARGO = "__ALL__"

RULE_OPEN_CARGO = "REGLA_APERTURA_CARGO"
RULE_CLOSE_CARGO = "REGLA_CIERRE_CARGO"

__VERSION__ = "v9.9"
DOW_MAP_REV = {v: k for k, v in DOW_MAP.items()}

# -------------------------
# Helpers base
# -------------------------
def _as_date(x) -> pd.Timestamp:
    return pd.to_datetime(x).normalize()

def _get_param(dfs: Dict[str, pd.DataFrame], key: str):
    df = dfs.get("Parametros")
    if df is None or df.empty:
        raise ValueError("Falta hoja 'Parametros'")
    if "parametro" not in df.columns or "valor" not in df.columns:
        raise ValueError("Hoja 'Parametros' debe tener columnas: parametro, valor")
    row = df.loc[df["parametro"].astype(str) == key]
    if row.empty:
        raise ValueError(f"Falta parámetro '{key}' en hoja Parametros")
    return row.iloc[0]["valor"]



def _get_param_optional(dfs: Dict[str, pd.DataFrame], key: str, default: Any = None):
    '''Obtiene un parámetro desde la hoja Parametros si existe; si no, retorna default.'''
    df = dfs.get("Parametros")
    if df is None or df.empty:
        return default
    if "parametro" not in df.columns or "valor" not in df.columns:
        return default
    row = df.loc[df["parametro"].astype(str) == key]
    if row.empty:
        return default
    val = row.iloc[0]["valor"]
    try:
        import pandas as _pd
        if _pd.isna(val):
            return default
    except Exception:
        pass
    return val

def _date_horizon(dfs: Dict[str, pd.DataFrame]) -> List[pd.Timestamp]:
    start = _as_date(_get_param(dfs, "fecha_inicio_mes"))
    weeks = int(_get_param(dfs, "semanas"))
    days = weeks * 7
    return [start + pd.Timedelta(days=i) for i in range(days)]

def _norm_keys(df: pd.DataFrame, cols_upper: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for c in cols_upper:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    return df

def _norm_hhmm(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    if len(s.split(":")) >= 2:
        parts = s.split(":")
        return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    return ""


def _norm_empid(x) -> str:
    """Normaliza employee_id (RUT) para matching consistente entre hojas.
    - Quita puntos, espacios, y guiones raros.
    - Inserta guion antes del DV si no existe.
    - NaN/None/'' => ''.
    """
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
    except Exception:
        pass
    s = str(x).strip().upper()
    if s in ("", "NAN", "NONE", "NULL"):
        return ""
    # normaliza guiones unicode a '-'
    s = s.replace("–", "-").replace("—", "-").replace("‐", "-").replace("‑", "-").replace("‒", "-")
    # quita puntos y espacios
    s = s.replace(".", "").replace(" ", "").replace("\u00A0", "")
    # deja solo dígitos y K (y elimina guiones para reinsertar)
    s2 = re.sub(r"[^0-9K]", "", s)
    if len(s2) < 2:
        return s2
    dv = s2[-1]
    num = s2[:-1]
    if num.isdigit():
        return f"{num}-{dv}"
    return s2

def _to_min(hhmm: str) -> Optional[int]:
    s = _norm_hhmm(hhmm)
    if not s:
        return None
    parts = s.split(":")
    if len(parts) < 2:
        return None
    h = int(parts[0])
    m = int(parts[1])
    return h * 60 + m



def _min_to_hhmm(minutes: int) -> str:
    """Convierte minutos desde 00:00 a string HH:MM (24h)."""
    if minutes is None:
        return ""
    try:
        minutes = int(minutes)
    except Exception:
        return ""
    if minutes < 0:
        minutes = 0
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def _ranges(start_min: Optional[int], end_min: Optional[int]) -> List[Tuple[int, int]]:
    if start_min is None or end_min is None:
        return []
    if end_min >= start_min:
        return [(start_min, end_min)]
    # cruza medianoche
    return [(start_min, 1440), (0, end_min)]

# -------------------------
# Normalización día de semana (JUE/JUEVES/MIÉ/SÁB/etc.)
# -------------------------
def _norm_dow(s: Any) -> str:
    """
    Acepta: JUE, JUEVES, MIÉ, MIERCOLES, SÁB, SABADO, etc.
    Devuelve: LUN/MAR/MIE/JUE/VIE/SAB/DOM o '' si no se puede.
    """
    if s is None:
        return ""
    t = str(s).strip().upper()
    if t in ("", "NAN"):
        return ""
    t = (
        t.replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
    )
    t3 = t[:3]
    return t3 if t3 in DOW_MAP_REV else ""

# -------------------------
# Turnos: minutos, tiempos, coberturas
# -------------------------
def _shift_minutes_map(cat: pd.DataFrame) -> Dict[str, int]:
    mp: Dict[str, int] = {}
    if cat is None or cat.empty:
        mp["LIBRE"] = 0
        return mp

    for _, r in cat.iterrows():
        sid = str(r.get("shift_id", "")).strip().upper()
        if not sid:
            continue
        raw = pd.to_numeric(r.get("minutos_efectivos", 0), errors="coerce")
        if pd.isna(raw):
            raw = 0
        mp[sid] = int(raw)

    mp.setdefault("LIBRE", 0)
    return mp

def _shift_time_map(cat: pd.DataFrame) -> Dict[str, Tuple[str, str]]:
    mp: Dict[str, Tuple[str, str]] = {}
    if cat is None or cat.empty:
        mp["LIBRE"] = ("00:00", "00:00")
        return mp

    for _, r in cat.iterrows():
        sid = str(r.get("shift_id", "")).strip().upper()
        if not sid:
            continue
        mp[sid] = (_norm_hhmm(r.get("inicio", "")), _norm_hhmm(r.get("fin", "")))
    mp.setdefault("LIBRE", ("00:00", "00:00"))
    return mp

def _shift_span_minutes(shift_id: str, shift_times: Dict[str, Tuple[str, str]]) -> Tuple[Optional[int], Optional[int]]:
    s, e = shift_times.get(shift_id, ("", ""))
    ss = _to_min(s)
    ee = _to_min(e)
    if ss is None or ee is None:
        return None, None
    ee_cmp = ee if ee >= ss else ee + 1440
    return ss, ee_cmp

def _rest_minutes(prev_shift: str, next_shift: str, shift_times: Dict[str, Tuple[str, str]]) -> Optional[int]:
    """Minutos de descanso entre un turno del día D (prev_shift) y el turno del día D+1 (next_shift).

    Importante: si el turno previo cruza medianoche (ej: 19:00-04:00), su término real ocurre en D+1,
    por lo que el descanso real hasta el próximo inicio (en D+1) puede ser MUCHO menor.
    """
    if not prev_shift or prev_shift.strip().upper() in ("LIBRE", "LM", "VAC", "PA"):
        return 10**9
    if not next_shift or next_shift.strip().upper() in ("LIBRE", "LM", "VAC", "PA"):
        return 10**9

    _, prev_end_cmp = _shift_span_minutes(prev_shift, shift_times)   # término desde inicio del día D (puede ser >1440)
    next_start, _ = _shift_span_minutes(next_shift, shift_times)     # inicio desde inicio del día D+1 (0..1439)
    if prev_end_cmp is None or next_start is None:
        return None

    # El próximo turno ocurre en el día siguiente: offset +1440 desde el inicio del día D.
    # Descanso = (1440 + next_start) - prev_end_cmp
    return (1440 + int(next_start)) - int(prev_end_cmp)

def _shift_type_3(shift_id: str, shift_times: Dict[str, Tuple[str, str]]) -> str:
    ss, ee_cmp = _shift_span_minutes(shift_id, shift_times)
    if ss is None or ee_cmp is None:
        return "UNK"
    if ee_cmp > 1440:
        return "NOCHE"
    return "MANANA" if ss < 12 * 60 else "TARDE"



def _match_shift_ids_token(token: str, universe: Set[str], shift_times: Dict[str, Tuple[str, str]]) -> Set[str]:
    """Intenta matchear un shift desde un token 'humano'.

    Soporta:
    - match exacto de shift_id,
    - prefijos (p.ej. 'S_0730_1600' matchea 'S_0730_1600_60'),
    - separadores '-' vs '_',
    - match por horas (cualquier shift del universo con mismo inicio/fin).

    Devuelve set de shift_ids a remover/usar dentro del universo.
    """
    try:
        t = str(token).strip().upper()
    except Exception:
        return set()
    if not t or t in ("NAN", "NONE", "NULL"):
        return set()

    if t in universe:
        return {t}

    # prefijo exacto
    pref = {s for s in universe if isinstance(s, str) and s.startswith(t)}
    if pref:
        return pref

    # prefijo con normalización de separadores
    t2 = t.replace("-", "_")
    pref2 = {s for s in universe if isinstance(s, str) and s.replace("-", "_").startswith(t2)}
    if pref2:
        return pref2

    # match por horas (extrae 2 secuencias de 4 dígitos del token)
    digs = re.findall(r"\d{4}", t)
    if len(digs) >= 2:
        s4, e4 = digs[0], digs[1]
        start = f"{s4[:2]}:{s4[2:]}"
        end = f"{e4[:2]}:{e4[2:]}"
        start_n = _norm_hhmm(start)
        end_n = _norm_hhmm(end)
        out = set()
        for sid in universe:
            if not isinstance(sid, str):
                continue
            ini, fin = shift_times.get(sid, ("", ""))
            if _norm_hhmm(ini) == start_n and _norm_hhmm(fin) == end_n:
                out.add(sid)
        if out:
            return out

    return set()

def _shift_slot_coverage(shift_id: str, shift_times: Dict[str, Tuple[str, str]], slot_min: int) -> List[int]:
    s, e = shift_times.get(shift_id, ("", ""))
    ss = _to_min(s)
    ee = _to_min(e)
    if ss is None or ee is None:
        return []
    rr = _ranges(ss, ee)
    covered = []
    for a0, a1 in rr:
        t = a0
        while t < a1:
            covered.append((t // slot_min) * slot_min)
            t += slot_min
    return sorted(set([c % 1440 for c in covered]))

def _build_required_slots(need_day_uc: pd.DataFrame, slot_min: int) -> Tuple[Dict[int, int], int, int]:
    required_slots: Dict[int, int] = {}
    open_min: Optional[int] = None
    close_min: Optional[int] = None

    if need_day_uc is None or need_day_uc.empty:
        return required_slots, 0, 0

    for _, nr in need_day_uc.iterrows():
        req = int(pd.to_numeric(nr.get("requeridos", 0), errors="coerce") or 0)
        if req <= 0:
            continue

        ws = _to_min(str(nr.get("inicio", "")))
        we = _to_min(str(nr.get("fin", "")))
        if ws is None or we is None:
            continue

        if open_min is None or ws < open_min:
            open_min = ws

        we_cmp = we if we >= ws else we + 1440
        close_min = we_cmp if close_min is None else max(close_min, we_cmp)

        t = ws
        while t < we_cmp:
            key = t % 1440
            required_slots[key] = max(required_slots.get(key, 0), req)
            t += slot_min

    if open_min is None:
        open_min = 0
    if close_min is None:
        close_min = 0

    return required_slots, int(open_min), int(close_min)


def _build_required_slots_dual(need_day_uc: pd.DataFrame, slot_min: int) -> Tuple[Dict[int, int], Dict[int, int], int, int]:
    """
    v3: Construye DOS diccionarios de slots:
    - required_slots_min: mínimos operativos (restricción dura)
    - required_slots_ideal: demanda real/ideal (objetivo blando)
    
    Si requeridos_ideal no existe o es <= requeridos, ideal = mínimo.
    """
    required_slots_min: Dict[int, int] = {}
    required_slots_ideal: Dict[int, int] = {}
    open_min: Optional[int] = None
    close_min: Optional[int] = None

    if need_day_uc is None or need_day_uc.empty:
        return required_slots_min, required_slots_ideal, 0, 0

    for _, nr in need_day_uc.iterrows():
        req_min = int(pd.to_numeric(nr.get("requeridos", 0), errors="coerce") or 0)
        req_ideal = int(pd.to_numeric(nr.get("requeridos_ideal", req_min), errors="coerce") or req_min)
        # Asegurar que ideal >= mínimo
        req_ideal = max(req_ideal, req_min)
        
        if req_min <= 0:
            continue

        ws = _to_min(str(nr.get("inicio", "")))
        we = _to_min(str(nr.get("fin", "")))
        if ws is None or we is None:
            continue

        if open_min is None or ws < open_min:
            open_min = ws

        we_cmp = we if we >= ws else we + 1440
        close_min = we_cmp if close_min is None else max(close_min, we_cmp)

        t = ws
        while t < we_cmp:
            key = t % 1440
            required_slots_min[key] = max(required_slots_min.get(key, 0), req_min)
            required_slots_ideal[key] = max(required_slots_ideal.get(key, 0), req_ideal)
            t += slot_min

    if open_min is None:
        open_min = 0
    if close_min is None:
        close_min = 0

    return required_slots_min, required_slots_ideal, int(open_min), int(close_min)

# -------------------------
# Lectura Jornadas / Restricciones / Ausentismos
# -------------------------
def _load_jornadas(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    jor = dfs.get("Jornadas")
    if jor is None or jor.empty:
        return pd.DataFrame()
    jor = jor.copy()
    if "jornada_id" not in jor.columns:
        raise ValueError("Hoja 'Jornadas' debe tener columna 'jornada_id'")
    jor["jornada_id"] = jor["jornada_id"].astype(str).str.strip().str.upper()
    return jor


def _load_restricciones(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    r = dfs.get("RestriccionesEmpleado")
    if r is None or r.empty:
        return pd.DataFrame()

    r = r.copy()

    # IMPORTANTE: evitar "NAN" por astype(str) cuando vienen vacíos
    r = r.fillna("")

    # Normaliza fechas (mantiene NaT para vacíos; se interpreta como "sin filtro")
    if "fecha" in r.columns:
        try:
            r["fecha"] = pd.to_datetime(r["fecha"], errors="coerce").dt.normalize()
        except Exception:
            pass


    # Normalizamos texto base
    for c in ["tipo", "dia_semana", "valor1", "valor2"]:
        if c in r.columns:
            r[c] = r[c].astype(str).str.strip().str.upper()

    # employee_id con normalización única (RUT)
    if "employee_id" in r.columns:
        r["employee_id"] = r["employee_id"].apply(_norm_empid)

    # Soportar múltiples días (compat): se aceptan en 'dia_semana' y también en valor1/valor2
    # (en algunos casos antiguos DIA_LIBRE_FIJO usa valor1/valor2 para días).
    def _split_dows_from_row(row: pd.Series) -> List[str]:
        parts_src: List[str] = []
        for col in ["dia_semana", "valor1", "valor2"]:
            if col in row.index:
                v = row[col]
                if v is None:
                    continue
                t = str(v).strip().upper()
                if t in ("", "NAN"):
                    continue
                if not re.search(r"\b(LUN|MAR|MIE|JUE|VIE|SAB|DOM)\b", t):
                    continue
                parts_src.append(t)

        if not parts_src:
            return [""]  # sin restricción de día

        t = ",".join(parts_src)
        t = t.replace(";", ",").replace(".", ",").replace("|", ",").replace("/", ",")
        t = ",".join([p for p in t.replace(" ", ",").split(",") if p != ""])
        parts = [p.strip() for p in t.split(",") if p.strip() != ""]
        if not parts:
            return [""]

        out: List[str] = []
        for p in parts:
            d = _norm_dow(p)
            if d:
                out.append(d)

        return out if out else [""]

    r["_dia_list"] = r.apply(_split_dows_from_row, axis=1)
    r = r.explode("_dia_list", ignore_index=True)
    r["dia_semana"] = r["_dia_list"].astype(str)
    r.drop(columns=["_dia_list"], inplace=True)

    if "fecha" in r.columns:
        r["fecha"] = pd.to_datetime(r["fecha"], errors="coerce").dt.date

    return r

# (1) REEMPLAZO: _load_ausentismos(...) por versión nueva (DataFrame diario)
def _load_ausentismos(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Carga ausentismos y los normaliza a filas diarias: (employee_id, ausentismo, fecha).

    Soporta dos formatos:
      1) Hoja 'AusentismoEmpleado' con columnas:
         - employee_id, ausentismo, fecha_inicio, fecha_fin (inclusive)
      2) Hoja 'Ausentismos' legado con columnas:
         - employee_id, ausentismo, fecha
         (o también fecha_inicio/fecha_fin)
    """
    a = dfs.get("AusentismoEmpleado")
    if a is None or a.empty:
        a = dfs.get("Ausentismos")

    if a is None or a.empty:
        return pd.DataFrame()

    a = a.copy()

    # Normaliza texto (evitar 'NAN' y normalizar RUT)
    a = a.fillna("")
    if "employee_id" in a.columns:
        a["employee_id"] = a["employee_id"].apply(_norm_empid)
    if "ausentismo" in a.columns:
        a["ausentismo"] = a["ausentismo"].astype(str).str.strip().str.upper()

    # Caso simple: columna fecha
    has_range = ("fecha_inicio" in a.columns) or ("fecha_fin" in a.columns)
    if "fecha" in a.columns and not has_range:
        a["fecha"] = pd.to_datetime(a["fecha"], errors="coerce").dt.normalize()
        a = a.loc[pd.notna(a["fecha"])].copy()
        return a[["employee_id", "ausentismo", "fecha"]].copy()

    # Caso rango: fecha_inicio / fecha_fin
    if "fecha_inicio" not in a.columns and "fecha" in a.columns:
        a["fecha_inicio"] = a["fecha"]
    if "fecha_fin" not in a.columns and "fecha_inicio" in a.columns:
        a["fecha_fin"] = a["fecha_inicio"]

    if "fecha_inicio" not in a.columns or "fecha_fin" not in a.columns:
        raise ValueError("Hoja de ausentismos debe tener 'fecha' o bien 'fecha_inicio'/'fecha_fin'.")

    a["fecha_inicio"] = pd.to_datetime(a["fecha_inicio"], errors="coerce").dt.normalize()
    a["fecha_fin"] = pd.to_datetime(a["fecha_fin"], errors="coerce").dt.normalize()
    a = a.loc[pd.notna(a["fecha_inicio"]) & pd.notna(a["fecha_fin"])].copy()

    rows = []
    for _, r in a.iterrows():
        emp = _norm_empid(r.get("employee_id", ""))
        code = str(r.get("ausentismo", "")).strip().upper()
        ini = r["fecha_inicio"]
        fin = r["fecha_fin"]
        if not emp or not code:
            continue
        if fin < ini:
            ini, fin = fin, ini
        for d in pd.date_range(ini, fin, freq="D"):
            rows.append((emp, code, pd.to_datetime(d).normalize()))

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=["employee_id", "ausentismo", "fecha"])

def _get_jornada_param(jor_df: pd.DataFrame, jornada_id: str, col: str, default_val: Any) -> Any:
    if jor_df is None or jor_df.empty:
        return default_val
    jornada_id = str(jornada_id).strip().upper()
    row = jor_df.loc[jor_df["jornada_id"].astype(str) == jornada_id]
    if row.empty or col not in row.columns:
        return default_val
    v = row.iloc[0][col]
    if pd.isna(v):
        return default_val
    return v

# -------------------------
# Pool index (allowed shifts por OU/cargo/día)
# -------------------------
def _build_pool_index(pool: pd.DataFrame) -> Dict[Tuple[str, str, str], List[str]]:
    idx: Dict[Tuple[str, str, str], List[str]] = {}
    if pool is None or pool.empty:
        return idx

    for _, r in pool.iterrows():
        hab = pd.to_numeric(r.get("habilitado", 1), errors="coerce")
        hab = int(hab) if pd.notna(hab) else 1
        if hab != 1:
            continue

        ou = str(r.get("org_unit_id", "")).strip().upper()
        cargo = str(r.get("cargo_id", "")).strip().upper()
        dow = _norm_dow(r.get("dia_semana", ""))
        sid = str(r.get("shift_id", "")).strip().upper()

        if not ou or not cargo or not dow or not sid:
            continue

        idx.setdefault((ou, cargo, dow), []).append(sid)

    return idx

# -------------------------
# Solver CP-SAT
# -------------------------
def solve_case(dfs: Dict[str, pd.DataFrame], out_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out_dir.mkdir(parents=True, exist_ok=True)

    cat = dfs["CatalogoTurnos"].copy()
    dot = _norm_keys(dfs["Dotacion"], ["org_unit_id", "cargo_id", "employee_id"])
    if "employee_id" in dot.columns:
        dot["employee_id"] = dot["employee_id"].apply(_norm_empid)
    pool = _norm_keys(dfs["PoolTurnos"], ["org_unit_id", "cargo_id", "dia_semana", "shift_id"])
    # Demanda principal (backward compatible):
    # - Si existe hoja DemandaUnidad y tiene filas => demanda por unidad (cargo indiferente).
    # - Si no existe / vacía => se usa NecesidadMinimos (demanda por cargo, comportamiento antiguo).
    demanda_unidad = dfs.get("DemandaUnidad")
    if demanda_unidad is not None:
        demanda_unidad = _norm_keys(demanda_unidad, ["org_unit_id", "dia_semana"])
        if "dia_semana" in demanda_unidad.columns:
            demanda_unidad["dia_semana"] = demanda_unidad["dia_semana"].apply(_norm_dow)
        # v3: Si existe columna requeridos_ideal, usarla; si no, copiar requeridos
        if "requeridos_ideal" not in demanda_unidad.columns:
            demanda_unidad["requeridos_ideal"] = demanda_unidad.get("requeridos", 0)
        else:
            # Fallback: si requeridos_ideal es NaN o vacío, usar requeridos
            demanda_unidad["requeridos_ideal"] = demanda_unidad["requeridos_ideal"].fillna(demanda_unidad["requeridos"])
    else:
        demanda_unidad = pd.DataFrame()

    need = _norm_keys(dfs["NecesidadMinimos"], ["org_unit_id", "cargo_id", "dia_semana"])
    if need is not None and not need.empty and "dia_semana" in need.columns:
        need["dia_semana"] = need["dia_semana"].apply(_norm_dow)

    use_demanda_unidad = demanda_unidad is not None and (not demanda_unidad.empty)

    # Precalcula apertura/cierre por unidad y día semana desde DemandaUnidad (source of truth cuando existe)
    open_start_by_ou_dow: Dict[Tuple[str, str], int] = {}
    close_end_by_ou_dow: Dict[Tuple[str, str], int] = {}
    if use_demanda_unidad:
        du = demanda_unidad.copy()
        # normaliza columnas esperadas
        for c in ["inicio", "fin", "requeridos"]:
            if c not in du.columns:
                raise ValueError("Hoja 'DemandaUnidad' debe tener columnas: org_unit_id, dia_semana, inicio, fin, requeridos")
        du["inicio_min"] = du["inicio"].astype(str).apply(_to_min)
        du["fin_min"] = du["fin"].astype(str).apply(_to_min)
        du["requeridos"] = pd.to_numeric(du["requeridos"], errors="coerce").fillna(0).astype(int)
        du = du.loc[(du["requeridos"] > 0) & pd.notna(du["inicio_min"]) & pd.notna(du["fin_min"])].copy()
        for (ou, dow), g in du.groupby(["org_unit_id", "dia_semana"]):
            try:
                open_start_by_ou_dow[(ou, dow)] = int(g["inicio_min"].min())
                close_end_by_ou_dow[(ou, dow)] = int(g["fin_min"].max())
            except Exception:
                continue

    # Minimos por cargo dentro de la unidad (mix) - opcional, nunca hace infeasible: penaliza under.
    min_cargo_unidad = dfs.get("MinimosCargoUnidad")
    if min_cargo_unidad is not None:
        min_cargo_unidad = _norm_keys(min_cargo_unidad, ["org_unit_id", "cargo_id", "dia_semana"])
        if "dia_semana" in min_cargo_unidad.columns:
            min_cargo_unidad["dia_semana"] = min_cargo_unidad["dia_semana"].apply(_norm_dow)
    else:
        min_cargo_unidad = pd.DataFrame()

    rules = dfs.get("Reglas", pd.DataFrame())
    exc = dfs.get("ExcepcionesDemanda", pd.DataFrame())
    if exc is None:
        exc = pd.DataFrame()
    exc = _norm_keys(exc, ["org_unit_id", "cargo_id"])

    jornadas_df = _load_jornadas(dfs)
    restr_df = _load_restricciones(dfs)

    # Reglas parametrizables de apertura/cierre por CARGO (sin nuevas hojas):
    # Se configuran en RestriccionesEmpleado con:
    # - tipo = REGLA_APERTURA_CARGO / REGLA_CIERRE_CARGO
    # - valor1 = cargo_id (texto)
    # - valor2 = org_unit_id opcional (vacío => todas las unidades)
    # - dia_semana opcional (vacío => todos)
    # - fecha opcional (vacío => todo el horizonte)
    # - hard = 1 => estricto (si aplica y no se puede, el solve debe fallar)
    role_cargo_rules: List[Dict[str, Any]] = []
    if restr_df is not None and not restr_df.empty:
        for _, rr in restr_df.iterrows():
            rtype = str(rr.get("tipo", "")).strip().upper()
            if rtype not in (RULE_OPEN_CARGO, RULE_CLOSE_CARGO):
                continue
            cargo_req = str(rr.get("valor1", "")).strip().upper()
            if not cargo_req:
                continue
            ou_req = str(rr.get("valor2", "")).strip().upper()
            dow_req = _norm_dow(str(rr.get("dia_semana", "")).strip().upper())
            fecha_req = rr.get("fecha", None)
            hard_raw = rr.get("hard", 1)
            hard_req = int(pd.to_numeric(hard_raw, errors="coerce") or 0)
            # Por requerimiento comercial: si existe una regla REGLA_* en la hoja, por defecto es HARD
            # (salvo que explícitamente venga hard=0)
            if pd.isna(pd.to_numeric(hard_raw, errors="coerce")):
                hard_req = 1
            pen_raw = rr.get("penalizacion", 0)
            pen_req = int(pd.to_numeric(pen_raw, errors="coerce") or 0)
            role_cargo_rules.append({
                "tipo": rtype,
                "cargo_id": cargo_req,
                "org_unit_id": ou_req,
                "dia_semana": dow_req,
                "fecha": fecha_req,
                "hard": hard_req,
                "penalizacion": pen_req,
            })

    horizon = _date_horizon(dfs)
    weeks = int(_get_param(dfs, "semanas"))


    # Parametros opcionales (no rompen si no existen)
    # Permite relajar la demanda (under) en etapas de contrato. 0 = no relajar.
    demanda_epsilon = int(pd.to_numeric(_get_param_optional(dfs, "demanda_epsilon", 0), errors="coerce") or 0)
    # v9.6: Control fino para relajar demanda al optimizar contrato
    # - demanda_epsilon_slots: tolerancia en *cantidad de slots* con déficit (preferencia 0)
    # - demanda_epsilon_weighted: tolerancia en déficit ponderado (minutos * peso crítico/no crítico)
    demanda_epsilon_slots = int(_get_param_optional(dfs, "demanda_epsilon_slots", 0) or 0)
    demanda_epsilon_weighted = int(_get_param_optional(dfs, "demanda_epsilon_weighted", demanda_epsilon) or 0)

    # v9.6: Si un día no tiene curva (req=0), por defecto se considera "cerrado" (solo LIBRE)
    # FIX v9.7: default=0 (permisivo) para evitar cerrar días cuando la demanda no está perfectamente configurada
    cerrar_dias_sin_demanda = int(_get_param_optional(dfs, "cerrar_dias_sin_demanda", 0) or 0)

    # Si contrato_max_min_semana viene vacío en alguna dotación, usamos este default (en horas).
    # Esto te permite pasar de 44 a 42 sin re-editar cada fila si el caso lo amerita.
    default_weekly_hours = float(pd.to_numeric(_get_param_optional(dfs, "default_weekly_hours", 44), errors="coerce") or 44)
    default_contract_week_min = int(round(default_weekly_hours * 60))

    # Desempate para preferir turnos con más minutos efectivos (independiente de sufijos _30/_60).
    # 1 = habilitado, 0 = deshabilitado.
    prefer_long_shifts = int(pd.to_numeric(_get_param_optional(dfs, "prefer_long_shifts", 1), errors="coerce") or 1)

    # Tiempos (segundos) por etapa (opcionales). En casos grandes (100+ personas) conviene subirlos.
    t1_sec = float(pd.to_numeric(_get_param_optional(dfs, "t1_sec", 25), errors="coerce") or 25)
    t2a_sec = float(pd.to_numeric(_get_param_optional(dfs, "t2a_sec", 60), errors="coerce") or 60)
    t2c_sec = float(pd.to_numeric(_get_param_optional(dfs, "t2c_sec", 45), errors="coerce") or 45)
    t3_sec = float(pd.to_numeric(_get_param_optional(dfs, "t3_sec", 60), errors="coerce") or 60)

    # Semilla para reproducibilidad (debug). Si no viene, no forzamos.
    random_seed = _get_param_optional(dfs, "random_seed", None)

    horizon_dates = [d.date() for d in horizon]
    horizon_dow = {d.date(): DOW_MAP[pd.Timestamp(d).dayofweek] for d in horizon}

    slot_min = 30
    min_rest_hours_default = 11
    legal_weekly_cap_hours_default = 44
    sundays_off_default = 2

    # --- Part-time (PT) y regla de domingos ---
    # En muchos clientes (especialmente retail), el descanso mínimo de domingos aplica solo a contratos >= umbral (ej. 30h).
    # Para contratos bajo el umbral, por defecto NO se exige un mínimo de domingos libres (salvo que la jornada lo especifique).
    pt_umbral_horas = _get_param_optional(dfs, "pt_umbral_horas", 29)
    pt_umbral_horas = int(pd.to_numeric(pt_umbral_horas, errors="coerce") or 29)
    pt_umbral_min = int(max(0, pt_umbral_horas) * 60)
    pt_domingos_libre_default = _get_param_optional(dfs, "pt_domingos_libre_mes_default", 0)
    pt_domingos_libre_default = int(pd.to_numeric(pt_domingos_libre_default, errors="coerce") or 0)
    pt_exencion_domingo_auto = _get_param_optional(dfs, "pt_exencion_domingo_auto", 1)
    pt_exencion_domingo_auto = int(pd.to_numeric(pt_exencion_domingo_auto, errors="coerce") or 1)



    # Criticidad de slots (opcional): hoja CriticidadSlots con columnas:
    # - banda: MANANA / INTERMEDIO / NOCHE
    # - peso: número (int)
    # Si no existe, usamos defaults: MANANA=10, INTERMEDIO=3, NOCHE=7
    crit_df = dfs.get("CriticidadSlots")
    crit_weights = {"MANANA": 10, "INTERMEDIO": 3, "NOCHE": 7}
    try:
        if crit_df is not None and not crit_df.empty:
            ctmp = crit_df.copy()
            if "banda" in ctmp.columns and "peso" in ctmp.columns:
                for _, rr in ctmp.iterrows():
                    b = str(rr.get("banda", "")).strip().upper()
                    w = rr.get("peso", None)
                    if not b:
                        continue
                    try:
                        w = int(float(w))
                    except Exception:
                        continue
                    if b in crit_weights:
                        crit_weights[b] = w
    except Exception:
        pass


    # Politica de expertise/mentoría (opcional): hoja ExpertisePolitica con columnas:
    # banda, mentor_min, alta_min, allow_baja_solo, peso_baja_solo, peso_concentracion_alta
    # Defaults recomendados (pueden ser ajustados por cliente):
    # MANANA: mentor_min=1, alta_min=1, allow_baja_solo=0, peso_baja_solo=5_000_000, peso_concentracion_alta=2_000_000
    # INTERMEDIO: mentor_min=1, alta_min=0, allow_baja_solo=1, peso_baja_solo=1_000_000, peso_concentracion_alta=2_000_000
    # NOCHE: mentor_min=1, alta_min=1, allow_baja_solo=0, peso_baja_solo=5_000_000, peso_concentracion_alta=2_000_000
    pol_df = dfs.get("ExpertisePolitica")
    expertise_policy = {
        "MANANA": {"mentor_min": 1, "alta_min": 1, "allow_baja_solo": 0, "peso_baja_solo": 5_000_000, "peso_concentracion_alta": 2_000_000},
        "INTERMEDIO": {"mentor_min": 1, "alta_min": 0, "allow_baja_solo": 1, "peso_baja_solo": 1_000_000, "peso_concentracion_alta": 2_000_000},
        "NOCHE": {"mentor_min": 1, "alta_min": 1, "allow_baja_solo": 0, "peso_baja_solo": 5_000_000, "peso_concentracion_alta": 2_000_000},
    }
    try:
        if pol_df is not None and not pol_df.empty:
            ptmp = pol_df.copy()
            # normalizar nombres de columnas
            ptmp.columns = [str(c).strip() for c in ptmp.columns]
            for _, rr in ptmp.iterrows():
                b = str(rr.get("banda", "")).strip().upper()
                if b not in expertise_policy:
                    continue
                def _to_int(x, default):
                    try:
                        if x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip()=="":
                            return default
                        return int(float(x))
                    except Exception:
                        return default
                expertise_policy[b]["mentor_min"] = _to_int(rr.get("mentor_min", expertise_policy[b]["mentor_min"]), expertise_policy[b]["mentor_min"])
                expertise_policy[b]["alta_min"] = _to_int(rr.get("alta_min", expertise_policy[b]["alta_min"]), expertise_policy[b]["alta_min"])
                expertise_policy[b]["allow_baja_solo"] = _to_int(rr.get("allow_baja_solo", expertise_policy[b]["allow_baja_solo"]), expertise_policy[b]["allow_baja_solo"])
                expertise_policy[b]["peso_baja_solo"] = _to_int(rr.get("peso_baja_solo", expertise_policy[b]["peso_baja_solo"]), expertise_policy[b]["peso_baja_solo"])
                expertise_policy[b]["peso_concentracion_alta"] = _to_int(rr.get("peso_concentracion_alta", expertise_policy[b]["peso_concentracion_alta"]), expertise_policy[b]["peso_concentracion_alta"])
    except Exception:
        pass

    def _band_from_minute(m: int) -> str:
        # MAÑANA: 05:00-11:59, INTERMEDIO: 12:00-16:59, NOCHE: 17:00-04:59
        m = int(m) % 1440
        if 300 <= m <= 719:
            return "MANANA"
        if 720 <= m <= 1019:
            return "INTERMEDIO"
        return "NOCHE"
    if not rules.empty and "regla" in rules.columns and "valor" in rules.columns:
        try:
            slot_min = int(rules.loc[rules["regla"] == "slot_minutos", "valor"].iloc[0])
        except Exception:
            pass
        try:
            min_rest_hours_default = int(rules.loc[rules["regla"] == "min_descanso_horas", "valor"].iloc[0])
        except Exception:
            pass
        try:
            legal_weekly_cap_hours_default = int(
                rules.loc[rules["regla"] == "tope_legal_horas_semana", "valor"].iloc[0]
            )
        except Exception:
            pass
        try:
            sundays_off_default = int(
                rules.loc[rules["regla"] == "domingos_libre_objetivo_mes", "valor"].iloc[0]
            )
        except Exception:
            pass

    legal_weekly_cap_min_default = int(legal_weekly_cap_hours_default * 60)

    cat["shift_id"] = cat["shift_id"].astype(str).str.strip().str.upper()

    # Tipo de turno según catálogo (si existe columna 'tipo')
    shift_tipo_cat: Dict[str, str] = {}
    if "tipo" in cat.columns:
        try:
            cat["tipo"] = cat["tipo"].fillna("").astype(str).str.strip().str.upper()
        except Exception:
            pass
        for _, rr_cat in cat.iterrows():
            sid0 = str(rr_cat.get("shift_id", "")).strip().upper()
            if not sid0:
                continue
            t0 = str(rr_cat.get("tipo", "")).strip().upper()
            shift_tipo_cat[sid0] = t0


    shift_min = _shift_minutes_map(cat)
    shift_times = _shift_time_map(cat)

    # Minutos de inicio/fin por turno (para restricciones de ventanas de operación)
    shift_start_min: Dict[str, int] = {}
    shift_end_min: Dict[str, int] = {}
    for sid, (ini, fin) in shift_times.items():
        sm = _to_min(ini)
        em = _to_min(fin)
        shift_start_min[sid] = int(sm) if sm is not None else 0
        shift_end_min[sid] = int(em) if em is not None else 0

    all_shifts = sorted(set([str(x).strip().upper() for x in cat["shift_id"].astype(str).tolist() if str(x).strip()]))
    if "LIBRE" not in all_shifts:
        all_shifts.append("LIBRE")

    ABS_CODES = {"LM", "VAC", "PA"}
    work_shifts = [s for s in all_shifts if shift_min.get(s, 0) > 0]

    work_shift_set = set(work_shifts)
    if "dia_semana" in pool.columns:
        pool["dia_semana"] = pool["dia_semana"].apply(_norm_dow)
    pool_idx = _build_pool_index(pool)
    # Índice alternativo por unidad (unión de turnos de todos los cargos) para demanda por unidad
    pool_idx_ou: Dict[Tuple[str, str], List[str]] = {}
    for (ou_, cargo_, dow_), sh_list in pool_idx.items():
        key = (ou_, dow_)
        cur = pool_idx_ou.get(key)
        if cur is None:
            pool_idx_ou[key] = list(sh_list)
        else:
            cur.extend(sh_list)
    # dedup manteniendo orden
    for k, lst in list(pool_idx_ou.items()):
        seen=set(); out=[]
        for s in lst:
            if s in seen: continue
            seen.add(s); out.append(s)
        pool_idx_ou[k]=out

    exc_close: Dict[Tuple[Any, str, str], bool] = {}
    if exc is not None and not exc.empty and "fecha" in exc.columns:
        exc2 = exc.copy()
        exc2["fecha"] = pd.to_datetime(exc2["fecha"], errors="coerce").dt.normalize()
        for _, r in exc2.iterrows():
            if pd.isna(r.get("fecha")):
                continue
            if int(pd.to_numeric(r.get("requeridos", 0), errors="coerce") or 0) == 0:
                oux = str(r.get("org_unit_id", "")).strip().upper()
                cgx = str(r.get("cargo_id", "")).strip().upper()
                if not cgx or cgx == "NAN":
                    cgx = ANY_CARGO
                key = (pd.Timestamp(r["fecha"]).date(), oux, cgx)
                exc_close[key] = True

    employees = sorted([e for e in dot["employee_id"].astype(str).apply(_norm_empid).tolist() if e])

    emp_ou = {_norm_empid(r.get("employee_id","")): str(r["org_unit_id"]).strip().upper() for _, r in dot.iterrows() if _norm_empid(r.get("employee_id",""))}
    emp_cargo_id = {_norm_empid(r.get("employee_id","")): str(r["cargo_id"]).strip().upper() for _, r in dot.iterrows() if _norm_empid(r.get("employee_id",""))}

    # Índice de empleados por (org_unit_id, cargo_id) para diagnósticos rápidos
    emps_by_oucargo: Dict[Tuple[str, str], List[str]] = {}
    emps_by_ou = {}
    for emp in employees:
        ou = emp_ou.get(emp, '')
        if ou:
            emps_by_ou.setdefault(ou, []).append(emp)

    for emp in employees:
        emps_by_oucargo.setdefault((emp_ou.get(emp, ""), emp_cargo_id.get(emp, "")), []).append(emp)


    emp_cargo_label: Dict[str, str] = {}
    if "cargo" in dot.columns:
        for _, r in dot.iterrows():
            emp = str(r["employee_id"]).strip().upper()
            emp_cargo_label[emp] = str(r.get("cargo", "")).strip()
    else:
        emp_cargo_label = {e: emp_cargo_id[e] for e in employees}

    jornada_emp: Dict[str, str] = {}
    for _, r in dot.iterrows():
        emp = str(r["employee_id"]).strip().upper()
        jid = ""
        if "jornada_id" in dot.columns:
            jid = str(r.get("jornada_id", "")).strip().upper()
        jornada_emp[emp] = jid if jid else "J_DEFAULT"


    # Expertise simple (ALTA/MEDIA/BAJA) desde Dotacion.expertise
    # - Si la columna no existe o viene vacía -> MEDIA
    # - Se usa en el objetivo final (como preferencia) y en reportes (reporte_expertise.csv)
    emp_expertise: dict[str, str] = {}
    emp_expertise_score: dict[str, int] = {}
    _valid_exp = {"ALTA": 3, "MEDIA": 2, "BAJA": 1}
    if "expertise" in dot.columns:
        for _, r in dot.iterrows():
            emp = _norm_empid(r.get("employee_id", ""))
            lvl = str(r.get("expertise", "MEDIA")).strip().upper() or "MEDIA"
            if lvl not in _valid_exp:
                lvl = "MEDIA"
            emp_expertise[emp] = lvl
            emp_expertise_score[emp] = _valid_exp[lvl]
    for emp in employees:
        if emp not in emp_expertise:
            emp_expertise[emp] = "MEDIA"
            emp_expertise_score[emp] = _valid_exp["MEDIA"]

    # max legal/operacional por semana (hard cap)
    cap_week_emp: Dict[str, int] = {}
    # minutos contratados por semana (target mínimo a cumplir)
    contract_week_emp: Dict[str, int] = {}
    dias_trab_obj_emp: Dict[str, int] = {}
    dom_libre_mes_emp: Dict[str, int] = {}
    min_rest_emp: Dict[str, int] = {}

    for _, r in dot.iterrows():
        emp = str(r["employee_id"]).strip().upper()
        jid = jornada_emp[emp]

        # contrato_max_min_semana se interpreta como MINUTOS CONTRATADOS/SEMANA (target),
        # no como tope. El tope real viene de la jornada/ley.
        contract_week = int(pd.to_numeric(r.get("contrato_max_min_semana", 0), errors="coerce") or 0)
        if contract_week <= 0:
            contract_week = int(default_contract_week_min)
        cap_min_sem = _get_jornada_param(jornadas_df, jid, "cap_min_semana", None)
        dias_trab_obj = _get_jornada_param(jornadas_df, jid, "dias_trabajo_obj_sem", None)
        dom_mes = _get_jornada_param(jornadas_df, jid, "domingos_libre_mes", sundays_off_default)
        min_rest_h = _get_jornada_param(jornadas_df, jid, "min_descanso_horas", min_rest_hours_default)

        cap_from_j = int(pd.to_numeric(cap_min_sem, errors="coerce") or legal_weekly_cap_min_default)
        max_week = int(min(cap_from_j, legal_weekly_cap_min_default))
        cap_week_emp[emp] = max_week

        contract_target = contract_week if contract_week and contract_week > 0 else max_week
        contract_target = int(max(0, min(contract_target, max_week)))
        contract_week_emp[emp] = contract_target

        # días de trabajo objetivo (si viene vacío, inferimos por contrato)
        if dias_trab_obj is None or str(dias_trab_obj).strip() == "":
            desired = 6 if contract_target >= 2400 else 5
        else:
            desired = int(pd.to_numeric(dias_trab_obj, errors="coerce") or 6)
        # safeguard: un contrato >=40h no puede quedar con 4 días de trabajo objetivo
        if contract_target >= 2400:
            desired = max(desired, 5)
        dias_trab_obj_emp[emp] = max(1, min(7, desired))

        # domingos libres mínimos por mes (por jornada; PT puede quedar exento por umbral de horas)
        raw_dom = pd.to_numeric(dom_mes, errors="coerce")
        dom_val = int(raw_dom) if (raw_dom is not None and not pd.isna(raw_dom)) else int(sundays_off_default)
        if pt_exencion_domingo_auto and int(contract_target) > 0 and int(contract_target) <= int(pt_umbral_min):
            # Si la jornada no especifica algo distinto (o está en el default), aplicamos exención PT
            if (dom_mes is None or str(dom_mes).strip() == "") or (dom_val == int(sundays_off_default)):
                dom_val = int(pt_domingos_libre_default)
        dom_libre_mes_emp[emp] = max(0, int(dom_val))
        min_rest_emp[emp] = int(float(min_rest_h) * 60)

    # días permitidos por jornada (opcional): Jornadas.dias_permitidos_semana
    jornada_allowed_dows: Dict[str, Optional[Set[str]]] = {}
    if jornadas_df is not None and not jornadas_df.empty and ("dias_permitidos_semana" in jornadas_df.columns):
        tmpj = jornadas_df.copy()
        tmpj["jornada_id"] = tmpj["jornada_id"].astype(str).str.strip().str.upper()
        for _, jr in tmpj.iterrows():
            jid = str(jr.get("jornada_id", "")).strip().upper()
            raw = str(jr.get("dias_permitidos_semana", "")).strip().upper()
            if not jid:
                continue
            if not raw:
                jornada_allowed_dows[jid] = None
                continue
            parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
            dset = set(_norm_dow(p) for p in parts if _norm_dow(p))
            jornada_allowed_dows[jid] = dset if dset else None
    allowed_dows_emp: Dict[str, Optional[Set[str]]] = {emp: jornada_allowed_dows.get(jornada_emp.get(emp, ""), None) for emp in employees}

    shift_type3 = {s: _shift_type_3(s, shift_times) for s in all_shifts}

    rest_cache: Dict[Tuple[str, str], Optional[int]] = {}
    for s1 in all_shifts:
        for s2 in all_shifts:
            rest_cache[(s1, s2)] = _rest_minutes(s1, s2, shift_times)

    # (2) CAMBIO: usar _load_ausentismos(dfs) y armar forced_abs
    ausent = _load_ausentismos(dfs)
    forced_abs: Dict[Tuple[str, Any], str] = {}
    if ausent is not None and not ausent.empty:
        horizon_set = set(horizon_dates)
        for _, r in ausent.iterrows():
            emp = _norm_empid(r.get("employee_id", ""))
            code = str(r.get("ausentismo", "")).strip().upper()
            fecha = r.get("fecha", pd.NaT)
            if not emp or not code or pd.isna(fecha):
                continue
            d = pd.to_datetime(fecha).normalize().date()
            if d not in horizon_set:
                continue
            forced_abs[(emp, d)] = code
            # asegura que el código exista como "turno" en el modelo
            if code not in all_shifts:
                all_shifts.append(code)
            shift_min.setdefault(code, 0)
            shift_times.setdefault(code, ("00:00", "00:00"))

    # -------------------------
    # Construcción de necesidad por slots
    # v3: Dos diccionarios - mínimos (restricción dura) e ideales (objetivo blando)
    # -------------------------
    required_need: Dict[Tuple[str, str, Any, int], int] = {}       # Mínimos operativos
    required_need_ideal: Dict[Tuple[str, str, Any, int], int] = {} # Demanda real/ideal
    weight_under: Dict[Tuple[str, str, Any, int], int] = {}

    # Ventana operacional (minutos) por (org_unit_id, cargo_id, fecha) según NecesidadMinimos (y excepciones)
    open_need: Dict[Tuple[str, str, Any], int] = {}
    close_need: Dict[Tuple[str, str, Any], int] = {}
    need_total_req: Dict[Tuple[str, str, object], int] = {}  # (ou, cargo, date) -> sum requeridos
    need_total_ideal: Dict[Tuple[str, str, object], int] = {}  # (ou, cargo, date) -> sum ideal
    for dts in horizon:
        d = dts.date()
        dia_sem = DOW_MAP[dts.dayofweek]
        if use_demanda_unidad:
            need_day = demanda_unidad.loc[demanda_unidad["dia_semana"] == dia_sem]
        else:
            need_day = need.loc[need["dia_semana"] == dia_sem]
        if need_day.empty:
            continue

        if use_demanda_unidad:
            pairs = [(str(ou).strip().upper(), ANY_CARGO) for ou in need_day[["org_unit_id"]].drop_duplicates()["org_unit_id"].tolist()]
        else:
            pairs = [(str(ou).strip().upper(), str(cargo).strip().upper()) for (ou, cargo) in need_day[["org_unit_id", "cargo_id"]].drop_duplicates().itertuples(index=False, name=None)]

        for (ou, cargo) in pairs:
            ou = str(ou).strip().upper()
            cargo = str(cargo).strip().upper()

            if exc_close.get((d, ou, cargo), False) or (use_demanda_unidad and exc_close.get((d, ou, ANY_CARGO), False)):
                continue

            if use_demanda_unidad:
                need_uc = need_day.loc[
                    (need_day["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                ].copy()
            else:
                need_uc = need_day.loc[
                    (need_day["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                    & (need_day["cargo_id"].astype(str).str.strip().str.upper() == cargo)
                ].copy()

            # v3: Usar versión dual si estamos en demanda por unidad
            if use_demanda_unidad:
                req_slots_min, req_slots_ideal, open_min, close_cmp = _build_required_slots_dual(need_uc, slot_min)
                req_slots = req_slots_min  # Mantener compatibilidad
            else:
                req_slots, open_min, close_cmp = _build_required_slots(need_uc, slot_min)
                req_slots_min = req_slots
                req_slots_ideal = req_slots  # Sin holgura en el modo antiguo

            # v9.6: si no hay curva real para (ou,cargo,d), registramos 0 y NO abrimos ventana
            # total requeridos del día (robusto ante distintos formatos)
            if not req_slots_min:
                total_req = 0
                total_ideal = 0
            elif isinstance(req_slots_min, dict):
                total_req = int(sum(int(v) for v in req_slots_min.values()))
                total_ideal = int(sum(int(v) for v in req_slots_ideal.values()))
            else:
                # lista/iterable: puede venir como ints o tuplas
                _tmp = 0
                for it in req_slots_min:
                    if isinstance(it, (list, tuple)) and len(it) >= 3:
                        _tmp += int(it[2])
                    else:
                        _tmp += int(it)
                total_req = int(_tmp)
                total_ideal = total_req

            need_total_req[(ou, cargo, d)] = total_req
            need_total_ideal[(ou, cargo, d)] = total_ideal

            if not req_slots_min:
                continue

            open_need[(ou, cargo, d)] = int(open_min)
            close_need[(ou, cargo, d)] = int(close_cmp)

            critical_set: Set[int] = set()
            for k in range(0, 60, slot_min):
                critical_set.add((open_min + k) % 1440)
            for k in range(0, 60, slot_min):
                critical_set.add(((close_cmp - 60 + k) % 1440))

            # v3: Guardar ambos - mínimos e ideales
            for sl, req in req_slots_min.items():
                key = (ou, cargo, d, sl)
                required_need[key] = int(req)
                required_need_ideal[key] = int(req_slots_ideal.get(sl, req))
                weight_under[key] = 20 if sl in critical_set else 1

        # -------------------------
    # Mínimos por cargo en la unidad (mix) como soft-constraints
    # -------------------------
    mix_required: Dict[Tuple[str, str, Any, int], int] = {}
    if min_cargo_unidad is not None and not min_cargo_unidad.empty:
        mcu = min_cargo_unidad.copy()
        # La hoja trae 'min_requeridos'. Reusamos _build_required_slots renombrando a 'requeridos'.
        if "min_requeridos" in mcu.columns and "requeridos" not in mcu.columns:
            mcu = mcu.rename(columns={"min_requeridos": "requeridos"})
        for dts in horizon:
            d = dts.date()
            dia_sem = DOW_MAP[dts.dayofweek]
            mday = mcu.loc[mcu["dia_semana"] == dia_sem]
            if mday.empty:
                continue
            for (ou, cargo) in mday[["org_unit_id", "cargo_id"]].drop_duplicates().itertuples(index=False, name=None):
                ou = str(ou).strip().upper()
                cargo = str(cargo).strip().upper()
                if not ou or not cargo or cargo == "NAN":
                    continue
                m_uc = mday.loc[
                    (mday["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                    & (mday["cargo_id"].astype(str).str.strip().str.upper() == cargo)
                ].copy()
                req_slots, _, _ = _build_required_slots(m_uc, slot_min)
                for sl, req in req_slots.items():
                    mix_required[(ou, cargo, d, sl)] = int(req)

# -------------------------
    
    # -------------------------
    # Restricciones por empleado (HARD SIEMPRE)
    # -------------------------
    restr_by_emp: Dict[str, List[dict]] = {e: [] for e in employees}
    restr_global: List[dict] = []
    if restr_df is not None and not restr_df.empty:
        for _, rr in restr_df.iterrows():
            emp = _norm_empid(rr.get("employee_id", ""))
            rdict = rr.to_dict()
            rdict["employee_id"] = emp
            if emp:
                if emp in restr_by_emp:
                    restr_by_emp[emp].append(rdict)
            else:
                restr_global.append(rdict)


    # -------------------------
    # Diagnóstico de RestriccionesEmpleado (para detectar typos y match fallido)
    # -------------------------
    restr_diag_rows: List[Dict[str, Any]] = []
    if restr_df is not None and not restr_df.empty:
        supported_types = {
            "DIA_LIBRE_FIJO",
            "NO_TRABAJAR_FECHA",
            "PROHIBIR_TURNO",
            "SOLO_TURNOS_TIPO",
            "VENTANA_HORARIA",
            RULE_OPEN_CARGO,
            RULE_CLOSE_CARGO,
        }
        all_emp_set = set(employees)
        all_shift_ids_set = set(all_shifts)

        for _, rr0 in restr_df.iterrows():
            tipo0 = str(rr0.get("tipo", "")).strip().upper()
            emp0 = _norm_empid(rr0.get("employee_id", ""))
            v10 = str(rr0.get("valor1", "")).strip().upper()
            v20 = str(rr0.get("valor2", "")).strip().upper()
            msg = ""
            ok = True

            if tipo0 and tipo0 not in supported_types:
                ok = False
                msg = f"TIPO_NO_SOPORTADO:{tipo0}"

            if emp0 and emp0 not in all_emp_set:
                ok = False
                msg = (msg + "|" if msg else "") + "EMPLOYEE_ID_NO_EXISTE_EN_DOTACION"

            if tipo0 == "PROHIBIR_TURNO" and v10:
                matches_any = _match_shift_ids_token(v10, all_shift_ids_set, shift_times)
                if not matches_any:
                    ok = False
                    msg = (msg + "|" if msg else "") + "SHIFT_NO_EXISTE_EN_CATALOGO"

            if tipo0 == "SOLO_TURNOS_TIPO" and v10:
                allowed_tipo = {"MANANA", "TARDE", "NOCHE"}
                catalog_tipos = set([t for t in shift_tipo_cat.values() if t])
                if (v10 not in allowed_tipo) and (v10 not in catalog_tipos):
                    ok = False
                    msg = (msg + "|" if msg else "") + "TIPO_TURNO_DESCONOCIDO"

            if tipo0 == "VENTANA_HORARIA":
                # Acepta HH:MM o HHMM (e.g. 0730)
                def _try_min(x: str) -> Optional[int]:
                    if not x:
                        return None
                    x2 = x.replace(".", ":")
                    if len(x2) == 4 and x2.isdigit():
                        x2 = f"{x2[:2]}:{x2[2:]}"
                    return _to_min(x2)

                if _try_min(v10) is None or _try_min(v20) is None:
                    ok = False
                    msg = (msg + "|" if msg else "") + "VENTANA_HORARIA_INVALIDA"

            restr_diag_rows.append({
                "employee_id": emp0,
                "tipo": tipo0,
                "valor1": v10,
                "valor2": v20,
                "hard": rr0.get("hard", ""),
                "ok": 1 if ok else 0,
                "msg": msg,
            })

        try:
            pd.DataFrame(restr_diag_rows).to_csv(out_dir / "restr_diag.csv", index=False, encoding="utf-8")
        except Exception:
            pass


    def _norm_rdate(x: Any) -> Optional[Any]:
        """Normaliza fecha de restricción a datetime.date o None.

        Excel suele producir NaT en columnas datetime; NaT es truthy y puede romper ifs.
        """
        if x is None:
            return None
        # pandas NaT / NaN
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        # strings vacíos
        if isinstance(x, str):
            t = x.strip()
            if not t or t.upper() in ("NAN", "NONE", "NULL"):
                return None
            try:
                return pd.to_datetime(t, errors="coerce").normalize().date()
            except Exception:
                return None
        try:
            dt = pd.to_datetime(x, errors="coerce")
        except Exception:
            return None
        if pd.isna(dt):
            return None
        return pd.Timestamp(dt).normalize().date()

    def _restriction_applies(rr: dict, d: Any, dia_sem: str) -> bool:
        rtype = str(rr.get("tipo", "")).strip().upper()
        rdate = _norm_rdate(rr.get("fecha", None))
        rdow = _norm_dow(rr.get("dia_semana", ""))

        if rtype == "DIA_LIBRE_FIJO" and not rdow:
            rdow = _norm_dow(rr.get("valor1", ""))

        # d viene como datetime.date en este solver
        if rdate is not None and rdate != d:
            return False
        if rdow:
            return rdow == dia_sem
        return True

    def _is_shift_within_window(shift_id: str, win_start: str, win_end: str) -> bool:
        ss, ee_cmp = _shift_span_minutes(shift_id, shift_times)
        ws = _to_min(win_start)
        we = _to_min(win_end)
        if ss is None or ee_cmp is None or ws is None or we is None:
            return False
        if ee_cmp > 1440:
            return False
        return (ss >= ws) and ((ee_cmp % 1440) <= we)

    # -------------------------
    # CP-SAT variables
    # -------------------------
    model = cp_model.CpModel()
    x: Dict[Tuple[str, Any, str], cp_model.IntVar] = {}

    # Flag de relajación (fallback) para continuidad con PlanPrevio.
    # Si por historial previo el problema se vuelve INFEASIBLE, permitimos relajar SOLO restricciones de continuidad
    # (p.ej., corte de consecutivos al inicio). Se penaliza extremadamente alto en el objetivo para que solo se use
    # si es estrictamente necesario para encontrar factibilidad.
    relax_prev_boundary = model.NewBoolVar("relax_prev_boundary")

    
    # -------------------------
    # PlanPrevio: historial (idealmente 7 días previos) para continuidad legal
    # -------------------------
    plan_previo = dfs.get("PlanPrevio")
    prev_shift_map: Dict[Tuple[str, dt.date], str] = {}
    if plan_previo is not None and not plan_previo.empty:
        pp = plan_previo.copy()
        # Normaliza columnas mínimas
        if "employee_id" in pp.columns:
            pp["employee_id"] = pp["employee_id"].apply(_norm_empid)
        if "fecha" in pp.columns:
            pp["fecha"] = pd.to_datetime(pp["fecha"], errors="coerce").dt.date
        if "shift_id" in pp.columns:
            pp["shift_id"] = pp["shift_id"].astype(str).str.strip().str.upper()
        # Carga en dict (si hay duplicados, el último gana)
        for _, rr in pp.iterrows():
            emp = _norm_empid(rr.get("employee_id", ""))
            fd = rr.get("fecha", None)
            sid = str(rr.get("shift_id", "")).strip().upper()
            if not emp or fd is None or pd.isna(fd) or not sid:
                continue
            prev_shift_map[(emp, fd)] = sid

    start_date = horizon_dates[0] if horizon_dates else None
    prev_date = (start_date - dt.timedelta(days=1)) if start_date is not None else None

    # Turno del día anterior al inicio del horizonte (por empleado), si existe
    prev_shift_before_start: Dict[str, str] = {}
    prev_cross_before_start: Dict[str, int] = {}
    if prev_date is not None:
        for emp in employees:
            sid = prev_shift_map.get((emp, prev_date), "")
            sid = sid.strip().upper() if sid else ""
            if sid:
                prev_shift_before_start[emp] = sid
                # Cruza medianoche?
                if sid in work_shift_set:
                    try:
                        _s, _e = _shift_span_minutes(sid, shift_times)
                        prev_cross_before_start[emp] = 1 if _e > 1440 else 0
                    except Exception:
                        prev_cross_before_start[emp] = 0
                else:
                    prev_cross_before_start[emp] = 0

    # Consecutivos "carry-in": cantidad de días calendario trabajados consecutivos inmediatamente antes del inicio
    carry_in_consec_work: Dict[str, int] = {emp: 0 for emp in employees}
    if start_date is not None:
        # Miramos hasta 7 días hacia atrás (suficiente para regla de 6 consecutivos)
        lookback_days = 7
        for emp in employees:
            c = 0
            for k in range(1, lookback_days + 1):
                day = start_date - dt.timedelta(days=k)
                sid = prev_shift_map.get((emp, day), "")
                sid = sid.strip().upper() if sid else ""
                # Determina si ese día cuenta como trabajado calendario
                worked_from_start = (sid in work_shift_set)
                # saliente si el día anterior (day-1) cruza medianoche
                prev_day = day - dt.timedelta(days=1)
                sid_prev = prev_shift_map.get((emp, prev_day), "")
                sid_prev = sid_prev.strip().upper() if sid_prev else ""
                saliente = 0
                if sid_prev in work_shift_set:
                    try:
                        _s2, _e2 = _shift_span_minutes(sid_prev, shift_times)
                        saliente = 1 if _e2 > 1440 else 0
                    except Exception:
                        saliente = 0
                worked_calendar = worked_from_start or bool(saliente)
                # AUS y LIBRE rompen consecutivos (salvo saliente, ya considerado)
                if sid in ABS_CODES or sid == "LIBRE":
                    # Si hay saliente, cuenta como trabajado; si no, rompe.
                    if not worked_calendar:
                        break
                if not worked_calendar:
                    break
                c += 1
                if c >= 6:
                    break
            carry_in_consec_work[emp] = c

    allowed_emp_day: Dict[Tuple[str, Any], Set[str]] = {}  # para diagnóstico

    cover_map: Dict[str, set[int]] = {s: set(_shift_slot_coverage(s, shift_times, slot_min)) for s in work_shifts}

    # (3) CAMBIO: ausentismo forzado NO bloqueado por pool; si hay, solo ese código
    for emp in employees:
        ou = emp_ou[emp]
        cargo_id = emp_cargo_id[emp]

        for d in horizon_dates:
            dow = horizon_dow[d]

            abs_code = forced_abs.get((emp, d), "")

            # si hay ausentismo, ese día SOLO puede ser ese código (aunque no esté en pool)
            if abs_code:
                for s in all_shifts:
                    var = model.NewBoolVar(f"x_{emp}_{d}_{s}")
                    x[(emp, d, s)] = var
                    if s != abs_code:
                        model.Add(var == 0)

                model.Add(sum(x[(emp, d, s)] for s in all_shifts) == 1)
                model.Add(x[(emp, d, abs_code)] == 1)
                allowed_emp_day[(emp, d)] = {abs_code}
                continue

            pool_list = pool_idx.get((ou, cargo_id, dow), [])
            pool_set = set([s.strip().upper() for s in pool_list if str(s).strip()])
            # Source of truth: AusentismoEmpleado. PoolTurnos debe contener SOLO turnos trabajables (S_*).
            # Ignoramos LIBRE y códigos de ausentismo si vinieran en el pool por compatibilidad.
            pool_set.discard("LIBRE")
            for _c in ABS_CODES:
                pool_set.discard(_c)
            pool_set = {s for s in pool_set if s in work_shift_set}

            # v9.3: Siempre permitir LIBRE. El pool define turnos de trabajo permitidos, pero el descanso
            # debe ser una opción universal (si no hay ausentismo forzado).
            if not pool_set:
                allowed = set(work_shifts + ["LIBRE"])
            else:
                allowed = set(pool_set)
                allowed.add("LIBRE")

            # Restricción opcional: días permitidos por jornada (si se especifica).
            adows = allowed_dows_emp.get(emp, None)
            if adows is not None and dow not in adows:
                # Fuera de días permitidos: solo LIBRE (si hay ausentismo forzado, ya se manejó arriba).
                allowed = {"LIBRE"}

            # Continuidad (PlanPrevio): descanso mínimo desde el último turno del día anterior al inicio del horizonte.
            if start_date is not None and d == start_date:
                prev_sid = prev_shift_before_start.get(emp, "")
                prev_sid = prev_sid.strip().upper() if prev_sid else ""
                if prev_sid and (prev_sid in work_shift_set):
                    # min_rest_emp ya está en minutos (línea 997). Default es min_rest_hours_default * 60.
                    min_rest_min = int(min_rest_emp.get(emp, min_rest_hours_default * 60))
                    # Elimina turnos de trabajo que violen el descanso mínimo respecto del turno previo
                    to_remove = []
                    for s in allowed:
                        if s in work_shift_set:
                            try:
                                if _rest_minutes(prev_sid, s, shift_times) < min_rest_min:
                                    to_remove.append(s)
                            except Exception:
                                pass
                    for s in to_remove:
                        allowed.discard(s)
                    # Asegura que exista al menos LIBRE si quedamos sin turnos trabajables
                    if not any((s in work_shift_set) for s in allowed):
                        allowed.add("LIBRE")

            forced_shift: Optional[str] = None

            for rr in (restr_by_emp.get(emp, []) + restr_global):
                if not _restriction_applies(rr, d, dow):
                    continue
                rtype = str(rr.get("tipo", "")).strip().upper()
                v1 = str(rr.get("valor1", "")).strip().upper()

                if rtype in ("DIA_LIBRE_FIJO", "NO_TRABAJAR_FECHA"):
                    forced_shift = "LIBRE"
                    break

                if rtype == "PROHIBIR_TURNO" and v1:
                    matches = _match_shift_ids_token(v1, allowed, shift_times)
                    for ms in matches:
                        allowed.discard(ms)

                elif rtype == "SOLO_TURNOS_TIPO" and v1:
                    keep = set()
                    for s in allowed:
                        if s == "LIBRE":
                            keep.add(s)
                        elif (shift_type3.get(s, "UNK") == v1) or (shift_tipo_cat.get(s, "") == v1):
                            keep.add(s)
                    allowed = keep

                elif rtype == "VENTANA_HORARIA":
                    ws = str(rr.get("valor1", "")).strip()
                    we = str(rr.get("valor2", "")).strip()
                    keep = set()
                    for s in allowed:
                        if s == "LIBRE":
                            keep.add(s)
                        else:
                            if _is_shift_within_window(s, ws, we):
                                keep.add(s)
                    allowed = keep

            if not allowed:
                allowed = {"LIBRE"}

            # v9.6.1: Cierre por ausencia de demanda / excepciones
            # - Si ExcepcionesDemanda marca requeridos=0 para la fecha+OU+cargo => cerrar (solo LIBRE)
            # - Si cerrar_dias_sin_demanda=1 y NO existe curva (total_req=0) => cerrar (solo LIBRE)
            if (forced_shift is None or forced_shift == "LIBRE"):
                if exc_close.get((d, ou, cargo_id), False):
                    allowed = {"LIBRE"}
                elif cerrar_dias_sin_demanda and (need_total_req.get((ou, (ANY_CARGO if use_demanda_unidad else cargo_id), d), 0) == 0):
                    allowed = {"LIBRE"}

            # Ventana operacional por necesidad (evita iniciar antes de lo requerido por la curva)
            # BUGFIX: la llave correcta es (org_unit_id, cargo_id, fecha). Antes se usaba 'cargo' (stale).
            open_req = open_need.get((ou, cargo_id, d))
            if open_req is not None and (forced_shift is None or forced_shift == "LIBRE"):
                allowed2: Set[str] = set()
                for s in allowed:
                    if s == "LIBRE":
                        allowed2.add(s)
                        continue
                    # códigos 0-min (ausencias u otros) no están sujetos a esta regla
                    if int(shift_min.get(s, 0)) <= 0:
                        allowed2.add(s)
                        continue
                    if shift_start_min.get(s, 0) >= int(open_req):
                        allowed2.add(s)
                allowed = allowed2 if allowed2 else {"LIBRE"}

            allowed_emp_day[(emp, d)] = set(allowed)
            for s in all_shifts:
                var = model.NewBoolVar(f"x_{emp}_{d}_{s}")
                x[(emp, d, s)] = var

                if s not in allowed:
                    model.Add(var == 0)

                if s in ABS_CODES:
                    model.Add(var == 0)

            model.Add(sum(x[(emp, d, s)] for s in all_shifts) == 1)

            if forced_shift is not None:
                if (emp, d, forced_shift) not in x:
                    v = model.NewBoolVar(f"x_{emp}_{d}_{forced_shift}")
                    x[(emp, d, forced_shift)] = v
                model.Add(x[(emp, d, forced_shift)] == 1)

    # -------------------------
    # Diagnóstico de turnos permitidos (v9.7-fix)
    # -------------------------
    try:
        total_emp_days = len(employees) * len(horizon_dates)
        only_libre_count = sum(1 for (emp, d), allowed in allowed_emp_day.items() if allowed == {"LIBRE"})
        has_work_count = total_emp_days - only_libre_count
        pct_libre = 100.0 * only_libre_count / total_emp_days if total_emp_days > 0 else 0
        diag_msg = f"[DIAG] Empleados*Días: {total_emp_days}, con turnos trabajo: {has_work_count}, solo LIBRE: {only_libre_count} ({pct_libre:.1f}%)"
        print(diag_msg)
        if pct_libre > 50:
            print(f"[WARN] Más del 50% de emp*día solo tienen LIBRE. Revisa: PoolTurnos, NecesidadMinimos/DemandaUnidad, cerrar_dias_sin_demanda")
        # Guardar diagnóstico
        (out_dir / "diag_turnos_permitidos.txt").write_text(diag_msg, encoding="utf-8")
    except Exception:
        pass

    # descanso mínimo
    for emp in employees:
        min_rest = int(min_rest_emp.get(emp, min_rest_hours_default * 60))
        for i in range(len(horizon_dates) - 1):
            d1 = horizon_dates[i]
            d2 = horizon_dates[i + 1]
            for s1 in all_shifts:
                for s2 in all_shifts:
                    rest = rest_cache.get((s1, s2))
                    if rest is None:
                        continue
                    if rest < min_rest:
                        model.Add(x[(emp, d1, s1)] + x[(emp, d2, s2)] <= 1)


    # -------------------------
    # SALIENTE (turnos que cruzan medianoche)
    # Un día con 'saliente' NO debe contarse como LIBRE real (calendario),
    # aunque el turno esté asignado al día anterior por hora de inicio.
    #
    # off_real[(emp, d)] = 1 si el día es realmente LIBRE (sin trabajo calendario):
    #   - x[(emp, d, "LIBRE")] = 1
    #   - y NO hay turno del día anterior que cruce medianoche (saliente)
    # -------------------------
    cross_shifts = [
        s for s in work_shifts
        if int(shift_min.get(s, 0)) > 0 and int(shift_end_min.get(s, 0)) < int(shift_start_min.get(s, 0))
    ]

    off_real = {}  # (emp, date) -> BoolVar
    if cross_shifts:
        for emp in employees:
            for i, d in enumerate(horizon_dates):
                x_libre = x[(emp, d, "LIBRE")]
                if i == 0:
                    b = model.NewBoolVar(f"off_real_{emp}_{d}")
                    pc0 = int(prev_cross_before_start.get(emp, 0) or 0)
                    if pc0 == 0:
                        model.Add(b == x_libre)
                    else:
                        # Hay saliente desde el día anterior (fuera del horizonte): no puede ser LIBRE real
                        model.Add(b == 0)
                    off_real[(emp, d)] = b
                else:
                    prev_d = horizon_dates[i - 1]
                    pc = model.NewIntVar(0, 1, f"prev_cross_{emp}_{d}")
                    model.Add(pc == sum(x[(emp, prev_d, s)] for s in cross_shifts))

                    b = model.NewBoolVar(f"off_real_{emp}_{d}")
                    # b == 1  <=>  x_libre == 1 AND pc == 0
                    model.Add(b <= x_libre)
                    model.Add(b + pc <= 1)
                    model.Add(b >= x_libre - pc)
                    off_real[(emp, d)] = b
    else:
        for emp in employees:
            for d in horizon_dates:
                off_real[(emp, d)] = x[(emp, d, "LIBRE")]

    # máximo 6 días consecutivos trabajados (AUS no cuenta como trabajado)
    for emp in employees:
        for i in range(len(horizon_dates) - 6):
            window = horizon_dates[i : i + 7]
            worked_bools = []
            for d in window:
                b = model.NewBoolVar(f"worked_{emp}_{d}")
                nonwork_terms = [off_real[(emp, d)]]
                for ac in ABS_CODES:
                    if (emp, d, ac) in x:
                        nonwork_terms.append(x[(emp, d, ac)])
                model.Add(sum(nonwork_terms) == 0).OnlyEnforceIf(b)
                model.Add(sum(nonwork_terms) >= 1).OnlyEnforceIf(b.Not())
                worked_bools.append(b)
            model.Add(sum(worked_bools) <= 6)


    # Continuidad (PlanPrevio): si venimos con días consecutivos trabajados antes del inicio del horizonte,
    # forzamos un "corte" dentro de los primeros (7 - carry_in) días para no exceder 6 consecutivos reales.
    if horizon_dates:
        for emp in employees:
            c = int(carry_in_consec_work.get(emp, 0) or 0)
            if c >= 1 and c <= 6:
                k = max(1, 7 - c)
                first_days = horizon_dates[:k]
                nonwork_first = []
                for d in first_days:
                    terms = [off_real[(emp, d)]]
                    for ac in ABS_CODES:
                        if (emp, d, ac) in x:
                            terms.append(x[(emp, d, ac)])
                    nonwork_first.append(sum(terms))
                # Al menos un día no trabajado real en esos primeros k días
                model.Add(sum(nonwork_first) >= 1).OnlyEnforceIf(relax_prev_boundary.Not())

    # domingos libres mínimos (AUS en domingo no cuenta como LIBRE)
    sundays = [d.date() for d in horizon if d.dayofweek == 6]
    for emp in employees:
        must = int(dom_libre_mes_emp.get(emp, sundays_off_default))
        if sundays:
            model.Add(sum(off_real[(emp, sd)] for sd in sundays) >= must)

    # reparto domingo por mitades (soft fuerte) para >=30h/sem
    FT_MIN = 30 * 60
    group_emps: Dict[Tuple[str, str], List[str]] = {}
    for emp in employees:
        g = (emp_ou[emp], emp_cargo_id[emp])
        group_emps.setdefault(g, []).append(emp)

    sunday_group_slacks: List[cp_model.IntVar] = []
    for g, emps_g in group_emps.items():
        elig = [e for e in emps_g if contract_week_emp.get(e, cap_week_emp.get(e, 0)) >= FT_MIN]
        if not elig:
            continue
        n = len(elig)
        target = n // 2
        for sd in sundays:
            count_off = model.NewIntVar(0, n, f"sun_off_{g[0]}_{g[1]}_{sd}")
            model.Add(count_off == sum(off_real[(e, sd)] for e in elig))
            slack_pos = model.NewIntVar(0, n, f"sun_slack_pos_{g[0]}_{g[1]}_{sd}")
            slack_neg = model.NewIntVar(0, n, f"sun_slack_neg_{g[0]}_{g[1]}_{sd}")
            model.Add(count_off + slack_neg - slack_pos == target)
            sunday_group_slacks.extend([slack_pos, slack_neg])

    # ausentismo por semana (constante)
    absent_days_week: Dict[Tuple[str, int], int] = {}
    for emp in employees:
        for w in range(weeks):
            days_w = horizon_dates[w * 7 : (w + 1) * 7]
            c = 0
            for d in days_w:
                if forced_abs.get((emp, d), ""):
                    c += 1
            absent_days_week[(emp, w)] = c

    # minutos por semana
    # - hard cap: cap_week_emp (jornada/ley)
    # - target: contract_week_emp (minutos contratados), ajustado por ausentismo
    # - se penaliza FUERTE el déficit (no llegar al contrato); y SUAVE el exceso (overtime) si ocurre.
    minutes_under_vars: List[cp_model.IntVar] = []
    minutes_over_vars: List[cp_model.IntVar] = []
    minutes_short_bools: List[cp_model.BoolVar] = []
    # Para reporte/diagnóstico
    week_total_min_var: Dict[Tuple[str, int], cp_model.IntVar] = {}
    week_under_var: Dict[Tuple[str, int], cp_model.IntVar] = {}
    week_over_var: Dict[Tuple[str, int], cp_model.IntVar] = {}
    target_min_week: Dict[Tuple[str, int], int] = {}

    for emp in employees:
        cap = int(cap_week_emp.get(emp, legal_weekly_cap_min_default))
        contract = int(contract_week_emp.get(emp, cap))
        desired_workdays = int(dias_trab_obj_emp.get(emp, 6))
        desired_workdays = max(1, min(7, desired_workdays))

        for w in range(weeks):
            days_w = horizon_dates[w * 7 : (w + 1) * 7]
            total_min = model.NewIntVar(0, cap, f"min_{emp}_w{w}")

            expr = []
            for d in days_w:
                for s in work_shifts:
                    expr.append(x[(emp, d, s)] * int(shift_min.get(s, 0)))
            model.Add(total_min == (sum(expr) if expr else 0))
            model.Add(total_min <= cap)

            aus_days = absent_days_week[(emp, w)]
            if aus_days >= 7:
                tgt = 0
            else:
                daily_nominal = int(round(contract / desired_workdays)) if desired_workdays > 0 else contract
                tgt = max(0, contract - daily_nominal * aus_days)

            target_min_week[(emp, w)] = int(tgt)
            week_total_min_var[(emp, w)] = total_min

            under = model.NewIntVar(0, contract, f"under_min_{emp}_w{w}")
            model.Add(under >= tgt - total_min)
            model.Add(under >= 0)
            minutes_under_vars.append(under)
            is_short = model.NewBoolVar(f"short_{emp}_w{w}")
            if tgt == 0:
                model.Add(is_short == 0)
            else:
                model.Add(under >= 1).OnlyEnforceIf(is_short)
                model.Add(under == 0).OnlyEnforceIf(is_short.Not())
            minutes_short_bools.append(is_short)
            week_under_var[(emp, w)] = under

            over = model.NewIntVar(0, cap, f"over_min_{emp}_w{w}")
            model.Add(over >= total_min - contract)
            model.Add(over >= 0)
            minutes_over_vars.append(over)
            week_over_var[(emp, w)] = over

    # jornada: libres mínimos semanales; extra libre penalizado (no infeasible)
    extra_off_vars: List[cp_model.IntVar] = []
    for emp in employees:
        desired_workdays = int(dias_trab_obj_emp.get(emp, 6))
        desired_workdays = max(1, min(7, desired_workdays))
        base_off = 7 - desired_workdays

        for w in range(weeks):
            days_w = horizon_dates[w * 7 : (w + 1) * 7]
            aus_days = absent_days_week[(emp, w)]

            off_w = model.NewIntVar(0, 7, f"off_{emp}_w{w}")
            model.Add(off_w == sum(off_real[(emp, d)] for d in days_w))

            sunday_in_w = [d for d in days_w if pd.Timestamp(d).dayofweek == 6]
            sun_off_var = x[(emp, sunday_in_w[0], "LIBRE")] if sunday_in_w else None

            if aus_days >= 7:
                required_off_val = 0
                extra = model.NewIntVar(0, 7, f"extraoff_{emp}_w{w}")
                model.Add(extra >= off_w - required_off_val)
                extra_off_vars.append(extra)
                continue

            # Regla: libres mínimos semanales según jornada (base_off).
            # IMPORTANTE: NO sumar un libre extra por el hecho de que el domingo sea libre.
            # (Eso forzaba 2 libres en 6x1 cuando el domingo quedaba libre, bajando a 5 días trabajados).
            required_off_val = int(base_off)
            extra = model.NewIntVar(0, 7, f"extraoff_{emp}_w{w}")
            model.Add(off_w >= required_off_val)
            model.Add(extra == off_w - required_off_val)
            extra_off_vars.append(extra)


    # -------------------------
    
    # -------------------------
    # Reglas STRICT de apertura/cierre por cargo (parametrizables en RestriccionesEmpleado)
    # -------------------------
    # Semántica:
    # - A partir de DemandaUnidad calculamos por (org_unit_id, dia_semana):
    #     open_time  = min(inicio) donde requeridos>0
    #     close_time = max(fin)    donde requeridos>0
    # - REGLA_APERTURA_CARGO: debe existir al menos 1 persona asignada a un turno que **empiece exacto** en open_time.
    # - REGLA_CIERRE_CARGO:   debe existir al menos 1 persona asignada a un turno que **termine exacto** en close_time.
    # - Preferencia comercial: si hay alguien del cargo objetivo elegible ese día, debe ser de ese cargo.
    #   Si el cargo está completamente no-elegible (LIBRE/ausente/restricciones), puede hacerlo otro cargo para evitar vacío.
    #
    # Nota: esto es un CONSTRAINT real (HARD) sobre x. Si una regla aplica y no hay opciones, fallamos explícitamente.
    roles_diag_rows: List[Dict[str, Any]] = []
    if role_cargo_rules:
        if not use_demanda_unidad:
            raise ValueError("REGLA_APERTURA_CARGO/REGLA_CIERRE_CARGO requieren DemandaUnidad como fuente de apertura/cierre.")

        # sets de turnos por hora exacta de inicio/fin (solo turnos que NO cruzan medianoche para cierre)
        shifts_by_start: Dict[int, List[str]] = {}
        shifts_by_end: Dict[int, List[str]] = {}
        for s in work_shifts:
            sm = int(shift_start_min.get(s, 0))
            em = int(shift_end_min.get(s, 0))
            shifts_by_start.setdefault(sm, []).append(s)
            # para cierre exigimos fin \"del día\", no un fin de turno que cruza medianoche
            if em >= sm:
                shifts_by_end.setdefault(em, []).append(s)

        # universo de unidades donde DemandaUnidad define operación
        ous_all = sorted(set(str(x).strip().upper() for x in demanda_unidad['org_unit_id'].tolist()))

        def _rule_applies(rule: Dict[str, Any], ou: str, d: dt.date, dow: str) -> bool:
            if rule.get("org_unit_id"):
                if str(rule["org_unit_id"]).strip().upper() != ou:
                    return False
            if rule.get("dia_semana"):
                if str(rule["dia_semana"]).strip().upper() != dow:
                    return False
            if rule.get("fecha") is not None and str(rule.get("fecha")) not in ("", "NaT", "nan", "None"):
                try:
                    if rule["fecha"] != d:
                        return False
                except Exception:
                    return False
            return True


        for rule in role_cargo_rules:
            rtype = str(rule.get("tipo", "")).strip().upper()
            cargo_req = str(rule.get("cargo_id", "")).strip().upper()
            hard_req = int(rule.get("hard", 1) or 0)
            ou_filter = str(rule.get("org_unit_id", "")).strip().upper()

            for d in horizon_dates:
                dow = horizon_dow[d]
                ous = ous_all
                if ou_filter:
                    ous = [ou_filter] if ou_filter in ous_all else []

                for ou in ous:
                    if not _rule_applies(rule, ou, d, dow):
                        continue

                    open_min = open_need.get((ou, ANY_CARGO, d))
                    close_min = close_need.get((ou, ANY_CARGO, d))
                    target = open_min if rtype == RULE_OPEN_CARGO else close_min
                    target_label = "open_time" if rtype == RULE_OPEN_CARGO else "close_time"

                    if target is None:
                        roles_diag_rows.append({
                            'org_unit_id': ou,
                            'fecha': str(d),
                            'dia_semana': dow,
                            'tipo': rtype,
                            'cargo_objetivo': cargo_req,
                            'open_time': _min_to_hhmm(open_min) if open_min is not None else '',
                            'close_time': _min_to_hhmm(close_min) if close_min is not None else '',
                            'turnos_exactos_pool': 0,
                            'empleados_elegibles': 0,
                            'vars_candidatas': 0,
                            'scope_usado': '',
                            'motivo': 'NO_DEMANDA',
                        })
                        continue

                    if target is None:
                        roles_diag_rows.append({
                            "org_unit_id": ou, "fecha": d, "dia_semana": dow, "tipo": rtype,
                            "cargo_objetivo": cargo_req,
                            "open_time": _min_to_hhmm(open_min) if open_min is not None else "",
                            "close_time": _min_to_hhmm(close_min) if close_min is not None else "",
                            "turnos_exactos_pool": 0,
                            "empleados_elegibles": 0,
                            "vars_candidatas": 0,
                            "motivo": "NO_DEMANDA",
                        })
                        if hard_req:
                            raise ValueError(f"{rtype} HARD sin {target_label} (DemandaUnidad vacía) para org_unit_id={ou}, dow={dow}.")
                        continue

                    # turnos exactos según pool (unión de cargos) para ese ou+dow
                    pool_shifts = pool_idx_ou.get((ou, dow), [])
                    if rtype == RULE_OPEN_CARGO:
                        exact_pool = [s for s in pool_shifts if s in work_shift_set and shift_start_min.get(s, -1) == int(target)]
                        cand_shifts = shifts_by_start.get(int(target), [])
                    else:
                        exact_pool = [s for s in pool_shifts if s in work_shift_set and (shift_end_min.get(s, -1) == int(target)) and (shift_end_min.get(s,0) >= shift_start_min.get(s,0))]
                        cand_shifts = shifts_by_end.get(int(target), [])

                    if not cand_shifts:
                        roles_diag_rows.append({
                            "org_unit_id": ou, "fecha": d, "dia_semana": dow, "tipo": rtype,
                            "cargo_objetivo": cargo_req,
                            "open_time": _min_to_hhmm(open_min) if open_min is not None else "",
                            "close_time": _min_to_hhmm(close_min) if close_min is not None else "",
                            "turnos_exactos_pool": len(exact_pool),
                            "empleados_elegibles": 0,
                            "vars_candidatas": 0,
                            "motivo": "NO_SHIFT_MATCH",
                        })
                        if hard_req:
                            raise ValueError(f"{rtype} HARD sin turnos en catálogo para target={_min_to_hhmm(int(target))} (revisa CatalogoTurnos) ou={ou} dow={dow}.")
                        continue

                                        # Candidatos:
                    # Semántica HARD solicitada:
                    # - Si ALGÚN empleado del cargo objetivo trabaja ese día (en cualquier turno trabajado),
                    #   entonces la apertura/cierre debe ser cumplida por ese cargo.
                    # - Si NINGÚN empleado del cargo objetivo trabaja (porque todos quedaron LIBRE/ausentes/bloqueados),
                    #   entonces puede hacerlo cualquier cargo elegible (fallback) para evitar vacío.
                    #
                    # Esto se modela con un booleano work_cargo_day que indica si existe al menos un turno trabajado
                    # asignado a empleados del cargo objetivo ese día.

                    def _collect_candidates(emp_list: List[str]) -> List[cp_model.IntVar]:
                        out: List[cp_model.IntVar] = []
                        for emp in emp_list:
                            # Ausentismo => no puede cumplir rol ese día
                            if forced_abs.get((emp, d), ""):
                                continue
                            allowed_set_day = allowed_emp_day.get((emp, d), set())
                            for s in cand_shifts:
                                if s not in allowed_set_day:
                                    continue
                                v = x.get((emp, d, s))
                                if v is not None:
                                    out.append(v)
                        return out

                    emps_ou_all = emps_by_ou.get(ou, [])
                    emps_ou_cargo = [e for e in emps_ou_all if emp_cargo_id.get(e, "") == cargo_req]

                    cand_cargo = _collect_candidates(emps_ou_cargo)
                    cand_any = _collect_candidates(emps_ou_all)

                    # Booleano: "hay alguien del cargo objetivo trabajando este día"
                    if emps_ou_cargo:
                        work_cargo_day = model.NewBoolVar(f"workcargo_{rtype}_{ou}_{d}_{cargo_req}")
                        work_terms = []
                        for emp in emps_ou_cargo:
                            if forced_abs.get((emp, d), ""):
                                continue
                            allowed_set_day = allowed_emp_day.get((emp, d), set())
                            for s in work_shifts:
                                if s not in allowed_set_day:
                                    continue
                                v = x.get((emp, d, s))
                                if v is not None:
                                    work_terms.append(v)
                        if work_terms:
                            model.Add(sum(work_terms) >= 1).OnlyEnforceIf(work_cargo_day)
                            model.Add(sum(work_terms) == 0).OnlyEnforceIf(work_cargo_day.Not())
                        else:
                            # No hay variables de trabajo posibles para el cargo => no pueden "trabajar"
                            model.Add(work_cargo_day == 0)
                    else:
                        work_cargo_day = None

                    # Diagnóstico
                    motivo = ""
                    if not cand_any:
                        motivo = "BLOCKED_BY_REST" if emps_ou_all else "NO_EMP_ELIGIBLE"

                    roles_diag_rows.append({
                        "org_unit_id": ou, "fecha": d, "dia_semana": dow, "tipo": rtype,
                        "cargo_objetivo": cargo_req,
                        "open_time": _min_to_hhmm(open_min) if open_min is not None else "",
                        "close_time": _min_to_hhmm(close_min) if close_min is not None else "",
                        "turnos_exactos_pool": len(exact_pool),
                        "empleados_elegibles": len(emps_ou_cargo),
                        "vars_candidatas": len(cand_any),
                        "motivo": motivo,
                        "scope_usado": "COND_CARGO_ELSE_ANY",
                    })

                    if hard_req:
                        if not cand_any:
                            raise ValueError(
                                f"{rtype} HARD sin variables candidatas: ou={ou} fecha={d} dow={dow} "
                                f"cargo_obj={cargo_req} target={_min_to_hhmm(int(target))}. "
                                f"Revisa PoolTurnos, RestriccionesEmpleado, Jornadas, descanso mínimo y ausentismos."
                            )

                        if work_cargo_day is None:
                            # No existe el cargo en la unidad => siempre fallback a cualquiera
                            model.Add(sum(cand_any) >= 1)
                        else:
                            # Si el cargo trabaja => debe cumplir el rol (candidatos del cargo)
                            if not cand_cargo:
                                # Si llega a darse que work_cargo_day=1 pero no hay candidatos exactos del cargo, será infeasible (correcto).
                                # Igual lo dejamos explícito para el diagnóstico.
                                pass
                            model.Add(sum(cand_cargo) >= 1).OnlyEnforceIf(work_cargo_day)
                            model.Add(sum(cand_any) >= 1).OnlyEnforceIf(work_cargo_day.Not())



    # cobertura under/over
    # v3: Dos capas de cobertura
    # - under_min_vars: déficit respecto al MÍNIMO operativo (restricción blanda con peso alto)
    # - under_ideal_vars: déficit respecto al IDEAL (objetivo blando con peso medio)
    under_vars = []           # Mantener compatibilidad (bajo mínimo)
    under_ideal_vars = []     # Nuevo: bajo ideal
    over_vars = []
    under_mix_vars = []
    
    # Diagnóstico v3: contar slots con holgura definida
    slots_con_holgura = sum(1 for k in required_need if required_need_ideal.get(k, required_need[k]) > required_need[k])
    if slots_con_holgura > 0:
        print(f"[v3] Dos curvas activas: {slots_con_holgura} slots con holgura (ideal > mínimo)")
    
    for (ou, cargo, d, sl), req_min in required_need.items():
        req_ideal = required_need_ideal.get((ou, cargo, d, sl), req_min)
        
        cov_terms = []
        for emp in employees:
            if emp_ou[emp] != ou or (cargo != ANY_CARGO and emp_cargo_id[emp] != cargo):
                continue
            for s in work_shifts:
                if sl in cover_map.get(s, set()):
                    cov_terms.append(x[(emp, d, s)])

        # máximo teórico de cobertura (cota superior)
        if cargo == ANY_CARGO:
            max_cov = len([e for e in employees if emp_ou[e] == ou])
        else:
            max_cov = len([e for e in employees if emp_ou[e] == ou and emp_cargo_id[e] == cargo])
        cov = model.NewIntVar(0, max_cov, f"cov_{ou}_{cargo}_{d}_{sl}")
        model.Add(cov == (sum(cov_terms) if cov_terms else 0))

        # v3: Dos variables under - una para mínimo, otra para ideal
        under_min = model.NewIntVar(0, req_min, f"under_min_{ou}_{cargo}_{d}_{sl}")
        under_ideal = model.NewIntVar(0, req_ideal, f"under_ideal_{ou}_{cargo}_{d}_{sl}")
        over = model.NewIntVar(0, max_cov, f"over_{ou}_{cargo}_{d}_{sl}")

        # Déficit respecto al mínimo (restricción blanda con peso MUY alto)
        model.Add(under_min >= req_min - cov)
        model.Add(under_min >= 0)
        
        # Déficit respecto al ideal (objetivo blando)
        model.Add(under_ideal >= req_ideal - cov)
        model.Add(under_ideal >= 0)
        
        # Over respecto al ideal (no al mínimo)
        model.Add(over >= cov - req_ideal)
        model.Add(over >= 0)

        under_vars.append(((ou, cargo, d, sl), under_min))
        under_ideal_vars.append(((ou, cargo, d, sl), under_ideal))
        over_vars.append(((ou, cargo, d, sl), over))

        # objetivo (3 etapas, lexicográfico)
    # v3: Ahora tenemos 4 niveles:
    # 1) Minimizar faltantes de demanda MÍNIMA (restricción blanda con peso MUY alto)
    # 2) Minimizar faltantes de demanda IDEAL (objetivo blando con peso medio)
    # 3) Con la demanda óptima fija, minimizar déficit de minutos de contrato (llegar a horas)
    # 4) Con (1), (2) y (3) fijos, minimizar el resto de preferencias (over, extras, domingos, etc.)

    W_UNDER_MIN_BASE = 10_000_000   # Peso muy alto para déficit bajo MÍNIMO (restricción blanda fuerte)
    W_UNDER_MIN_CRIT = 30_000_000   # Peso aún mayor para slots críticos bajo mínimo
    W_UNDER_IDEAL_BASE = 1_000_000  # Peso medio para déficit bajo IDEAL (objetivo blando)
    W_UNDER_IDEAL_CRIT = 3_000_000  # Peso mayor para slots críticos bajo ideal
    # Mantener compatibilidad con nombres antiguos
    W_UNDER_BASE = W_UNDER_MIN_BASE
    W_UNDER_CRIT = W_UNDER_MIN_CRIT
    W_OVER = 0  # permitir sobre-cobertura para cumplir contrato (no penalizar over de demanda)
    W_EXTRA_OFF = 8_000_000
    # Minutos: preferimos llegar al contrato. Overtime (si se usa) es penalizado suave.
    W_OVER_CONTRACT_MIN = 500
    W_SUN_GROUP_DEV = 2_000_000

    # --- Componentes objetivo ---
    demand_under_terms = []       # Términos de déficit bajo mínimo (peso alto)
    demand_under_ideal_terms = [] # Términos de déficit bajo ideal (peso medio)

    # Mix mínimos por cargo dentro de unidad (soft): si no alcanza, penaliza under pero no rompe factibilidad
    if mix_required:
        for (ou, cargo, d, sl), req in mix_required.items():
            cov_terms = []
            for emp in employees:
                if emp_ou[emp] != ou or emp_cargo_id[emp] != cargo:
                    continue
                for s in work_shifts:
                    if sl in cover_map.get(s, set()):
                        cov_terms.append(x[(emp, d, s)])
            if cov_terms:
                cov = model.NewIntVar(0, len(cov_terms), f"cov_mix_{ou}_{cargo}_{d}_{sl}")
                model.Add(cov == sum(cov_terms))
            else:
                cov = model.NewConstant(0)
            u = model.NewIntVar(0, int(req), f"under_mix_{ou}_{cargo}_{d}_{sl}")
            model.Add(u >= req - cov)
            model.Add(u >= 0)
            under_mix_vars.append(((ou, cargo, d, sl), u))


    # -------------------------
    # Roles Apertura / Cierre (desde RestriccionesEmpleado)
    # Definición de apertura/cierre por DemandaUnidad: primer y último tramo con requeridos>0 en la unidad.
    # No crea infeasible por defecto: si no alcanza, penaliza under con el peso indicado en 'penalizacion'.
    # -------------------------
    role_pen_terms = []  # términos (peso * under_role) que se suman a la etapa 1 de demanda

    if use_demanda_unidad and restr_df is not None and (not restr_df.empty):
        # Extraemos filas de rol
        role_rows = restr_df.loc[restr_df["tipo"].isin(["ROL_APERTURA", "ROL_CIERRE"])].copy() if "tipo" in restr_df.columns else pd.DataFrame()
        if not role_rows.empty and "employee_id" in role_rows.columns:
            # Mapa por (ou, d, tipo) -> {emps, hard_any, peso_max}
            role_map: Dict[Tuple[str, object, str], Dict[str, Any]] = {}
            for rr in role_rows.itertuples(index=False):
                emp = str(getattr(rr, "employee_id", "")).strip()
                if not emp:
                    continue
                # unidades por empleado (ya normalizadas)
                ou = emp_ou.get(emp, "")
                if not ou:
                    continue

                rtype = str(getattr(rr, "tipo", "")).strip().upper()
                rdate = getattr(rr, "fecha", None) if hasattr(rr, "fecha") else None
                rdate = _norm_rdate(rdate)
                rdow = _norm_dow(getattr(rr, "dia_semana", "")) if hasattr(rr, "dia_semana") else ""

                # scope de aplicación
                for dts in horizon:
                    d = dts.date()
                    if rdate is not None and rdate != d:
                        continue
                    dia_sem = DOW_MAP[dts.dayofweek]
                    if rdow and rdow != dia_sem:
                        continue

                    key = (ou, d, rtype)
                    item = role_map.setdefault(key, {"emps": set(), "hard": False, "peso": 0})
                    item["emps"].add(emp)
                    # hard / penalizacion
                    hard_val = getattr(rr, "hard", None) if hasattr(rr, "hard") else None
                    try:
                        if int(pd.to_numeric(hard_val, errors="coerce") or 0) == 1:
                            item["hard"] = True
                    except Exception:
                        pass
                    pen_val = getattr(rr, "penalizacion", None) if hasattr(rr, "penalizacion") else None
                    try:
                        p = int(pd.to_numeric(pen_val, errors="coerce") or 0)
                        if p > item["peso"]:
                            item["peso"] = p
                    except Exception:
                        pass

            # Restricciones por unidad y día
            for (ou, d, rtype), item in role_map.items():
                emps = sorted(list(item["emps"]))
                if not emps:
                    continue
                # apertura/cierre definidos por DemandaUnidad (cargo ANY_CARGO)
                open_min = open_need.get((ou, ANY_CARGO, d), None)
                close_cmp = close_need.get((ou, ANY_CARGO, d), None)
                if open_min is None or close_cmp is None:
                    continue
                sl_open = int(open_min) % 1440
                sl_close = int((int(close_cmp) - slot_min) % 1440)

                target_sl = sl_open if rtype == "ROL_APERTURA" else sl_close

                # cobertura del slot por parte del grupo de rol (presentes en ese tramo)
                cov_terms = []
                for emp in emps:
                    if emp_ou.get(emp, "") != ou:
                        continue
                    for s in work_shifts:
                        if target_sl in cover_map.get(s, set()):
                            cov_terms.append(x[(emp, d, s)])

                cov_role = model.NewIntVar(0, len(emps), f"cov_role_{rtype}_{ou}_{d}_{target_sl}")
                model.Add(cov_role == (sum(cov_terms) if cov_terms else 0))

                # (A) "Debe haber al menos 1 rol cubriendo el slot" (soft por defecto)
                if item["hard"]:
                    model.Add(cov_role >= 1)
                else:
                    under_role = model.NewIntVar(0, 1, f"under_role_{rtype}_{ou}_{d}_{target_sl}")
                    model.Add(under_role >= 1 - cov_role)
                    model.Add(under_role >= 0)
                    peso = int(item["peso"] or 10_000_000)
                    role_pen_terms.append(peso * under_role)

                # (B) Evitar vacío del rol: al menos 1 del grupo trabaja en el día (no LIBRE)
                day_work_terms = []
                for emp in emps:
                    if emp_ou.get(emp, "") != ou:
                        continue
                    for s in work_shifts:
                        day_work_terms.append(x[(emp, d, s)])
                day_work = model.NewIntVar(0, len(emps), f"daywork_role_{rtype}_{ou}_{d}")
                model.Add(day_work == (sum(day_work_terms) if day_work_terms else 0))

                if item["hard"]:
                    model.Add(day_work >= 1)
                else:
                    under_day = model.NewIntVar(0, 1, f"under_dayrole_{rtype}_{ou}_{d}")
                    model.Add(under_day >= 1 - day_work)
                    model.Add(under_day >= 0)
                    peso = int(item["peso"] or 10_000_000)
                    # penalización un poco menor que el slot específico (pero misma escala)
                    role_pen_terms.append((peso // 2) * under_day)

    for key, uvar in under_vars:
        ou, cargo, d, sl = key
        w = weight_under.get((ou, cargo, d, sl), 1)
        demand_under_terms.append((W_UNDER_CRIT if w >= 20 else W_UNDER_BASE) * uvar)

    # v3: Agregar términos de déficit bajo ideal (peso más bajo que mínimo)
    for key, uvar_ideal in under_ideal_vars:
        ou, cargo, d, sl = key
        w = weight_under.get((ou, cargo, d, sl), 1)
        demand_under_ideal_terms.append((W_UNDER_IDEAL_CRIT if w >= 20 else W_UNDER_IDEAL_BASE) * uvar_ideal)

    # Roles apertura/cierre (soft)
    if role_pen_terms:
        demand_under_terms.extend(role_pen_terms)

    demand_over_terms = [W_OVER * ovar for _, ovar in over_vars]
    W_MIX_UNDER = 200  # penalización suave por no cumplir mix de cargo
    mix_under_terms = [W_MIX_UNDER * uvar for _, uvar in under_mix_vars]

    # -------------------------
    # Balance de OFF real por día (suave) por grupo operativo (org_unit_id + cargo_id)
    # Objetivo: evitar picos de muchos LIBRE reales el mismo día (ej. 7 libres un martes),
    # sin convertirlo en restricción dura (para no volver infeasible).
    # Se aplica como penalización en etapa 3 (fairness), debajo de demanda/contrato.
    # -------------------------
    off_balance_terms = []
    try:
        # Agrupar empleados por (org_unit_id, cargo_id)
        emp_group = {}
        for emp in employees:
            ou = str(emp_ou.get(emp, "NA")).strip().upper()
            cg = str(emp_cargo_id.get(emp, "NA")).strip().upper()
            key = (ou, cg)
            emp_group.setdefault(key, []).append(emp)

        # Particionar fechas por semana calendario (LUN-DOM) usando week_start (lunes)
        dates_by_week = {}
        for d in horizon_dates:
            dts = pd.to_datetime(d).normalize()
            wk_start = (dts - pd.Timedelta(days=int(dts.weekday()))).normalize()
            dates_by_week.setdefault(wk_start, []).append(d)

        W_OFF_BALANCE = 2_000_000  # peso suave (<< demanda/contrato), ajustable

        # Nueva métrica (más fina): minimizar desviación respecto al promedio semanal de OFF real.
        # En vez de (max-min), usamos:
        #   total_off = sum_d off_count[d]
        #   penalizamos sum_d |off_count[d] * 7 - total_off|
        # Esto equivale a 7 * sum_d |off_count[d] - avg|, sin división.
        for (ou, cg), emps_g in emp_group.items():
            n_g = len(emps_g)
            if n_g <= 1:
                continue
            for wk_start, dlist in dates_by_week.items():
                if len(dlist) <= 1:
                    continue

                cnt_vars = []
                for d in dlist:
                    cnt = model.NewIntVar(0, n_g, f"off_cnt__{ou}__{cg}__{wk_start.date()}__{d.date()}")
                    terms = []
                    for emp in emps_g:
                        if (emp, d) in off_real:
                            terms.append(off_real[(emp, d)])
                        else:
                            terms.append(x[(emp, d, "LIBRE")])
                    model.Add(cnt == sum(terms))
                    cnt_vars.append(cnt)

                # total OFF real del grupo en la semana
                total_off = model.NewIntVar(0, n_g * len(dlist), f"off_sum__{ou}__{cg}__{wk_start.date()}")
                model.Add(total_off == sum(cnt_vars))

                # Desviación por día respecto al promedio (sin división):
                # dev_d >= off_cnt[d]*7 - total_off
                # dev_d >= total_off - off_cnt[d]*7
                for i, d in enumerate(dlist):
                    dev = model.NewIntVar(0, 7 * n_g, f"off_dev__{ou}__{cg}__{wk_start.date()}__{d.date()}")
                    model.Add(dev >= cnt_vars[i] * 7 - total_off)
                    model.Add(dev >= total_off - cnt_vars[i] * 7)
                    off_balance_terms.append(W_OFF_BALANCE * dev)

    except Exception:
        # Si algo falla, no rompemos el solver (fairness opcional)
        off_balance_terms = []

    other_terms = []
    other_terms.extend(W_EXTRA_OFF * ex for ex in extra_off_vars)
    other_terms.extend(W_SUN_GROUP_DEV * sv for sv in sunday_group_slacks)
    other_terms.extend(off_balance_terms)
    # Overtime contractual (exceso vs contrato) se decide recién en etapa 3
    contract_over_terms = [W_OVER_CONTRACT_MIN * omin for omin in minutes_over_vars]

    expr_demand_under = sum(demand_under_terms) if demand_under_terms else 0
    # v3: Expresión de déficit bajo ideal
    expr_demand_under_ideal = sum(demand_under_ideal_terms) if demand_under_ideal_terms else 0
    # v9.6: Objetivo lexicográfico de demanda:
    #  - primero minimizar la cantidad de slots con déficit (under>0)
    #  - luego minimizar el déficit ponderado (más peso a tramos críticos)
    expr_demand_under_slots = sum(uvar for _, uvar in under_vars) if under_vars else 0
    expr_demand_under_ideal_slots = sum(uvar for _, uvar in under_ideal_vars) if under_ideal_vars else 0
    expr_demand_under_weighted = expr_demand_under
    # BIG-M suficientemente grande para que 1 slot adicional con déficit pese más que cualquier mejora ponderada
    total_req_slots = int(sum(required_need.values())) if required_need else 0
    total_ideal_slots = int(sum(required_need_ideal.values())) if required_need_ideal else 0
    BIG_M_DEM = total_req_slots * W_UNDER_CRIT + 1
    BIG_M_IDEAL = total_ideal_slots * W_UNDER_IDEAL_CRIT + 1  # v3: para ideales
    BIG_M_RELAX = 10**18
    # v3: El objetivo ahora prioriza: 1) mínimos, 2) ideales, 3) contrato
    expr_demand_obj = (relax_prev_boundary * BIG_M_RELAX + 
                       expr_demand_under_slots * BIG_M_DEM + expr_demand_under_weighted +
                       expr_demand_under_ideal)

    expr_contract_under_min = sum(minutes_under_vars) if minutes_under_vars else 0

    expr_contract_short = sum(minutes_short_bools) if minutes_short_bools else 0
    # solve (lexicográfico robusto con fallback)
    # NOTA: CP-SAT puede devolver UNKNOWN por límite de tiempo aunque exista solución.
    # En ese caso, caemos a la mejor solución encontrada en etapas previas y NO reventamos.

    def _status_name(code: int) -> str:
        try:
            return cp_model.CpSolver().StatusName(code)
        except Exception:
            return str(code)

    def _capture_x(slv) -> dict:
        return {k: int(slv.Value(v)) for k, v in x.items()}
    _hint_done = [False]

    def _apply_hint(xvals: dict):
        # IMPORTANTE: CP-SAT invalida el modelo si se agregan hints duplicados para la misma variable.
        # Por eso aplicamos hints SOLO UNA VEZ (típicamente después de etapa 1).
        if _hint_done[0]:
            return
        for k, var in x.items():
            model.AddHint(var, int(xvals.get(k, 0)))
        _hint_done[0] = True

    # ---- Etapa 1: demanda (minimizar under de demanda) ----
    solver1 = cp_model.CpSolver()
    if random_seed is not None and str(random_seed).strip() != "":
        try:
            solver1.parameters.random_seed = int(pd.to_numeric(random_seed, errors="coerce") or 0)
        except Exception:
            pass
    solver1.parameters.num_search_workers = 8
    solver1.parameters.max_time_in_seconds = t1_sec

    model.Minimize(expr_demand_obj)
    status1 = solver1.Solve(model)
    if status1 not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        # Dump diagnósticos útiles ANTES de abortar (para que el usuario tenga evidencia inmediata)
        try:
            # roles_diag (apertura/cierre por cargo)
            if 'roles_diag_rows' in locals():
                pd.DataFrame(roles_diag_rows).to_csv(out_dir / 'roles_diag.csv', index=False, encoding='utf-8-sig')
        except Exception:
            pass
        try:
            pd.DataFrame([{'etapa': '1_demanda', 'status': _status_name(status1), 'objective': ''}]).to_csv(
                out_dir / 'solver_diag.csv', index=False, encoding='utf-8-sig'
            )
            (out_dir / 'solver_diag.txt').write_text(f"1_demanda: {_status_name(status1)}\n", encoding='utf-8')
        except Exception:
            pass

        # Si hay reglas HARD de apertura/cierre, intenta dar un resumen rápido del primer bloqueo
        try:
            if 'roles_diag_rows' in locals() and roles_diag_rows:
                df_rd = pd.DataFrame(roles_diag_rows)
                # prioriza las reglas HARD sin variables candidatas
                if 'vars_candidatas' in df_rd.columns:
                    bad = df_rd[(df_rd.get('vars_candidatas', 0) == 0) & (df_rd.get('motivo', '') != 'NO_DEMANDA')].head(5)
                else:
                    bad = df_rd.head(5)
                if not bad.empty:
                    rows = []
                    for r in bad.to_dict('records'):
                        rows.append(
                            f"{r.get('tipo','')} ou={r.get('org_unit_id','')} fecha={r.get('fecha','')} target_open={r.get('open_time','')} target_close={r.get('close_time','')} cargo={r.get('cargo_objetivo','')} motivo={r.get('motivo','')} scope={r.get('scope_usado','')} shifts={r.get('turnos_exactos_pool','')} emps={r.get('empleados_elegibles','')} vars={r.get('vars_candidatas','')}"
                        )
                    msg = "\n".join(rows)
                    raise RuntimeError(
                        f"CP-SAT INFEASIBLE en etapa 1. Hay reglas de rol/apertura-cierre que quedaron sin soporte.\n" + msg
                    )
        except RuntimeError:
            raise
        except Exception:
            pass

        raise RuntimeError(f"CP-SAT no encontró solución factible (etapa 1: demanda). Status={_status_name(status1)}")


    best_dem = int(round(solver1.ObjectiveValue()))
    # v9.6: Guardamos también el óptimo de slots y de déficit ponderado para locks posteriores
    best_dem_slots = int(sum(solver1.Value(uvar) for _, uvar in under_vars))
    best_dem_weighted = int(sum((W_UNDER_CRIT if weight_under.get(key, 1) >= 20 else W_UNDER_BASE) * solver1.Value(uvar) for key, uvar in under_vars))
    # v3: Diagnóstico de cobertura ideal
    best_dem_ideal_slots = int(sum(solver1.Value(uvar) for _, uvar in under_ideal_vars)) if under_ideal_vars else 0
    if slots_con_holgura > 0:
        print(f"[v3] Etapa 1 - Cobertura mínimo: {best_dem_slots} slots con déficit | Cobertura ideal: {best_dem_ideal_slots} slots bajo ideal")

    # IMPORTANTE: usamos <= (no ==) para evitar bloqueos por redondeo/doble y permitir mejoras si aparecen.
    # Lock de demanda (v9.6): primero slots, luego ponderado
    model.Add(expr_demand_under_slots <= best_dem_slots + demanda_epsilon_slots)
    model.Add(expr_demand_under_weighted <= best_dem_weighted + demanda_epsilon_weighted)

    # Capturamos la mejor solución de Etapa 1 (demanda).
    best_x = _capture_x(solver1)
    best_stage = 'etapa1_demanda'

    # -------- Etapa 1c: desempate dominical dentro del lock de demanda --------
    # Objetivo: entre soluciones igual de buenas en demanda global (slots+ponderado),
    # preferimos la que minimiza el under de DEMANDA en domingos (por slot).
    sundays: Set[pd.Timestamp] = set()
    try:
        for d in horizon_dates:
            dd = pd.to_datetime(d).normalize()
            if dd.weekday() == 6:  # DOM
                sundays.add(dd)
    except Exception:
        sundays = set()

    expr_demand_under_sunday = 0
    sunday_under_terms = []
    try:
        if under_vars and sundays:
            sunday_under_terms = [
                uvar for (ou, cargo, d, sl), uvar in under_vars
                if pd.to_datetime(d).normalize() in sundays
            ]
            if sunday_under_terms:
                expr_demand_under_sunday = sum(sunday_under_terms)
    except Exception:
        sunday_under_terms = []
        expr_demand_under_sunday = 0

    # OJO: no se puede usar `if expr != 0` porque es una expresión CP-SAT. Usamos la lista de términos.
    if sunday_under_terms:
        solver1c = cp_model.CpSolver()
        if random_seed is not None and str(random_seed).strip() != "":
            try:
                solver1c.parameters.random_seed = int(pd.to_numeric(random_seed, errors="coerce") or 0)
            except Exception:
                pass
        solver1c.parameters.num_search_workers = 8
        # Un 25% del tiempo de Etapa 1, mínimo 5s.
        try:
            solver1c.parameters.max_time_in_seconds = max(5.0, float(t1_sec) * 0.25)
        except Exception:
            solver1c.parameters.max_time_in_seconds = 5.0

        # Minimizamos under dominical, manteniendo lock de demanda global.
        model.Minimize(expr_demand_under_sunday)
        status1c = solver1c.Solve(model)

        if status1c in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            best_x = _capture_x(solver1c)
            best_stage = 'etapa1c_demanda_domingo'

        # Restauramos objetivo original de demanda para continuar con Etapa 2a (se re-define más abajo igualmente).
        model.Minimize(expr_demand_obj)

    # NOTA: NO aplicamos hint aquí. Lo aplicamos después de la etapa 2a (contrato deep),
    # para que el warm-start refleje la mejor solución que además cumple contrato.
    # (Si 2a falla, aplicaremos hint con la mejor solución disponible en el fallback.)

    # ---- Etapa 2a: contrato (deep search; evita quedarse corto de horas) ----
    # Objetivo: dentro del óptimo de demanda, empujar fuerte a cumplir contrato.
    # Estrategia: búsqueda larga (hasta 15 min) + reinicios con seeds distintos si aún queda under.
    t2a_deep_sec = max(float(t2a_sec), 900.0)

    # BIG_M debe ser mayor que el máximo total posible de minutos bajo contrato.
    if minutes_under_vars:
        big_m = int(sum(int(contract_week_emp.get(emp, 0)) for emp in employees) * max(int(weeks), 1) + 1)
    else:
        big_m = 1
    obj_2a = expr_contract_short * big_m + expr_contract_under_min
    model.Minimize(obj_2a)

    def _contract_metrics(slv: cp_model.CpSolver) -> Tuple[int, int]:
        short = int(sum(slv.Value(v) for v in minutes_short_bools)) if minutes_short_bools else 0
        under = int(sum(slv.Value(v) for v in minutes_under_vars)) if minutes_under_vars else 0
        return short, under

    def _lex_better(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
        # True si a es mejor que b (lexicográfico: short, luego under)
        return (a[0] < b[0]) or (a[0] == b[0] and a[1] < b[1])

    def _run_contract(max_sec: float, seed: int, hint_x: Optional[Dict[Tuple[str, Any, str], int]]) -> Tuple[int, Optional[Dict[Tuple[str, Any, str], int]], Optional[Tuple[int, int]]]:
        # Aplica hint para partir desde una solución ya factible (demanda) y acelerar hallazgo de FEASIBLE.
        if hint_x is not None:
            _apply_hint(hint_x)
        slv = cp_model.CpSolver()
        if seed is not None:
            try:
                slv.parameters.random_seed = int(seed)
            except Exception:
                pass
        slv.parameters.num_search_workers = 8
        slv.parameters.max_time_in_seconds = float(max_sec)
        slv.parameters.randomize_search = True
        status = slv.Solve(model)
        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            metrics = _contract_metrics(slv)
            return status, _capture_x(slv), metrics
        return status, None, None

    # Intentos con presupuesto total (15 min). Partimos con la mejor solución conocida (best_x) como hint.
    base_seed = 0
    if random_seed is not None and str(random_seed).strip() != "":
        try:
            base_seed = int(pd.to_numeric(random_seed, errors="coerce") or 0)
        except Exception:
            base_seed = 0

    budget = float(t2a_deep_sec)
    attempts: List[Tuple[str, float, int]] = []
    # 1) intento principal
    main_sec = min(budget, 450.0)
    attempts.append(("main", main_sec, base_seed))
    budget -= main_sec
    # 2) reinicios si queda presupuesto (para explorar combinatorias)
    if budget > 0:
        # 2 o 3 reinicios
        k = 3 if budget >= 240 else 2
        per = max(60.0, budget / k)
        for i in range(k):
            attempts.append((f"restart{i+1}", per, base_seed + i + 1))

    best2a_x: Optional[Dict[Tuple[str, Any, str], int]] = None
    best2a_metrics: Optional[Tuple[int, int]] = None

    for name, sec, seed in attempts:
        status, sol_x, metrics = _run_contract(sec, seed, best2a_x or best_x)
        if status == cp_model.MODEL_INVALID:
            raise RuntimeError("CP-SAT devolvió MODEL_INVALID en etapa 2a (contrato). Revisa modelo/hints.")
        if sol_x is None or metrics is None:
            continue
        if (best2a_metrics is None) or _lex_better(metrics, best2a_metrics):
            best2a_metrics = metrics
            best2a_x = sol_x
            # si ya logramos 0 short y 0 under, no tiene sentido seguir
            if best2a_metrics[0] == 0 and best2a_metrics[1] == 0:
                break

    if best2a_x is not None and best2a_metrics is not None:
        status2a = cp_model.FEASIBLE
        best_short, best_contract_under = best2a_metrics
        obj2a_val = int(best_short) * int(big_m) + int(best_contract_under)

        # Fijamos cotas para evitar que etapas posteriores rompan contrato logrado.
        model.Add(expr_contract_short <= int(best_short))
        model.Add(expr_contract_under_min <= int(best_contract_under))
        best_x = best2a_x
        best_stage = 'etapa2a_contrato_deep'
        _apply_hint(best_x)
    else:
        status2a = cp_model.UNKNOWN
        obj2a_val = None
        # Fallback: mantenemos la mejor solución (etapa 1) y seguimos.
        best_short = None
        best_contract_under = None
        best_under_from_2a = None
        _apply_hint(best_x)

    
    # --- Baseline de OFF real (para permitir "swaps" controlados en etapa 3) ---
    # Idea: mantenemos el patrón de LIBRE/Off (incluyendo salientes que NO cuentan como OFF real),
    # pero permitimos mover algunos OFF cuando eso mejora cobertura (ej. domingos), pagando un costo suave.
    baseline_off_real: Optional[Dict[Tuple[str, Any], int]] = None
    try:
        if best_x is not None:
            baseline_off_real = {}
            if cross_shifts:
                for emp in employees:
                    for i, d in enumerate(horizon_dates):
                        xlib = int(best_x.get((emp, d, "LIBRE"), 0))
                        if i == 0:
                            pc0 = int(prev_cross_before_start.get(emp, 0) or 0)
                            off = 1 if (xlib == 1 and pc0 == 0) else 0
                        else:
                            prev_d = horizon_dates[i - 1]
                            prev_cross = 0
                            for s in cross_shifts:
                                if int(best_x.get((emp, prev_d, s), 0)) == 1:
                                    prev_cross = 1
                                    break
                            off = 1 if (xlib == 1 and prev_cross == 0) else 0
                        baseline_off_real[(emp, d)] = int(off)
            else:
                for emp in employees:
                    for d in horizon_dates:
                        baseline_off_real[(emp, d)] = int(best_x.get((emp, d, "LIBRE"), 0))
    except Exception:
        baseline_off_real = None

# ---- Etapa 2b: contrato (minimizar minutos bajo contrato) ----

    # v9.5: Ya se optimiza en 2a (lexicográfico). Se deja 2b como NO ejecutada para reducir UNKNOWN.
    solver2b = None
    status2b = None
    # best_contract_under ya quedó fijado con el valor observado en 2a (si existe variable, quedó en 0).

    # Inicialización (la etapa 2c puede no ejecutarse)
    solver2c = None
    status2c = None


    # ---- Etapa 2c: desempate (preferir turnos con más minutos efectivos) ----
    # Independiente de sufijos (_30/_60). Se basa en minutos efectivos y en la necesidad
    # de cumplir el contrato semanal con el patrón de días trabajados (desired_workdays).
    # Idea: minimizar el 'déficit diario' = max(0, minutos_objetivo_diario - minutos_turno)
    # como desempate. Esto empuja al solver a elegir turnos más largos cuando existe libertad.

    best_tiebreak = None
    if prefer_long_shifts and best_contract_under is not None:
        # minutos objetivo diario por empleado (aprox): contrato_sem / días trabajados objetivo
        daily_need_emp = {
            emp: int(math.ceil(contract_week_emp.get(emp, 0) / max(int(dias_trab_obj_emp.get(emp, 1)), 1)))
            for emp in employees
        }

        deficit_terms = []
        for emp in employees:
            dn = int(daily_need_emp.get(emp, 0))
            if dn <= 0:
                continue
            # Solo consideramos como 'cortos' los turnos con minutos efectivos < dn
            short_candidates = [s for s in work_shifts if int(shift_min.get(s, 0)) < dn]
            if not short_candidates:
                continue
            for d in horizon_dates:
                for s in short_candidates:
                    coef = dn - int(shift_min.get(s, 0))
                    if coef > 0:
                        deficit_terms.append(coef * x[(emp, d, s)])

        # Etapa 2c: desempate para preferir turnos con más minutos efectivos (no depende de sufijos _30/_60).
        # OJO: no se puede usar `if expr_day_deficit != 0` porque expr_day_deficit es una expresión CP-SAT.
        expr_day_deficit = sum(deficit_terms) if deficit_terms else None
        if deficit_terms:
            solver2c = cp_model.CpSolver()
            if random_seed is not None and str(random_seed).strip() != "":
                try:
                    solver2c.parameters.random_seed = int(pd.to_numeric(random_seed, errors="coerce") or 0)
                except Exception:
                    pass
            solver2c.parameters.num_search_workers = 8
            solver2c.parameters.max_time_in_seconds = t2c_sec

            model.Minimize(expr_day_deficit)
            status2c = solver2c.Solve(model)
            if status2c == cp_model.MODEL_INVALID:
                raise RuntimeError("CP-SAT devolvió MODEL_INVALID en etapa 2c (desempate). Revisa modelo.")
            if status2c in (cp_model.OPTIMAL, cp_model.FEASIBLE):
                best_tiebreak = int(round(solver2c.ObjectiveValue()))
                model.Add(expr_day_deficit <= best_tiebreak)
                best_x = _capture_x(solver2c)
                best_stage = 'etapa2c_prefer_long_shifts'
            else:
                best_tiebreak = None

    # ---- Etapa 3: resto (over, extras, domingos, etc.) ----
    solver3 = cp_model.CpSolver()
    if random_seed is not None and str(random_seed).strip() != "":
        try:
            solver3.parameters.random_seed = int(pd.to_numeric(random_seed, errors="coerce") or 0)
        except Exception:
            pass
    solver3.parameters.num_search_workers = 8
    solver3.parameters.max_time_in_seconds = max(float(t3_sec), 120.0)

    final_terms = []
    final_terms.extend(demand_over_terms)
    if 'mix_under_terms' in locals():
        final_terms.extend(mix_under_terms)
    final_terms.extend(other_terms)
    # v9.4: Fallback fuerte para cerrar contrato en minutos (si existe slack) dentro del epsilon de demanda.
    W_CONTRACT_UNDER_MIN_FINAL = 50_000
    if minutes_under_vars:
        final_terms.extend(W_CONTRACT_UNDER_MIN_FINAL * u for u in minutes_under_vars)
    final_terms.extend(contract_over_terms)

    # -------------------------
    # (NUEVO v9.6.37) "OFF swap" suave: mantener patrón de OFF real del baseline,
    # pero permitir mover algunos OFF cuando eso mejora cobertura (ej. domingos),
    # pagando un costo controlado. No es restricción dura.
    # -------------------------
    try:
        W_OFF_SWAP = int(pd.to_numeric(_get_param_optional(dfs, "w_off_swap", 200_000), errors="coerce") or 200_000)
    except Exception:
        W_OFF_SWAP = 200_000
    if W_OFF_SWAP > 0 and baseline_off_real is not None:
        for (emp, d), base in baseline_off_real.items():
            v = off_real.get((emp, d), None)
            if v is None:
                continue
            if int(base) == 1:
                # base OFF real = 1  => penaliza si lo pierde
                final_terms.append(W_OFF_SWAP * (1 - v))
            else:
                # base OFF real = 0  => penaliza si lo gana
                final_terms.append(W_OFF_SWAP * v)

    # -------------------------
    # (NUEVO v9.6.37) Domingos: balance por DÉFICIT de cobertura (under) por domingo
    # por (org_unit_id, cargo_id). Esto NO cambia la meta de demanda (ya está lockeada),
    # pero ayuda a redistribuir under entre domingos cuando existe libertad (epsilon).
    # Además, actúa por SLOT (sumando under por slots) en vez de contar "personas" por día.
    # -------------------------
    try:
        W_SUN_UNDER_BAL = int(pd.to_numeric(_get_param_optional(dfs, "w_sun_under_balance", 2_500_000), errors="coerce") or 2_500_000)
    except Exception:
        W_SUN_UNDER_BAL = 2_500_000
    try:
        W_SUN_UNDER_COVER = int(pd.to_numeric(_get_param_optional(dfs, "w_sun_under_cover", 1_000_000), errors="coerce") or 1_000_000)
    except Exception:
        W_SUN_UNDER_COVER = 1_000_000

    if sundays and under_vars and (W_SUN_UNDER_BAL > 0 or W_SUN_UNDER_COVER > 0):
        # index under vars by (ou, cargo, date)
        under_by_day = {}
        req_by_day = {}
        for (key, uvar) in under_vars:
            ou, cargo, d, sl = key
            under_by_day.setdefault((ou, cargo, d), []).append(uvar)
            try:
                req_by_day[(ou, cargo, d)] = int(need_total_req.get((ou, cargo, d), req_by_day.get((ou, cargo, d), 0)))
            except Exception:
                pass

        # agrupar por (ou,cargo)
        sun_by_group = {}
        for sd in sundays:
            for (ou, cargo, d) in list(under_by_day.keys()):
                if d == sd:
                    sun_by_group.setdefault((ou, cargo), []).append(sd)

        # construir under_sum por domingo y balancear si demanda dominical estable
        for (ou, cargo), sdays in sun_by_group.items():
            # únicos y ordenados
            sdays = sorted(list(dict.fromkeys(sdays)))
            if len(sdays) <= 1:
                continue

            # demanda dominical total del grupo (suma de slots) por domingo
            dom_reqs = [int(req_by_day.get((ou, cargo, sd), 0)) for sd in sdays]
            avg_req = float(sum(dom_reqs)) / float(len(dom_reqs)) if dom_reqs else 0.0
            if avg_req <= 0:
                continue
            rng = int(max(dom_reqs) - min(dom_reqs)) if dom_reqs else 0
            stable = rng <= max(1, int(round(0.10 * avg_req)))

            # under_sum vars por domingo
            under_day_vars = []
            under_day_max = []
            for sd in sdays:
                ulist = under_by_day.get((ou, cargo, sd), [])
                umax = int(req_by_day.get((ou, cargo, sd), 0))
                uday = model.NewIntVar(0, umax, f"sun_under_sum__{ou}__{cargo}__{sd}")
                model.Add(uday == (sum(ulist) if ulist else 0))
                under_day_vars.append((sd, uday))
                under_day_max.append(umax)

            # Cobertura: reducir under dominical si hay libertad (dentro de epsilon)
            if W_SUN_UNDER_COVER > 0:
                for sd, uday in under_day_vars:
                    final_terms.append(W_SUN_UNDER_COVER * uday)

            # Balance: solo si la demanda dominical es estable
            if stable and W_SUN_UNDER_BAL > 0:
                total_under = model.NewIntVar(0, sum(under_day_max), f"sun_under_total__{ou}__{cargo}")
                model.Add(total_under == sum(ud for _, ud in under_day_vars))
                K = len(under_day_vars)
                for sd, uday in under_day_vars:
                    dev = model.NewIntVar(0, K * max(under_day_max), f"sun_under_dev__{ou}__{cargo}__{sd}")
                    model.Add(dev >= uday * K - total_under)
                    model.Add(dev >= total_under - uday * K)
                    final_terms.append(W_SUN_UNDER_BAL * dev)


    
    # ---- Preferencia de expertise en horarios críticos (suave, no rompe factibilidad) ----
    # Solo actúa DESPUÉS de fijar demanda/contrato (por las restricciones agregadas en etapas 1 y 2a).
    # Empuja a que ALTA se use primero en MAÑANA/NOCHE y luego en INTERMEDIO.
    try:
        W_EXPERTISE = int(pd.to_numeric(_get_param_optional(dfs, "w_expertise", 50), errors="coerce") or 50)
    except Exception:
        W_EXPERTISE = 50

    if W_EXPERTISE > 0:
        exp_reward_terms = []
        for emp in employees:
            esc = int(emp_expertise_score.get(emp, 2))
            if esc <= 0:
                continue
            for d in horizon_dates:
                for s in work_shifts:
                    v = x.get((emp, d, s), None)
                    if v is None:
                        continue
                    st = shift_start_min.get(s, None)
                    mm = int(shift_min.get(s, 0) or 0)
                    if st is None or mm <= 0:
                        continue
                    band = _band_from_minute(int(st))
                    bw = int(crit_weights.get(band, 1))
                    # recompensa proporcional a minutos del turno
                    coef = int(W_EXPERTISE * esc * bw * mm)
                    exp_reward_terms.append(-coef * v)  # negativo porque minimizamos
        if exp_reward_terms:
            final_terms.extend(exp_reward_terms)

    # ---- Política de expertise: mentoría (BAJA no solo) + reparto de ALTA por bandas (suave) ----
    # Objetivos suaves en etapa 3 (no rompen factibilidad). Aplican por (org_unit_id, cargo_id).
    try:
        # 1) Penalizar "BAJA solo" en banda (por slot) y falta de ALTA/mentor según política.
        #    Nota: se evalúa por slot de necesidad (sl) usando la hora del slot (si es HH:MM).
        exp_pol_terms = []
        # Preindex de empleados por grupo y por nivel (para acelerar)
        emps_by_group = {}
        for emp in employees:
            ou = str(emp_ou.get(emp, "")).strip().upper()
            cg = str(emp_cargo_id.get(emp, "")).strip().upper()
            emps_by_group.setdefault((ou, cg), []).append(emp)

        # Helper: lista de variables x que cubren (ou,cg,d,sl) filtradas por expertise
        def _cov_terms_for_slot(ou: str, cg: str, d, sl, want_levels: Set[str]):
            terms = []
            for emp in emps_by_group.get((ou, cg), []):
                if emp_expertise.get(emp, "MEDIA") not in want_levels:
                    continue
                for s in work_shifts:
                    if sl in cover_map.get(s, set()):
                        v = x.get((emp, d, s), None)
                        if v is not None:
                            terms.append(v)
            return terms

        # Slots con demanda (req>0) por banda
        for (ou, cg, d, sl), req in required_need.items():
            if req <= 0:
                continue
            # hora de slot (si no se puede parsear, saltamos)
            st = _to_min(str(sl))
            if st is None:
                continue
            band = _band_from_minute(int(st))
            pol = expertise_policy.get(band, None)
            if pol is None:
                continue

            mentor_min = int(pol.get("mentor_min", 0) or 0)
            alta_min = int(pol.get("alta_min", 0) or 0)
            allow_baja_solo = int(pol.get("allow_baja_solo", 0) or 0)
            peso_baja_solo = int(pol.get("peso_baja_solo", 0) or 0)

            # contamos presencia por nivel en este slot (en cobertura)
            # mentor = ALTA o MEDIA
            mentor_terms = _cov_terms_for_slot(str(ou).upper(), str(cg).upper(), d, sl, {"ALTA", "MEDIA"})
            baja_terms = _cov_terms_for_slot(str(ou).upper(), str(cg).upper(), d, sl, {"BAJA"})
            alta_terms = _cov_terms_for_slot(str(ou).upper(), str(cg).upper(), d, sl, {"ALTA"})

            max_cov = len(emps_by_group.get((str(ou).upper(), str(cg).upper()), []))
            if max_cov <= 0:
                continue

            mentor_cnt = model.NewIntVar(0, max_cov, f"mentor_cnt__{ou}__{cg}__{d}__{sl}")
            baja_cnt = model.NewIntVar(0, max_cov, f"baja_cnt__{ou}__{cg}__{d}__{sl}")
            alta_cnt = model.NewIntVar(0, max_cov, f"alta_cnt__{ou}__{cg}__{d}__{sl}")

            model.Add(mentor_cnt == (sum(mentor_terms) if mentor_terms else 0))
            model.Add(baja_cnt == (sum(baja_terms) if baja_terms else 0))
            model.Add(alta_cnt == (sum(alta_terms) if alta_terms else 0))

            # BAJA solo => baja_cnt>=1 y mentor_cnt==0
            baja_ge1 = model.NewBoolVar(f"baja_ge1__{ou}__{cg}__{d}__{sl}")
            mentor_ge1 = model.NewBoolVar(f"mentor_ge1__{ou}__{cg}__{d}__{sl}")
            model.Add(baja_cnt >= 1).OnlyEnforceIf(baja_ge1)
            model.Add(baja_cnt == 0).OnlyEnforceIf(baja_ge1.Not())
            model.Add(mentor_cnt >= 1).OnlyEnforceIf(mentor_ge1)
            model.Add(mentor_cnt == 0).OnlyEnforceIf(mentor_ge1.Not())

            baja_solo = model.NewBoolVar(f"baja_solo__{ou}__{cg}__{d}__{sl}")
            model.AddBoolAnd([baja_ge1, mentor_ge1.Not()]).OnlyEnforceIf(baja_solo)
            model.AddBoolOr([baja_ge1.Not(), mentor_ge1]).OnlyEnforceIf(baja_solo.Not())

            if allow_baja_solo == 0 and peso_baja_solo > 0:
                exp_pol_terms.append(int(peso_baja_solo) * baja_solo)

            # Faltas de mentor/ALTA como slack (suave)
            if mentor_min > 0 and peso_baja_solo > 0:
                slack_m = model.NewIntVar(0, mentor_min, f"slack_mentor__{ou}__{cg}__{d}__{sl}")
                model.Add(slack_m >= mentor_min - mentor_cnt)
                model.Add(slack_m >= 0)
                exp_pol_terms.append(int(peso_baja_solo) * slack_m)

            if alta_min > 0 and peso_baja_solo > 0:
                slack_a = model.NewIntVar(0, alta_min, f"slack_alta__{ou}__{cg}__{d}__{sl}")
                model.Add(slack_a >= alta_min - alta_cnt)
                model.Add(slack_a >= 0)
                exp_pol_terms.append(int(peso_baja_solo) * slack_a)

        if exp_pol_terms:
            final_terms.extend(exp_pol_terms)

        # 2) Reparto de ALTA por bandas (por semana y grupo): evitar concentración.
        exp_spread_terms = []
        # agrupar fechas por semana (lunes)
        dates_by_week2 = {}
        for dd in horizon_dates:
            dts = pd.to_datetime(dd).normalize()
            wk_start = (dts - pd.Timedelta(days=int(dts.weekday()))).normalize()
            dates_by_week2.setdefault(wk_start, []).append(dd)

        for (ou, cg), emps_g in emps_by_group.items():
            if len(emps_g) <= 1:
                continue
            for wk_start, dlist in dates_by_week2.items():
                # construir sumas ALTA por banda contando cobertura por slot
                alta_band = {}
                for b in ("MANANA", "INTERMEDIO", "NOCHE"):
                    alta_band[b] = model.NewIntVar(0, 10**9, f"alta_band__{ou}__{cg}__{wk_start.date()}__{b}")
                total_alta = model.NewIntVar(0, 10**9, f"alta_total__{ou}__{cg}__{wk_start.date()}")

                band_terms = {"MANANA": [], "INTERMEDIO": [], "NOCHE": []}
                total_terms = []
                for dd in dlist:
                    for (ou2, cg2, d2, sl), req in required_need.items():
                        if req <= 0:
                            continue
                        if str(ou2).strip().upper() != ou or (cg != ANY_CARGO and str(cg2).strip().upper() != cg):
                            continue
                        if d2 != dd:
                            continue
                        st = _to_min(str(sl))
                        if st is None:
                            continue
                        b = _band_from_minute(int(st))
                        alta_terms = _cov_terms_for_slot(ou, cg, dd, sl, {"ALTA"})
                        if alta_terms:
                            band_terms[b].extend(alta_terms)
                            total_terms.extend(alta_terms)

                model.Add(alta_band["MANANA"] == (sum(band_terms["MANANA"]) if band_terms["MANANA"] else 0))
                model.Add(alta_band["INTERMEDIO"] == (sum(band_terms["INTERMEDIO"]) if band_terms["INTERMEDIO"] else 0))
                model.Add(alta_band["NOCHE"] == (sum(band_terms["NOCHE"]) if band_terms["NOCHE"] else 0))
                model.Add(total_alta == (sum(total_terms) if total_terms else 0))

                # penalizamos sum_b |alta_band[b]*3 - total_alta|
                w_spread = int(expertise_policy.get("MANANA", {}).get("peso_concentracion_alta", 2_000_000) or 2_000_000)
                for b in ("MANANA", "INTERMEDIO", "NOCHE"):
                    diff = model.NewIntVar(0, 10**9, f"alta_diff__{ou}__{cg}__{wk_start.date()}__{b}")
                    tmp = model.NewIntVar(-10**9, 10**9, f"alta_tmp__{ou}__{cg}__{wk_start.date()}__{b}")
                    model.Add(tmp == alta_band[b] * 3 - total_alta)
                    model.AddAbsEquality(diff, tmp)
                    exp_spread_terms.append(w_spread * diff)

        if exp_spread_terms:
            final_terms.extend(exp_spread_terms)

        
        # 3) Domingos (general):
        #    (a) Cobertura dominical por (org_unit_id,cargo): penalizar que el domingo quede "bajo" respecto
        #        de la demanda dominical de ese mismo domingo (sin hardcode de cantidades).
        #    (b) Balance dominical (suave) SOLO cuando la demanda dominical del grupo es "estable" en el mes.
        #        Esto evita tuneo al case y hace que se active solo cuando aplica.
        sun_terms = []
        W_SUN_COVER = int(pd.to_numeric(_get_param_optional(dfs, "w_sun_cover", 3_000_000), errors="coerce") or 3_000_000)
        W_SUN_WORK_BALANCE = int(pd.to_numeric(_get_param_optional(dfs, "w_sun_work_balance", 2_000_000), errors="coerce") or 2_000_000)
        if W_SUN_COVER > 0 or W_SUN_WORK_BALANCE > 0:
            sundays = [d for d in horizon_dates if pd.Timestamp(d).dayofweek == 6]
            if len(sundays) >= 2:
                for (ou, cg), emps_g in emps_by_group.items():
                    n_g = len(emps_g)
                    if n_g <= 1:
                        continue

                    # ----- demanda dominical del grupo por domingo (target) -----
                    req_by_sun = []
                    for d in sundays:
                        req_total = 0
                        for (ou2, cg2, d2, sl), req in required_need.items():
                            if req <= 0:
                                continue
                            if str(ou2).strip().upper() != str(ou).strip().upper():
                                continue
                            if str(cg2).strip().upper() != str(cg).strip().upper():
                                continue
                            if d2 != d:
                                continue
                            req_total += int(req)
                        req_by_sun.append(req_total)

                    # si no hay demanda dominical para este grupo, no aplicamos nada
                    if sum(req_by_sun) <= 0:
                        continue

                    # ----- work_dom: cantidad de personas trabajando ese domingo (cualquier turno de trabajo) -----
                    work_cnt = []
                    for d in sundays:
                        cnt = model.NewIntVar(0, n_g, f"sun_work_cnt__{ou}__{cg}__{d}")
                        terms = []
                        for emp in emps_g:
                            terms.append(sum(x[(emp, d, s)] for s in work_shifts))
                        model.Add(cnt == (sum(terms) if terms else 0))
                        work_cnt.append(cnt)

                    # ----- (a) Cobertura dominical respecto de demanda (under_dom = max(0, req_total - work_cnt)) -----
                    if W_SUN_COVER > 0:
                        # Peso adaptativo: escalar por demanda promedio para que sea "comparables" entre grupos
                        avg_req = max(1, int(round(sum(req_by_sun) / len(req_by_sun))))
                        w_cover = int(W_SUN_COVER * avg_req)
                        for i, d in enumerate(sundays):
                            req_total = int(req_by_sun[i])
                            if req_total <= 0:
                                continue
                            under = model.NewIntVar(0, req_total, f"sun_under__{ou}__{cg}__{d}")
                            tmp = model.NewIntVar(-n_g, req_total, f"sun_under_tmp__{ou}__{cg}__{d}")
                            model.Add(tmp == req_total - work_cnt[i])
                            model.AddMaxEquality(under, [tmp, 0])
                            sun_terms.append(w_cover * under)

                    # ----- (b) Balance dominical SOLO si la demanda dominical es estable -----
                    if W_SUN_WORK_BALANCE > 0:
                        # estabilidad: rango <= max(1, 10% del promedio)
                        avg_req = float(sum(req_by_sun)) / float(len(req_by_sun))
                        tol = max(1.0, 0.10 * avg_req)
                        if (max(req_by_sun) - min(req_by_sun)) <= tol:
                            # Balance por promedio de trabajo (sin hardcode): minimizar dispersion de work_cnt
                            total = model.NewIntVar(0, n_g * len(sundays), f"sun_work_total__{ou}__{cg}")
                            model.Add(total == sum(work_cnt))
                            # Peso adaptativo: escala por tamaño de grupo (más pequeño => más notorio)
                            # y por demanda promedio.
                            base = float(W_SUN_WORK_BALANCE) * max(1.0, avg_req)
                            scale = 1.0
                            if n_g <= 10:
                                scale = 1.5
                            elif n_g >= 40:
                                scale = 0.7
                            w_bal = int(base * scale)
                            for i, d in enumerate(sundays):
                                diff = model.NewIntVar(0, n_g * len(sundays), f"sun_work_diff__{ou}__{cg}__{d}")
                                tmp = model.NewIntVar(-n_g * len(sundays), n_g * len(sundays), f"sun_work_tmp__{ou}__{cg}__{d}")
                                model.Add(tmp == work_cnt[i] * len(sundays) - total)
                                model.AddAbsEquality(diff, tmp)
                                sun_terms.append(w_bal * diff)

        if sun_terms:
            final_terms.extend(sun_terms)
    except Exception:
        # No romper solución si algo falla en los objetivos suaves de expertise/fairness
        pass
    model.Minimize(sum(final_terms) if final_terms else 0)
    status3 = solver3.Solve(model)

    if status3 in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        best_x = _capture_x(solver3)
        best_stage = 'etapa3_final'
    # else: fallback silencioso con best_x anterior

    # Guardamos diagnóstico de estado
    # -------------------------
    # Diagnóstico de solver (etapas)
    # -------------------------
    diag_rows = []

    # Etapa 1
    diag_rows.append({'etapa': '1_demanda', 'status': _status_name(status1), 'objective': (int(round(solver1.ObjectiveValue())) if status1 in (cp_model.OPTIMAL, cp_model.FEASIBLE) else None)})

    # Etapa 2a (contrato) - puede ser deep
    diag_rows.append({'etapa': '2a_contrato_short', 'status': _status_name(status2a), 'objective': (int(obj2a_val) if obj2a_val is not None else None)})

    # Si usamos deep, lo dejamos explícito
    if best_stage == 'etapa2a_contrato_deep':
        diag_rows.append({'etapa': 'etapa2a_contrato_deep', 'status': _status_name(status2a), 'objective': (int(obj2a_val) if obj2a_val is not None else None)})

    # Etapas opcionales (si se ejecutan)
    if status2b is not None:
        diag_rows.append({'etapa': '2b_contrato_min', 'status': _status_name(status2b), 'objective': (int(round(solver2b.ObjectiveValue())) if (solver2b is not None and status2b in (cp_model.OPTIMAL, cp_model.FEASIBLE)) else None)})
    else:
        diag_rows.append({'etapa': '2b_contrato_min', 'status': 'SKIPPED', 'objective': None})

    if status2c is not None:
        diag_rows.append({'etapa': '2c_turnos_cortos', 'status': _status_name(status2c), 'objective': (int(round(solver2c.ObjectiveValue())) if (solver2c is not None and status2c in (cp_model.OPTIMAL, cp_model.FEASIBLE)) else None)})
    else:
        diag_rows.append({'etapa': '2c_turnos_cortos', 'status': 'SKIPPED', 'objective': None})

    # Etapa 3
    diag_rows.append({'etapa': '3_resto', 'status': _status_name(status3), 'objective': (int(round(solver3.ObjectiveValue())) if (solver3 is not None and status3 in (cp_model.OPTIMAL, cp_model.FEASIBLE)) else None)})

    # Marcador de etapa usada
    diag_rows.append({'etapa': 'usada', 'status': best_stage, 'objective': None})


    # Escribe diagnóstico de solver (no rompe si falla)

    # -------------------------
    # Post-proceso de diagnóstico (robusto)
    # -------------------------
    try:
        # 1) Normalizar NaN
        for r in diag_rows:
            st = r.get("status", None)
            if st is None or (isinstance(st, float) and pd.isna(st)):
                r["status"] = "SKIPPED"
            obj = r.get("objective", None)
            if isinstance(obj, float) and pd.isna(obj):
                r["objective"] = None

        # 2) Detectar etapa usada
        used = None
        for r in diag_rows:
            if str(r.get("etapa", "")).strip() == "usada":
                used = str(r.get("status", "")).strip()  # aquí se guarda el nombre de etapa usada
                break

        # 3) Agregar fila real para etapa2a_contrato_deep si corresponde
        if used == "etapa2a_contrato_deep":
            has_deep = any(str(r.get("etapa","")).strip() == "etapa2a_contrato_deep" for r in diag_rows)
            if not has_deep:
                proxy_status = None
                for r in diag_rows:
                    if str(r.get("etapa","")).strip() == "2a_contrato_short":
                        proxy_status = str(r.get("status","")).strip()
                        break
                if not proxy_status or proxy_status in ("", "SKIPPED", "nan"):
                    proxy_status = "FEASIBLE"

                short_weeks = int((contrato["under_min"].fillna(0) > 0).sum()) if "under_min" in contrato.columns else 0
                under_total = int(round(float(contrato["under_min"].fillna(0).sum()))) if "under_min" in contrato.columns else 0
                obj_contract = int(short_weeks) * 1_000_000_000 + int(under_total)

                idx_usada = next((i for i, rr in enumerate(diag_rows) if str(rr.get("etapa","")).strip()=="usada"), len(diag_rows))
                diag_rows.insert(idx_usada, {"etapa": "etapa2a_contrato_deep", "status": proxy_status, "objective": obj_contract})
    except Exception:
        pass

    try:
        pd.DataFrame(diag_rows).to_csv(out_dir / "solver_diag.csv", index=False, encoding="utf-8-sig")
        # Diagnóstico de reglas de apertura/cierre por cargo
        try:
            if 'roles_diag_rows' in locals():
                pd.DataFrame(roles_diag_rows).to_csv(out_dir / 'roles_diag.csv', index=False, encoding='utf-8-sig')
        except Exception:
            pass
        (out_dir / "solver_diag.txt").write_text(
            "\n".join([f"{r['etapa']}: {r['status']} {r.get('objective','')}" for r in diag_rows]),
            encoding="utf-8",
        )
    except Exception:
        pass

# -------------------------
    # Output plan (NO CAMBIAR columnas)
    # -------------------------
        # -------------------------
    # Output plan (NO CAMBIAR columnas)
    # -------------------------
    # Nota: "SALIENTE" es solo etiqueta de salida para días calendario donde el turno del día anterior
    # cruza medianoche y hoy no inicia un turno (se ve "LIBRE" en x, pero NO es libre real).
    cross_shifts_out = [
        s for s in work_shifts
        if int(shift_min.get(s, 0)) > 0 and int(shift_end_min.get(s, 0)) < int(shift_start_min.get(s, 0))
    ]

    plan_rows = []
    for emp in employees:
        prev_chosen = None
        for i, d in enumerate(horizon_dates):
            chosen = "LIBRE"
            for s in all_shifts:
                if best_x.get((emp, d, s), 0) == 1:
                    chosen = s
                    break

            chosen_out = chosen
            es_saliente = 0
            if chosen == "LIBRE" and ((i > 0 and prev_chosen in cross_shifts_out) or (i == 0 and int(prev_cross_before_start.get(emp, 0) or 0) == 1)):
                chosen_out = "SALIENTE"
                es_saliente = 1

            plan_rows.append(
                {
                    "employee_id": emp,
                    "fecha": d,
                    "dia_semana": horizon_dow[d],
                    "org_unit_id": emp_ou[emp],
                    "cargo": emp_cargo_label.get(emp, emp_cargo_id[emp]),
                    "shift_id": chosen_out,
                    "es_saliente": es_saliente,
                    "nota": best_stage,
                }
            )

            prev_chosen = chosen

    plan = pd.DataFrame(plan_rows)


    # -------------------------
    # Reporte de expertise (no rompe outputs existentes)
    # -------------------------
    # Generamos SIEMPRE el archivo reporte_expertise.csv (aunque quede vacío) para QA/BI.
    # Si algo falla, dejamos evidencia en solver_diag_extra (sin romper columnas existentes).
    rep_cols = [
        "org_unit_id", "cargo", "week_start", "banda",
        "minutos_cubiertos", "minutos_cubiertos_alta", "minutos_cubiertos_media", "minutos_cubiertos_baja", "pct_alta"
    ]
    rep_path = out_dir / "reporte_expertise.csv"
    try:
        # acumulamos minutos por banda y expertise usando la malla (asignaciones) + cobertura por slots
        rows = []
        # slots cubiertos por turno (ya calculado arriba como cover_map) usa minutos desde medianoche
        for r in plan_rows:
            sid = str(r.get("shift_id", "")).strip().upper()
            if (not sid) or (sid in ("LIBRE", "SALIENTE")) or (sid in ABS_CODES):
                continue
            # ignorar si no es turno de trabajo
            if sid not in cover_map:
                continue

            emp = _norm_empid(r.get("employee_id", ""))
            d = r.get("fecha", None)
            if d is None:
                continue
            ou = str(r.get("org_unit_id", "")).strip().upper()
            cg = str(r.get("cargo", "")).strip()
            exp = str(emp_expertise.get(emp, "MEDIA")).strip().upper()
            if exp not in ("ALTA", "MEDIA", "BAJA"):
                exp = "MEDIA"

            # Lunes de la semana (ISO): date - dayofweek
            dd = pd.to_datetime(d)
            week_start = (dd.normalize() - pd.to_timedelta(int(dd.dayofweek), unit="D")).date()

            for sl in cover_map.get(sid, set()):
                band = _band_from_minute(int(sl))
                rows.append(
                    {
                        "org_unit_id": ou,
                        "cargo": cg,
                        "week_start": str(week_start),
                        "banda": band,
                        "expertise": exp,
                        "minutos": int(slot_min),
                    }
                )

        if rows:
            df = pd.DataFrame(rows)
            piv = (
                df.pivot_table(
                    index=["org_unit_id", "cargo", "week_start", "banda"],
                    columns=["expertise"],
                    values="minutos",
                    aggfunc="sum",
                    fill_value=0,
                )
                .reset_index()
            )
            for col in ["ALTA", "MEDIA", "BAJA"]:
                if col not in piv.columns:
                    piv[col] = 0
            piv["minutos_cubiertos"] = piv["ALTA"] + piv["MEDIA"] + piv["BAJA"]
            piv["minutos_cubiertos_alta"] = piv["ALTA"]
            piv["minutos_cubiertos_media"] = piv["MEDIA"]
            piv["minutos_cubiertos_baja"] = piv["BAJA"]
            piv["pct_alta"] = piv.apply(
                lambda rr: (100.0 * rr["minutos_cubiertos_alta"] / rr["minutos_cubiertos"])
                if rr["minutos_cubiertos"] > 0
                else 0.0,
                axis=1,
            )
            piv = piv[rep_cols]
            piv.to_csv(rep_path, index=False, encoding="utf-8-sig")

            # resumen global para certificado
            man = piv[piv["banda"] == "MANANA"]
            noc = piv[piv["banda"] == "NOCHE"]
            pct_alta_man = (
                float(100.0 * man["minutos_cubiertos_alta"].sum() / man["minutos_cubiertos"].sum())
                if man["minutos_cubiertos"].sum() > 0
                else 0.0
            )
            pct_alta_noc = (
                float(100.0 * noc["minutos_cubiertos_alta"].sum() / noc["minutos_cubiertos"].sum())
                if noc["minutos_cubiertos"].sum() > 0
                else 0.0
            )
            import json as _json

            (out_dir / "expertise_summary.json").write_text(
                _json.dumps({"pct_alta_manana": pct_alta_man, "pct_alta_noche": pct_alta_noc}, ensure_ascii=False),
                encoding="utf-8",
            )
        else:
            # Sin filas: igual creamos el archivo vacío (con headers).
            pd.DataFrame(columns=rep_cols).to_csv(rep_path, index=False, encoding="utf-8-sig")
    except Exception as _e:
        # Dejar el archivo vacío y registrar el error
        try:
            pd.DataFrame(columns=rep_cols).to_csv(rep_path, index=False, encoding="utf-8-sig")
        except Exception:
            pass
        try:
            (out_dir / "solver_diag_extra.txt").write_text(f"reporte_expertise error: {_e}\n", encoding="utf-8")
        except Exception:
            pass

# brechas
    brechas = []
    for dts in horizon:
        d = dts.date()
        dia_sem = DOW_MAP[dts.dayofweek]
        if use_demanda_unidad:
            need_day = demanda_unidad.loc[demanda_unidad["dia_semana"] == dia_sem]
        else:
            need_day = need.loc[need["dia_semana"] == dia_sem]
        if need_day.empty:
            continue

        if use_demanda_unidad:
            pairs = [(str(ou).strip().upper(), ANY_CARGO) for ou in need_day[["org_unit_id"]].drop_duplicates()["org_unit_id"].tolist()]
        else:
            pairs = [(str(ou).strip().upper(), str(cargo).strip().upper()) for (ou, cargo) in need_day[["org_unit_id", "cargo_id"]].drop_duplicates().itertuples(index=False, name=None)]

        for (ou, cargo) in pairs:
            ou = str(ou).strip().upper()
            cargo = str(cargo).strip().upper()

            if exc_close.get((d, ou, cargo), False) or (use_demanda_unidad and exc_close.get((d, ou, ANY_CARGO), False)):
                continue

            if use_demanda_unidad:
                need_uc = need_day.loc[
                    (need_day["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                ].copy()
            else:
                need_uc = need_day.loc[
                    (need_day["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                    & (need_day["cargo_id"].astype(str).str.strip().str.upper() == cargo)
                ].copy()

            # v3: Usar versión dual para obtener mínimos e ideales
            if use_demanda_unidad:
                req_slots_min, req_slots_ideal, _, _ = _build_required_slots_dual(need_uc, slot_min)
                req_slots = req_slots_min  # Mantener compatibilidad
            else:
                req_slots, _, _ = _build_required_slots(need_uc, slot_min)
                req_slots_min = req_slots
                req_slots_ideal = req_slots
            if not req_slots_min:
                continue

            # FIX v9.9: Cuando cargo == "__ALL__", no filtrar por cargo (demanda_unidad mode)
            if cargo == ANY_CARGO or cargo == "__ALL__":
                plan_day = plan.loc[
                    (plan["fecha"] == d)
                    & (plan["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                ].copy()
            else:
                plan_day = plan.loc[
                    (plan["fecha"] == d)
                    & (plan["org_unit_id"].astype(str).str.strip().str.upper() == ou)
                    & (plan["cargo"].astype(str).str.strip().str.upper() == cargo)
                ].copy()

            for sl_min, req in sorted(req_slots_min.items(), key=lambda x: x[0]):
                req_ideal = req_slots_ideal.get(sl_min, req)  # v3: ideal para este slot
                covered = 0
                for _, pr in plan_day.iterrows():
                    sh = str(pr["shift_id"]).strip().upper()
                    if sh == "" or sh == "LIBRE" or sh in ABS_CODES:
                        continue
                    if sl_min in cover_map.get(sh, set()):
                        covered += 1

                faltan_min = max(0, int(req) - covered)  # Faltantes vs mínimo
                faltan_ideal = max(0, int(req_ideal) - covered)  # v3: Faltantes vs ideal
                t = pd.Timestamp(d) + pd.Timedelta(minutes=sl_min)
                t_end = t + pd.Timedelta(minutes=slot_min)

                # Diagnóstico de brecha: ¿hay turnos en pool que cubran este slot y cuántos empleados son elegibles?
                pool_list = (pool_idx_ou.get((ou, dia_sem), []) if cargo == "__ALL__" else pool_idx.get((ou, cargo, dia_sem), []))
                pool_shifts_covering = 0
                for sh in pool_list:
                    if sl_min in cover_map.get(sh, set()):
                        pool_shifts_covering += 1

                eligible_emps = 0
                emp_list = (emps_by_ou.get(ou, []) if cargo == ANY_CARGO else emps_by_oucargo.get((ou, cargo), []))
                for emp in emp_list:
                    allowed_set = allowed_emp_day.get((emp, d), set())
                    ok = False
                    for sh in allowed_set:
                        if sh == "LIBRE":
                            continue
                        if int(shift_min.get(sh, 0)) <= 0:
                            continue
                        if sl_min in cover_map.get(sh, set()):
                            ok = True
                            break
                    if ok:
                        eligible_emps += 1

                if int(req) <= 0:
                    diag = "NO_NEED"
                elif pool_shifts_covering == 0:
                    diag = "NO_SHIFT_COVERS_SLOT"
                elif eligible_emps == 0:
                    diag = "NO_EMP_ELIGIBLE_FOR_SLOT"
                elif eligible_emps < int(req):
                    diag = "NOT_ENOUGH_ELIGIBLE"
                else:
                    diag = "SOLVER_TRADEOFF_OR_OTHER_CONSTRAINT"

                # v3: Diagnóstico adicional para ideal vs mínimo
                if faltan_min > 0:
                    diag_ideal = "BAJO_MINIMO"
                elif faltan_ideal > 0:
                    diag_ideal = "BAJO_IDEAL"
                else:
                    diag_ideal = "SOBRE_IDEAL"

                brechas.append(
                    {
                        "fecha": d,
                        "dia_semana": dia_sem,
                        "org_unit_id": ou,
                        "cargo": cargo,
                        "tramo_inicio": t.strftime("%H:%M"),
                        "tramo_fin": t_end.strftime("%H:%M"),
                        "requeridos_min_personas": int(req),           # v3: renombrado para claridad
                        "requeridos_ideal_personas": int(req_ideal),   # v3: NUEVO
                        "cubiertos_personas": int(covered),
                        "over_vs_min_personas": max(0, int(covered) - int(req)),
                        "over_vs_ideal_personas": max(0, int(covered) - int(req_ideal)),  # v3: NUEVO
                        "requeridos_min_persona_min": int(req) * slot_min,
                        "requeridos_ideal_persona_min": int(req_ideal) * slot_min,        # v3: NUEVO
                        "cubiertos_persona_min": int(covered) * slot_min,
                        "faltantes_vs_min_personas": faltan_min,                          # v3: renombrado
                        "faltantes_vs_ideal_personas": faltan_ideal,                      # v3: NUEVO
                        "faltantes_vs_min_persona_min": faltan_min * slot_min,
                        "faltantes_vs_ideal_persona_min": faltan_ideal * slot_min,        # v3: NUEVO
                        "pool_shifts_covering_slot": pool_shifts_covering,
                        "eligible_emps_for_slot": eligible_emps,
                        "diagnostic": diag,
                        "diagnostic_ideal": diag_ideal,                                   # v3: NUEVO
                        # Mantener compatibilidad con nombres antiguos
                        "requeridos_personas": int(req),
                        "over_personas": max(0, int(covered) - int(req)),
                        "requeridos_persona_min": int(req) * slot_min,
                        "over_persona_min": max(0, int(covered) - int(req)) * slot_min,
                        "faltantes_personas": faltan_min,
                        "faltantes_persona_min": faltan_min * slot_min,
                    }
                )

    brechas_df = pd.DataFrame(brechas)


    # Pre-cálculo rápido para contrato: minutos por shift desde el plan
    plan_min = plan.copy()
    plan_min['shift_id'] = plan_min['shift_id'].astype(str).str.strip().str.upper()
    plan_min['minutos_efectivos'] = plan_min['shift_id'].map(lambda s: int(shift_min.get(s, 0)))
    plan_min['es_saliente'] = (plan_min['shift_id'] == 'SALIENTE').astype(int)
    # LIBRE real para conteos (no incluye saliente)
    plan_min['libre_real'] = ((plan_min['shift_id'] == 'LIBRE') & (plan_min['es_saliente'] == 0)).astype(int)
    plan_min['week_index'] = (plan_min['fecha'].rank(method='dense').astype(int) - 1) // 7
    # OJO: week_index asume que horizon_dates está ordenado y son múltiplos de 7 días.

    # -------------------------
    # Reporte de cumplimiento de contrato (diagnóstico)
    # -------------------------
    contrato_rows = []
    for emp in employees:
        cap = int(cap_week_emp.get(emp, legal_weekly_cap_min_default))
        contract = int(contract_week_emp.get(emp, cap))
        desired_workdays = int(dias_trab_obj_emp.get(emp, 6))
        desired_workdays = max(1, min(7, desired_workdays))
        base_off = 7 - desired_workdays
        for w in range(weeks):
            days_w = horizon_dates[w * 7 : (w + 1) * 7]
            tgt = int(target_min_week.get((emp, w), contract))
            planned = int(plan_min[(plan_min['employee_id'] == emp) & (plan_min['week_index'] == w)]['minutos_efectivos'].sum())
            under = max(0, tgt - planned)
            aus_days = int(absent_days_week.get((emp, w), 0))

            # Conteos desde plan
            week_plan = plan_min[(plan_min['employee_id'] == emp) & (plan_min['week_index'] == w)]
            off_days = int(week_plan['libre_real'].sum())
            worked_days = int(((week_plan['minutos_efectivos'] > 0) | (week_plan['es_saliente'] == 1)).sum())

            # cota superior simple: máximo por día según allowed (después de filtros)
            day_max = []
            days_no_work_option = 0
            for d in days_w:
                allowed_set = allowed_emp_day.get((emp, d), set(work_shifts + ['LIBRE']))
                m = 0
                for s in allowed_set:
                    if s == "LIBRE":
                        continue
                    if s in ABS_CODES:
                        continue
                    mm = int(shift_min.get(s, 0))
                    if mm > m:
                        m = mm
                if m <= 0:
                    days_no_work_option += 1
                day_max.append(m)

            # Máximo trabajable por semana según mínimos de libres (aprox).
            max_work_days = max(0, 7 - base_off)  # 6x1 => 6, 5x2 => 5
            # Si hay días sin opción de trabajo, esta cota se ajusta sola al ordenar (son 0).
            max_possible = int(sum(sorted(day_max, reverse=True)[:max_work_days])) if day_max else 0

            if under <= 0:
                reason = "OK"
                detail = ""
            else:
                if max_possible < tgt:
                    reason = "INFEASIBLE_PARA_CONTRATO"
                    detail = (
                        f"Cota max est {max_possible} < target {tgt}. "
                        f"Base_off={base_off}, aus_days={aus_days}, dias_sin_opcion_trabajo={days_no_work_option}. "
                        f"Revisa pool/filtros (ventana demanda, descanso, restricciones) y duraciones de turnos."
                    )
                else:
                    reason = "TRADEOFF_GLOBAL"
                    detail = (
                        "Hay capacidad por persona (cota sugiere que podría), pero se sacrificó por demanda/otras reglas globales. "
                        "Si quieres priorizar contrato sobre demanda, invierte las etapas o permite un epsilon de demanda under."
                    )

            contrato_rows.append({
                "employee_id": emp,
                "week_index": w,
                "week_start": str(days_w[0]) if days_w else "",
                "org_unit_id": emp_ou.get(emp, ""),
                "cargo_id": emp_cargo_id.get(emp, ""),
                "target_min": tgt,
                "planned_min": planned,
                "under_min": under,
                "contract_min": contract,
                "cap_week_min": cap,
                "desired_workdays": desired_workdays,
                "base_off": base_off,
                "aus_days": aus_days,
                "worked_days": worked_days,
                "off_days": off_days,
                "days_no_work_option": days_no_work_option,
                "max_possible_est_min": max_possible,
                "max_day_min": max(day_max) if day_max else 0,
                "reason": reason,
                "detail": detail,
            })

    contrato_df = pd.DataFrame(contrato_rows)

    # ═══════════════════════════════════════════════════════════════════════════
    # POST-PROCESO: Asignación inteligente de colaciones (AUTOMÁTICO)
    # Se activa si hay turnos con colación definida en el shift_id (ej: S_0730_1600_60)
    # ═══════════════════════════════════════════════════════════════════════════
    
    # Detectar si hay turnos que necesitan colación
    turnos_con_colacion = 0
    for _, row in plan.iterrows():
        sid = str(row.get('shift_id', '')).strip().upper()
        if sid.startswith('S_') and _col_parse_break_minutes(sid) > 0:
            turnos_con_colacion += 1
    
    if turnos_con_colacion > 0:
        try:
            print(f"[COLACIONES] Detectados {turnos_con_colacion} turnos con colación definida")
            print("[COLACIONES] Iniciando asignación inteligente...")
            
            # Asignar colaciones usando brechas (excedente real del solver)
            plan = _asignar_colaciones_inteligente(
                plan=plan,
                shift_times=shift_times,
                brechas_df=brechas_df,
                slot_min=slot_min,
                verbose=True,
            )
            
            # Generar diagnóstico
            diag_col = _generar_diagnostico_colaciones(
                plan=plan,
                shift_times=shift_times,
                brechas_df=brechas_df,
                slot_min=slot_min,
            )
            
            # Guardar diagnóstico
            diag_col_path = out_dir / "diagnostico_colaciones.csv"
            diag_col.to_csv(diag_col_path, index=False, encoding="utf-8-sig")
            print(f"[COLACIONES] Diagnóstico guardado en {diag_col_path}")
            
            # Alertas de slots bajo mínimo
            alertas = diag_col[diag_col["alerta"] != ""]
            if not alertas.empty:
                print(f"[COLACIONES] ⚠️ {len(alertas)} slots bajo mínimo operativo:")
                for _, row in alertas.head(10).iterrows():
                    print(f"  - {row['fecha']} {row['slot']}: "
                          f"dotación={row['dotacion_real']}, mín={row['requeridos_min']}")
            else:
                print("[COLACIONES] ✓ Todas las colaciones respetan el mínimo operativo")
                
        except Exception as e:
            print(f"[COLACIONES] Error en asignación: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("[COLACIONES] No hay turnos con colación definida, saltando asignación")

    return plan, brechas_df, contrato_df

def save_outputs(plan: pd.DataFrame, brechas: pd.DataFrame, contrato: pd.DataFrame, out_dir: Path) -> None:
    out_plan = out_dir / "plan_mensual.xlsx"
    out_brechas = out_dir / "reporte_brechas.xlsx"

    with pd.ExcelWriter(out_plan, engine="openpyxl") as w:
        plan.to_excel(w, sheet_name="PlanMensual", index=False)

    with pd.ExcelWriter(out_brechas, engine="openpyxl") as w:
        brechas.to_excel(w, sheet_name="ReporteBrechas", index=False)
        contrato.to_excel(w, sheet_name="ReporteContrato", index=False)

    # extra (no rompe)
    try:
        plan.to_csv(out_dir / "plan_mensual.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass
    try:
        brechas.to_csv(out_dir / "reporte_brechas.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass
    try:
        contrato.to_csv(out_dir / "reporte_contrato.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass


    # -------------------------
    # Certificado ejecutivo (explicabilidad)
    # -------------------------
    try:
        diag_path = out_dir / "solver_diag.csv"
        diag_df = pd.read_csv(diag_path) if diag_path.exists() else pd.DataFrame(columns=["etapa", "status", "objective"])

        # Etapa usada
        used_stage = "N/A"
        if not diag_df.empty:
            ru = diag_df[diag_df["etapa"].astype(str).str.strip() == "usada"]
            if len(ru) > 0:
                used_stage = str(ru.iloc[0]["status"]).strip()

        # Status real + objective de la etapa usada
        used_status = "N/A"
        used_obj = None
        if used_stage != "N/A" and not diag_df.empty:
            rs = diag_df[diag_df["etapa"].astype(str).str.strip() == used_stage]
            if len(rs) > 0:
                used_status = str(rs.iloc[0]["status"]).strip()
                used_obj = rs.iloc[0].get("objective", None)
            elif used_stage == "etapa2a_contrato_deep":
                # fallback
                rs = diag_df[diag_df["etapa"].astype(str).str.strip() == "2a_contrato_short"]
                if len(rs) > 0:
                    used_status = str(rs.iloc[0]["status"]).strip()

        # Métricas de demanda (desde brechas, si trae columnas)
        under_slots = None
        under_min_total = None
        try:
            cols_l = {str(c).lower(): c for c in brechas.columns}
            if "under_slots" in cols_l:
                under_slots = int(round(float(brechas[cols_l["under_slots"]].fillna(0).sum())))
            elif "under" in cols_l:
                under_slots = int((brechas[cols_l["under"]].fillna(0) > 0).sum())
            for key in ("under_min", "deficit_min", "faltante_min", "under_minutes"):
                if key in cols_l:
                    under_min_total = int(round(float(brechas[cols_l[key]].fillna(0).sum())))
                    break
        except Exception:
            pass


        # Demanda total / cobertura (intenta inferir columnas típicas)
        demand_total_min = None
        demand_covered_min = None
        demand_covered_pct = None
        contract_vs_demand_pct = None
        planned_vs_demand_pct = None
        try:
            cols_l = {str(c).lower(): c for c in brechas.columns}
            # Total requerido
            for key in ("need_min", "required_min", "demanda_min", "demand_min", "need_minutes", "required_minutes"):
                if key in cols_l:
                    demand_total_min = int(round(float(brechas[cols_l[key]].fillna(0).sum())))
                    break
            # Total cubierto/asignado
            for key in ("assigned_min", "covered_min", "asignado_min", "planned_min", "cobertura_min"):
                if key in cols_l:
                    demand_covered_min = int(round(float(brechas[cols_l[key]].fillna(0).sum())))
                    break
            # Inferencias
            if demand_total_min is not None and demand_covered_min is None and under_min_total is not None:
                demand_covered_min = max(0, demand_total_min - under_min_total)
            if demand_total_min is None and demand_covered_min is not None and under_min_total is not None:
                demand_total_min = demand_covered_min + under_min_total
            if demand_total_min is not None and demand_total_min > 0 and demand_covered_min is not None:
                demand_covered_pct = 100.0 * demand_covered_min / demand_total_min
        except Exception:
            pass

        # Contrato (desde contrato)
        contract_short_weeks = int((contrato["under_min"].fillna(0) > 0).sum()) if "under_min" in contrato.columns else 0
        contract_under_min_total = int(round(float(contrato["under_min"].fillna(0).sum()))) if "under_min" in contrato.columns else 0

        # Utilización de contrato
        total_target = int(round(float(contrato["target_min"].fillna(0).sum()))) if "target_min" in contrato.columns else None
        total_planned = int(round(float(contrato["planned_min"].fillna(0).sum()))) if "planned_min" in contrato.columns else None
        util_pct = (100.0 * total_planned / total_target) if (total_target and total_planned is not None and total_target > 0) else None

        # Evidencia: última demanda/contrato con objective no nulo
        dem_obj = "N/A"
        con_obj = "N/A"
        try:
            if not diag_df.empty:
                ddf = diag_df[diag_df["etapa"].astype(str).str.contains("demanda", case=False, na=False) & diag_df["objective"].notna()]
                if len(ddf) > 0:
                    dem_obj = float(ddf.iloc[-1]["objective"])
                cdf = diag_df[diag_df["etapa"].astype(str).str.contains("contrato", case=False, na=False)].copy()
                if "objective" in cdf.columns:
                    cdf["objective_num"] = pd.to_numeric(cdf["objective"], errors="coerce")
                    cdf = cdf[cdf["objective_num"].notna()]
                if len(cdf) > 0:
                    con_obj = float(cdf.iloc[-1].get("objective_num", cdf.iloc[-1].get("objective")))
        except Exception:
            pass

        cert_lines = []
        cert_lines.append("CERTIFICADO DE CALIDAD DEL PLAN (Turnera)")
        cert_lines.append("")
        cert_lines.append(f"Etapa usada: {used_stage}")
        cert_lines.append(f"Status (etapa usada): {used_status}")
        if used_obj is not None and str(used_obj).lower() != "nan":
            cert_lines.append(f"Objective (etapa usada): {used_obj}")
        cert_lines.append("")
        cert_lines.append("Métricas (menor es mejor, por orden de prioridad):")
        # 1) Demanda
        if under_slots is not None:
            cert_lines.append(f"1) Demanda: slots con faltantes = {under_slots}")
        if under_min_total is not None:
            cert_lines.append(f"   Demanda: minutos faltantes total = {under_min_total}")
        if demand_total_min is not None:
            cert_lines.append(f"   Demanda: minutos requeridos total = {demand_total_min}")
        if demand_covered_min is not None:
            cert_lines.append(f"   Demanda: minutos cubiertos = {demand_covered_min}")
        if demand_covered_pct is not None:
            cert_lines.append(f"   Demanda: cobertura = {demand_covered_pct:.2f}%")

        # 2) Contrato
        cert_lines.append(f"2) Contrato: semanas-persona con déficit = {contract_short_weeks}")
        cert_lines.append(f"   Contrato: minutos bajo contrato total = {contract_under_min_total}")
        if util_pct is not None:
            cert_lines.append(f"   Utilización vs contrato = {util_pct:.2f}% (planned/target)")

        # Relación capacidad/demanda (solo informativo)
        try:
            if demand_total_min is not None and demand_total_min > 0:
                if total_target is not None:
                    contract_vs_demand_pct = 100.0 * total_target / demand_total_min
                    cert_lines.append(f"   Capacidad contratada vs demanda = {contract_vs_demand_pct:.2f}% (target/demand)")
                if total_planned is not None:
                    planned_vs_demand_pct = 100.0 * total_planned / demand_total_min
                    cert_lines.append(f"   Planificado vs demanda = {planned_vs_demand_pct:.2f}% (planned/demand)")
        except Exception:
            pass

        # Conclusión automática (para jefatura)
        try:
            if demand_total_min is not None and demand_total_min > 0 and total_target is not None:
                if total_target < demand_total_min:
                    cert_lines.append("   Conclusión: Hay déficit estructural de capacidad (contratos < demanda).")
                else:
                    cert_lines.append("   Conclusión: Hay capacidad contratada suficiente para cubrir la demanda (teóricamente).")
        except Exception:
            pass


        # Expertise (si existe reporte)
        try:
            js_path = out_dir / "expertise_summary.json"
            if js_path.exists():
                import json as _json
                _s = _json.loads(js_path.read_text(encoding="utf-8"))
                cert_lines.append(f"Expertise: % minutos cubiertos por ALTA (MAÑANA) = {float(_s.get('pct_alta_manana', 0.0)):.2f}%")
                cert_lines.append(f"Expertise: % minutos cubiertos por ALTA (NOCHE) = {float(_s.get('pct_alta_noche', 0.0)):.2f}%")
        except Exception:
            pass

        cert_lines.append("")
        cert_lines.append("Evidencia de búsqueda (objetivos reportados por etapa):")
        cert_lines.append(f"- Última etapa de demanda objective = {dem_obj}")
        cert_lines.append(f"- Última etapa de contrato objective = {con_obj}")
        cert_lines.append("")
        cert_lines.append("Interpretación de optimalidad:")
        cert_lines.append("- OPTIMAL: el solver probó que no existe una solución mejor con las mismas reglas.")
        cert_lines.append("- FEASIBLE: es la mejor solución encontrada dentro del tiempo de búsqueda.")
        cert_lines.append("- UNKNOWN: no se pudo concluir optimalidad; se entrega la mejor solución encontrada antes del corte.")
        (out_dir / "certificado_plan.txt").write_text("\n".join(cert_lines), encoding="utf-8")
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", required=True, help="Ruta al Excel case.xlsx")
    ap.add_argument("--out", default="outputs", help="Carpeta outputs")
    args = ap.parse_args()

    case_path = Path(args.case)
    out_dir = Path(args.out)

    dfs = pd.read_excel(case_path, sheet_name=None)
    plan, brechas, contrato = solve_case(dfs, out_dir)
    save_outputs(plan, brechas, contrato, out_dir)
    print("OK ->", out_dir)

if __name__ == "__main__":
    main()