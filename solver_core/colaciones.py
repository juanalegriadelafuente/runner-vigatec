# solver_core/colaciones.py
"""
Módulo de asignación inteligente de colaciones v3.

CAMBIOS v3 (fix principal):
- La demanda mínima se captura UNA VEZ al inicio (snapshot estático).
  Ya no usa dotación decrementada como fallback, eliminando el bug del
  "piso móvil" que forzaba violaciones.
- Primer pase: HARD BLOCK - jamás asigna donde holgura < 0.
- Segundo pase: solo para empleados sin slot válido (obligación legal).
  En este caso elige el slot de MENOR déficit.
- Ordenamiento: ventana más estrecha primero (quien tiene menos opciones
  elige primero), luego por hora de entrada como desempate.
- Staggering mejorado: cuando varios compiten por el mismo slot, el bonus
  por holgura positiva empuja naturalmente a distribuir.

PRINCIPIOS:
1. NUNCA bajar del mínimo operativo durante la colación (salvo fuerza mayor)
2. Escalonar colaciones cuando la dotación está justa
3. Preferir horarios cercanos al mediodía (12:00-13:00)
4. Respetar Art. 34 CT Chile: colación entre hora 3 y 5 desde inicio del turno

Este módulo es POST-PROCESO: se ejecuta DESPUÉS de que el solver asigna turnos.
NO afecta la optimización ni el cover_map del solver.

Autor: Vigatec Runner
"""

from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Set
from collections import defaultdict
from dataclasses import dataclass
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════════

SLOT_MIN = 30  # Granularidad en minutos
PREF_COLACION_INICIO = 12 * 60  # Preferencia: 12:00
PREF_COLACION_FIN = 13 * 60     # Preferencia: 13:00

# Art. 34 CT Chile: colación no antes de 3h ni después de 5h desde inicio
MIN_HORAS_PARA_COLACION = 3
MAX_HORAS_PARA_COLACION = 5


# ═══════════════════════════════════════════════════════════════════════════════
# ESTRUCTURAS DE DATOS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TurnoInfo:
    """Información de un turno para calcular colación."""
    employee_id: str
    fecha: str
    shift_id: str
    inicio_min: int      # Minutos desde medianoche
    fin_min: int         # Minutos desde medianoche (puede ser > 1440 si cruza)
    duracion_colacion: int  # Minutos de colación (ej: 60)
    ventana_inicio: int  # Primer slot válido para colación
    ventana_fin: int     # Último slot válido para colación
    colacion_asignada: Optional[int] = None  # Slot asignado (minutos)


@dataclass 
class SlotDemanda:
    """Demanda mínima en un slot horario."""
    minimo: int
    ideal: int


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════════

def _to_min(hhmm: str) -> Optional[int]:
    """Convierte HH:MM a minutos desde medianoche."""
    if not hhmm or not isinstance(hhmm, str):
        return None
    hhmm = hhmm.strip()
    if ":" not in hhmm:
        return None
    parts = hhmm.split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0])
        m = int(parts[1])
        return h * 60 + m
    except ValueError:
        return None


def _min_to_hhmm(minutes: int) -> str:
    """Convierte minutos a HH:MM."""
    if minutes is None:
        return ""
    minutes = int(minutes) % 1440
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def _parse_break_minutes(shift_id: str) -> int:
    """
    Extrae minutos de colación del shift_id.
    Formato: S_HHMM_HHMM_XX donde XX son los minutos de colación.
    Ejemplo: S_0730_1630_60 → 60 minutos
    """
    if not shift_id or not isinstance(shift_id, str):
        return 0
    shift_id = shift_id.strip().upper()
    if not shift_id.startswith("S_"):
        return 0
    parts = shift_id.split("_")
    if len(parts) >= 4 and parts[3].isdigit():
        return int(parts[3])
    return 0


def _calcular_ventana_colacion(
    inicio_turno: int,
    fin_turno: int,
    duracion_colacion: int,
) -> Tuple[int, int]:
    """
    Calcula la ventana legal de colación según Art. 34 CT Chile.
    
    Reglas:
    - No antes de 3 horas desde el inicio
    - No más tarde de 5 horas desde el inicio
    - La colación debe terminar antes del fin del turno (margen 30 min)
    
    Returns:
        (inicio_ventana, fin_ventana) en minutos desde medianoche
    """
    fin_cmp = fin_turno if fin_turno >= inicio_turno else fin_turno + 1440
    
    ventana_inicio = inicio_turno + MIN_HORAS_PARA_COLACION * 60
    ventana_fin = inicio_turno + MAX_HORAS_PARA_COLACION * 60
    
    # La colación debe terminar antes del fin del turno (margen 30 min)
    ventana_fin = min(ventana_fin, fin_cmp - duracion_colacion - 30)
    
    ventana_inicio = ventana_inicio % 1440
    ventana_fin = ventana_fin % 1440
    
    return ventana_inicio, ventana_fin


def _obtener_demanda_estatica(
    demanda_slots: Dict[int, int],
    slot: int,
    dotacion_base_slot: int,
) -> int:
    """
    Obtiene la demanda mínima para un slot usando datos ESTÁTICOS.
    
    A diferencia de la versión anterior, el fallback usa dotacion_BASE
    (el snapshot original, ANTES de descontar colaciones), no la dotación
    dinámica que se va decrementando.
    
    CRÍTICO: El mínimo operativo NUNCA puede ser 0.
    Siempre debe quedar al menos 1 persona trabajando.
    """
    demanda = 0
    
    if slot in demanda_slots:
        demanda = demanda_slots[slot]
    elif demanda_slots:
        # Buscar slot más cercano con demanda
        closest_slot = min(demanda_slots.keys(), key=lambda s: abs(s - slot))
        if abs(closest_slot - slot) < 120:
            demanda = demanda_slots[closest_slot]
        else:
            # Fallback: usar dotacion BASE - 1 (no la dinámica)
            demanda = max(1, dotacion_base_slot - 1)
    else:
        demanda = max(1, dotacion_base_slot - 1)
    
    return max(1, demanda)


def _slots_en_ventana(ventana_inicio: int, ventana_fin: int, slot_min: int) -> List[int]:
    """Genera lista de slots dentro de una ventana (maneja cruce de medianoche)."""
    slots = []
    if ventana_fin >= ventana_inicio:
        t = ventana_inicio
        while t <= ventana_fin:
            slots.append(t % 1440)
            t += slot_min
    else:
        # Cruza medianoche
        t = ventana_inicio
        while t < 1440:
            slots.append(t % 1440)
            t += slot_min
        t = 0
        while t <= ventana_fin:
            slots.append(t % 1440)
            t += slot_min
    return slots


# ═══════════════════════════════════════════════════════════════════════════════
# ALGORITMO PRINCIPAL v3
# ═══════════════════════════════════════════════════════════════════════════════

def asignar_colaciones(
    plan: pd.DataFrame,
    shift_times: Dict[str, Tuple[str, str]],
    demanda_por_fecha_slot: Dict[str, Dict[int, int]],
    slot_min: int = SLOT_MIN,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Asigna colaciones de forma inteligente respetando el mínimo operativo.
    
    ALGORITMO v3:
    1. Construir mapa de dotación BASE (snapshot estático, nunca cambia).
    2. Construir mapa de dotación DINÁMICA (se decrementa al asignar colaciones).
    3. Construir mapa de demanda ESTÁTICA por slot (usando dotación BASE para fallbacks).
    4. PRIMER PASE (hard constraint):
       - Ordenar por ventana más estrecha primero (quien tiene menos opciones, primero).
       - Solo asignar si holgura >= 0 en TODOS los slots de la colación.
    5. SEGUNDO PASE (forzado legal):
       - Los que no se pudieron asignar en pase 1.
       - Elegir el slot con MENOR déficit (menos daño).
    
    Args:
        plan: DataFrame con el plan de turnos (debe tener shift_id, employee_id, fecha)
        shift_times: Dict con {shift_id: (inicio, fin)} en formato HH:MM
        demanda_por_fecha_slot: Dict con {fecha_str: {slot_min: demanda_minima}}
        slot_min: Granularidad en minutos (default 30)
        verbose: Si True, imprime diagnóstico
        
    Returns:
        DataFrame con columna 'colacion_inicio' agregada (formato HH:MM)
    """
    plan = plan.copy()
    
    if 'colacion_inicio' not in plan.columns:
        plan['colacion_inicio'] = ''
    
    # ─── Paso 1: Construir dotación BASE (snapshot estático) ──────────────────
    # Esta NUNCA se modifica. Sirve para calcular demanda fallback y capacidad.
    
    dotacion_base: Dict[Tuple[str, int], int] = defaultdict(int)
    
    for _, row in plan.iterrows():
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
            
        ini_str, fin_str = shift_times.get(sid, ('', ''))
        ss = _to_min(ini_str)
        ee = _to_min(fin_str)
        
        if ss is None or ee is None:
            continue
            
        ee_cmp = ee if ee >= ss else ee + 1440
        t = ss
        while t < ee_cmp:
            dotacion_base[(fecha, t % 1440)] += 1
            t += slot_min
    
    # ─── Paso 2: Construir dotación DINÁMICA (se irá decrementando) ───────────
    dotacion_dyn: Dict[Tuple[str, int], int] = dict(dotacion_base)
    
    # ─── Paso 3: Pre-calcular demanda estática por (fecha, slot) ──────────────
    # Usa dotacion_base para fallbacks, así NO cambia al asignar colaciones.
    
    demanda_cache: Dict[Tuple[str, int], int] = {}
    
    def _get_demanda(fecha: str, slot: int) -> int:
        key = (fecha, slot)
        if key not in demanda_cache:
            demanda_slots = demanda_por_fecha_slot.get(fecha, {})
            base = dotacion_base.get(key, 0)
            demanda_cache[key] = _obtener_demanda_estatica(demanda_slots, slot, base)
        return demanda_cache[key]
    
    if verbose:
        print(f"[COLACIONES v3] Dotación base construida: {len(dotacion_base)} slots")
    
    # ─── Paso 4: Identificar empleados que necesitan colación ─────────────────
    
    turnos_pendientes: List[Tuple[int, TurnoInfo]] = []
    
    for idx, row in plan.iterrows():
        if row.get('colacion_inicio', ''):
            continue
            
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        emp_id = str(row.get('employee_id', ''))
        
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
        
        duracion_col = _parse_break_minutes(sid)
        if duracion_col <= 0:
            continue
            
        ini_str, fin_str = shift_times.get(sid, ('', ''))
        ss = _to_min(ini_str)
        ee = _to_min(fin_str)
        
        if ss is None or ee is None:
            continue
            
        ee_cmp = ee if ee >= ss else ee + 1440
        vent_ini, vent_fin = _calcular_ventana_colacion(ss, ee_cmp, duracion_col)
        
        if vent_ini > vent_fin:
            vent_ini = vent_fin = (ss + MIN_HORAS_PARA_COLACION * 60) % 1440
        
        info = TurnoInfo(
            employee_id=emp_id,
            fecha=fecha,
            shift_id=sid,
            inicio_min=ss,
            fin_min=ee_cmp,
            duracion_colacion=duracion_col,
            ventana_inicio=vent_ini,
            ventana_fin=vent_fin,
        )
        
        turnos_pendientes.append((idx, info))
    
    if verbose:
        print(f"[COLACIONES v3] {len(turnos_pendientes)} turnos necesitan colación")
    
    if not turnos_pendientes:
        return plan
    
    # ─── Paso 5: Ordenar por ventana más estrecha (menos opciones primero) ────
    # Desempate: hora de entrada (primero en entrar, primero en comer)
    
    def _ventana_size(item):
        _, info = item
        size = (info.ventana_fin - info.ventana_inicio + 1440) % 1440
        if size == 0:
            size = 1  # ventana mínima de 1 slot
        return size
    
    def _sort_key(item):
        _, info = item
        return (_ventana_size(item), info.inicio_min)
    
    turnos_pendientes.sort(key=_sort_key)
    
    if verbose:
        print(f"[COLACIONES v3] Orden: ventana estrecha primero, luego hora de entrada")
        for idx, info in turnos_pendientes[:5]:
            print(f"  - {info.employee_id[:12]}: ventana "
                  f"{_min_to_hhmm(info.ventana_inicio)}-{_min_to_hhmm(info.ventana_fin)} "
                  f"({_ventana_size((idx, info))} min)")
    
    # ─── Paso 6: PRIMER PASE - hard constraint (holgura >= 0) ────────────────
    
    asignaciones_ok = 0
    asignaciones_forzadas = 0
    pendientes_pase2: List[Tuple[int, TurnoInfo]] = []
    
    def _evaluar_slot(fecha: str, slot_inicio: int, duracion: int) -> Tuple[int, float]:
        """
        Evalúa un slot candidato para colación.
        Returns: (holgura_minima, penalidad_preferencia)
        """
        holgura_min = float('inf')
        tt = slot_inicio
        for _ in range(duracion // slot_min):
            s = tt % 1440
            dot_actual = dotacion_dyn.get((fecha, s), 0)
            dem = _get_demanda(fecha, s)
            holgura = (dot_actual - 1) - dem  # -1 porque esta persona sale
            holgura_min = min(holgura_min, holgura)
            tt += slot_min
        
        # Preferencia por mediodía
        dist = abs(slot_inicio - PREF_COLACION_INICIO)
        if dist > 720:
            dist = 1440 - dist
        penalidad_pref = dist / 60.0  # 1 punto por hora de distancia
        
        return int(holgura_min) if holgura_min != float('inf') else -999, penalidad_pref
    
    def _aplicar_colacion(fecha: str, slot_inicio: int, duracion: int):
        """Decrementa dotación dinámica para los slots de la colación."""
        tt = slot_inicio
        for _ in range(duracion // slot_min):
            s = tt % 1440
            dotacion_dyn[(fecha, s)] = max(0, dotacion_dyn.get((fecha, s), 0) - 1)
            tt += slot_min
    
    for idx, info in turnos_pendientes:
        fecha = info.fecha
        slots_ventana = _slots_en_ventana(info.ventana_inicio, info.ventana_fin, slot_min)
        
        # Buscar el MEJOR slot con holgura >= 0
        mejor_slot = None
        mejor_penalidad = float('inf')
        mejor_holgura = -999
        
        for s in slots_ventana:
            holgura, penalidad = _evaluar_slot(fecha, s, info.duracion_colacion)
            
            if holgura < 0:
                continue  # HARD BLOCK: no asignar si baja del mínimo
            
            # Entre slots válidos, elegir por: más holgura primero, luego más cerca de mediodía
            # Penalidad compuesta: menor es mejor
            score = penalidad - (holgura * 50)  # Bonus fuerte por holgura
            
            if score < mejor_penalidad:
                mejor_penalidad = score
                mejor_slot = s
                mejor_holgura = holgura
        
        if mejor_slot is not None:
            # ✓ Asignación exitosa sin violar mínimo
            plan.at[idx, 'colacion_inicio'] = _min_to_hhmm(mejor_slot)
            _aplicar_colacion(fecha, mejor_slot, info.duracion_colacion)
            asignaciones_ok += 1
            
            if verbose:
                print(f"[COLACIONES v3] ✓ {info.employee_id[:12]}: "
                      f"{_min_to_hhmm(mejor_slot)} (holgura={mejor_holgura})")
        else:
            # No hay slot sin violar mínimo → pase 2
            pendientes_pase2.append((idx, info))
    
    # ─── Paso 7: SEGUNDO PASE - forzado legal (menor déficit posible) ─────────
    # Estos son casos donde NO hay slot que mantenga la dotación sobre el mínimo.
    # Obligación legal: la colación se debe dar igual. Elegimos el menor daño.
    
    if pendientes_pase2 and verbose:
        print(f"[COLACIONES v3] ⚠️ {len(pendientes_pase2)} turnos van a pase 2 (forzado)")
    
    for idx, info in pendientes_pase2:
        fecha = info.fecha
        slots_ventana = _slots_en_ventana(info.ventana_inicio, info.ventana_fin, slot_min)
        
        mejor_slot = None
        mejor_holgura = -999_999
        mejor_penalidad = float('inf')
        
        for s in slots_ventana:
            holgura, penalidad = _evaluar_slot(fecha, s, info.duracion_colacion)
            
            # En pase 2: maximizar holgura (minimizar déficit), luego preferencia mediodía
            score = -(holgura * 10_000) + penalidad  # holgura domina
            
            if score < mejor_penalidad:
                mejor_penalidad = score
                mejor_slot = s
                mejor_holgura = holgura
        
        if mejor_slot is not None:
            plan.at[idx, 'colacion_inicio'] = _min_to_hhmm(mejor_slot)
            _aplicar_colacion(fecha, mejor_slot, info.duracion_colacion)
        else:
            # Último recurso: inicio de ventana
            mejor_slot = info.ventana_inicio
            plan.at[idx, 'colacion_inicio'] = _min_to_hhmm(mejor_slot)
            _aplicar_colacion(fecha, mejor_slot, info.duracion_colacion)
        
        asignaciones_forzadas += 1
        if verbose:
            print(f"[COLACIONES v3] ⚠️ FORZADO {info.employee_id[:12]}: "
                  f"{_min_to_hhmm(mejor_slot)} (holgura={mejor_holgura}, "
                  f"dotación insuficiente para mantener mínimo)")
    
    if verbose:
        print(f"[COLACIONES v3] Resultado: {asignaciones_ok} OK, "
              f"{asignaciones_forzadas} forzadas (total: {len(turnos_pendientes)})")
        if asignaciones_forzadas > 0:
            print(f"[COLACIONES v3] Las {asignaciones_forzadas} forzadas indican que "
                  f"la dotación es insuficiente para cubrir mínimo durante colaciones. "
                  f"Considerar agregar personal o ampliar franja horaria de turnos.")
    
    return plan


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN DE DIAGNÓSTICO
# ═══════════════════════════════════════════════════════════════════════════════

def diagnostico_colaciones(
    plan: pd.DataFrame,
    shift_times: Dict[str, Tuple[str, str]],
    demanda_por_fecha_slot: Dict[str, Dict[int, int]],
    slot_min: int = SLOT_MIN,
) -> pd.DataFrame:
    """
    Genera un reporte de diagnóstico de las colaciones asignadas.
    
    Muestra por cada (fecha, slot):
    - Demanda mínima
    - Dotación SIN descontar colaciones
    - Dotación CON colaciones descontadas
    - Personas en colación
    - Holgura (dotación - demanda)
    - Alerta si holgura < 0
    
    Returns:
        DataFrame con el diagnóstico
    """
    dotacion_base: Dict[Tuple[str, int], int] = defaultdict(int)
    
    for _, row in plan.iterrows():
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        
        if not sid or sid in ('LIBRE', 'SALIENTE') or not sid.startswith('S_'):
            continue
            
        ini_str, fin_str = shift_times.get(sid, ('', ''))
        ss = _to_min(ini_str)
        ee = _to_min(fin_str)
        
        if ss is None or ee is None:
            continue
            
        ee_cmp = ee if ee >= ss else ee + 1440
        t = ss
        while t < ee_cmp:
            dotacion_base[(fecha, t % 1440)] += 1
            t += slot_min
    
    # Calcular personas en colación por slot
    en_colacion: Dict[Tuple[str, int], int] = defaultdict(int)
    
    for _, row in plan.iterrows():
        col_inicio = row.get('colacion_inicio', '')
        if not col_inicio:
            continue
            
        sid = str(row.get('shift_id', '')).strip().upper()
        fecha = str(row.get('fecha', ''))[:10]
        
        duracion_col = _parse_break_minutes(sid)
        if duracion_col <= 0:
            continue
            
        col_min = _to_min(col_inicio)
        if col_min is None:
            continue
            
        t = col_min
        for _ in range(duracion_col // slot_min):
            en_colacion[(fecha, t % 1440)] += 1
            t += slot_min
    
    # Generar reporte
    rows = []
    fechas = sorted(set(k[0] for k in dotacion_base.keys()))
    
    for fecha in fechas:
        demanda_slots = demanda_por_fecha_slot.get(fecha, {})
        
        for slot in sorted(set(k[1] for k in dotacion_base.keys() if k[0] == fecha)):
            dem_min = demanda_slots.get(slot, 0)
            dot_base = dotacion_base.get((fecha, slot), 0)
            en_col = en_colacion.get((fecha, slot), 0)
            dot_real = dot_base - en_col
            holgura = dot_real - dem_min
            
            rows.append({
                'fecha': fecha,
                'slot': _min_to_hhmm(slot),
                'demanda_min': dem_min,
                'dotacion_base': dot_base,
                'en_colacion': en_col,
                'dotacion_real': dot_real,
                'holgura': holgura,
                'alerta': '⚠️ BAJO MÍNIMO' if holgura < 0 else '',
            })
    
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST / DEMO
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Módulo de colaciones v3 cargado correctamente.")
    print("Uso: from colaciones import asignar_colaciones")
    print()
    print("Ejemplo:")
    print("  plan_con_colaciones = asignar_colaciones(")
    print("      plan=plan_df,")
    print("      shift_times={'S_0730_1630_60': ('07:30', '16:30')},")
    print("      demanda_por_fecha_slot={'2026-04-01': {480: 2, 510: 2, ...}},")
    print("      verbose=True,")
    print("  )")
