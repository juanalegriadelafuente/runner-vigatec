-- ============================================================
-- MIGRACIÓN: Agregar columna requeridos_ideal a demand_unit
-- Vigatec Runner v3 — Dos Curvas de Demanda
-- Fecha: 2026-03-18
-- ============================================================

-- Paso 1: Agregar la nueva columna (nullable para compatibilidad)
ALTER TABLE demand_unit 
ADD COLUMN IF NOT EXISTS requeridos_ideal INTEGER;

-- Paso 2: Comentario descriptivo
COMMENT ON COLUMN demand_unit.requeridos IS 
'Mínimo Operativo: piso absoluto para que la OU funcione. Restricción DURA del solver.';

COMMENT ON COLUMN demand_unit.requeridos_ideal IS 
'Demanda Real/Ideal: lo que realmente se necesita para operar bien. Objetivo BLANDO del solver. Si es NULL, se asume igual a requeridos.';

-- Paso 3 (OPCIONAL): Si quieres inicializar ideal = mínimo para todos los registros existentes
-- Descomenta la siguiente línea solo si quieres que todos tengan holgura = 0 por defecto
-- UPDATE demand_unit SET requeridos_ideal = requeridos WHERE requeridos_ideal IS NULL;

-- ============================================================
-- VERIFICACIÓN
-- ============================================================
-- Ejecuta esto para verificar que la migración fue exitosa:
-- SELECT column_name, data_type, is_nullable 
-- FROM information_schema.columns 
-- WHERE table_name = 'demand_unit' 
-- ORDER BY ordinal_position;
