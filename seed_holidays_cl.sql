-- Tabla catálogo nacional de feriados Chile
CREATE TABLE IF NOT EXISTS holidays_cl (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fecha DATE NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    irrenunciable BOOLEAN NOT NULL DEFAULT false,
    nacional BOOLEAN NOT NULL DEFAULT true,  -- false = feriado regional
    region VARCHAR(200) DEFAULT NULL,         -- ej: "Arica y Parinacota"
    CONSTRAINT uq_holidays_cl_fecha_region UNIQUE (fecha, region)
);

-- 2026
INSERT INTO holidays_cl (fecha, nombre, irrenunciable, nacional) VALUES
('2026-01-01', 'Año Nuevo', true, true),
('2026-04-03', 'Viernes Santo', false, true),
('2026-04-04', 'Sábado Santo', false, true),
('2026-05-01', 'Día Nacional del Trabajo', true, true),
('2026-05-21', 'Día de las Glorias Navales', false, true),
('2026-06-21', 'Día Nacional de los Pueblos Indígenas', false, true),
('2026-06-29', 'San Pedro y San Pablo', false, true),
('2026-07-16', 'Día de la Virgen del Carmen', false, true),
('2026-08-15', 'Asunción de la Virgen', false, true),
('2026-09-18', 'Independencia Nacional', true, true),
('2026-09-19', 'Día de las Glorias del Ejército', true, true),
('2026-10-12', 'Encuentro de Dos Mundos', false, true),
('2026-10-31', 'Día de las Iglesias Evangélicas y Protestantes', false, true),
('2026-11-01', 'Día de Todos los Santos', false, true),
('2026-12-08', 'Inmaculada Concepción', false, true),
('2026-12-25', 'Navidad', true, true)
ON CONFLICT (fecha, region) DO NOTHING;

-- 2026 regionales (region NOT NULL, nacional=false)
INSERT INTO holidays_cl (fecha, nombre, irrenunciable, nacional, region) VALUES
('2026-06-07', 'Asalto y Toma del Morro de Arica', false, false, 'Arica y Parinacota'),
('2026-08-20', 'Nacimiento del Prócer de la Independencia', false, false, 'Chillán y Chillán Viejo')
ON CONFLICT (fecha, region) DO NOTHING;

-- 2027
INSERT INTO holidays_cl (fecha, nombre, irrenunciable, nacional) VALUES
('2027-01-01', 'Año Nuevo', true, true),
('2027-03-26', 'Viernes Santo', false, true),
('2027-03-27', 'Sábado Santo', false, true),
('2027-05-01', 'Día Nacional del Trabajo', true, true),
('2027-05-21', 'Día de las Glorias Navales', false, true),
('2027-06-21', 'Día Nacional de los Pueblos Indígenas', false, true),
('2027-06-28', 'San Pedro y San Pablo', false, true),
('2027-07-16', 'Día de la Virgen del Carmen', false, true),
('2027-08-15', 'Asunción de la Virgen', false, true),
('2027-09-17', 'Feriado Fiestas Patrias', false, true),
('2027-09-18', 'Independencia Nacional', true, true),
('2027-09-19', 'Día de las Glorias del Ejército', true, true),
('2027-10-11', 'Encuentro de Dos Mundos', false, true),
('2027-10-31', 'Día de las Iglesias Evangélicas y Protestantes', false, true),
('2027-11-01', 'Día de Todos los Santos', false, true),
('2027-12-08', 'Inmaculada Concepción', false, true),
('2027-12-25', 'Navidad', true, true)
ON CONFLICT (fecha, region) DO NOTHING;

-- 2027 regionales
INSERT INTO holidays_cl (fecha, nombre, irrenunciable, nacional, region) VALUES
('2027-06-07', 'Asalto y Toma del Morro de Arica', false, false, 'Arica y Parinacota'),
('2027-08-20', 'Nacimiento del Prócer de la Independencia', false, false, 'Chillán y Chillán Viejo')
ON CONFLICT (fecha, region) DO NOTHING;
