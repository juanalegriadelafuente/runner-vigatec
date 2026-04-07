"""
Migración: crear tabla plan_override_responses
Ejecutar UNA VEZ: docker exec vigatec_api python /app/api/migrate_override_responses.py
"""
from api.db import engine
from sqlalchemy import text

SQL = """
CREATE TABLE IF NOT EXISTS plan_override_responses (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    override_id     UUID NOT NULL,
    run_id          UUID NOT NULL,
    company_id      UUID NOT NULL,
    employee_id     VARCHAR(100) NOT NULL,
    fecha           VARCHAR(10)  NOT NULL,
    shift_id_old    VARCHAR(100),
    shift_id_new    VARCHAR(100),
    token           UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    estado          VARCHAR(20)  NOT NULL DEFAULT 'pending',
    fecha_limite    TIMESTAMPTZ  NOT NULL,
    fecha_respuesta TIMESTAMPTZ,
    notificado_supervisor BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_por_token    ON plan_override_responses(token);
CREATE INDEX IF NOT EXISTS idx_por_override ON plan_override_responses(override_id);
CREATE INDEX IF NOT EXISTS idx_por_run      ON plan_override_responses(run_id);
CREATE INDEX IF NOT EXISTS idx_por_estado   ON plan_override_responses(estado);
"""

with engine.connect() as conn:
    conn.execute(text(SQL))
    conn.commit()
    print("✅ Tabla plan_override_responses creada correctamente.")