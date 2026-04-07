from __future__ import annotations

import uuid
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from api.db import Base


class SolverVocabItem(Base):
    """
    Catálogo de valores permitidos por empresa (controlado).
    Ej: AUSENTISMO_CODE: LM, VAC...
        RESTRICCION_TIPO: DIA_LIBRE_FIJO, REGLA_APERTURA_CARGO...
        JORNADA_ID: J_6X1_44, J_2X5_16...
    """
    __tablename__ = "solver_vocab_items"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)

    category = Column(String(64), nullable=False)   # AUSENTISMO_CODE, RESTRICCION_TIPO, JORNADA_ID, etc.
    value = Column(String(128), nullable=False)     # valor exacto que lee el solver
    label = Column(String(256), nullable=True)      # texto amigable (opcional)

    active = Column(Boolean, nullable=False, server_default="true")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)