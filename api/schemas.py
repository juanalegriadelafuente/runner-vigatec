from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class RunCreateResponse(BaseModel):
    id: uuid.UUID
    status: str


class RunStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    original_filename: Optional[str] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None

    # QA (opcionales)
    qa_status: Optional[str] = None
    qa_message: Optional[str] = None
    qa_summary: Optional[dict[str, Any]] = None


class ArtifactItem(BaseModel):
    name: str
    size_bytes: int
    # IMPORTANTE: antes estaba como str, pero file_mtime_utc devuelve datetime
    modified_at: datetime


class ArtifactListResponse(BaseModel):
    id: uuid.UUID
    artifacts: list[ArtifactItem]
