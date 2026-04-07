from __future__ import annotations

import os
from celery import Celery

BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")

celery_app = Celery(
    "vigatec_api",
    broker=BROKER,
    backend=BACKEND,
)

# Config segura para JSON (bien para SaaS)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
