import os
from celery import Celery

BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")

celery_app = Celery(
    "vigatec_worker",
    broker=BROKER,
    backend=BACKEND,
    include=["worker.tasks"],  # ✅ NO incluir worker.publish_tasks (no existe)
)

celery_app.conf.update(
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    timezone="UTC",
    enable_utc=True,
)