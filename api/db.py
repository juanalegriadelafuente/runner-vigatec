import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from api.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db_with_retry(max_attempts: int = 30, sleep_seconds: float = 1.0) -> None:
    from sqlalchemy import text
    from api import models  # noqa

    last_exc = None
    for _ in range(max_attempts):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            Base.metadata.create_all(bind=engine)
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(sleep_seconds)

    raise RuntimeError(f"DB not ready after {max_attempts} attempts. Last error: {last_exc}")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
