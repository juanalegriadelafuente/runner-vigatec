from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+psycopg2://vigatec:vigatec@localhost:5432/vigatec"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"
    STORAGE_PATH: str = "./storage"
    SOLVER_CMD: str = "python -u solver_core/solve.py"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
