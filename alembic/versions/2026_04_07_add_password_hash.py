"""add password_hash to users

Revision ID: 2026_04_07_password
Revises:
Create Date: 2026-04-07

"""
from alembic import op
import sqlalchemy as sa

revision = "2026_04_07_password"
down_revision = None   # <-- Alembic llenará esto automáticamente con --autogenerate
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("password_hash", sa.String(255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "password_hash")
