"""add holidays table

Revision ID: a1b2c3d4e5f6
Revises: <PON_AQUI_TU_REVISION_ANTERIOR>
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "a1b2c3d4e5f6"
down_revision = None   # ← reemplaza con tu última revisión
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "holidays",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "branch_id",
            UUID(as_uuid=True),
            sa.ForeignKey("branches.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("fecha", sa.Date(), nullable=False),
        sa.Column("nombre", sa.String(200), nullable=False, server_default=""),
        sa.Column("irrenunciable", sa.Boolean(), nullable=False, server_default="false"),
        sa.UniqueConstraint("branch_id", "fecha", name="uq_holiday_branch_fecha"),
    )


def downgrade() -> None:
    op.drop_table("holidays")
