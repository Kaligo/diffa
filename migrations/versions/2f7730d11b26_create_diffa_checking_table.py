"""create diffa checking table

Revision ID: 2f7730d11b26
Revises: 
Create Date: 2025-02-10 00:46:22.212652

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from src.diffa.config import DIFFA_DB_SCHEMA, DIFFA_DB_TABLE

# revision identifiers, used by Alembic.
revision: str = '2f7730d11b26'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {DIFFA_DB_SCHEMA}")
    op.create_table(
        f"{DIFFA_DB_TABLE}",
        sa.Column("id", sa.UUID, primary_key=True),
        sa.Column("table_name", sa.String, nullable=False),
        sa.Column("start_check_date", sa.DateTime, nullable=False),
        sa.Column("end_check_date", sa.DateTime, nullable=False),
        sa.Column("source_count", sa.Integer, nullable=False),
        sa.Column("target_count", sa.Integer, nullable=False),
        sa.Column("status", sa.String, nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("last_reconciled_at", sa.DateTime, nullable=True),
        schema=DIFFA_DB_SCHEMA,
    )

def downgrade() -> None:
    op.drop_table(f"{DIFFA_DB_TABLE}")
