"""create diffa checking table

Revision ID: 2f7730d11b26
Revises:
Create Date: 2025-02-10 00:46:22.212652

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from diffa.config import ConfigManager

# revision identifiers, used by Alembic.
revision: str = "2f7730d11b26"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

config_manager = ConfigManager()


def upgrade() -> None:
    op.execute(
        f"CREATE SCHEMA IF NOT EXISTS {config_manager.diffa_check.get_db_schema()}"
    )
    op.create_table(
        f"{config_manager.diffa_check.get_db_table()}",
        sa.Column("id", sa.UUID, primary_key=True),
        sa.Column("source_database", sa.String, nullable=False),
        sa.Column("source_schema", sa.String, nullable=False),
        sa.Column("source_table", sa.String, nullable=False),
        sa.Column("target_database", sa.String, nullable=False),
        sa.Column("target_schema", sa.String, nullable=False),
        sa.Column("target_table", sa.String, nullable=False),
        sa.Column("check_date", sa.Date, nullable=False),
        sa.Column("source_count", sa.Integer, nullable=False),
        sa.Column("target_count", sa.Integer, nullable=False),
        sa.Column("is_valid", sa.Boolean, nullable=False),
        sa.Column("diff_count", sa.Integer, nullable=False),
        sa.Column(
            "created_at", sa.DateTime, server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False
        ),
        schema=config_manager.diffa_check.get_db_schema(),
    )


def downgrade() -> None:
    op.drop_table(
        f"{config_manager.diffa_check.get_db_table()}",
        schema=config_manager.diffa_check.get_db_schema(),
    )
