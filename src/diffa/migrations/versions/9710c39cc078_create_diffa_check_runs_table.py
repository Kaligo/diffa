"""create diffa_check_runs table

Revision ID: 9710c39cc078
Revises: 2f7730d11b26
Create Date: 2025-03-11 00:46:18.178930

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from diffa.config import ConfigManager


# revision identifiers, used by Alembic.
revision: str = '9710c39cc078'
down_revision: Union[str, None] = '2f7730d11b26'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

config_manager = ConfigManager()


def upgrade() -> None:
    op.create_table(
        f"{config_manager.get_table('diffa', 'check_runs')}",
        sa.Column("run_id", sa.UUID, primary_key=True),
        sa.Column("source", sa.String, nullable=False),
        sa.Column("target", sa.String, nullable=False),
        sa.Column("status", sa.String, nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        schema=config_manager.get_schema("diffa"),
    )


def downgrade() -> None:
    op.drop_table(
        f"{config_manager.get_table('diffa', 'check_runs')}",
        schema=config_manager.get_schema("diffa"),
    )