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
revision: str = "9710c39cc078"
down_revision: Union[str, None] = "2f7730d11b26"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

config_manager = ConfigManager()


def upgrade() -> None:
    op.create_table(
        f"{config_manager.diffa_check_run.get_db_table()}",
        sa.Column("run_id", sa.UUID, primary_key=True),
        sa.Column("source_database", sa.String, nullable=False),
        sa.Column("source_schema", sa.String, nullable=False),
        sa.Column("source_table", sa.String, nullable=False),
        sa.Column("target_database", sa.String, nullable=False),
        sa.Column("target_schema", sa.String, nullable=False),
        sa.Column("target_table", sa.String, nullable=False),
        sa.Column("status", sa.String, nullable=False),
        sa.Column(
            "created_at", sa.DateTime, server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False
        ),
        schema=config_manager.diffa_check_run.get_db_schema(),
    )
    op.create_index(
        "idx_unique_running_check_runs",
        table_name=f"{config_manager.diffa_check_run.get_db_table()}",
        columns=[
            "source_database",
            "source_schema",
            "source_table",
            "target_database",
            "target_schema",
            "target_table",
        ],
        unique=True,
        schema=config_manager.diffa_check_run.get_db_schema(),
        postgresql_where=sa.text("status = 'RUNNING'"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_unique_running_check_runs",
        table_name=f"{config_manager.diffa_check_run.get_db_table()}",
        schema=config_manager.diffa_check_run.get_db_schema(),
        if_exists=True,
    )
    op.drop_table(
        f"{config_manager.diffa_check_run.get_db_table()}",
        schema=config_manager.diffa_check_run.get_db_schema(),
    )
