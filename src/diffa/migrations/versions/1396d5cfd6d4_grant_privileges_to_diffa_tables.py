"""grant_privileges_to_diffa_tables

Revision ID: 1396d5cfd6d4
Revises: 9710c39cc078
Create Date: 2025-03-28 00:41:13.445635

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from diffa.config import ConfigManager

# revision identifiers, used by Alembic.
revision: str = "1396d5cfd6d4"
down_revision: Union[str, None] = "9710c39cc078"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

config_manager = ConfigManager()
diffa_schema = config_manager.diffa_check.get_db_schema()


def upgrade() -> None:
    """
    Grants all privileges on all tables in the specified schema to PUBLIC.
    """
    op.execute(f"GRANT USAGE ON SCHEMA {diffa_schema} TO PUBLIC")
    op.execute(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {diffa_schema} TO PUBLIC")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {diffa_schema} GRANT ALL PRIVILEGES ON TABLES TO PUBLIC"
    )


def downgrade() -> None:
    """
    Revokes all privileges on all tables in the specified schema from PUBLIC.
    """
    op.execute(
        f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {diffa_schema} FROM PUBLIC"
    )
    op.execute(f"REVOKE USAGE ON SCHEMA {diffa_schema} FROM PUBLIC")
    op.execute(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {diffa_schema} REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC"
    )
