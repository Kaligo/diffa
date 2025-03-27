from typing import List
from contextlib import contextmanager

from sqlalchemy import update
from sqlalchemy.sql.functions import now
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from diffa.db.connect import DiffaConnection
from diffa.config import DBConfig, ConfigManager
from diffa.db.data_models import (
    DiffaCheckRunSchema,
    DiffaCheckRun,
)
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()


class DiffaCheckRunDatabase:
    """SQLAlchemy Database Adapter for Diffa running state management"""

    def __init__(self, db_config: DBConfig):
        self.db_config = db_config
        self.conn = DiffaConnection(self.db_config.get_db_config())

    @contextmanager
    def acquire_exclusive_lock(self, session):
        """Aquire an exclusive lock on the diffa_check_run table"""

        session.execute(
            text(
                f"LOCK TABLE {self.db_config.get_db_schema()}.{self.db_config.get_db_table()} IN EXCLUSIVE MODE;"
            )
        )
        yield

    def create_diffa_check_run_record(
        self, diffa_check_run_schema: DiffaCheckRunSchema
    ):
        """Create a new diffa check run record"""
        with self.conn.db_session() as session:
            with session.begin():
                with self.acquire_exclusive_lock(session):
                    session.execute(
                        insert(DiffaCheckRun).values(
                            diffa_check_run_schema.model_dump()
                        )
                    )

    def get_running_check_runs(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> List[DiffaCheckRunSchema]:

        with self.conn.db_session() as session:
            running_check_runs = (
                session.query(DiffaCheckRun)
                .filter(DiffaCheckRun.source_database == source_database)
                .filter(DiffaCheckRun.source_schema == source_schema)
                .filter(DiffaCheckRun.source_table == source_table)
                .filter(DiffaCheckRun.target_database == target_database)
                .filter(DiffaCheckRun.target_schema == target_schema)
                .filter(DiffaCheckRun.target_table == target_table)
                .filter(DiffaCheckRun.status == "RUNNING")
                .all()
            )

        for running_check_run in running_check_runs:
            yield DiffaCheckRunSchema.model_validate(running_check_run)

    def update_diffa_check_run_record_with_status(self, run_id: str, status: str):
        """Update a diffa check run record"""
        with self.conn.db_session() as session:
            with session.begin():
                with self.acquire_exclusive_lock(session):
                    session.execute(
                        update(DiffaCheckRun)
                        .where(DiffaCheckRun.run_id == run_id)
                        .values(status=status, updated_at=now())
                    )


class DiffaCheckRunService:

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.diffa_check_run_db = DiffaCheckRunDatabase(config_manager.diffa_check_run)

    def getting_running_check_runs(self) -> list[str]:
        """Check if there are any running check runs. Return all run_ids of RUNNING check runs."""

        running_check_run_ids = []

        checking_runs = self.diffa_check_run_db.get_running_check_runs(
            source_database=self.config_manager.source.get_db_name(),
            source_schema=self.config_manager.source.get_db_schema(),
            source_table=self.config_manager.source.get_db_table(),
            target_database=self.config_manager.target.get_db_name(),
            target_schema=self.config_manager.target.get_db_schema(),
            target_table=self.config_manager.target.get_db_table(),
        )
        for running_check_run in checking_runs:
            running_check_run_ids.append(str(running_check_run.run_id))

        return running_check_run_ids

    def create_new_check_run(self, diffa_check_run_schema: DiffaCheckRunSchema):
        """Create a new check run"""

        self.diffa_check_run_db.create_diffa_check_run_record(diffa_check_run_schema)
        logger.info(f"Created new check run with id: {diffa_check_run_schema.run_id}")

    def update_check_run_as_status(
        self, diffa_check_run_schema: DiffaCheckRunSchema, status: str
    ):
        """Mark a check run as completed"""

        diffa_check_run_schema.status = status
        self.diffa_check_run_db.update_diffa_check_run_record_with_status(
            diffa_check_run_schema.run_id, status
        )
