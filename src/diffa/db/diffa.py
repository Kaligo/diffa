from typing import Optional, List, Iterable
from contextlib import contextmanager

from sqlalchemy import create_engine, update
from sqlalchemy.sql.functions import now
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from diffa.db.base import Database
from diffa.db.data_models import (
    DiffaCheckSchema,
    DiffaCheck,
    DiffaCheckRunSchema,
    DiffaCheckRun,
)
from diffa.config import ConfigManager
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()
config = ConfigManager()


class DiffaDatabase(Database):
    """Base QLAlchemy Database Adapter for Diffa Database operations"""

    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.engine = None
        self.session = None

    def connect(self):
        self.engine = create_engine(
            self.db_config["db_url"] + "?sslmode=prefer"
        )  # Prefer SSL mode
        self.session = sessionmaker(bind=self.engine)()

    def execute_query(self, query: str, sql_params: dict = None):
        self.connect()
        try:
            result = self.session.execute(query, sql_params)
            for row in result:
                yield dict(row)
        finally:
            self.close()

    def execute_non_query(self, query, params: dict = None):
        self.connect()
        try:
            with self.session.begin():  # Ensures transaction integrity
                self.session.execute(query, params)
        finally:
            self.close()

    def close(self):
        self.session.close()


class DiffaCheckDatabase(DiffaDatabase):
    """SQLAlchemy Database Adapter for Diffa state management"""

    def get_latest_check(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> Optional[dict]:
        """Get the latest invalid check. If not found, return None"""

        self.connect()
        try:
            diffa_check = (
                self.session.query(DiffaCheck)
                .filter(DiffaCheck.source_database == source_database)
                .filter(DiffaCheck.source_schema == source_schema)
                .filter(DiffaCheck.source_table == source_table)
                .filter(DiffaCheck.target_database == target_database)
                .filter(DiffaCheck.target_schema == target_schema)
                .filter(DiffaCheck.target_table == target_table)
                .order_by(DiffaCheck.check_date.desc())
                .first()
            )
            return (
                DiffaCheckSchema.model_validate(diffa_check).model_dump()
                if diffa_check
                else None
            )
        finally:
            self.close()

    def get_invalid_checks(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> List[DiffaCheckSchema]:
        self.connect()
        try:
            invalid_checks = (
                self.session.query(DiffaCheck)
                .filter(DiffaCheck.source_database == source_database)
                .filter(DiffaCheck.source_schema == source_schema)
                .filter(DiffaCheck.source_table == source_table)
                .filter(DiffaCheck.target_database == target_database)
                .filter(DiffaCheck.target_schema == target_schema)
                .filter(DiffaCheck.target_table == target_table)
                .filter(DiffaCheck.is_valid == False)
                .all()
            )
            for invalid_check in invalid_checks:
                yield DiffaCheckSchema.model_validate(invalid_check).model_dump()
        finally:
            self.close()

    def upsert_diffa_checks(self, diffa_check_schemas: Iterable[DiffaCheckSchema]):
        """Save a diff record"""

        self.connect()
        try:
            diffa_checks = [
                diffa_check.model_dump() for diffa_check in diffa_check_schemas
            ]
            if len(diffa_checks) > 0:
                with self.session.begin():
                    stmt = insert(DiffaCheck).values(diffa_checks)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[DiffaCheck.id],
                        set_={
                            "source_count": stmt.excluded.source_count,
                            "target_count": stmt.excluded.target_count,
                            "is_valid": stmt.excluded.is_valid,
                            "diff_count": stmt.excluded.diff_count,
                            "check_date": stmt.excluded.check_date,
                            "updated_at": now(),
                        },
                    )
                    self.session.execute(stmt)

                logger.info(f"Upserted {len(diffa_checks)} records successfully!")
            else:
                logger.info("No records to upsert")
        finally:
            self.close()


class DiffaCheckRunDatabase(DiffaDatabase):
    """SQLAlchemy Database Adapter for Diffa running state management"""

    @contextmanager
    def acquire_exclusive_lock(self):
        """Aquire an exclusive lock on the diffa_check_run table"""

        self.connect()
        try:
            self.session.execute(
                text(
                    f"LOCK TABLE {self.db_config['schema']}.{self.db_config['table']} IN EXCLUSIVE MODE;"
                )
            )
            yield
        finally:
            self.close()

    def create_diffa_check_run_record(
        self, diffa_check_run_schema: DiffaCheckRunSchema
    ):
        """Create a new diffa check run record"""

        self.connect()
        try:
            with self.session.begin():
                with self.acquire_exclusive_lock():
                    self.session.execute(
                        insert(DiffaCheckRun).values(
                            diffa_check_run_schema.model_dump()
                        )
                    )
                    self.session.commit()
        finally:
            self.close()

    def get_running_check_runs(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> List[DiffaCheckRunSchema]:

        self.connect()
        try:
            running_check_runs = (
                self.session.query(DiffaCheckRun)
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
        finally:
            self.close()

    def update_diffa_check_run_record_with_status(self, run_id: str, status: str):
        """Update a diffa check run record"""

        self.connect()
        try:
            with self.session.begin():
                with self.acquire_exclusive_lock():
                    self.session.execute(
                        update(DiffaCheckRun)
                        .where(DiffaCheckRun.run_id == run_id)
                        .values(status=status, updated_at=now())
                    )
                    self.session.commit()
        finally:
            self.close()
