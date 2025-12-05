from datetime import date
from typing import Optional, List, Iterable

from sqlalchemy.sql.functions import now
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import insert

from diffa.db.connect import DiffaConnection
from diffa.config import DiffaConfig, ConfigManager, DIFFA_BEGIN_DATE
from diffa.db.data_models import (
    DiffaCheckSchema,
    DiffaCheck,
)
from diffa.utils import Logger

logger = Logger(__name__)
Base = declarative_base()


class DiffaCheckDatabase:
    """SQLAlchemy Database Adapter for Diffa state management"""

    def __init__(self, db_config: DiffaConfig):
        self.db_config = db_config
        self.conn = DiffaConnection(self.db_config.get_db_config())

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

        with self.conn.db_session() as session:
            diffa_check = (
                session.query(DiffaCheck)
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

    def get_invalid_checks(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ) -> List[DiffaCheckSchema]:

        with self.conn.db_session() as session:
            invalid_checks = (
                session.query(DiffaCheck)
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

    def upsert_diffa_checks(self, diffa_checks: Iterable[dict]):
        """Save a diff record"""

        with self.conn.db_session() as session:
            with session.begin():
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
                session.execute(stmt)


class DiffaCheckService:

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.diffa_db = DiffaCheckDatabase(self.config_manager.diffa_check)
        self.is_full_diff = self.config_manager.diffa_check.is_full_diff()

    def get_last_check_date(self) -> date:

        latest_check = self.diffa_db.get_latest_check(
            source_database=self.config_manager.source.get_db_name(),
            source_schema=self.config_manager.source.get_db_schema(),
            source_table=self.config_manager.source.get_db_table(),
            target_database=self.config_manager.target.get_db_name(),
            target_schema=self.config_manager.target.get_db_schema(),
            target_table=self.config_manager.target.get_db_table(),
        )

        if not self.is_full_diff:
            check_date = (
                latest_check["check_date"] if latest_check else DIFFA_BEGIN_DATE
            )
            logger.info(f"Last check date: {check_date}")
        else:
            check_date = DIFFA_BEGIN_DATE
            logger.info(
                f"Full diff mode is enabled. Checking from the beginning. Last check date: {check_date}"
            )

        return check_date

    def get_invalid_check_dates(self) -> Iterable[date]:

        invalid_checks = self.diffa_db.get_invalid_checks(
            source_database=self.config_manager.source.get_db_name(),
            source_schema=self.config_manager.source.get_db_schema(),
            source_table=self.config_manager.source.get_db_table(),
            target_database=self.config_manager.target.get_db_name(),
            target_schema=self.config_manager.target.get_db_schema(),
            target_table=self.config_manager.target.get_db_table(),
        )

        invalid_check_dates = [
            invalid_check["check_date"] for invalid_check in invalid_checks
        ]
        if self.is_full_diff:
            return None
        elif len(invalid_check_dates) > 0:
            logger.info(
                f"The number of invalid check dates is: {len(invalid_check_dates)}"
            )
            return invalid_check_dates
        else:
            logger.info("No invalid check dates found")
            return None

    def save_diffa_checks(self, merged_count_check_schemas: Iterable[DiffaCheckSchema]):
        """Upsert all the merged count checks to the diffa database"""

        diffa_checks = [
            diffa_check.model_dump() for diffa_check in merged_count_check_schemas
        ]
        if len(diffa_checks) > 0:
            self.diffa_db.upsert_diffa_checks(diffa_checks)
            logger.info(f"Upserted {len(diffa_checks)} records successfully!")
        else:
            logger.info("No records to upsert")
