from datetime import date
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

from diffa.db.factory import DatabaseFactory
from diffa.db.diffa import SQLAlchemyDiffaDatabase
from diffa.db.data_models import CountCheck, DiffaCheckSchema
from diffa.config import ConfigManager, DIFFA_BEGIN_DATE
from diffa.utils import Logger

logger = Logger(__name__)


class DatabaseHandler:
    """Handler for all database operations"""

    def __init__(self, config_manager: ConfigManager):

        self.config_manager = config_manager
        self.source_db = DatabaseFactory.create_database(
            config_manager.get_db_config("source")
        )
        self.target_db = DatabaseFactory.create_database(
            config_manager.get_db_config("target")
        )
        self.diffa_db = SQLAlchemyDiffaDatabase(config_manager.get_db_config("diffa"))

    def get_counts(
        self, last_check_date: date, invalid_check_dates: Iterable[date]
    ) -> Iterable[CountCheck]:
        def to_count_check(count_dict: dict) -> CountCheck:
            return CountCheck(**count_dict)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_source_count = executor.submit(
                self.source_db.get_counts, last_check_date, invalid_check_dates
            )
            future_target_count = executor.submit(
                self.target_db.get_counts, last_check_date, invalid_check_dates
            )

        source_counts, target_counts = (
            future_source_count.result(),
            future_target_count.result(),
        )
        return map(to_count_check, source_counts), map(to_count_check, target_counts)

    def get_last_check_date(self) -> date:

        latest_check = self.diffa_db.get_latest_check(
            source_database=self.config_manager.get_database("source"),
            source_schema=self.config_manager.get_schema("source"),
            source_table=self.config_manager.get_table("source"),
            target_database=self.config_manager.get_database("target"),
            target_schema=self.config_manager.get_schema("target"),
            target_table=self.config_manager.get_table("target"),
        )

        check_date = latest_check["check_date"] if latest_check else DIFFA_BEGIN_DATE
        logger.info(f"Last check date: {check_date}")

        return check_date

    def get_invalid_check_dates(self) -> Iterable[date]:

        invalid_checks = self.diffa_db.get_invalid_checks(
            source_database=self.config_manager.get_database("source"),
            source_schema=self.config_manager.get_schema("source"),
            source_table=self.config_manager.get_table("source"),
            target_database=self.config_manager.get_database("target"),
            target_schema=self.config_manager.get_schema("target"),
            target_table=self.config_manager.get_table("target"),
        )

        invalid_check_dates = [
            invalid_check["check_date"] for invalid_check in invalid_checks
        ]
        if len(invalid_check_dates) > 0:
            logger.info(f"The number of invalid check dates is: {len(invalid_check_dates)}")
            return invalid_check_dates
        else:
            logger.info("No invalid check dates found")
            return None

    def save_diffa_checks(self, merged_count_check_schemas: Iterable[DiffaCheckSchema]):
        """Upsert all the merged count checks to the diffa database"""

        self.diffa_db.upsert_diffa_checks(merged_count_check_schemas)
