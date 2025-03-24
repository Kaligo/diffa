from datetime import date
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

from diffa.db.factory import DatabaseFactory
from diffa.db.diffa import DiffaCheckDatabase, DiffaCheckRunDatabase
from diffa.db.data_models import CountCheck, DiffaCheckSchema, DiffaCheckRunSchema
from diffa.config import ConfigManager, DIFFA_BEGIN_DATE
from diffa.utils import Logger

logger = Logger(__name__)


class SourceTargetHandler:

    def __init__(self, config_manager: ConfigManager):
        self.source_db = DatabaseFactory.create_database(
            config_manager.source.get_db_config()
        )
        self.target_db = DatabaseFactory.create_database(
            config_manager.target.get_db_config()
        )

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


class DiffaCheckHandler:

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.diffa_db = DiffaCheckDatabase(
            config_manager.diffa_check.get_db_config()
        )

    def get_last_check_date(self) -> date:

        latest_check = self.diffa_db.get_latest_check(
            source_database=self.config_manager.source.get_db_name(),
            source_schema=self.config_manager.source.get_db_schema(),
            source_table=self.config_manager.source.get_db_table(),
            target_database=self.config_manager.target.get_db_name(),
            target_schema=self.config_manager.target.get_db_schema(),
            target_table=self.config_manager.target.get_db_table(),
        )

        check_date = latest_check["check_date"] if latest_check else DIFFA_BEGIN_DATE
        logger.info(f"Last check date: {check_date}")

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
        if len(invalid_check_dates) > 0:
            logger.info(
                f"The number of invalid check dates is: {len(invalid_check_dates)}"
            )
            return invalid_check_dates
        else:
            logger.info("No invalid check dates found")
            return None

    def save_diffa_checks(self, merged_count_check_schemas: Iterable[DiffaCheckSchema]):
        """Upsert all the merged count checks to the diffa database"""

        self.diffa_db.upsert_diffa_checks(merged_count_check_schemas)


class DiffaCheckRunHandler:

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.diffa_check_run_db = DiffaCheckRunDatabase(
            config_manager.diffa_check_run.get_db_config()
        )

    def checking_running_check_runs(self) -> list[str]:
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

    def update_check_run_as_status(
        self, diffa_check_run_schema: DiffaCheckRunSchema, status: str
    ):
        """Mark a check run as completed"""

        diffa_check_run_schema.status = status
        self.diffa_check_run_db.update_diffa_check_run_record_with_status(
            diffa_check_run_schema.run_id, status
        )
