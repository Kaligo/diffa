from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from diffa.db.factory import DatabaseFactory
from diffa.config import ConfigManager
from diffa.db.diffa import DiffRecordSchema, SQLAlchemyDiffaDatabase
from diffa.utils import Logger

logger = Logger(__name__)


class DiffaService:
    def __init__(self):
        self.cm = ConfigManager()

    def __get_time_range(self, execution_date: datetime, lookback_window: int):
        start_date, end_date = (
            execution_date - timedelta(days=lookback_window),
            execution_date,
        )
        return start_date, end_date

    def compare_tables(self, execution_date: datetime, lookback_window: int):
        start_date, end_date = self.__get_time_range(execution_date, lookback_window)

        source_db, target_db, history_db = (
            DatabaseFactory.create_database(self.cm.get_db_config("source")),
            DatabaseFactory.create_database(self.cm.get_db_config("target")),
            SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa")),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_source_count = executor.submit(
                source_db.get_count, start_date, end_date
            )
            future_target_count = executor.submit(
                target_db.get_count, start_date, end_date
            )

        source_count, target_count = (
            future_source_count.result(),
            future_target_count.result(),
        )

        status = "valid" if source_count <= target_count else "invalid"
        logger.info(
            f"Source count: {source_count}, Target count: {target_count}, Status: {status}"
        )
        diff_record = DiffRecordSchema(
            source_database=source_db.db_config["database"],
            source_schema=source_db.db_config["schema"],
            source_table=source_db.db_config["table"],
            target_database=target_db.db_config["database"],
            target_schema=target_db.db_config["schema"],
            target_table=target_db.db_config["table"],
            start_check_date=start_date,
            end_check_date=end_date,
            source_count=source_count,
            target_count=target_count,
            status=status,
        )
        history_db.save_diff_record(diff_record)

        return True if status == "valid" else False

    def verify_diff_status(self, start_date: datetime, end_date: datetime):
        """To Check during a specific time range, data is miss or not"""

        history_db = SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa"))
        number_of_checks = history_db.get_number_of_checks(
            source_database=self.cm.get_database("source"),
            source_schema=self.cm.get_schema("source"),
            source_table=self.cm.get_table("source"),
            target_database=self.cm.get_database("target"),
            target_schema=self.cm.get_schema("target"),
            target_table=self.cm.get_table("target"),
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(
            f"Number of checks from {start_date} to {end_date}: {number_of_checks}"
        )
        if number_of_checks == 0:
            return True

        latest_valid_check = history_db.get_latest_valid_diff_check(
            source_database=self.cm.get_database("source"),
            source_schema=self.cm.get_schema("source"),
            source_table=self.cm.get_table("source"),
            target_database=self.cm.get_database("target"),
            target_schema=self.cm.get_schema("target"),
            target_table=self.cm.get_table("target"),
            start_date=start_date,
            end_date=end_date,
        )
        latest_valid_check = (
            latest_valid_check.model_dump() if latest_valid_check else None
        )
        if latest_valid_check:
            logger.info(
                f"Valid diff check: created at {latest_valid_check['created_at']}"
            )
            return True
        else:
            logger.info(
                f"No valid check found during {start_date} to {end_date}. It's diff !!!"
            )
            return False

    def get_oldest_pending_reconciliation(self):
        """Get the oldest pending reconciliation"""

        history_db = SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa"))
        oldest_start_check_date, oldest_end_check_date = (
            history_db.get_unrecociled_diff_checks(
                source_database=self.cm.get_database("source"),
                source_schema=self.cm.get_schema("source"),
                source_table=self.cm.get_table("source"),
                target_database=self.cm.get_database("target"),
                target_schema=self.cm.get_schema("target"),
                target_table=self.cm.get_table("target"),
            )
        )
        return {
            "start_check_date": oldest_start_check_date,
            "end_check_date": oldest_end_check_date,
        }
