from datetime import datetime, timedelta

from diffa.db.factory import DatabaseFactory
from diffa.config import ConfigManager
from diffa.db.diffa import DiffRecordSchema, SQLAlchemyDiffaDatabase
from diffa.utils import Logger

logger = Logger(__name__)


class DiffaService:
    def __init__(self):
        self.cm = ConfigManager()

    def compare_tables(self, execution_date: datetime, lookback_window: int):
        start_date, end_date = self.__get_time_range(execution_date, lookback_window)

        source_db, target_db, history_db = (
            DatabaseFactory.create_database(self.cm.get_db_config("source")),
            DatabaseFactory.create_database(self.cm.get_db_config("target")),
            SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa")),
        )

        source_count, target_count = (
            source_db.get_count(start_date, end_date),
            target_db.get_count(start_date, end_date),
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

    def __get_time_range(self, execution_date: datetime, lookback_window: int):
        start_date, end_date = (
            execution_date - timedelta(days=lookback_window),
            execution_date,
        )
        return start_date, end_date
