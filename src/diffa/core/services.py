from datetime import datetime, timedelta
from diffa.core.db.databases import DatabaseManager
from diffa.core.db.models import DiffRecordSchema
from diffa.utils.logger import Logger

logger = Logger(__name__)


class DiffaService:
    def __init__(self, database_manager: DatabaseManager):
        self.dm = database_manager

    def compare_tables(self, execution_date: datetime, lookback_window: int):
        start_date, end_date = self.__get_time_range(execution_date, lookback_window)

        source_db, target_db, history_db = (
            self.dm.get_source_db(),
            self.dm.get_target_db(),
            self.dm.get_history_db(),
        )

        source_count, target_count = (
            source_db.get_count(start_date, end_date),
            target_db.get_count(start_date, end_date),
        )

        status = "valid" if source_count <= target_count else "invalid"
        logger.info(f"Source count: {source_count}, Target count: {target_count}, Status: {status}")
        diff_record = DiffRecordSchema(
            table_name=source_db.db_info["table"],
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
