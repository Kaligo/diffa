from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor

from diffa.db.factory import DatabaseFactory
from diffa.config import ConfigManager, DIFFA_BEGIN_DATE
from diffa.db.diffa import DiffRecordSchema, SQLAlchemyDiffaDatabase
from diffa.utils import Logger

logger = Logger(__name__)


class DiffaService:
    def __init__(self):
        self.cm = ConfigManager()

    def compare_tables(self):
        checking_date = self.__get_checking_date()
        logger.info(
            f"Checking date: {checking_date} for source: {self.cm.get_database('source')}.{self.cm.get_table('source')}"
        )

        source_db, target_db, history_db = (
            DatabaseFactory.create_database(self.cm.get_db_config("source")),
            DatabaseFactory.create_database(self.cm.get_db_config("target")),
            SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa")),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_source_count = executor.submit(source_db.get_count, checking_date)
            future_target_count = executor.submit(target_db.get_count, checking_date)

        source_count, target_count = (
            future_source_count.result(),
            future_target_count.result(),
        )

        status = "match" if source_count <= target_count else "mismatch"
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
            check_date=checking_date,
            source_count=source_count,
            target_count=target_count,
            status=status,
        )
        history_db.save_diff_record(diff_record)

        return True if status == "match" else False

    def __get_checking_date(self) -> date:
        today_date = date.today()

        history_db = SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa"))
        latest_record = history_db.get_latest_record(
            source_database=self.cm.get_database("source"),
            source_schema=self.cm.get_schema("source"),
            source_table=self.cm.get_table("source"),
            target_database=self.cm.get_database("target"),
            target_schema=self.cm.get_schema("target"),
            target_table=self.cm.get_table("target"),
        )

        if latest_record:
            if latest_record["status"] == "match":
                logical_next_date = latest_record["check_date"] + timedelta(days=1)
                yesterday_date = today_date - timedelta(days=1)
                return (
                    logical_next_date
                    if logical_next_date < yesterday_date
                    else yesterday_date
                )
            elif latest_record["status"] == "mismatch":
                return latest_record["check_date"]
        else:
            return DIFFA_BEGIN_DATE
