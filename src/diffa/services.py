from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from typing import Iterable, Optional
from concurrent.futures import ThreadPoolExecutor

from diffa.db.factory import DatabaseFactory
from diffa.db.diffa import DiffaCheckSchema, SQLAlchemyDiffaDatabase
from diffa.db.base import CountCheck
from diffa.config import ConfigManager, DIFFA_BEGIN_DATE
from diffa.utils import Logger

logger = Logger(__name__)


@dataclass
class MergedCountCheck:
    source_count: int
    target_count: int
    check_date: date
    is_valid: bool = field(init=False)

    def __post_init__(self):
        self.is_valid = True if self.source_count <= self.target_count else False

    @classmethod
    def from_counts(
        cls, source: Optional[CountCheck] = None, target: Optional[CountCheck] = None
    ):
        if source and target:
            if source.check_date != target.check_date:
                raise ValueError("Source and target counts are not for the same date.")
        elif not source and not target:
            raise ValueError("Both source and target counts are missing.")

        check_date = source.check_date if source else target.check_date
        source_count = source.cnt if source else 0
        target_count = target.cnt if target else 0

        return cls(source_count, target_count, check_date)

    def to_DiffaCheckSchema(
        self,
        source_database: str,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
    ):
        logger.info(
            f"""
            Merging result:
                Source: {source_database}.{source_schema}.{source_table}
                Target: {target_database}.{target_schema}.{target_table}
                Source Count: {self.source_count}
                Target Count: {self.target_count}
                Check Date: {self.check_date}
                Is Valid: {self.is_valid}
            """
        )

        return DiffaCheckSchema(
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
            target_database=target_database,
            target_schema=target_schema,
            target_table=target_table,
            check_date=self.check_date,
            source_count=self.source_count,
            target_count=self.target_count,
            is_valid=self.is_valid,
            diff_count=self.target_count - self.source_count
        )


class DiffaService:
    def __init__(self):

        self.cm = ConfigManager()

    def compare_tables(self):
        logger.info(
            f"Starting diffa comparison for source: {self.cm.get_database('source')}.{self.cm.get_table('source')}"
        )

        last_check_date = self.__get_last_check_date()
        logger.info(f"Last check date: {last_check_date}")

        invalid_check_dates = self.__get_invalid_check_dates()
        logger.info(f"Invalid check dates: {invalid_check_dates}")

        source_db, target_db, history_db = (
            DatabaseFactory.create_database(self.cm.get_db_config("source")),
            DatabaseFactory.create_database(self.cm.get_db_config("target")),
            SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa")),
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_source_count = executor.submit(
                source_db.get_counts, last_check_date, invalid_check_dates
            )
            future_target_count = executor.submit(
                target_db.get_counts, last_check_date, invalid_check_dates
            )

        source_counts, target_counts = (
            future_source_count.result(),
            future_target_count.result(),
        )

        merged_count_check_schemas = self.__merge_count_checks(
            source_counts, target_counts
        )
        history_db.upsert_diffa_checks(merged_count_check_schemas)

    def __get_last_check_date(self) -> date:

        history_db = SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa"))
        latest_check = history_db.get_latest_check(
            source_database=self.cm.get_database("source"),
            source_schema=self.cm.get_schema("source"),
            source_table=self.cm.get_table("source"),
            target_database=self.cm.get_database("target"),
            target_schema=self.cm.get_schema("target"),
            target_table=self.cm.get_table("target"),
        )

        return latest_check["check_date"] if latest_check else DIFFA_BEGIN_DATE

    def __get_invalid_check_dates(self) -> Optional[list[date] | None]:

        history_db = SQLAlchemyDiffaDatabase(self.cm.get_db_config("diffa"))
        invalid_checks = history_db.get_invalid_checks(
            source_database=self.cm.get_database("source"),
            source_schema=self.cm.get_schema("source"),
            source_table=self.cm.get_table("source"),
            target_database=self.cm.get_database("target"),
            target_schema=self.cm.get_schema("target"),
            target_table=self.cm.get_table("target"),
        )

        invalid_check_dates = [
            invalid_check["check_date"] for invalid_check in invalid_checks
        ]
        if len(invalid_check_dates) > 0:
            return invalid_check_dates
        else:
            return None

    def __merge_count_checks(
        self, source_counts: Iterable[CountCheck], target_counts: Iterable[CountCheck]
    ) -> Iterable[MergedCountCheck]:
        """
        Merging source and target counts.
        The algorithm is based on the following logic:
         Input: Iterable A: [1,2,5,6]
                Iterable B: [2,4,5,7]
         Output [(1,0), (2,2), (0,4), (5,5), (6,0), (0,7)]
        """

        db_infos = {
            "source_database": self.cm.get_database("source"),
            "source_schema": self.cm.get_schema("source"),
            "source_table": self.cm.get_table("source"),
            "target_database": self.cm.get_database("target"),
            "target_schema": self.cm.get_schema("target"),
            "target_table": self.cm.get_table("target"),
        }

        source_dict = {count.check_date: count for count in source_counts}
        target_dict = {count.check_date: count for count in target_counts}

        all_dates = set(source_dict.keys()) | set(target_dict.keys())

        merged_count_check_schemas = []
        for check_date in all_dates:
            source_count = source_dict.get(check_date)
            target_count = target_dict.get(check_date)
            merged_count_check = MergedCountCheck.from_counts(
                source_count, target_count
            ).to_DiffaCheckSchema(**db_infos)
            merged_count_check_schemas.append(merged_count_check)

        return merged_count_check_schemas
