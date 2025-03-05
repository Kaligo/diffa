from typing import Iterable

from diffa.db.data_models import CountCheck, MergedCountCheck
from diffa.db.database_handler import DatabaseHandler
from diffa.config import ConfigManager
from diffa.utils import Logger

logger = Logger(__name__)


class DiffaService:
    def __init__(self):

        self.cm = ConfigManager()
        self.db_handler = DatabaseHandler(self.cm)

    def compare_tables(self):
        """Data-diff comparison service"""

        logger.info(
            f"""Starting diffa comparison for:
                - Source: {self.cm.get_database('source')}.{self.cm.get_schema('source')}.{self.cm.get_table('source')}
                - Target: {self.cm.get_database('target')}.{self.cm.get_schema('target')}.{self.cm.get_table('target')}
            """
        )

        # Step 1: Get the last check date (for backfill mechanism)
        last_check_date = self.db_handler.get_last_check_date()

        # Step 2: Get the invalid check dates (for re-check mechanism)
        invalid_check_dates = self.db_handler.get_invalid_check_dates()

        # Step 3: Compare and merge the counts from the source and target databases
        source_counts, target_counts = self.db_handler.get_counts(
            last_check_date, invalid_check_dates
        )
        merged_count_check_schemas = self.__merge_count_checks(
            source_counts, target_counts
        )

        # Step 4: Save the merged count checks to the diffa database
        self.db_handler.save_diffa_checks(merged_count_check_schemas)

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
            ).to_diffa_check_schema(**db_infos)
            merged_count_check_schemas.append(merged_count_check)

        return merged_count_check_schemas
