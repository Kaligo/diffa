from typing import Iterable

from diffa.db.data_models import CountCheck, MergedCountCheck
from diffa.db.database_handler import SourceTargetHandler, DiffaCheckHandler
from diffa.config import ConfigManager
from diffa.utils import Logger, InvalidDiffException

logger = Logger(__name__)


class CheckManager:

    def __init__(self, config_manager: ConfigManager):
        self.cm = config_manager
        self.source_target_handler = SourceTargetHandler(self.cm)
        self.diffa_check_handler = DiffaCheckHandler(self.cm)

    def data_diff(self):
        """This will interupt the process when there are invalid diff found."""

        if self.compare_tables():
            raise InvalidDiffException

    def compare_tables(self):
        """Data-diff comparison service. Will return True if there is any invalid diff."""

        logger.info(
            f"""Starting diffa comparison for:
                - Source: {self.cm.source.get_db_name()}.{self.cm.source.get_db_schema()}.{self.cm.source.get_db_table()}
                - Target: {self.cm.target.get_db_name()}.{self.cm.target.get_db_schema()}.{self.cm.target.get_db_table()}
            """
        )

        # Step 1: Get the last check date (for backfill mechanism)
        last_check_date = self.diffa_check_handler.get_last_check_date()

        # Step 2: Get the invalid check dates (for re-check mechanism)
        invalid_check_dates = self.diffa_check_handler.get_invalid_check_dates()

        # Step 3: Compare and merge the counts from the source and target databases
        source_counts, target_counts = self.source_target_handler.get_counts(
            last_check_date, invalid_check_dates
        )
        merged_count_checks = self._merge_count_checks(source_counts, target_counts)

        # Step 4: Save the merged count checks to the diffa database
        self.diffa_check_handler.save_diffa_checks(
            map(
                lambda merged_count_check: merged_count_check.to_diffa_check_schema(
                    source_database=self.cm.source.get_db_name(),
                    source_schema=self.cm.source.get_db_schema(),
                    source_table=self.cm.source.get_db_table(),
                    target_database=self.cm.target.get_db_name(),
                    target_schema=self.cm.target.get_db_schema(),
                    target_table=self.cm.target.get_db_table(),
                ),
                merged_count_checks,
            )
        )

        # Return True if there is any invalid diff
        return self._check_if_invalid_diff(merged_count_checks)

    def _check_if_invalid_diff(
        self, merged_count_checks: Iterable[MergedCountCheck]
    ) -> bool:
        for merged_count_check in merged_count_checks:
            if not merged_count_check.is_valid:
                return True
        return False

    def _merge_count_checks(
        self, source_counts: Iterable[CountCheck], target_counts: Iterable[CountCheck]
    ) -> Iterable[MergedCountCheck]:
        """
        Merging source and target counts.
        The algorithm is based on the following logic:
         Input: Iterable A: [1,2,5,6]
                Iterable B: [2,4,5,7]
         Output [(1,0), (2,2), (0,4), (5,5), (6,0), (0,7)]
        """

        source_dict = {count.check_date: count for count in source_counts}
        target_dict = {count.check_date: count for count in target_counts}

        all_dates = set(source_dict.keys()) | set(target_dict.keys())

        merged_count_checks = []
        for check_date in all_dates:
            source_count = source_dict.get(check_date)
            target_count = target_dict.get(check_date)
            merged_count_check = MergedCountCheck.from_counts(
                source_count, target_count
            )
            merged_count_checks.append(merged_count_check)

        return merged_count_checks
