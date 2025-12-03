from typing import Iterable
from datetime import date
from collections import defaultdict
from functools import reduce

from diffa.db.data_models import CountCheck, MergedCountCheck
from diffa.db.diffa_check import DiffaCheckService
from diffa.db.source_target import SourceTargetService
from diffa.config import ConfigManager
from diffa.utils import Logger, InvalidDiffException

logger = Logger(__name__)


class CheckManager:

    def __init__(self, config_manager: ConfigManager):
        self.cm = config_manager
        self.source_target_service = SourceTargetService(self.cm)
        self.diffa_check_service = DiffaCheckService(self.cm)

    def data_diff(self):
        """This will interupt the process when there are invalid diff found."""

        if not self.compare_tables():
            logger.error("❌ There is an invalid diff between source and target.")
            raise InvalidDiffException
        logger.info("✅ There is no invalid diff between source and target.")

    def compare_tables(self):
        """Data-diff comparison service. Will return True if there is any invalid diff."""

        logger.info(
            f"""Starting diffa comparison for:
                - Source: {self.cm.source.get_db_name()}.{self.cm.source.get_db_schema()}.{self.cm.source.get_db_table()}
                - Target: {self.cm.target.get_db_name()}.{self.cm.target.get_db_schema()}.{self.cm.target.get_db_table()}
            """
        )

        # Step 1: Get the last check date (for backfill mechanism)
        last_check_date = self.diffa_check_service.get_last_check_date()

        # Step 2: Get the invalid check dates (for re-check mechanism)
        invalid_check_dates = self.diffa_check_service.get_invalid_check_dates()

        # Step 3: Compare and merge the counts from the source and target databases
        source_counts, target_counts = self.source_target_service.get_counts(
            last_check_date, invalid_check_dates
        )
        merged_count_checks = self._merge_count_checks(source_counts, target_counts)
        merged_by_date = self._merge_by_check_date(merged_count_checks)

        # Step 4: Save the merged count checks to the diffa database
        self.diffa_check_service.save_diffa_checks(
            map(
                lambda merged_count_check: merged_count_check.to_diffa_check_schema(
                    source_database=self.cm.source.get_db_name(),
                    source_schema=self.cm.source.get_db_schema(),
                    source_table=self.cm.source.get_db_table(),
                    target_database=self.cm.target.get_db_name(),
                    target_schema=self.cm.target.get_db_schema(),
                    target_table=self.cm.target.get_db_table(),
                ),
                merged_by_date.values(),
            )
        )

        # Step 5: Build and log the check summary
        self._build_check_summary(merged_count_checks, merged_by_date)

        # Return True if there is any invalid diff
        return self._check_if_valid_diff(merged_by_date.values())

    def _check_if_valid_diff(self, merged_by_date: list[MergedCountCheck]) -> bool:
        return all(mcc.is_valid for mcc in merged_by_date)

    def _build_check_summary(
        self,
        merged_count_checks: Iterable[MergedCountCheck],
        merged_by_date: dict[date, MergedCountCheck],
    ):
        stats_by_day = {
            check_date: {
                "detailed_msgs": self._get_check_messages(
                    self._get_checks_by_date(merged_count_checks, check_date)
                ),
                "summary_msg": self._get_check_messages([mcc])[0],
            }
            for check_date, mcc in filter(
                lambda x: not x[1].is_valid, merged_by_date.items()
            )
        }

        summary_lines = [
            f"""
            - {check_date}:
                summary: 
                    {stats['summary_msg']}
                detailed: 
                    {stats['detailed_msgs']}
            """
            for check_date, stats in stats_by_day.items()
        ]
        stats_summary = (
            "\n".join(summary_lines)
            if summary_lines
            else "No failed days stats available"
        )

        logger.info(
            f"""
                Data-diff comparison result:
                Summary:
                - Total days checked: {len(merged_by_date)}
                - Stats by failed days:
                    {stats_summary}
            """
        )

    @staticmethod
    def _get_check_messages(merged_count_checks: Iterable[MergedCountCheck]):
        return [
            f"{'✅ No Diff' if mcc.is_valid else '❌ Diff'} {mcc}"
            for mcc in merged_count_checks
        ]

    @staticmethod
    def _get_checks_by_date(
        merged_count_checks: Iterable[MergedCountCheck], check_date: date
    ) -> list[MergedCountCheck]:
        return [mcc for mcc in merged_count_checks if mcc.check_date == check_date]

    @staticmethod
    def _merge_by_check_date(
        merged_count_checks: Iterable[MergedCountCheck],
    ) -> dict[date, MergedCountCheck]:
        merged = defaultdict(
            lambda: dict(check_date=None, source_count=0, target_count=0, is_valid=True)
        )
        for mcc in merged_count_checks:
            entry = merged[mcc.check_date]
            entry["source_count"] += mcc.source_count
            entry["target_count"] += mcc.target_count
            entry["is_valid"] &= mcc.is_valid
            entry["check_date"] = mcc.check_date

        return {cd: MergedCountCheck(**data) for cd, data in merged.items()}

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

        source_dict = reduce(
            lambda x, y: x | y,
            map(lambda x: x.to_flatten_dimension_format(), source_counts),
            {},
        )
        target_dict = reduce(
            lambda x, y: x | y,
            map(lambda x: x.to_flatten_dimension_format(), target_counts),
            {},
        )

        all_dims = set(source_dict.keys()) | set(target_dict.keys())

        merged_count_checks = []
        for dim in all_dims:
            source_count = source_dict.get(dim)
            target_count = target_dict.get(dim)
            merged_count_check = MergedCountCheck.from_counts(
                source_count, target_count
            )
            merged_count_checks.append(merged_count_check)

        return sorted(merged_count_checks, key=lambda x: x.check_date)
