from datetime import date
from typing import List, Iterable, Optional
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import psycopg2.extras

from diffa.utils import Logger
from diffa.db.connect import PostgresConnection
from diffa.config import SourceConfig
from diffa.db.data_models import CountCheck
from diffa.config import ConfigManager

logger = Logger(__name__)


class SourceTargetDatabase:
    """Base class for the Source Target DB handling"""

    def __init__(self, db_config: SourceConfig) -> None:
        self.db_config = db_config
        self.conn = PostgresConnection(self.db_config.get_db_config())

    def _execute_query(self, query: str, sql_params: tuple = None):

        conn = self.conn.connect()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, sql_params)
                for row in cursor:
                    yield row
        except Exception as e:
            logger.info("Error encountered. Closing the DB connection...")
            conn.close()
            raise e

    def _build_count_query(
        self,
        latest_check_date: date,
        invalid_check_dates: List[date],
        diff_dimension_cols: Optional[List[str]] = None,
    ):
        backfill_where_clause = (
            f" (created_at::DATE IN ({','.join([f"'{date}'" for date in invalid_check_dates])})) OR"
            if invalid_check_dates
            else ""
        )
        catchup_where_clause = f"""(
            created_at::DATE > '{latest_check_date}'
            AND 
            created_at::DATE <= CURRENT_DATE - INTERVAL '2 DAY' 
        )
        """
        group_by_diff_dimensions_clause = (
            f"{','.join(diff_dimension_cols)}" if diff_dimension_cols else ""
        )
        select_diff_dimensions_clause = (
            f"{','.join([f'{col}::text' for col in diff_dimension_cols])}"
            if diff_dimension_cols
            else ""
        )

        return f"""
            SELECT 
                created_at::DATE as check_date,
                COUNT(*) AS cnt,
                {select_diff_dimensions_clause}
            FROM {self.db_config.get_db_schema()}.{self.db_config.get_db_table()}
            WHERE
                {backfill_where_clause}
                {catchup_where_clause}
            GROUP BY created_at::DATE, {group_by_diff_dimensions_clause}
            ORDER BY created_at::DATE ASC
        """

    def count(self, latest_check_date: date, invalid_check_dates: List[date]):

        if self.db_config.get_diff_dimension_cols():
            count_query = self._build_count_query(
                latest_check_date,
                invalid_check_dates,
                self.db_config.get_diff_dimension_cols(),
            )
            logger.warning(
                "Diff dimensions are enabled. May impact the performance of the query"
            )
        else:
            count_query = self._build_count_query(
                latest_check_date, invalid_check_dates
            )
        logger.info(
            f"Executing the count query on {self.db_config.get_db_scheme()}: {count_query}"
        )
        return self._execute_query(count_query)


class SourceTargetService:

    def __init__(self, config_manager: ConfigManager):
        self.source_db = SourceTargetDatabase(config_manager.source)
        self.target_db = SourceTargetDatabase(config_manager.target)

    def get_counts(
        self, last_check_date: date, invalid_check_dates: Iterable[date]
    ) -> Iterable[CountCheck]:
        def to_count_check(
            count_dict: dict, diff_dimension_cols: Optional[List[str]] = None
        ) -> CountCheck:
            if diff_dimension_cols:
                return CountCheck.create_with_dimensions(diff_dimension_cols)(
                    **count_dict
                )
            else:
                return CountCheck(**count_dict)

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_source_count = executor.submit(
                self.source_db.count, last_check_date, invalid_check_dates
            )
            future_target_count = executor.submit(
                self.target_db.count, last_check_date, invalid_check_dates
            )

        source_counts, target_counts = (
            future_source_count.result(),
            future_target_count.result(),
        )
        return map(
            partial(
                to_count_check,
                diff_dimension_cols=self.source_db.db_config.get_diff_dimension_cols(),
            ),
            source_counts,
        ), map(
            partial(
                to_count_check,
                diff_dimension_cols=self.target_db.db_config.get_diff_dimension_cols(),
            ),
            target_counts,
        )
