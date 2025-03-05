from datetime import date
from typing import List

import psycopg2
import psycopg2.extras

from diffa.utils import Logger
from diffa.db.base import Database, CountCheck

logger = Logger(__name__)


class PosgrestDatabase(Database):
    """PostgreSQL and Redshift Database Adapter"""

    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self.conn = None

    def connect(self):
        if not self.conn:
            self.conn = psycopg2.connect(
                host=self.db_config["host"],
                port=self.db_config["port"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                sslmode="prefer",  # Prefer SSL mode
            )
            self.conn.set_session(autocommit=True)

    def execute_query(self, query: str, sql_params: tuple = None):
        self.connect()
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, sql_params)
                for row in cursor:
                    yield row
        finally:
            self.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def execute_non_query(self, query: str, params: dict = None):
        self.connect()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
        finally:
            self.close()

    def get_counts(self, latest_check_date: date, invalid_check_dates: List[date]):
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
        query = f"""
            SELECT 
                created_at::DATE as check_date,
                COUNT(*) AS cnt 
            FROM {self.db_config['schema']}.{self.db_config['table']}
            WHERE
                {catchup_where_clause}
                {backfill_where_clause}
            GROUP BY created_at::DATE
            ORDER BY created_at::DATE ASC
        """
        try:
            logger.info(f"Executing query: {query}")
            for row in self.execute_query(query):
                yield CountCheck(**row)
        finally:
            self.close()
