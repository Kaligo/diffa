from datetime import datetime, date

import psycopg2
import psycopg2.extras

from diffa.utils import Logger
from diffa.db.base import Database

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
                sslmode="prefer", # Prefer SSL mode
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

    def get_count(self, checking_date: date):
        query = f"""
            SELECT COUNT(*) AS results 
            FROM {self.db_config['schema']}.{self.db_config['table']}
            WHERE created_at::DATE = '{checking_date}'
        """
        try:
            logger.info(f"Querying: {query}")
            result = int(
                list(self.execute_query(query))[0]["results"]
            )
            logger.info(f"Result of the query '{query}': {result}")
            return result
        finally:
            self.close()
