import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Iterable
from diffa.core.db.models import DiffRecordSchema, DiffRecord
from diffa.core.db.config import ConfigManager
from diffa.utils.logger import Logger

logger = Logger(__name__)


class Database:
    def __init__(self, db_info: dict):
        self.db_info = db_info
        self.conn = None

    def connect(self):
        if self.conn is None or self.close():
            self.conn = psycopg2.connect(
                host=self.db_info.get("host"),
                port=self.db_info.get("port"),
                database=self.db_info.get("database"),
                user=self.db_info.get("user"),
                password=self.db_info.get("password"),
            )
            self.conn.set_session(autocommit=True)

    def execute_query(self, query: str):
        self.connect()
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query)
                for row in cursor:
                    yield row
        finally:
            self.close()

    def execute_non_query(self, query: str, params: dict = None):
        self.connect()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
        finally:
            self.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None


class SourceTargetDatabase(Database):

    def get_count(self, start_date: datetime, end_date: datetime):
        query = f"""SELECT COUNT(*) AS results FROM {self.db_info['schema']}.{self.db_info['table']} 
            WHERE created_at BETWEEN '{start_date}' AND '{end_date}'"""
        try:
            logger.info(f"Querying: {query}")
            result = int(list(self.execute_query(query))[0]["results"])
            logger.info(f"Result of the query '{query}': {result}")
            return result
        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        finally:
            self.close()


class DiffaDatabase(Database):

    def connect(self):
        engine = create_engine(self.db_info["db_url"])
        self.conn = sessionmaker(bind=engine)()
        return self.conn

    def execute_query(self, query: str):
        self.connect()
        try:
            result = self.conn.execute(query)
            for row in result:
                yield dict(row)
        finally:
            self.close()

    def execute_non_query(self, query, params: dict = None):
        self.connect()
        try:
            self.conn.execute(query, params)
        finally:
            self.close()

    def execute_non_query(self, query: str, params: dict = None):
        self.connect()
        self.conn.execute(query, params)

    def get_invalid_diff_records(self) -> Iterable[DiffRecordSchema]:
        self.connect()
        try:
            diff_records = (
                self.conn.query(DiffRecord)
                .filter(DiffRecord.is_valid == "Invalid")
                .all()
            )
            for record in diff_records:
                yield DiffRecordSchema.model_validate(record)
        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        finally:
            self.close()

    def save_diff_record(self, diff_record: DiffRecordSchema):
        self.connect()
        record_dict = diff_record.model_dump()
        try:
            self.conn.add(DiffRecord(**record_dict))
            self.conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        finally:
            self.close()
    
    def close(self):
        self.conn = None
    
    def create_diff_table(self):
        self.connect()
        try:
            DiffRecord.metadata.create_all(self.conn.get_bind())
        finally:
            self.close()


class DatabaseManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_manager: ConfigManager):
        self.source_db_info = config_manager.get_source_db_info()
        self.target_db_info = config_manager.get_target_db_info()
        self.history_db_info = config_manager.get_history_db_info()

    def get_source_db(self):
        return SourceTargetDatabase(self.source_db_info)

    def get_target_db(self):
        return SourceTargetDatabase(self.target_db_info)

    def get_history_db(self):
        return DiffaDatabase(self.history_db_info)
