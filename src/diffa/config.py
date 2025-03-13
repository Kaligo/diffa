import os
import json
from datetime import date
from enum import Enum
from abc import ABC, abstractmethod

import dsnparse

from diffa.utils import Logger

CONFIG_DIR = os.path.expanduser("~/.diffa")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
DIFFA_DB_SCHEMA = "diffa"
DIFFA_DB_TABLE = "diffa_checks"
DIFFA_CHECK_RUNS_TABLE = "diffa_check_runs"
DIFFA_BEGIN_DATE = date(2024, 1, 1)


class ExitCode(Enum):
    INVALID_DIFF = 4  # Invalid diff detected


logger = Logger(__name__)


class BaseConfig(ABC):
    def __init__(self, db_info: str = None, database: str = None):
        self.db_info = db_info
        self.database = database
        self.parsed_db_info = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key) and value is not None:
                setattr(self, key, value)

    def parse_db_info(self):
        if not self.parsed_db_info:
            try:
                dns = dsnparse.parse(self.db_info)
                self.parsed_db_info = self._extract_db_details(dns)
            except TypeError:
                logger.error("Invalid db info", exc_info=True)
                raise
        return self.parsed_db_info

    @abstractmethod
    def _extract_db_details(self, dns):
        pass

    def get_db_config(self):
        return self.parse_db_info()

    def get_database(self):
        return self.get_db_config().get("database")

    def get_schema(self):
        return self.get_db_config().get("schema")

    def get_db_info(self):
        return self.db_info

    def get_table(self):
        return self.get_db_config().get("table")


class SourceTargetConfig(BaseConfig):
    def __init__(
        self,
        db_info: str = None,
        database: str = None,
        schema: str = None,
        table: str = None,
    ):
        super().__init__(db_info, database)
        self.schema = schema
        self.table = table

    def _extract_db_details(self, dns):
        db_database = self.database or dns.database
        db_schema = self.schema or dns.schema
        db_table = self.table or dns.table
        return {
            "host": dns.host,
            "scheme": dns.scheme,
            "port": dns.port,
            "database": db_database,
            "user": dns.username,
            "password": dns.password,
            "schema": db_schema,
            "table": db_table,
            "db_url": f"{dns.scheme}://{dns.username}:{dns.password}@{dns.host}:{dns.port}/{db_database}",
        }


class DiffaDBConfig(BaseConfig):
    def __init__(self, db_info: str = None, database: str = None):
        super().__init__(db_info, database)
        self.schema = DIFFA_DB_SCHEMA
        self.tables = {
            "checks": DIFFA_DB_TABLE,
            "check_runs": DIFFA_CHECK_RUNS_TABLE,
        }

    def _extract_db_details(self, dns):
        db_database = self.database or dns.database
        return {
            "host": dns.host,
            "scheme": dns.scheme,
            "port": dns.port,
            "database": db_database,
            "user": dns.username,
            "password": dns.password,
            "schema": self.schema,
            "tables": self.tables,
            "db_url": f"{dns.scheme}://{dns.username}:{dns.password}@{dns.host}:{dns.port}/{db_database}",
        }
    
    def parse_db_info(self, table_key: str = None):
        if not self.parsed_db_info:
            try:
                dns = dsnparse.parse(self.db_info)
                self.parsed_db_info = self._extract_db_details(dns)
            except TypeError:
                logger.error("Invalid db info", exc_info=True)
                raise
        self.parsed_db_info["table"] = self.tables[table_key] if table_key else None
        return self.parsed_db_info

    def get_db_config(self, table_key: str = None):
        return self.parse_db_info(table_key)
    
    def get_table(self, table_key: str):
        return self.get_db_config(table_key)["table"]


class ConfigManager:
    """Manage all the configuration needed for Diffa Operations"""

    def __init__(
        self,
    ):
        self.config = {
            "source": SourceTargetConfig(),
            "target": SourceTargetConfig(),
            "diffa": DiffaDBConfig(),
        }
        self.__load_config()

    def configure(
        self,
        *,
        source_db_info: str = None,
        source_database: str = None,
        source_schema: str = "public",
        source_table: str,
        target_db_info: str = None,
        target_database: str = None,
        target_schema: str = "public",
        target_table: str,
        diffa_db_info: str = None,
    ):
        self.config["source"].update(
            db_info=source_db_info,
            database=source_database,
            schema=source_schema,
            table=source_table,
        )
        self.config["target"].update(
            db_info=target_db_info,
            database=target_database,
            schema=target_schema,
            table=target_table,
        )
        self.config["diffa"].update(
            db_info=diffa_db_info,
        )

    def __load_config(self):
        uri_config = {}
        os.makedirs(CONFIG_DIR, exist_ok=True)
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                uri_config = json.load(f)

        self.config["source"].update(
            db_info=os.getenv("DIFFA__SOURCE_URI", uri_config.get("source_uri"))
        )
        self.config["target"].update(
            db_info=os.getenv("DIFFA__TARGET_URI", uri_config.get("target_uri"))
        )
        self.config["diffa"].update(
            db_info=os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
        )

    def get_db_config(self, db_key: str, *args, **kwargs):
        return self.config[db_key].get_db_config(*args, **kwargs)

    def get_database(self, db_key: str, *args, **kwargs):
        return self.config[db_key].get_database(*args, **kwargs)

    def get_schema(self, db_key: str, *args, **kwargs):
        return self.config[db_key].get_schema(*args, **kwargs)

    def get_table(self, db_key: str, *args, **kwargs):
        return self.config[db_key].get_table(*args, **kwargs)

    def get_db_info(self, db_key: str, *args, **kwargs):
        return self.config[db_key].get_db_info(*args, **kwargs)
