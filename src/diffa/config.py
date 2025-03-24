import os
import json
from datetime import date
from enum import Enum
from typing import Any

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


class DBConfig:
    def __init__(
        self,
        db_url: str = None,
        db_name: str = None,
        db_schema: str = None,
        db_table: str = None,
    ):
        self.db_url = db_url
        self.db_name = db_name
        self.db_schema = db_schema
        self.db_table = db_table

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, self.__class__):
            return NotImplemented
        return self.__dict__ == __value.__dict__

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key) and value is not None:
                setattr(self, key, value)
        return self

    def _parse_db_info(self):
        try:
            dns = dsnparse.parse(self.db_url)
            parsed_db_info = self._extract_db_details(dns)
            self._validate_parsed_db_info(parsed_db_info)
            return parsed_db_info
        except TypeError:
            logger.error("Invalid db info", exc_info=True)
            raise

    def _validate_parsed_db_info(self, parsed_db_info: dict):
        for key, value in parsed_db_info.items():
            if not value:
                raise ValueError(
                    f"Configuration for {key} is missing. Please provide it!"
                )

    def _extract_db_details(self, dns):
        db_database = self.db_name or dns.database
        return {
            "host": dns.host,
            "scheme": dns.scheme,
            "port": dns.port,
            "database": db_database,
            "user": dns.username,
            "password": dns.password,
            "schema": self.db_schema,
            "table": self.db_table,
            "db_url": f"{dns.scheme}://{dns.username}:{dns.password}@{dns.host}:{dns.port}/{db_database}",
        }

    def get_db_config(self):
        return self._parse_db_info()

    def get_db_name(self):
        return self.get_db_config().get("database")

    def get_db_schema(self):
        return self.get_db_config().get("schema")

    def get_db_url(self):
        return self.db_url

    def get_db_table(self):
        return self.get_db_config().get("table")


class ConfigManager:
    """Manage all the configuration needed for Diffa Operations"""

    def __init__(
        self,
        source_config: DBConfig = DBConfig(),
        target_config: DBConfig = DBConfig(),
        diffa_check_config: DBConfig = DBConfig(),
        diffa_check_run_config: DBConfig = DBConfig(),
    ):
        self.config = {
            "source": source_config,
            "target": target_config,
            "diffa_check": diffa_check_config.update(
                db_schema=DIFFA_DB_SCHEMA, db_table=DIFFA_DB_TABLE
            ),
            "diffa_check_run": diffa_check_run_config.update(
                db_schema=DIFFA_DB_SCHEMA, db_table=DIFFA_CHECK_RUNS_TABLE
            ),
        }
        self.__load_config()

    def configure(
        self,
        *,
        source_db_url: str = None,
        source_database: str = None,
        source_schema: str = "public",
        source_table: str,
        target_db_url: str = None,
        target_database: str = None,
        target_schema: str = "public",
        target_table: str,
        diffa_db_url: str = None,
    ):
        self.source.update(
            db_url=source_db_url,
            db_name=source_database,
            db_schema=source_schema,
            db_table=source_table,
        )
        self.target.update(
            db_url=target_db_url,
            db_name=target_database,
            db_schema=target_schema,
            db_table=target_table,
        )
        self.diffa_check.update(
            db_url=diffa_db_url,
        )
        self.diffa_check_run.update(
            db_url=diffa_db_url,
        )
        return self

    def __load_config(self):
        uri_config = {}
        os.makedirs(CONFIG_DIR, exist_ok=True)
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                uri_config = json.load(f)

        self.source.update(
            db_url=self.source.db_url
            or os.getenv("DIFFA__SOURCE_URI", uri_config.get("source_uri"))
        )
        self.target.update(
            db_url=self.target.db_url
            or os.getenv("DIFFA__TARGET_URI", uri_config.get("target_uri"))
        )
        self.diffa_check.update(
            db_url=self.diffa_check.db_url
            or os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
        )
        self.diffa_check_run.update(
            db_url=self.diffa_check_run.db_url
            or os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
        )

    def __getattr__(self, __name: str) -> DBConfig:
        """Dynamically access DBConfig attributes (e.g config_manager.source.database)"""
        if __name in self.config:
            return self.config[__name]
        raise ArithmeticError(f"'ConfigManager' has no config '{__name}'")
