import os
import json
from datetime import date
from enum import Enum
from urllib.parse import urlparse
from typing import List, Optional

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
        db_uri: str = None,
        db_name: str = None,
        db_schema: str = None,
        db_table: str = None,
    ):
        self.db_uri = db_uri
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
            # Users can ref an env var in the DB_URI in the command
            if self.db_uri.startswith("$"):
                self.db_uri = os.getenv(self.db_uri[1:], "")
            dns = urlparse(self.db_uri)
            parsed_db_info = self._extract_db_details(dns)
            self._validate_parsed_db_info(parsed_db_info)
            return parsed_db_info
        except Exception as e:
            logger.error(f"Error parsing DB info: {e}")
            raise

    def _validate_parsed_db_info(self, parsed_db_info: dict):
        for key, value in parsed_db_info.items():
            if not value:
                raise ValueError(
                    f"Configuration for {key} is missing. Please provide it!"
                )

    def _extract_db_details(self, dns):
        db_database = self.db_name or dns.path.lstrip("/")
        return {
            "host": dns.hostname,
            "scheme": dns.scheme,
            "port": dns.port,
            "database": db_database,
            "user": dns.username,
            "password": dns.password,
            "schema": self.db_schema,
            "table": self.db_table,
            "db_uri": f"{dns.scheme}://{dns.username}:{dns.password}@{dns.hostname}:{dns.port}/{db_database}",
        }

    def get_db_config(self):
        return self._parse_db_info()

    def get_db_name(self):
        return self.get_db_config().get("database")

    def get_db_schema(self):
        return self.get_db_config().get("schema")

    def get_db_scheme(self):
        return self.get_db_config().get("scheme")

    def get_db_uri(self):
        return self.db_uri

    def get_db_table(self):
        return self.get_db_config().get("table")


class SourceConfig(DBConfig):
    """A class to handle the configs for the Source DBs"""
    def __init__(self, *args, diff_dimension_cols: Optional[List[str]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.diff_dimension_cols = diff_dimension_cols or []

    def get_diff_dimension_cols(self):
        return self.diff_dimension_cols
class DiffaConfig(DBConfig):
    """A class to handle the configs for the Diffa DB"""
    def __init__(self, *args, full_diff: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.full_diff = full_diff

    def is_full_diff(self):
        return self.full_diff

class ConfigManager:
    """Manage all the configuration needed for Diffa Operations"""

    def __init__(
        self,
        source_config: SourceConfig = SourceConfig(),
        target_config: SourceConfig = SourceConfig(),
        diffa_check_config: DiffaConfig = DiffaConfig(),
        diffa_check_run_config: DiffaConfig = DiffaConfig(),
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
        source_db_uri: str = None,
        source_database: str = None,
        source_schema: str = "public",
        source_table: str,
        target_db_uri: str = None,
        target_database: str = None,
        target_schema: str = "public",
        target_table: str,
        diffa_db_uri: str = None,
        diff_dimension_cols: List[str] = None,
        full_diff: bool = False,
    ):
        self.source.update(
            db_uri=source_db_uri,
            db_name=source_database,
            db_schema=source_schema,
            db_table=source_table,
            diff_dimension_cols=diff_dimension_cols,
        )
        self.target.update(
            db_uri=target_db_uri,
            db_name=target_database,
            db_schema=target_schema,
            db_table=target_table,
            diff_dimension_cols=diff_dimension_cols,
        )
        self.diffa_check.update(
            db_uri=diffa_db_uri,
            full_diff=full_diff,
        )
        self.diffa_check_run.update(
            db_uri=diffa_db_uri,
        )
        return self

    def __load_config(self):
        uri_config = {}
        os.makedirs(CONFIG_DIR, exist_ok=True)
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                uri_config = json.load(f)

        self.source.update(
            db_uri=self.source.db_uri
            or os.getenv("DIFFA__SOURCE_URI", uri_config.get("source_uri"))
        )
        self.target.update(
            db_uri=self.target.db_uri
            or os.getenv("DIFFA__TARGET_URI", uri_config.get("target_uri"))
        )
        self.diffa_check.update(
            db_uri=self.diffa_check.db_uri
            or os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
        )
        self.diffa_check_run.update(
            db_uri=self.diffa_check_run.db_uri
            or os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
        )

    @classmethod
    def save_config(self, source_uri: str, target_uri: str, diffa_uri: str):
        """Saving the Config into the FileSystem"""

        config = {
            "source_uri": source_uri,
            "target_uri": target_uri,
            "diffa_uri": diffa_uri,
        }
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)

        logger.info("Configuration saved to successfully.")

    def __getattr__(self, __name: str) -> DBConfig:
        """Dynamically access DBConfig attributes (e.g config_manager.source.get_db_name())"""

        if __name in self.config:
            return self.config[__name]
        raise ArithmeticError(f"'ConfigManager' has no config '{__name}'")
