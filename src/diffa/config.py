import os
import json
from enum import Enum

import dsnparse

from diffa.utils import Logger

CONFIG_DIR = os.path.expanduser("~/.diffa")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
DIFFA_DB_SCHEMA = "diffa"
DIFFA_DB_TABLE = "diffa_history"

class ExitCode(Enum):
    SUCCESS = 0
    DIFF = 4 # Data mismatch detected
    ERROR = 1 # Unexpected error (e.g., network, infra issues)

logger = Logger(__name__)


class ConfigManager:
    """Singleton Pattern for ConfigManager to ensure that the config is loaded only once"""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
    ):
        if not hasattr(self, "config"):
            self.config = {
                "source": {
                    "db_info": None,
                    "database": None,
                    "schema": None,
                    "table": None,
                },
                "target": {
                    "db_info": None,
                    "database": None,
                    "schema": None,
                    "table": None,
                },
                "diffa": {
                    "db_info": None,
                    "database": None,
                    "schema": DIFFA_DB_SCHEMA,
                    "table": DIFFA_DB_TABLE,
                },
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
            {
                "db_info": source_db_info or self.config["source"].get("db_info"),
                "database": source_database or self.config["source"].get("database"),
                "schema": source_schema or self.config["source"].get("schema"),
                "table": source_table,
            }
        )
        self.config["target"].update(
            {
                "db_info": target_db_info or self.config["target"].get("db_info"),
                "database": target_database or self.config["target"].get("database"),
                "schema": target_schema or self.config["target"].get("schema"),
                "table": target_table,
            }
        )
        self.config["diffa"].update(
            {
                "db_info": diffa_db_info or self.config["diffa"].get("db_info"),
            }
        )

    def __load_config(self):
        uri_config = {}
        os.makedirs(CONFIG_DIR, exist_ok=True)
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                uri_config = json.load(f)

        self.config["source"].update(
            {
                "db_info": os.getenv("DIFFA__SOURCE_URI", uri_config.get("source_uri"))
            }
        )
        self.config["target"].update(
            {
                "db_info": os.getenv("DIFFA__TARGET_URI", uri_config.get("target_uri"))
            }
        )
        self.config["diffa"].update(
            {
                "db_info": os.getenv("DIFFA__DIFFA_DB_URI", uri_config.get("diffa_uri")),
            }
        )

    def __parse_db_config(self, db_key: str):
        try:
            dns = dsnparse.parse(self.config[db_key]["db_info"])
            db_database = self.config[db_key]["database"] or dns.database
            db_schema = self.config[db_key]["schema"]
            db_table = self.config[db_key]["table"]
        except TypeError:
            logger.error(f"There's something wrong with the db info for {db_key}", exc_info=True)
            raise
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

    def get_db_config(self, db_key: str):
        return self.__parse_db_config(db_key=db_key)

    def get_database(self, db_key: str):
        return self.__parse_db_config(db_key)["database"]

    def get_schema(self, db_key: str):
        return self.__parse_db_config(db_key)["schema"]

    def get_table(self, db_key: str):
        return self.__parse_db_config(db_key)["table"]

    def get_db_info(self, db_key: str):
        return self.config[db_key]["db_info"]
