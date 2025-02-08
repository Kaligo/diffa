import os
import dsnparse
import json
from diffa.utils import Logger

CONFIG_DIR = os.path.expanduser("~/.diffa")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")

logger = Logger(__name__)


# TODO: Don't mainain a config manager like this. The class should be decoupled
class ConfigManager:
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
                    "source_db_info": None,
                    "source_schema": None,
                    "source_table": None,
                },
                "target": {
                    "target_db_info": None,
                    "target_schema": None,
                    "target_table": None,
                },
                "diffa": {
                    "diffa_db_info": None,
                    "diffa_schema": "public",
                    "diffa_table": "diffa_history",
                },
            }
            self.__load_config()

    def config(
        self,
        *,
        source_db_info: str = None,
        source_schema: str = "public",
        source_table: str,
        target_db_info: str = None,
        target_schema: str = "public",
        target_table: str,
        diffa_db_info: str = None,
    ):
        self.config["source"].update(
            {
                "source_db_info": source_db_info
                or self.config["source"].get("source_db_info"),
                "source_schema": source_schema
                or self.config["source"].get("source_schema"),
                "source_table": source_table
                or self.config["source"].get("source_table"),
            }
        )
        self.config["target"].update(
            {
                "target_db_info": target_db_info
                or self.config["target"].get("target_db_info"),
                "target_schema": target_schema
                or self.config["target"].get("target_schema"),
                "target_table": target_table
                or self.config["target"].get("target_table"),
            }
        )
        self.config["diffa"].update(
            {
                "diffa_db_info": diffa_db_info
                or self.config["diffa"].get("diffa_db_info"),
            }
        )

    def __load_config(self):
        uri_config = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                uri_config = json.load(f)
        self.config["source"].update(
            {
                "source_db_info": os.getenv("DIFFA__SOURCE_URI")
                or uri_config.get("source_uri"),
            }
        )
        self.config["target"].update(
            {
                "target_db_info": os.getenv("DIFFA__TARGET_URI")
                or uri_config.get("target_uri"),
            }
        )

    def __parse_db_info(self, db_key: str):
        try:
            db_info = self.config[db_key]["db_info"]
            db_schema = self.config[db_key]["schema"]
            db_table = self.config[db_key]["table"]
            dns = dsnparse.parse(db_info)
        except TypeError as e:
            logger.error(f"Seems like you have not set the db info for {db_key}")
            raise e
        return {
            "host": dns.host,
            "port": dns.port,
            "database": dns.database,
            "user": dns.username,
            "password": dns.password,
            "schema": db_schema,
            "table": db_table,
            "db_url": db_info,
        }

    def get_source_db_info(self):
        return self.__parse_db_info(db_key="source")

    def get_target_db_info(self):
        return self.__parse_db_info(db_key="target")

    def get_diffa_db_info(self):
        return self.__parse_db_info(db_key="diffa")
