import os
import dsnparse
import json

DIFFA__HISTORY_SCHEMA = os.getenv("DIFFA__HISTORY_SCHEMA", "public")
DIFFA__HISTORY_TABLE = os.getenv("DIFFA__HISTORY_TABLE", "diffa_history")


#TODO: Don't mainain a config manager like this. The class should be decoupled
class ConfigManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        *,
        source_db_info: str = None,
        source_schema: str = "public",
        source_table: str,
        target_db_info: str = None,
        target_schema: str = "public",
        target_table: str,
        history_db_info: str = None,
    ):
        # Config file < Env < Args
        self.source_db_info, self.target_db_info, self.history_db_info = (
            None,
            None,
            None,
        )
        self.__get_db_info_from_config_file()
        self.__get_db_info_from_env()
        self.__get_db_info_from_config(source_db_info, target_db_info, history_db_info)

        self.source_schema = source_schema
        self.source_table = source_table
        self.target_schema = target_schema
        self.target_table = target_table
        self.history_schema = DIFFA__HISTORY_SCHEMA
        self.history_table = DIFFA__HISTORY_TABLE

    def __get_db_info_from_config(
        self, source_db_info, target_db_info, history_db_info
    ):
        self.source_db_info = source_db_info if source_db_info else self.source_db_info
        self.target_db_info = target_db_info if target_db_info else self.target_db_info
        self.history_db_info = (
            history_db_info if history_db_info else self.history_db_info
        )

    def __get_db_info_from_env(self):
        self.source_db_info = os.environ.get("DIFFA__SOURCE_URI", self.source_db_info)
        self.target_db_info = os.environ.get("DIFFA__TARGET_URI", self.target_db_info)
        self.history_db_info = os.environ.get(
            "DIFFA__HISTORY_URI", self.history_db_info
        )

    def __get_db_info_from_config_file(self):
        config_file = os.path.join(os.path.expanduser("~/.diffa"), "config.json")
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            self.source_db_info = config.get("source_uri", self.source_db_info)
            self.target_db_info = config.get("target_uri", self.target_db_info)
            self.history_db_info = config.get("diffa_db_uri", self.history_db_info)

    def __get_db_info(self, db_info: str, db_schema: str, db_table: str):
        dns = dsnparse.parse(db_info)
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
        return self.__get_db_info(
            self.source_db_info, self.source_schema, self.source_table
        )

    def get_target_db_info(self):
        return self.__get_db_info(
            self.target_db_info, self.target_schema, self.target_table
        )

    def get_history_db_info(self):
        return self.__get_db_info(
            self.history_db_info, self.history_schema, self.history_table
        )
