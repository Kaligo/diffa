from diffa.config import (
    SourceConfig,
    DiffaConfig,
    ConfigManager,
)

TEST_POSTGRESQL_CONN_STRING = "postgresql://postgres:Password1@localhost:5432/postgres"

DB_CONFIGS = {
    "postgresql": {
        "source": {
            "db_uri": TEST_POSTGRESQL_CONN_STRING,
            "db_name": "postgres",
            "db_schema": "public_source",
            "db_table": "test",
        },
        "target": {
            "db_uri": TEST_POSTGRESQL_CONN_STRING,
            "db_name": "postgres",
            "db_schema": "public_target",
            "db_table": "test",
        },
        "diffa": {
            "db_uri": TEST_POSTGRESQL_CONN_STRING,
            "db_name": "postgres_diffa",
        },
    }
}


def get_source_target_test_configs(db_scheme: str = "postgresql"):
    return {
        "source": SourceConfig(**DB_CONFIGS.get(db_scheme, {}).get("source")),
        "target": SourceConfig(**DB_CONFIGS.get(db_scheme, {}).get("target")),
    }


def get_diffa_test_config(db_scheme: str = "postgresql"):
    return DiffaConfig(**DB_CONFIGS.get(db_scheme, {}).get("diffa"))


def get_test_config_manager(db_scheme: str = "postgresql"):
    return ConfigManager(
        source_config=get_source_target_test_configs(db_scheme)["source"],
        target_config=get_source_target_test_configs(db_scheme)["target"],
        diffa_check_config=get_diffa_test_config(db_scheme),
        diffa_check_run_config=get_diffa_test_config(db_scheme),
    )
