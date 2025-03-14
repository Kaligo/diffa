from diffa.config import (
    SourceTargetConfig,
    DiffaDBConfig,
    ConfigManager,
)

TEST_POSTGRESQL_CONN_STRING = "postgresql://postgres:Password1@localhost:5432/postgres"

DB_CONFIGS = {
    "postgresql": {
        "source": {
            "db_info": TEST_POSTGRESQL_CONN_STRING,
            "database": "postgres",
            "schema": "public_source",
            "table": "test",
        },
        "target": {
            "db_info": TEST_POSTGRESQL_CONN_STRING,
            "database": "postgres",
            "schema": "public_target",
            "table": "test",
        },
        "diffa": {
            "db_info": TEST_POSTGRESQL_CONN_STRING,
            "database": "postgres_diffa",
        },
    }
}


def get_source_target_test_configs(db_scheme: str = "postgresql"):
    return {
        "source": SourceTargetConfig(**DB_CONFIGS.get(db_scheme, {}).get("source")),
        "target": SourceTargetConfig(**DB_CONFIGS.get(db_scheme, {}).get("target")),
    }


def get_diffa_test_config(db_scheme: str = "postgresql"):
    return DiffaDBConfig(**DB_CONFIGS.get(db_scheme, {}).get("diffa"))


def get_test_config_manager(db_scheme: str = "postgresql"):
    return ConfigManager(
        source_config=get_source_target_test_configs(db_scheme)["source"],
        target_config=get_source_target_test_configs(db_scheme)["target"],
        diffa_config=get_diffa_test_config(db_scheme),
    )
