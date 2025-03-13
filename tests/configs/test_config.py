import os

import pytest

from diffa.config import (
    ConfigManager,
    SourceTargetConfig,
    DiffaDBConfig,
    DIFFA_DB_SCHEMA,
    DIFFA_DB_TABLE,
    DIFFA_CHECK_RUNS_TABLE,
)

@pytest.mark.parametrize(
    "db_info, database, schema, table, expected_parsed_config",
    [
        # Case 1: Complete db_info and all parameters provided
        (
            "postgresql://user:password@localhost:5432/mydb",
            "mydb2",
            "myschema",
            "users",
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb2",
                "user": "user",
                "password": "password",
                "schema": "myschema",
                "table": "users",
                "db_url": "postgresql://user:password@localhost:5432/mydb2"
            }
        ),

        # Case 2: Partial db_info (missing database), with complete parameters
        (
            "postgresql://user:password@localhost:5432",
            "mydb2",
            "myschema",
            "users",
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb2",
                "user": "user",
                "password": "password",
                "schema": "myschema",
                "table": "users",
                "db_url": "postgresql://user:password@localhost:5432/mydb2"
            }
        ),

        # Case 3: Complete db_info, but missing database parameter (fallback to db_info value)
        (
            "postgresql://user:password@localhost:5432/mydb",
            None,
            "myschema",
            "users",
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb",
                "user": "user",
                "password": "password",
                "schema": "myschema",
                "table": "users",
                "db_url": "postgresql://user:password@localhost:5432/mydb"
            }
        ),
    ]
)
def test_source_target_config_parse_db_info(db_info, database, schema, table, expected_parsed_config):
    
    config = SourceTargetConfig(db_info=db_info, database=database, schema=schema, table=table)
    parsed_db_config = config.parse_db_info()

    assert parsed_db_config == expected_parsed_config

@pytest.mark.parametrize(
    "db_info, database, table_key, expected_parsed_config",
    [
        # Case 1: Complete db_info, table_key for checks, and all parameters provided
        (
            "postgresql://user:password@localhost:5432/mydb",
            "mydb2",
            "checks",
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb2",
                "user": "user",
                "password": "password",
                "schema": DIFFA_DB_SCHEMA,
                "tables": {
                    "checks": DIFFA_DB_TABLE,
                    "check_runs": DIFFA_CHECK_RUNS_TABLE
                },
                "table": DIFFA_DB_TABLE,
                "db_url": "postgresql://user:password@localhost:5432/mydb2"
            }
        ),

        # Case 2: Partial db_info (missing database), table_key for check_runs, with complete parameters
        (
            "postgresql://user:password@localhost:5432",
            "mydb2",
            "check_runs",
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb2",
                "user": "user",
                "password": "password",
                "schema": DIFFA_DB_SCHEMA,
                "tables": {
                    "checks": DIFFA_DB_TABLE,
                    "check_runs": DIFFA_CHECK_RUNS_TABLE
                },
                "table": DIFFA_CHECK_RUNS_TABLE,
                "db_url": "postgresql://user:password@localhost:5432/mydb2"
            }
        ),

        # Case 3: Complete db_info, and table_key is None, but missing database parameter (fallback to db_info value)
        (
            "postgresql://user:password@localhost:5432/mydb",
            None,
            None,
            {
                "host": "localhost",
                "scheme": "postgresql",
                "port": 5432,
                "database": "mydb",
                "user": "user",
                "password": "password",
                "schema": DIFFA_DB_SCHEMA,
                "tables": {
                    "checks": DIFFA_DB_TABLE,
                    "check_runs": DIFFA_CHECK_RUNS_TABLE
                },
                "table": None,
                "db_url": "postgresql://user:password@localhost:5432/mydb"
            }
        )
    ]
)
def test_diffa_config_parse_db_info(db_info, database, table_key, expected_parsed_config):

    config = DiffaDBConfig(db_info=db_info, database=database)
    parsed_db_config = config.parse_db_info()
    parsed_db_config = config.get_db_config(table_key=table_key)

    assert parsed_db_config == expected_parsed_config

