import os
import json
from unittest.mock import patch, mock_open

import pytest
from common import TEST_POSTGRESQL_CONN_STRING

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
                "db_url": "postgresql://user:password@localhost:5432/mydb2",
            },
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
                "db_url": "postgresql://user:password@localhost:5432/mydb2",
            },
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
                "db_url": "postgresql://user:password@localhost:5432/mydb",
            },
        ),
    ],
)
def test_source_target_config_parse_db_info(
    db_info, database, schema, table, expected_parsed_config
):

    config = SourceTargetConfig(
        db_info=db_info, database=database, schema=schema, table=table
    )
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
                    "check_runs": DIFFA_CHECK_RUNS_TABLE,
                },
                "table": DIFFA_DB_TABLE,
                "db_url": "postgresql://user:password@localhost:5432/mydb2",
            },
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
                    "check_runs": DIFFA_CHECK_RUNS_TABLE,
                },
                "table": DIFFA_CHECK_RUNS_TABLE,
                "db_url": "postgresql://user:password@localhost:5432/mydb2",
            },
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
                    "check_runs": DIFFA_CHECK_RUNS_TABLE,
                },
                "table": None,
                "db_url": "postgresql://user:password@localhost:5432/mydb",
            },
        ),
    ],
)
def test_diffa_config_parse_db_info(
    db_info, database, table_key, expected_parsed_config
):

    config = DiffaDBConfig(db_info=db_info, database=database)
    parsed_db_config = config.parse_db_info()
    parsed_db_config = config.get_db_config(table_key=table_key)

    assert parsed_db_config == expected_parsed_config


@patch.dict(os.environ, {"DIFFA__TARGET_URI": TEST_POSTGRESQL_CONN_STRING})
@patch("os.path.exists", return_value=True)
@patch("os.makedirs")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=json.dumps(
        {
            "source_uri": TEST_POSTGRESQL_CONN_STRING,
            "target_uri": None,
            "diffa_uri": None,
        }
    ),
)
def test_config_manager_load_config(mock_open_file, mock_mkdirs, mock_path_exists):
    config_manager = ConfigManager(
        source_config=SourceTargetConfig(),
        target_config=SourceTargetConfig(),
        diffa_config=DiffaDBConfig(db_info=TEST_POSTGRESQL_CONN_STRING),
    )
    assert config_manager.get_db_info("source") == TEST_POSTGRESQL_CONN_STRING
    assert config_manager.get_db_info("target") == TEST_POSTGRESQL_CONN_STRING
    assert config_manager.get_db_info("diffa") == TEST_POSTGRESQL_CONN_STRING


@patch.dict(os.environ, {"DIFFA__TARGET_URI": "test_target_uri"})
@patch("os.path.exists", return_value=True)
@patch("os.makedirs")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data=json.dumps(
        {
            "source_uri": "test_source_uri",
            "target_uri": None,
            "diffa_uri": TEST_POSTGRESQL_CONN_STRING,
        }
    ),
)
def test_config_manager_configure(mock_open_file, mock_mkdirs, mock_path_exists):
    # When initializing this as ConfigManager(), the params explicitly passed are not in the scope of the patch. Causing the issue.
    config_manager = ConfigManager(
        source_config=SourceTargetConfig(),
        target_config=SourceTargetConfig(),
        diffa_config=DiffaDBConfig(),
    )

    config_manager.configure(
        source_db_info=TEST_POSTGRESQL_CONN_STRING,
        source_schema="test_schema",
        source_table="test_table",
        target_db_info=TEST_POSTGRESQL_CONN_STRING,
        target_schema="test_schema",
        target_table="test_table",
    )
    assert config_manager.get_config("source") == SourceTargetConfig(
        db_info=TEST_POSTGRESQL_CONN_STRING, schema="test_schema", table="test_table"
    )
    assert config_manager.get_config("target") == SourceTargetConfig(
        db_info=TEST_POSTGRESQL_CONN_STRING, schema="test_schema", table="test_table"
    )
    assert config_manager.get_config("diffa") == DiffaDBConfig(
        db_info=TEST_POSTGRESQL_CONN_STRING,
    )
