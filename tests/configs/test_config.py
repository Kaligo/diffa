import os
import json
from unittest.mock import patch, mock_open

import pytest
from common import TEST_POSTGRESQL_CONN_STRING

from diffa.config import (
    ConfigManager,
    DBConfig,
    DIFFA_DB_SCHEMA,
    DIFFA_DB_TABLE,
    DIFFA_CHECK_RUNS_TABLE,
)


@pytest.mark.parametrize(
    "db_url, db_name, db_schema, db_table, expected_parsed_config",
    [
        # Case 1: Complete db_url and all parameters provided
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
        # Case 2: Partial db_url (missing database), with complete parameters
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
        # Case 3: Complete db_url, but missing database parameter (fallback to db_url value)
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
def test_db_config_parse_db_info(
    db_url, db_name, db_schema, db_table, expected_parsed_config
):

    config = DBConfig(
        db_url=db_url, db_name=db_name, db_schema=db_schema, db_table=db_table
    )
    parsed_db_config = config._parse_db_info()

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
        source_config=DBConfig(),
        target_config=DBConfig(),
        diffa_check_config=DBConfig(db_url=TEST_POSTGRESQL_CONN_STRING),
        diffa_check_run_config=DBConfig(db_url=TEST_POSTGRESQL_CONN_STRING),
    )
    assert config_manager.source.get_db_url() == TEST_POSTGRESQL_CONN_STRING
    assert config_manager.target.get_db_url() == TEST_POSTGRESQL_CONN_STRING
    assert config_manager.diffa_check.get_db_url() == TEST_POSTGRESQL_CONN_STRING
    assert config_manager.diffa_check_run.get_db_url() == TEST_POSTGRESQL_CONN_STRING


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
        source_config=DBConfig(),
        target_config=DBConfig(),
        diffa_check_config=DBConfig(),
        diffa_check_run_config=DBConfig(),
    ).configure(
        source_db_url=TEST_POSTGRESQL_CONN_STRING,
        source_schema="test_schema",
        source_table="test_table",
        target_db_url=TEST_POSTGRESQL_CONN_STRING,
        target_schema="test_schema",
        target_table="test_table",
    )
    assert config_manager.source == DBConfig(
        db_url=TEST_POSTGRESQL_CONN_STRING,
        db_schema="test_schema",
        db_table="test_table",
    )
    assert config_manager.target == DBConfig(
        db_url=TEST_POSTGRESQL_CONN_STRING,
        db_schema="test_schema",
        db_table="test_table",
    )
    assert config_manager.diffa_check == DBConfig(
        db_url=TEST_POSTGRESQL_CONN_STRING,
        db_schema=DIFFA_DB_SCHEMA,
        db_table=DIFFA_DB_TABLE,
    )
    assert config_manager.diffa_check_run == DBConfig(
        db_url=TEST_POSTGRESQL_CONN_STRING,
        db_schema=DIFFA_DB_SCHEMA,
        db_table=DIFFA_CHECK_RUNS_TABLE,
    )
