import sys
import os
from datetime import datetime
import json
import click
from diffa.core.services import DiffaService
from diffa.core.db.config import ConfigManager
from diffa.core.db.databases import DatabaseManager

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--source-schema",
    type=str,
    help="Source table schema (default: public).",
)
@click.option(
    "--source-table",
    required=True,
    type=str,
    help="Source table name.",
)
@click.option(
    "--target-schema",
    type=str,
    help="Target table schema (default: public).",
)
@click.option(
    "--target-table",
    required=True,
    type=str,
    help="Target table name.",
)
@click.option(
    "--lookback-window",
    required=True,
    type=int,
    help="Lookback window in days.",
)
@click.option(
    "--execution-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Execution date with format YYYY-MM-DD.",
)
def data_diff(
    *,
    source_schema: str = None,
    source_table: str,
    target_schema: str = None,
    target_table: str,
    lookback_window: int,
    execution_date: datetime,
):
    database_manager = DatabaseManager(
        ConfigManager(
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_table,
        )
    )
    diff_service = DiffaService(database_manager)
    return diff_service.compare_tables(execution_date, lookback_window)
    # database_manager.get_history_db().create_diff_table()


@cli.command()
def configure():
    source_db_info = click.prompt("Enter the source db connection string")
    target_db_info = click.prompt("Enter the target db connection string")
    diffa_db_info = click.prompt("Enter the diffa db connection string")

    diffa_config_dir = os.path.expanduser("~/.diffa")
    if not os.path.exists(diffa_config_dir):
        os.makedirs(diffa_config_dir)
    config_file = os.path.join(diffa_config_dir, "config.json")
    with open(config_file, "w", encoding="utf-8") as f:
        config = {
            "source_uri": source_db_info,
            "target_uri": target_db_info,
            "diffa_db_uri": diffa_db_info,
        }
        json.dump(config, f)


if __name__ == "__main__":
    cli()
