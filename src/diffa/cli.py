import sys
import os
from datetime import datetime
import json

import click

from diffa.services import DiffaService
from diffa.config import ConfigManager, CONFIG_FILE

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--source-database",
    type=str,
    required=True,
    help="Source database name.",
)
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
    "--target-database",
    type=str,
    required=True,
    help="Target database name.",
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
    source_database: str,
    source_schema: str = None,
    source_table: str,
    target_database: str,
    target_schema: str = None,
    target_table: str,
    lookback_window: int,
    execution_date: datetime,
):
    ConfigManager().configure(
        source_database=source_database,
        source_schema=source_schema,
        source_table=source_table,
        target_database=target_database,
        target_schema=target_schema,
        target_table=target_table,
    )
    diff_service = DiffaService()
    return diff_service.compare_tables(execution_date, lookback_window)


@cli.command()
def configure():
    config_manager = ConfigManager()
    config = {}

    config["source_uri"] = click.prompt(
        "Enter the source db connection string",
        default=config_manager.get_db_info("source"),
    )
    config["target_uri"] = click.prompt(
        "Enter the target db connection string",
        default=config_manager.get_db_info("target"),
    )
    config["diffa_uri"] = click.prompt(
        "Enter the diffa db connection string",
        default=config_manager.get_db_info("diffa"),
    )

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4)

    click.echo("Configuration saved to successfully.")


if __name__ == "__main__":
    cli()
