import sys
import os
from datetime import datetime
import json

import click
from alembic import command
from alembic.config import Config

from diffa.services import DiffaService
from diffa.config import ConfigManager, CONFIG_FILE

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
sys.path.append(os.path.dirname(SCRIPT_DIR))


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--source-database",
    type=str,
    help="Source database name.",
)
@click.option(
    "--source-schema",
    type=str,
    help="Source table schema (default: public).",
    default="public",
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
    help="Target database name.",
)
@click.option(
    "--target-schema",
    type=str,
    help="Target table schema (default: public).",
    default="public",
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
    source_database: str = None,
    source_schema: str = "public",
    source_table: str,
    target_database: str = None,
    target_schema: str = "public",
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
    is_not_diff = diff_service.compare_tables(execution_date, lookback_window)
    if is_not_diff:
        click.echo("No difference found.")
        sys.exit(0)
    else:
        click.echo("Difference found.")
        sys.exit(4)  # This is for Airflow to recognize the failure due to diff


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


@cli.command()
def migrate():
    alembic_cfg = Config(os.path.join(BASE_DIR, "alembic.ini"))
    command.upgrade(alembic_cfg, "head")
    click.echo("Database migration completed successfully.")

if __name__ == "__main__":
    cli()
