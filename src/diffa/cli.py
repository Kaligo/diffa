import sys
import os
from datetime import datetime
import json

import click
from alembic import command
from alembic.config import Config

from diffa.services import DiffaService
from diffa.config import ConfigManager, CONFIG_FILE, ExitCode

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
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
    "--start-datetime",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="Start datetime with format YYYY-MM-DD HH:MM:SS.",
)
@click.option(
    "--end-datetime",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="End datetime with format YYYY-MM-DD HH:MM:SS.",
)
def data_diff(
    *,
    source_database: str = None,
    source_schema: str = "public",
    source_table: str,
    target_database: str = None,
    target_schema: str = "public",
    target_table: str,
    start_datetime: datetime,
    end_datetime: datetime,
    
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
    is_not_diff = diff_service.compare_tables(start_datetime, end_datetime)
    if is_not_diff:
        click.echo("No difference found.")
        sys.exit(ExitCode.SUCCESS.value)
    else:
        # This is for Airflow to recognize the failure due to diff
        click.echo("Data is mismatched between Source and DW")
        sys.exit(ExitCode.DIFF.value)


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
    sys.exit(ExitCode.SUCCESS.value)


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
    "--start-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="Start date to check with format YYYY-MM-DD HH:MM:SS.",
)
@click.option(
    "--end-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="End date to check with format YYYY-MM-DD HH:MM:SS.",
)
def verify_diff_status(
    *,
    source_database: str = None,
    source_schema: str = "public",
    source_table: str,
    target_database: str = None,
    target_schema: str = "public",
    target_table: str,
    start_date: datetime,
    end_date: datetime,
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
    has_valid = diff_service.verify_diff_status(start_date, end_date)
    click.echo(json.dumps({"is_diff": not has_valid}), err=False)

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
def find_unreconciled_interval(
    *,
    source_database: str = None,
    source_schema: str = "public",
    source_table: str,
    target_database: str = None,
    target_schema: str = "public",
    target_table: str,
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
    oldest_unreconciled_interval = diff_service.get_oldest_pending_reconciliation()
    click.echo(json.dumps(oldest_unreconciled_interval), err=False)

@cli.command()
def migrate():
    alembic_cfg = Config(os.path.join(SCRIPT_DIR, "migrations", "alembic.ini"))
    command.upgrade(alembic_cfg, "head")
    click.echo("Database migration completed successfully.")
    sys.exit(ExitCode.SUCCESS.value)


if __name__ == "__main__":
    cli()
