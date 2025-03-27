import sys
import os
import json

import click
from alembic import command
from alembic.config import Config

from diffa.managers.check_manager import CheckManager
from diffa.managers.run_manager import RunManager
from diffa.config import ConfigManager, ExitCode
from diffa.utils import RunningCheckRunsException, InvalidDiffException

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


@click.group()
def cli():
    pass


@cli.command()
@click.option("--source-db-uri", type=str, help="Source database info.")
@click.option("--target-db-uri", type=str, help="Target database info.")
@click.option("--diffa-db-uri", type=str, help="Diffa database info.")
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
def data_diff(
    *,
    source_db_uri: str = None,
    target_db_uri: str = None,
    diffa_db_uri: str = None,
    source_database: str = None,
    source_schema: str = "public",
    source_table: str,
    target_database: str = None,
    target_schema: str = "public",
    target_table: str,
):
    config_manager = ConfigManager().configure(
        source_database=source_database,
        source_schema=source_schema,
        source_table=source_table,
        target_database=target_database,
        target_schema=target_schema,
        target_table=target_table,
        source_db_uri=source_db_uri,
        target_db_uri=target_db_uri,
        diffa_db_uri=diffa_db_uri,
    )
    run_manager = RunManager(config_manager=config_manager)
    check_manager = CheckManager(config_manager=config_manager)
    try:
        run_manager.start_run()
        check_manager.data_diff()
        run_manager.complete_run()
    except RunningCheckRunsException:
        raise
    except InvalidDiffException:
        run_manager.complete_run()
        sys.exit(ExitCode.INVALID_DIFF.value)
    except Exception:
        run_manager.fail_run()
        raise


@cli.command()
def configure():
    config_manager = ConfigManager()

    source_uri = click.prompt(
        "Enter the source db connection string",
        default=config_manager.source.get_db_uri(),
    )
    target_uri = click.prompt(
        "Enter the target db connection string",
        default=config_manager.target.get_db_uri(),
    )
    diffa_uri = click.prompt(
        "Enter the diffa db connection string",
        default=config_manager.diffa_check.get_db_uri(),
    )

    ConfigManager.save_config(
        source_uri=source_uri, target_uri=target_uri, diffa_uri=diffa_uri
    )


@cli.command()
def migrate():
    alembic_cfg = Config(os.path.join(SCRIPT_DIR, "migrations", "alembic.ini"))
    command.upgrade(alembic_cfg, "head")
    click.echo("Database migration completed successfully.")


if __name__ == "__main__":
    cli()
