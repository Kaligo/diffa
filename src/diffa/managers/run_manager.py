import sys
import signal

from diffa.db.data_models import DiffaCheckRunSchema
from diffa.db.database_handler import DiffaCheckRunHandler
from diffa.config import ConfigManager
from diffa.utils import Logger, RunningCheckRunsException

logger = Logger(__name__)


class RunManager:

    def __init__(self, config_manager: ConfigManager):
        self.cm = config_manager
        self.check_run_handler = DiffaCheckRunHandler(self.cm)
        self.current_run = DiffaCheckRunSchema(
            source_database=self.cm.source.get_db_name(),
            source_schema=self.cm.source.get_db_schema(),
            source_table=self.cm.source.get_db_table(),
            target_database=self.cm.target.get_db_name(),
            target_schema=self.cm.target.get_db_schema(),
            target_table=self.cm.target.get_db_table(),
            status="RUNNING",
        )

    def start_run(self):

        running_runs = self.check_run_handler.checking_running_check_runs()
        if len(running_runs) > 0:
            raise RunningCheckRunsException(
                running_runs, "There are other RUNNING checks"
            )

        self.check_run_handler.create_new_check_run(self.current_run)
        logger.info(f"Created new check run with id: {self.current_run.run_id}")

        # Register signal handlers
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigint)

    def complete_run(self):
        self.check_run_handler.update_check_run_as_status(self.current_run, "COMPLETED")
        logger.info(f"Check run {self.current_run.run_id} marked as COMPLETED")

    def fail_run(self):
        self.check_run_handler.update_check_run_as_status(self.current_run, "FAILED")
        logger.info(f"Check run {self.current_run.run_id} marked as FAILED")

    def handle_sigterm(self, signal_number, frame):
        """Handle SIGTERM and clean up"""

        logger.warning("Received SIGTERM. Marking run as FAILED...")
        self.fail_run()
        sys.exit(1)

    def handle_sigint(self, signal_number, frame):
        """Handle SIGINT (Ctrl+C) and clean up"""

        logger.warning("Received SIGINT. Marking run as FAILED...")
        self.fail_run()
        sys.exit(1)
