import logging
from typing import Iterable

class Logger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(sh)

    def info(self, message: str, *args, **kwargss):
        self.logger.info(message, *args, **kwargss)

    def error(self, message: str, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)

class DiffaException(Exception):
    """Base class for all Diffa exceptions."""

class InvalidDiffException(DiffaException):
    """Raised when an invalid diff is detected between source and target."""

class RunningCheckRunsException(DiffaException):
    """Raised when there are other running check runs."""

    def __init__(self, run_ids: Iterable[str], message: str = None):
        self.run_ids = run_ids
        self.message = f"{message} Run IDs: {', '.join(self.run_ids)}"
        super().__init__(self.message)

    
    def get_running_run_ids(self):
        """Return the IDs of the running records"""

        return self.run_ids