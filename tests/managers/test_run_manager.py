import pytest
from unittest.mock import MagicMock, Mock, patch

from diffa.db.database_handler import DiffaCheckRunHandler
from diffa.config import ConfigManager
from diffa.utils import RunningCheckRunsException
from diffa.managers.run_manager import RunManager


@pytest.fixture
def mock_config_manager() -> MagicMock:
    mock_cm = MagicMock(spec=ConfigManager)
    mock_cm.get_database.return_value = "mock_database"
    mock_cm.get_schema.return_value = "mock_schema"
    mock_cm.get_table.return_value = "mock_table"
    return mock_cm


@pytest.fixture
def mock_diffa_check_run_handler() -> MagicMock:
    return MagicMock(spec=DiffaCheckRunHandler)


@pytest.fixture
def run_manager(mock_config_manager, mock_diffa_check_run_handler):

    run_manager = RunManager(mock_config_manager)
    run_manager.check_run_handler = mock_diffa_check_run_handler
    return run_manager


def test_start_run_no_running_checks(run_manager):

    run_manager.check_run_handler.checking_running_check_runs.return_value = []

    run_manager.start_run()

    run_manager.check_run_handler.create_new_check_run.assert_called_once_with(
        run_manager.current_run
    )


def test_start_run_with_running_checks(run_manager):

    run_manager.check_run_handler.checking_running_check_runs.return_value = [
        "mock_running_1",
        "mock_running_2",
    ]

    with pytest.raises(RunningCheckRunsException):
        run_manager.start_run()


def test_complete_run(run_manager):

    run_manager.complete_run()

    run_manager.check_run_handler.update_check_run_as_status.assert_called_once_with(
        run_manager.current_run, "COMPLETED"
    )


def test_fail_run(run_manager):

    run_manager.fail_run()

    run_manager.check_run_handler.update_check_run_as_status.assert_called_once_with(
        run_manager.current_run, "FAILED"
    )


def test_handle_sigterm(run_manager):

    with patch("sys.exit") as mock_exit:
        run_manager.handle_sigterm(signal_number=15, frame=None)

        run_manager.check_run_handler.update_check_run_as_status.assert_called_once_with(
            run_manager.current_run, "FAILED"
        )
        mock_exit.assert_called_once_with(1)


def test_handle_sigint(run_manager):
    with patch("sys.exit") as mock_exit:
        run_manager.handle_sigint(signal_number=2, frame=None)

        run_manager.check_run_handler.update_check_run_as_status.assert_called_once_with(
            run_manager.current_run, "FAILED"
        )
        mock_exit.assert_called_once_with(1)
