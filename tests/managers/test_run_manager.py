from unittest.mock import MagicMock, patch

import pytest

from diffa.db.diffa_check_run import DiffaCheckRunService
from diffa.utils import RunningCheckRunsException
from diffa.managers.run_manager import RunManager
from common import get_test_config_manager


@pytest.fixture
def config_manager() -> MagicMock:
    return get_test_config_manager()


@pytest.fixture
def mock_diffa_check_run_service() -> MagicMock:
    return MagicMock(spec=DiffaCheckRunService)


@pytest.fixture
def run_manager(config_manager, mock_diffa_check_run_service):

    run_manager = RunManager(config_manager)
    run_manager.diffa_check_run_service = mock_diffa_check_run_service
    return run_manager


def test_start_run_no_running_checks(run_manager):

    run_manager.diffa_check_run_service.getting_running_check_runs.return_value = []

    run_manager.start_run()

    run_manager.diffa_check_run_service.create_new_check_run.assert_called_once_with(
        run_manager.current_run
    )


def test_start_run_with_running_checks(run_manager):

    run_manager.diffa_check_run_service.getting_running_check_runs.return_value = [
        "mock_running_1",
        "mock_running_2",
    ]

    with pytest.raises(RunningCheckRunsException):
        run_manager.start_run()


def test_complete_run(run_manager):

    run_manager.complete_run()

    run_manager.diffa_check_run_service.update_check_run_as_status.assert_called_once_with(
        run_manager.current_run, "COMPLETED"
    )


def test_fail_run(run_manager):

    run_manager.fail_run()

    run_manager.diffa_check_run_service.update_check_run_as_status.assert_called_once_with(
        run_manager.current_run, "FAILED"
    )


def test_handle_sigterm(run_manager):

    with patch("sys.exit") as mock_exit:
        run_manager.handle_sigterm(signal_number=15, frame=None)

        run_manager.diffa_check_run_service.update_check_run_as_status.assert_called_once_with(
            run_manager.current_run, "FAILED"
        )
        mock_exit.assert_called_once_with(1)


def test_handle_sigint(run_manager):
    with patch("sys.exit") as mock_exit:
        run_manager.handle_sigint(signal_number=2, frame=None)

        run_manager.diffa_check_run_service.update_check_run_as_status.assert_called_once_with(
            run_manager.current_run, "FAILED"
        )
        mock_exit.assert_called_once_with(1)
