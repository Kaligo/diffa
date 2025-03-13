from datetime import datetime

import pytest

from diffa.managers.check_manager import CheckManager
from diffa.config import ConfigManager
from diffa.db.data_models import CountCheck, MergedCountCheck


@pytest.fixture
def check_manager():
    return CheckManager(ConfigManager())


@pytest.mark.parametrize(
    "source_counts, target_counts, expected_merged_counts",
    [
        # case 1: Checking dates are in both source and target
        (
            [
                CountCheck(
                    cnt=100,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ],
            [
                CountCheck(
                    cnt=200,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ],
            [
                MergedCountCheck(
                    source_count=100,
                    target_count=200,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date()
                )
            ]
        ),
        # case 2: Checking dates are in source only
        (
            [
                CountCheck(
                    cnt=100,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ],
            [],
            [
                MergedCountCheck(
                    source_count=100,
                    target_count=0,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ]
        ),
        # case 3: Checking dates are in target only
        (
            [],
            [
                CountCheck(
                    cnt=200,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ],
            [
                MergedCountCheck(
                    source_count=0,
                    target_count=200,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                )
            ]
        ),
        # case 4: Checking dates are in neither source nor target
        (
            [],
            [],
            []
        ),
    ],
)
def test__merge_count_check(
    check_manager, source_counts, target_counts, expected_merged_counts
):
    merged_counts = check_manager._merge_count_checks(source_counts, target_counts)
    assert expected_merged_counts == merged_counts


@pytest.mark.parametrize(
    "merged_count_checks, expected_result",
    [
        # case 1: All merged count checks are valid
        [
            [
                MergedCountCheck(
                    source_count=100,
                    target_count=100,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                ),
                MergedCountCheck(
                    source_count=100,
                    target_count=150,
                    check_date=datetime.strptime("2024-01-02", "%Y-%m-%d").date(),
                ),
            ],
            False,
        ],
        # case 2: All merged count checks are invalid
        [
            [
                MergedCountCheck(
                    source_count=150,
                    target_count=100,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                ),
            ],
            True,
        ],
        # case 3: Mixed valid and invalid merged count checks
        [
            [
                MergedCountCheck(
                    source_count=100,
                    target_count=100,
                    check_date=datetime.strptime("2024-01-01", "%Y-%m-%d").date(),
                ),
                MergedCountCheck(
                    source_count=150,
                    target_count=100,
                    check_date=datetime.strptime("2024-01-02", "%Y-%m-%d").date(),
                ),
                MergedCountCheck(
                    source_count=100,
                    target_count=150,
                    check_date=datetime.strptime("2024-01-03", "%Y-%m-%d").date(),
                ),
            ],
            True,
        ],
    ],
)
def test__check_if_invalid_diff(check_manager, merged_count_checks, expected_result):
    is_invalid_diff = check_manager._check_if_invalid_diff(merged_count_checks)
    assert is_invalid_diff == expected_result
