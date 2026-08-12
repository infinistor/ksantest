"""Pytest fixtures for S3 tests."""

from __future__ import annotations

import pytest

from s3tests.config import resolve_config_path, set_config_path
from s3tests.test_base import S3TestBase


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--s3tests-ini",
        action="store",
        default=None,
        help="INI configuration file used by the S3 tests",
    )


def pytest_configure(config: pytest.Config) -> None:
    set_config_path(config.getoption("--s3tests-ini"))


@pytest.fixture(scope="session")
def s3_config_path() -> str:
    return resolve_config_path()


@pytest.fixture(autouse=True)
def cleanup_buckets(request):
    yield
    instance = getattr(request, "instance", None)
    if instance is not None and isinstance(instance, S3TestBase):
        test_name = request.node.name
        instance.clear(test_name)
