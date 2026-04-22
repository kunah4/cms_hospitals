"""Shared pytest fixtures."""

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_metastore_path() -> Path:
    return FIXTURES / "sample_metastore.json"
