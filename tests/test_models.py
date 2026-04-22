"""Parse + dataclass behavior for Dataset, DatasetStatus."""

import json
from dataclasses import FrozenInstanceError

import pytest

from cms_hospitals.pipeline import Dataset, DatasetStatus


@pytest.fixture
def first_hospitals_raw(sample_metastore_path):
    with sample_metastore_path.open() as f:
        records = json.load(f)
    return next(r for r in records if "Hospitals" in r.get("theme", []))


def test_dataset_from_dict_parses_identifier_and_modified(first_hospitals_raw):
    dataset = Dataset.from_dict(first_hospitals_raw)

    assert dataset.identifier == first_hospitals_raw["identifier"]
    assert dataset.modified == first_hospitals_raw["modified"]


def test_dataset_from_dict_pulls_download_url_from_first_distribution(
    first_hospitals_raw,
):
    dataset = Dataset.from_dict(first_hospitals_raw)

    first_dist = first_hospitals_raw["distribution"][0]
    assert dataset.download_url == first_dist["downloadURL"]
    assert dataset.media_type == first_dist["mediaType"]


def test_dataset_is_frozen_and_hashable(first_hospitals_raw):
    dataset = Dataset.from_dict(first_hospitals_raw)

    with pytest.raises(FrozenInstanceError):
        dataset.identifier = "changed"  # type: ignore[misc]
    assert len({dataset}) == 1


def test_dataset_status_is_string_comparable():
    assert DatasetStatus.NEW == "new"
    assert DatasetStatus.MODIFIED.value == "modified"
    assert DatasetStatus("unchanged") is DatasetStatus.UNCHANGED
