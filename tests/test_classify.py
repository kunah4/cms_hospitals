"""Classify: decide(dataset, known_modified) → DatasetStatus."""

from cms_hospitals.pipeline import Dataset, DatasetStatus, decide


def _dataset(identifier="test-id", modified="2026-01-01"):
    return Dataset(
        identifier=identifier,
        title="t",
        modified=modified,
        download_url="https://x/f.csv",
        media_type="text/csv",
    )


def test_decide_returns_new_when_identifier_is_unknown():
    dataset = _dataset()

    result = decide(dataset, known_modified=None)

    assert result is DatasetStatus.NEW


def test_decide_returns_modified_when_modified_date_changed():
    dataset = _dataset(modified="2026-02-01")

    result = decide(dataset, known_modified="2026-01-01")

    assert result is DatasetStatus.MODIFIED


def test_decide_returns_unchanged_when_modified_date_matches():
    dataset = _dataset(modified="2026-01-01")

    result = decide(dataset, known_modified="2026-01-01")

    assert result is DatasetStatus.UNCHANGED
