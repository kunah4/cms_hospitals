"""SQLite state store: schema, UPSERT, get_known_modified filtering, run totals."""

from pathlib import Path

import pytest

from cms_hospitals.pipeline import (
    Dataset,
    DatasetStatus,
    DownloadOutcome,
    apply_schema,
    connect,
    finish_run,
    get_known_modified,
    start_run,
    upsert_dataset,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "test_state.db"
    c = connect(db)
    apply_schema(c)
    yield c
    c.close()


def _dataset(identifier="test-id", modified="2026-01-01"):
    return Dataset(
        identifier=identifier,
        title="title",
        modified=modified,
        download_url="https://x/f.csv",
        media_type="text/csv",
    )


def _success_outcome(identifier="test-id", row_count=100):
    return DownloadOutcome(
        identifier=identifier,
        output_path=Path("/tmp/out.csv"),
        row_count=row_count,
        error=None,
    )


def _failed_outcome(identifier="test-id"):
    return DownloadOutcome(
        identifier=identifier,
        output_path=None,
        row_count=None,
        error="boom",
    )


def test_apply_schema_is_idempotent(tmp_path):
    c = connect(tmp_path / "s.db")
    try:
        apply_schema(c)
        apply_schema(c)  # second call should not raise
    finally:
        c.close()


def test_get_known_modified_returns_empty_on_fresh_db(conn):
    assert get_known_modified(conn) == {}


def test_get_known_modified_excludes_failed_rows(conn):
    upsert_dataset(
        conn,
        _dataset(identifier="a", modified="2026-01-01"),
        _success_outcome(identifier="a"),
        DatasetStatus.NEW,
        run_id="run-1",
    )
    upsert_dataset(
        conn,
        _dataset(identifier="b", modified="2026-01-02"),
        _failed_outcome(identifier="b"),
        DatasetStatus.FAILED,
        run_id="run-1",
    )

    known = get_known_modified(conn)

    assert known == {"a": "2026-01-01"}


def test_get_known_modified_excludes_first_attempt_failures(conn):
    upsert_dataset(
        conn,
        _dataset(identifier="a", modified="2026-01-01"),
        _failed_outcome(identifier="a"),
        DatasetStatus.FAILED,
        run_id="run-1",
    )

    assert get_known_modified(conn) == {}


def test_upsert_dataset_inserts_then_updates_on_second_call(conn):
    upsert_dataset(
        conn,
        _dataset(identifier="a", modified="2026-01-01"),
        _success_outcome(identifier="a", row_count=100),
        DatasetStatus.NEW,
        run_id="run-1",
    )
    upsert_dataset(
        conn,
        _dataset(identifier="a", modified="2026-02-01"),
        _success_outcome(identifier="a", row_count=200),
        DatasetStatus.MODIFIED,
        run_id="run-2",
    )

    row = conn.execute(
        "SELECT source_modified_at, row_count, last_run_id, last_status "
        "FROM dataset_runs WHERE identifier = 'a'"
    ).fetchone()
    assert row == ("2026-02-01", 200, "run-2", "modified")


def test_finish_run_records_totals(conn):
    start_run(conn, "run-1")

    finish_run(
        conn,
        "run-1",
        discovered=73,
        new=10,
        modified=5,
        unchanged=55,
        failed=3,
        status="partial",
    )

    row = conn.execute(
        "SELECT datasets_discovered, datasets_new, datasets_modified, "
        "datasets_unchanged, datasets_failed, status "
        "FROM pipeline_runs WHERE run_id = 'run-1'"
    ).fetchone()
    assert row == (73, 10, 5, 55, 3, "partial")


def test_schema_survives_existing_data(conn):
    upsert_dataset(
        conn,
        _dataset(identifier="a", modified="2026-01-01"),
        _success_outcome(identifier="a"),
        DatasetStatus.NEW,
        run_id="run-1",
    )

    apply_schema(conn)  # re-apply: should be idempotent, preserve data

    assert get_known_modified(conn) == {"a": "2026-01-01"}
