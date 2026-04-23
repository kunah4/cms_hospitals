"""End-to-end integration tests against a mocked API.

Covers Day-N scenarios (MODIFIED on second run, FAILED retry, dry-run
idempotency) that can't be exercised live without waiting for CMS upstream
updates. Each test runs `run_pipeline` against an `httpx_mock`-seeded queue
of responses that simulate the desired timeline.
"""

import sqlite3

import pytest
import tenacity

from cms_hospitals import pipeline
from cms_hospitals.pipeline import _METASTORE_URL, Config, run_pipeline

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    # Keep any tenacity-wrapped call instant in integration tests.
    monkeypatch.setattr(pipeline._get_metastore.retry, "wait", tenacity.wait_fixed(0))


CSV_BYTES = b"Hospital Name,Rating\nAcme,5\nBeta,3\n"


def _hospital(identifier: str, modified: str, url: str) -> dict:
    return {
        "identifier": identifier,
        "title": f"Hospital {identifier}",
        "modified": modified,
        "theme": ["Hospitals"],
        "distribution": [{"downloadURL": url, "mediaType": "text/csv"}],
    }


def _non_hospital() -> dict:
    return {
        "identifier": "d1",
        "title": "Dialysis Facility",
        "modified": "2026-01-01",
        "theme": ["Dialysis facilities"],
        "distribution": [
            {"downloadURL": "https://example.test/d.csv", "mediaType": "text/csv"}
        ],
    }


def _make_config(tmp_path, *, dry_run: bool = False) -> Config:
    return Config(
        output_dir=tmp_path / "output",
        state_db=tmp_path / "state.db",
        staging_dir=tmp_path / "staging",
        workers=2,
        timeout=30.0,
        theme="Hospitals",
        log_level="INFO",
        dry_run=dry_run,
        full_refresh=False,
        limit=None,
    )


def _latest_run_tally(db_path) -> tuple:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT datasets_new, datasets_modified, datasets_unchanged, datasets_failed "
        "FROM pipeline_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return row


URLS = [
    "https://example.test/h1.csv",
    "https://example.test/h2.csv",
    "https://example.test/h3.csv",
]


def _day1_records() -> list[dict]:
    return [
        _hospital("h1", "2026-01-01", URLS[0]),
        _hospital("h2", "2026-01-02", URLS[1]),
        _hospital("h3", "2026-01-03", URLS[2]),
        _non_hospital(),  # proves theme filter excludes this
    ]


def test_happy_path_all_new_downloads_and_records(httpx_mock, tmp_path):
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    for url in URLS:
        httpx_mock.add_response(url=url, content=CSV_BYTES)

    config = _make_config(tmp_path)
    exit_code = run_pipeline(config)

    assert exit_code == 0
    assert _latest_run_tally(config.state_db) == (3, 0, 0, 0)
    written_csvs = list(config.output_dir.rglob("*.csv"))
    assert len(written_csvs) == 3


def test_day_2_unchanged_when_no_modifications(httpx_mock, tmp_path):
    # Day 1: full run
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    for url in URLS:
        httpx_mock.add_response(url=url, content=CSV_BYTES)
    # Day 2: metastore only — no CSV downloads expected
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())

    config = _make_config(tmp_path)
    run_pipeline(config)  # Day 1
    run_pipeline(config)  # Day 2

    # Most recent pipeline_runs row reflects Day 2.
    assert _latest_run_tally(config.state_db) == (0, 0, 3, 0)


def test_day_2_modified_triggers_redownload(httpx_mock, tmp_path):
    # Day 1
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    for url in URLS:
        httpx_mock.add_response(url=url, content=CSV_BYTES)

    # Day 2: h1's modified date changed — only h1 should re-download
    day2_records = [
        _hospital("h1", "2026-02-15", URLS[0]),  # MODIFIED
        _hospital("h2", "2026-01-02", URLS[1]),  # UNCHANGED
        _hospital("h3", "2026-01-03", URLS[2]),  # UNCHANGED
        _non_hospital(),
    ]
    httpx_mock.add_response(url=_METASTORE_URL, json=day2_records)
    httpx_mock.add_response(url=URLS[0], content=CSV_BYTES)

    config = _make_config(tmp_path)
    run_pipeline(config)
    run_pipeline(config)

    assert _latest_run_tally(config.state_db) == (0, 1, 2, 0)


def test_day_2_previously_failed_dataset_retries(httpx_mock, tmp_path):
    # Day 1: h1 succeeds, h2 fails (500, no worker retries), h3 succeeds
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    httpx_mock.add_response(url=URLS[0], content=CSV_BYTES)
    httpx_mock.add_response(url=URLS[1], status_code=500)
    httpx_mock.add_response(url=URLS[2], content=CSV_BYTES)
    # Day 2: h2 now succeeds; h1/h3 are UNCHANGED (no CSV mocks needed)
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    httpx_mock.add_response(url=URLS[1], content=CSV_BYTES)

    config = _make_config(tmp_path)
    run_pipeline(config)
    # Day 1 tally: h1 NEW, h3 NEW, h2 FAILED
    assert _latest_run_tally(config.state_db) == (2, 0, 0, 1)

    run_pipeline(config)
    # Day 2: h2 is NEW (get_known_modified excludes FAILED rows, so h2 looks
    # unknown → decide returns NEW); h1 and h3 are UNCHANGED.
    assert _latest_run_tally(config.state_db) == (1, 0, 2, 0)


def test_dry_run_leaves_dataset_rows_unchanged(httpx_mock, tmp_path):
    # Day 1: full run
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())
    for url in URLS:
        httpx_mock.add_response(url=url, content=CSV_BYTES)
    # Day 2 dry-run: metastore only
    httpx_mock.add_response(url=_METASTORE_URL, json=_day1_records())

    config = _make_config(tmp_path)
    run_pipeline(config)

    before = _snapshot_dataset_rows(config.state_db)

    dry_config = _make_config(tmp_path, dry_run=True)
    run_pipeline(dry_config)

    after = _snapshot_dataset_rows(dry_config.state_db)
    assert before == after


def _snapshot_dataset_rows(db_path) -> list[tuple]:
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT identifier, source_modified_at, last_status, last_run_id, row_count "
        "FROM dataset_runs ORDER BY identifier"
    ).fetchall()
    conn.close()
    return rows
