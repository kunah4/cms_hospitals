"""CMS Hospitals incremental ingestion pipeline.

Single-module implementation. See README for design decisions and the
modularization plan that triggers future splits.
"""

from __future__ import annotations

import contextlib
import csv
import functools
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx
import tenacity

# === Models ================================================================


class DatasetStatus(StrEnum):
    NEW = "new"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Dataset:
    identifier: str
    title: str
    modified: str
    download_url: str
    media_type: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Dataset:
        dist = raw["distribution"][0]  # verified: 73/73 Hospitals have exactly 1
        return cls(
            identifier=raw["identifier"],
            title=raw["title"],
            modified=raw["modified"],
            download_url=dist["downloadURL"],
            media_type=dist["mediaType"],
        )


@dataclass(slots=True)
class DownloadOutcome:
    """What a worker returns. Deliberately does NOT include DatasetStatus —
    status is decided by the main thread by combining the classify() intent
    with the worker's success/failure. Keeps the worker honest."""

    identifier: str
    output_path: Path | None
    row_count: int | None
    error: str | None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass(frozen=True, slots=True)
class PendingWork:
    """A dataset the main thread intends to process, paired with the status
    it should carry if the download succeeds. Self-documenting alternative to
    list[tuple[Dataset, DatasetStatus]]."""

    dataset: Dataset
    intended_status: DatasetStatus


# === Header Normalization (pure) ============================================

_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")


@functools.cache
def normalize_header(raw: str) -> str:
    lowered = raw.strip().lower()
    collapsed = _NON_ALPHANUMERIC.sub("_", lowered)
    return collapsed.strip("_")


def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for h in headers:
        if h not in seen:
            seen[h] = 1
            result.append(h)
        else:
            seen[h] += 1
            result.append(f"{h}_{seen[h]}")
    return result


def rewrite_csv(src: Path, dst: Path) -> int:
    """Rewrite CSV with snake_case headers. Returns row count."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with contextlib.ExitStack() as stack:
        fin = stack.enter_context(src.open("r", newline="", encoding="utf-8-sig"))
        fout = stack.enter_context(dst.open("w", newline="", encoding="utf-8"))
        reader = csv.DictReader(fin)
        originals = reader.fieldnames or []
        normalized = _dedupe_headers([normalize_header(h) for h in originals])
        header_map = dict(zip(originals, normalized, strict=True))
        writer = csv.DictWriter(fout, fieldnames=normalized)
        writer.writeheader()
        for row in reader:
            writer.writerow({header_map[k]: v for k, v in row.items()})
            row_count += 1
    return row_count


# === Classify (pure) ========================================================


def decide(dataset: Dataset, known_modified: str | None) -> DatasetStatus:
    if known_modified is None:
        return DatasetStatus.NEW
    if known_modified != dataset.modified:
        return DatasetStatus.MODIFIED
    return DatasetStatus.UNCHANGED


# === State (SQLite) =========================================================

_SCHEMA = """
CREATE TABLE IF NOT EXISTS dataset_runs (
    identifier          TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    download_url        TEXT NOT NULL,
    source_modified_at  TEXT NOT NULL,
    downloaded_at       TEXT NOT NULL,
    output_path         TEXT NOT NULL,
    row_count           INTEGER,
    last_run_id         TEXT NOT NULL,
    last_status         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              TEXT PRIMARY KEY,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    datasets_discovered INTEGER,
    datasets_new        INTEGER,
    datasets_modified   INTEGER,
    datasets_unchanged  INTEGER,
    datasets_failed     INTEGER,
    status              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dataset_runs_last_run
    ON dataset_runs(last_run_id);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None puts sqlite3 in autocommit mode — each UPSERT is
    # atomic on its own. Single-writer orchestrator means no need for
    # explicit BEGIN/COMMIT.
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)


def get_known_modified(conn: sqlite3.Connection) -> dict[str, str]:
    """Return last-known-good modified date per dataset.

    Filters to successful runs only. A dataset whose last attempt FAILED is
    treated as unknown so the next run retries it rather than skipping it as
    UNCHANGED. Without this filter, a failed UPSERT (which still records the
    CMS-reported modified date) would cause the classifier to think the
    dataset is up to date and skip it forever.
    """
    cur = conn.execute(
        "SELECT identifier, source_modified_at FROM dataset_runs "
        "WHERE last_status != 'failed'"
    )
    return dict(cur.fetchall())


def upsert_dataset(
    conn: sqlite3.Connection,
    dataset: Dataset,
    outcome: DownloadOutcome,
    status: DatasetStatus,
    run_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO dataset_runs
            (identifier, title, download_url, source_modified_at,
             downloaded_at, output_path, row_count, last_run_id, last_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(identifier) DO UPDATE SET
            title=excluded.title,
            download_url=excluded.download_url,
            source_modified_at=excluded.source_modified_at,
            downloaded_at=excluded.downloaded_at,
            output_path=excluded.output_path,
            row_count=excluded.row_count,
            last_run_id=excluded.last_run_id,
            last_status=excluded.last_status
        """,
        (
            dataset.identifier,
            dataset.title,
            dataset.download_url,
            dataset.modified,
            datetime.now(UTC).isoformat(),
            str(outcome.output_path) if outcome.output_path else "",
            outcome.row_count,
            run_id,
            status.value,
        ),
    )


def start_run(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute(
        "INSERT INTO pipeline_runs (run_id, started_at, status) "
        "VALUES (?, ?, 'running')",
        (run_id, datetime.now(UTC).isoformat()),
    )


def finish_run(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    discovered: int,
    new: int,
    modified: int,
    unchanged: int,
    failed: int,
    status: str,
) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs SET
            finished_at=?, datasets_discovered=?, datasets_new=?,
            datasets_modified=?, datasets_unchanged=?, datasets_failed=?,
            status=?
        WHERE run_id=?
        """,
        (
            datetime.now(UTC).isoformat(),
            discovered,
            new,
            modified,
            unchanged,
            failed,
            status,
            run_id,
        ),
    )


# === API Client (httpx + tenacity) ==========================================


_METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)


def make_client(timeout: float = 30.0) -> httpx.Client:
    return httpx.Client(timeout=timeout, follow_redirects=True)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return False


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    retry=tenacity.retry_if_exception(_is_transient),
    reraise=True,
)
def _get_metastore(client: httpx.Client, url: str) -> list[dict[str, Any]]:
    response = client.get(url)
    response.raise_for_status()
    return response.json()


def fetch_datasets(
    client: httpx.Client,
    theme: str = "Hospitals",
    url: str = _METASTORE_URL,
) -> list[Dataset]:
    raw_records = _get_metastore(client, url)
    return [Dataset.from_dict(r) for r in raw_records if theme in r.get("theme", [])]


# === Worker (download + transform) ==========================================


def download_and_transform(
    dataset: Dataset,
    client: httpx.Client,
    staging_dir: Path,
    output_dir: Path,
) -> DownloadOutcome:
    staging_path = staging_dir / f"{dataset.identifier}.csv.download"
    basename = dataset.download_url.rsplit("/", 1)[-1]
    final_path = output_dir / f"{dataset.identifier}__{basename}"

    try:
        # mkdir INSIDE the try: permission or disk-full failures become a
        # FAILED DownloadOutcome, not an unhandled exception that crashes
        # the main thread.
        staging_dir.mkdir(parents=True, exist_ok=True)

        with client.stream("GET", dataset.download_url) as response:
            response.raise_for_status()
            with staging_path.open("wb") as fout:
                for chunk in response.iter_bytes(chunk_size=64 * 1024):
                    fout.write(chunk)

        row_count = rewrite_csv(staging_path, final_path)
        staging_path.unlink(missing_ok=True)

        return DownloadOutcome(
            identifier=dataset.identifier,
            output_path=final_path,
            row_count=row_count,
            error=None,
        )
    except Exception as exc:
        staging_path.unlink(missing_ok=True)
        return DownloadOutcome(
            identifier=dataset.identifier,
            output_path=None,
            row_count=None,
            error=str(exc),
        )
