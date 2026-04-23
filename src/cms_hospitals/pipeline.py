"""CMS Hospitals incremental ingestion pipeline.

Single-module implementation. See README for design decisions and the
modularization plan that triggers future splits.
"""

from __future__ import annotations

import contextlib
import csv
import functools
import json
import logging
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import click
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
        try:
            dist = raw["distribution"][0]  # verified: 73/73 Hospitals have exactly 1
            return cls(
                identifier=raw["identifier"],
                title=raw["title"],
                modified=raw["modified"],
                download_url=dist["downloadURL"],
                media_type=dist["mediaType"],
            )
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(f"malformed CMS record: {exc!r}") from exc


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


# === Config ================================================================

# Deployment defaults — edit if your environment needs different paths.
DEFAULT_OUTPUT_ROOT = Path("output")
DEFAULT_STATE_DB = Path(".cms_hospitals_state.db")
DEFAULT_STAGING_DIR = Path(".staging")
THEME = "Hospitals"
MAX_WORKERS = 16

_DEFAULTS: dict[str, Any] = {
    "output_dir": str(DEFAULT_OUTPUT_ROOT),
    "state_db": str(DEFAULT_STATE_DB),
    "staging_dir": str(DEFAULT_STAGING_DIR),
    "workers": 4,
    "timeout": 30.0,
    "theme": THEME,
    "log_level": "INFO",
}

# Keys that persist across runs (written to JSON sidecar).
_PERSISTENT_KEYS = frozenset(_DEFAULTS)

# Per-invocation only (NEVER persisted).
_PER_RUN_KEYS = frozenset({"dry_run", "full_refresh", "limit"})


@dataclass(slots=True)
class Config:
    output_dir: Path
    state_db: Path
    staging_dir: Path
    workers: int
    timeout: float
    theme: str
    log_level: str
    dry_run: bool
    full_refresh: bool
    limit: int | None


def _sidecar_path(state_db: Path) -> Path:
    """Sidecar sits next to the state DB: <stem>.config.json"""
    return state_db.with_suffix(".config.json")


def _load_sidecar(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}  # corrupt sidecar — fall back to defaults


def _write_sidecar(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # default=str lets json.dumps serialize Path values from CLI overrides
    path.write_text(
        json.dumps(data, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def load_config(cli_overrides: dict[str, Any]) -> Config:
    """Resolve configuration from CLI > sidecar > defaults.

    Persistent CLI overrides (e.g. ``--workers``) are saved to a JSON sidecar
    so the next invocation reuses them without re-specifying. Per-run flags
    (``--dry-run``, ``--full-refresh``, ``--limit``) are never persisted.
    """
    cli = {k: v for k, v in cli_overrides.items() if v is not None}

    # Resolve state_db first since it anchors the sidecar location.
    state_db = Path(cli.get("state_db", _DEFAULTS["state_db"]))
    sidecar = _sidecar_path(state_db)
    persisted = _load_sidecar(sidecar)

    persistent_effective = {
        **_DEFAULTS,
        **persisted,
        **{k: v for k, v in cli.items() if k in _PERSISTENT_KEYS},
    }

    workers = int(persistent_effective["workers"])
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError(
            f"workers={workers} out of range [1, {MAX_WORKERS}]. "
            "Raise MAX_WORKERS deliberately if you need more."
        )

    # Persist any new CLI overrides that changed persisted values.
    cli_persistent_updates = {k: v for k, v in cli.items() if k in _PERSISTENT_KEYS}
    if cli_persistent_updates and cli_persistent_updates != {
        k: persisted.get(k) for k in cli_persistent_updates
    }:
        _write_sidecar(sidecar, persistent_effective)

    return Config(
        output_dir=Path(persistent_effective["output_dir"]),
        state_db=Path(persistent_effective["state_db"]),
        staging_dir=Path(persistent_effective["staging_dir"]),
        workers=workers,
        timeout=float(persistent_effective["timeout"]),
        theme=str(persistent_effective["theme"]),
        log_level=str(persistent_effective["log_level"]),
        dry_run=bool(cli.get("dry_run", False)),
        full_refresh=bool(cli.get("full_refresh", False)),
        limit=int(cli["limit"]) if cli.get("limit") is not None else None,
    )


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
    if not isinstance(raw_records, list):
        raise ValueError(
            f"metastore returned {type(raw_records).__name__}, expected list"
        )
    logger = logging.getLogger("cms_hospitals")
    datasets: list[Dataset] = []
    for raw in raw_records:
        if not isinstance(raw, dict):
            logger.warning(
                "skipping non-dict record",
                extra={"record_type": type(raw).__name__},
            )
            continue
        if theme not in raw.get("theme", []):
            continue
        try:
            datasets.append(Dataset.from_dict(raw))
        except ValueError as exc:
            logger.warning(
                "skipping malformed record",
                extra={
                    "identifier": raw.get("identifier", "?"),
                    "error": str(exc),
                },
            )
    return datasets


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


# === Orchestrator ===========================================================


def _acquire_run_lock(lock_path: Path) -> None:
    """Create a PID lock; abort if another live process holds it.

    Stale locks (whose PID is no longer running) are silently taken over.
    """
    if lock_path.exists():
        try:
            existing_pid = int(lock_path.read_text().strip())
            os.kill(existing_pid, 0)  # signal 0 = existence check only
        except ProcessLookupError:
            pass  # stale lock; previous process died
        except (ValueError, OSError):
            pass  # corrupt/unreadable; treat as stale
        else:
            raise RuntimeError(
                f"Another run is in progress (PID {existing_pid}). "
                f"If this is stale, remove {lock_path} manually."
            )
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(str(os.getpid()))


def _release_run_lock(lock_path: Path) -> None:
    lock_path.unlink(missing_ok=True)


def run_pipeline(config: Config) -> int:
    lock_path = config.state_db.with_suffix(".lock")
    _acquire_run_lock(lock_path)
    try:
        return _run_pipeline_locked(config)
    finally:
        _release_run_lock(lock_path)


def _run_pipeline_locked(config: Config) -> int:
    # Microsecond precision prevents run_id collisions when two invocations
    # fire within the same second (rapid manual reruns, CI validation loops,
    # back-to-back test runs). Plan's per-second format only held under the
    # daily-cron assumption; microseconds make it robust without cost.
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    logger = logging.getLogger("cms_hospitals")
    logger.info("run starting", extra={"run_id": run_id})

    run_output_dir = config.output_dir / datetime.now(UTC).strftime("%Y-%m-%d")

    with contextlib.ExitStack() as stack:
        conn = stack.enter_context(contextlib.closing(connect(config.state_db)))
        apply_schema(conn)
        start_run(conn, run_id)

        client = stack.enter_context(make_client(timeout=config.timeout))

        datasets = fetch_datasets(client, theme=config.theme)
        if config.limit:
            datasets = datasets[: config.limit]

        known = {} if config.full_refresh else get_known_modified(conn)

        to_process: list[PendingWork] = []
        counts = {s: 0 for s in DatasetStatus}
        for ds in datasets:
            intended_status = decide(ds, known.get(ds.identifier))
            if intended_status is DatasetStatus.UNCHANGED:
                counts[intended_status] += 1
                logger.debug("unchanged", extra={"identifier": ds.identifier})
            else:
                to_process.append(
                    PendingWork(dataset=ds, intended_status=intended_status)
                )

        if config.dry_run:
            logger.info(
                "dry-run summary",
                extra={
                    "discovered": len(datasets),
                    "would_download": len(to_process),
                    "unchanged": counts[DatasetStatus.UNCHANGED],
                },
            )
            finish_run(
                conn,
                run_id,
                discovered=len(datasets),
                new=0,
                modified=0,
                unchanged=counts[DatasetStatus.UNCHANGED],
                failed=0,
                status="success",
            )
            return 0

        pool = stack.enter_context(
            ThreadPoolExecutor(
                max_workers=config.workers,
                thread_name_prefix="cms_hospitals",
            )
        )
        futures = {
            pool.submit(
                download_and_transform,
                work.dataset,
                client,
                config.staging_dir,
                run_output_dir,
            ): work
            for work in to_process
        }

        for future in as_completed(futures):
            work = futures[future]
            # Belt-and-suspenders: even if a worker raises before its own
            # try/except (e.g. OOM, interpreter bug), one dataset can't take
            # down the whole run. Log and move on.
            try:
                outcome = future.result()
            except Exception as exc:
                logger.exception(
                    "worker crashed",
                    extra={"identifier": work.dataset.identifier},
                )
                outcome = DownloadOutcome(
                    identifier=work.dataset.identifier,
                    output_path=None,
                    row_count=None,
                    error=f"worker crashed: {exc!r}",
                )
            final_status = (
                work.intended_status if outcome.succeeded else DatasetStatus.FAILED
            )
            counts[final_status] += 1
            upsert_dataset(conn, work.dataset, outcome, final_status, run_id)

        status = "success" if counts[DatasetStatus.FAILED] == 0 else "partial"
        finish_run(
            conn,
            run_id,
            discovered=len(datasets),
            new=counts[DatasetStatus.NEW],
            modified=counts[DatasetStatus.MODIFIED],
            unchanged=counts[DatasetStatus.UNCHANGED],
            failed=counts[DatasetStatus.FAILED],
            status=status,
        )

    logger.info(
        "run complete",
        extra={
            "run_id": run_id,
            "discovered": len(datasets),
            "new": counts[DatasetStatus.NEW],
            "modified": counts[DatasetStatus.MODIFIED],
            "unchanged": counts[DatasetStatus.UNCHANGED],
            "failed": counts[DatasetStatus.FAILED],
        },
    )
    return 0 if counts[DatasetStatus.FAILED] == 0 else 1


# === Logging ================================================================

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s %(contextual)s"

_STANDARD_LOGRECORD_ATTRS = set(
    logging.LogRecord("x", logging.INFO, "x", 0, "x", None, None).__dict__.keys()
) | {"message", "asctime"}


class _ContextFilter(logging.Filter):
    """Ensure every record has a 'contextual' field for the formatter."""

    def filter(self, record: logging.LogRecord) -> bool:
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _STANDARD_LOGRECORD_ATTRS and not k.startswith("_")
        }
        record.contextual = json.dumps(extras, default=str) if extras else ""
        return True


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    handler.addFilter(_ContextFilter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


# === CLI ====================================================================


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="Where cleaned CSVs are written.",
)
@click.option(
    "--state-db",
    type=click.Path(path_type=Path),
    help="Path to SQLite state database.",
)
@click.option(
    "--staging-dir",
    type=click.Path(path_type=Path),
    help="Staging directory for in-progress downloads.",
)
@click.option(
    "--workers",
    type=click.IntRange(1, MAX_WORKERS),
    help=f"Max worker threads (1-{MAX_WORKERS}). Default 4 for 4GB-RAM safety.",
)
@click.option("--timeout", type=float, help="HTTP timeout in seconds.")
@click.option("--theme", help="Filter theme (default: Hospitals).")
@click.option(
    "--dry-run",
    is_flag=True,
    default=None,
    help="Discover and classify but do not download.",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    default=None,
    help="Ignore state; treat all datasets as NEW.",
)
@click.option("--limit", type=int, help="Process at most N datasets (for smoke tests).")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Logging verbosity.",
)
def cli_main(**kwargs: Any) -> None:
    """Incremental daily ingestion of CMS Hospitals datasets."""
    cli_overrides = {k: v for k, v in kwargs.items() if v is not None}
    config = load_config(cli_overrides)
    setup_logging(config.log_level)
    exit_code = run_pipeline(config)
    raise SystemExit(exit_code)
