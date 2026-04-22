"""CMS Hospitals incremental ingestion pipeline.

Single-module implementation. See README for design decisions and the
modularization plan that triggers future splits.
"""

from __future__ import annotations

import contextlib
import csv
import functools
import re
from dataclasses import dataclass
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
