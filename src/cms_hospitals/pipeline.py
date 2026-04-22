"""CMS Hospitals incremental ingestion pipeline.

Single-module implementation. See README for design decisions and the
modularization plan that triggers future splits.
"""

from __future__ import annotations

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
