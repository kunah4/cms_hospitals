"""API client behavior: theme filtering, retry semantics."""

import json

import httpx
import pytest
import tenacity

from cms_hospitals import pipeline
from cms_hospitals.pipeline import _METASTORE_URL, fetch_datasets, make_client


@pytest.fixture(autouse=True)
def fast_retry(monkeypatch):
    # override tenacity's exponential wait so retry tests run instantly
    monkeypatch.setattr(pipeline._get_metastore.retry, "wait", tenacity.wait_fixed(0))


@pytest.fixture
def sample_records(sample_metastore_path):
    with sample_metastore_path.open() as f:
        return json.load(f)


def test_fetch_datasets_filters_by_theme(httpx_mock, sample_records):
    httpx_mock.add_response(url=_METASTORE_URL, json=sample_records)

    with make_client() as client:
        datasets = fetch_datasets(client)

    expected = [r for r in sample_records if "Hospitals" in r.get("theme", [])]
    assert len(datasets) == len(expected)
    assert {d.identifier for d in datasets} == {r["identifier"] for r in expected}


def test_fetch_datasets_retries_on_5xx_then_succeeds(httpx_mock, sample_records):
    httpx_mock.add_response(url=_METASTORE_URL, status_code=500)
    httpx_mock.add_response(url=_METASTORE_URL, status_code=500)
    httpx_mock.add_response(url=_METASTORE_URL, json=sample_records)

    with make_client() as client:
        datasets = fetch_datasets(client)

    assert len(datasets) == 3  # fixture has 3 Hospitals records
    assert len(httpx_mock.get_requests()) == 3


def test_fetch_datasets_does_not_retry_4xx(httpx_mock):
    httpx_mock.add_response(url=_METASTORE_URL, status_code=404)

    with make_client() as client, pytest.raises(httpx.HTTPStatusError):
        fetch_datasets(client)

    assert len(httpx_mock.get_requests()) == 1


def test_fetch_datasets_gives_up_after_three_5xx_retries(httpx_mock):
    for _ in range(3):
        httpx_mock.add_response(url=_METASTORE_URL, status_code=500)

    with make_client() as client, pytest.raises(httpx.HTTPStatusError):
        fetch_datasets(client)

    assert len(httpx_mock.get_requests()) == 3
