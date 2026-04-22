"""End-to-end worker test: download_and_transform via httpx_mock."""

import csv

from cms_hospitals.pipeline import Dataset, download_and_transform, make_client


def _fake_dataset(url: str) -> Dataset:
    return Dataset(
        identifier="test-id",
        title="Test Dataset",
        modified="2026-01-01",
        download_url=url,
        media_type="text/csv",
    )


def test_download_and_transform_writes_snake_cased_csv(httpx_mock, tmp_path):
    url = "https://example.test/ASC_Facility.csv"
    httpx_mock.add_response(
        url=url,
        content=(
            b"Hospital Name,CMS Certification Number (CCN)\nAcme,12345\nBeta,67890\n"
        ),
    )
    dataset = _fake_dataset(url)
    staging = tmp_path / "staging"
    output = tmp_path / "output"

    with make_client() as client:
        outcome = download_and_transform(dataset, client, staging, output)

    assert outcome.succeeded
    assert outcome.row_count == 2
    assert outcome.output_path == output / "test-id__ASC_Facility.csv"
    assert outcome.output_path.exists()

    with outcome.output_path.open() as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == ["hospital_name", "cms_certification_number_ccn"]

    # staging cleaned up after success
    assert not (staging / "test-id.csv.download").exists()


def test_download_and_transform_returns_failed_outcome_on_http_error(
    httpx_mock, tmp_path
):
    url = "https://example.test/ASC_Facility.csv"
    httpx_mock.add_response(url=url, status_code=500)
    dataset = _fake_dataset(url)

    with make_client() as client:
        outcome = download_and_transform(
            dataset, client, tmp_path / "staging", tmp_path / "output"
        )

    assert not outcome.succeeded
    assert outcome.error is not None
    assert outcome.output_path is None
    assert outcome.row_count is None
    # staging cleaned up on failure too
    assert not (tmp_path / "staging" / "test-id.csv.download").exists()
