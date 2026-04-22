"""Pure-domain normalization: snake_case header + CSV rewrite."""

import csv
import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cms_hospitals.pipeline import _dedupe_headers, normalize_header, rewrite_csv

VALID_SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Hospital Name", "hospital_name"),
        (
            "Patients' rating of the facility linear mean score",
            "patients_rating_of_the_facility_linear_mean_score",
        ),
        ("CMS Certification Number (CCN)", "cms_certification_number_ccn"),
        ("  Leading/Trailing Spaces  ", "leading_trailing_spaces"),
        ("__Already__Snake__", "already_snake"),
        ("% Measure", "measure"),
        ("Address1", "address1"),
        ("", ""),
    ],
)
def test_normalize_header_handles_common_cms_patterns(raw, expected):
    assert normalize_header(raw) == expected


def test_normalize_is_cached():
    normalize_header.cache_clear()
    normalize_header("Unique Header Value")
    hits_before = normalize_header.cache_info().hits

    normalize_header("Unique Header Value")
    hits_after = normalize_header.cache_info().hits

    assert hits_after - hits_before == 1


def test_dedupe_headers_appends_counter_suffix():
    assert _dedupe_headers(["a", "b", "a", "a"]) == ["a", "b", "a_2", "a_3"]


def test_rewrite_csv_snake_cases_headers_and_preserves_rows(tmp_path):
    src = tmp_path / "in.csv"
    dst = tmp_path / "out.csv"
    src.write_text(
        "Hospital Name,CMS Certification Number (CCN),Address1\n"
        "Acme Hospital,12345,123 Main St\n"
        "Beta Clinic,67890,456 Oak Ave\n"
    )

    row_count = rewrite_csv(src, dst)

    assert row_count == 2
    with dst.open() as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == [
            "hospital_name",
            "cms_certification_number_ccn",
            "address1",
        ]
        rows = list(reader)
    assert rows[0]["hospital_name"] == "Acme Hospital"
    assert rows[1]["address1"] == "456 Oak Ave"


def test_rewrite_csv_deduplicates_collided_headers(tmp_path):
    src = tmp_path / "in.csv"
    dst = tmp_path / "out.csv"
    src.write_text("Hospital Name,hospital name\nAcme,Legacy\n")

    rewrite_csv(src, dst)

    with dst.open() as f:
        header_line = f.readline().strip()
    assert header_line == "hospital_name,hospital_name_2"


def test_rewrite_csv_strips_utf8_bom(tmp_path):
    src = tmp_path / "in.csv"
    dst = tmp_path / "out.csv"
    src.write_text("\ufeffHospital Name\nAcme\n", encoding="utf-8")

    rewrite_csv(src, dst)

    with dst.open() as f:
        header_line = f.readline().strip()
    assert header_line == "hospital_name"


@given(st.text(min_size=1, max_size=200))
def test_normalize_always_produces_valid_snake_case(raw):
    result = normalize_header(raw)
    if result:
        assert VALID_SNAKE.match(result), f"invalid snake_case: {result!r} from {raw!r}"
