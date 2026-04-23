"""CLI surface: click wiring, option validation, help text."""

from click.testing import CliRunner

from cms_hospitals.pipeline import cli_main


def test_workers_over_cap_rejected():
    result = CliRunner().invoke(cli_main, ["--workers", "100"])

    assert result.exit_code != 0
    assert "16" in result.output  # error mentions the MAX_WORKERS cap


def test_help_option():
    result = CliRunner().invoke(cli_main, ["--help"])

    assert result.exit_code == 0
    assert "Hospitals" in result.output
