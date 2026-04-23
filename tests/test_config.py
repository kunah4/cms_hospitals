"""Config resolution: CLI > sidecar > defaults; persistence rules."""

import json

import pytest

from cms_hospitals.pipeline import load_config


def test_defaults_used_when_no_sidecar_and_no_cli(tmp_path):
    state_db = tmp_path / "s.db"

    config = load_config({"state_db": state_db})

    assert config.state_db == state_db
    assert config.workers == 4
    assert config.timeout == 30.0
    assert config.theme == "Hospitals"
    assert config.log_level == "INFO"
    assert config.dry_run is False
    assert config.full_refresh is False
    assert config.limit is None


def test_cli_overrides_defaults(tmp_path):
    state_db = tmp_path / "s.db"

    config = load_config({"state_db": state_db, "workers": 8, "log_level": "DEBUG"})

    assert config.workers == 8
    assert config.log_level == "DEBUG"


def test_cli_override_persisted_to_sidecar(tmp_path):
    state_db = tmp_path / "s.db"
    load_config({"state_db": state_db, "workers": 8})

    sidecar = state_db.with_suffix(".config.json")
    assert sidecar.exists()
    data = json.loads(sidecar.read_text())
    assert data["workers"] == 8
    assert data["log_level"] == "INFO"  # default carried through


def test_sidecar_read_and_applied_on_second_load(tmp_path):
    state_db = tmp_path / "s.db"
    load_config({"state_db": state_db, "workers": 8})

    config = load_config({"state_db": state_db})

    assert config.workers == 8  # picked up from sidecar, not default


def test_per_run_flags_never_persisted(tmp_path):
    state_db = tmp_path / "s.db"
    load_config(
        {
            "state_db": state_db,
            "workers": 8,  # persistent — forces sidecar write
            "dry_run": True,
            "full_refresh": True,
            "limit": 5,
        }
    )

    sidecar = state_db.with_suffix(".config.json")
    data = json.loads(sidecar.read_text())
    assert data["workers"] == 8
    assert "dry_run" not in data
    assert "full_refresh" not in data
    assert "limit" not in data


def test_corrupt_sidecar_falls_back_to_defaults(tmp_path):
    state_db = tmp_path / "s.db"
    sidecar = state_db.with_suffix(".config.json")
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text("{{ not valid json")

    config = load_config({"state_db": state_db})

    assert config.workers == 4
    assert config.log_level == "INFO"


def test_workers_over_max_rejected_with_helpful_error(tmp_path):
    state_db = tmp_path / "s.db"

    with pytest.raises(ValueError, match="out of range"):
        load_config({"state_db": state_db, "workers": 100})
