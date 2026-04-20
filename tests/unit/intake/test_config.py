"""Unit tests for ami.dataops.intake.config."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from ami.dataops.intake.config import (
    DEFAULT_GLOBAL_CONCURRENCY,
    IntakeConfig,
    load_intake_config,
)

_TEST_PORT = 9180
_DEFAULT_FILE_MB = 1
_DEFAULT_BUNDLE_MB = 500
_DEFAULT_FILE_COUNT = 1000


class TestIntakeConfig:
    def test_minimal_config_applies_defaults(self, tmp_path: Path) -> None:
        cfg = IntakeConfig.model_validate(
            {"intake_port": _TEST_PORT, "intake_root": str(tmp_path / "intake")}
        )
        assert cfg.intake_port == _TEST_PORT
        assert cfg.intake_root == (tmp_path / "intake").absolute()
        assert cfg.persist is False
        assert cfg.max_file_mb == _DEFAULT_FILE_MB
        assert cfg.max_bundle_mb == _DEFAULT_BUNDLE_MB
        assert cfg.max_files_per_bundle == _DEFAULT_FILE_COUNT
        assert cfg.global_concurrency == DEFAULT_GLOBAL_CONCURRENCY
        assert cfg.allowed_senders == []

    def test_bytes_properties_match_mib(self, tmp_path: Path) -> None:
        cfg = IntakeConfig.model_validate(
            {
                "intake_port": 9180,
                "intake_root": str(tmp_path),
                "max_file_mb": 7,
                "max_bundle_mb": 13,
            }
        )
        assert cfg.max_file_bytes == 7 * 1024 * 1024
        assert cfg.max_bundle_bytes == 13 * 1024 * 1024

    def test_duplicate_senders_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            IntakeConfig.model_validate(
                {
                    "intake_port": 9180,
                    "intake_root": str(tmp_path),
                    "allowed_senders": ["alpha", "alpha"],
                }
            )

    def test_port_range_enforced(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            IntakeConfig.model_validate(
                {"intake_port": 0, "intake_root": str(tmp_path)}
            )
        with pytest.raises(ValidationError):
            IntakeConfig.model_validate(
                {"intake_port": 70000, "intake_root": str(tmp_path)}
            )

    def test_unknown_field_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            IntakeConfig.model_validate(
                {
                    "intake_port": 9180,
                    "intake_root": str(tmp_path),
                    "future_option": "x",
                }
            )

    def test_positive_limits_enforced(self, tmp_path: Path) -> None:
        with pytest.raises(ValidationError):
            IntakeConfig.model_validate(
                {
                    "intake_port": 9180,
                    "intake_root": str(tmp_path),
                    "max_file_mb": 0,
                }
            )


class TestLoadIntakeConfig:
    def test_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_intake_config(tmp_path / "does-not-exist.yml")

    def test_non_mapping_yaml_rejected(self, tmp_path: Path) -> None:
        config_path = tmp_path / "c.yml"
        config_path.write_text("- a\n- b\n")
        with pytest.raises(TypeError, match="is not a YAML mapping"):
            load_intake_config(config_path)

    def test_round_trip_from_yaml(self, tmp_path: Path) -> None:
        config_path = tmp_path / "c.yml"
        config_path.write_text(
            f"intake_port: 9180\n"
            f"intake_root: {tmp_path}/intake\n"
            "persist: true\n"
            "allowed_senders:\n"
            "  - alpha\n"
            "  - bravo\n"
        )
        cfg = load_intake_config(config_path)
        assert cfg.persist is True
        assert cfg.allowed_senders == ["alpha", "bravo"]
