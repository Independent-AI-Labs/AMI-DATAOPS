"""Render the intake Ansible Jinja2 templates and assert their structure."""

from __future__ import annotations

from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

_MINIMAL_PORT = 9180
_OVERRIDE_PORT = 9200
_DEFAULT_FILE_MB = 1
_OVERRIDE_FILE_MB = 50
_OVERRIDE_BUNDLE_MB = 250
_OVERRIDE_FILE_COUNT = 500
_OVERRIDE_CONCURRENCY = 8


def _render(template_dir: Path, template: str, context: dict) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    return env.get_template(template).render(**context)


class TestIntakeConfigRender:
    def test_minimal_config_produces_valid_yaml(
        self, serve_templates_dir: Path
    ) -> None:
        rendered = _render(
            serve_templates_dir,
            "ami-intake-config.yml.j2",
            {
                "intake_config": {
                    "intake_port": _MINIMAL_PORT,
                    "intake_root": "/var/lib/ami-intake",
                    "allowed_senders": ["alpha", "bravo"],
                },
            },
        )
        parsed = yaml.safe_load(rendered)
        assert parsed["intake_port"] == _MINIMAL_PORT
        assert parsed["intake_root"] == "/var/lib/ami-intake"
        assert parsed["allowed_senders"] == ["alpha", "bravo"]
        assert parsed["persist"] is False
        assert parsed["max_file_mb"] == _DEFAULT_FILE_MB

    def test_overrides_pass_through(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            "ami-intake-config.yml.j2",
            {
                "intake_config": {
                    "intake_port": _OVERRIDE_PORT,
                    "intake_root": "/srv/intake",
                    "allowed_senders": ["gamma"],
                    "persist": True,
                    "max_file_mb": _OVERRIDE_FILE_MB,
                    "max_bundle_mb": _OVERRIDE_BUNDLE_MB,
                    "max_files_per_bundle": _OVERRIDE_FILE_COUNT,
                    "global_concurrency": _OVERRIDE_CONCURRENCY,
                },
            },
        )
        parsed = yaml.safe_load(rendered)
        assert parsed["persist"] is True
        assert parsed["max_file_mb"] == _OVERRIDE_FILE_MB
        assert parsed["max_bundle_mb"] == _OVERRIDE_BUNDLE_MB
        assert parsed["max_files_per_bundle"] == _OVERRIDE_FILE_COUNT
        assert parsed["global_concurrency"] == _OVERRIDE_CONCURRENCY


class TestIntakeServiceRender:
    def test_unit_has_expected_sections(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            "ami-intake.service.j2",
            {
                "ami_root": "/ami/root",
                "python_bin": "/ami/root/.boot-linux/bin/python",
                "intake_config_path": "/opt/ami/config/intake-config.yml",
            },
        )
        assert "Description=AMI Intake daemon" in rendered
        assert "Restart=always" in rendered
        assert "RestartSec=5" in rendered
        assert "WantedBy=default.target" in rendered
        assert "serve --config /opt/ami/config/intake-config.yml" in rendered

    def test_execstart_references_main_py(self, serve_templates_dir: Path) -> None:
        rendered = _render(
            serve_templates_dir,
            "ami-intake.service.j2",
            {
                "ami_root": "/ami/root",
                "python_bin": "/ami/root/.boot-linux/bin/python",
                "intake_config_path": "/x/y/config.yml",
            },
        )
        assert "/ami/root/projects/AMI-DATAOPS/ami/dataops/intake/main.py" in rendered
