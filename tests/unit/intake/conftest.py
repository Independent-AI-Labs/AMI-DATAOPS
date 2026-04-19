"""Shared fixtures for intake unit tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def serve_templates_dir() -> Path:
    """Absolute path to `res/ansible/templates` (shared with the serve tests).

    Locates the DATAOPS project root by walking up from this file until
    `pyproject.toml` is found, so the fixture works regardless of cwd.
    """
    current = Path(__file__).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file() and (
            candidate / "res" / "ansible"
        ).is_dir():
            return candidate / "res" / "ansible" / "templates"
    msg = f"Could not locate DATAOPS project root from {current}"
    raise RuntimeError(msg)
