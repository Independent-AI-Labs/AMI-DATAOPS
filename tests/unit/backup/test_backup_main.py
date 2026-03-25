"""Unit tests for backup main entry points."""

import os
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.create.main import main as backup_main
from ami.dataops.backup.restore.main import main as restore_main


@pytest.fixture(autouse=True)
def _isolate_project_root(monkeypatch):
    """Prevent get_project_root from finding the real project root."""

    def _raise_runtime_error():
        raise RuntimeError

    monkeypatch.setattr(
        "ami.dataops.backup.common.paths.get_project_root",
        _raise_runtime_error,
    )


class TestBackupMain:
    """Test backup main entry point."""

    def test_help_works_without_env(self, tmp_path):
        """Test that --help works even without .env file."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-backup", "--help"]):
            result = backup_main()
            assert result == 0

    def test_help_flag_short(self, tmp_path):
        """Test that -h works even without .env file."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-backup", "-h"]):
            result = backup_main()
            assert result == 0

    def test_missing_env_returns_error(self, tmp_path):
        """Test that missing .env file returns error code 1."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-backup"]):
            result = backup_main()
            assert result == 1


class TestRestoreMain:
    """Test restore main entry point."""

    def test_help_works_without_env(self, tmp_path):
        """Test that --help works even without .env file."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-restore", "--help"]):
            result = restore_main()
            assert result == 0

    def test_help_flag_short(self, tmp_path):
        """Test that -h works even without .env file."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-restore", "-h"]):
            result = restore_main()
            assert result == 0

    def test_missing_env_returns_error(self, tmp_path):
        """Test that missing .env file returns error code 1."""
        os.chdir(tmp_path)
        with patch("sys.argv", ["ami-restore", "--latest-local"]):
            result = restore_main()
            assert result == 1


class TestBackupMainInit:
    """Test successful initialization paths."""

    def _mock_create_deps(self):
        """Create mocks for all create main dependencies."""
        mocks = {}
        for name in [
            "BackupConfig.load",
            "AuthenticationManager",
            "BackupUploader",
            "BackupService",
            "BackupCLI",
            "asyncio.run",
        ]:
            mocks[name] = MagicMock()
        mocks["asyncio.run"].return_value = 0
        cli = MagicMock()
        cli.parse_arguments.return_value = MagicMock()
        mocks["BackupCLI"].return_value = cli
        return mocks

    def test_create_main_wires_dependencies(self, tmp_path):
        """Test create main wires DI correctly."""
        os.chdir(tmp_path)
        mocks = self._mock_create_deps()
        prefix = "ami.dataops.backup.create.main"
        patches = {k: patch(f"{prefix}.{k}", v) for k, v in mocks.items()}
        for p in patches.values():
            p.start()
        try:
            with patch("sys.argv", ["ami-backup"]):
                assert backup_main() == 0
        finally:
            for p in patches.values():
                p.stop()

    def test_restore_main_wires_dependencies(self, tmp_path):
        """Test restore main wires DI correctly."""
        os.chdir(tmp_path)
        prefix = "ami.dataops.backup.restore.main"
        names = [
            "BackupRestoreConfig.load",
            "AuthenticationManager",
            "DriveRestoreClient",
            "BackupRestoreService",
            "RevisionsClient",
            "RestoreCLI",
            "asyncio.run",
        ]
        mocks = {n: MagicMock() for n in names}
        mocks["asyncio.run"].return_value = 0
        cli = MagicMock()
        cli.parse_arguments.return_value = MagicMock()
        mocks["RestoreCLI"].return_value = cli
        patches = {k: patch(f"{prefix}.{k}", v) for k, v in mocks.items()}
        for p in patches.values():
            p.start()
        try:
            with patch("sys.argv", ["ami-restore", "--latest-local"]):
                assert restore_main() == 0
        finally:
            for p in patches.values():
                p.stop()
