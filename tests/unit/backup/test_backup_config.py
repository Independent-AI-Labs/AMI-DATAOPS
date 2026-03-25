"""Unit tests for backup configuration."""

import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.backup_config import (
    BackupConfig,
    _check_and_refresh_adc_token,
    check_adc_credentials_valid,
    refresh_adc_credentials,
)
from ami.dataops.backup.backup_exceptions import BackupConfigError
from ami.dataops.backup.core.config import BackupRestoreConfig

EXPECTED_SUBPROCESS_CALL_COUNT = 2


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Clear GDRIVE env vars before each test."""
    for key in list(os.environ.keys()):
        if key.startswith("GDRIVE_"):
            monkeypatch.delenv(key, raising=False)


class TestBackupConfig:
    """Test backup configuration loading."""

    @pytest.fixture(autouse=True)
    def _isolate_project_root(self, monkeypatch):
        """Prevent get_project_root from finding the real project root."""

        def _raise_runtime_error():
            raise RuntimeError

        monkeypatch.setattr(
            "ami.dataops.backup.backup_config.get_project_root",
            _raise_runtime_error,
        )

    def test_load_missing_env_file_raises(self, tmp_path):
        """Test that missing .env file raises BackupConfigError."""
        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert ".env file not found" in str(exc_info.value)

    def test_load_with_env_file(self, tmp_path):
        """Test loading config from .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\n")

        config = BackupConfig.load(tmp_path)

        assert config.auth_method == "oauth"
        assert config.root_dir == tmp_path

    def test_load_invalid_auth_method_raises(self, tmp_path):
        """Test that invalid auth method raises BackupConfigError."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=invalid\n")

        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert "Invalid GDRIVE_AUTH_METHOD='invalid'" in str(exc_info.value)

    def test_load_impersonation_without_email_raises(self, tmp_path):
        """Test that impersonation without service account email raises."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=impersonation\n")

        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert "GDRIVE_SERVICE_ACCOUNT_EMAIL" in str(exc_info.value)

    def test_load_key_without_credentials_file_raises(self, tmp_path):
        """Test that key auth without credentials file raises."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=key\n")

        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert "GDRIVE_CREDENTIALS_FILE" in str(exc_info.value)

    def test_load_key_with_missing_credentials_file_raises(self, tmp_path):
        """Test that key auth with missing credentials file raises."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE=/nonexistent/path.json\n"
        )

        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert "Service account key file not found" in str(exc_info.value)

    def test_load_key_with_valid_credentials_file(self, tmp_path):
        """Test loading key auth with valid credentials file."""
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text("{}")

        env_file = tmp_path / ".env"
        env_file.write_text(
            f"GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE={creds_file}\n"
        )

        config = BackupConfig.load(tmp_path)

        assert config.auth_method == "key"
        assert config.credentials_file == str(creds_file)

    def test_load_folder_id(self, tmp_path):
        """Test loading folder ID from .env."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=oauth\nGDRIVE_BACKUP_FOLDER_ID=folder123\n"
        )

        config = BackupConfig.load(tmp_path)

        assert config.folder_id == "folder123"

    def test_valid_auth_methods(self):
        """Test that valid auth methods constant is correct."""
        assert BackupConfig.VALID_AUTH_METHODS == ("impersonation", "key", "oauth")

    def test_load_impersonation_with_valid_config(self, tmp_path, monkeypatch):
        """Test loading impersonation auth with valid config."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=impersonation\n"
            "GDRIVE_SERVICE_ACCOUNT_EMAIL=test@project.iam.gserviceaccount.com\n"
        )

        # Mock find_gcloud to return a valid path
        monkeypatch.setattr(
            "ami.dataops.backup.backup_config.find_gcloud",
            lambda: "/usr/bin/gcloud",
        )
        # Mock the ADC check
        monkeypatch.setattr(
            "ami.dataops.backup.backup_config.check_adc_credentials_valid",
            lambda: True,
        )

        config = BackupConfig.load(tmp_path)

        assert config.auth_method == "impersonation"
        assert config.service_account_email == "test@project.iam.gserviceaccount.com"
        assert config.gcloud_path == "/usr/bin/gcloud"

    def test_load_impersonation_without_gcloud_raises(self, tmp_path, monkeypatch):
        """Test that impersonation without gcloud CLI raises."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=impersonation\n"
            "GDRIVE_SERVICE_ACCOUNT_EMAIL=test@project.iam.gserviceaccount.com\n"
        )

        # Mock find_gcloud to return None
        monkeypatch.setattr(
            "ami.dataops.backup.backup_config.find_gcloud",
            lambda: None,
        )

        with pytest.raises(BackupConfigError) as exc_info:
            BackupConfig.load(tmp_path)

        assert "gcloud CLI is required" in str(exc_info.value)

    def test_load_key_with_relative_path(self, tmp_path):
        """Test loading key auth with relative credentials path."""
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("{}")

        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE=creds.json\n"
        )

        config = BackupConfig.load(tmp_path)

        assert config.auth_method == "key"
        assert config.credentials_file == str(creds_file)

    def test_load_oauth_config(self, tmp_path):
        """Test loading oauth config logs appropriate messages."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\n")

        config = BackupConfig.load(tmp_path)

        assert config.auth_method == "oauth"
        # oauth doesn't require additional config
        assert config.credentials_file is None
        assert config.service_account_email is None


class TestCheckAdcCredentialsValid:
    """Tests for check_adc_credentials_valid function."""

    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_when_no_gcloud(self, mock_find_gcloud):
        """Test returns False when gcloud not found."""
        mock_find_gcloud.return_value = None

        result = check_adc_credentials_valid()

        assert result is False

    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_when_no_adc_file(self, mock_find_gcloud, mock_adc_path):
        """Test returns False when ADC file doesn't exist."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = False

        result = check_adc_credentials_valid()

        assert result is False

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_true_when_token_valid(
        self, mock_find_gcloud, mock_adc_path, mock_run
    ):
        """Test returns True when token is valid."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_run.return_value = MagicMock(returncode=0)

        result = check_adc_credentials_valid()

        assert result is True
        mock_run.assert_called_once()

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_when_token_invalid(
        self, mock_find_gcloud, mock_adc_path, mock_run
    ):
        """Test returns False when token is invalid."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_run.return_value = MagicMock(returncode=1)

        result = check_adc_credentials_valid()

        assert result is False

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_on_timeout(self, mock_find_gcloud, mock_adc_path, mock_run):
        """Test returns False on subprocess timeout."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="gcloud", timeout=30)

        result = check_adc_credentials_valid()

        assert result is False

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_on_exception(
        self, mock_find_gcloud, mock_adc_path, mock_run
    ):
        """Test returns False on unexpected exception."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_run.side_effect = Exception("Unexpected error")

        result = check_adc_credentials_valid()

        assert result is False


class TestRefreshAdcCredentials:
    """Tests for refresh_adc_credentials function."""

    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_when_no_gcloud(self, mock_find_gcloud):
        """Test returns False when gcloud not found."""
        mock_find_gcloud.return_value = None

        result = refresh_adc_credentials()

        assert result is False

    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_when_no_adc_file(self, mock_find_gcloud, mock_adc_path):
        """Test returns False when ADC file doesn't exist."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = False

        result = refresh_adc_credentials()

        assert result is False

    @patch("ami.dataops.backup.backup_config._check_and_refresh_adc_token")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_delegates_to_check_and_refresh(
        self, mock_find_gcloud, mock_adc_path, mock_check_refresh
    ):
        """Test delegates to _check_and_refresh_adc_token."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_check_refresh.return_value = True

        result = refresh_adc_credentials()

        assert result is True
        mock_check_refresh.assert_called_once_with("/usr/bin/gcloud")

    @patch("ami.dataops.backup.backup_config._check_and_refresh_adc_token")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_on_timeout(
        self, mock_find_gcloud, mock_adc_path, mock_check_refresh
    ):
        """Test returns False on timeout."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_check_refresh.side_effect = subprocess.TimeoutExpired(
            cmd="gcloud", timeout=30
        )

        result = refresh_adc_credentials()

        assert result is False

    @patch("ami.dataops.backup.backup_config._check_and_refresh_adc_token")
    @patch("ami.dataops.backup.backup_config.ADC_CREDENTIALS_PATH")
    @patch("ami.dataops.backup.backup_config.find_gcloud")
    def test_returns_false_on_exception(
        self, mock_find_gcloud, mock_adc_path, mock_check_refresh
    ):
        """Test returns False on unexpected exception."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_adc_path.exists.return_value = True
        mock_check_refresh.side_effect = Exception("Unexpected error")

        result = refresh_adc_credentials()

        assert result is False


class TestCheckAndRefreshAdcToken:
    """Tests for _check_and_refresh_adc_token function."""

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    def test_returns_true_when_token_valid(self, mock_run):
        """Test returns True when existing token is valid."""
        mock_run.return_value = MagicMock(returncode=0)

        result = _check_and_refresh_adc_token("/usr/bin/gcloud")

        assert result is True
        assert mock_run.call_count == 1  # Only print-access-token called

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    def test_refreshes_when_token_invalid(self, mock_run):
        """Test refreshes token when invalid."""
        # First call (print-access-token) fails, second call (login) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1, stderr="Token expired"),
            MagicMock(returncode=0),
        ]

        result = _check_and_refresh_adc_token("/usr/bin/gcloud")

        assert result is True
        assert mock_run.call_count == EXPECTED_SUBPROCESS_CALL_COUNT

    @patch("ami.dataops.backup.backup_config.subprocess.run")
    def test_returns_false_when_refresh_fails(self, mock_run):
        """Test returns False when refresh also fails."""
        # Both calls fail
        mock_run.side_effect = [
            MagicMock(returncode=1, stderr="Token expired"),
            MagicMock(returncode=1, stderr="Login failed"),
        ]

        result = _check_and_refresh_adc_token("/usr/bin/gcloud")

        assert result is False
        assert mock_run.call_count == EXPECTED_SUBPROCESS_CALL_COUNT


class TestBackupRestoreConfig:
    """Tests for BackupRestoreConfig (core/config.py)."""

    @pytest.fixture(autouse=True)
    def _isolate(self, monkeypatch):
        """Prevent finding real project root."""
        monkeypatch.setattr(
            "ami.dataops.backup.core.config.get_project_root",
            lambda: (_ for _ in ()).throw(RuntimeError),
        )

    def test_load_defaults(self, tmp_path):
        """Test loading restore config with defaults."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\n")

        config = BackupRestoreConfig.load(tmp_path)
        assert config.auth_method == "oauth"
        default_timeout = 3600
        assert config.restore_timeout == default_timeout
        assert config.preserve_permissions is True
        assert config.preserve_timestamps is True

    def test_load_custom_restore_path(self, tmp_path):
        """Test RESTORE_PATH env var."""
        env_file = tmp_path / ".env"
        custom = tmp_path / "my_restore"
        env_file.write_text(f"GDRIVE_AUTH_METHOD=oauth\nRESTORE_PATH={custom}\n")
        config = BackupRestoreConfig.load(tmp_path)
        assert config.restore_path == custom

    def test_load_custom_timeout(self, tmp_path):
        """Test RESTORE_TIMEOUT env var."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\nRESTORE_TIMEOUT=600\n")
        config = BackupRestoreConfig.load(tmp_path)
        expected_timeout = 600
        assert config.restore_timeout == expected_timeout

    def test_load_invalid_timeout_raises(self, tmp_path, monkeypatch):
        """Test invalid RESTORE_TIMEOUT raises."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\n")
        monkeypatch.setenv("RESTORE_TIMEOUT", "abc")
        with pytest.raises(BackupConfigError, match="Invalid"):
            BackupRestoreConfig.load(tmp_path)

    def test_preserves_booleans_false(self, tmp_path):
        """Test boolean env vars set to false."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=oauth\n"
            "RESTORE_PRESERVE_PERMISSIONS=false\n"
            "RESTORE_PRESERVE_TIMESTAMPS=no\n"
        )
        config = BackupRestoreConfig.load(tmp_path)
        assert config.preserve_permissions is False
        assert config.preserve_timestamps is False

    @patch(
        "ami.dataops.backup.core.config.find_gcloud",
        return_value="/usr/bin/gcloud",
    )
    def test_impersonation_auth(self, mock_gcloud, tmp_path):
        """Test impersonation auth loading."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=impersonation\n"
            "GDRIVE_SERVICE_ACCOUNT_EMAIL=sa@p.iam.gserviceaccount.com\n"
        )
        config = BackupRestoreConfig.load(tmp_path)
        assert config.auth_method == "impersonation"
        assert config.service_account_email == "sa@p.iam.gserviceaccount.com"

    def test_key_auth(self, tmp_path):
        """Test key auth loading."""
        creds = tmp_path / "sa.json"
        creds.write_text("{}")
        env_file = tmp_path / ".env"
        env_file.write_text(
            f"GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE={creds}\n"
        )
        config = BackupRestoreConfig.load(tmp_path)
        assert config.auth_method == "key"
        assert config.credentials_file == str(creds)

    def test_key_auth_relative_path(self, tmp_path):
        """Test key auth with relative path."""
        creds = tmp_path / "creds.json"
        creds.write_text("{}")
        env_file = tmp_path / ".env"
        env_file.write_text(
            "GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE=creds.json\n"
        )
        config = BackupRestoreConfig.load(tmp_path)
        assert config.credentials_file == str(creds)

    def test_invalid_auth_raises(self, tmp_path):
        """Test invalid auth method raises."""
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=bad\n")
        with pytest.raises(BackupConfigError, match="Invalid"):
            BackupRestoreConfig.load(tmp_path)
