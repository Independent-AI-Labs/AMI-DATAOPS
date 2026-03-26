"""Tests for the interactive restore wizard."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.restore.wizard import FileSelection, RestoreWizard
from ami.dataops.backup.types import DriveRevisionInfo

WIZARD_MODULE = "ami.dataops.backup.restore.wizard"
EXPECTED_TWO_PATHS = 2


def _make_wizard(
    tmp_path: Path,
    backup_files: list | None = None,
    revisions: list | None = None,
) -> RestoreWizard:
    """Create a wizard with mocked dependencies."""
    service = MagicMock()
    service.list_available_drive_backups = AsyncMock(return_value=backup_files or [])
    service.restore_from_drive_by_file_id = AsyncMock(return_value=True)
    service.selective_restore_from_drive_by_file_id = AsyncMock(return_value=True)

    revisions_client = MagicMock()
    revisions_client.list_revisions = AsyncMock(return_value=revisions or [])
    revisions_client.download_revision = AsyncMock(return_value=True)

    config = MagicMock()
    return RestoreWizard(service, revisions_client, config, tmp_path)


class TestWizardSelectBackupFile:
    """Tests for RestoreWizard._select_backup_file."""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_files(self, tmp_path: Path) -> None:
        """Test returns None when no backup files found."""
        wizard = _make_wizard(tmp_path)
        result = await wizard._select_backup_file()
        assert result is None

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.select_backup_interactive")
    async def test_returns_file_id_and_name(self, mock_select, tmp_path: Path) -> None:
        """Test returns tuple of file_id and name."""
        files = [{"id": "f1", "name": "backup.tar.zst"}]
        wizard = _make_wizard(tmp_path, backup_files=files)
        mock_select.return_value = "f1"

        result = await wizard._select_backup_file()

        assert result == FileSelection("f1", "backup.tar.zst")

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.select_backup_interactive")
    async def test_returns_none_when_cancelled(
        self, mock_select, tmp_path: Path
    ) -> None:
        """Test returns None when user cancels selection."""
        files = [{"id": "f1", "name": "backup.tar.zst"}]
        wizard = _make_wizard(tmp_path, backup_files=files)
        mock_select.return_value = None

        result = await wizard._select_backup_file()

        assert result is None


class TestWizardSelectRevision:
    """Tests for RestoreWizard._select_revision."""

    @pytest.mark.asyncio
    async def test_returns_head_when_no_revisions(self, tmp_path: Path) -> None:
        """Test returns head revision when no history."""
        wizard = _make_wizard(tmp_path, revisions=[])
        result = await wizard._select_revision("file1")

        assert result is not None
        assert result["id"] == "head"

    @pytest.mark.asyncio
    async def test_auto_selects_single_revision(self, tmp_path: Path) -> None:
        """Test auto-selects when only one revision."""
        rev = DriveRevisionInfo(
            id="r1", modifiedTime="2024-01-01T00:00:00", size="1024"
        )
        wizard = _make_wizard(tmp_path, revisions=[rev])
        result = await wizard._select_revision("file1")

        assert result is not None
        assert result["id"] == "r1"

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.MenuSelector")
    async def test_presents_menu_for_multiple(
        self, mock_menu_cls, tmp_path: Path
    ) -> None:
        """Test presents menu when multiple revisions."""
        revs = [
            DriveRevisionInfo(id="r2", modifiedTime="2024-02-01", size="2048"),
            DriveRevisionInfo(id="r1", modifiedTime="2024-01-01", size="1024"),
        ]
        wizard = _make_wizard(tmp_path, revisions=revs)

        mock_item = MagicMock()
        mock_item.value = revs[0]
        mock_menu_cls.return_value.run.return_value = [mock_item]

        result = await wizard._select_revision("file1")

        assert result == revs[0]
        mock_menu_cls.assert_called_once()

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.MenuSelector")
    async def test_returns_none_when_cancelled(
        self, mock_menu_cls, tmp_path: Path
    ) -> None:
        """Test returns None when user cancels."""
        revs = [
            DriveRevisionInfo(id="r2", modifiedTime="2024-02-01"),
            DriveRevisionInfo(id="r1", modifiedTime="2024-01-01"),
        ]
        wizard = _make_wizard(tmp_path, revisions=revs)
        mock_menu_cls.return_value.run.return_value = None

        result = await wizard._select_revision("file1")

        assert result is None


class TestWizardChooseRestorePath:
    """Tests for RestoreWizard._choose_restore_path."""

    @patch(f"{WIZARD_MODULE}.confirm", return_value=True)
    def test_returns_default_when_confirmed(self, mock_confirm, tmp_path: Path) -> None:
        """Test returns default path when user confirms."""
        wizard = _make_wizard(tmp_path)
        result = wizard._choose_restore_path()

        assert result == tmp_path
        mock_confirm.assert_called_once()

    @patch(f"{WIZARD_MODULE}.MenuSelector")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_none_when_menu_cancelled(
        self, mock_confirm, mock_menu_cls, tmp_path: Path
    ) -> None:
        """Test returns None when user cancels alternative menu."""
        wizard = _make_wizard(tmp_path)
        mock_menu_cls.return_value.run.return_value = None

        result = wizard._choose_restore_path()

        assert result is None

    @patch(f"{WIZARD_MODULE}.MenuSelector")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_selected_alternative(
        self, mock_confirm, mock_menu_cls, tmp_path: Path
    ) -> None:
        """Test returns selected alternative path."""
        wizard = _make_wizard(tmp_path)
        mock_item = MagicMock()
        mock_item.value = str(tmp_path / "alt")
        mock_menu_cls.return_value.run.return_value = [mock_item]

        result = wizard._choose_restore_path()

        assert result == tmp_path / "alt"

    @patch("builtins.input", return_value="/custom/path")
    @patch(f"{WIZARD_MODULE}.MenuSelector")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_custom_input_path(
        self, mock_confirm, mock_menu_cls, mock_input, tmp_path: Path
    ) -> None:
        """Test returns custom path from user input."""
        wizard = _make_wizard(tmp_path)
        mock_item = MagicMock()
        mock_item.value = "Enter custom path"
        mock_menu_cls.return_value.run.return_value = [mock_item]

        result = wizard._choose_restore_path()

        assert result == Path("/custom/path")


class TestWizardSelectPaths:
    """Tests for RestoreWizard._select_paths."""

    @patch(f"{WIZARD_MODULE}.confirm", return_value=True)
    def test_returns_none_when_all_confirmed(
        self, mock_confirm, tmp_path: Path
    ) -> None:
        """Test returns None when user wants all files."""
        wizard = _make_wizard(tmp_path)
        result = wizard._select_paths()

        assert result is None
        mock_confirm.assert_called_once()

    @patch("builtins.input", side_effect=["etc/config.yaml", "var/data", ""])
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_paths_from_input(
        self, mock_confirm, mock_input, tmp_path: Path
    ) -> None:
        """Test returns list of paths from user input."""
        wizard = _make_wizard(tmp_path)
        result = wizard._select_paths()

        assert result is not None
        assert len(result) == EXPECTED_TWO_PATHS
        assert result[0] == Path("etc/config.yaml")
        assert result[1] == Path("var/data")

    @patch("builtins.input", return_value="")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_none_on_empty_input(
        self, mock_confirm, mock_input, tmp_path: Path
    ) -> None:
        """Test returns None when no paths entered."""
        wizard = _make_wizard(tmp_path)
        result = wizard._select_paths()

        assert result is None


class TestWizardConfirmRestore:
    """Tests for RestoreWizard._confirm_restore."""

    @patch(f"{WIZARD_MODULE}.TUI")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=True)
    def test_returns_true_when_confirmed(
        self, mock_confirm, mock_tui, tmp_path: Path
    ) -> None:
        """Test returns True when user confirms."""
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="r1", modifiedTime="2024-01-01", size="1024")

        result = wizard._confirm_restore("backup.tar.zst", rev, tmp_path)

        assert result is True

    @patch(f"{WIZARD_MODULE}.TUI")
    @patch(f"{WIZARD_MODULE}.confirm", return_value=False)
    def test_returns_false_when_declined(
        self, mock_confirm, mock_tui, tmp_path: Path
    ) -> None:
        """Test returns False when user declines."""
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="r1", modifiedTime="2024-01-01", size="1024")

        result = wizard._confirm_restore("backup.tar.zst", rev, tmp_path)

        assert result is False


class TestWizardExecuteRestore:
    """Tests for RestoreWizard._execute_restore."""

    @pytest.mark.asyncio
    async def test_delegates_to_service_for_head(self, tmp_path: Path) -> None:
        """Test delegates to service for head revision."""
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="head", modifiedTime="current")

        result = await wizard._execute_restore("f1", rev, tmp_path)

        assert result is True
        wizard.service.restore_from_drive_by_file_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_delegates_to_service_for_empty_id(self, tmp_path: Path) -> None:
        """Test delegates to service when revision ID is empty."""
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="", modifiedTime="current")

        result = await wizard._execute_restore("f1", rev, tmp_path)

        assert result is True
        wizard.service.restore_from_drive_by_file_id.assert_called_once()

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.extract_specific_paths", new_callable=AsyncMock)
    async def test_downloads_specific_revision(
        self, mock_extract, tmp_path: Path
    ) -> None:
        """Test downloads and extracts specific revision."""
        mock_extract.return_value = True
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="r123", modifiedTime="2024-01-01", size="1024")

        result = await wizard._execute_restore("f1", rev, tmp_path)

        assert result is True
        wizard.revisions_client.download_revision.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_false_on_download_failure(self, tmp_path: Path) -> None:
        """Test returns False when revision download fails."""
        wizard = _make_wizard(tmp_path)
        wizard.revisions_client.download_revision = AsyncMock(return_value=False)
        rev = DriveRevisionInfo(id="r123", modifiedTime="2024-01-01")

        result = await wizard._execute_restore("f1", rev, tmp_path)

        assert result is False

    @pytest.mark.asyncio
    async def test_selective_restore_for_head_with_paths(self, tmp_path: Path) -> None:
        """Test uses selective restore when paths provided for head."""
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="head", modifiedTime="current")
        paths = [Path("etc/config.yaml")]

        result = await wizard._execute_restore("f1", rev, tmp_path, paths)

        assert result is True
        wizard.service.selective_restore_from_drive_by_file_id.assert_called_once()
        wizard.service.restore_from_drive_by_file_id.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.extract_specific_paths", new_callable=AsyncMock)
    async def test_passes_paths_to_extract_for_revision(
        self, mock_extract, tmp_path: Path
    ) -> None:
        """Test passes paths to extract_specific_paths for specific revision."""
        mock_extract.return_value = True
        wizard = _make_wizard(tmp_path)
        rev = DriveRevisionInfo(id="r123", modifiedTime="2024-01-01", size="1024")
        paths = [Path("etc/config.yaml"), Path("var/data")]

        result = await wizard._execute_restore("f1", rev, tmp_path, paths)

        assert result is True
        mock_extract.assert_called_once()
        call_args = mock_extract.call_args
        assert call_args[0][1] == paths


class TestWizardRun:
    """Tests for RestoreWizard.run end-to-end."""

    @pytest.mark.asyncio
    async def test_returns_false_when_no_files(self, tmp_path: Path) -> None:
        """Test returns False when no backup files found."""
        wizard = _make_wizard(tmp_path)
        result = await wizard.run()
        assert result is False

    @pytest.mark.asyncio
    @patch(f"{WIZARD_MODULE}.select_backup_interactive")
    async def test_returns_false_when_file_cancelled(
        self, mock_select, tmp_path: Path
    ) -> None:
        """Test returns False when file selection cancelled."""
        files = [{"id": "f1", "name": "backup.tar.zst"}]
        wizard = _make_wizard(tmp_path, backup_files=files)
        mock_select.return_value = None

        result = await wizard.run()

        assert result is False

    @pytest.mark.asyncio
    @patch(
        f"{WIZARD_MODULE}.extract_specific_paths",
        new_callable=AsyncMock,
        return_value=True,
    )
    @patch(f"{WIZARD_MODULE}.confirm", return_value=True)
    @patch(f"{WIZARD_MODULE}.TUI")
    @patch(f"{WIZARD_MODULE}.select_backup_interactive")
    async def test_full_wizard_success(
        self, mock_select, mock_tui, mock_confirm, mock_extract, tmp_path: Path
    ) -> None:
        """Test full wizard flow succeeds."""
        files = [{"id": "f1", "name": "backup.tar.zst"}]
        revs = [DriveRevisionInfo(id="r1", modifiedTime="2024-01-01", size="1024")]
        wizard = _make_wizard(tmp_path, backup_files=files, revisions=revs)
        mock_select.return_value = "f1"
        # confirm is called three times: path + paths(all) + final confirm
        mock_confirm.side_effect = [True, True, True]

        result = await wizard.run()

        assert result is True
