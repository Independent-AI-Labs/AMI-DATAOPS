"""Integration tests for CLI and wizard module coverage."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupError
from ami.dataops.backup.create.archiver import create_zip_archive
from ami.dataops.backup.create.cli import BackupCLI
from ami.dataops.backup.create.service import BackupService
from ami.dataops.backup.restore.cli import RestoreCLI
from ami.dataops.backup.restore.extractor import list_archive_contents
from ami.dataops.backup.restore.revision_display import display_revision_list
from ami.dataops.backup.restore.selector import select_backup_interactive
from ami.dataops.backup.restore.service import BackupRestoreService
from ami.dataops.backup.restore.wizard import FileSelection, RestoreWizard
from ami.dataops.backup.types import DriveRevisionInfo
from ami.dataops.backup.utils.archive_utils import create_archive


@pytest.mark.asyncio
async def test_display_revision_list(capsys):
    """Test revision display prints formatted output."""
    revisions: list[DriveRevisionInfo] = [
        {"id": "r1", "modifiedTime": "2026-01-01", "size": "1024"},
        {"id": "r2", "modifiedTime": "2026-01-02", "size": "2048"},
    ]
    display_revision_list("backup.tar.zst", revisions)
    out = capsys.readouterr().out
    assert "r1" in out
    assert "r2" in out
    assert "backup.tar.zst" in out


@pytest.mark.asyncio
async def test_display_revision_list_empty(capsys):
    """Test revision display handles empty list."""
    display_revision_list("x.tar.zst", [])
    out = capsys.readouterr().out
    assert "No revisions" in out


@pytest.mark.asyncio
async def test_selector_empty_list():
    """Test selector returns None for empty list."""
    assert select_backup_interactive([]) is None


@pytest.mark.asyncio
async def test_wizard_step_header(capsys):
    """Test wizard step header output."""
    svc = MagicMock()
    rev = MagicMock()
    auth = MagicMock()
    w = RestoreWizard(svc, rev, auth, Path("/tmp"))
    w._print_step_header(1, "Test Step")
    out = capsys.readouterr().out
    assert "Test Step" in out


@pytest.mark.asyncio
async def test_file_selection_namedtuple():
    """Test FileSelection fields."""
    fs = FileSelection(file_id="abc", file_name="test.tar.zst")
    assert fs.file_id == "abc"
    assert fs.file_name == "test.tar.zst"


@pytest.mark.asyncio
async def test_wizard_step_output(capsys):
    """Test wizard methods produce output."""
    svc = MagicMock()
    rev = MagicMock()
    auth = MagicMock()
    w = RestoreWizard(svc, rev, auth, Path("/tmp/restore"))
    w._print_step_header(2, "Choose Path")
    out = capsys.readouterr().out
    assert "Choose Path" in out


@pytest.mark.asyncio
async def test_restore_cli_verbose_flag():
    """Test verbose flag enables debug logging."""
    cli = RestoreCLI()
    args = cli.parse_arguments(["--latest-local", "--verbose"])
    assert args.verbose is True


@pytest.mark.asyncio
async def test_backup_cli_setup_auth_flag():
    """Test --setup-auth flag."""
    cli = BackupCLI()
    args = cli.parse_arguments(["--setup-auth"])
    assert args.setup_auth is True


@pytest.mark.asyncio
async def test_restore_cli_list_revisions():
    """Test --list-revisions flag."""
    cli = RestoreCLI()
    args = cli.parse_arguments(["--list-revisions"])
    assert args.list_revisions is True


@pytest.mark.asyncio
async def test_restore_cli_no_action_returns_error():
    """Test CLI returns error when no action specified."""
    mock_svc = AsyncMock()
    cli = RestoreCLI(mock_svc)
    args = cli.parse_arguments([])
    result = await cli.run(args)
    assert result == 1


@pytest.mark.asyncio
async def test_archive_utils_create_and_list(tmp_path):
    """Test archive creation and listing."""

    src = tmp_path / "data"
    src.mkdir()
    (src / "a.txt").write_text("aaa")
    (src / "b.txt").write_text("bbb")

    archive = await create_archive(src, tmp_path / "test.tar.zst")
    assert archive.exists()

    contents = await list_archive_contents(archive)
    assert any("a.txt" in c for c in contents)
    assert any("b.txt" in c for c in contents)


@pytest.mark.asyncio
async def test_restore_service_list_archive_contents(tmp_path):
    """Test listing archive contents through service layer."""

    src = tmp_path / "proj"
    src.mkdir()
    (src / "x.py").write_text("x = 1")
    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="ls-test", output_dir=out)
    contents = await list_archive_contents(archive)
    assert any("x.py" in c for c in contents)


@pytest.mark.asyncio
async def test_backup_config_folder_id(tmp_path, monkeypatch):
    """Test config loads folder_id from env."""

    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    monkeypatch.setenv("GDRIVE_BACKUP_FOLDER_ID", "xyz")
    (tmp_path / ".env").write_text("GDRIVE_AUTH_METHOD=oauth\n")
    cfg = BackupConfig.load(tmp_path)
    assert cfg.folder_id == "xyz"


@pytest.mark.asyncio
async def test_backup_config_impersonation_setup(tmp_path, monkeypatch):
    """Test config loads impersonation auth."""
    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.find_gcloud",
        lambda: "/usr/bin/gcloud",
    )
    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.check_adc_credentials_valid",
        lambda: True,
    )
    monkeypatch.setenv("GDRIVE_AUTH_METHOD", "impersonation")
    monkeypatch.setenv("GDRIVE_SERVICE_ACCOUNT_EMAIL", "sa@p.iam.gserviceaccount.com")
    (tmp_path / ".env").write_text("GDRIVE_AUTH_METHOD=impersonation\n")
    cfg = BackupConfig.load(tmp_path)
    assert cfg.auth_method == "impersonation"
    assert cfg.service_account_email == "sa@p.iam.gserviceaccount.com"


@pytest.mark.asyncio
async def test_wizard_attributes():
    """Test wizard stores config correctly."""
    svc = MagicMock()
    rev = MagicMock()
    auth = MagicMock()
    restore_path = Path("/tmp/test-restore")
    w = RestoreWizard(svc, rev, auth, restore_path)
    assert w.service is svc
    assert w.revisions_client is rev
    assert w.config is auth
    assert w.default_restore_path == restore_path


@pytest.mark.asyncio
async def test_backup_service_init():
    """Test backup service stores dependencies."""
    mock_uploader = AsyncMock()
    mock_auth = MagicMock()
    svc = BackupService(mock_uploader, mock_auth)
    assert svc.uploader is mock_uploader
    assert svc.auth_manager is mock_auth


@pytest.mark.asyncio
async def test_restore_service_rejects_missing_archive():
    """Test restore service raises on missing archive."""

    svc = BackupRestoreService(MagicMock(), MagicMock())
    with pytest.raises(BackupError):
        await svc.restore_local_backup(Path("/nonexistent.tar.zst"), Path("/tmp/dest"))
