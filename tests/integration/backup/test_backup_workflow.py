"""Integration tests for backup creation and restore workflows."""

import asyncio
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

import ami.dataops.backup.restore.service as svc_mod
from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupConfigError, UploadError
from ami.dataops.backup.common.auth import (
    AuthenticationManager,
    OAuthCredentialsProvider,
    ServiceAccountCredentialsProvider,
)
from ami.dataops.backup.core.config import BackupRestoreConfig
from ami.dataops.backup.create import archiver
from ami.dataops.backup.create.archiver import create_zip_archive
from ami.dataops.backup.create.cli import BackupCLI
from ami.dataops.backup.create.main import main as create_main
from ami.dataops.backup.create.secondary import copy_to_secondary_backup
from ami.dataops.backup.create.service import BackupOptions, BackupService
from ami.dataops.backup.create.utils import (
    cleanup_local_zip,
    cleanup_old_backups,
    validate_backup_file,
)
from ami.dataops.backup.restore import local_client
from ami.dataops.backup.restore.cli import RestoreCLI
from ami.dataops.backup.restore.extractor import (
    extract_specific_paths,
    list_archive_contents,
)
from ami.dataops.backup.restore.main import main as restore_main
from ami.dataops.backup.restore.selector import select_backup_interactive
from ami.dataops.backup.restore.service import BackupRestoreService

DATA_BIN_SIZE = 1024
EXPECTED_BACKUP_COUNT = 3
LATEST_BACKUP_SIZE = 300
OLD_BACKUP_COUNT = 7
KEEP_COUNT = 3
SECONDARY_SIZE = 512
DEFAULT_TIMEOUT = 3600
CUSTOM_TIMEOUT = 120
CUSTOM_REVISION = 2


@pytest.mark.asyncio
async def test_backup_workflow_on_git_directory():
    """Test backup against .git directory (stress test)."""
    git_dir = Path.cwd() / ".git"
    if not git_dir.exists():
        pytest.skip("No .git directory found")

    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)
        archive_path = await archiver.create_zip_archive(
            git_dir, output_dir=output_dir, ignore_exclusions=True
        )
        assert archive_path.exists()
        assert archive_path.stat().st_size > 0


@pytest.mark.asyncio
async def test_backup_config_loads_from_env(tmp_path, monkeypatch):
    """Test BackupConfig loads from .env file."""
    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    (tmp_path / ".env").write_text("GDRIVE_AUTH_METHOD=oauth\n")
    config = BackupConfig.load(tmp_path)
    assert config.auth_method == "oauth"
    assert config.root_dir == tmp_path


@pytest.mark.asyncio
async def test_full_backup_create_and_restore(tmp_path):
    """Full cycle: create → archive → restore → verify."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "file1.txt").write_text("hello world")
    (src / "subdir").mkdir()
    (src / "subdir" / "file2.txt").write_text("nested content")
    (src / "data.bin").write_bytes(b"\x00" * DATA_BIN_SIZE)

    out = tmp_path / "output"
    out.mkdir()
    archive = await create_zip_archive(
        src, output_filename="test-backup", output_dir=out
    )
    assert archive.exists()

    dest = tmp_path / "restored"
    dest.mkdir()
    ok = await extract_specific_paths(archive, None, dest)
    assert ok is True

    restored = dest / "source"
    assert (restored / "file1.txt").read_text() == "hello world"
    assert (restored / "subdir" / "file2.txt").read_text() == "nested content"
    assert (restored / "data.bin").stat().st_size == DATA_BIN_SIZE


@pytest.mark.asyncio
async def test_selective_restore(tmp_path):
    """Test restoring specific paths from archive."""
    src = tmp_path / "project"
    src.mkdir()
    (src / "keep.txt").write_text("keep me")
    (src / "skip.txt").write_text("skip me")

    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(
        src, output_filename="sel-backup", output_dir=out
    )

    dest = tmp_path / "restored"
    dest.mkdir()
    ok = await extract_specific_paths(archive, [Path("project/keep.txt")], dest)
    assert ok is True
    assert (dest / "project" / "keep.txt").exists()
    assert not (dest / "project" / "skip.txt").exists()


@pytest.mark.asyncio
async def test_backup_exclusion_patterns(tmp_path):
    """Test that .pyc files are excluded."""
    src = tmp_path / "myproject"
    src.mkdir()
    (src / "code.py").write_text("print('hi')")
    (src / "__pycache__").mkdir()
    (src / "__pycache__" / "code.cpython-311.pyc").write_bytes(b"\x00")

    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="excl-test", output_dir=out)
    contents = await list_archive_contents(archive)
    pyc_files = [c for c in contents if c.endswith(".pyc")]
    assert len(pyc_files) == 0


@pytest.mark.asyncio
async def test_backup_custom_filename(tmp_path):
    """Test archive with custom filename."""
    src = tmp_path / "data"
    src.mkdir()
    (src / "f.txt").write_text("x")
    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(
        src, output_filename="my-custom-name", output_dir=out
    )
    assert archive.name == "my-custom-name.tar.zst"


@pytest.mark.asyncio
async def test_local_client_list_and_find(tmp_path):
    """Test local_client backup discovery."""
    for i in range(EXPECTED_BACKUP_COUNT):
        p = tmp_path / f"backup-{i}.tar.zst"
        p.write_bytes(b"\x00" * (i + 1) * 100)
        time.sleep(0.05)

    backups = await local_client.list_backups_in_directory(tmp_path)
    assert len(backups) == EXPECTED_BACKUP_COUNT
    assert backups[0].name == "backup-2.tar.zst"

    latest = await local_client.find_latest_backup(tmp_path)
    assert latest is not None
    assert latest.name == "backup-2.tar.zst"

    size = await local_client.get_backup_size(backups[0])
    assert size == LATEST_BACKUP_SIZE


@pytest.mark.asyncio
async def test_local_client_verify(tmp_path):
    """Test backup verification."""
    real = tmp_path / "exists.tar.zst"
    real.write_bytes(b"\x00")
    assert await local_client.verify_backup_exists(real) is True
    assert await local_client.verify_backup_exists(tmp_path / "nope") is False


@pytest.mark.asyncio
async def test_secondary_backup_copy(tmp_path, monkeypatch):
    """Test secondary backup copies to mount."""
    mount = tmp_path / "backup_mount"
    mount.mkdir()
    monkeypatch.setenv("AMI_BACKUP_MOUNT", str(mount))
    src_file = tmp_path / "test.tar.zst"
    src_file.write_bytes(b"\x00" * SECONDARY_SIZE)
    ok = await copy_to_secondary_backup(src_file)
    assert ok is True
    assert (mount / "test.tar.zst").stat().st_size == SECONDARY_SIZE


@pytest.mark.asyncio
async def test_backup_utils_cleanup(tmp_path):
    """Test cleanup utilities."""
    f = tmp_path / "keep.tar.zst"
    f.write_bytes(b"\x00")
    await cleanup_local_zip(f, keep_local=True)
    assert f.exists()

    f2 = tmp_path / "delete.tar.zst"
    f2.write_bytes(b"\x00")
    await cleanup_local_zip(f2, keep_local=False)
    assert not f2.exists()

    for i in range(OLD_BACKUP_COUNT):
        (tmp_path / f"old-{i:02d}.tar.zst").write_bytes(b"\x00")
    await cleanup_old_backups(tmp_path, keep_count=KEEP_COUNT)
    assert len(list(tmp_path.glob("*.tar.zst"))) == KEEP_COUNT


@pytest.mark.asyncio
async def test_restore_config_full_load(tmp_path, monkeypatch):
    """Test BackupRestoreConfig full load path."""
    monkeypatch.setattr(
        "ami.dataops.backup.core.config.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    monkeypatch.setenv("RESTORE_TIMEOUT", "120")
    monkeypatch.setenv("RESTORE_PRESERVE_PERMISSIONS", "false")
    (tmp_path / ".env").write_text("GDRIVE_AUTH_METHOD=oauth\n")
    config = BackupRestoreConfig.load(tmp_path)
    assert config.restore_timeout == CUSTOM_TIMEOUT
    assert config.preserve_permissions is False
    assert config.preserve_timestamps is True


@pytest.mark.asyncio
async def test_backup_service_restore_local(tmp_path):
    """Test BackupRestoreService local restore."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("aaa")
    out = tmp_path / "archives"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="svc-test", output_dir=out)
    svc = BackupRestoreService(MagicMock(), MagicMock())
    dest = tmp_path / "restored"
    ok = await svc.restore_local_backup(archive, dest)
    assert ok is True
    assert (dest / "src" / "a.txt").read_text() == "aaa"


@pytest.mark.asyncio
async def test_backup_service_selective_local(tmp_path):
    """Test selective local restore via service."""
    src = tmp_path / "proj"
    src.mkdir()
    (src / "yes.txt").write_text("keep")
    (src / "no.txt").write_text("skip")
    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="sel", output_dir=out)
    svc = BackupRestoreService(MagicMock(), MagicMock())
    dest = tmp_path / "rest"
    ok = await svc.selective_restore_local_backup(archive, [Path("proj/yes.txt")], dest)
    assert ok is True
    assert (dest / "proj" / "yes.txt").exists()
    assert not (dest / "proj" / "no.txt").exists()


@pytest.mark.asyncio
async def test_backup_cli_parser():
    """Test CLI argument parsing."""
    cli = BackupCLI()
    args = cli.parse_arguments(["--keep-local", "--name", "my-bak", "--verbose"])
    assert args.keep_local is True
    assert args.name == "my-bak"
    assert args.verbose is True


@pytest.mark.asyncio
async def test_restore_cli_parser():
    """Test restore CLI argument parsing."""
    cli = RestoreCLI()
    args = cli.parse_arguments(["--local-path", "/tmp/bak.tar.zst", "--verbose"])
    assert str(args.local_path) == "/tmp/bak.tar.zst"
    assert args.verbose is True


@pytest.mark.asyncio
async def test_restore_service_latest_local(tmp_path):
    """Test restore_latest_local finds and restores."""
    src = tmp_path / "data"
    src.mkdir()
    (src / "f.txt").write_text("found")
    bak_dir = tmp_path / "backup_mount"
    bak_dir.mkdir()
    await create_zip_archive(
        src, output_filename="ami-agents-backup", output_dir=bak_dir
    )
    svc = BackupRestoreService(MagicMock(), MagicMock())
    dest = tmp_path / "restored"
    orig = svc_mod.DEFAULT_BACKUP_MOUNT
    svc_mod.DEFAULT_BACKUP_MOUNT = bak_dir
    try:
        ok = await svc.restore_latest_local(dest)
    finally:
        svc_mod.DEFAULT_BACKUP_MOUNT = orig
    assert ok is True
    assert (dest / "data" / "f.txt").read_text() == "found"


@pytest.mark.asyncio
async def test_backup_validate_file(tmp_path):
    """Test archive validation."""
    src = tmp_path / "s"
    src.mkdir()
    (src / "x.txt").write_text("x")
    out = tmp_path / "o"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="val", output_dir=out)
    assert await validate_backup_file(archive) is True


@pytest.mark.asyncio
async def test_auth_manager_creation(tmp_path, monkeypatch):
    """Test AuthenticationManager creation for each method."""
    monkeypatch.setattr(
        "ami.dataops.backup.backup_config.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    (tmp_path / ".env").write_text("GDRIVE_AUTH_METHOD=oauth\n")
    cfg = BackupConfig.load(tmp_path)
    mgr = AuthenticationManager(cfg)
    assert isinstance(mgr._provider, OAuthCredentialsProvider)

    creds = tmp_path / "k.json"
    creds.write_text("{}")
    monkeypatch.setenv("GDRIVE_AUTH_METHOD", "key")
    monkeypatch.setenv("GDRIVE_CREDENTIALS_FILE", str(creds))
    (tmp_path / ".env").write_text(
        f"GDRIVE_AUTH_METHOD=key\nGDRIVE_CREDENTIALS_FILE={creds}\n"
    )
    cfg2 = BackupConfig.load(tmp_path)
    mgr2 = AuthenticationManager(cfg2)
    assert isinstance(mgr2._provider, ServiceAccountCredentialsProvider)


@pytest.mark.asyncio
async def test_backup_service_full_run(tmp_path, monkeypatch):
    """Test BackupService.run_backup with mocked upload."""
    src = tmp_path / "project"
    src.mkdir()
    (src / "main.py").write_text("print('hi')")
    mock_uploader = AsyncMock()
    mock_uploader.upload_to_gdrive.return_value = "fake_id"
    mock_auth = MagicMock()
    mock_auth.get_credentials.return_value = MagicMock()
    monkeypatch.setattr(
        "ami.dataops.backup.create.service.BackupConfig.load",
        lambda _: MagicMock(auth_method="oauth", folder_id=None, root_dir=tmp_path),
    )
    svc = BackupService(mock_uploader, mock_auth)
    opts = BackupOptions(source_dir=src, keep_local=True, config_path=tmp_path)
    file_id = await svc.run_backup(opts)
    assert file_id == "fake_id"


@pytest.mark.asyncio
async def test_restore_cli_local_path_run(tmp_path):
    """Exercise restore CLI run with --local-path."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "f.txt").write_text("data")
    out = tmp_path / "out"
    out.mkdir()
    archive = await create_zip_archive(src, output_filename="cli-test", output_dir=out)
    mock_svc = AsyncMock()
    mock_svc.restore_local_backup.return_value = True
    cli = RestoreCLI(mock_svc)
    dest = tmp_path / "dest"
    args = cli.parse_arguments(
        ["--local-path", str(archive), "--restore-path", str(dest)]
    )
    assert await cli.run(args) == 0


@pytest.mark.asyncio
async def test_restore_cli_latest_local_run(tmp_path):
    """Exercise restore CLI run with --latest-local."""
    mock_svc = AsyncMock()
    mock_svc.restore_latest_local.return_value = True
    cli = RestoreCLI(mock_svc)
    args = cli.parse_arguments(
        ["--latest-local", "--restore-path", str(tmp_path / "dest")]
    )
    assert await cli.run(args) == 0


@pytest.mark.asyncio
async def test_create_main_entry(tmp_path, monkeypatch):
    """Exercise create main() entry point (fails without .env)."""
    monkeypatch.setattr(
        "ami.dataops.backup.common.paths.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    os.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["ami-backup"])
    assert create_main() == 1


@pytest.mark.asyncio
async def test_restore_main_entry(tmp_path, monkeypatch):
    """Exercise restore main() entry point (fails without .env)."""
    monkeypatch.setattr(
        "ami.dataops.backup.common.paths.get_project_root",
        lambda: (_ for _ in ()).throw(RuntimeError),
    )
    os.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["ami-restore", "--latest-local"])
    assert restore_main() == 1


@pytest.mark.asyncio
async def test_selector_module_loads():
    """Test selector returns None for empty list."""
    assert select_backup_interactive([]) is None


@pytest.mark.asyncio
async def test_restore_cli_file_id_run():
    """Exercise restore CLI with --file-id."""
    mock_svc = AsyncMock()
    mock_svc.restore_from_drive_by_file_id.return_value = True
    cli = RestoreCLI(mock_svc)
    args = cli.parse_arguments(["--file-id", "abc123"])
    args.config_path = Path("/tmp")
    assert await cli.run(args) == 0


@pytest.mark.asyncio
async def test_restore_cli_revision_run():
    """Exercise restore CLI with --revision."""
    mock_svc = AsyncMock()
    mock_svc.restore_from_drive_by_revision.return_value = True
    cli = RestoreCLI(mock_svc)
    args = cli.parse_arguments(["--revision", "0"])
    args.config_path = Path("/tmp")
    assert await cli.run(args) == 0


@pytest.mark.asyncio
async def test_restore_service_validate_path(tmp_path):
    """Test restore path validation."""
    svc = BackupRestoreService(MagicMock(), MagicMock())
    assert await svc.validate_restore_path(tmp_path) is True
    assert await svc.validate_restore_path(tmp_path / "new") is True
    f = tmp_path / "file.txt"
    f.write_text("x")
    assert await svc.validate_restore_path(f) is False


@pytest.mark.asyncio
async def test_backup_cli_error_suggestions():
    """Test CLI error suggestion logic."""
    cli = BackupCLI()
    cli._log_error_suggestions(BackupConfigError("GDRIVE_AUTH_METHOD bad"), True)
    cli._log_error_suggestions(UploadError("reauthentication"), True)
    cli._log_error_suggestions(Exception("random"), False)


@pytest.mark.asyncio
async def test_cli_full_parsers():
    """Exercise all CLI argument combinations."""
    bcli = BackupCLI()
    args = bcli.parse_arguments(
        [
            "/src",
            "--name",
            "x",
            "--keep-local",
            "--include-all",
            "--no-auth-retry",
            "--auth-mode",
            "impersonation",
            "--verbose",
        ]
    )
    assert args.keep_local is True
    assert args.auth_mode == "impersonation"

    rcli = RestoreCLI()
    assert rcli.parse_arguments(["--file-id", "a"]).file_id == "a"
    assert rcli.parse_arguments(["--latest-local"]).latest_local is True
    r = rcli.parse_arguments(["--revision", str(CUSTOM_REVISION)])
    assert r.revision == CUSTOM_REVISION


if __name__ == "__main__":
    asyncio.run(test_backup_workflow_on_git_directory())
