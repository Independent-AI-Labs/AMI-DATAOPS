"""
Command line interface for backup restore operations.

Handles command line argument parsing and execution.
"""

import argparse
import sys
from pathlib import Path
from typing import NamedTuple, cast

from loguru import logger

from ami.cli_components.selector import (
    BackupFileInfo,
    select_backup_interactive,
)
from ami.dataops.backup.core.config import BackupRestoreConfig
from ami.dataops.backup.restore.revision_display import display_revision_list
from ami.dataops.backup.restore.revisions_client import RevisionsClient
from ami.dataops.backup.restore.service import BackupRestoreService
from ami.dataops.backup.restore.wizard import RestoreWizard


class RestoreExecuteResult(NamedTuple):
    """Result from executing a restore operation."""

    success: bool
    handled: bool


class RestoreCLI:
    """Command line interface for backup restore operations."""

    def __init__(
        self,
        service: BackupRestoreService | None = None,
        revisions_client: RevisionsClient | None = None,
    ):
        self.service = service
        self.revisions_client = revisions_client

    def _require_service(self) -> BackupRestoreService:
        """Get the service, raising an error if not initialized."""
        if self.service is None:
            msg = "Restore service not initialized"
            raise RuntimeError(msg)
        return self.service

    def create_parser(self) -> argparse.ArgumentParser:
        """Create the argument parser with all available options."""
        parser = argparse.ArgumentParser(
            prog="backup_restore",
            description="Restore backups from Google Drive or local files",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Restore from specific Google Drive file ID
  backup_restore --file-id 1aBcDeFgHiJkLmNoPqRsTuVwXyZ

  # Restore the latest local backup
  backup_restore --latest-local

  # Restore from specific local file
  backup_restore --local-path /path/to/backup.tar.zst

  # Go back 2 revisions in Google Drive backups
  backup_restore --revision 2 --dest /tmp/restore

  # List available backup revisions
  backup_restore --list-revisions

  # Interactive selection of backup
  backup_restore --interactive

  # Restore with custom location
  backup_restore --latest-local --restore-path /opt/ami-restored
            """,
        )

        # Authentication and configuration options
        parser.add_argument(
            "--config-path",
            type=Path,
            default=Path.cwd(),
            help="Path to directory containing .env file (default: current directory)",
        )

        # Restore source options (mutually exclusive)
        source_group = parser.add_mutually_exclusive_group(required=False)
        source_group.add_argument(
            "--file-id", help="Google Drive file ID for backup to restore"
        )
        source_group.add_argument(
            "--local-path", type=Path, help="Path to local backup file to restore"
        )
        source_group.add_argument(
            "--latest-local",
            action="store_true",
            help="Restore the latest local backup found in common locations",
        )
        source_group.add_argument(
            "--interactive",
            action="store_true",
            help="Interactively select backup from Google Drive",
        )
        source_group.add_argument(
            "--revision",
            type=int,
            help="Go back N revisions (like Git ~1, ~2) - requires Drive access",
        )
        source_group.add_argument(
            "--list-revisions",
            action="store_true",
            help="List available backup revisions (non-interactive)",
        )

        # Restore destination options
        parser.add_argument(
            "--restore-path",
            type=Path,
            default=None,
            help="Specify custom restore location (default: configured restore path)",
        )
        parser.add_argument(
            "--dest",
            type=Path,
            dest="restore_path",
            help="Alias for --restore-path (DEPRECATED: Use --restore-path instead)",
        )

        # Additional options
        parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging"
        )

        # Add positional arguments for file paths to restore (for selective restoration)
        parser.add_argument(
            "paths",
            nargs="*",
            type=Path,
            help="Specific file/directory paths to restore (selective restoration)",
        )

        return parser

    def parse_arguments(self, argv: list[str]) -> argparse.Namespace:
        """Parse command line arguments."""
        parser = self.create_parser()
        return parser.parse_args(argv)

    async def run_restore_by_revision(
        self, revision: int, restore_path: Path, config: BackupRestoreConfig
    ) -> bool:
        """Run restore by revision."""
        service = self._require_service()
        return await service.restore_from_drive_by_revision(
            revision, restore_path, config
        )

    async def run_restore_by_file_id(
        self, file_id: str, restore_path: Path, config: BackupRestoreConfig
    ) -> bool:
        """Run restore by file ID."""
        service = self._require_service()
        return await service.restore_from_drive_by_file_id(
            file_id, restore_path, config
        )

    async def run_restore_local(self, backup_path: Path, restore_path: Path) -> bool:
        """Run local restore."""
        service = self._require_service()
        return await service.restore_local_backup(backup_path, restore_path)

    async def run_restore_latest_local(self, restore_path: Path) -> bool:
        """Run latest local restore."""
        service = self._require_service()
        return await service.restore_latest_local(restore_path)

    async def run_interactive_selection(
        self, config: BackupRestoreConfig, restore_path: Path
    ) -> bool:
        """Run interactive backup selection and restore."""
        service = self._require_service()
        logger.info("Fetching backup files from Google Drive...")
        backup_files = await service.list_available_drive_backups(config)

        if not backup_files:
            logger.error("No backup files found")
            return False

        files_info = cast(list[BackupFileInfo], backup_files)
        selected_file_id = select_backup_interactive(files_info)
        if selected_file_id is None:
            logger.info("No backup selected, exiting")
            return False

        # Restore from the selected Google Drive backup
        return await service.restore_from_drive_by_file_id(
            selected_file_id, restore_path, config
        )

    async def run_list_revisions(self, config: BackupRestoreConfig) -> bool:
        """List available backup revisions using Drive Revisions API."""
        service = self._require_service()
        logger.info("Fetching backup files from Google Drive...")
        backup_files = await service.list_available_drive_backups(config)

        if not backup_files:
            logger.error("No backup files found")
            return False

        if not self.revisions_client:
            logger.error("Revisions client not available")
            return False

        # Use first backup file to show revision history
        file_id = backup_files[0].get("id", "")
        file_name = backup_files[0].get("name", "Unknown")

        revisions = await self.revisions_client.list_revisions(file_id)
        if not revisions:
            logger.warning("No revision history for this file")
            return False

        display_revision_list(file_name, revisions)
        count = len(revisions)
        logger.info(f"Listed {count} revision(s)")
        return True

    def _setup_logging(self, verbose: bool) -> None:
        """Configure logging level based on verbose flag."""
        logger.remove()
        logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")
        logger.info("=" * 60)
        logger.info("AMI Orchestrator Restore from Google Drive")
        logger.info("=" * 60)

    async def _restore_from_file_id(
        self, args: argparse.Namespace, restore_path: Path, config: BackupRestoreConfig
    ) -> bool:
        """Handle restore from Google Drive file ID."""
        service = self._require_service()
        if args.paths:
            logger.info(
                f"Restoring specific paths from Google Drive file ID: {args.file_id}"
            )
            return await service.selective_restore_from_drive_by_file_id(
                args.file_id, args.paths, restore_path, config
            )
        logger.info(f"Restoring from Google Drive file ID: {args.file_id}")
        return await self.run_restore_by_file_id(args.file_id, restore_path, config)

    async def _restore_from_local_path(
        self, args: argparse.Namespace, restore_path: Path
    ) -> bool:
        """Handle restore from local backup path."""
        service = self._require_service()
        if args.paths:
            logger.info(
                f"Restoring specific paths from local backup: {args.local_path}"
            )
            return await service.selective_restore_local_backup(
                args.local_path, args.paths, restore_path
            )
        logger.info(f"Restoring from local backup: {args.local_path}")
        return await self.run_restore_local(args.local_path, restore_path)

    async def _restore_from_revision(
        self, args: argparse.Namespace, restore_path: Path, config: BackupRestoreConfig
    ) -> bool:
        """Handle restore from revision number."""
        service = self._require_service()
        if args.paths:
            logger.info(f"Restoring specific paths from revision {args.revision}")
            return await service.selective_restore_from_drive_by_revision(
                args.revision, args.paths, restore_path, config
            )
        logger.info(f"Restoring backup from revision {args.revision}")
        return await self.run_restore_by_revision(args.revision, restore_path, config)

    async def _execute_restore(
        self, args: argparse.Namespace, restore_path: Path, config: BackupRestoreConfig
    ) -> RestoreExecuteResult:
        """Execute the appropriate restore operation."""
        # Handle modes that don't support selective restoration
        if args.latest_local or args.interactive:
            if args.paths:
                mode = "latest-local" if args.latest_local else "interactive"
                logger.warning(f"{mode} mode doesn't support selective restoration.")

            if args.latest_local:
                logger.info("Restoring latest local backup")
                result = await self.run_restore_latest_local(restore_path)
            else:
                logger.info("Starting interactive backup selection")
                result = await self.run_interactive_selection(config, restore_path)
            return RestoreExecuteResult(success=result, handled=True)

        # Handle modes that support selective restoration
        if args.file_id:
            success = await self._restore_from_file_id(args, restore_path, config)
            return RestoreExecuteResult(success=success, handled=True)
        if args.local_path:
            success = await self._restore_from_local_path(args, restore_path)
            return RestoreExecuteResult(success=success, handled=True)
        if args.revision is not None:
            success = await self._restore_from_revision(args, restore_path, config)
            return RestoreExecuteResult(success=success, handled=True)
        if args.list_revisions:
            logger.info("Listing available backup revisions")
            success = await self.run_list_revisions(config)
            return RestoreExecuteResult(success=success, handled=True)

        return RestoreExecuteResult(success=False, handled=False)

    async def _run_wizard(self, config: BackupRestoreConfig, restore_path: Path) -> int:
        """Launch the interactive restore wizard."""
        service = self._require_service()
        if not self.revisions_client:
            logger.error("Revisions client not available")
            return 1

        wizard = RestoreWizard(service, self.revisions_client, config, restore_path)
        try:
            success = await wizard.run()
        except KeyboardInterrupt:
            logger.info("\nWizard cancelled by user")
            return 1

        if success:
            self._log_success(restore_path, None)
            return 0
        logger.error("Restore wizard failed or was cancelled")
        return 1

    def _log_success(self, restore_path: Path, paths: list[Path] | None) -> None:
        """Log successful restore completion."""
        logger.info("=" * 60)
        logger.info("✓ Restore completed successfully")
        logger.info(f"  Restored to: {restore_path.absolute()}")
        if paths:
            logger.info(f"  Specific paths restored: {[str(p) for p in paths]}")
        logger.info("=" * 60)

    async def run(self, args: argparse.Namespace) -> int:
        """Main execution method."""
        self._setup_logging(args.verbose)

        config = BackupRestoreConfig.load(args.config_path)
        restore_path = args.restore_path or config.restore_path

        service = self._require_service()
        if not await service.validate_restore_path(restore_path):
            logger.error(f"Invalid restore path: {restore_path}")
            return 1

        try:
            execute_result = await self._execute_restore(args, restore_path, config)
        except (KeyboardInterrupt, Exception) as e:
            msg = (
                "\nOperation cancelled by user"
                if isinstance(e, KeyboardInterrupt)
                else f"Restore failed with error: {e}"
            )
            logger.error(msg)
            return 1

        if not execute_result.handled:
            if args.paths:
                logger.error(
                    "Paths specified but no source. "
                    "Use --file-id, --local-path, or --revision."
                )
                return 1
            return await self._run_wizard(config, restore_path)

        if not execute_result.success:
            logger.error("Restore failed")
            return 1

        self._log_success(restore_path, args.paths)
        return 0


# Note: The main entry point is in backup/restore/main.py
# This file provides the CLI class that main.py uses
