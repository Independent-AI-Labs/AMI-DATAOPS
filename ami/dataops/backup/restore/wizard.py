"""
Interactive restore wizard.

5-step wizard: select file, select revision, choose path, select paths, confirm.
"""

import tempfile
from pathlib import Path
from typing import NamedTuple, cast

from loguru import logger

from ami.cli_components.dialogs import confirm
from ami.cli_components.format_utils import format_file_size
from ami.cli_components.menu_selector import MenuItem, MenuSelector
from ami.cli_components.selector import (
    BackupFileInfo,
    select_backup_interactive,
)
from ami.cli_components.text_input_utils import Colors
from ami.cli_components.tui import TUI, BoxStyle
from ami.dataops.backup.core.config import BackupRestoreConfig
from ami.dataops.backup.restore.extractor import extract_specific_paths
from ami.dataops.backup.restore.revisions_client import RevisionsClient
from ami.dataops.backup.restore.service import BackupRestoreService
from ami.dataops.backup.types import DriveRevisionInfo

WIZARD_TOTAL_STEPS = 5


class FileSelection(NamedTuple):
    """Result from backup file selection step."""

    file_id: str
    file_name: str


class RestoreWizard:
    """Interactive 5-step restore wizard."""

    def __init__(
        self,
        service: BackupRestoreService,
        revisions_client: RevisionsClient,
        config: BackupRestoreConfig,
        default_restore_path: Path,
    ) -> None:
        self.service = service
        self.revisions_client = revisions_client
        self.config = config
        self.default_restore_path = default_restore_path

    async def run(self) -> bool:
        """Run the full interactive restore wizard.

        Returns:
            True if restore completed successfully
        """
        logger.info("Starting interactive restore wizard...")

        # Step 1: Select backup file
        selection = await self._select_backup_file()
        if selection is None:
            logger.info("Wizard cancelled at file selection")
            return False
        file_id, file_name = selection

        # Step 2: Select revision
        revision = await self._select_revision(file_id)
        if revision is None:
            logger.info("Wizard cancelled at revision selection")
            return False

        # Step 3: Choose restore path
        restore_path = self._choose_restore_path()
        if restore_path is None:
            logger.info("Wizard cancelled at path selection")
            return False

        # Step 4: Select paths to restore
        paths = self._select_paths()

        # Step 5: Confirm
        if not self._confirm_restore(file_name, revision, restore_path, paths):
            logger.info("Restore cancelled by user")
            return False

        # Execute
        return await self._execute_restore(file_id, revision, restore_path, paths)

    async def _select_backup_file(self) -> FileSelection | None:
        """Step 1: Select a backup file from Google Drive.

        Returns:
            FileSelection or None if cancelled
        """
        self._print_step_header(1, "Select Backup File")
        logger.info("Fetching backup files from Google Drive...")

        backup_files = await self.service.list_available_drive_backups(self.config)
        if not backup_files:
            logger.error("No backup files found in Google Drive")
            return None

        # DriveFileMetadata is structurally identical to BackupFileInfo
        files_info = cast(list[BackupFileInfo], backup_files)
        file_id = select_backup_interactive(files_info)
        if file_id is None:
            return None

        # Find the name for the selected file
        file_name = "Unknown"
        for f in backup_files:
            if f.get("id") == file_id:
                file_name = f.get("name", "Unknown")
                break

        return FileSelection(file_id, file_name)

    async def _select_revision(self, file_id: str) -> DriveRevisionInfo | None:
        """Step 2: Select a revision of the backup file.

        Args:
            file_id: Google Drive file ID

        Returns:
            Selected revision info or None if cancelled
        """
        self._print_step_header(2, "Select Revision")
        logger.info("Fetching revision history...")

        revisions = await self.revisions_client.list_revisions(file_id)

        if not revisions:
            logger.warning("No revision history available")
            logger.info("Will restore the current version")
            return DriveRevisionInfo(
                id="head",
                modifiedTime="current",
                size="",
            )

        if len(revisions) == 1:
            rev = revisions[0]
            modified = rev.get("modifiedTime", "Unknown")
            logger.info(f"Only 1 revision available ({modified})")
            return rev

        # Build menu items for revision selection
        menu_items: list[MenuItem[DriveRevisionInfo]] = []
        for i, rev in enumerate(revisions):
            modified = rev.get("modifiedTime", "Unknown")
            size_str = format_file_size(rev.get("size", ""))
            label = "Latest" if i == 0 else f"Revision ~{i}"
            desc = f"Date: {modified} | Size: {size_str}"
            menu_items.append(MenuItem(str(i), label, rev, description=desc))

        selector: MenuSelector[DriveRevisionInfo] = MenuSelector(
            menu_items,
            "Select a revision to restore",
            max_visible_items=10,
        )
        selected = selector.run()

        if selected and len(selected) > 0:
            return cast(DriveRevisionInfo, selected[0].value)
        return None

    def _choose_restore_path(self) -> Path | None:
        """Step 3: Choose the restore destination path.

        Returns:
            Selected path or None if cancelled
        """
        self._print_step_header(3, "Choose Restore Path")
        default = self.default_restore_path

        msg = f"Restore to: {default.absolute()}"
        if confirm(msg, title="Restore Path"):
            return default

        # Offer alternatives
        alternatives = [
            str(default.absolute()),
            str(Path.cwd() / "_restored"),
            "Enter custom path",
        ]
        items: list[MenuItem[str]] = [
            MenuItem(str(i), alt, alt) for i, alt in enumerate(alternatives)
        ]
        selector: MenuSelector[str] = MenuSelector(items, "Select restore path")
        result = selector.run()

        if not result:
            return None

        chosen = str(result[0].value)
        if chosen == "Enter custom path":
            custom = input("Enter restore path: ").strip()
            if not custom:
                return None
            return Path(custom)

        return Path(chosen)

    def _select_paths(self) -> list[Path] | None:
        """Step 4: Select specific paths to restore.

        Returns:
            List of paths to restore, or None for all files
        """
        self._print_step_header(4, "Select Paths to Restore")

        if confirm("Restore all files?", title="Path Filter"):
            return None

        print("Enter paths to restore (one per line, blank line to finish):")
        paths: list[Path] = []
        while True:
            line = input("> ").strip()
            if not line:
                break
            paths.append(Path(line))

        if not paths:
            logger.info("No paths entered, restoring all files")
            return None

        logger.info(f"Selected {len(paths)} path(s) to restore")
        return paths

    def _confirm_restore(
        self,
        file_name: str,
        revision: DriveRevisionInfo,
        restore_path: Path,
        paths: list[Path] | None = None,
    ) -> bool:
        """Step 5: Show summary and confirm.

        Args:
            file_name: Backup file name
            revision: Selected revision info
            restore_path: Destination path
            paths: Specific paths to restore, or None for all

        Returns:
            True if user confirms
        """
        self._print_step_header(5, "Confirm Restore")

        modified = revision.get("modifiedTime", "Unknown")
        size_str = format_file_size(revision.get("size", ""))
        rev_id = revision.get("id", "")

        paths_display = "* (all files)" if paths is None else f"{len(paths)} selected"

        content = [
            f"{Colors.YELLOW}File:{Colors.RESET}     {file_name}",
            f"{Colors.YELLOW}Revision:{Colors.RESET} {modified}",
            f"{Colors.YELLOW}Size:{Colors.RESET}     {size_str}",
            f"{Colors.YELLOW}ID:{Colors.RESET}       {rev_id}",
            f"{Colors.YELLOW}Paths:{Colors.RESET}    {paths_display}",
        ]
        if paths:
            content.extend(f"  - {p}" for p in paths)
        content.append("")
        content.append(f"{Colors.YELLOW}Restore to:{Colors.RESET}")
        content.append(f"  {restore_path.absolute()}")

        TUI.draw_box(
            content=content,
            title="Restore Summary",
            style=BoxStyle(width=72),
        )

        result: bool = confirm("Proceed with restore?", title="Confirm Restore")
        return result

    async def _execute_restore(
        self,
        file_id: str,
        revision: DriveRevisionInfo,
        restore_path: Path,
        paths: list[Path] | None = None,
    ) -> bool:
        """Execute the actual restore operation.

        Args:
            file_id: Google Drive file ID
            revision: Selected revision info
            restore_path: Destination path
            paths: Specific paths to restore, or None for all

        Returns:
            True if restore was successful
        """
        rev_id = revision.get("id", "")
        is_head = rev_id in {"head", ""}

        if is_head:
            if paths is not None:
                return await self.service.selective_restore_from_drive_by_file_id(
                    file_id, paths, restore_path, self.config
                )
            return await self.service.restore_from_drive_by_file_id(
                file_id, restore_path, self.config
            )

        # Download specific revision to temp, then extract
        return await self._restore_specific_revision(
            file_id, rev_id, restore_path, paths
        )

    async def _restore_specific_revision(
        self,
        file_id: str,
        revision_id: str,
        restore_path: Path,
        paths: list[Path] | None = None,
    ) -> bool:
        """Download and extract a specific revision.

        Args:
            file_id: Google Drive file ID
            revision_id: Revision ID to download
            restore_path: Destination path
            paths: Specific paths to restore, or None for all

        Returns:
            True if successful
        """
        restore_path.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = Path(temp_dir) / "revision.tar.zst"

            logger.info("Downloading revision...")
            success = await self.revisions_client.download_revision(
                file_id, revision_id, archive_path
            )
            if not success:
                logger.error("Failed to download revision")
                return False

            logger.info(f"Extracting to: {restore_path}")
            try:
                return await extract_specific_paths(archive_path, paths, restore_path)
            except Exception as e:
                logger.error(f"Extraction failed: {e}")
                return False

    @staticmethod
    def _print_step_header(step: int, title: str) -> None:
        """Print a step header."""
        total = WIZARD_TOTAL_STEPS
        print(f"\n{Colors.CYAN}[Step {step}/{total}] {title}{Colors.RESET}")
