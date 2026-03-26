"""
Non-interactive revision list display.

Formats and prints a colorized table of file revisions.
"""

from __future__ import annotations

from ami.cli_components.format_utils import format_file_size
from ami.cli_components.text_input_utils import Colors
from ami.dataops.backup.types import DriveRevisionInfo

BOX_WIDTH = 78


def display_revision_list(file_name: str, revisions: list[DriveRevisionInfo]) -> None:
    """Display a colorized list of file revisions.

    Args:
        file_name: Name of the backup file
        revisions: List of revision metadata (newest first)
    """
    if not revisions:
        print(f"{Colors.YELLOW}No revisions found.{Colors.RESET}")
        return

    border = Colors.CYAN
    title = f"Revisions for {file_name}"

    print(f"\n{border}{'─' * BOX_WIDTH}{Colors.RESET}")
    print(f"  {title:^{BOX_WIDTH - 4}}")
    print(f"{border}{'─' * BOX_WIDTH}{Colors.RESET}")

    for i, rev in enumerate(revisions):
        modified = rev.get("modifiedTime", "Unknown")
        size = rev.get("size", "Unknown")
        rev_id = rev.get("id", "")
        kept = rev.get("keepForever", False)

        size_str = format_file_size(size)
        label = "Latest" if i == 0 else f"Rev ~{i}"

        kept_marker = f" {Colors.GREEN}[kept]{Colors.RESET}" if kept else ""

        print(
            f"{Colors.GREEN}{label:>8}{Colors.RESET}  "
            f"{Colors.YELLOW}Date:{Colors.RESET} {modified}  "
            f"{Colors.YELLOW}Size:{Colors.RESET} {size_str}"
            f"{kept_marker}"
        )
        print(f"          {Colors.YELLOW}ID:{Colors.RESET} {rev_id}")

    print(f"{border}{'─' * BOX_WIDTH}{Colors.RESET}")
    count = len(revisions)
    print(f"  {count} revision(s) available\n")
