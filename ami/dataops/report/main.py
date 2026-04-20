"""Entry point for the `ami-report` extension."""

from __future__ import annotations

import sys

from ami.dataops.report.cli import main

if __name__ == "__main__":
    sys.exit(main())
