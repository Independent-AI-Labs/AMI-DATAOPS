"""Entry point for the `ami-intake` extension."""

from __future__ import annotations

import sys

from ami.dataops.intake.cli import main

if __name__ == "__main__":
    sys.exit(main())
