from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_app_import_does_not_initialize_unselected_version_adapters() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from dsctl.app import app; assert app is not None",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
