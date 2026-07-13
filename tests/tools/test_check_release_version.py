from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "tools" / "check_release_version.py"


def write_version_files(
    root: Path,
    *,
    project_version: str = "0.3.0",
    runtime_version: str = "0.3.0",
) -> None:
    (root / "src" / "dsctl").mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "dolphinscheduler-cli"\nversion = "{project_version}"\n',
        encoding="utf-8",
    )
    (root / "src" / "dsctl" / "__init__.py").write_text(
        f'__version__ = "{runtime_version}"\n',
        encoding="utf-8",
    )


def run_version_check(
    root: Path, *, tag: str | None = None
) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, str(SCRIPT), "--root", str(root)]
    if tag is not None:
        command.extend(("--tag", tag))
    return subprocess.run(  # noqa: S603
        command,
        check=False,
        capture_output=True,
        text=True,
    )


def test_check_release_version_accepts_matching_source_and_tag(tmp_path: Path) -> None:
    write_version_files(tmp_path)

    result = run_version_check(tmp_path, tag="v0.3.0")

    assert result.returncode == 0
    assert result.stdout == "release version check passed: 0.3.0\n"
    assert result.stderr == ""


def test_check_release_version_rejects_runtime_mismatch(tmp_path: Path) -> None:
    write_version_files(tmp_path, runtime_version="0.2.0")

    result = run_version_check(tmp_path)

    assert result.returncode == 1
    assert "version mismatch" in result.stderr


def test_check_release_version_rejects_tag_mismatch(tmp_path: Path) -> None:
    write_version_files(tmp_path)

    result = run_version_check(tmp_path, tag="v0.2.0")

    assert result.returncode == 1
    assert "release tag mismatch" in result.stderr


def test_check_release_version_ignores_empty_dispatch_tag(tmp_path: Path) -> None:
    write_version_files(tmp_path)

    result = run_version_check(tmp_path, tag="")

    assert result.returncode == 0
