"""Verify that generated code in src/ matches a fresh codegen run.

This quality gate ensures generated packages are only modified through the
generation pipeline, never by hand.  It works by:

1. Running the package generator into a temporary directory.
2. Comparing every generated version package in the temp output against
   src/dsctl/generated/.
3. Failing with a clear diff if any file diverges.

Usage:
    python tools/check_generated_freshness.py

Exit codes:
    0 — generated code is fresh.
    1 — generated code has hand-edits or is out of date.
    2 — generated code has not been copied into src/ yet (skip check).
"""

from __future__ import annotations

import filecmp
import shutil
import sys
import tempfile
from pathlib import Path

from ds_codegen.api import (
    build_contract_snapshot,
    write_generated_package,
)

ROOT = Path(__file__).resolve().parents[1]
SRC_GENERATED = ROOT / "src" / "dsctl" / "generated" / "versions"
CACHE_ROOT = ROOT / "build" / "ds_contract" / ".freshness_cache"
CACHE_OUTPUT = CACHE_ROOT / "fresh"
CACHE_STAMP = CACHE_ROOT / ".stamp"
PYTHON = ROOT / ".venv" / "bin" / "python"
INPUT_ROOTS = (
    ROOT / "tools" / "ds_codegen",
    ROOT / "tools" / "generate_ds_contract.py",
    ROOT / "tools" / "check_generated_freshness.py",
    ROOT / "references" / "dolphinscheduler",
)
IGNORED_INPUT_DIRS = {"__pycache__", "target", ".git"}


def _find_version_dirs(base: Path) -> list[Path]:
    """Return version package dirs (e.g. ds_3_4_1/) that contain code."""
    if not base.is_dir():
        return []
    return [
        d
        for d in sorted(base.iterdir())
        if d.is_dir() and not d.name.startswith(("_", "."))
    ]


def _iter_input_files() -> list[Path]:
    """Return codegen input files whose mtimes invalidate the cache."""
    files: list[Path] = []
    for root in INPUT_ROOTS:
        if root.is_file():
            files.append(root)
            continue
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in IGNORED_INPUT_DIRS for part in path.parts):
                continue
            files.append(path)
    return files


def _latest_input_mtime_ns() -> int:
    """Return the newest input mtime used to invalidate cached output."""
    latest = 0
    for path in _iter_input_files():
        latest = max(latest, path.stat().st_mtime_ns)
    return latest


def _cache_is_fresh() -> bool:
    """Return whether the cached fresh output is newer than all inputs."""
    if not CACHE_STAMP.exists():
        return False
    cached_versions = CACHE_OUTPUT / "generated" / "versions"
    if not _find_version_dirs(cached_versions):
        return False
    return CACHE_STAMP.stat().st_mtime_ns >= _latest_input_mtime_ns()


def _materialize_fresh_output() -> Path:
    """Return the output root containing fresh generated package output."""
    if _cache_is_fresh():
        return CACHE_OUTPUT

    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    tmp_root = Path(tempfile.mkdtemp(prefix="freshness_cache_", dir=CACHE_ROOT))
    tmp_output = tmp_root / "fresh"
    try:
        snapshot = build_contract_snapshot(ROOT)
        write_generated_package(ROOT, snapshot, tmp_output)
        if CACHE_OUTPUT.exists():
            shutil.rmtree(CACHE_OUTPUT)
        tmp_output.replace(CACHE_OUTPUT)
        CACHE_STAMP.touch()
    except Exception:
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise
    else:
        shutil.rmtree(tmp_root, ignore_errors=True)
    return CACHE_OUTPUT


def _compare_trees(src: Path, ref: Path) -> list[str]:
    """Recursively compare two directory trees. Return list of differences."""
    diffs: list[str] = []

    cmp = filecmp.dircmp(str(src), str(ref))
    _collect_diffs(cmp, diffs, rel=Path())
    return diffs


def _collect_diffs(cmp: filecmp.dircmp[str], diffs: list[str], rel: Path) -> None:
    diffs.extend(f"  hand-added:   {rel / name}" for name in sorted(cmp.left_only))
    diffs.extend(f"  missing:      {rel / name}" for name in sorted(cmp.right_only))
    diffs.extend(f"  modified:     {rel / name}" for name in sorted(cmp.diff_files))
    for sub_name, sub_cmp in sorted(cmp.subdirs.items()):
        _collect_diffs(sub_cmp, diffs, rel / sub_name)


def main() -> int:
    # --- Guard: skip if generated code hasn't been copied to src/ yet ---
    version_dirs = _find_version_dirs(SRC_GENERATED)
    if not version_dirs:
        print("check_generated_freshness: no version packages in src/, skipping.")
        return 0

    command_hint = str(PYTHON) if PYTHON.exists() else sys.executable

    try:
        fresh_output = _materialize_fresh_output()
    except Exception as exc:
        print("check_generated_freshness: generator failed:")
        print(str(exc))
        return 2

    # --- Compare each version package ---
    fresh_versions = fresh_output / "generated" / "versions"
    all_diffs: list[str] = []

    for src_version in version_dirs:
        slug = src_version.name
        ref_version = fresh_versions / slug
        if not ref_version.is_dir():
            all_diffs.append(f"  version {slug} exists in src/ but not in fresh output")
            continue
        diffs = _compare_trees(src_version, ref_version)
        if diffs:
            all_diffs.append(f"src/dsctl/generated/versions/{slug}/:")
            all_diffs.extend(diffs)

    if all_diffs:
        print("check_generated_freshness: FAILED — generated code has diverged.")
        print()
        print("The following files in src/dsctl/generated/ do not match a fresh")
        print("codegen run. Generated code must only be changed by re-running:")
        print(f"  {command_hint} tools/generate_ds_contract.py --package-output <dir>")
        print("`--package-output` only writes src/dsctl/generated/... style output.")
        print()
        print("\n".join(all_diffs))
        return 1

    print("check_generated_freshness: OK — generated code is fresh.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
