"""Verify that generated code in src/ matches a fresh codegen run.

This quality gate ensures generated packages are only modified through the
generation pipeline, never by hand.  It works by:

1. Running the package generator into a temporary directory.
2. Comparing every generated version package and namespace initializer in the
   temp output against src/dsctl/generated/.
3. Failing with a clear diff if any file diverges.

Usage:
    python tools/check_generated_freshness.py

Exit codes:
    0 — generated code is fresh.
    1 — generated code has hand-edits or is out of date.
    2 — the generator failed before freshness could be checked.
"""

from __future__ import annotations

import hashlib
import shutil
import stat
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
IGNORED_GENERATED_ENTRIES = frozenset({"__pycache__"})


def _find_version_dirs(base: Path) -> list[Path]:
    """Return version package dirs (e.g. ds_3_4_1/) that contain code."""
    if not base.is_dir():
        return []
    return [
        d
        for d in sorted(base.iterdir())
        if d.is_dir() and not d.name.startswith(("_", "."))
    ]


def _iter_input_entries() -> list[tuple[str, Path]]:
    """Return deterministic codegen input paths for cache fingerprinting."""
    entries: list[tuple[str, Path]] = []
    for root in INPUT_ROOTS:
        root_label = _input_path_label(root)
        entries.append((root_label, root))
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            relative_path = path.relative_to(root)
            if any(part in IGNORED_INPUT_DIRS for part in relative_path.parts):
                continue
            entries.append((f"{root_label}/{relative_path.as_posix()}", path))
    return sorted(entries, key=lambda item: item[0])


def _input_path_label(path: Path) -> str:
    """Return one checkout-independent label when the input is under ROOT."""
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.absolute().as_posix()


def _input_fingerprint() -> str:
    """Hash input paths, entry kinds, and contents into one cache key."""
    digest = hashlib.sha256()
    for label, path in _iter_input_entries():
        kind, content_digest = _input_entry_signature(path)
        _update_digest(digest, label)
        _update_digest(digest, kind)
        _update_digest(digest, content_digest)
    return digest.hexdigest()


def _input_entry_signature(path: Path) -> tuple[str, str]:
    """Return the identity that codegen observes, following file symlinks."""
    kind, identity = _entry_signature(path)
    if kind != "symlink":
        return (kind, identity)
    if path.is_file():
        return ("symlink-file", f"{identity}\0{_file_digest(path)}")
    if path.is_dir():
        return ("symlink-directory", identity)
    return (kind, identity)


def _update_digest(digest: hashlib._Hash, value: str) -> None:
    encoded = value.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, byteorder="big"))
    digest.update(encoded)


def _entry_signature(path: Path) -> tuple[str, str]:
    """Return an entry kind plus content identity without following symlinks."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return ("missing", "")
    if stat.S_ISLNK(mode):
        return ("symlink", str(path.readlink()))
    if stat.S_ISDIR(mode):
        return ("directory", "")
    if stat.S_ISREG(mode):
        return ("file", _file_digest(path))
    return ("other", str(mode))


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while chunk := file_handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _cache_is_fresh(input_fingerprint: str) -> bool:
    """Return whether cached output was generated from the same inputs."""
    if not CACHE_STAMP.exists():
        return False
    cached_versions = CACHE_OUTPUT / "generated" / "versions"
    if not _find_version_dirs(cached_versions):
        return False
    try:
        cached_fingerprint = CACHE_STAMP.read_text(encoding="utf-8").strip()
    except OSError:
        return False
    return cached_fingerprint == input_fingerprint


def _materialize_fresh_output() -> Path:
    """Return the output root containing fresh generated package output."""
    input_fingerprint = _input_fingerprint()
    if _cache_is_fresh(input_fingerprint):
        return CACHE_OUTPUT

    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    tmp_root = Path(tempfile.mkdtemp(prefix="freshness_cache_", dir=CACHE_ROOT))
    tmp_output = tmp_root / "fresh"
    try:
        snapshot = build_contract_snapshot(ROOT)
        write_generated_package(ROOT, snapshot, tmp_output)
        _require_unchanged_inputs(input_fingerprint)
        if CACHE_OUTPUT.exists():
            shutil.rmtree(CACHE_OUTPUT)
        tmp_output.replace(CACHE_OUTPUT)
        CACHE_STAMP.write_text(f"{input_fingerprint}\n", encoding="utf-8")
    except Exception:
        shutil.rmtree(tmp_root, ignore_errors=True)
        raise
    else:
        shutil.rmtree(tmp_root, ignore_errors=True)
    return CACHE_OUTPUT


def _require_unchanged_inputs(expected_fingerprint: str) -> None:
    if _input_fingerprint() == expected_fingerprint:
        return
    message = "Codegen inputs changed while freshness output was generated"
    raise RuntimeError(message)


def _compare_trees(src: Path, ref: Path) -> list[str]:
    """Recursively compare two directory trees. Return list of differences."""
    src_entries = _tree_entries(src)
    ref_entries = _tree_entries(ref)
    src_paths = set(src_entries)
    ref_paths = set(ref_entries)
    diffs = [
        f"  hand-added:   {Path('generated') / path}"
        for path in sorted(src_paths - ref_paths)
    ]
    diffs.extend(
        f"  missing:      {Path('generated') / path}"
        for path in sorted(ref_paths - src_paths)
    )
    for path in sorted(src_paths & ref_paths):
        src_kind, src_identity = src_entries[path]
        ref_kind, ref_identity = ref_entries[path]
        rendered_path = Path("generated") / path
        if src_kind != ref_kind:
            diffs.append(f"  type-changed: {rendered_path}")
        elif src_identity != ref_identity:
            diffs.append(f"  modified:     {rendered_path}")
    return diffs


def _tree_entries(root: Path) -> dict[Path, tuple[str, str]]:
    entries: dict[Path, tuple[str, str]] = {}

    def visit(directory: Path, relative_directory: Path) -> None:
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.name in IGNORED_GENERATED_ENTRIES:
                continue
            relative_path = relative_directory / path.name
            signature = _entry_signature(path)
            entries[relative_path] = signature
            if signature[0] == "directory":
                visit(path, relative_path)

    visit(root, Path())
    return entries


def main() -> int:
    # --- Guard: committed generated code must contain at least one version. ---
    version_dirs = _find_version_dirs(SRC_GENERATED)
    if not version_dirs:
        print("check_generated_freshness: FAILED — no version packages in src/.")
        return 1

    command_hint = str(PYTHON) if PYTHON.exists() else sys.executable

    try:
        fresh_output = _materialize_fresh_output()
    except Exception as exc:
        print("check_generated_freshness: generator failed:")
        print(str(exc))
        return 2

    # --- Compare the complete generated namespace symmetrically. ---
    fresh_versions = fresh_output / "generated" / "versions"
    all_diffs = _compare_trees(SRC_GENERATED.parent, fresh_versions.parent)

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
