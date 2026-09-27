"""Prepare exact upstream source checkouts declared by the runtime manifest."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

from runtime_bundle_manifest import (
    DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    RUNTIME_BUNDLE_SELECTIONS,
    RuntimeBundleSpec,
    load_runtime_bundle_specs,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DS_REMOTE = "https://github.com/apache/dolphinscheduler.git"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare exact DolphinScheduler sources from the bundle manifest"
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=ROOT,
        help="dolphinscheduler-cli repository root",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_RUNTIME_BUNDLE_MANIFEST,
        help="runtime bundle manifest JSON",
    )
    parser.add_argument(
        "--selection",
        action="append",
        choices=sorted(RUNTIME_BUNDLE_SELECTIONS),
        help="prepare only this bundle selection; repeat to select both",
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_DS_REMOTE,
        help="upstream Git remote containing the exact release tags",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    manifest_path = _resolve(repo_root, args.manifest)
    selections = frozenset(args.selection or RUNTIME_BUNDLE_SELECTIONS)
    specs = tuple(
        spec
        for spec in load_runtime_bundle_specs(repo_root, manifest_path)
        if spec.selection in selections
    )
    if not specs:
        requested = ", ".join(sorted(selections))
        message = f"runtime bundle manifest has no bundles selected by: {requested}"
        raise ValueError(message)

    for spec in specs:
        _prepare_source(spec, remote=args.remote)
    print(f"prepared {len(specs)} exact runtime bundle source(s)")
    return 0


def _resolve(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def _prepare_source(spec: RuntimeBundleSpec, *, remote: str) -> None:
    if spec.source_root.exists():
        _require_exact_checkout(spec)
        print(f"reused DolphinScheduler {spec.version}: {spec.source_root}")
        return
    spec.source_root.parent.mkdir(parents=True, exist_ok=True)
    _run_git(
        "clone",
        "--depth",
        "1",
        "--branch",
        spec.version,
        "--single-branch",
        remote,
        str(spec.source_root),
    )
    _require_exact_checkout(spec)
    print(f"prepared DolphinScheduler {spec.version}: {spec.source_root}")


def _require_exact_checkout(spec: RuntimeBundleSpec) -> None:
    if not spec.source_root.is_dir():
        message = f"runtime source path is not a directory: {spec.source_root}"
        raise RuntimeError(message)
    top_level = _run_git(
        "-C",
        str(spec.source_root),
        "rev-parse",
        "--show-toplevel",
    )
    if Path(top_level).resolve() != spec.source_root.resolve():
        message = f"runtime source is not a Git worktree root: {spec.source_root}"
        raise RuntimeError(message)
    tags = {
        tag.strip()
        for tag in _run_git(
            "-C",
            str(spec.source_root),
            "tag",
            "--points-at",
            "HEAD",
        ).splitlines()
        if tag.strip()
    }
    if spec.version not in tags:
        actual = ", ".join(sorted(tags)) or "none"
        message = (
            f"runtime source {spec.source_root} does not point at expected tag "
            f"{spec.version!r}; tags at HEAD: {actual}"
        )
        raise RuntimeError(message)
    dirty = _run_git(
        "-C",
        str(spec.source_root),
        "status",
        "--porcelain",
        "--untracked-files=normal",
    )
    if dirty:
        message = f"runtime source checkout is dirty: {spec.source_root}"
        raise RuntimeError(message)


def _run_git(*args: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        message = "git is required to prepare DolphinScheduler sources"
        raise RuntimeError(message)
    completed = subprocess.run(  # noqa: S603
        [executable, *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip()
        message = f"git {' '.join(args)} failed: {details}"
        raise RuntimeError(message)
    return completed.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
