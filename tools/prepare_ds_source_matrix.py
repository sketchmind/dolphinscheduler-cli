"""Prepare every exact upstream checkout used by mechanical analyzers."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

from ds_codegen.contract_inputs import require_exact_git_source_identity
from ds_codegen.source_matrix import (
    DEFAULT_EXACT_SOURCE_MANIFEST,
    ExactSourceSpec,
    load_exact_source_specs,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DS_REMOTE = "https://github.com/apache/dolphinscheduler.git"


def build_parser() -> argparse.ArgumentParser:
    """Build the exact source-matrix preparation parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Prepare the complete clean exact-tag DolphinScheduler source matrix "
            "used by mechanical compatibility analyzers."
        )
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
        default=DEFAULT_EXACT_SOURCE_MANIFEST,
        help="exact source matrix JSON",
    )
    parser.add_argument(
        "--remote",
        default=DEFAULT_DS_REMOTE,
        help="upstream Git remote containing every exact release tag",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Create missing checkouts and reject any inexact existing source root."""
    args = build_parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    manifest_path = _resolve(repo_root, args.manifest)
    specs = load_exact_source_specs(repo_root, manifest_path)
    for spec in specs:
        _prepare_source(spec, remote=args.remote)
    print(f"prepared {len(specs)} exact compatibility source(s)")
    return 0


def _resolve(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def _prepare_source(spec: ExactSourceSpec, *, remote: str) -> None:
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


def _require_exact_checkout(spec: ExactSourceSpec) -> None:
    try:
        require_exact_git_source_identity(spec.source_root, label=spec.version)
    except ValueError as exc:
        message = f"invalid exact source for DolphinScheduler {spec.version}: {exc}"
        raise RuntimeError(message) from exc


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
