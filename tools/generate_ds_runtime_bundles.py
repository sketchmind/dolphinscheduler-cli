"""Generate the complete tracked set of runtime wire bundles atomically."""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

from ds_codegen.runtime_bundles import (
    DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    DEFAULT_RUNTIME_SNAPSHOT_DIR,
    RUNTIME_SNAPSHOT_MODES,
    load_runtime_bundles,
    render_runtime_bundles,
    require_runtime_bundle_sources_unchanged,
)

ROOT = Path(__file__).resolve().parents[1]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate all exact-version DolphinScheduler runtime bundles"
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
        "--output-root",
        type=Path,
        default=Path("src/dsctl"),
        help="root that owns the generated/ namespace",
    )
    parser.add_argument(
        "--snapshot-mode",
        choices=sorted(RUNTIME_SNAPSHOT_MODES),
        default="prefer",
        help=(
            "contract input policy: prefer a verified snapshot and fall back to "
            "exact source (default), require a verified snapshot, or use source only"
        ),
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_RUNTIME_SNAPSHOT_DIR,
        help="exact contract snapshot cache directory",
    )
    return parser


def _resolve(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    manifest_path = _resolve(repo_root, args.manifest)
    output_root = _resolve(repo_root, args.output_root)
    snapshot_dir = _resolve(repo_root, args.snapshot_dir)

    # Validate and compile every exact source before creating any output.
    bundles = load_runtime_bundles(
        repo_root,
        manifest_path,
        snapshot_dir=snapshot_dir,
        snapshot_mode=args.snapshot_mode,
    )
    _report_snapshot_fallbacks(bundles)
    output_root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="dsctl_runtime_bundles_",
        dir=output_root.parent,
    ) as temporary_directory:
        staging_root = Path(temporary_directory) / "output"
        render_runtime_bundles(bundles, staging_root)
        require_runtime_bundle_sources_unchanged(bundles)
        _replace_generated_namespace(staging_root, output_root)

    snapshot_count = sum(
        getattr(bundle, "input_kind", "source") == "snapshot" for bundle in bundles
    )
    print(
        f"generated {len(bundles)} runtime bundles in {output_root / 'generated'} "
        f"(contract inputs: {snapshot_count} snapshots, "
        f"{len(bundles) - snapshot_count} exact sources; "
        f"snapshot mode: {args.snapshot_mode})"
    )
    return 0


def _report_snapshot_fallbacks(bundles: tuple[object, ...]) -> None:
    for bundle in bundles:
        reason = getattr(bundle, "snapshot_fallback_reason", None)
        if not isinstance(reason, str):
            continue
        spec = getattr(bundle, "spec", None)
        version = getattr(spec, "version", "unknown")
        print(
            f"snapshot fallback for DS {version}: {reason}",
            file=sys.stderr,
        )


def _replace_generated_namespace(staging_root: Path, output_root: Path) -> None:
    staged_generated = staging_root / "generated"
    if not staged_generated.is_dir():
        message = "runtime bundle renderer produced no generated namespace"
        raise RuntimeError(message)

    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / "generated"
    backup = staging_root.parent / "previous_generated"
    had_target = target.exists()
    if had_target:
        target.replace(backup)
    try:
        staged_generated.replace(target)
    except Exception:
        if had_target and backup.exists() and not target.exists():
            backup.replace(target)
        raise
    if backup.exists():
        shutil.rmtree(backup)


if __name__ == "__main__":
    raise SystemExit(main())
