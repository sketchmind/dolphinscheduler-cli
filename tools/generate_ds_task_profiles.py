"""Generate publishable exact-version task profiles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ds_codegen.task_profiles import (
    DEFAULT_TASK_PROFILE_FACTS,
    DEFAULT_TASK_PROFILE_REVIEWS,
    GENERATED_TASK_PROFILE_PATH,
    compile_task_profile_data,
    load_task_profile_document,
    project_task_profile_facts,
    render_task_profile_data,
    write_task_profile_data,
    write_task_profile_facts,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TASK_PLUGIN_INVENTORY = Path("build/ds_task_plugins/all-version-inventory.json")


def build_parser() -> argparse.ArgumentParser:
    """Build the task-profile generator argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Compile exact task-plugin facts and reviewed typed-authoring "
            "decisions into the runtime task-profile module."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=ROOT,
        help="dolphinscheduler-cli repository root",
    )
    parser.add_argument(
        "--facts",
        type=Path,
        default=DEFAULT_TASK_PROFILE_FACTS,
        help="tracked exact task-profile facts JSON",
    )
    parser.add_argument(
        "--reviews",
        type=Path,
        default=DEFAULT_TASK_PROFILE_REVIEWS,
        help="tracked typed-authoring review ledger JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=GENERATED_TASK_PROFILE_PATH,
        help="generated runtime Python module",
    )
    parser.add_argument(
        "--refresh-facts",
        action="store_true",
        help="refresh tracked facts from the complete exact inventory first",
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        default=DEFAULT_TASK_PLUGIN_INVENTORY,
        help="complete exact task-plugin inventory used with --refresh-facts",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify tracked facts and generated output without writing",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Refresh facts, generate profiles, or verify generated freshness."""
    args = build_parser().parse_args(argv)
    try:
        repo_root = args.repo_root.resolve()
        facts_path = _resolve(repo_root, args.facts)
        reviews_path = _resolve(repo_root, args.reviews)
        output_path = _resolve(repo_root, args.output)
        if args.refresh_facts:
            inventory_path = _resolve(repo_root, args.inventory)
            inventory = load_task_profile_document(inventory_path)
            projected = project_task_profile_facts(inventory)
            if args.check:
                tracked = load_task_profile_document(facts_path)
                if tracked != projected:
                    print(
                        f"stale task-profile facts: {facts_path}",
                        file=sys.stderr,
                    )
                    return 1
                facts = tracked
            else:
                facts = write_task_profile_facts(facts_path, inventory)
        else:
            facts = load_task_profile_document(facts_path)

        reviews = load_task_profile_document(reviews_path)
        data = compile_task_profile_data(facts, reviews)
        rendered = render_task_profile_data(data)
        if args.check:
            if (
                not output_path.is_file()
                or output_path.read_text(encoding="utf-8") != rendered
            ):
                print(f"stale generated task profiles: {output_path}", file=sys.stderr)
                return 1
            return 0
        write_task_profile_data(
            output_path,
            facts=facts,
            reviews=reviews,
        )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"generated task profiles in {output_path}")
    return 0


def _resolve(repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    return repo_root / path


if __name__ == "__main__":
    raise SystemExit(main())
