from __future__ import annotations

import argparse
from pathlib import Path

from audit_dsctl_error_translation import CodeReference, build_report
from extract_dsctl_error_translation_matrix import build_report as build_matrix_report

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST_PATH = ROOT / "tools" / "error_translation_allowlist.txt"


def current_entries() -> list[str]:
    report = build_report()
    matrix_report = build_matrix_report()
    raw_excepts = [
        f"except|{module.module}|{site.function_name or '<module>'}"
        for module in report.modules
        for site in module.api_result_error_excepts
        if not site.translator_calls
    ]
    missing_source_chain = [
        (f"source_chain|{module.module}|{site.function_name or '<module>'}|{site.line}")
        for module in report.modules
        for site in module.api_result_error_excepts
        if site.translator_calls and not site.preserves_source_chain
    ]
    raw_page_hooks = [
        f"page_hook|{module.module}|{hook.function_name or '<module>'}"
        for module in report.modules
        for hook in module.pagination_hooks
        if hook.translate_error_expr is None
    ]
    raw_matrix_outcomes = [
        (
            "raw_matrix|"
            f"{helper.module}|{helper.helper}|"
            f"{','.join(code_key(reference) for reference in mapping.codes)}"
        )
        for helper in matrix_report.helpers
        for mapping in helper.mappings
        if "ApiResultError" in mapping.outcomes
    ]
    return sorted(
        {
            *raw_excepts,
            *missing_source_chain,
            *raw_page_hooks,
            *raw_matrix_outcomes,
        }
    )


def load_entries(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def write_entries(path: Path, entries: list[str]) -> None:
    lines = [
        "# Reviewed error-translation governance allowlist.",
        "# Each line is one reviewed finding key.",
        *entries,
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def code_key(reference: CodeReference) -> str:
    if reference.name is not None:
        return reference.name
    value = reference.value
    if value is None:
        message = (
            "translation-matrix governance expected every code reference to have a key"
        )
        raise ValueError(message)
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write-allowlist",
        action="store_true",
        help="rewrite the reviewed allowlist to match current findings",
    )
    args = parser.parse_args()

    entries = current_entries()
    if args.write_allowlist:
        write_entries(ALLOWLIST_PATH, entries)
        print(f"wrote {len(entries)} governance entries to {ALLOWLIST_PATH}")
        return 0

    allowlist = load_entries(ALLOWLIST_PATH)
    current = set(entries)
    unexpected = sorted(current - allowlist)
    stale = sorted(allowlist - current)
    if not unexpected and not stale:
        print(
            f"error-translation governance passed with {len(entries)} reviewed findings"
        )
        return 0
    if unexpected:
        print("unexpected error-translation findings:")
        for entry in unexpected:
            print(f"  + {entry}")
    if stale:
        print("stale error-translation allowlist entries:")
        for entry in stale:
            print(f"  - {entry}")
    print(
        "review the findings and refresh the allowlist with "
        "`python tools/check_error_translation_governance.py --write-allowlist`"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
