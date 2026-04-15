from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCES_ROOT = ROOT / "references" / "dolphinscheduler"
API_SRC_ROOT = REFERENCES_ROOT / "dolphinscheduler-api" / "src" / "main" / "java"
STATUS_PATH = (
    API_SRC_ROOT
    / "org"
    / "apache"
    / "dolphinscheduler"
    / "api"
    / "enums"
    / "Status.java"
)

STATUS_ENTRY_PATTERN = re.compile(
    r"^\s*(?P<name>[A-Z0-9_]+)\("
    r"\s*(?P<code>\d+),\s*"
    r'"(?P<en>(?:[^"\\]|\\.)*)",\s*'
    r'"(?P<zh>(?:[^"\\]|\\.)*)"'
    r"\)",
    re.MULTILINE,
)
BARE_SERVICE_EXCEPTION_PATTERN = re.compile(
    r'new\s+ServiceException\(\s*"(?P<message>(?:[^"\\]|\\.)*)"',
)
DIRECT_HTTP_STATUS_PATTERN = re.compile(
    r"response\.setStatus\((?P<expr>[^;]+)\);",
)


@dataclass(frozen=True)
class StatusEntry:
    name: str
    code: int
    en_msg: str
    zh_msg: str


@dataclass(frozen=True)
class SourceFinding:
    path: str
    line: int
    snippet: str


@dataclass(frozen=True)
class InventoryReport:
    status_entries: list[StatusEntry]
    duplicate_codes: dict[int, list[StatusEntry]]
    bare_service_exceptions: list[SourceFinding]
    direct_http_status_sites: list[SourceFinding]


def extract_status_entries(source: str) -> list[StatusEntry]:
    return [
        StatusEntry(
            name=match.group("name"),
            code=int(match.group("code")),
            en_msg=match.group("en"),
            zh_msg=match.group("zh"),
        )
        for match in STATUS_ENTRY_PATTERN.finditer(source)
    ]


def find_duplicate_codes(entries: list[StatusEntry]) -> dict[int, list[StatusEntry]]:
    duplicates: dict[int, list[StatusEntry]] = defaultdict(list)
    for entry in entries:
        duplicates[entry.code].append(entry)
    return {
        code: values for code, values in sorted(duplicates.items()) if len(values) > 1
    }


def find_bare_service_exceptions(
    source: str,
    *,
    relative_path: str,
) -> list[SourceFinding]:
    return [
        SourceFinding(
            path=relative_path,
            line=_line_number(source, match.start()),
            snippet=_normalize_snippet(match.group(0)),
        )
        for match in BARE_SERVICE_EXCEPTION_PATTERN.finditer(source)
    ]


def find_direct_http_status_sites(
    source: str,
    *,
    relative_path: str,
) -> list[SourceFinding]:
    return [
        SourceFinding(
            path=relative_path,
            line=_line_number(source, match.start()),
            snippet=_normalize_snippet(match.group(0)),
        )
        for match in DIRECT_HTTP_STATUS_PATTERN.finditer(source)
    ]


def build_inventory() -> InventoryReport:
    status_entries = extract_status_entries(STATUS_PATH.read_text(encoding="utf-8"))
    bare_service_exceptions: list[SourceFinding] = []
    direct_http_status_sites: list[SourceFinding] = []

    for path in sorted(API_SRC_ROOT.rglob("*.java")):
        relative_path = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        bare_service_exceptions.extend(
            find_bare_service_exceptions(source, relative_path=relative_path)
        )
        direct_http_status_sites.extend(
            find_direct_http_status_sites(source, relative_path=relative_path)
        )

    return InventoryReport(
        status_entries=status_entries,
        duplicate_codes=find_duplicate_codes(status_entries),
        bare_service_exceptions=bare_service_exceptions,
        direct_http_status_sites=direct_http_status_sites,
    )


def render_summary(report: InventoryReport) -> str:
    duplicate_count = len(report.duplicate_codes)
    bare_count = len(report.bare_service_exceptions)
    http_count = len(report.direct_http_status_sites)
    lines = [
        "ds api error inventory",
        f"  status entries: {len(report.status_entries)}",
        f"  duplicate status codes: {duplicate_count}",
        f"  bare ServiceException sites: {bare_count}",
        f"  direct response.setStatus sites: {http_count}",
    ]
    if report.duplicate_codes:
        lines.append("  duplicate examples:")
        for code, entries in list(report.duplicate_codes.items())[:5]:
            names = ", ".join(entry.name for entry in entries)
            lines.append(f"    {code}: {names}")
    return "\n".join(lines)


def render_markdown(report: InventoryReport) -> str:
    lines = [
        "# DS API Error Inventory",
        "",
        "## Summary",
        "",
        f"- Status entries: {len(report.status_entries)}",
        f"- Duplicate status codes: {len(report.duplicate_codes)}",
        (
            '- Bare `ServiceException("...")` sites: '
            f"{len(report.bare_service_exceptions)}"
        ),
        (
            "- Direct `response.setStatus(...)` sites: "
            f"{len(report.direct_http_status_sites)}"
        ),
        "",
        "## Duplicate Status Codes",
        "",
        "| Code | Names |",
        "| --- | --- |",
    ]
    for code, entries in report.duplicate_codes.items():
        names = ", ".join(entry.name for entry in entries)
        lines.append(f"| {code} | {names} |")

    lines.extend(
        [
            "",
            "## Bare ServiceException Sites",
            "",
            "| Path | Line | Snippet |",
            "| --- | --- | --- |",
        ]
    )
    lines.extend(
        f"| {finding.path} | {finding.line} | `{finding.snippet}` |"
        for finding in report.bare_service_exceptions
    )

    lines.extend(
        [
            "",
            "## Direct HTTP Status Sites",
            "",
            "| Path | Line | Snippet |",
            "| --- | --- | --- |",
        ]
    )
    lines.extend(
        f"| {finding.path} | {finding.line} | `{finding.snippet}` |"
        for finding in report.direct_http_status_sites
    )
    return "\n".join(lines)


def render_json(report: InventoryReport) -> str:
    jsonable = {
        "status_entries": [asdict(entry) for entry in report.status_entries],
        "duplicate_codes": {
            str(code): [asdict(entry) for entry in entries]
            for code, entries in report.duplicate_codes.items()
        },
        "bare_service_exceptions": [
            asdict(finding) for finding in report.bare_service_exceptions
        ],
        "direct_http_status_sites": [
            asdict(finding) for finding in report.direct_http_status_sites
        ],
    }
    return json.dumps(jsonable, ensure_ascii=False, indent=2, sort_keys=True)


def _line_number(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def _normalize_snippet(value: str) -> str:
    return " ".join(value.split())


def _write_output(*, output: str, path: Path | None) -> None:
    if path is None:
        print(output)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--format",
        choices=("summary", "json", "markdown"),
        default="summary",
        help="render inventory as a compact summary, JSON, or Markdown",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the rendered inventory to a file instead of stdout",
    )
    args = parser.parse_args()

    report = build_inventory()
    if args.format == "summary":
        rendered = render_summary(report)
    elif args.format == "json":
        rendered = render_json(report)
    else:
        rendered = render_markdown(report)
    _write_output(output=rendered, path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
