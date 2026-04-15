from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from audit_dsctl_error_translation import (
    SERVICES_ROOT,
    CodeReference,
    analyze_module,
    called_name,
    extract_code_references,
    is_result_code_expr,
    iter_functions,
)


@dataclass(frozen=True)
class BranchMapping:
    line: int
    codes: list[CodeReference]
    outcomes: list[str]


@dataclass(frozen=True)
class HelperMatrix:
    module: str
    helper: str
    line: int
    mappings: list[BranchMapping]


@dataclass(frozen=True)
class MatrixReport:
    helpers: list[HelperMatrix]


def build_report() -> MatrixReport:
    helpers: list[HelperMatrix] = []
    for path in sorted(SERVICES_ROOT.glob("*.py")):
        if path.name == "__init__.py":
            continue
        module_report = analyze_module(path)
        if not module_report.translators:
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        translator_names = {translator.name for translator in module_report.translators}
        functions = {
            function.name: function
            for function in iter_functions(tree)
            if function.name in translator_names
        }
        constants = {
            reference.name: reference.value
            for translator in module_report.translators
            for reference in translator.handled_codes
            if reference.name is not None and reference.value is not None
        }
        for translator in module_report.translators:
            function = functions[translator.name]
            helpers.append(
                HelperMatrix(
                    module=module_report.module,
                    helper=translator.name,
                    line=translator.line,
                    mappings=collect_branch_mappings(function, constants),
                )
            )
    return MatrixReport(helpers=helpers)


def collect_branch_mappings(
    function: ast.FunctionDef,
    constants: dict[str, int],
) -> list[BranchMapping]:
    mappings: list[BranchMapping] = []
    for node in ast.walk(function):
        if not isinstance(node, ast.If):
            continue
        codes = extract_codes_from_test(node.test, constants)
        if not codes:
            continue
        outcomes = collect_outcomes(node.body)
        if not outcomes:
            outcomes = ["<no-explicit-outcome>"]
        mappings.append(
            BranchMapping(
                line=node.lineno,
                codes=sorted(
                    codes,
                    key=lambda ref: (
                        ref.name is None,
                        ref.value is None,
                        ref.value if ref.value is not None else -1,
                        ref.name or "",
                    ),
                ),
                outcomes=outcomes,
            )
        )
    return sorted(mappings, key=lambda mapping: mapping.line)


def extract_codes_from_test(
    node: ast.expr,
    constants: dict[str, int],
) -> set[CodeReference]:
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or len(node.comparators) != 1:
            return set()
        if not is_result_code_expr(node.left):
            return set()
        op = node.ops[0]
        if not isinstance(op, (ast.Eq, ast.In)):
            return set()
        return extract_code_references(node.comparators[0], constants)
    if isinstance(node, ast.BoolOp):
        refs: set[CodeReference] = set()
        for value in node.values:
            refs.update(extract_codes_from_test(value, constants))
        return refs
    return set()


def collect_outcomes(statements: list[ast.stmt]) -> list[str]:
    outcomes: set[str] = set()
    for statement in statements:
        for node in ast.walk(statement):
            if isinstance(node, ast.Return):
                outcomes.update(return_outcomes(node))
            elif isinstance(node, ast.Raise):
                outcomes.update(raise_outcomes(node))
    return sorted(outcomes)


def return_outcomes(node: ast.Return) -> set[str]:
    value = node.value
    if value is None:
        return {"None"}
    if isinstance(value, ast.Name) and value.id in {"error", "exc"}:
        return {"ApiResultError"}
    if isinstance(value, ast.Call):
        name = called_name(value.func)
        return {name} if name else {"<unknown-return-call>"}
    if isinstance(value, ast.Name):
        return {value.id}
    return {"<unknown-return>"}


def raise_outcomes(node: ast.Raise) -> set[str]:
    exc = node.exc
    if isinstance(exc, ast.Call):
        name = called_name(exc.func)
        return {name} if name else {"<unknown-raise-call>"}
    if isinstance(exc, ast.Name):
        return {exc.id}
    return {"<unknown-raise>"}


def render_summary(report: MatrixReport) -> str:
    mapping_count = sum(len(helper.mappings) for helper in report.helpers)
    outcome_types = sorted(
        {
            outcome
            for helper in report.helpers
            for mapping in helper.mappings
            for outcome in mapping.outcomes
        }
    )
    lines = [
        "dsctl error translation matrix",
        f"  helpers: {len(report.helpers)}",
        f"  branch mappings: {mapping_count}",
        f"  outcome types: {', '.join(outcome_types)}",
    ]
    return "\n".join(lines)


def render_markdown(report: MatrixReport) -> str:
    lines = [
        "# dsctl Error Translation Matrix",
        "",
        "| Module | Helper | DS codes | Outcomes |",
        "| --- | --- | --- | --- |",
    ]
    for helper in report.helpers:
        if not helper.mappings:
            lines.append(f"| {helper.module} | {helper.helper} | `<none>` | `<none>` |")
            continue
        for mapping in helper.mappings:
            code_text = ", ".join(format_code(reference) for reference in mapping.codes)
            outcome_text = ", ".join(mapping.outcomes)
            lines.append(
                f"| {helper.module} | {helper.helper} | {code_text} | {outcome_text} |"
            )
    return "\n".join(lines)


def render_json(report: MatrixReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True)


def format_code(reference: CodeReference) -> str:
    if reference.name is None:
        return str(reference.value)
    if reference.value is None:
        return reference.name
    return f"{reference.name} ({reference.value})"


def write_output(*, output: str, path: Path | None) -> None:
    if path is None:
        print(output)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(output, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--format",
        choices=("summary", "markdown", "json"),
        default="summary",
        help="render the matrix as summary, markdown, or JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the rendered matrix to a file instead of stdout",
    )
    args = parser.parse_args()

    report = build_report()
    if args.format == "summary":
        rendered = render_summary(report)
    elif args.format == "markdown":
        rendered = render_markdown(report)
    else:
        rendered = render_json(report)
    write_output(output=rendered, path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
