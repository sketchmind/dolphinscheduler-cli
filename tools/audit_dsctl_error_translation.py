from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICES_ROOT = ROOT / "src" / "dsctl" / "services"
TRANSLATION_HELPER_PREFIXES = ("_translate_", "translate_", "_raise_")


@dataclass(frozen=True, order=True)
class CodeReference:
    name: str | None
    value: int | None


@dataclass(frozen=True)
class TranslatorReport:
    name: str
    line: int
    handled_codes: list[CodeReference]


@dataclass(frozen=True)
class ExceptSiteReport:
    function_name: str | None
    line: int
    translator_calls: list[str]
    preserves_source_chain: bool


@dataclass(frozen=True)
class PaginationHookReport:
    function_name: str | None
    line: int
    translate_error_expr: str | None


@dataclass(frozen=True)
class ModuleReport:
    module: str
    path: str
    translators: list[TranslatorReport]
    api_result_error_excepts: list[ExceptSiteReport]
    pagination_hooks: list[PaginationHookReport]


@dataclass(frozen=True)
class AuditReport:
    modules: list[ModuleReport]


def build_report() -> AuditReport:
    modules: list[ModuleReport] = []
    for path in sorted(SERVICES_ROOT.glob("*.py")):
        if path.name == "__init__.py":
            continue
        module_report = analyze_module(path)
        if (
            not module_report.translators
            and not module_report.api_result_error_excepts
            and not module_report.pagination_hooks
        ):
            continue
        modules.append(module_report)
    return AuditReport(modules=modules)


def analyze_module(path: Path) -> ModuleReport:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    constants = collect_module_int_constants(tree)
    translators = collect_translators(tree, constants)
    excepts = collect_api_result_error_excepts(tree)
    hooks = collect_pagination_hooks(tree, source)
    return ModuleReport(
        module=path.stem,
        path=path.relative_to(ROOT).as_posix(),
        translators=translators,
        api_result_error_excepts=excepts,
        pagination_hooks=hooks,
    )


def collect_module_int_constants(tree: ast.Module) -> dict[str, int]:
    constants: dict[str, int] = {}
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = assigned_int_value(node)
        if value is None:
            continue
        targets = assignment_target_names(node)
        for target in targets:
            if target.isupper():
                constants[target] = value
    return constants


def collect_translators(
    tree: ast.Module,
    constants: dict[str, int],
) -> list[TranslatorReport]:
    translators: list[TranslatorReport] = []
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if not is_translator_function(node):
            continue
        translators.append(
            TranslatorReport(
                name=node.name,
                line=node.lineno,
                handled_codes=collect_handled_codes(node, constants),
            )
        )
    return translators


def collect_handled_codes(
    function: ast.FunctionDef,
    constants: dict[str, int],
) -> list[CodeReference]:
    seen: set[CodeReference] = set()
    for node in ast.walk(function):
        if not isinstance(node, ast.Compare):
            continue
        if len(node.ops) != 1 or len(node.comparators) != 1:
            continue
        left = node.left
        right = node.comparators[0]
        op = node.ops[0]
        if is_result_code_expr(left) and isinstance(op, (ast.Eq, ast.In)):
            seen.update(extract_code_references(right, constants))
    return sorted(
        seen,
        key=lambda ref: (
            ref.value is None,
            ref.value if ref.value is not None else -1,
            ref.name or "",
        ),
    )


def collect_api_result_error_excepts(tree: ast.Module) -> list[ExceptSiteReport]:
    reports: list[ExceptSiteReport] = []
    for function in iter_functions(tree):
        for node in ast.walk(function):
            if not isinstance(node, ast.Try):
                continue
            for handler in node.handlers:
                if not contains_api_result_error(handler.type):
                    continue
                reports.append(
                    ExceptSiteReport(
                        function_name=function.name,
                        line=handler.lineno,
                        translator_calls=sorted(
                            {
                                call
                                for statement in handler.body
                                for call in collect_translator_calls(statement)
                            }
                        ),
                        preserves_source_chain=preserves_source_chain(handler),
                    )
                )
    return reports


def collect_pagination_hooks(
    tree: ast.Module,
    source: str,
) -> list[PaginationHookReport]:
    reports: list[PaginationHookReport] = []
    function_lines = {
        function.lineno: function.name for function in iter_functions(tree)
    }
    sorted_lines = sorted(function_lines)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not is_requested_page_data_call(node):
            continue
        translate_error_expr: str | None = None
        for keyword in node.keywords:
            if keyword.arg == "translate_error" and keyword.value is not None:
                translate_error_expr = ast.get_source_segment(source, keyword.value)
                if translate_error_expr is None:
                    translate_error_expr = ast.unparse(keyword.value)
                break
        reports.append(
            PaginationHookReport(
                function_name=enclosing_function_name(
                    node.lineno,
                    function_lines,
                    sorted_lines,
                ),
                line=node.lineno,
                translate_error_expr=translate_error_expr,
            )
        )
    return sorted(reports, key=lambda report: report.line)


def iter_functions(tree: ast.Module) -> list[ast.FunctionDef]:
    return [node for node in tree.body if isinstance(node, ast.FunctionDef)]


def is_translator_function(function: ast.FunctionDef) -> bool:
    if not is_translation_helper_name(function.name):
        return False
    return function_uses_api_result_error(function)


def assigned_int_value(node: ast.Assign | ast.AnnAssign) -> int | None:
    value_node = node.value
    if isinstance(value_node, ast.Constant) and isinstance(value_node.value, int):
        return value_node.value
    return None


def assignment_target_names(node: ast.Assign | ast.AnnAssign) -> list[str]:
    targets: list[ast.expr] = (
        node.targets if isinstance(node, ast.Assign) else [node.target]
    )
    return [target.id for target in targets if isinstance(target, ast.Name)]


def is_result_code_expr(node: ast.expr) -> bool:
    return (isinstance(node, ast.Name) and node.id == "result_code") or (
        isinstance(node, ast.Attribute) and node.attr == "result_code"
    )


def extract_code_references(
    node: ast.expr,
    constants: dict[str, int],
) -> set[CodeReference]:
    if isinstance(node, ast.Name):
        return {CodeReference(name=node.id, value=constants.get(node.id))}
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return {CodeReference(name=None, value=node.value)}
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        refs: set[CodeReference] = set()
        for item in node.elts:
            refs.update(extract_code_references(item, constants))
        return refs
    return set()


def contains_api_result_error(node: ast.expr | None) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Name) and node.id == "ApiResultError":
        return True
    if isinstance(node, ast.Tuple):
        return any(contains_api_result_error(item) for item in node.elts)
    return False


def collect_translator_calls(node: ast.stmt) -> set[str]:
    calls: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Raise) and isinstance(child.exc, ast.Call):
            name = called_name(child.exc.func)
            if name and name != "ApiResultError":
                calls.add(name)
            continue
        if not isinstance(child, ast.Call):
            continue
        name = called_name(child.func)
        if is_translation_helper_name(name):
            calls.add(name)
    return calls


def preserves_source_chain(handler: ast.ExceptHandler) -> bool:
    handler_name = handler.name
    if handler_name is None:
        return True
    for child in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
        if not isinstance(child, ast.Raise):
            continue
        if raise_preserves_source_chain(child, handler_name=handler_name):
            continue
        return False
    return True


def raise_preserves_source_chain(
    node: ast.Raise,
    *,
    handler_name: str,
) -> bool:
    if node.exc is None:
        return True
    if isinstance(node.exc, ast.Name) and node.exc.id == handler_name:
        return True
    if node.cause is None:
        return False
    return expr_references_name(node.cause, name=handler_name)


def expr_references_name(node: ast.expr, *, name: str) -> bool:
    return any(
        isinstance(child, ast.Name) and child.id == name for child in ast.walk(node)
    )


def called_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def is_requested_page_data_call(node: ast.Call) -> bool:
    return called_name(node.func) == "requested_page_data"


def is_translation_helper_name(name: str) -> bool:
    return name.startswith(TRANSLATION_HELPER_PREFIXES) or name.endswith("_error")


def is_translation_call_name(name: str) -> bool:
    return is_translation_helper_name(name) or name.endswith("Error")


def function_uses_api_result_error(function: ast.FunctionDef) -> bool:
    for arg in function.args.args:
        if contains_api_result_error(arg.annotation):
            return True
    return any(
        is_result_code_expr(node)
        for node in ast.walk(function)
        if isinstance(node, ast.expr)
    )


def enclosing_function_name(
    line: int,
    function_lines: dict[int, str],
    sorted_lines: list[int],
) -> str | None:
    current_name: str | None = None
    for function_line in sorted_lines:
        if function_line > line:
            break
        current_name = function_lines[function_line]
    return current_name


def render_summary(report: AuditReport) -> str:
    translators = [
        translator for module in report.modules for translator in module.translators
    ]
    unique_codes = {
        reference
        for translator in translators
        for reference in translator.handled_codes
    }
    except_sites = [
        site for module in report.modules for site in module.api_result_error_excepts
    ]
    translated_excepts = [site for site in except_sites if site.translator_calls]
    chained_translated_excepts = [
        site for site in translated_excepts if site.preserves_source_chain
    ]
    pagination_hooks = [
        hook for module in report.modules for hook in module.pagination_hooks
    ]
    translated_hooks = [
        hook for hook in pagination_hooks if hook.translate_error_expr is not None
    ]
    raw_hook_modules = sorted(
        {
            module.module
            for module in report.modules
            for hook in module.pagination_hooks
            if hook.translate_error_expr is None
        }
    )
    lines = [
        "dsctl error translation audit",
        f"  modules with findings: {len(report.modules)}",
        f"  translator functions: {len(translators)}",
        f"  unique handled code references: {len(unique_codes)}",
        f"  ApiResultError except sites: {len(except_sites)}",
        f"  translated except sites: {len(translated_excepts)}",
        (
            "  translated except sites preserving source chain: "
            f"{len(chained_translated_excepts)}"
        ),
        f"  requested_page_data hooks: {len(pagination_hooks)}",
        f"  hooks with translate_error: {len(translated_hooks)}",
    ]
    if raw_hook_modules:
        lines.append("  hooks without translate_error: " + ", ".join(raw_hook_modules))
    return "\n".join(lines)


def render_markdown(report: AuditReport) -> str:
    lines = [
        "# dsctl Error Translation Audit",
        "",
        "## Module Summary",
        "",
        (
            "| Module | Translators | Codes | Excepts | "
            "Translated excepts | Page hooks | Hooked pages |"
        ),
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    _append_module_summary(lines, report)
    _append_pagination_hook_section(lines, report)
    _append_raw_except_section(lines, report)
    _append_missing_source_chain_section(lines, report)
    return "\n".join(lines)


def _append_module_summary(lines: list[str], report: AuditReport) -> None:
    for module in report.modules:
        code_count = sum(
            len(translator.handled_codes) for translator in module.translators
        )
        translated_except_count = sum(
            1 for site in module.api_result_error_excepts if site.translator_calls
        )
        hooked_page_count = sum(
            1
            for hook in module.pagination_hooks
            if hook.translate_error_expr is not None
        )
        lines.append(
            "| "
            f"{module.module} | {len(module.translators)} | {code_count} | "
            f"{len(module.api_result_error_excepts)} | {translated_except_count} | "
            f"{len(module.pagination_hooks)} | {hooked_page_count} |"
        )


def _append_pagination_hook_section(lines: list[str], report: AuditReport) -> None:
    lines.extend(
        [
            "",
            "## Pagination Hooks Missing `translate_error`",
            "",
            "| Module | Function | Line |",
            "| --- | --- | --- |",
        ]
    )
    for module in report.modules:
        for hook in module.pagination_hooks:
            if hook.translate_error_expr is not None:
                continue
            lines.append(
                "| "
                f"{module.module} | {hook.function_name or '<module>'} | "
                f"{hook.line} |"
            )


def _append_raw_except_section(lines: list[str], report: AuditReport) -> None:
    lines.extend(
        [
            "",
            "## Raw `ApiResultError` Except Sites",
            "",
            "| Module | Function | Line |",
            "| --- | --- | --- |",
        ]
    )
    for module in report.modules:
        for site in module.api_result_error_excepts:
            if site.translator_calls:
                continue
            lines.append(
                "| "
                f"{module.module} | {site.function_name or '<module>'} | "
                f"{site.line} |"
            )


def _append_missing_source_chain_section(
    lines: list[str],
    report: AuditReport,
) -> None:
    lines.extend(
        [
            "",
            "## Translated `ApiResultError` Except Sites Missing Source Chain",
            "",
            "| Module | Function | Line |",
            "| --- | --- | --- |",
        ]
    )
    for module in report.modules:
        for site in module.api_result_error_excepts:
            if not site.translator_calls or site.preserves_source_chain:
                continue
            lines.append(
                "| "
                f"{module.module} | {site.function_name or '<module>'} | "
                f"{site.line} |"
            )


def render_json(report: AuditReport) -> str:
    return json.dumps(asdict(report), ensure_ascii=False, indent=2, sort_keys=True)


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
        help="render the audit as summary, markdown, or JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="write the rendered audit to a file instead of stdout",
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
