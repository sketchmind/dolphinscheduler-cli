from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("audit_dsctl_error_translation")


def test_collects_translators_excepts_and_pagination_hooks() -> None:
    audit = _load_module()
    source = """
USER_NO_OPERATION_PERM = 30001
QUERY_FAILED = 42

def _translate_demo_api_error(error: ApiResultError) -> Exception:
    result_code = error.result_code
    if result_code == USER_NO_OPERATION_PERM:
        raise ValueError
    if error.result_code in {QUERY_FAILED, 99}:
        raise RuntimeError
    return error

def list_demo() -> None:
    try:
        pass
    except ApiResultError as error:
        raise _translate_demo_api_error(error) from error
    requested_page_data(
        fetch_page,
        page_no=1,
        page_size=100,
        all_pages=False,
        serialize_item=str,
        resource="demo",
        translate_error=lambda error: _translate_demo_api_error(error),
    )
"""
    tree = audit.ast.parse(source)
    constants = audit.collect_module_int_constants(tree)
    translators = audit.collect_translators(tree, constants)
    excepts = audit.collect_api_result_error_excepts(tree)
    hooks = audit.collect_pagination_hooks(tree, source)

    assert constants == {"USER_NO_OPERATION_PERM": 30001, "QUERY_FAILED": 42}
    assert translators == [
        audit.TranslatorReport(
            name="_translate_demo_api_error",
            line=5,
            handled_codes=[
                audit.CodeReference(name="QUERY_FAILED", value=42),
                audit.CodeReference(name=None, value=99),
                audit.CodeReference(name="USER_NO_OPERATION_PERM", value=30001),
            ],
        )
    ]
    assert excepts == [
        audit.ExceptSiteReport(
            function_name="list_demo",
            line=16,
            translator_calls=["_translate_demo_api_error"],
            preserves_source_chain=True,
        )
    ]
    assert hooks == [
        audit.PaginationHookReport(
            function_name="list_demo",
            line=18,
            translate_error_expr=("lambda error: _translate_demo_api_error(error)"),
        )
    ]


def test_real_report_contains_known_translation_surfaces() -> None:
    audit = _load_module()

    report = audit.build_report()
    modules = {module.module: module for module in report.modules}

    access_token = modules["access_token"]
    assert any(
        translator.name == "_translate_access_token_api_error"
        for translator in access_token.translators
    )
    assert any(
        reference.value == 30001
        for translator in access_token.translators
        for reference in translator.handled_codes
    )
    assert any(
        hook.translate_error_expr is not None for hook in access_token.pagination_hooks
    )
    assert all(
        site.preserves_source_chain
        for module in report.modules
        for site in module.api_result_error_excepts
        if site.translator_calls
    )


def test_collect_api_result_error_excepts_flags_missing_source_chain() -> None:
    audit = _load_module()
    source = """
def _translate_demo_api_error(error: ApiResultError) -> Exception:
    return ValueError()

def mutate_demo() -> None:
    try:
        pass
    except ApiResultError as error:
        raise _translate_demo_api_error(error)
"""
    tree = audit.ast.parse(source)

    assert audit.collect_api_result_error_excepts(tree) == [
        audit.ExceptSiteReport(
            function_name="mutate_demo",
            line=8,
            translator_calls=["_translate_demo_api_error"],
            preserves_source_chain=False,
        )
    ]
