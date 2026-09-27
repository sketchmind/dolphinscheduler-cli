from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

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


@pytest.mark.parametrize(
    ("domain", "catch_count"), [("workflow", 18), ("workflow_instance", 11)]
)
def test_workflow_package_audit_preserves_inventory_with_reviewed_additions(
    domain: str, catch_count: int
) -> None:
    audit = _load_module()
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "error_translation"
        / "pre_package_workflow_inventory.json"
    )
    expected = json.loads(fixture.read_text(encoding="utf-8"))[domain]
    if domain == "workflow":
        # Trigger RPC failures now inspect the exact start-error code at the
        # shared run boundary, preserving uncertain dispatch and its source.
        run_translator = next(
            item
            for item in expected["translators"]
            if item["name"] == "_raise_workflow_run_error"
        )
        assert run_translator["handled_codes"] == []
        run_translator["handled_codes"] = [
            {"name": "START_WORKFLOW_INSTANCE_ERROR", "value": 50014}
        ]
        # The domain boundary now translates release-refresh failures before
        # lifecycle catches DsctlError. Pre-mutation detail reads instead own
        # this API-error catch under the shared workflow read translator.
        retired_release_refresh = {
            "function_name": "_refresh_workflow_after_release",
            "translator_calls": ["_raise_workflow_release_refresh_error"],
            "preserves_source_chain": True,
        }
        assert expected["api_result_error_excepts"].count(retired_release_refresh) == 1
        expected["api_result_error_excepts"].remove(retired_release_refresh)
        expected["api_result_error_excepts"].append(
            {
                "function_name": "_load_workflow_detail",
                "translator_calls": ["raise_workflow_read_api_error"],
                "preserves_source_chain": True,
            }
        )
    if domain == "workflow_instance":
        # Exact-profile STOP code 50015 is now a reviewed ambiguous mutation
        # outcome; the generic code remains scoped to the action translator.
        action_translator = next(
            item
            for item in expected["translators"]
            if item["name"] == "_raise_workflow_instance_action_error"
        )
        assert action_translator["handled_codes"][-1] == {
            "name": "WORKFLOW_INSTANCE_NOT_FINISHED",
            "value": 50071,
        }
        action_translator["handled_codes"].insert(
            -1,
            {"name": "EXECUTE_WORKFLOW_INSTANCE_ERROR", "value": 50015},
        )
        expected["api_result_error_excepts"].extend(
            [
                {
                    "function_name": "_stop_workflow_instance_result",
                    "translator_calls": ["_raise_workflow_instance_action_error"],
                    "preserves_source_chain": True,
                },
                {
                    "function_name": "_list_by_trigger",
                    "translator_calls": ["PermissionDeniedError"],
                    "preserves_source_chain": True,
                },
            ]
        )
    modules = [
        module
        for module in audit.build_report().modules
        if module.module.startswith(f"{domain}.")
    ]

    assert (
        sum(len(module.api_result_error_excepts) for module in modules) == catch_count
    )
    for collection in ("translators", "api_result_error_excepts", "pagination_hooks"):
        # Source lines move; function scopes, translations, codes and hooks must not.
        actual = [
            {key: value for key, value in asdict(item).items() if key != "line"}
            for module in modules
            for item in getattr(module, collection)
        ]
        assert sorted(actual, key=repr) == sorted(expected[collection], key=repr)


def test_package_report_resolves_explicit_imports_without_executing_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = _load_module()
    services = tmp_path / "src" / "dsctl" / "services"
    workflow = services / "workflow"
    workflow.mkdir(parents=True)
    (workflow / "__init__.py").write_text(
        "from ._types import DENIED as OPERATION_DENIED\n", encoding="utf-8"
    )
    (workflow / "_types.py").write_text(
        'raise RuntimeError("audit must never import service modules")\n'
        "DENIED = 30001\nMISSING: int = 50003\n",
        encoding="utf-8",
    )
    (workflow / "_errors.py").write_text(
        """
from dsctl.services.workflow import OPERATION_DENIED
from ._types import MISSING
from unavailable_external_module import UNKNOWN

def _translate_demo(error: ApiResultError) -> Exception:
    if error.result_code == OPERATION_DENIED:
        return PermissionDeniedError("denied")
    if error.result_code == MISSING:
        return NotFoundError("missing")
    if error.result_code == UNKNOWN:
        return error
    return error
""",
        encoding="utf-8",
    )
    other = services / "workflow_instance"
    other.mkdir()
    (other / "_errors.py").write_text(
        """
def _translate_demo(error: ApiResultError) -> Exception:
    if error.result_code == 42:
        return UserInputError("invalid")
    return error
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "SERVICES_ROOT", services)

    modules = {module.module: module for module in audit.build_report().modules}

    assert set(modules) == {"workflow._errors", "workflow_instance._errors"}
    assert modules["workflow._errors"].translators[0].handled_codes == [
        audit.CodeReference(name="OPERATION_DENIED", value=30001),
        audit.CodeReference(name="MISSING", value=50003),
        audit.CodeReference(name="UNKNOWN", value=None),
    ]
    assert modules["workflow_instance._errors"].translators[0].handled_codes == [
        audit.CodeReference(name=None, value=42)
    ]


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


@pytest.mark.parametrize("helper", ["requested_page_data", "paged_command_result"])
def test_pagination_hooks_require_an_error_translator(helper: str) -> None:
    audit = _load_module()
    source = """
def raw_page() -> None:
    paged_command_result(fetch_page, resource="demo")

def explicit_none() -> None:
    paged_command_result(fetch_page, resource="demo", translate_error=None)

def translated_page() -> None:
    paged_command_result(
        fetch_page,
        resource="demo",
        translate_error=translate_demo_error,
    )
"""

    source = source.replace("paged_command_result(", f"{helper}(")
    hooks = audit.collect_pagination_hooks(audit.ast.parse(source), source)
    assert [
        hook.function_name for hook in hooks if hook.translate_error_expr is None
    ] == [
        "raw_page",
        "explicit_none",
    ]
