from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    from ds_codegen.java_source import JavaParseCache


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_java_source_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("ds_codegen.java_source")


def test_load_type_declaration_builds_controller_import_context() -> None:
    java_source = _load_java_source_module()
    repo_root = Path(__file__).resolve().parents[2]
    parse_cache: JavaParseCache = {}

    loaded = java_source.load_type_declaration(
        repo_root,
        "org.apache.dolphinscheduler.api.controller.ExecutorController",
        parse_cache,
    )

    assert loaded is not None
    _, type_declaration, import_map, package_name = loaded
    assert type_declaration.name == "ExecutorController"
    assert package_name == "org.apache.dolphinscheduler.api.controller"
    assert (
        import_map["TaskDependType"]
        == "org.apache.dolphinscheduler.common.enums.TaskDependType"
    )


def test_resolve_referenced_import_path_handles_imports_and_global_index() -> None:
    java_source = _load_java_source_module()
    repo_root = Path(__file__).resolve().parents[2]
    parse_cache: JavaParseCache = {}
    loaded = java_source.load_type_declaration(
        repo_root,
        "org.apache.dolphinscheduler.api.controller.ExecutorController",
        parse_cache,
    )

    assert loaded is not None
    _, _, import_map, package_name = loaded
    assert (
        java_source.resolve_referenced_import_path(
            repo_root,
            "TaskDependType",
            import_map,
            package_name,
        )
        == "org.apache.dolphinscheduler.common.enums.TaskDependType"
    )
    assert (
        java_source.resolve_referenced_import_path(
            repo_root,
            "EnvironmentDto",
            {},
            None,
        )
        == "org.apache.dolphinscheduler.api.dto.EnvironmentDto"
    )
    assert (
        java_source.logical_type_name(
            "org.apache.dolphinscheduler.common.enums.TaskDependType"
        )
        == "TaskDependType"
    )
