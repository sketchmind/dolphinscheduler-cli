from __future__ import annotations

import gc
import importlib
import sys
import weakref
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

import javalang
import pytest

from ds_codegen.extract.pipeline import build_contract_snapshot
from ds_codegen.snapshot_resolution import SnapshotTypeResolver

if TYPE_CHECKING:
    from types import ModuleType

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.java_source import JavaParseCache


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_java_source_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("ds_codegen.java_source")


@pytest.mark.source_contract
def test_load_type_declaration_builds_controller_import_context(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    java_source = _load_java_source_module()
    parse_cache: JavaParseCache = {}

    with exact_contract_corpus.codegen_repo_root("3.4.1") as repo_root:
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


@pytest.mark.source_contract
def test_resolve_referenced_import_path_uses_explicit_source_scope(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    java_source = _load_java_source_module()
    parse_cache: JavaParseCache = {}
    with exact_contract_corpus.codegen_repo_root("3.4.1") as repo_root:
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
                java_source.SourceResolutionScope(
                    import_map,
                    package_name,
                    ("org.apache.dolphinscheduler.api.controller.ExecutorController"),
                ),
            )
            == "org.apache.dolphinscheduler.common.enums.TaskDependType"
        )
        assert (
            java_source.resolve_referenced_import_path(
                repo_root,
                "EnvironmentDto",
                java_source.SourceResolutionScope(
                    {
                        "EnvironmentDto": (
                            "org.apache.dolphinscheduler.api.dto.EnvironmentDto"
                        )
                    },
                    None,
                ),
            )
            == "org.apache.dolphinscheduler.api.dto.EnvironmentDto"
        )
    assert (
        java_source.logical_type_name(
            "org.apache.dolphinscheduler.common.enums.TaskDependType"
        )
        == "TaskDependType"
    )


def _write_extraction_source(repo_root: Path, *, field_type: str = "String") -> None:
    source_root = repo_root / "references/dolphinscheduler"
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "pom.xml").write_text("<project><version>9.9.9</version></project>")
    java_root = source_root / "dolphinscheduler-api/src/main/java"
    controller = (
        java_root / "org/apache/dolphinscheduler/api/controller/ThingController.java"
    )
    controller.parent.mkdir(parents=True, exist_ok=True)
    controller.write_text(
        """
package org.apache.dolphinscheduler.api.controller;
import org.apache.dolphinscheduler.dao.entity.Thing;
@RequestMapping("/things")
public class ThingController {
  @GetMapping("/first")
  public Thing first() { return null; }
  @GetMapping("/second")
  public Thing second() { return null; }
}
"""
    )
    model = java_root / "org/apache/dolphinscheduler/dao/entity/Thing.java"
    model.parent.mkdir(parents=True, exist_ok=True)
    model.write_text(
        "package org.apache.dolphinscheduler.dao.entity; "
        f"public class Thing {{ public {field_type} value; }}"
    )


def test_extraction_parses_each_compilation_unit_once_and_releases_asts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_extraction_source(tmp_path)
    parse = javalang.parse.parse
    calls: Counter[str] = Counter()
    references: list[weakref.ReferenceType[javalang.tree.CompilationUnit]] = []

    def counted_parse(source: str) -> javalang.tree.CompilationUnit:
        calls[source] += 1
        unit = parse(source)
        references.append(weakref.ref(unit))
        return unit

    monkeypatch.setattr(javalang.parse, "parse", counted_parse)
    snapshot = build_contract_snapshot(tmp_path)

    assert snapshot.operation_count == 2
    assert snapshot.models[0].fields[0].java_type == "String"
    assert calls
    assert set(calls.values()) == {1}
    gc.collect()
    assert all(reference() is None for reference in references)

    _write_extraction_source(tmp_path, field_type="Long")
    changed = build_contract_snapshot(tmp_path)
    assert changed.models[0].fields[0].java_type == "Long"
    assert changed != snapshot


def test_failed_extraction_releases_asts_and_does_not_poison_next_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_extraction_source(tmp_path)
    parse = javalang.parse.parse
    references: list[weakref.ReferenceType[javalang.tree.CompilationUnit]] = []

    def captured_parse(source: str) -> javalang.tree.CompilationUnit:
        unit = parse(source)
        references.append(weakref.ref(unit))
        return unit

    def fail_compilation(*args: object, **kwargs: object) -> None:
        message = "diagnostic snapshot failure"
        raise ValueError(message)

    monkeypatch.setattr(javalang.parse, "parse", captured_parse)
    with monkeypatch.context() as failure:
        failure.setattr(SnapshotTypeResolver, "compile", fail_compilation)
        with pytest.raises(ValueError, match="diagnostic snapshot failure"):
            build_contract_snapshot(tmp_path)
    gc.collect()
    assert references
    assert all(reference() is None for reference in references)
    _write_extraction_source(tmp_path, field_type="Long")
    assert build_contract_snapshot(tmp_path).models[0].fields[0].java_type == "Long"
