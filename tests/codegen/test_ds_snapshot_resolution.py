from __future__ import annotations

import ast
import importlib
import inspect
import json
import re
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_ambiguous_unqualified_use_requires_exact_source_refresh() -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")

    with pytest.raises(
        resolution.AmbiguousTypeReferenceError,
        match=r"not fully qualified.*refresh the snapshot from exact source",
    ):
        resolution.SnapshotTypeResolver.compile(_ambiguous_snapshot(ir))


def test_unique_reference_is_inferred_without_persisted_decisions() -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    ambiguous = _ambiguous_snapshot(ir)
    snapshot = replace(
        ambiguous,
        model_count=1,
        models=ambiguous.models[:1],
    )

    resolver = resolution.SnapshotTypeResolver.compile(snapshot)

    assert (
        resolver.resolve(
            "Thing",
            scope=resolution.ResolutionScope(
                "operation_response",
                snapshot.operations[0].operation_id,
            ),
        )
        == "example.first.Thing"
    )
    assert "reference_resolutions" not in snapshot.to_json_dict()


def test_qualified_identity_resolves_with_same_leaf_candidates() -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    ambiguous = _ambiguous_snapshot(ir)
    operation = replace(
        ambiguous.operations[0],
        inferred_return_type="example.second.Thing",
        logical_return_type="example.second.Thing",
    )
    snapshot = replace(ambiguous, operations=[operation])

    resolver = resolution.SnapshotTypeResolver.compile(snapshot)

    assert (
        resolver.resolve(
            "example.second.Thing",
            scope=resolution.ResolutionScope(
                "operation_response",
                operation.operation_id,
            ),
        )
        == "example.second.Thing"
    )


@pytest.mark.parametrize(
    ("java_type", "expected"),
    [
        ("java.util.List<java.time.ZonedDateTime>", "list[str]"),
        ("java.util.Map<java.lang.String, java.time.Instant>", "dict[str, str]"),
        ("java.util.Optional<java.lang.Long>", "int | None"),
        ("com.fasterxml.jackson.databind.node.ObjectNode", "dict[str, object]"),
        ("org.springframework.web.multipart.MultipartFile", "UploadFileLike"),
    ],
)
def test_qualified_platform_types_keep_canonical_wire_semantics(
    java_type: str,
    expected: str,
) -> None:
    type_refs = _module("ds_codegen.contract_type_refs")
    resolution = _module("ds_codegen.snapshot_resolution")
    planner = _module("ds_codegen.render.package.planner")
    type_support = _module("ds_codegen.render.package.type_support")
    ir = _module("ds_codegen.ir")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[_operation(ir, logical_return_type=java_type)],
        enums=[],
        dtos=[],
        models=[],
    )

    resolution.SnapshotTypeResolver.compile(snapshot)
    context = planner.build_package_context(snapshot)

    assert type_refs.collect_type_reference_names(java_type) == set()
    assert (
        type_support.render_annotation_type(
            java_type,
            owner_import_path=snapshot.operations[0].operation_id,
            owner_kind="operation_response",
            context=context,
        )
        == expected
    )


def test_contract_type_expression_tree_keeps_the_narrow_canonical_grammar() -> None:
    type_refs = _module("ds_codegen.contract_type_refs")

    expression = type_refs.parse_contract_type_expression(
        "java.util.Optional<"
        "java.util.List<java.util.Map<java.lang.String, example.Thing>>>"
    )
    array = type_refs.parse_contract_type_expression("java.lang.Byte[]")

    assert expression.name == "Optional"
    assert expression.arguments[0].name == "List"
    assert expression.arguments[0].arguments[0].name == "Map"
    assert expression.render() == "Optional<List<Map<String, example.Thing>>>"
    assert array.name == "Byte"
    assert array.arguments == ()
    assert array.is_array is True
    assert array.render() == "Byte[]"


@pytest.mark.parametrize(
    ("java_type", "model_import_path", "expected_annotation"),
    [
        (
            "List<org.apache.dolphinscheduler.dao.entity.Thing>",
            "org.apache.dolphinscheduler.dao.entity.Thing",
            "list[Thing]",
        ),
        (
            "com.example.External<java.lang.String>",
            None,
            "JsonValue",
        ),
    ],
)
def test_annotation_and_import_projection_share_one_type_plan(
    java_type: str,
    model_import_path: str | None,
    expected_annotation: str,
) -> None:
    type_support = _module("ds_codegen.render.package.type_support")
    planner = _module("ds_codegen.render.package.planner")
    ir = _module("ds_codegen.ir")
    operation = _operation(ir, logical_return_type=java_type)
    models = (
        [_model(ir, model_import_path, name="Thing")]
        if model_import_path is not None
        else []
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=len(models),
        operations=[operation],
        enums=[],
        dtos=[],
        models=models,
    )
    context = planner.build_package_context(snapshot)
    current_module_parts = ("api", "operations", "thing")

    assert (
        type_support.render_annotation_type(
            operation.logical_return_type,
            owner_import_path=operation.operation_id,
            owner_kind="operation_response",
            context=context,
        )
        == expected_annotation
    )
    targets = type_support.collect_annotation_import_targets(
        operation.logical_return_type,
        owner_import_path=operation.operation_id,
        owner_kind="operation_response",
        current_module_parts=current_module_parts,
        context=context,
    )
    if model_import_path is None:
        assert targets == set()
    else:
        assignment = context.assignments_by_import_path[model_import_path]
        assert targets == {(assignment.module_parts, assignment.class_name)}
        assert (
            type_support.collect_annotation_import_targets(
                operation.logical_return_type,
                owner_import_path=operation.operation_id,
                owner_kind="operation_response",
                current_module_parts=assignment.module_parts,
                context=context,
            )
            == set()
        )


@pytest.mark.parametrize(
    "missing_type",
    [
        "generated.view.Missing",
        "org.apache.dolphinscheduler.api.dto.Missing",
    ],
)
def test_missing_contract_owned_type_fails_closed(missing_type: str) -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[_operation(ir, logical_return_type=missing_type)],
        enums=[],
        dtos=[],
        models=[],
    )

    with pytest.raises(
        resolution.UnresolvedTypeReferenceError,
        match=rf"{re.escape(missing_type)}.*regenerate the snapshot",
    ):
        resolution.SnapshotTypeResolver.compile(snapshot)


@pytest.mark.parametrize(
    "java_type",
    [
        pytest.param("List<extends Thing>", id="bounded-wildcard"),
        pytest.param("Outer<T>.Inner<Thing>", id="parameterized-nested-owner"),
        pytest.param("String[][]", id="multidimensional-array"),
    ],
)
def test_snapshot_compile_rejects_unproved_type_expression_grammar(
    java_type: str,
) -> None:
    type_refs = _module("ds_codegen.contract_type_refs")
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    snapshot = replace(
        _ambiguous_snapshot(ir),
        operations=[_operation(ir, logical_return_type=java_type)],
    )

    with pytest.raises(
        type_refs.UnsupportedContractTypeExpressionError,
        match="unsupported contract type expression",
    ):
        resolution.SnapshotTypeResolver.compile(snapshot)


def test_snapshot_round_trip_and_digest_bind_qualified_identity(
    tmp_path: Path,
) -> None:
    contract_inputs = _module("ds_codegen.contract_inputs")
    ir = _module("ds_codegen.ir")
    ambiguous = _ambiguous_snapshot(ir)
    first_operation = replace(
        ambiguous.operations[0],
        inferred_return_type="example.first.Thing",
        logical_return_type="example.first.Thing",
    )
    second_operation = replace(
        first_operation,
        inferred_return_type="example.second.Thing",
        logical_return_type="example.second.Thing",
    )
    snapshot = replace(ambiguous, operations=[first_operation])
    other = replace(ambiguous, operations=[second_operation])

    payload = snapshot.to_json_dict()
    assert contract_inputs.snapshot_from_json(payload) == snapshot
    assert contract_inputs.contract_snapshot_digest(snapshot) != (
        contract_inputs.contract_snapshot_digest(other)
    )

    path = tmp_path / "contract.json"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        path,
        provenance={
            "contract_digest": contract_inputs.contract_snapshot_digest(snapshot)
        },
    )
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["operations"][0]["logical_return_type"] = "example.second.Thing"
    path.write_text(json.dumps(tampered), encoding="utf-8")

    with pytest.raises(ValueError, match="contract digest does not match provenance"):
        contract_inputs.load_contract_input(
            contract_inputs.ContractInput("9.9.9", path, "snapshot"),
            require_exact=False,
        )


def test_extractor_fingerprint_covers_type_and_visibility_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_inputs = _module("ds_codegen.contract_inputs")
    assert {
        "contract_inputs.py",
        "contract_type_refs.py",
        "contract_visibility.py",
        "snapshot_resolution.py",
    } <= {path.name for path in contract_inputs._EXTRACTOR_CORE_INPUTS}

    fingerprint_input = tmp_path / "contract_type_refs.py"
    fingerprint_input.write_text("first\n", encoding="utf-8")
    contract_inputs.contract_extractor_fingerprint.cache_clear()
    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(contract_inputs, "_EXTRACTOR_ROOT", tmp_path)
            scoped.setattr(
                contract_inputs,
                "_EXTRACTOR_CORE_INPUTS",
                (fingerprint_input,),
            )
            first = contract_inputs.contract_extractor_fingerprint()
            fingerprint_input.write_text("second\n", encoding="utf-8")
            contract_inputs.contract_extractor_fingerprint.cache_clear()
            second = contract_inputs.contract_extractor_fingerprint()
        assert first != second
    finally:
        contract_inputs.contract_extractor_fingerprint.cache_clear()


def test_snapshot_with_retired_resolution_table_requires_refresh() -> None:
    contract_inputs = _module("ds_codegen.contract_inputs")
    ir = _module("ds_codegen.ir")
    ambiguous = _ambiguous_snapshot(ir)
    operation = replace(
        ambiguous.operations[0],
        logical_return_type="example.first.Thing",
    )
    payload = replace(ambiguous, operations=[operation]).to_json_dict()
    payload["reference_resolutions"] = []

    with pytest.raises(
        ValueError,
        match=r"retired reference_resolutions side table.*refresh it from exact source",
    ):
        contract_inputs.snapshot_from_json(payload)


def test_transitive_closure_records_qualified_structured_edges() -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    operation = _operation(ir, logical_return_type="example.Container")
    container = _model(
        ir,
        "example.Container",
        name="Container",
        fields=[_field(ir, "value", "example.first.Thing")],
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=3,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            container,
            _model(ir, "example.first.Thing", name="Thing"),
            _model(ir, "example.second.Thing", name="Thing"),
        ],
    )
    resolver = resolution.SnapshotTypeResolver.compile(snapshot)

    graph = resolver.resolve_type_graph(
        [
            resolution.ScopedTypeUse(
                operation.logical_return_type,
                resolution.ResolutionScope(
                    "operation_response",
                    operation.operation_id,
                ),
            )
        ]
    )

    assert graph.import_paths == frozenset(
        {container.import_path, "example.first.Thing"}
    )
    assert any(
        edge.owner_kind == "structured_type"
        and edge.owner_ref == container.import_path
        and edge.reference_name == "example.first.Thing"
        and edge.import_path == "example.first.Thing"
        for edge in graph.edges
    )


def test_generated_view_import_collection_keeps_exact_qualified_path(
    tmp_path: Path,
) -> None:
    type_extraction = _module("ds_codegen.extract.type_extraction")
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    _write_java_source(tmp_path, "first.Thing", "public class Thing {}")
    _write_java_source(tmp_path, "second.Thing", "public class Thing {}")
    _write_controller_source(
        tmp_path,
        "ThingController",
        imports=("second.Thing",),
    )
    generated_view = _model(
        ir,
        "generated.view.Generated",
        name="Generated",
        fields=[_field(ir, "data", "second.Thing")],
        kind="generated_view",
    )
    java_source.clear_java_source_caches()
    try:
        assert type_extraction.collect_generated_view_model_imports(
            tmp_path,
            [generated_view],
            {"first.Thing"},
        ) == {"second.Thing"}
    finally:
        java_source.clear_java_source_caches()

    models = [_model(ir, "first.Thing", name="Thing")]
    pipeline._extend_model_specs_by_import_path(
        models,
        [
            _model(ir, "first.Thing", name="Thing"),
            _model(ir, "second.Thing", name="Thing"),
        ],
    )
    assert [model.import_path for model in models] == [
        "first.Thing",
        "second.Thing",
    ]
    operation = _operation(ir, logical_return_type="Generated")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=3,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[generated_view, *models],
    )
    assert (
        resolution.SnapshotTypeResolver.compile(snapshot).resolve(
            "second.Thing",
            scope=resolution.ResolutionScope(
                "structured_type",
                generated_view.import_path,
            ),
        )
        == "second.Thing"
    )


def test_generated_view_import_collection_uses_exact_enum_identity(
    tmp_path: Path,
) -> None:
    type_extraction = _module("ds_codegen.extract.type_extraction")
    java_source = _module("ds_codegen.java_source")
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    _write_java_source(tmp_path, "first.Kind", "public class Kind {}")
    _write_java_source(tmp_path, "second.Kind", "public enum Kind { A }")
    _write_controller_source(
        tmp_path,
        "ThingController",
        imports=("second.Kind",),
    )
    generated_view = _model(
        ir,
        "generated.view.Generated",
        name="Generated",
        fields=[_field(ir, "data", "second.Kind")],
        kind="generated_view",
    )
    java_source.clear_java_source_caches()
    try:
        additional_imports = type_extraction.collect_generated_view_model_imports(
            tmp_path,
            [generated_view],
            set(),
        )
        enum_specs = type_extraction.extract_enum_specs(
            tmp_path,
            additional_imports,
            {},
        )
    finally:
        java_source.clear_java_source_caches()
    assert additional_imports == {"second.Kind"}
    assert [enum_spec.import_path for enum_spec in enum_specs] == ["second.Kind"]

    operation = _operation(ir, logical_return_type="Generated")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=1,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=enum_specs,
        dtos=[],
        models=[generated_view, _model(ir, "first.Kind", name="Kind")],
    )
    assert (
        resolution.SnapshotTypeResolver.compile(snapshot).resolve(
            "second.Kind",
            scope=resolution.ResolutionScope(
                "structured_type",
                generated_view.import_path,
            ),
        )
        == "second.Kind"
    )


def test_generated_view_import_collection_indexes_exact_nested_identity(
    tmp_path: Path,
) -> None:
    type_extraction = _module("ds_codegen.extract.type_extraction")
    java_source = _module("ds_codegen.java_source")
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    _write_java_source(
        tmp_path,
        "first.Outer",
        "public class Outer { public static class Inner {} }",
    )
    _write_controller_source(
        tmp_path,
        "ThingController",
        imports=("first.Outer.Inner",),
    )
    generated_view = _model(
        ir,
        "generated.view.Generated",
        name="Generated",
        fields=[_field(ir, "data", "first.Outer.Inner")],
        kind="generated_view",
    )
    java_source.clear_java_source_caches()
    try:
        additional_imports = type_extraction.collect_generated_view_model_imports(
            tmp_path,
            [generated_view],
            set(),
        )
    finally:
        java_source.clear_java_source_caches()
    assert additional_imports == {"first.Outer.Inner"}

    operation = _operation(ir, logical_return_type="Generated")
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            generated_view,
            _model(ir, "first.Outer.Inner", name="Outer.Inner"),
        ],
    )
    assert (
        resolution.SnapshotTypeResolver.compile(snapshot).resolve(
            "first.Outer.Inner",
            scope=resolution.ResolutionScope(
                "structured_type",
                generated_view.import_path,
            ),
        )
        == "first.Outer.Inner"
    )


@pytest.mark.parametrize("generated_view", [False, True])
def test_service_payload_keeps_its_declaring_source_identity(
    tmp_path: Path,
    *,
    generated_view: bool,
) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    if generated_view:
        method_source = """
  @GetMapping("/one")
  public Map<String, Object> queryThing() {
    Map<String, Object> result = new HashMap<>();
    result.put("data", thingService.fetch());
    return result;
  }
"""
        collection_imports = "import java.util.Map; import java.util.HashMap;"
    else:
        method_source = """
  @GetMapping("/one")
  public Result<Object> queryThing() { return thingService.fetch(); }
"""
        collection_imports = ""
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/ThingController.java",
        f"""
package org.apache.dolphinscheduler.api.controller;
{collection_imports}
import example.bad.Thing;
import example.service.ThingService;
@RequestMapping("/things")
public class ThingController {{
  private ThingService thingService;
{method_source}
}}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/service/ThingService.java",
        """
package example.service;
import example.good.Thing;
public class ThingService {
  public Result<Thing> fetch() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/good/Thing.java",
        "package example.good; public class Thing { public String good; }",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/bad/Thing.java",
        "package example.bad; public class Thing { public String bad; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    assert [model.import_path for model in snapshot.models] == (
        [
            "example.good.Thing",
            "generated.view.ThingController_queryThing_result",
        ]
        if generated_view
        else ["example.good.Thing"]
    )
    if generated_view:
        generated_model = next(
            model for model in snapshot.models if model.kind == "generated_view"
        )
        assert generated_model.fields[0].java_type == "example.good.Thing"
    else:
        assert snapshot.operations[0].logical_return_type == "example.good.Thing"


def test_service_payload_uses_service_lexical_nested_type(tmp_path: Path) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/ServiceController.java",
        """
package org.apache.dolphinscheduler.api.controller;
import example.service.S;
@RequestMapping("/service")
public class ServiceController {
  private S service;
  @GetMapping("/one")
  public Result<Object> query() { return service.fetch(); }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/service/S.java",
        """
package example.service;
public class S {
  public static class Thing { public String nested; }
  public Result<Thing> fetch() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/service/Thing.java",
        "package example.service; public class Thing { public String topLevel; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    assert snapshot.operations[0].logical_return_type == "example.service.S.Thing"
    assert [model.import_path for model in snapshot.models] == [
        "example.service.S.Thing"
    ]
    assert [field.wire_name for field in snapshot.models[0].fields] == ["nested"]


def test_nested_type_reference_uses_java_lexical_owner_scope(tmp_path: Path) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/NestedController.java",
        """
package org.apache.dolphinscheduler.api.controller;
import example.Outer.Inner;
@RequestMapping("/nested")
public class NestedController {
  @GetMapping("/one")
  public Result<Inner> queryNested() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Outer.java",
        """
package example;
public class Outer {
  public static class Sibling { public String nested; }
  public static class Inner { public Sibling value; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Sibling.java",
        "package example; public class Sibling { public String topLevel; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    models = {model.import_path: model for model in snapshot.models}
    assert set(models) == {"example.Outer.Inner", "example.Outer.Sibling"}
    assert models["example.Outer.Inner"].fields[0].java_type == "example.Outer.Sibling"


def test_controller_request_and_response_use_controller_lexical_scope(
    tmp_path: Path,
) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/C.java",
        """
package org.apache.dolphinscheduler.api.controller;
@RequestMapping("/nested")
public class C {
  public static class Thing { public String nested; }
  @GetMapping("/one")
  public Result<Thing> query(@RequestParam Thing input) { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/Thing.java",
        """
package org.apache.dolphinscheduler.api.controller;
public class Thing { public String topLevel; }
""",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    assert [model.import_path for model in snapshot.models] == [
        "org.apache.dolphinscheduler.api.controller.C.Thing"
    ]
    assert [field.wire_name for field in snapshot.models[0].fields] == ["nested"]


def test_inherited_member_type_precedes_same_package_top_level(
    tmp_path: Path,
) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/InheritedController.java",
        """
package org.apache.dolphinscheduler.api.controller;
import example.Child;
@RequestMapping("/inherited")
public class InheritedController {
  @GetMapping("/one")
  public Result<Child> queryInherited() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Parent.java",
        """
package example;
public class Parent {
  public static class Thing { public String inherited; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Child.java",
        "package example; public class Child extends Parent { public Thing value; }",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Thing.java",
        "package example; public class Thing { public String topLevel; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    models = {model.import_path: model for model in snapshot.models}
    assert "example.Parent.Thing" in models
    assert "example.Thing" not in models
    assert [field.wire_name for field in models["example.Parent.Thing"].fields] == [
        "inherited"
    ]


def test_same_compilation_unit_type_precedes_global_namesake(tmp_path: Path) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/SameUnitController.java",
        """
package org.apache.dolphinscheduler.api.controller;
import example.Outer;
@RequestMapping("/same-unit")
public class SameUnitController {
  @GetMapping("/one")
  public Result<Outer> querySameUnit() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/Outer.java",
        """
package example;
public class Outer { public Helper value; }
class Helper { public String sameUnit; }
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/other/Helper.java",
        "package other; public class Helper { public String global; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()

    models = {model.import_path: model for model in snapshot.models}
    assert "example.Helper" in models
    assert "other.Helper" not in models
    assert [field.wire_name for field in models["example.Helper"].fields] == [
        "sameUnit"
    ]


def test_on_demand_import_resolves_exactly_and_rejects_ambiguity(
    tmp_path: Path,
) -> None:
    pipeline = _module("ds_codegen.extract.pipeline")
    java_source = _module("ds_codegen.java_source")
    _write_extractor_source(
        tmp_path,
        "pom.xml",
        "<project><version>9.9.9</version></project>",
    )
    controller_path = (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
        "api/controller/WildcardController.java"
    )
    _write_extractor_source(
        tmp_path,
        controller_path,
        """
package org.apache.dolphinscheduler.api.controller;
import example.good.*;
@RequestMapping("/wildcard")
public class WildcardController {
  @GetMapping("/one")
  public Result<Thing> queryWildcard() { return null; }
}
""",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/good/Thing.java",
        "package example.good; public class Thing { public String good; }",
    )
    _write_extractor_source(
        tmp_path,
        "dolphinscheduler-api/src/main/java/example/bad/Thing.java",
        "package example.bad; public class Thing { public String bad; }",
    )

    java_source.clear_java_source_caches()
    try:
        snapshot = pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()
    assert [model.import_path for model in snapshot.models] == ["example.good.Thing"]

    _write_extractor_source(
        tmp_path,
        controller_path,
        """
package org.apache.dolphinscheduler.api.controller;
import example.good.*;
import example.bad.*;
@RequestMapping("/wildcard")
public class WildcardController {
  @GetMapping("/one")
  public Result<Thing> queryWildcard() { return null; }
}
""",
    )
    java_source.clear_java_source_caches()
    try:
        with pytest.raises(
            java_source.AmbiguousJavaTypeReferenceError,
            match="ambiguous on-demand imports",
        ):
            pipeline.build_contract_snapshot(tmp_path)
    finally:
        java_source.clear_java_source_caches()


def test_effective_candidate_records_qualified_identity_without_rename_noise() -> None:
    adapter_candidates = _module("ds_codegen.adapter_wire_candidates")
    ir = _module("ds_codegen.ir")
    first = _ambiguous_graph_snapshot(ir, target="example.first.Thing")
    second = _ambiguous_graph_snapshot(ir, target="example.second.Thing")

    first_candidate = adapter_candidates.effective_response_candidate(
        first.operations[0],
        first,
    )
    second_candidate = adapter_candidates.effective_response_candidate(
        second.operations[0],
        second,
    )

    assert first_candidate["type_closure"] == second_candidate["type_closure"]
    assert first_candidate["reference_edges"] == second_candidate["reference_edges"]
    assert first_candidate["logical_return"] != second_candidate["logical_return"]
    assert first_candidate != second_candidate

    renamed_operation = replace(
        first.operations[0],
        operation_id="RenamedController.fetchThing",
        controller="RenamedController",
        method_name="fetchThing",
    )
    renamed = replace(first, operations=[renamed_operation])
    assert (
        adapter_candidates.effective_response_candidate(
            renamed_operation,
            renamed,
        )
        == first_candidate
    )


def test_resolver_rejects_unknown_references_and_duplicate_type_surfaces() -> None:
    resolution = _module("ds_codegen.snapshot_resolution")
    ir = _module("ds_codegen.ir")
    empty = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )
    resolver = resolution.SnapshotTypeResolver.compile(empty)

    with pytest.raises(
        resolution.UnresolvedTypeReferenceError,
        match="unresolved contract snapshot reference",
    ):
        resolver.resolve(
            "Missing",
            scope=resolution.ResolutionScope("structured_type", "example.Owner"),
        )

    duplicate = _model(ir, "example.Duplicate", name="Duplicate")
    duplicate_dto = ir.DtoSpec(
        name="Duplicate",
        import_path=duplicate.import_path,
        documentation=None,
        extends=None,
        fields=[],
    )
    with pytest.raises(
        resolution.SnapshotResolutionError,
        match="import path appears on multiple surfaces",
    ):
        resolution.SnapshotTypeResolver.compile(
            replace(
                empty,
                dto_count=1,
                model_count=1,
                dtos=[duplicate_dto],
                models=[duplicate],
            )
        )


def test_owner_aware_runtime_slice_keeps_only_the_selected_type_identity() -> None:
    impact = _module("ds_codegen.compatibility_impact")
    ir = _module("ds_codegen.ir")
    runtime_contract = _module("ds_codegen.runtime_contract")
    snapshot = _ambiguous_snapshot(ir, target="example.first.Thing")
    binding = _binding(impact, source_operation=snapshot.operations[0].operation_id)

    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"identity.current": binding},
    )

    assert [model.import_path for model in sliced.models] == ["example.first.Thing"]


def test_runtime_slice_preserves_the_target_when_both_candidates_survive() -> None:
    impact = _module("ds_codegen.compatibility_impact")
    ir = _module("ds_codegen.ir")
    runtime_contract = _module("ds_codegen.runtime_contract")
    snapshot = _ambiguous_snapshot(ir, target="example.first.Thing")
    binding = replace(
        _binding(impact, source_operation=snapshot.operations[0].operation_id),
        type_closure=(impact.WireTypeRef("models", "example.second.Thing"),),
    )

    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"identity.current": binding},
    )

    assert {model.import_path for model in sliced.models} == {
        "example.first.Thing",
        "example.second.Thing",
    }


def test_specialization_planning_is_exact_and_model_order_invariant(
    tmp_path: Path,
) -> None:
    ir = _module("ds_codegen.ir")
    package_renderer = _module("ds_codegen.render.package")
    planner = _module("ds_codegen.render.package.planner")
    snapshot = _generic_base_snapshot(
        ir,
        target="org.apache.dolphinscheduler.api.dto.Box",
    )
    reversed_snapshot = replace(snapshot, models=list(reversed(snapshot.models)))

    first_context = planner.build_package_context(snapshot)
    reversed_context = planner.build_package_context(reversed_snapshot)

    api_box = "org.apache.dolphinscheduler.api.dto.Box"
    value = "org.apache.dolphinscheduler.dao.entity.Value"
    api_specialization = f"{api_box}<{value}>"
    assert set(first_context.specialized_by_java_type) == {api_specialization}
    assert first_context.specialized_by_java_type == (
        reversed_context.specialized_by_java_type
    )
    assert (
        first_context.specialized_by_java_type[api_specialization].base_import_path
        == api_box
    )

    package_renderer.write_generated_package(snapshot, tmp_path / "first")
    package_renderer.write_generated_package(reversed_snapshot, tmp_path / "reversed")
    assert _rendered_bytes(tmp_path / "first") == _rendered_bytes(tmp_path / "reversed")

    second_context = planner.build_package_context(
        _generic_base_snapshot(
            ir,
            target="org.apache.dolphinscheduler.dao.entity.Box",
        )
    )
    dao_box = "org.apache.dolphinscheduler.dao.entity.Box"
    dao_inner = "org.apache.dolphinscheduler.dao.entity.Inner"
    assert set(second_context.specialized_by_java_type) == {
        f"{dao_box}<{value}>",
        f"{dao_inner}<{value}>",
    }


def test_generic_substitution_does_not_leak_into_native_field_references() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    first_value = "org.apache.dolphinscheduler.api.dto.first.Value"
    second_value = "org.apache.dolphinscheduler.api.dto.second.Value"
    outer_path = "org.apache.dolphinscheduler.api.dto.Outer"
    inner_path = "org.apache.dolphinscheduler.api.dto.Inner"
    operation = _operation(
        ir,
        logical_return_type=f"{outer_path}<{first_value}>",
    )
    outer = _model(
        ir,
        outer_path,
        name="Outer",
        fields=[
            _field(ir, "item", "T"),
            _field(ir, "inner", f"{inner_path}<{second_value}>"),
        ],
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=4,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            outer,
            _model(
                ir,
                inner_path,
                name="Inner",
                fields=[_field(ir, "value", "T")],
            ),
            _model(
                ir,
                first_value,
                name="Value",
            ),
            _model(
                ir,
                second_value,
                name="Value",
            ),
        ],
    )

    context = planner.build_package_context(snapshot)

    assert set(
        context.specialized_by_java_type[
            f"{outer_path}<{first_value}>"
        ].reference_import_paths.values()
    ) == {first_value}
    assert set(
        context.specialized_by_java_type[
            f"{inner_path}<{second_value}>"
        ].reference_import_paths.values()
    ) == {second_value}


def test_generated_view_unwrap_keeps_the_structured_owner_scope(
    tmp_path: Path,
) -> None:
    ir = _module("ds_codegen.ir")
    package_renderer = _module("ds_codegen.render.package")
    operation = _operation(ir, logical_return_type="ThingController_queryThing_result")
    view = _model(
        ir,
        "generated.view.ThingController_queryThing_result",
        name="ThingController_queryThing_result",
        fields=[
            _field(
                ir,
                "data",
                "org.apache.dolphinscheduler.api.dto.Thing",
            )
        ],
        kind="generated_view",
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=3,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            view,
            _model(
                ir,
                "org.apache.dolphinscheduler.api.dto.Thing",
                name="Thing",
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Thing",
                name="Thing",
            ),
        ],
    )

    package_renderer.write_generated_package(snapshot, tmp_path)

    operation_modules = [
        path.read_text(encoding="utf-8")
        for path in tmp_path.rglob("*.py")
        if "def query_thing(" in path.read_text(encoding="utf-8")
    ]
    assert len(operation_modules) == 1
    assert "from ..contracts.thing import Thing" in operation_modules[0]


def test_hidden_request_identity_cannot_flip_during_slicing() -> None:
    adapter_candidates = _module("ds_codegen.adapter_wire_candidates")
    impact = _module("ds_codegen.compatibility_impact")
    ir = _module("ds_codegen.ir")
    resolution = _module("ds_codegen.snapshot_resolution")
    runtime_contract = _module("ds_codegen.runtime_contract")
    operation = replace(
        _operation(ir, logical_return_type="String"),
        parameters=[
            ir.ParameterSpec(
                name="hiddenThing",
                java_type="Thing",
                binding="request_attribute",
                wire_name="hiddenThing",
                required=None,
                default_value=None,
                hidden=True,
                description=None,
                example=None,
                allowable_values=None,
                schema_type=None,
            )
        ],
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(ir, "example.first.Thing", name="Thing"),
            _model(ir, "example.second.Thing", name="Thing"),
        ],
    )
    resolution.SnapshotTypeResolver.compile(snapshot)
    binding = replace(
        _binding(impact, source_operation=operation.operation_id),
        type_closure=(impact.WireTypeRef("models", "example.second.Thing"),),
    )

    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"identity.current": binding},
    )

    assert [model.import_path for model in sliced.models] == ["example.second.Thing"]
    assert adapter_candidates.effective_request_candidate(operation, snapshot) == {
        "http_method": "GET",
        "path": "things",
        "consumes": [],
        "parameters": [],
        "type_closure": [],
    }


@pytest.mark.parametrize(
    "logical_return_type",
    ["Resource", "org.springframework.core.io.Resource"],
)
def test_transport_resource_envelope_is_not_a_logical_response_closure(
    logical_return_type: str,
) -> None:
    adapter_candidates = _module("ds_codegen.adapter_wire_candidates")
    ir = _module("ds_codegen.ir")
    resolution = _module("ds_codegen.snapshot_resolution")
    operation = replace(
        _operation(ir, logical_return_type=logical_return_type),
        return_type="ResponseEntity",
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(ir, "example.domain.Resource", name="Resource"),
            _model(ir, "org.springframework.core.io.Resource", name="Resource"),
        ],
    )

    resolver = resolution.SnapshotTypeResolver.compile(snapshot)

    assert adapter_candidates.effective_response_candidate(
        operation,
        snapshot,
        resolver=resolver,
    ) == {"logical_return": logical_return_type, "type_closure": []}


def test_package_path_arguments_use_visible_exact_request_scope(
    tmp_path: Path,
) -> None:
    ir = _module("ds_codegen.ir")
    package_renderer = _module("ds_codegen.render.package")
    operation = replace(
        _operation(ir, logical_return_type="String"),
        path="things/{thing}",
        parameters=[
            _parameter(
                ir,
                "thing",
                "org.apache.dolphinscheduler.dao.entity.Thing",
                binding="path_variable",
            )
        ],
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(
                ir,
                "org.apache.dolphinscheduler.api.dto.Thing",
                name="Thing",
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Thing",
                name="Thing",
            ),
        ],
    )

    package_renderer.write_generated_package(snapshot, tmp_path)

    operation_module = next(
        path.read_text(encoding="utf-8")
        for path in tmp_path.rglob("*.py")
        if "def query_thing(" in path.read_text(encoding="utf-8")
    )
    assert "thing: Thing" in operation_module
    assert "from ...dao.entities.thing import Thing" in operation_module


def test_hidden_explicit_path_binding_remains_client_supplied(
    tmp_path: Path,
) -> None:
    ir = _module("ds_codegen.ir")
    package_renderer = _module("ds_codegen.render.package")
    operation = replace(
        _operation(ir, logical_return_type="String"),
        path="things/{secret}",
        parameters=[
            _parameter(
                ir,
                "secret",
                "String",
                binding="path_variable",
                hidden=True,
            )
        ],
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[],
    )

    package_renderer.write_generated_package(snapshot, tmp_path)

    operation_module = next(
        path.read_text(encoding="utf-8")
        for path in tmp_path.rglob("*.py")
        if "def query_thing(" in path.read_text(encoding="utf-8")
    )
    assert "secret: str" in operation_module
    assert 'path = f"things/{secret}"' in operation_module


def test_specialized_rendering_uses_only_snapshot_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ir = _module("ds_codegen.ir")
    java_source = _module("ds_codegen.java_source")
    package_renderer = _module("ds_codegen.render.package")
    page_info_path = "org.apache.dolphinscheduler.api.utils.PageInfo"
    resource_path = "org.apache.dolphinscheduler.dao.entity.Resource"
    specialization = f"{page_info_path}<{resource_path}>"
    operation = replace(
        _operation(ir, logical_return_type=specialization),
        return_type=f"Result<{specialization}>",
        parameters=[
            ir.ParameterSpec(
                name="sessionPage",
                java_type=specialization,
                binding="request_attribute",
                wire_name="sessionPage",
                required=None,
                default_value=None,
                hidden=False,
                description=None,
                example=None,
                allowable_values=None,
                schema_type=None,
            )
        ],
    )
    page_info = _model(
        ir,
        page_info_path,
        name="PageInfo",
        fields=[_field(ir, "totalList", "List<T>")],
        kind="api_util",
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=3,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(
                ir,
                "org.apache.dolphinscheduler.api.dto.Resource",
                name="Resource",
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Resource",
                name="Resource",
            ),
            page_info,
        ],
    )
    source_resolution_calls = 0

    def reject_source_resolution(*args: object, **kwargs: object) -> None:
        nonlocal source_resolution_calls
        source_resolution_calls += 1
        raise AssertionError((args, kwargs))

    monkeypatch.setattr(
        java_source,
        "load_type_declaration",
        reject_source_resolution,
    )
    package_renderer.write_generated_package(snapshot, tmp_path / "exact")

    assert source_resolution_calls == 0
    specialized_modules = [
        path.read_text(encoding="utf-8")
        for path in (tmp_path / "exact").rglob("*.py")
        if f"Specialized view for {specialization}" in path.read_text(encoding="utf-8")
    ]
    assert len(specialized_modules) == 1
    imports = [
        line
        for line in specialized_modules[0].splitlines()
        if line.endswith(" import Resource")
    ]
    assert len(imports) == 1
    assert "dao.entities.resource" in imports[0]


def test_renderer_and_snapshot_resolver_have_no_transitive_source_dependency() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    reached = _transitive_ds_codegen_imports(
        tools_dir,
        roots=("ds_codegen.render.package", "ds_codegen.snapshot_resolution"),
    )

    assert {
        "ds_codegen.render.package.entrypoint",
        "ds_codegen.render.package.operations_renderer",
        "ds_codegen.render.package.planner",
        "ds_codegen.render.package.type_renderer",
        "ds_codegen.render.package.type_support",
    } <= reached
    assert "ds_codegen.java_source" not in reached
    assert not {name for name in reached if name.startswith("ds_codegen.extract")}

    package = _module("ds_codegen.render.package")
    assert tuple(inspect.signature(package.write_generated_package).parameters) == (
        "snapshot",
        "output_root",
        "shared_runtime",
    )
    shared_runtime = inspect.signature(package.write_generated_package).parameters[
        "shared_runtime"
    ]
    assert shared_runtime.kind is inspect.Parameter.KEYWORD_ONLY
    assert shared_runtime.default is False


@pytest.mark.source_rebuild
def test_all_exact_source_and_snapshot_runtime_packages_are_byte_identical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exact_contract_corpus: Any,
) -> None:
    java_source = _module("ds_codegen.java_source")
    package_renderer = _module("ds_codegen.render.package")
    runtime_contract = _module("ds_codegen.runtime_contract")
    version_diff = _module("ds_codegen.version_diff")
    source_resolution_calls = 0

    def reject_source_resolution(*args: object, **kwargs: object) -> None:
        nonlocal source_resolution_calls
        source_resolution_calls += 1
        raise AssertionError((args, kwargs))

    for version in exact_contract_corpus.versions:
        source_snapshot = version_diff.build_snapshot_from_ds_source(
            exact_contract_corpus.source_root(version)
        )
        cached_snapshot = exact_contract_corpus.snapshot(version)
        assert source_snapshot == cached_snapshot
        with monkeypatch.context() as source_guard:
            source_guard.setattr(
                java_source, "load_type_declaration", reject_source_resolution
            )
            source_slice = runtime_contract.configured_runtime_contract_slice(
                source_snapshot
            )
            cached_slice = runtime_contract.configured_runtime_contract_slice(
                cached_snapshot
            )
            source_root = tmp_path / "source" / version
            cached_root = tmp_path / "snapshot" / version
            package_renderer.write_generated_package(source_slice, source_root)
            package_renderer.write_generated_package(cached_slice, cached_root)
            assert _rendered_bytes(source_root) == _rendered_bytes(cached_root)
        del source_snapshot, source_slice, cached_slice
    assert source_resolution_calls == 0


def _transitive_ds_codegen_imports(
    tools_dir: Path,
    *,
    roots: tuple[str, ...],
) -> set[str]:
    pending = list(roots)
    reached: set[str] = set()
    while pending:
        module_name = pending.pop()
        if module_name in reached:
            continue
        reached.add(module_name)
        module_source = _module_source(tools_dir, module_name)
        if module_source is None:
            continue
        module_path, is_package = module_source
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            imported = _imported_modules(
                node,
                module_name=module_name,
                is_package=is_package,
            )
            pending.extend(
                name
                for name in imported
                if name == "ds_codegen" or name.startswith("ds_codegen.")
            )
    return reached


def _module_source(tools_dir: Path, module_name: str) -> tuple[Path, bool] | None:
    relative = Path(*module_name.split("."))
    module_path = tools_dir / relative.with_suffix(".py")
    if module_path.is_file():
        return module_path, False
    package_path = tools_dir / relative / "__init__.py"
    return (package_path, True) if package_path.is_file() else None


def _imported_modules(
    node: ast.AST,
    *,
    module_name: str,
    is_package: bool,
) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if not isinstance(node, ast.ImportFrom):
        return []
    if node.level == 0:
        return [node.module] if node.module is not None else []
    module_parts = module_name.split(".")
    package_parts = module_parts if is_package else module_parts[:-1]
    parent_parts = package_parts[: -(node.level - 1) or None]
    if node.module is not None:
        return [".".join((*parent_parts, *node.module.split(".")))]
    return [".".join((*parent_parts, alias.name)) for alias in node.names]


def _ambiguous_snapshot(ir: Any, *, target: str | None = None) -> Any:
    operation = _operation(ir, logical_return_type=target or "Thing")
    return ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(ir, "example.first.Thing", name="Thing"),
            _model(ir, "example.second.Thing", name="Thing"),
        ],
    )


def _ambiguous_graph_snapshot(ir: Any, *, target: str) -> Any:
    operation = _operation(
        ir,
        logical_return_type=(
            f"Triple<{target}, example.first.Thing, example.second.Thing>"
        ),
    )
    return ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=3,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(ir, "example.Triple", name="Triple"),
            _model(ir, "example.first.Thing", name="Thing"),
            _model(ir, "example.second.Thing", name="Thing"),
        ],
    )


def _generic_base_snapshot(ir: Any, *, target: str) -> Any:
    value = "org.apache.dolphinscheduler.dao.entity.Value"
    operation = _operation(ir, logical_return_type=f"{target}<{value}>")
    return ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=4,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[
            _model(
                ir,
                "org.apache.dolphinscheduler.api.dto.Box",
                name="Box",
                fields=[_field(ir, "value", "T")],
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Box",
                name="Box",
                fields=[
                    _field(
                        ir,
                        "inner",
                        "org.apache.dolphinscheduler.dao.entity.Inner<T>",
                    )
                ],
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Inner",
                name="Inner",
                fields=[_field(ir, "value", "T")],
            ),
            _model(
                ir,
                "org.apache.dolphinscheduler.dao.entity.Value",
                name="Value",
            ),
        ],
    )


def _operation(ir: Any, *, logical_return_type: str) -> Any:
    return ir.OperationSpec(
        operation_id="ThingController.queryThing",
        controller="ThingController",
        method_name="queryThing",
        api_group="v1",
        http_method="GET",
        path="things",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type=logical_return_type,
        inferred_return_type=None,
        logical_return_type=logical_return_type,
        response_projection="direct",
        parameters=[],
    )


def _parameter(
    ir: Any,
    name: str,
    java_type: str,
    *,
    binding: str,
    hidden: bool = False,
) -> Any:
    return ir.ParameterSpec(
        name=name,
        java_type=java_type,
        binding=binding,
        wire_name=name,
        required=None,
        default_value=None,
        hidden=hidden,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=None,
    )


def _model(
    ir: Any,
    import_path: str,
    *,
    name: str,
    fields: list[Any] | None = None,
    kind: str = "other_class",
) -> Any:
    return ir.ModelSpec(
        name=name,
        import_path=import_path,
        kind=kind,
        documentation=None,
        extends=None,
        fields=fields or [],
    )


def _field(ir: Any, name: str, java_type: str) -> Any:
    return ir.DtoFieldSpec(
        name=name,
        java_type=java_type,
        wire_name=name,
        required=None,
        default_value=None,
        nullable=False,
        default_factory=None,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )


def _binding(impact: Any, *, source_operation: str) -> Any:
    return impact.ReviewedBinding(
        source_operations=(source_operation,),
        type_closure=(impact.WireTypeRef("models", "example.first.Thing"),),
        selector_semantics=(),
        evidence_sources=(
            impact.EvidenceSource("controller", "ThingController.java#queryThing"),
            impact.EvidenceSource("ui", "users/index.ts"),
        ),
    )


def _rendered_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*.py"))
    }


def _write_java_source(repo_root: Path, import_path: str, declaration: str) -> None:
    parts = import_path.split(".")
    package_name = ".".join(parts[:-1])
    path = (
        repo_root
        / "references/dolphinscheduler/test-src/main/java"
        / Path(*parts[:-1])
        / f"{parts[-1]}.java"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"package {package_name}; {declaration}\n",
        encoding="utf-8",
    )


def _write_extractor_source(
    repo_root: Path,
    relative_path: str,
    source: str,
) -> None:
    path = repo_root / "references/dolphinscheduler" / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def _write_controller_source(
    repo_root: Path,
    controller_name: str,
    *,
    imports: tuple[str, ...],
) -> None:
    package_name = "org.apache.dolphinscheduler.api.controller"
    path = (
        repo_root
        / "references/dolphinscheduler/dolphinscheduler-api/src/main/java"
        / Path(*package_name.split("."))
        / f"{controller_name}.java"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    import_lines = " ".join(f"import {item};" for item in imports)
    path.write_text(
        f"package {package_name}; {import_lines} public class {controller_name} {{}}\n",
        encoding="utf-8",
    )
