from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_fully_qualified_specialization_uses_short_declaration_names() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    project = "org.apache.dolphinscheduler.dao.entity.Project"
    java_type = f"{_PAGE_INFO}<{project}>"

    context = planner.build_package_context(_snapshot(ir, [project]))

    assert context.specialized_by_java_type[java_type].java_type == java_type
    assert context.specialized_by_java_type[java_type].class_name == "PageInfoProject"


def test_platform_fqns_use_the_same_canonical_specialization_key_as_rendering() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    type_support = _module("ds_codegen.render.package.type_support")
    operation = _operation(
        ir,
        operation_id="ThingController.queryTimes",
        logical_return_type=f"{_PAGE_INFO}<java.util.List<java.time.Instant>>",
    )
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=1,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[_model(ir, _PAGE_INFO, fields=[_field(ir, "totalList", "List<T>")])],
    )

    context = planner.build_package_context(snapshot)
    canonical_type = f"{_PAGE_INFO}<List<Instant>>"

    assert set(context.specialized_by_java_type) == {canonical_type}
    assert (
        type_support.render_annotation_type(
            operation.logical_return_type,
            owner_import_path=operation.operation_id,
            owner_kind="operation_response",
            context=context,
        )
        == context.specialized_by_java_type[canonical_type].class_name
    )


def test_specialization_name_cannot_shadow_an_ordinary_type_in_its_module() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    value = "org.apache.dolphinscheduler.api.dto.Item"
    nested = f"{_PAGE_INFO}.Item"
    operation = _operation(
        ir,
        operation_id="ThingController.queryItem",
        logical_return_type=f"{_PAGE_INFO}<{value}>",
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
            _model(ir, _PAGE_INFO, fields=[_field(ir, "totalList", "List<T>")]),
            _model(ir, nested),
            _model(ir, value),
        ],
    )

    context = planner.build_package_context(snapshot)
    ordinary = context.assignments_by_import_path[nested]
    specialized = context.specialized_by_java_type[f"{_PAGE_INFO}<{value}>"]

    assert ordinary.module_parts == specialized.module_parts
    assert ordinary.class_name == "PageInfoItem"
    assert specialized.class_name != ordinary.class_name


def test_same_leaf_specializations_use_stable_shortest_package_suffix() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    alpha = "org.apache.dolphinscheduler.api.dto.alpha.Resource"
    beta = "org.apache.dolphinscheduler.api.dto.beta.Resource"
    snapshot = _snapshot(ir, [alpha, beta])
    reversed_snapshot = replace(
        snapshot,
        operations=list(reversed(snapshot.operations)),
        models=list(reversed(snapshot.models)),
    )

    first = _specialized_names(planner.build_package_context(snapshot))
    reversed_names = _specialized_names(
        planner.build_package_context(reversed_snapshot)
    )

    assert first == reversed_names
    assert first == {
        f"{_PAGE_INFO}<{alpha}>": "PageInfoResourceAlpha",
        f"{_PAGE_INFO}<{beta}>": "PageInfoResourceBeta",
    }


def test_package_suffix_expands_only_until_it_disambiguates() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    alpha = "org.apache.dolphinscheduler.api.dto.alpha.shared.Resource"
    beta = "org.apache.dolphinscheduler.api.dto.beta.shared.Resource"

    names = _specialized_names(
        planner.build_package_context(_snapshot(ir, [alpha, beta]))
    )

    assert names == {
        f"{_PAGE_INFO}<{alpha}>": "PageInfoResourceAlphaShared",
        f"{_PAGE_INFO}<{beta}>": "PageInfoResourceBetaShared",
    }


def test_normalized_package_collision_falls_back_to_stable_hash() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    first = "org.apache.dolphinscheduler.api.dto.foo_bar.Resource"
    second = "org.apache.dolphinscheduler.api.dto.foo__bar.Resource"
    snapshot = _snapshot(ir, [first, second])

    names = _specialized_names(planner.build_package_context(snapshot))
    reversed_names = _specialized_names(
        planner.build_package_context(
            replace(
                snapshot,
                operations=list(reversed(snapshot.operations)),
                models=list(reversed(snapshot.models)),
            )
        )
    )

    assert names == reversed_names
    assert len(set(names.values())) == 2
    assert all(name.startswith("PageInfoResourceH") for name in names.values())
    assert all(len(name) == len("PageInfoResourceH") + 8 for name in names.values())


def test_ordinary_types_that_normalize_to_one_name_are_deduplicated() -> None:
    ir = _module("ds_codegen.ir")
    planner = _module("ds_codegen.render.package.planner")
    first = "generated.view.FooController_query_result"
    second = "generated.view.FooController_query__result"
    snapshot = ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[],
        enums=[],
        dtos=[],
        models=[_model(ir, first), _model(ir, second)],
    )

    assignments = planner.build_package_context(snapshot).assignments_by_import_path

    assert assignments[first].module_parts == assignments[second].module_parts
    assigned_names = {assignments[first].class_name, assignments[second].class_name}
    assert len(assigned_names) == 2
    assert all(name.startswith("FooControllerQueryResult") for name in assigned_names)
    assert any(name.endswith("2") for name in assigned_names)


_PAGE_INFO = "org.apache.dolphinscheduler.api.utils.PageInfo"


def _snapshot(ir: Any, value_import_paths: list[str]) -> Any:
    operations = [
        _operation(
            ir,
            operation_id=f"ThingController.query{index}",
            logical_return_type=f"{_PAGE_INFO}<{import_path}>",
        )
        for index, import_path in enumerate(value_import_paths)
    ]
    models = [
        _model(ir, _PAGE_INFO, fields=[_field(ir, "totalList", "List<T>")]),
        *(_model(ir, import_path) for import_path in value_import_paths),
    ]
    return ir.ContractSnapshot(
        ds_version="9.9.9",
        operation_count=len(operations),
        enum_count=0,
        dto_count=0,
        model_count=len(models),
        operations=operations,
        enums=[],
        dtos=[],
        models=models,
    )


def _operation(
    ir: Any,
    *,
    operation_id: str,
    logical_return_type: str,
) -> Any:
    return ir.OperationSpec(
        operation_id=operation_id,
        controller="ThingController",
        method_name=operation_id.rsplit(".", 1)[-1],
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


def _model(
    ir: Any,
    import_path: str,
    *,
    fields: list[Any] | None = None,
) -> Any:
    return ir.ModelSpec(
        name=import_path.rsplit(".", 1)[-1],
        import_path=import_path,
        kind="other_class",
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


def _specialized_names(context: Any) -> dict[str, str]:
    return {
        java_type: specialization.class_name
        for java_type, specialization in context.specialized_by_java_type.items()
    }
