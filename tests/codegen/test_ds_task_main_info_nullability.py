"""Exact source and typed wire proof for nullable upstream task names."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING

import pytest
from pydantic import TypeAdapter, ValidationError

from ds_codegen.ir import DtoFieldSpec
from ds_codegen.render.package.executable_schema import render_operation_response_module
from ds_codegen.render.package.type_renderer import (
    _NULLABLE_UPSTREAM_TASK_NAMES,
    _reviewed_map_value_nullability,
)

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

pytestmark = pytest.mark.source_contract
_TASK_MAIN_INFO = "org.apache.dolphinscheduler.dao.entity.TaskMainInfo"
_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
    "service/impl/TaskDefinitionServiceImpl.java"
)
_MAPPER = (
    "dolphinscheduler-dao/src/main/resources/org/apache/dolphinscheduler/dao/"
    "mapper/TaskDefinitionMapper.xml"
)
_ENTITY = (
    "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/dao/"
    "entity/TaskMainInfo.java"
)


@pytest.mark.parametrize("version", sorted(_NULLABLE_UPSTREAM_TASK_NAMES))
def test_exact_task_page_source_can_emit_null_upstream_name(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    service = exact_contract_corpus.source_file(version, _SERVICE).read_text()
    mapper = exact_contract_corpus.source_file(version, _MAPPER).read_text()
    entity = exact_contract_corpus.source_file(version, _ENTITY).read_text()
    assert (
        "upstreamTaskMap.put(info.getUpstreamTaskCode(), info.getUpstreamTaskName())"
        in service
    )
    assert (
        "getUpstreamTaskMap().put(info.getUpstreamTaskCode(), "
        "info.getUpstreamTaskName())" in service
    )
    assert (
        "LEFT JOIN t_ds_task_definition up on pt.pre_task_code=up.code "
        "and pt.pre_task_version=up.version"
    ) in mapper
    assert "Map<Long, String> upstreamTaskMap" in entity
    models = tuple(
        item
        for item in exact_contract_corpus.snapshot(version).models
        if item.import_path == _TASK_MAIN_INFO
    )
    if models:
        field = next(
            item for item in models[0].fields if item.wire_name == "upstreamTaskMap"
        )
        assert (field.java_type, field.nullable) == ("Map<Long, String>", True)


def test_310_generated_task_page_accepts_only_nullable_string_map_values(
    exact_contract_corpus: ExactContractCorpus, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = exact_contract_corpus.snapshot("3.1.0")
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "TaskDefinitionController.queryTaskDefinitionListPaging"
    )
    rendered = render_operation_response_module(
        snapshot,
        operation,
        module_parts=(
            "wire_programs",
            "_schemas",
            "workflow_runtime",
            "task_page_probe",
        ),
        root_model_module_parts=("wire_runtime", "_models"),
    )
    assert "upstreamTaskMap: dict[int, str | None] | None" in rendered.source
    module = ModuleType(
        "dsctl.generated.wire_programs._schemas.workflow_runtime.task_page_probe"
    )
    module.__package__ = "dsctl.generated.wire_programs._schemas.workflow_runtime"
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(rendered.source, "<task-page-probe>", "exec"), module.__dict__)  # noqa: S102
    response_type = eval(rendered.adapter_annotation, module.__dict__)  # noqa: S307
    adapter = TypeAdapter(response_type)
    row: dict[str, object] = {
        "taskCode": 42,
        "taskName": "example",
        "taskVersion": 1,
        "upstreamTaskMap": {"7": None},
    }
    page = {
        "totalList": [row],
        "total": 1,
        "totalPage": 1,
        "pageSize": 100,
        "currentPage": 1,
    }
    validated = adapter.validate_python(page)
    assert validated.totalList[0].upstreamTaskMap == {7: None}
    for bad_map in ({"not-a-code": None}, {"7": 123}):
        with pytest.raises(ValidationError):
            adapter.validate_python(
                {
                    **page,
                    "totalList": [{**row, "upstreamTaskMap": bad_map}],
                }
            )


def test_nullable_map_overlay_rejects_shape_drift_and_other_owners() -> None:
    field = DtoFieldSpec(
        name="upstreamTaskMap",
        java_type="Map<Long, String>",
        wire_name="upstreamTaskMap",
        required=None,
        default_value=None,
        nullable=True,
        default_factory=None,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )
    assert _reviewed_map_value_nullability(
        version="3.1.0",
        owner_import_path=_TASK_MAIN_INFO,
        field=field,
        rendered_type="dict[int, str] | None",
    ) == ("dict[int, str | None] | None")
    assert _reviewed_map_value_nullability(
        version="3.4.2",
        owner_import_path=_TASK_MAIN_INFO,
        field=field,
        rendered_type="dict[int, str] | None",
    ) == ("dict[int, str] | None")
    assert (
        _reviewed_map_value_nullability(
            version="3.1.0",
            owner_import_path="other.Model",
            field=field,
            rendered_type="dict[int, str] | None",
        )
        == "dict[int, str] | None"
    )
    with pytest.raises(ValueError, match="shape changed"):
        _reviewed_map_value_nullability(
            version="3.1.0",
            owner_import_path=_TASK_MAIN_INFO,
            field=replace(field, java_type="Map<Long, Object>"),
            rendered_type="dict[int, str] | None",
        )
