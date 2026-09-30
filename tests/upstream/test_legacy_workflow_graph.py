from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.models.workflow_spec import WorkflowSpec, validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import (
    LegacyWorkflowGraphError,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskResourceRefIndex,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.models.common import YamlValue
    from dsctl.support.json_types import JsonObject


def _workflow_spec() -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": "daily"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                },
                {
                    "name": "load",
                    "type": "SHELL",
                    "command": "echo load",
                    "depends_on": ["extract"],
                },
            ],
        }
    )


def _mr_workflow_spec() -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": "daily-mr"},
            "tasks": [
                {
                    "name": "word-count",
                    "type": "MR",
                    "task_params": {
                        "mainJar": "/jobs/wordcount.jar",
                        "mainClass": "com.example.WordCount",
                        "mainArgs": ["hdfs:///input", "hdfs:///output"],
                    },
                }
            ],
        }
    )


def _mr_process_definition_json(params: object) -> str:
    return json.dumps(
        {
            "globalParams": [],
            "tasks": [
                {
                    "id": "tasks-mr",
                    "name": "word-count",
                    "type": "MR",
                    "params": params,
                    "preTasks": [],
                }
            ],
            "timeout": 0,
            "tenantId": -1,
        }
    )


def test_prepare_mr_uses_the_resolved_positive_resource_id_wire() -> None:
    prepared = prepare_legacy_workflow_graph(
        _mr_workflow_spec(),
        task_id_factory=lambda _name: "tasks-mr",
        resource_refs=TaskResourceRefIndex.from_id_by_full_name(
            {"/jobs/wordcount.jar": 41}
        ),
    )

    process_data = json.loads(prepared.preview()["processDefinitionJson"])
    assert process_data["tasks"][0]["params"] == {
        "localParams": [],
        "mainJar": {"id": 41},
        "mainClass": "com.example.WordCount",
        "mainArgs": "hdfs:///input hdfs:///output",
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "JAVA",
    }


def test_decode_mr_canonicalizes_the_exact_resolved_resource_wire() -> None:
    native_params = {
        "localParams": [],
        "mainJar": {"id": 41},
        "mainClass": "com.example.WordCount",
        "mainArgs": "hdfs:///input hdfs:///output",
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "JAVA",
    }
    resource_refs = TaskResourceRefIndex.from_id_by_full_name(
        {"/jobs/wordcount.jar": 41}
    )

    decoded = decode_legacy_workflow_graph(
        _mr_process_definition_json(native_params),
        locations=(
            '{"tasks-mr":{"name":"word-count","targetarr":"",'
            '"nodenumber":0,"x":0,"y":0}}'
        ),
        connects="[]",
        resource_refs=resource_refs,
    )

    assert (
        decoded.tasks[0].task_params_reencode_source is ProjectionSource.TYPED_AUTHORING
    )
    assert decoded.tasks[0].document["task_params"] == {
        "mainJar": "/jobs/wordcount.jar",
        "mainClass": "com.example.WordCount",
        "mainArgs": ("hdfs:///input", "hdfs:///output"),
    }
    prepared = prepare_legacy_workflow_graph(
        decoded.to_workflow_spec(name="daily-mr"),
        baseline=decoded,
        resource_refs=resource_refs,
    )
    process_data = json.loads(prepared.preview()["processDefinitionJson"])
    assert process_data["tasks"][0]["params"] == native_params


@pytest.mark.parametrize(
    ("native_params", "resource_refs"),
    [
        pytest.param(
            {
                "localParams": [],
                "mainJar": {"id": 41},
                "mainClass": "com.example.WordCount",
                "mainArgs": "hdfs:///input hdfs:///output",
                "others": "",
                "appName": "",
                "resourceList": [],
                "programType": "JAVA",
            },
            None,
            id="positive-id-without-reverse-binding",
        ),
        pytest.param(
            {
                "localParams": [],
                "mainJar": {"id": 0, "res": "/jobs/wordcount.jar"},
                "mainClass": "com.example.WordCount",
                "mainArgs": "hdfs:///input hdfs:///output",
                "others": "",
                "appName": "",
                "resourceList": [],
                "programType": "JAVA",
            },
            None,
            id="legacy-id-zero-res-escape",
        ),
        pytest.param(
            {
                "localParams": [],
                "mainJar": {"id": 41},
                "mainClass": "com.example.WordCount",
                "mainArgs": "hdfs:///input hdfs:///output",
                "others": "",
                "appName": "",
                "resourceList": [{"id": 99}],
                "programType": "JAVA",
            },
            TaskResourceRefIndex.from_id_by_full_name({"/jobs/wordcount.jar": 41}),
            id="richer-native-resource-state",
        ),
    ],
)
def test_decode_mr_preserves_every_unresolved_or_richer_native_wire(
    native_params: dict[str, object],
    resource_refs: TaskResourceRefIndex | None,
) -> None:
    decoded = decode_legacy_workflow_graph(
        _mr_process_definition_json(native_params),
        locations=(
            '{"tasks-mr":{"name":"word-count","targetarr":"",'
            '"nodenumber":0,"x":0,"y":0}}'
        ),
        connects="[]",
        resource_refs=resource_refs,
    )

    assert (
        decoded.tasks[0].task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    prepared = prepare_legacy_workflow_graph(
        decoded.to_workflow_spec(name="daily-mr"),
        baseline=decoded,
        resource_refs=resource_refs,
    )
    process_data = json.loads(prepared.preview()["processDefinitionJson"])
    assert process_data["tasks"][0]["params"] == native_params


def test_prepare_mr_keeps_the_reviewed_scala_program_mode_opaque() -> None:
    native_params: JsonObject = {
        "localParams": [],
        "mainJar": {"id": 41},
        "mainClass": "com.example.WordCount",
        "mainArgs": "hdfs:///input hdfs:///output",
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "SCALA",
    }
    catalog = get_task_authoring_catalog("1.3.9")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "daily-mr-scala"},
            "tasks": [
                {
                    "name": "word-count",
                    "type": "MR",
                    "task_params": cast("YamlValue", native_params),
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    prepared = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _name: "tasks-mr",
    )

    process_data = json.loads(prepared.preview()["processDefinitionJson"])
    assert process_data["tasks"][0]["params"] == native_params


def test_prepare_freezes_exact_ds_139_string_native_graph() -> None:
    native_ids: Iterator[str] = iter(("task-a", "task-b"))

    prepared = prepare_legacy_workflow_graph(
        _workflow_spec(),
        task_id_factory=lambda _name: next(native_ids),
    )

    assert prepared.required_task_id_count == 2
    assert prepared.task_ids == (("extract", "task-a"), ("load", "task-b"))
    assert prepared.edges == (("extract", "load"),)
    assert prepared.preview() == {
        "processDefinitionJson": (
            '{"globalParams":[],"tasks":['
            '{"id":"task-a","name":"extract","type":"SHELL",'
            '"description":"","runFlag":"NORMAL","dependence":{},'
            '"maxRetryTimes":0,"retryInterval":1,'
            '"params":{"resourceList":[],"localParams":[],'
            '"rawScript":"echo extract"},"preTasks":[],'
            '"taskInstancePriority":"MEDIUM","workerGroup":"default",'
            '"timeout":{"strategy":"","interval":null,"enable":false}},'
            '{"id":"task-b","name":"load","type":"SHELL",'
            '"description":"","runFlag":"NORMAL","dependence":{},'
            '"maxRetryTimes":0,"retryInterval":1,'
            '"params":{"resourceList":[],"localParams":[],'
            '"rawScript":"echo load"},"preTasks":["extract"],'
            '"taskInstancePriority":"MEDIUM","workerGroup":"default",'
            '"timeout":{"strategy":"","interval":null,"enable":false}}'
            '],"timeout":0,"tenantId":-1}'
        ),
        "locations": (
            '{"task-a":{"name":"extract","targetarr":"",'
            '"nodenumber":1,"x":0,"y":0},'
            '"task-b":{"name":"load","targetarr":"task-a",'
            '"nodenumber":0,"x":300,"y":0}}'
        ),
        "connects": ('[{"endPointSourceId":"task-a","endPointTargetId":"task-b"}]'),
    }
    assert prepared.materialize() == prepared.preview()


def test_prepare_generates_opaque_native_ids_inside_the_legacy_compiler() -> None:
    prepared = prepare_legacy_workflow_graph(_workflow_spec())

    assert prepared.required_task_id_count == 2
    assert [name for name, _task_id in prepared.task_ids] == ["extract", "load"]
    assert all(
        re.fullmatch(r"tasks-[0-9a-f]{32}", task_id)
        for _name, task_id in prepared.task_ids
    )
    assert len({task_id for _name, task_id in prepared.task_ids}) == 2


def test_decode_projects_canonical_document_and_preserves_native_fields() -> None:
    decoded = decode_legacy_workflow_graph(
        process_definition_json=(
            '{"globalParams":[{"prop":"biz_date","direct":"IN",'
            '"type":"VARCHAR","value":"2026-08-13"}],"tasks":['
            '{"id":"tasks-11","name":"extract","type":"SHELL",'
            '"desc":"extract source","params":{"rawScript":"echo extract",'
            '"resourceList":[],"localParams":[]},"preTasks":[],'
            '"runFlag":"NORMAL","maxRetryTimes":1,"retryInterval":2,'
            '"taskInstancePriority":"HIGH","workerGroup":"analytics",'
            '"futureTaskField":{"keep":true}},'
            '{"id":"tasks-22","name":"load","type":"SHELL",'
            '"params":{"rawScript":"echo load","resourceList":[],'
            '"localParams":[]},"preTasks":["extract"],'
            '"runFlag":"FORBIDDEN","maxRetryTimes":0,"retryInterval":0,'
            '"taskInstancePriority":"MEDIUM","workerGroup":"default"}'
            '],"timeout":30,"tenantId":7,"futureProcessField":"keep"}'
        ),
        locations=(
            '{"tasks-11":{"name":"extract","targetarr":"",'
            '"nodenumber":1,"x":10,"y":20},'
            '"tasks-22":{"name":"load","targetarr":"tasks-11",'
            '"nodenumber":0,"x":310,"y":20}}'
        ),
        connects=('[{"endPointSourceId":"tasks-11","endPointTargetId":"tasks-22"}]'),
    )

    assert decoded.task_ids == (("extract", "tasks-11"), ("load", "tasks-22"))
    assert decoded.edges == (("extract", "load"),)
    assert decoded.tasks[0].native_fields["futureTaskField"] == {"keep": True}
    assert decoded.native_process_fields["futureProcessField"] == "keep"
    assert decoded.workflow_document(
        name="daily",
        project="demo",
        description="nightly",
        release_state="ONLINE",
    ) == {
        "workflow": {
            "name": "daily",
            "project": "demo",
            "description": "nightly",
            "timeout": 30,
            "global_params": [
                {
                    "prop": "biz_date",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "2026-08-13",
                }
            ],
            "release_state": "ONLINE",
        },
        "tasks": [
            {
                "name": "extract",
                "type": "SHELL",
                "description": "extract source",
                "task_params": {
                    "rawScript": "echo extract",
                    "resourceList": [],
                    "localParams": [],
                },
                "flag": "YES",
                "worker_group": "analytics",
                "priority": "HIGH",
                "retry": {"times": 1, "interval": 2},
                "depends_on": [],
            },
            {
                "name": "load",
                "type": "SHELL",
                "task_params": {
                    "rawScript": "echo load",
                    "resourceList": [],
                    "localParams": [],
                },
                "flag": "NO",
                "worker_group": "default",
                "priority": "MEDIUM",
                "retry": {"times": 0, "interval": 0},
                "depends_on": ["extract"],
            },
        ],
    }
    assert decoded.to_workflow_spec(name="daily", project="demo").tasks[1].flag == "NO"


def test_prepare_from_baseline_reuses_ids_and_preserves_unknown_fields() -> None:
    decoded = decode_legacy_workflow_graph(
        process_definition_json=(
            '{"globalParams":[],"tasks":['
            '{"id":"tasks-11","name":"extract","type":"SHELL",'
            '"desc":"old description","params":{"rawScript":"echo extract",'
            '"resourceList":[],"localParams":[]},"preTasks":[],'
            '"runFlag":"NORMAL","maxRetryTimes":0,"retryInterval":1,'
            '"taskInstancePriority":"MEDIUM","workerGroup":"default",'
            '"futureTaskField":{"keep":true}},'
            '{"id":"tasks-22","name":"load","type":"SHELL",'
            '"description":"","params":{"rawScript":"echo load",'
            '"resourceList":[],"localParams":[]},"preTasks":["extract"],'
            '"runFlag":"NORMAL","maxRetryTimes":0,"retryInterval":1,'
            '"taskInstancePriority":"MEDIUM","workerGroup":"default"}'
            '],"timeout":0,"tenantId":7,"futureProcessField":"keep"}'
        ),
        locations=(
            '{"tasks-11":{"name":"extract","targetarr":"",'
            '"nodenumber":1,"x":17,"y":23,"futureLocationField":"keep"},'
            '"tasks-22":{"name":"load","targetarr":"tasks-11",'
            '"nodenumber":0,"x":317,"y":23}}'
        ),
        connects=('[{"endPointSourceId":"tasks-11","endPointTargetId":"tasks-22"}]'),
    )
    spec = decoded.to_workflow_spec(name="daily", project="demo")
    assert spec.tasks[0].retry.interval == 0
    spec.tasks[0].description = "new description"
    assert spec.tasks[1].task_params is not None
    spec.tasks[1].task_params["rawScript"] = "echo changed"

    def unexpected_id(_name: str) -> str:
        msg = "baseline ids must be reused"
        raise AssertionError(msg)

    prepared = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=unexpected_id,
        baseline=decoded,
    )

    assert prepared.required_task_id_count == 0
    assert prepared.task_ids == (("extract", "tasks-11"), ("load", "tasks-22"))
    process_data = json.loads(prepared.preview()["processDefinitionJson"])
    assert process_data["tenantId"] == 7
    assert process_data["futureProcessField"] == "keep"
    assert process_data["tasks"][0]["futureTaskField"] == {"keep": True}
    assert process_data["tasks"][0]["desc"] == "new description"
    assert "description" not in process_data["tasks"][0]
    assert process_data["tasks"][1]["params"]["rawScript"] == "echo changed"
    assert "code" not in process_data["tasks"][0]
    assert "version" not in process_data["tasks"][0]
    location_data = json.loads(prepared.preview()["locations"])
    assert location_data["tasks-11"] == {
        "name": "extract",
        "targetarr": "",
        "nodenumber": 1,
        "x": 17,
        "y": 23,
        "futureLocationField": "keep",
    }


@pytest.mark.parametrize(
    ("workflow_update", "task_update", "unsupported_field"),
    [
        ({"execution_type": "SERIAL_WAIT"}, {}, "workflow.execution_type"),
        ({}, {"environment_code": 7}, "environment_code"),
        (
            {},
            {"task_group_id": 8, "task_group_priority": 1},
            "task_group_id",
        ),
        ({}, {"delay": 1}, "delay"),
        ({}, {"cpu_quota": 2}, "cpu_quota"),
        ({}, {"memory_max": 1024}, "memory_max"),
    ],
)
def test_prepare_rejects_canonical_fields_absent_from_ds_139(
    workflow_update: dict[str, object],
    task_update: dict[str, object],
    unsupported_field: str,
) -> None:
    document = _workflow_spec().model_dump(mode="json")
    document["workflow"].update(workflow_update)
    document["tasks"][0].update(task_update)
    spec = WorkflowSpec.model_validate(document)

    with pytest.raises(LegacyWorkflowGraphError, match=unsupported_field):
        prepare_legacy_workflow_graph(
            spec,
            task_id_factory=lambda name: f"tasks-{name}",
        )


@pytest.mark.parametrize(
    ("second_id", "second_name", "duplicate"),
    [
        ("tasks-22", "extract", "name 'extract'"),
        ("tasks-11", "load", "id 'tasks-11'"),
    ],
)
def test_decode_rejects_duplicate_native_task_identity(
    second_id: str,
    second_name: str,
    duplicate: str,
) -> None:
    process_data = {
        "globalParams": [],
        "tasks": [
            {
                "id": "tasks-11",
                "name": "extract",
                "type": "SHELL",
                "params": {"rawScript": "echo extract"},
                "preTasks": [],
            },
            {
                "id": second_id,
                "name": second_name,
                "type": "SHELL",
                "params": {"rawScript": "echo load"},
                "preTasks": [],
            },
        ],
        "timeout": 0,
    }

    with pytest.raises(LegacyWorkflowGraphError, match=duplicate):
        decode_legacy_workflow_graph(
            json.dumps(process_data),
            locations="{}",
            connects="[]",
        )


@pytest.mark.parametrize("stored_count", ["4", 0])
def test_decode_preserves_stale_ui_counter_and_prepare_recomputes_it(
    stored_count: str | int,
) -> None:
    process_definition_json = json.dumps(
        {
            "tasks": [
                {
                    "id": "tasks-extract",
                    "name": "extract",
                    "type": "SHELL",
                    "params": {"rawScript": "echo extract"},
                    "preTasks": [],
                },
                {
                    "id": "tasks-load",
                    "name": "load",
                    "type": "SHELL",
                    "params": {"rawScript": "echo load"},
                    "preTasks": ["extract"],
                },
            ],
        }
    )
    locations: dict[str, dict[str, str | int]] = {
        "tasks-extract": {
            "name": "extract",
            "targetarr": "",
            "nodenumber": stored_count,
            "x": 17,
            "y": 23,
            "futureLocationField": "keep",
        },
        "tasks-load": {
            "name": "load",
            "targetarr": "tasks-extract",
            "nodenumber": "0",
            "x": 317,
            "y": 23,
        },
    }
    decoded = decode_legacy_workflow_graph(
        process_definition_json,
        locations=json.dumps(locations),
        connects=(
            '[{"endPointSourceId":"tasks-extract","endPointTargetId":"tasks-load"}]'
        ),
    )

    assert decoded.edges == (("extract", "load"),)
    assert decoded.tasks[1].depends_on == ("extract",)
    assert decoded.native_locations == locations

    prepared = prepare_legacy_workflow_graph(
        decoded.to_workflow_spec(name="daily"),
        baseline=decoded,
    ).materialize()
    updated_locations = json.loads(prepared["locations"])
    assert updated_locations == {
        "tasks-extract": {**locations["tasks-extract"], "nodenumber": 1},
        "tasks-load": {**locations["tasks-load"], "nodenumber": 0},
    }
    assert decoded.native_locations == locations
    assert json.loads(prepared["connects"]) == [
        {"endPointSourceId": "tasks-extract", "endPointTargetId": "tasks-load"}
    ]


@pytest.mark.parametrize(
    ("locations", "connects", "conflicting_source"),
    [
        (
            '{"tasks-11":{"name":"extract","targetarr":"",'
            '"nodenumber":0,"x":0,"y":0},'
            '"tasks-22":{"name":"load","targetarr":"",'
            '"nodenumber":0,"x":300,"y":0}}',
            '[{"endPointSourceId":"tasks-11","endPointTargetId":"tasks-22"}]',
            "locations.targetarr",
        ),
        (
            '{"tasks-11":{"name":"extract","targetarr":"",'
            '"nodenumber":1,"x":0,"y":0},'
            '"tasks-22":{"name":"load","targetarr":"tasks-11",'
            '"nodenumber":0,"x":300,"y":0}}',
            "[]",
            "connects",
        ),
    ],
)
def test_decode_rejects_conflicts_between_all_three_native_graphs(
    locations: str,
    connects: str,
    conflicting_source: str,
) -> None:
    process_definition_json = (
        '{"globalParams":[],"tasks":['
        '{"id":"tasks-11","name":"extract","type":"SHELL",'
        '"params":{"rawScript":"echo extract"},"preTasks":[]},'
        '{"id":"tasks-22","name":"load","type":"SHELL",'
        '"params":{"rawScript":"echo load"},"preTasks":["extract"]}'
        '],"timeout":0}'
    )

    with pytest.raises(LegacyWorkflowGraphError, match=conflicting_source):
        decode_legacy_workflow_graph(
            process_definition_json,
            locations=locations,
            connects=connects,
        )
