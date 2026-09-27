from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.models import WorkflowSpec
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.lint import lint_workflow_result
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.legacy_workflow_graph import (
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.common import YamlObject


_MINIMAL_PROCEDURE_PARAMS: YamlObject = {
    "type": "MYSQL",
    "datasource": 7,
    "method": "{call refresh_daily()}",
    "localParams": [],
}

_PROCEDURE_PARAMETER_TYPES = [
    "VARCHAR",
    "INTEGER",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "BOOLEAN",
]


def _task_document(ds_version: str, variant: str) -> YamlObject:
    catalog = get_task_authoring_catalog(ds_version)
    if variant == "params":
        yaml_text = parameter_example_yaml("PROCEDURE", ds_version)
    else:
        result = task_template_result(
            "PROCEDURE",
            variant=None if variant == "minimal" else variant,
            catalog=catalog,
        )
        assert isinstance(result.data, dict)
        yaml_text = result.data["yaml"]
        assert isinstance(yaml_text, str)
    document = yaml.safe_load(yaml_text)
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _write_workflow(
    tmp_path: Path,
    *,
    ds_version: str,
    task: YamlObject,
    suffix: str,
) -> Path:
    path = tmp_path / f"procedure-{ds_version}-{suffix}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "workflow": {"name": f"procedure-{ds_version}-{suffix}"},
                "tasks": [task],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _procedure_workflow_spec(params: YamlObject) -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": "procedure-roundtrip"},
            "tasks": [
                {
                    "name": "call-procedure",
                    "type": "PROCEDURE",
                    "task_params": params,
                }
            ],
        }
    )


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_procedure_schema_is_typed_and_exact_profile_discoverable(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    result = task_type_schema_result("PROCEDURE", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert result.resolved == {"task_type": "PROCEDURE", "view": "fields"}
    assert result.data["kind"] == "typed"
    assert {
        "task_params.type",
        "task_params.datasource",
        "task_params.method",
    }.issubset(path for path, field in fields.items() if field.get("required") is True)
    assert "task_params.localParams[]" in fields
    assert fields["task_params.localParams[].type"]["choices"] == (
        _PROCEDURE_PARAMETER_TYPES
    )
    assert "task_params.outProperty" not in fields
    if ds_version == "1.3.9":
        assert "task_params.varPool[]" not in fields
        compile_prefix = "processDefinitionJson.tasks[].params."
    else:
        assert "task_params.varPool[]" in fields
        compile_prefix = "taskDefinitionJson[].taskParams."
    assert fields["task_params.method"]["compile_path"] == (f"{compile_prefix}method")


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.1"])
def test_procedure_json_schema_exposes_cross_field_and_runtime_constraints(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "PROCEDURE",
        json_schema=True,
        catalog=get_task_authoring_catalog(ds_version),
    )
    data = result.data
    assert isinstance(data, dict)
    schema = data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)
    method = properties["method"]
    assert isinstance(method, dict)
    method_metadata = method["x-dsctl"]
    assert isinstance(method_metadata, dict)

    assert task_params["additionalProperties"] is False
    assert method["pattern"]
    assert method_metadata["placeholder_count_matches"] == "localParams.length"
    if ds_version == "1.3.9":
        assert "varPool" not in properties
    else:
        var_pool = properties["varPool"]
        assert isinstance(var_pool, dict)
        assert var_pool["maxItems"] == 0
        assert "must remain empty" in str(var_pool["description"])


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_procedure_templates_validate_through_exact_profile_lint(
    tmp_path: Path,
    ds_version: str,
    variant: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    task = _task_document(ds_version, variant)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "PROCEDURE"
    assert {"type", "datasource", "method", "localParams"}.issubset(params)
    if variant == "minimal":
        assert params["localParams"] == []
    else:
        assert params["localParams"]
    if ds_version == "1.3.9":
        assert "varPool" not in params
    else:
        assert params["varPool"] == []

    path = _write_workflow(
        tmp_path,
        ds_version=ds_version,
        task=task,
        suffix=variant,
    )
    lint_result = lint_workflow_result(file=path, catalog=catalog)

    assert isinstance(lint_result.data, dict)
    assert lint_result.data["valid"] is True
    assert lint_result.data["summary"]["taskTypeCounts"] == {"PROCEDURE": 1}


@pytest.mark.parametrize(
    ("ds_version", "native_method"),
    [
        ("1.3.9", "refresh_daily"),
        ("2.0.0", "{call refresh_daily(?)}"),
        ("2.0.9", "{call refresh_daily(${bizdate})}"),
        ("3.0.0", "{call refresh_daily(${bizdate})}"),
        ("3.0.6", "{call refresh_daily(${bizdate})}"),
        ("3.1.0", "{call refresh_daily(${bizdate})}"),
        ("3.1.9", "{call refresh_daily(${bizdate})}"),
        ("3.2.0", "{call refresh_daily(${bizdate})}"),
        ("3.2.1", "{call refresh_daily(${bizdate})}"),
        ("3.2.2", "{call refresh_daily(${bizdate})}"),
        ("3.3.1", "{call refresh_daily(${bizdate})}"),
        ("3.3.2", "{call refresh_daily(${bizdate})}"),
        ("3.4.0", "{call refresh_daily(${bizdate})}"),
        ("3.4.1", "{call refresh_daily(${bizdate})}"),
        ("3.4.2", "{call refresh_daily(${bizdate})}"),
    ],
)
def test_procedure_params_template_compiles_to_exact_method_epoch(
    ds_version: str,
    native_method: str,
) -> None:
    task = _task_document(ds_version, "params")
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": f"procedure-{ds_version}-params"},
            "tasks": [task],
        }
    )

    if ds_version == "1.3.9":
        legacy_payload = prepare_legacy_workflow_graph(
            spec,
            task_id_factory=lambda _task_name: "tasks-procedure",
        ).materialize()
        process_definition = json.loads(legacy_payload["processDefinitionJson"])
        native_params = process_definition["tasks"][0]["params"]
    else:
        modern_payload = prepare_workflow_create_compilation(
            spec,
            catalog=get_task_authoring_catalog(ds_version),
        ).materialize([101])
        task_definition = json.loads(modern_payload["taskDefinitionJson"])[0]
        native_params = json.loads(task_definition["taskParams"])

    assert native_params["method"] == native_method
    assert native_params["localParams"] == [
        {
            "prop": "bizdate",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "${system.biz.date}",
        }
    ]
    assert ("varPool" in native_params) is (ds_version != "1.3.9")


def test_procedure_compiles_to_exact_139_legacy_graph_payload() -> None:
    spec = _procedure_workflow_spec(dict(_MINIMAL_PROCEDURE_PARAMS))

    payload = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _task_name: "tasks-procedure",
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])

    assert process_definition["tasks"][0]["type"] == "PROCEDURE"
    assert process_definition["tasks"][0]["params"] == {
        "type": "MYSQL",
        "datasource": 7,
        "method": "refresh_daily",
        "localParams": [],
    }


def test_procedure_compiles_to_exact_341_prepared_payload() -> None:
    params = {**_MINIMAL_PROCEDURE_PARAMS, "varPool": []}
    spec = _procedure_workflow_spec(params)

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=get_task_authoring_catalog("3.4.1"),
    ).materialize([34_101])
    task_definition = json.loads(payload["taskDefinitionJson"])[0]

    assert task_definition["taskType"] == "PROCEDURE"
    assert json.loads(task_definition["taskParams"]) == {
        "type": "MYSQL",
        "datasource": 7,
        "method": "{call refresh_daily()}",
        "localParams": [],
        "varPool": [],
    }


def test_procedure_139_allows_jdbc_out_without_var_pool(tmp_path: Path) -> None:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 7,
        "method": "{call reporting.refresh_daily(?)}",
        "localParams": [
            {
                "prop": "result",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "",
            }
        ],
    }
    task: YamlObject = {
        "name": "legacy-procedure-out",
        "type": "PROCEDURE",
        "task_params": params,
    }
    path = _write_workflow(
        tmp_path,
        ds_version="1.3.9",
        task=task,
        suffix="jdbc-out",
    )

    lint_result = lint_workflow_result(
        file=path,
        catalog=get_task_authoring_catalog("1.3.9"),
    )
    payload = prepare_legacy_workflow_graph(
        _procedure_workflow_spec(params),
        task_id_factory=lambda _task_name: "tasks-procedure",
    ).materialize()
    native_params = json.loads(payload["processDefinitionJson"])["tasks"][0]["params"]

    assert isinstance(lint_result.data, dict)
    assert lint_result.data["valid"] is True
    assert native_params == {
        **params,
        "method": "reporting.refresh_daily",
    }
    assert "varPool" not in native_params


@pytest.mark.parametrize(
    ("invalid_params", "field"),
    [
        (
            {**_MINIMAL_PROCEDURE_PARAMS, "type": "   "},
            "type",
        ),
        (
            {**_MINIMAL_PROCEDURE_PARAMS, "datasource": 0},
            "datasource",
        ),
        (
            {**_MINIMAL_PROCEDURE_PARAMS, "datasource": -1},
            "datasource",
        ),
        (
            {**_MINIMAL_PROCEDURE_PARAMS, "method": "   "},
            "method",
        ),
        (
            {**_MINIMAL_PROCEDURE_PARAMS, "futureField": True},
            "futureField",
        ),
        (
            {
                **_MINIMAL_PROCEDURE_PARAMS,
                "method": "{call refresh_daily(?)}",
                "localParams": [
                    {
                        "prop": "records",
                        "direct": "IN",
                        "type": "LIST",
                        "value": "[]",
                    }
                ],
            },
            "LIST",
        ),
        (
            {
                **_MINIMAL_PROCEDURE_PARAMS,
                "method": "{call refresh_daily(?)}",
                "localParams": [
                    {
                        "prop": "attachment",
                        "direct": "IN",
                        "type": "FILE",
                        "value": "input.csv",
                    }
                ],
            },
            "FILE",
        ),
        (
            {
                **_MINIMAL_PROCEDURE_PARAMS,
                "method": "{call refresh_daily(?)}",
            },
            "placeholder count",
        ),
        (
            {
                **_MINIMAL_PROCEDURE_PARAMS,
                "varPool": [
                    {
                        "prop": "result",
                        "direct": "OUT",
                        "type": "VARCHAR",
                        "value": "ready",
                    }
                ],
            },
            "varPool",
        ),
    ],
    ids=[
        "blank-type",
        "zero-datasource",
        "negative-datasource",
        "blank-method",
        "unknown-field",
        "list-parameter",
        "file-parameter",
        "placeholder-mismatch",
        "nonempty-var-pool",
    ],
)
def test_procedure_typed_authoring_fails_closed_for_invalid_params(
    tmp_path: Path,
    invalid_params: YamlObject,
    field: str,
) -> None:
    path = _write_workflow(
        tmp_path,
        ds_version="3.4.1",
        task={
            "name": "invalid-procedure",
            "type": "PROCEDURE",
            "task_params": invalid_params,
        },
        suffix=field,
    )

    result = lint_workflow_result(
        file=path,
        catalog=get_task_authoring_catalog("3.4.1"),
    )

    assert isinstance(result.failure, UserInputError)
    assert field in result.failure.message


def test_procedure_139_typed_authoring_rejects_var_pool(tmp_path: Path) -> None:
    path = _write_workflow(
        tmp_path,
        ds_version="1.3.9",
        task={
            "name": "legacy-procedure",
            "type": "PROCEDURE",
            "task_params": {**_MINIMAL_PROCEDURE_PARAMS, "varPool": []},
        },
        suffix="var-pool",
    )

    result = lint_workflow_result(
        file=path,
        catalog=get_task_authoring_catalog("1.3.9"),
    )

    assert isinstance(result.failure, UserInputError)
    assert result.failure.details["field"] == "tasks[].task_params.varPool"


def test_procedure_compile_rejects_duplicate_local_param_names() -> None:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 7,
        "method": "{call reporting.refresh_daily(?,?)}",
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "2026-08-19",
            },
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "2026-08-20",
            },
        ],
        "varPool": [],
    }

    with pytest.raises(UserInputError) as captured:
        prepare_workflow_create_compilation(
            _procedure_workflow_spec(params),
            catalog=get_task_authoring_catalog("3.4.1"),
        )

    assert captured.value.details["reason"] == "duplicate-local-param-name"


def test_procedure_139_native_export_is_canonical_and_recompiles_exactly() -> None:
    native_params = {
        "type": "MYSQL",
        "datasource": 7,
        "method": "reporting.refresh_daily",
        "localParams": [
            {
                "prop": "result",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "ready",
            }
        ],
    }
    decoded = decode_legacy_workflow_graph(
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "tasks-procedure",
                        "name": "call-procedure",
                        "type": "PROCEDURE",
                        "params": native_params,
                        "preTasks": [],
                    }
                ],
                "timeout": 0,
                "tenantId": -1,
            }
        ),
        locations=json.dumps(
            {
                "tasks-procedure": {
                    "name": "call-procedure",
                    "targetarr": "",
                    "nodenumber": 0,
                    "x": 0,
                    "y": 0,
                }
            }
        ),
        connects="[]",
    )

    document = decoded.workflow_document(name="procedure-roundtrip")
    tasks = document["tasks"]
    assert isinstance(tasks, list)
    exported_task = tasks[0]
    assert isinstance(exported_task, dict)
    exported_params = exported_task["task_params"]
    assert isinstance(exported_params, dict)
    assert exported_params == {
        **native_params,
        "method": "{call reporting.refresh_daily(?)}",
    }
    assert "varPool" not in exported_params

    compiled = prepare_legacy_workflow_graph(
        decoded.to_workflow_spec(name="procedure-roundtrip"),
        baseline=decoded,
    ).materialize()
    recompiled_params = json.loads(compiled["processDefinitionJson"])["tasks"][0][
        "params"
    ]

    assert recompiled_params == native_params


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_procedure_known_runtime_state_is_removed_from_authored_payloads(
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.4.0")

    normalized = catalog.normalize_task_params(
        "PROCEDURE",
        {
            **_MINIMAL_PROCEDURE_PARAMS,
            "type": " mysql ",
            "varPool": [],
            "outProperty": {"row_count": "7"},
        },
        intent=intent,
    )

    assert normalized == {**_MINIMAL_PROCEDURE_PARAMS, "varPool": []}
