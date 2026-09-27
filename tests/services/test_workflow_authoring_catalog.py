import json
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    empty_task_adapter,
)
from tests.runtime_instance_domain_fakes import install_runtime_instance_domain_runtime
from tests.services._task_authoring_prep import parameter_example_yaml
from tests.support import make_profile
from tests.workflow_domain_fakes import install_workflow_domain_runtime

from dsctl.errors import ConfigError, UnsupportedFeatureError, UserInputError
from dsctl.models.common import GlobalParamSpec, YamlObject
from dsctl.models.workflow_patch import validate_workflow_patch_document
from dsctl.models.workflow_spec import WorkflowSpec, validate_workflow_document
from dsctl.output import require_json_object
from dsctl.services import _task_templates
from dsctl.services import workflow as workflow_service
from dsctl.services import workflow_instance as workflow_instance_service
from dsctl.services._workflow import authoring as authoring_service
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.mutation import (
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.lint import lint_workflow_result
from dsctl.services.selection import ResourceDefaults
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringCatalog,
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream import Availability, Verification, get_action_capability
from dsctl.upstream.resolver import ResolvedProject

_INLINE_SQL_PARAMS: YamlObject = {
    "type": "MYSQL",
    "datasource": 1,
    "sql": "  select 1;  ",
    "sqlType": 0,
}
_RESOURCE_SQL_PARAMS: YamlObject = {
    "type": "MYSQL",
    "datasource": 1,
    "sqlType": 0,
    "sqlSource": "FILE",
    "sqlResource": "/sql/report.sql",
    "futureNativeField": {"enabled": True, "revision": 7},
}


def _workflow_document(task_params: YamlObject) -> YamlObject:
    return {
        "workflow": {"name": "sql-report", "project": "analytics"},
        "tasks": [
            {
                "name": "report",
                "type": "SQL",
                "task_params": task_params,
            }
        ],
    }


def _resource_sql_dag() -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=101,
            name="sql-report",
            project_code_value=7,
            project_name_value="analytics",
            execution_type_value=FakeEnumValue("PARALLEL"),
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=201,
                name="report",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value="SQL",
                task_params_value=json.dumps(_RESOURCE_SQL_PARAMS),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )


def _project() -> ResolvedProject:
    return ResolvedProject(code=7, name="analytics", description=None)


def _selected_profile_file(tmp_path: Path, version: str) -> Path:
    path = tmp_path / f"ds-{version}.env"
    path.write_text(
        f"DS_VERSION={version}\nDS_API_URL=http://example.test/dolphinscheduler\n"
        "DS_API_TOKEN=test-token\n",
        encoding="utf-8",
    )
    return path


def _resource_workflow_file(tmp_path: Path) -> Path:
    path = tmp_path / "resource-workflow.yaml"
    path.write_text(
        yaml.safe_dump(
            _workflow_document(dict(_RESOURCE_SQL_PARAMS)),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _inline_workflow_file(tmp_path: Path) -> Path:
    path = tmp_path / "inline-workflow.yaml"
    path.write_text(
        yaml.safe_dump(
            _workflow_document(dict(_INLINE_SQL_PARAMS)),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _resource_patch_file(tmp_path: Path) -> Path:
    path = tmp_path / "resource-workflow.patch.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "patch": {
                    "tasks": {
                        "create": [
                            {
                                "name": "new-report",
                                "type": "SQL",
                                "task_params": dict(_RESOURCE_SQL_PARAMS),
                            }
                        ]
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _metadata_patch_file(tmp_path: Path) -> Path:
    path = tmp_path / "metadata-workflow.patch.yaml"
    path.write_text(
        yaml.safe_dump(
            {"patch": {"workflow": {"set": {"timeout": 45}}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def test_selected_catalog_controls_parse_normalization_without_global_state() -> None:
    normalized_by_version: dict[str, object] = {}
    for version in ("3.4.1", "3.4.2"):
        context = workflow_authoring_context(
            catalog=get_task_authoring_catalog(version),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        spec = validate_workflow_document(
            _workflow_document(dict(_INLINE_SQL_PARAMS)),
            authoring_context=context,
        )
        normalized_by_version[version] = spec.tasks[0].task_params

    assert normalized_by_version["3.4.1"] == normalized_by_version["3.4.2"]
    assert normalized_by_version["3.4.2"] == {
        "type": "MYSQL",
        "datasource": 1,
        "sql": "select 1;",
        "sqlType": 0,
        "preStatements": [],
        "postStatements": [],
        "localParams": [],
        "varPool": [],
    }

    preserve_context = workflow_authoring_context(
        catalog=get_task_authoring_catalog("3.4.2"),
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    preserved = validate_workflow_document(
        _workflow_document(dict(_RESOURCE_SQL_PARAMS)),
        authoring_context=preserve_context,
    )
    assert preserved.tasks[0].task_params == _RESOURCE_SQL_PARAMS

    typed_context = workflow_authoring_context(
        catalog=get_task_authoring_catalog("3.4.2"),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    with pytest.raises(UnsupportedFeatureError):
        validate_workflow_document(
            _workflow_document(dict(_RESOURCE_SQL_PARAMS)),
            authoring_context=typed_context,
        )


@pytest.mark.parametrize(
    ("version", "data_type", "supported"),
    [
        ("1.3.9", "LIST", False),
        ("2.0.0", "LIST", True),
        ("2.0.0", "FILE", False),
        ("3.1.9", "FILE", False),
        ("3.2.0", "FILE", True),
        ("3.4.2", "FILE", True),
    ],
)
def test_workflow_global_parameter_data_types_follow_the_exact_profile(
    version: str,
    data_type: str,
    supported: bool,  # noqa: FBT001
) -> None:
    document: YamlObject = {
        "workflow": {
            "name": "parameter-types",
            "global_params": [
                {
                    "prop": "items",
                    "value": "[]",
                    "direct": "IN",
                    "type": data_type,
                }
            ],
        },
        "tasks": [
            {
                "name": "consume",
                "type": "SHELL",
                "command": "printf '%s\\n' \"${items}\"",
            }
        ],
    }
    context = workflow_authoring_context(
        catalog=get_task_authoring_catalog(version),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    if supported:
        accepted = validate_workflow_document(
            document,
            authoring_context=context,
        )
        global_params = accepted.workflow.global_params
        assert isinstance(global_params, list)
        assert global_params[0].type.value == data_type
        return

    with pytest.raises(UnsupportedFeatureError) as captured:
        validate_workflow_document(
            document,
            authoring_context=context,
        )

    assert captured.value.details["selected_version"] == version
    assert captured.value.details["field"] == "workflow.global_params[].type"
    assert captured.value.details["value"] == data_type
    allowed_values = captured.value.details["allowed_values"]
    assert isinstance(allowed_values, list)
    assert data_type not in allowed_values


def test_parameter_semantics_reject_absent_output_and_task_fields() -> None:
    legacy = get_task_authoring_catalog("1.3.9")

    with pytest.raises(UnsupportedFeatureError) as global_error:
        legacy.validate_global_params(
            [
                GlobalParamSpec(
                    prop="result",
                    direct="OUT",
                    type="VARCHAR",
                )
            ]
        )
    with pytest.raises(UnsupportedFeatureError) as task_type_error:
        legacy.validate_authored_task_params(
            "SHELL",
            {
                "rawScript": "true",
                "localParams": [
                    {
                        "prop": "items",
                        "direct": "IN",
                        "type": "LIST",
                        "value": "[]",
                    }
                ],
            },
        )
    with pytest.raises(UnsupportedFeatureError) as var_pool_error:
        legacy.validate_authored_task_params(
            "SHELL",
            {"rawScript": "true", "varPool": []},
        )

    assert global_error.value.details["field"] == "workflow.global_params[].direct"
    assert global_error.value.details["value"] == "OUT"
    assert task_type_error.value.details["field"] == (
        "tasks[].task_params.localParams[].type"
    )
    assert task_type_error.value.details["value"] == "LIST"
    assert var_pool_error.value.details["field"] == "tasks[].task_params.varPool"


@pytest.mark.parametrize(
    ("version", "task_params", "field"),
    [
        (
            "1.3.9",
            {"rawScript": "true", "varPool": []},
            "tasks[].task_params.varPool",
        ),
        (
            "2.0.0",
            {
                "rawScript": "true",
                "localParams": [{"prop": "file", "direct": "IN", "type": "FILE"}],
            },
            "tasks[].task_params.localParams[].type",
        ),
    ],
)
def test_unreviewed_native_payloads_still_get_cross_cutting_parameter_gates(
    version: str,
    task_params: YamlObject,
    field: str,
) -> None:
    context = workflow_authoring_context(
        catalog=get_task_authoring_catalog(version),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    with pytest.raises(UnsupportedFeatureError) as captured:
        validate_workflow_document(
            {
                "workflow": {"name": "parameter-gate"},
                "tasks": [
                    {"name": "task", "type": "SHELL", "task_params": task_params}
                ],
            },
            authoring_context=context,
        )

    assert captured.value.details["selected_version"] == version
    assert captured.value.details["field"] == field


def test_opaque_preservation_does_not_apply_authored_parameter_gates() -> None:
    params: YamlObject = {
        "rawScript": "true",
        "varPool": [{"prop": "future", "direct": "OUT", "type": "LIST", "value": "[]"}],
    }

    preserved = get_task_authoring_catalog("1.3.9").normalize_task_params(
        "SHELL",
        params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == params
    assert preserved is not params


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_conditions_typed_authoring_rejects_runtime_condition_success(
    intent: TaskAuthoringIntent,
) -> None:
    params: YamlObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [{"task": "upstream-task", "status": "SUCCESS"}],
                }
            ],
        },
        "conditionResult": {
            "conditionSuccess": True,
            "successNode": ["on-success"],
            "failedNode": ["on-failed"],
        },
    }

    with pytest.raises(ValueError, match=r"conditionResult\.conditionSuccess"):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "CONDITIONS",
            params,
            intent=intent,
        )


def test_conditions_opaque_preserve_keeps_runtime_condition_success() -> None:
    params: YamlObject = {
        "dependence": {"futureNativeShape": True},
        "conditionResult": {"conditionSuccess": True},
    }

    preserved = get_task_authoring_catalog("3.4.1").normalize_task_params(
        "CONDITIONS",
        params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == params
    assert preserved is not params
    assert preserved["conditionResult"] is not params["conditionResult"]


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
@pytest.mark.parametrize(
    ("task_type", "task_payload"),
    [
        (
            "SEATUNNEL",
            {
                "task_params": {
                    "rawScript": (
                        "env { execution.parallelism = 1 }\n"
                        "source { FakeSource {} }\n"
                        "sink { Console {} }\n"
                    ),
                }
            },
        ),
    ],
)
def test_342_workflow_authoring_uses_typed_mode_for_reviewed_seatunnel(
    intent: TaskAuthoringIntent,
    task_type: str,
    task_payload: YamlObject,
) -> None:
    document: YamlObject = {
        "workflow": {"name": "shell-workflow"},
        "tasks": [
            {
                "name": "shell-task",
                "type": task_type,
                **task_payload,
            }
        ],
    }

    accepted = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.2"),
            intent=intent,
        ),
    )

    assert accepted.tasks[0].type == task_type
    if "task_params" in task_payload:
        assert accepted.tasks[0].task_params == task_payload["task_params"]


@pytest.mark.parametrize(
    "version",
    [
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.3.1",
        "3.4.2",
    ],
)
def test_reviewed_exact_shell_fingerprints_are_strict_typed_authoring(
    version: str,
) -> None:
    context = workflow_authoring_context(
        catalog=get_task_authoring_catalog(version),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    with pytest.raises(
        ValueError,
        match=r"unsupported fields for typed authoring: futureNativeField",
    ):
        validate_workflow_document(
            {
                "workflow": {"name": "strict-shell-workflow"},
                "tasks": [
                    {
                        "name": "shell-task",
                        "type": "SHELL",
                        "task_params": {
                            "rawScript": "echo strict",
                            "futureNativeField": True,
                        },
                    }
                ],
            },
            authoring_context=context,
        )


@pytest.mark.parametrize(
    ("version", "task_type"),
    [
        ("3.2.0", "SHELL"),
        ("3.2.0", "PYTHON"),
        ("3.2.0", "REMOTESHELL"),
        ("3.2.0", "SQL"),
        ("3.2.1", "SHELL"),
        ("3.2.1", "PYTHON"),
        ("3.2.1", "REMOTESHELL"),
        ("3.2.1", "SQL"),
        ("3.2.2", "SQL"),
    ],
)
def test_32x_canonical_subset_templates_compile_without_unowned_fields(
    version: str,
    task_type: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    assert catalog.supports_typed_authoring(task_type) is True
    task_document = yaml.safe_load(
        _task_templates.task_template_yaml(task_type, catalog=catalog)
    )
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"{task_type.lower()}-{version}"},
            "tasks": [task_document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([201])
    task_definition = json.loads(payload["taskDefinitionJson"])[0]
    task_params = json.loads(task_definition["taskParams"])
    allowed_fields = {
        "SHELL": {"rawScript", "localParams", "varPool", "resourceList"},
        "PYTHON": {"rawScript", "localParams", "varPool", "resourceList"},
        "REMOTESHELL": {
            "rawScript",
            "type",
            "datasource",
            "localParams",
            "varPool",
        },
        "SQL": {
            "type",
            "datasource",
            "sql",
            "sqlType",
            "sendEmail",
            "displayRows",
            "showType",
            "connParams",
            "preStatements",
            "postStatements",
            "groupId",
            "title",
            "limit",
            "localParams",
            "varPool",
        },
    }

    assert set(task_params) <= allowed_fields[task_type]
    assert "rawScript" in task_params or {
        "type",
        "datasource",
        "sql",
        "sqlType",
    } <= set(task_params)
    assert "udfs" not in task_params
    assert "sqlSource" not in task_params
    assert "sqlResource" not in task_params


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2"])
def test_32x_inline_sql_typed_authoring_rejects_unowned_udfs(
    version: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=r"unsupported fields for typed authoring: udfs",
    ):
        validate_workflow_document(
            _workflow_document({**_INLINE_SQL_PARAMS, "udfs": "legacy_udf"}),
            authoring_context=workflow_authoring_context(
                catalog=get_task_authoring_catalog(version),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )


@pytest.mark.parametrize(
    ("version", "task_type"),
    [
        (version, task_type)
        for version in ("2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9")
        for task_type in ("SHELL", "PYTHON", "SQL")
    ]
    + [("3.2.1", "HTTP"), ("3.2.2", "HTTP")],
)
def test_legacy_simple_subset_templates_compile_without_unowned_fields(
    version: str,
    task_type: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    assert catalog.supports_typed_authoring(task_type) is True
    task_document = yaml.safe_load(
        _task_templates.task_template_yaml(task_type, catalog=catalog)
    )
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"{task_type.lower()}-{version}"},
            "tasks": [task_document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(spec, catalog=catalog).materialize(
        [201]
    )
    task_definition = json.loads(payload["taskDefinitionJson"])[0]
    task_params = json.loads(task_definition["taskParams"])

    assert "udfs" not in task_params
    assert "segmentSeparator" not in task_params
    if task_type == "HTTP":
        assert task_params["socketTimeout"] == 60000
        assert "httpBody" in task_params
    elif task_type == "SQL":
        assert "socketTimeout" not in task_params
        assert {"type", "datasource", "sql", "sqlType"} <= set(task_params)
    else:
        assert "socketTimeout" not in task_params
        assert "rawScript" in task_params


@pytest.mark.parametrize(
    ("version", "field"),
    [
        ("2.0.0", "udfs"),
        ("2.0.9", "udfs"),
        ("3.0.0", "udfs"),
        ("3.0.0", "segmentSeparator"),
        ("3.0.6", "udfs"),
        ("3.0.6", "segmentSeparator"),
        ("3.1.0", "udfs"),
        ("3.1.0", "segmentSeparator"),
        ("3.1.9", "udfs"),
        ("3.1.9", "segmentSeparator"),
    ],
)
def test_20_to_31_sql_typed_authoring_rejects_unowned_upstream_fields(
    version: str,
    field: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"unsupported fields for typed authoring: {field}",
    ):
        validate_workflow_document(
            _workflow_document({**_INLINE_SQL_PARAMS, field: "opaque-native-value"}),
            authoring_context=workflow_authoring_context(
                catalog=get_task_authoring_catalog(version),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )


@pytest.mark.parametrize("version", ["3.2.1", "3.2.2"])
def test_32x_http_typed_authoring_rejects_unowned_socket_timeout(
    version: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=r"unsupported fields for typed authoring: socketTimeout",
    ):
        validate_workflow_document(
            {
                "workflow": {"name": "strict-http"},
                "tasks": [
                    {
                        "name": "http-task",
                        "type": "HTTP",
                        "task_params": {
                            "url": "https://example.test/health",
                            "httpMethod": "GET",
                            "connectTimeout": 10000,
                            "socketTimeout": 10000,
                        },
                    }
                ],
            },
            authoring_context=workflow_authoring_context(
                catalog=get_task_authoring_catalog(version),
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_341_legacy_authoring_memberships_are_finite_and_mode_specific(
    intent: TaskAuthoringIntent,
) -> None:
    context = workflow_authoring_context(
        catalog=get_task_authoring_catalog("3.4.1"),
        intent=intent,
    )
    accepted = validate_workflow_document(
        {
            "workflow": {"name": "shell-workflow"},
            "tasks": [
                {
                    "name": "shell-task",
                    "type": "SHELL",
                    "command": "echo explicit-legacy-fallback",
                }
            ],
        },
        authoring_context=context,
    )

    assert accepted.tasks[0].command == "echo explicit-legacy-fallback"

    sqoop_params: YamlObject = {
        "subcommand": "import",
        "args": ["--table", "orders", "--target-dir", "hdfs:///orders"],
    }
    sqoop = validate_workflow_document(
        {
            "workflow": {"name": "sqoop-workflow"},
            "tasks": [
                {
                    "name": "sqoop-task",
                    "type": "SQOOP",
                    "task_params": sqoop_params,
                }
            ],
        },
        authoring_context=context,
    )

    assert sqoop.tasks[0].task_params == sqoop_params
    assert sqoop.tasks[0].task_params is not sqoop_params

    with pytest.raises(UnsupportedFeatureError) as captured:
        validate_workflow_document(
            {
                "workflow": {"name": "future-workflow"},
                "tasks": [
                    {
                        "name": "future-task",
                        "type": "FUTURE_PLUGIN",
                        "task_params": {"sql": "select 1"},
                    }
                ],
            },
            authoring_context=context,
        )

    assert captured.value.details["selected_version"] == "3.4.1"
    assert captured.value.details["task_type"] == "FUTURE_PLUGIN"
    assert captured.value.details["intent"] in {"opaque_create", "opaque_edit"}

    with pytest.raises(
        ValueError,
        match=r"unsupported fields for typed authoring: futureNativeField",
    ):
        validate_workflow_document(
            {
                "workflow": {"name": "shell-workflow"},
                "tasks": [
                    {
                        "name": "shell-task",
                        "type": "SHELL",
                        "task_params": {
                            "rawScript": "echo strict",
                            "futureNativeField": True,
                        },
                    }
                ],
            },
            authoring_context=context,
        )


def test_opaque_preserve_keeps_unknown_task_types_and_fields_losslessly() -> None:
    task_params: YamlObject = {
        "sql": "select 1",
        "futureNativeField": {"enabled": True},
    }
    spec = validate_workflow_document(
        {
            "workflow": {"name": "future-workflow"},
            "tasks": [
                {
                    "name": "future-task",
                    "type": "FUTURE_PLUGIN",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.2"),
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )

    assert spec.tasks[0].task_params == task_params
    assert spec.tasks[0].task_params is not task_params


def test_compile_selects_equivalent_inline_profiles_and_blocks_resource_first() -> None:
    inline_spec = WorkflowSpec.model_validate(
        _workflow_document(dict(_INLINE_SQL_PARAMS))
    )
    payloads = [
        prepare_workflow_create_compilation(
            inline_spec,
            catalog=get_task_authoring_catalog(version),
        ).materialize([201])
        for version in ("3.4.1", "3.4.2")
    ]
    assert payloads[0] == payloads[1]

    resource_spec = validate_workflow_document(
        _workflow_document(dict(_RESOURCE_SQL_PARAMS)),
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.2"),
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )

    with pytest.raises(UnsupportedFeatureError):
        prepare_workflow_create_compilation(
            resource_spec,
            catalog=get_task_authoring_catalog("3.4.2"),
        )


def test_inline_sql_template_corpus_compiles_equivalently() -> None:
    catalogs = [get_task_authoring_catalog(version) for version in ("3.4.1", "3.4.2")]
    # Public defaults and coupled scenarios are complete task fragments. Ordinary
    # lifecycle input is an independent field fixture, not an alternate factory.
    templates = [
        ("default", _task_templates.task_template_yaml("SQL", catalog=catalogs[0])),
        *[
            (
                name,
                _task_templates.task_template_yaml(
                    "SQL", variant=name, catalog=catalogs[0]
                ),
            )
            for name in _task_templates.task_template_variants(
                "SQL", catalog=catalogs[0]
            )
        ],
        (
            "lifecycle-input",
            parameter_example_yaml("SQL", "3.4.1", example="pre-post-statements"),
        ),
    ]

    for name, template in templates:
        task_document = yaml.safe_load(template)
        spec = WorkflowSpec.model_validate(
            {
                "workflow": {"name": f"sql-{name}"},
                "tasks": [task_document],
            }
        )
        payloads = [
            prepare_workflow_create_compilation(
                spec,
                catalog=catalog,
            ).materialize([201])
            for catalog in catalogs
        ]

        assert payloads[0] == payloads[1]
        if name == "lifecycle-input":
            tasks = json.loads(payloads[0]["taskDefinitionJson"])
            params = json.loads(tasks[0]["taskParams"])
            assert params["sqlType"] == 1
            assert params["preStatements"] == [
                "set session sql_mode = 'STRICT_TRANS_TABLES'"
            ]
            assert params["postStatements"] == ["analyze table target_table"]


def test_lint_uses_the_selected_catalog_before_compilation(tmp_path: Path) -> None:
    path = tmp_path / "workflow.yaml"
    path.write_text(
        yaml.safe_dump(
            _workflow_document(dict(_RESOURCE_SQL_PARAMS)),
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = lint_workflow_result(
        file=path,
        catalog=get_task_authoring_catalog("3.4.2"),
    )

    assert isinstance(result.failure, UserInputError)
    assert isinstance(result.data, dict)
    assert result.data["valid"] is False


def test_export_and_live_baseline_preserve_opaque_resource_sql_fields() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    dag = _resource_sql_dag()

    exported = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_project(),
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert exported["tasks"][0]["task_params"] == _RESOURCE_SQL_PARAMS

    baseline = workflow_live_baseline(dag, project=_project(), catalog=catalog)
    assert baseline.spec.tasks[0].task_params == _RESOURCE_SQL_PARAMS


def test_workflow_export_selects_catalog_from_runtime_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _resource_sql_dag()
    workflow = dag.workflow_definition_value
    assert workflow is not None
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="analytics")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={101: dag},
    )
    selected_versions: list[str] = []
    real_catalog_for_version = authoring_service.workflow_authoring_catalog_for_version

    def capture_catalog(ds_version: str) -> TaskAuthoringCatalog:
        selected_versions.append(ds_version)
        return real_catalog_for_version(ds_version)

    monkeypatch.setattr(
        workflow_authoring_service,
        "workflow_authoring_catalog_for_version",
        capture_catalog,
    )
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=empty_task_adapter(),
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.export_workflow_yaml_result(
        "sql-report",
        project="analytics",
    )

    result_data = require_json_object(result.data, label="workflow export result")
    document = yaml.safe_load(str(result_data["yaml"]))
    assert selected_versions == ["3.4.2"]
    assert document["tasks"][0]["task_params"] == _RESOURCE_SQL_PARAMS


def test_workflow_instance_export_selects_catalog_from_runtime_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _resource_sql_dag()
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="analytics")]
    )
    instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                dag_data_value=dag,
            )
        ]
    )
    selected_versions: list[str] = []
    real_catalog_for_version = authoring_service.workflow_authoring_catalog_for_version

    def capture_catalog(ds_version: str) -> TaskAuthoringCatalog:
        selected_versions.append(ds_version)
        return real_catalog_for_version(ds_version)

    monkeypatch.setattr(
        workflow_authoring_service,
        "workflow_authoring_catalog_for_version",
        capture_catalog,
    )
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_instance_adapter=instance_adapter,
        context=ResourceDefaults(project="analytics"),
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_instance_service.export_workflow_instance_yaml_result(901)

    result_data = require_json_object(
        result.data,
        label="workflow instance export result",
    )
    document = yaml.safe_load(str(result_data["yaml"]))
    assert selected_versions == ["3.4.2"]
    assert document["tasks"][0]["task_params"] == _RESOURCE_SQL_PARAMS


def test_dry_run_plan_preserves_opaque_resource_sql_on_metadata_edit() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    patch = validate_workflow_patch_document(
        {"patch": {"workflow": {"set": {"timeout": 45}}}},
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    ).patch

    plan = prepare_workflow_mutation_plan(
        _resource_sql_dag(),
        project=_project(),
        patch=patch,
        release_state="OFFLINE",
        catalog=catalog,
    )

    compiled_tasks = json.loads(plan.compilation.preview()["taskDefinitionJson"])
    assert plan.has_changes is True
    assert plan.merged_spec.tasks[0].task_params == _RESOURCE_SQL_PARAMS
    assert json.loads(compiled_tasks[0]["taskParams"]) == _RESOURCE_SQL_PARAMS


def test_mutation_blocks_typed_resource_sql_task_before_allocation() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    patch = validate_workflow_patch_document(
        {
            "patch": {
                "tasks": {
                    "create": [
                        {
                            "name": "new-report",
                            "type": "SQL",
                            "task_params": dict(_RESOURCE_SQL_PARAMS),
                        }
                    ]
                }
            }
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    ).patch

    with pytest.raises(UnsupportedFeatureError):
        prepare_workflow_mutation_plan(
            _resource_sql_dag(),
            project=_project(),
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )


def test_full_file_edit_preserves_resource_facet_and_blocks_authorship() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    baseline = workflow_live_baseline(
        _resource_sql_dag(),
        project=_project(),
        catalog=catalog,
    ).spec
    desired_metadata_edit = baseline.model_copy(
        update={"workflow": baseline.workflow.model_copy(update={"timeout": 45})},
        deep=True,
    )

    preserved_plan = prepare_workflow_file_mutation_plan(
        _resource_sql_dag(),
        project=_project(),
        desired=desired_metadata_edit,
        release_state="OFFLINE",
        catalog=catalog,
    )
    preserved_tasks = json.loads(
        preserved_plan.compilation.preview()["taskDefinitionJson"]
    )
    assert json.loads(preserved_tasks[0]["taskParams"]) == _RESOURCE_SQL_PARAMS

    changed_params = dict(_RESOURCE_SQL_PARAMS)
    changed_params["sqlResource"] = "/sql/changed.sql"
    changed_task = baseline.tasks[0].model_copy(
        update={"task_params": changed_params},
        deep=True,
    )
    authored_resource_edit = baseline.model_copy(
        update={"tasks": [changed_task]},
        deep=True,
    )

    with pytest.raises(UnsupportedFeatureError):
        prepare_workflow_file_mutation_plan(
            _resource_sql_dag(),
            project=_project(),
            desired=authored_resource_edit,
            release_state="OFFLINE",
            catalog=catalog,
        )


def test_workflow_create_loads_authoring_catalog_from_selected_profile(
    tmp_path: Path,
) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")

    with pytest.raises(UnsupportedFeatureError) as captured:
        workflow_service.create_workflow_result(
            file=_resource_workflow_file(tmp_path),
            env_file=str(profile_file),
        )

    assert captured.value.details["selected_version"] == "3.4.2"
    assert captured.value.details["intent"] == "typed_create"


def test_workflow_create_binds_one_immutable_authoring_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="analytics")]
    )
    selected_versions: list[str] = []
    real_catalog_for_version = authoring_service.workflow_authoring_catalog_for_version

    def capture_catalog(ds_version: str) -> TaskAuthoringCatalog:
        selected_versions.append(ds_version)
        return real_catalog_for_version(ds_version)

    monkeypatch.setattr(
        authoring_service,
        "workflow_authoring_catalog_for_version",
        capture_catalog,
    )
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        datasource_adapter=FakeDataSourceAdapter(
            [
                FakeDataSource(
                    id=1,
                    name="analytics-mysql",
                    type_value=FakeEnumValue("MYSQL"),
                )
            ]
        ),
        workflow_adapter=FakeWorkflowAdapter(workflows=[], dags={}),
        task_adapter=empty_task_adapter(),
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.create_workflow_result(
        file=_inline_workflow_file(tmp_path),
        project="analytics",
        dry_run=True,
        env_file=str(profile_file),
    )

    assert selected_versions == ["3.4.2"]
    result_data = require_json_object(result.data, label="workflow create result")
    assert result_data["dry_run"] is True


def test_workflow_create_rejects_runtime_profile_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="analytics")]
    )
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=FakeWorkflowAdapter(workflows=[], dags={}),
        task_adapter=empty_task_adapter(),
        profile=make_profile(ds_version="3.4.1"),
    )

    with pytest.raises(ConfigError) as captured:
        workflow_service.create_workflow_result(
            file=_inline_workflow_file(tmp_path),
            project="analytics",
            dry_run=True,
            env_file=str(profile_file),
        )

    assert captured.value.details == {
        "authoring_version": "3.4.2",
        "runtime_version": "3.4.1",
    }


def test_workflow_edits_load_authoring_catalog_from_selected_profile(
    tmp_path: Path,
) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")
    patch_file = _resource_patch_file(tmp_path)

    with pytest.raises(UnsupportedFeatureError) as workflow_error:
        workflow_service.edit_workflow_result(
            "sql-report",
            patch=patch_file,
            env_file=str(profile_file),
        )
    with pytest.raises(UnsupportedFeatureError) as instance_error:
        workflow_instance_service.edit_workflow_instance_result(
            901,
            patch=patch_file,
            env_file=str(profile_file),
        )

    assert workflow_error.value.details["selected_version"] == "3.4.2"
    assert workflow_error.value.details["intent"] == "typed_edit"
    assert instance_error.value.details["selected_version"] == "3.4.2"
    assert instance_error.value.details["intent"] == "typed_edit"


def test_workflow_instance_edit_reuses_selected_catalog_for_runtime_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="analytics")]
    )
    instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                dag_data_value=_resource_sql_dag(),
                state_value=FakeEnumValue("SUCCESS"),
            )
        ]
    )
    selected_versions: list[str] = []
    real_catalog_for_version = authoring_service.workflow_authoring_catalog_for_version

    def capture_catalog(ds_version: str) -> TaskAuthoringCatalog:
        selected_versions.append(ds_version)
        return real_catalog_for_version(ds_version)

    monkeypatch.setattr(
        authoring_service,
        "workflow_authoring_catalog_for_version",
        capture_catalog,
    )
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_instance_adapter=instance_adapter,
        context=ResourceDefaults(project="analytics"),
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_instance_service.edit_workflow_instance_result(
        901,
        patch=_metadata_patch_file(tmp_path),
        dry_run=True,
        env_file=str(profile_file),
    )

    assert selected_versions == ["3.4.2"]
    result_data = require_json_object(
        result.data,
        label="workflow instance edit result",
    )
    assert result_data["dry_run"] is True


def test_lint_loads_authoring_catalog_from_selected_profile(tmp_path: Path) -> None:
    profile_file = _selected_profile_file(tmp_path, "3.4.2")

    result = lint_workflow_result(
        file=_resource_workflow_file(tmp_path),
        env_file=str(profile_file),
    )

    assert isinstance(result.failure, UserInputError)
    assert result.failure.details["selected_version"] == "3.4.2"
    assert result.failure.details["intent"] == "typed_create"


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2"])
def test_32x_high_level_authoring_rejects_resource_file_sql(
    tmp_path: Path,
    version: str,
) -> None:
    profile_file = _selected_profile_file(tmp_path, version)

    result = lint_workflow_result(
        file=_resource_workflow_file(tmp_path),
        env_file=str(profile_file),
    )

    assert isinstance(result.failure, UserInputError)
    assert result.failure.details["selected_version"] == version
    assert result.failure.details["facet"] == "SQL/resource_file"
    assert result.failure.details["intent"] == "typed_create"


@pytest.mark.parametrize(
    ("action", "verification"),
    [
        ("workflow.create", Verification.CONTRACT_TESTED),
        ("workflow.edit", Verification.CONTRACT_TESTED),
        ("lint.workflow", Verification.STATIC),
        ("workflow-instance.edit", Verification.CONTRACT_TESTED),
        ("workflow-instance.export", Verification.CONTRACT_TESTED),
    ],
)
def test_342_workflow_actions_are_terminal_without_live_promotion(
    action: str,
    verification: Verification,
) -> None:
    capability = get_action_capability("3.4.2", action)

    assert capability.availability is Availability.SUPPORTED
    assert capability.verification is verification
