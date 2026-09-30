"""Workflow activation preflight, release changes, and schedule effects."""

import json
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeScheduleAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiHttpError,
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.models import WorkflowSpec
from dsctl.models.common import GlobalParamSpec
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.workflow import lifecycle as workflow_lifecycle
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph

if TYPE_CHECKING:
    from collections.abc import Callable


def test_online_workflow_result_warns_when_attached_schedule_remains_offline(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    daily = fake_workflow_adapter.workflows[0]
    schedule = daily.schedule
    assert schedule is not None
    offline_workflow = replace(
        daily,
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=None,
        schedule_value=None,
    )
    fake_workflow_adapter.workflows[0] = offline_workflow
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_workflow,
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
                release_state_value=FakeEnumValue("OFFLINE"),
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )

    result = workflow_service.online_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["releaseState"] == "ONLINE"
    assert data["scheduleReleaseState"] == "OFFLINE"
    assert _mapping(data["schedule"])["id"] == 23
    assert fake_workflow_adapter.release_calls[-1] == (101, "ONLINE")
    assert len(schedule_adapter.list_calls) == 1
    assert result.warnings == [
        "workflow brought online; any attached schedule remains offline until "
        "`schedule online` is requested"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_online_leaves_schedule_offline",
            "message": (
                "workflow brought online; any attached schedule remains offline "
                "until `schedule online` is requested"
            ),
            "action": "online",
            "workflow_release_state": "OFFLINE",
            "schedule_release_state": "OFFLINE",
        }
    ]


def test_139_online_workflow_uses_legacy_graph_for_activation_preflight(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    daily = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=None,
        schedule_value=None,
    )
    fake_workflow_adapter.workflows[0] = daily
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    graph = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "daily-sync", "project": "etl-prod"},
                "tasks": [{"name": "extract", "type": "SHELL", "command": "echo ok"}],
            }
        ),
        task_id_factory=lambda name: f"tasks-{name}",
    ).materialize()
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )

    def reject_code_native_dag(*args: object, **kwargs: object) -> object:
        del args, kwargs
        message = "DS 1.3.9 activation must not load a code-native DAG"
        raise AssertionError(message)

    monkeypatch.setattr(operations, "dag", reject_code_native_dag)

    result = workflow_service.online_workflow_result(
        "daily-sync",
        project="etl-prod",
    )

    assert _mapping(result.data)["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls == [(101, "ONLINE")]


@pytest.mark.parametrize("stale_counter", [False, True])
@pytest.mark.parametrize("activation_allowed", [False, True])
def test_139_online_workflow_keeps_typed_datax_activation_guard(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    *,
    stale_counter: bool,
    activation_allowed: bool,
) -> None:
    daily = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=None,
        schedule_value=None,
    )
    fake_workflow_adapter.workflows[0] = daily
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
    )
    catalog = get_task_authoring_catalog("1.3.9")
    spec = validate_workflow_document(
        {
            "workflow": {
                "name": "daily-sync",
                "project": "etl-prod",
                "global_params": {"batch": "2026-09-20"},
            },
            "tasks": [
                {
                    "name": "load",
                    "type": "DATAX",
                    "task_params": {"json": '{"job":{"content":[]}}'},
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    graph = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda name: f"tasks-{name}",
    ).materialize()
    if stale_counter:
        locations = json.loads(graph["locations"])
        locations["tasks-load"]["nodenumber"] = 4
        graph["locations"] = json.dumps(locations)
    operations.legacy_definitions[101] = (
        graph["processDefinitionJson"],
        graph["locations"],
        graph["connects"],
    )
    captured: dict[str, object] = {}

    def capture_datax_preflight(
        activation_spec: WorkflowSpec,
        *,
        profile_version: str,
    ) -> None:
        captured["spec"] = activation_spec
        captured["profile_version"] = profile_version
        if not activation_allowed:
            message = "DATAX activation preflight rejected"
            raise UserInputError(message)

    monkeypatch.setattr(
        workflow_lifecycle,
        "preflight_datax_runtime_activation",
        capture_datax_preflight,
    )

    if activation_allowed:
        workflow_service.online_workflow_result("daily-sync", project="etl-prod")
    else:
        with pytest.raises(UserInputError, match="DATAX activation preflight rejected"):
            workflow_service.online_workflow_result("daily-sync", project="etl-prod")

    activation_spec = captured["spec"]
    assert isinstance(activation_spec, WorkflowSpec)
    assert captured["profile_version"] == "1.3.9"
    assert [task.type for task in activation_spec.tasks] == ["DATAX"]
    global_params = activation_spec.workflow.global_params
    assert isinstance(global_params, list)
    typed_global_params = [
        param for param in global_params if isinstance(param, GlobalParamSpec)
    ]
    assert len(typed_global_params) == len(global_params)
    assert [param.prop for param in typed_global_params] == ["batch"]
    assert fake_workflow_adapter.release_calls == (
        [(101, "ONLINE")] if activation_allowed else []
    )


@pytest.mark.parametrize("initial_state", ["OFFLINE", "ONLINE"])
def test_139_online_workflow_accepts_stale_ui_counters_without_updating_graph(
    initial_state: str,
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    stale_legacy_workflow_graph: tuple[str, str, str],
) -> None:
    fake_workflow_adapter.workflows[0] = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue(initial_state),
        schedule_release_state_value=None,
        schedule_value=None,
    )
    operations = workflow_harness.install(profile=make_profile(ds_version="1.3.9"))
    operations.legacy_definitions[101] = stale_legacy_workflow_graph
    monkeypatch.setattr(
        operations,
        "apply_update",
        lambda _prepared: pytest.fail("workflow online sent an update request"),
    )

    result = workflow_service.online_workflow_result("daily-sync", project="etl-prod")

    assert _mapping(result.data)["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls == [(101, "ONLINE")]
    assert fake_workflow_adapter.update_calls == []
    assert operations.legacy_definitions[101] == stale_legacy_workflow_graph


def _kubeflow_release_dag(
    workflow: FakeWorkflow,
    *,
    richer: bool = False,
    retry_times: int = 1,
    timeout: int = 0,
    timeout_strategy: str | None = None,
) -> FakeDag:
    manifest = (
        'apiVersion: "kubeflow.org/v1"\n'
        "kind: TFJob\n"
        "metadata:\n"
        "  name: train-release-${system.workflow.instance.id}\n"
        "  namespace: kubeflow-team\n"
        "spec:\n"
        "  tfReplicaSpecs:\n"
        "    Worker:\n"
        "      replicas: 1\n"
    )
    task_params: dict[str, object] = {
        "yamlContent": manifest,
        "namespace": '{"name":"kubeflow-team","cluster":"production"}',
    }
    if richer:
        task_params["localParams"] = [
            {
                "prop": "native",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "preserve",
            }
        ]
        task_params["futureField"] = {"preserve": True}
    task = FakeTaskDefinition(
        code=301,
        name="unsafe-kubeflow-release",
        project_code_value=7,
        project_name_value="etl-prod",
        task_type_value="KUBEFLOW",
        task_params_value=json.dumps(task_params),
        worker_group_value="default",
        fail_retry_times_value=retry_times,
        fail_retry_interval_value=0,
        timeout=timeout,
        timeout_notify_strategy_value=(
            None if timeout_strategy is None else FakeEnumValue(timeout_strategy)
        ),
    )
    return FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )


def test_online_workflow_preflights_unsafe_kubeflow_before_release_rest(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _kubeflow_release_dag(adhoc)
    workflow_harness.install()

    with pytest.raises(UserInputError, match=r"retry\.times must be 0") as captured:
        workflow_service.online_workflow_result(
            "adhoc-backfill",
            project="etl-prod",
        )

    assert captured.value.details["reason"] == (
        "kubeflow-task-retry-cannot-preserve-identity"
    )
    assert fake_workflow_adapter.release_calls == []


def test_offline_workflow_skips_kubeflow_preflight_and_still_releases(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("ONLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _kubeflow_release_dag(adhoc)
    operations = workflow_harness.install()

    def unexpected_dag_load(*args: object, **kwargs: object) -> object:
        del args, kwargs
        message = "OFFLINE must not load the workflow DAG preflight"
        raise AssertionError(message)

    monkeypatch.setattr(operations, "dag", unexpected_dag_load)

    result = workflow_service.offline_workflow_result(
        "adhoc-backfill",
        project="etl-prod",
    )

    assert _mapping(result.data)["releaseState"] == "OFFLINE"
    assert fake_workflow_adapter.release_calls == [(102, "OFFLINE")]


def test_online_workflow_releases_valid_typed_kubeflow(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _kubeflow_release_dag(
        adhoc,
        retry_times=0,
        timeout=60,
        timeout_strategy="FAILED",
    )
    workflow_harness.install()

    result = workflow_service.online_workflow_result(
        "adhoc-backfill",
        project="etl-prod",
    )

    assert _mapping(result.data)["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls == [(102, "ONLINE")]


def test_online_workflow_rejects_richer_opaque_kubeflow_before_release(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _kubeflow_release_dag(
        adhoc,
        richer=True,
        retry_times=0,
        timeout=60,
        timeout_strategy="FAILED",
    )
    workflow_harness.install()

    with pytest.raises(UserInputError) as captured:
        workflow_service.online_workflow_result(
            "adhoc-backfill",
            project="etl-prod",
        )

    assert captured.value.details["reason"] == ("kubeflow-opaque-runtime-edit-closed")
    assert fake_workflow_adapter.release_calls == []


def test_online_workflow_without_kubeflow_releases_on_3_4_2(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = FakeDag(
        workflow_definition_value=adhoc,
        task_definition_list_value=[],
        workflow_task_relation_list_value=[],
    )
    workflow_harness.install(
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.online_workflow_result(
        "adhoc-backfill",
        project="etl-prod",
    )

    assert _mapping(result.data)["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls == [(102, "ONLINE")]


@pytest.mark.parametrize("version", ["3.1.1", "3.1.8", "3.4.1"])
def test_online_workflow_rejects_datax_globals_before_release_rest(
    version: str,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
        global_params_value='[{"prop":"unsafe","value":"value"}]',
        global_param_map_value={"unsafe": "value"},
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = FakeDag(
        workflow_definition_value=adhoc,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=302,
                name="datax-release",
                project_code_value=7,
                project_name_value="etl-prod",
                task_type_value="DATAX",
                task_params_value=json.dumps(
                    {
                        "customConfig": 1,
                        "json": '{"job":{"content":[]}}',
                        "xms": 1,
                        "xmx": 1,
                    }
                ),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )
    workflow_harness.install(profile=make_profile(ds_version=version))

    with pytest.raises(UserInputError, match="parameter-free") as captured:
        workflow_service.online_workflow_result("adhoc-backfill", project="etl-prod")

    assert captured.value.details["reason"] == (
        "datax-workflow-parameters-outside-typed-facet"
    )
    assert fake_workflow_adapter.release_calls == []


def _seatunnel_release_dag(
    workflow: FakeWorkflow,
    *,
    richer: bool = False,
) -> FakeDag:
    task_params: dict[str, object] = {
        "localParams": [],
        "startupScript": "seatunnel.sh",
        "useCustom": True,
        "rawScript": "env { execution.parallelism = 1 }\n",
        "resourceList": [],
        "deployMode": "local",
        "others": "",
    }
    if richer:
        task_params["futureField"] = {"preserve": True}
    return FakeDag(
        workflow_definition_value=workflow,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=302,
                name="seatunnel-release",
                project_code_value=7,
                project_name_value="etl-prod",
                task_type_value="SEATUNNEL",
                task_params_value=json.dumps(task_params),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )


def test_online_workflow_rejects_seatunnel_globals_before_release_rest(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
        global_params_value='[{"prop":"unsafe","value":"value"}]',
        global_param_map_value={"unsafe": "value"},
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _seatunnel_release_dag(adhoc)
    workflow_harness.install()

    with pytest.raises(UserInputError, match="parameter-free") as captured:
        workflow_service.online_workflow_result(
            "adhoc-backfill",
            project="etl-prod",
        )

    assert captured.value.details["reason"] == (
        "seatunnel-workflow-parameters-outside-typed-facet"
    )
    assert fake_workflow_adapter.release_calls == []


def test_online_workflow_releases_valid_typed_seatunnel(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _seatunnel_release_dag(adhoc)
    workflow_harness.install()

    result = workflow_service.online_workflow_result(
        "adhoc-backfill",
        project="etl-prod",
    )

    assert _mapping(result.data)["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls == [(102, "ONLINE")]


def test_online_workflow_rejects_richer_opaque_seatunnel_before_release(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    adhoc = replace(
        fake_workflow_adapter.workflows[1],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[1] = adhoc
    fake_workflow_adapter.dags[102] = _seatunnel_release_dag(adhoc, richer=True)
    workflow_harness.install()

    with pytest.raises(UserInputError) as captured:
        workflow_service.online_workflow_result(
            "adhoc-backfill",
            project="etl-prod",
        )

    assert captured.value.details["reason"] == "seatunnel-opaque-runtime-edit-closed"
    assert fake_workflow_adapter.release_calls == []


def test_offline_workflow_result_warns_when_schedule_is_also_taken_offline(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )

    result = workflow_service.offline_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["releaseState"] == "OFFLINE"
    assert data["scheduleReleaseState"] == "OFFLINE"
    assert _mapping(data["schedule"])["id"] == 23
    assert fake_workflow_adapter.release_calls[-1] == (101, "OFFLINE")
    assert len(schedule_adapter.list_calls) == 2
    assert result.warnings == [
        "workflow brought offline; any attached schedule is also taken offline"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_offline_also_offlines_schedule",
            "message": (
                "workflow brought offline; any attached schedule is also taken offline"
            ),
            "action": "offline",
            "workflow_release_state": "ONLINE",
            "schedule_release_state": "ONLINE",
        }
    ]


def test_offline_workflow_result_marks_post_mutation_schedule_lookup_failure(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
            )
        ],
        list_errors_by_call={
            2: ApiResultError(
                result_code=30001,
                result_message="schedule read permission denied",
            )
        },
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )

    with pytest.raises(PermissionDeniedError) as captured:
        workflow_service.offline_workflow_result(
            "daily-sync",
            project="etl-prod",
        )

    error = captured.value
    assert error.details["mutation_applied"] is True
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion
    assert fake_workflow_adapter.release_calls == [(101, "OFFLINE")]
    assert len(schedule_adapter.list_calls) == 2


@pytest.mark.parametrize(
    ("action", "result_code", "error_type"),
    [
        ("online", 99999, ApiTransportError),
        ("offline", 50003, NotFoundError),
        ("online", 30001, PermissionDeniedError),
    ],
)
def test_workflow_release_marks_post_mutation_workflow_refresh_failure(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    action: str,
    result_code: int,
    error_type: type[Exception],
) -> None:
    fake_workflow_adapter.get_errors_by_call = {
        (3 if action == "online" else 2): ApiResultError(
            result_code=result_code,
            result_message="workflow refresh failed",
        )
    }
    workflow_harness.install()

    release = (
        workflow_service.online_workflow_result
        if action == "online"
        else workflow_service.offline_workflow_result
    )
    with pytest.raises(error_type) as captured:
        release("daily-sync", project="etl-prod")

    error = captured.value
    assert isinstance(error, (ApiTransportError, NotFoundError, PermissionDeniedError))
    assert error.details["mutation_applied"] is True
    assert error.details["operation"] == f"workflow.{action}"
    assert error.details["phase"] == "post_mutation_refresh"
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion
    assert fake_workflow_adapter.release_calls == [(101, action.upper())]


@pytest.mark.parametrize(
    ("upstream_error", "error_type"),
    [
        (ApiTransportError("connection reset"), ApiTransportError),
        (
            ApiHttpError(
                "gateway unavailable",
                status_code=503,
                body={"message": "unavailable"},
            ),
            ApiHttpError,
        ),
    ],
)
def test_workflow_release_marks_post_mutation_transport_refresh_failure(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    upstream_error: Exception,
    error_type: type[Exception],
) -> None:
    fake_workflow_adapter.get_errors_by_call = {3: upstream_error}
    workflow_harness.install()

    with pytest.raises(error_type) as captured:
        workflow_service.online_workflow_result(
            "daily-sync",
            project="etl-prod",
        )

    error = captured.value
    assert isinstance(error, (ApiHttpError, ApiTransportError))
    assert error.details["mutation_applied"] is True
    assert error.details["upstream_error_type"] in {
        "api_http_error",
        "api_transport_error",
    }
    assert error.suggestion is not None
    assert "mutation completed" in error.suggestion
    assert fake_workflow_adapter.release_calls == [(101, "ONLINE")]


def test_workflow_mutations_fail_closed_when_attached_schedule_lookup_fails(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    schedule_adapter = FakeScheduleAdapter(
        schedules=[],
        list_error=ApiResultError(
            result_code=30001,
            result_message="schedule read permission denied",
        ),
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: updated
""".strip(),
        encoding="utf-8",
    )

    operations: tuple[Callable[[], object], ...] = (
        lambda: workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        ),
        lambda: workflow_service.online_workflow_result(
            "daily-sync",
            project="etl-prod",
        ),
        lambda: workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        ),
    )
    for operation in operations:
        with pytest.raises(PermissionDeniedError):
            operation()

    assert fake_workflow_adapter.update_calls == []
    assert fake_workflow_adapter.release_calls == []
    assert [workflow.code for workflow in fake_workflow_adapter.workflows] == [101, 102]


def test_online_workflow_result_maps_subworkflow_release_error(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.online_errors_by_code = {
        101: ApiResultError(
            result_code=50004,
            result_message="exist sub workflow definition not online",
        )
    }
    workflow_harness.install()

    with pytest.raises(
        InvalidStateError,
        match="sub-workflows are already online",
    ) as exc_info:
        workflow_service.online_workflow_result("daily-sync", project="etl-prod")
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks 101 --project 7` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry the original workflow online command."
    )


def test_online_workflow_result_maps_wrapped_subworkflow_release_error(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.online_errors_by_code = {
        101: ApiResultError(
            result_code=10000,
            result_message=(
                "Internal Server Error: SubWorkflowDefinition child is not online"
            ),
        )
    }
    workflow_harness.install()

    with pytest.raises(
        InvalidStateError,
        match="sub-workflows are already online",
    ) as exc_info:
        workflow_service.online_workflow_result("daily-sync", project="etl-prod")
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks 101 --project 7` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry the original workflow online command."
    )
