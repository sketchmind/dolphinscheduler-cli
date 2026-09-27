"""Workflow execution defaults, options, previews, and dispatch failures."""

import json
from dataclasses import replace
from typing import Any, cast

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    FakeUser,
    FakeUserAdapter,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.generated.workflow_profiles import WORKFLOW_PROFILE_FACTS
from dsctl.services import workflow as workflow_service
from dsctl.services._schedule_environment_guard import (
    require_workflow_environment_inheritance,
)
from dsctl.services.selection import ResourceDefaults
from dsctl.services.workflow import execution as workflow_execution


def test_139_environment_error_does_not_suggest_unavailable_task_setting() -> None:
    with pytest.raises(UnsupportedFeatureError) as caught:
        require_workflow_environment_inheritance("1.3.9", 44)
    assert caught.value.details["reason"] == "schedule_environment_unavailable"
    assert caught.value.suggestion is not None
    assert "environment_code on each task" not in caught.value.suggestion


@pytest.mark.parametrize("action", ["run", "run-task", "backfill"])
@pytest.mark.parametrize("source", ["explicit", "preference"])
@pytest.mark.parametrize("dry_run", [0, 1])
def test_legacy_workflow_execution_environment_rejected_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    action: str,
    source: str,
    dry_run: int,
) -> None:
    preferences = (
        FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=5,
                    code=5,
                    project_code_value=7,
                    state=1,
                    preferences_value='{"environmentCode":44}',
                )
            ]
        )
        if source == "preference"
        else None
    )
    workflow_harness.install(
        profile=make_profile(ds_version="3.2.1"),
        context=ResourceDefaults(project="etl-prod"),
        project_preference_adapter=preferences,
    )
    selected_environment = 44 if source == "explicit" else None

    def invoke() -> object:
        if action == "run":
            return workflow_service.run_workflow_result(
                "daily-sync",
                environment_code=selected_environment,
                dry_run=bool(dry_run),
            )
        if action == "run-task":
            return workflow_service.run_workflow_task_result(
                "daily-sync",
                task="extract",
                environment_code=selected_environment,
                dry_run=bool(dry_run),
            )
        return workflow_service.backfill_workflow_result(
            "daily-sync",
            start="2026-04-01 00:00:00",
            end="2026-04-03 00:00:00",
            environment_code=selected_environment,
            dry_run=bool(dry_run),
        )

    with pytest.raises(UnsupportedFeatureError) as caught:
        invoke()
    assert caught.value.suggestion is not None
    assert "environment_code on each task" in caught.value.suggestion
    assert fake_workflow_adapter.run_calls == []
    assert fake_workflow_adapter.backfill_calls == []


def test_legacy_workflow_execution_zero_bypasses_positive_preference(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="3.2.1"),
        context=ResourceDefaults(project="etl-prod"),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=5,
                    code=5,
                    project_code_value=7,
                    state=1,
                    preferences_value='{"environmentCode":44}',
                )
            ]
        ),
    )
    result = workflow_service.run_workflow_result("daily-sync", environment_code=0)
    assert _mapping(result.resolved["environment_code"])["value"] == 0
    assert len(fake_workflow_adapter.run_calls) == 1


def test_workflow_execution_environment_supported_from_322(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="3.2.2"),
        context=ResourceDefaults(project="etl-prod"),
    )
    workflow_service.run_workflow_result("daily-sync", environment_code=44)
    assert fake_workflow_adapter.run_calls[-1]["environment_code"] == 44


def test_run_workflow_result_uses_project_default_and_explicit_workflow(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
        user_adapter=FakeUserAdapter(
            users=[
                FakeUser(
                    id=11,
                    user_name_value="alice",
                    email="alice@example.com",
                    tenant_id_value=7,
                    tenant_code_value="tenant-current-user",
                )
            ]
        ),
    )

    result = workflow_service.run_workflow_result("daily-sync")
    data = _mapping(result.data)

    assert data["workflowInstanceIds"] == [901]
    assert _mapping(result.resolved["project"])["source"] == "context"
    assert _mapping(result.resolved["workflow"])["source"] == "flag"
    assert _mapping(result.resolved["worker_group"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["failure_strategy"]) == {
        "value": "CONTINUE",
        "source": "default",
    }
    assert _mapping(result.resolved["warning_type"]) == {
        "value": "NONE",
        "source": "default",
    }
    assert _mapping(result.resolved["workflow_instance_priority"]) == {
        "value": "MEDIUM",
        "source": "default",
    }
    assert _mapping(result.resolved["warning_group_id"]) == {
        "value": None,
        "source": "default",
    }
    assert _mapping(result.resolved["environment_code"]) == {
        "value": None,
        "source": "default",
    }
    assert _mapping(result.resolved["start_params"]) == {
        "names": [],
        "count": 0,
        "source": "default",
    }
    assert result.resolved["execution_dry_run"] is False


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize("operation", ["run", "backfill"])
def test_execution_schedule_time_uses_exact_consumer_shape(
    workflow_harness: _WorkflowServiceHarness,
    version: str,
    operation: str,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version=version),
        context=ResourceDefaults(project="etl-prod"),
    )

    if operation == "run":
        result = workflow_service.run_workflow_result("daily-sync", dry_run=True)
    else:
        result = workflow_service.backfill_workflow_result(
            "daily-sync",
            start="2026-04-01 00:00:00",
            end="2026-04-03 00:00:00",
            dry_run=True,
        )

    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    schedule_time = cast("str", form["scheduleTime"])
    shape = WORKFLOW_PROFILE_FACTS[version]["execution_schedule_time_shape"]
    if shape == "comma-range":
        start, end = schedule_time.split(",")
    else:
        payload = cast("dict[str, str]", json.loads(schedule_time))
        start = payload["complementStartDate"]
        end = payload["complementEndDate"]
    if operation == "run":
        assert start == end
    else:
        assert (start, end) == (
            "2026-04-01 00:00:00",
            "2026-04-03 00:00:00",
        )


@pytest.mark.parametrize("version", ["1.3.9", "2.0.2", "3.0.6"])
def test_comma_range_executor_rejects_unrepresentable_explicit_dates(
    workflow_harness: _WorkflowServiceHarness,
    version: str,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version=version),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.backfill_workflow_result(
            "daily-sync",
            dates=["2026-04-01 00:00:00"],
            dry_run=True,
        )

    assert exc_info.value.details["parameter"] == "complementScheduleDateList"
    assert exc_info.value.details["ds_version"] == version
    assert operations.prepared_executions == []


def test_comma_range_executor_rejects_reversed_range_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="2.0.2"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="end must be greater than or equal to start",
    ):
        workflow_service.backfill_workflow_result(
            "daily-sync",
            start="2026-04-03 00:00:00",
            end="2026-04-01 00:00:00",
            dry_run=True,
        )

    assert operations.prepared_executions == []


def test_run_workflow_result_uses_default_worker_group_without_remote_defaults(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_result("daily-sync")

    assert _mapping(result.resolved["worker_group"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }


def test_run_workflow_result_prefers_enabled_project_preference_defaults(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        profile=make_profile(),
        context=ResourceDefaults(project="etl-prod"),
        user_adapter=FakeUserAdapter(
            users=[
                FakeUser(
                    id=11,
                    user_name_value="alice",
                    email="alice@example.com",
                    tenant_id_value=7,
                    tenant_code_value="tenant-current-user",
                )
            ]
        ),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=5,
                    code=5,
                    project_code_value=7,
                    state=1,
                    preferences_value=(
                        '{"workerGroup":"pref-group","tenant":"tenant-pref",'
                        '"taskPriority":"HIGH","warningType":"FAILURE",'
                        '"alertGroups":12,"environmentCode":44}'
                    ),
                )
            ]
        ),
    )

    result = workflow_service.run_workflow_result("daily-sync")

    assert _mapping(result.resolved["worker_group"]) == {
        "value": "pref-group",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-pref",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["workflow_instance_priority"]) == {
        "value": "HIGH",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["warning_type"]) == {
        "value": "FAILURE",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["warning_group_id"]) == {
        "value": 12,
        "source": "project_preference",
    }
    assert _mapping(result.resolved["environment_code"]) == {
        "value": 44,
        "source": "project_preference",
    }


def test_run_workflow_result_applies_explicit_runtime_options(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_result(
        "daily-sync",
        worker_group="analytics",
        tenant="tenant-prod",
        failure_strategy="end",
        priority="highest",
        warning_type="all",
        warning_group_id=18,
        environment_code=33,
        params=["bizdate=20260415", "region=cn"],
        execution_dry_run=True,
    )

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert fake_workflow_adapter.run_calls[-1] == {
        "project_code": 7,
        "workflow_code": 101,
        "worker_group": "analytics",
        "tenant_code": "tenant-prod",
        "start_node_list": None,
        "task_scope": None,
        "failure_strategy": "END",
        "warning_type": "ALL",
        "workflow_instance_priority": "HIGHEST",
        "warning_group_id": 18,
        "environment_code": 33,
        "start_params": '{"bizdate":"20260415","region":"cn"}',
        "dry_run": True,
    }
    assert _mapping(result.resolved["start_params"]) == {
        "names": ["bizdate", "region"],
        "count": 2,
        "source": "flag",
    }
    assert result.warnings == [
        "DS execution dry-run is enabled; DolphinScheduler will create dry-run "
        "workflow/task instances and skip task plugin trigger execution."
    ]
    assert _mapping(result.warning_details[0])["code"] == "workflow_execution_dry_run"


def test_run_workflow_result_rejects_start_params_for_ds_139_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.run_workflow_result(
            "daily-sync",
            params=["bizdate=20260415"],
        )

    assert fake_workflow_adapter.run_calls == []
    assert exc_info.value.to_payload() == {
        "type": "unsupported_feature",
        "message": (
            "--param is not available in DolphinScheduler 1.3.9 because its "
            "workflow executor has no startParams field."
        ),
        "details": {
            "resource": "workflow",
            "action": "workflow.run",
            "parameter": "startParams",
            "ds_version": "1.3.9",
            "reason": "upstream_capability_absent",
        },
        "suggestion": "Omit --param for this exact DS version.",
    }


def test_run_workflow_result_rejects_explicit_legacy_tenant_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.run_workflow_result("daily-sync", tenant="tenant-prod")

    assert operations.prepared_executions == []
    assert fake_workflow_adapter.run_calls == []
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.run",
        "parameter": "tenantCode",
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


def test_run_workflow_task_result_rejects_explicit_legacy_environment_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.run_workflow_task_result(
            "daily-sync",
            task="extract",
            environment_code=7,
        )

    assert operations.prepared_executions == []
    assert fake_workflow_adapter.run_calls == []
    assert exc_info.value.details["action"] == "workflow.run-task"
    assert exc_info.value.details["parameter"] == "environmentCode"


def test_run_workflow_result_sends_start_params_at_ds_200_boundary(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="2.0.0"),
        context=ResourceDefaults(project="etl-prod"),
    )

    workflow_service.run_workflow_result(
        "daily-sync",
        params=["env=stage"],
    )

    assert fake_workflow_adapter.run_calls[-1]["start_params"] == ('{"env":"stage"}')


def test_run_workflow_result_rejects_undeclared_ds_200_start_param(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="2.0.0"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.run_workflow_result(
            "daily-sync",
            params=["bizdate=20260415"],
        )

    assert fake_workflow_adapter.run_calls == []
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.run",
        "parameter": "startParams",
        "ds_version": "2.0.0",
        "undeclared_keys": ["bizdate"],
        "declared_keys": ["env"],
        "reason": "upstream_silently_ignores_undeclared_keys",
    }


def test_run_workflow_result_sends_undeclared_start_param_from_ds_209(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="2.0.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_result(
        "daily-sync",
        params=["bizdate=20260415"],
        dry_run=True,
    )

    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    assert form["startParams"] == '{"bizdate":"20260415"}'
    assert fake_workflow_adapter.run_calls == []


def test_run_workflow_result_accepts_undeclared_start_param_from_ds_300(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="3.0.0"),
        context=ResourceDefaults(project="etl-prod"),
    )

    workflow_service.run_workflow_result(
        "daily-sync",
        params=["bizdate=20260415"],
    )

    assert fake_workflow_adapter.run_calls[-1]["start_params"] == (
        '{"bizdate":"20260415"}'
    )


def test_run_workflow_task_result_starts_selected_task_with_dependency_warning(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_task_result("daily-sync", task="extract")

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert _mapping(result.resolved["task"]) == {
        "code": 201,
        "name": "extract",
        "version": 1,
    }
    assert result.resolved["scope"] == "self"
    assert fake_workflow_adapter.run_calls[-1]["start_node_list"] == [201]
    assert fake_workflow_adapter.run_calls[-1]["task_scope"] == "self"
    assert result.warnings == [
        "Dependent downstream nodes may fail if their referenced task, whole "
        "workflow, or scheduled dependency instance has not produced a "
        "successful run; this request starts only the selected task."
    ]
    warning_detail = _mapping(result.warning_details[0])
    assert warning_detail["code"] == "workflow_run_task_dependent_context"
    assert warning_detail["blocking"] is False


def test_run_workflow_task_result_rejects_start_params_for_ds_139_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.run_workflow_task_result(
            "daily-sync",
            task="extract",
            params=["bizdate=20260415"],
        )

    assert fake_workflow_adapter.run_calls == []
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.run-task",
        "parameter": "startParams",
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


def test_run_workflow_result_dry_run_emits_start_request_without_run_call(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_result(
        "daily-sync",
        params=["bizdate=20260415"],
        dry_run=True,
        execution_dry_run=True,
    )

    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    assert data["dry_run"] is True
    assert request["method"] == "POST"
    assert request["path"] == "/projects/7/executors/start-workflow-instance"
    assert form["workflowDefinitionCode"] == 101
    assert form["taskDependType"] == "TASK_POST"
    assert form["dryRun"] == 1
    assert form["startParams"] == '{"bizdate":"20260415"}'
    assert fake_workflow_adapter.run_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "workflow_execution_dry_run",
    ]


def test_run_workflow_task_result_dry_run_uses_selected_task_scope(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.run_workflow_task_result(
        "daily-sync",
        task="extract",
        scope="self",
        dry_run=True,
    )

    form = _mapping(_mapping(first_dry_run_request(_mapping(result.data)))["form"])
    assert form["startNodeList"] == "201"
    assert form["taskDependType"] == "TASK_ONLY"
    assert form["dryRun"] == 0
    assert fake_workflow_adapter.run_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "workflow_run_task_dependent_context",
    ]


def test_run_workflow_task_result_reports_scope_choices(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Task execution scope") as exc_info:
        workflow_service.run_workflow_task_result(
            "daily-sync", task="extract", scope="down"
        )

    assert exc_info.value.suggestion == (
        "Pass `--scope self`, `--scope pre`, or `--scope post`."
    )


def test_backfill_workflow_result_uses_range_defaults(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.backfill_workflow_result(
        "daily-sync",
        start="2026-04-01 00:00:00",
        end="2026-04-03 00:00:00",
    )

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert fake_workflow_adapter.backfill_calls[-1] == {
        "project_code": 7,
        "workflow_code": 101,
        "schedule_time": (
            '{"complementStartDate":"2026-04-01 00:00:00",'
            '"complementEndDate":"2026-04-03 00:00:00"}'
        ),
        "run_mode": "RUN_MODE_SERIAL",
        "expected_parallelism_number": 2,
        "complement_dependent_mode": "OFF_MODE",
        "all_level_dependent": False,
        "execution_order": "DESC_ORDER",
        "worker_group": "default",
        "tenant_code": "default",
        "start_node_list": None,
        "task_scope": None,
        "failure_strategy": "CONTINUE",
        "warning_type": "NONE",
        "workflow_instance_priority": "MEDIUM",
        "warning_group_id": None,
        "environment_code": None,
        "start_params": None,
        "dry_run": False,
    }
    backfill_resolved = _mapping(result.resolved["backfill"])
    assert _mapping(backfill_resolved["run_mode"]) == {
        "value": "RUN_MODE_SERIAL",
        "source": "default",
    }
    assert backfill_resolved["schedule_time_mode"] == "range"


def test_backfill_workflow_result_rejects_start_params_for_ds_139_before_dispatch(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.backfill_workflow_result(
            None,
            start="2026-04-01 00:00:00",
            end="2026-04-03 00:00:00",
            params=["bizdate=20260401"],
        )

    assert fake_workflow_adapter.backfill_calls == []
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.backfill",
        "parameter": "startParams",
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


@pytest.mark.parametrize(
    ("options", "parameter"),
    [
        pytest.param(
            {"complement_dependent_mode": "all"},
            "complementDependentMode",
            id="dependent-mode",
        ),
        pytest.param(
            {"all_level_dependent": True},
            "allLevelDependent",
            id="all-level-dependent",
        ),
        pytest.param(
            {"execution_order": "asc"},
            "executionOrder",
            id="execution-order",
        ),
    ],
)
def test_backfill_workflow_result_rejects_explicit_legacy_execution_options(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    options: dict[str, object],
    parameter: str,
) -> None:
    operations = workflow_harness.install(
        profile=make_profile(ds_version="1.3.9"),
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UnsupportedFeatureError) as exc_info:
        workflow_service.backfill_workflow_result(
            None,
            start="2026-04-01 00:00:00",
            end="2026-04-03 00:00:00",
            **cast("Any", options),
        )

    assert operations.prepared_executions == []
    assert fake_workflow_adapter.backfill_calls == []
    assert exc_info.value.details["action"] == "workflow.backfill"
    assert exc_info.value.details["parameter"] == parameter


def test_backfill_workflow_result_dry_run_can_target_task_and_date_list(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.backfill_workflow_result(
        "daily-sync",
        dates=["2026-04-01 00:00:00", "2026-04-02 00:00:00"],
        task="extract",
        scope="self",
        run_mode="parallel",
        expected_parallelism_number=4,
        complement_dependent_mode="all",
        all_level_dependent=True,
        execution_order="asc",
        params=["bizdate=20260401"],
        dry_run=True,
        execution_dry_run=True,
    )

    data = _mapping(result.data)
    form = _mapping(_mapping(first_dry_run_request(data))["form"])
    assert data["dry_run"] is True
    assert form["execType"] == "COMPLEMENT_DATA"
    assert form["scheduleTime"] == (
        '{"complementScheduleDateList":"2026-04-01 00:00:00,2026-04-02 00:00:00"}'
    )
    assert form["startNodeList"] == "201"
    assert form["taskDependType"] == "TASK_ONLY"
    assert form["runMode"] == "RUN_MODE_PARALLEL"
    assert form["expectedParallelismNumber"] == 4
    assert form["complementDependentMode"] == "ALL_DEPENDENT"
    assert form["allLevelDependent"] is True
    assert form["executionOrder"] == "ASC_ORDER"
    assert form["startParams"] == '{"bizdate":"20260401"}'
    assert form["dryRun"] == 1
    assert fake_workflow_adapter.backfill_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "workflow_execution_dry_run",
        "workflow_run_task_dependent_context",
    ]


def test_backfill_workflow_result_requires_time_selection(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Workflow backfill requires"):
        workflow_service.backfill_workflow_result("daily-sync")


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"start": "2026-04-01 00:00:00"}, "requires --start and --end"),
        (
            {
                "start": "2026/04/01",
                "end": "2026-04-02 00:00:00",
            },
            "start must match DS datetime format",
        ),
        (
            {
                "start": "2026-04-03 00:00:00",
                "end": "2026-04-02 00:00:00",
            },
            "end must be greater than or equal to start",
        ),
        (
            {
                "dates": ["2026-04-01 00:00:00"],
                "end": "2026-04-02 00:00:00",
            },
            "accepts either --date or --start/--end",
        ),
        ({"dates": ["tomorrow"]}, "date must match DS datetime format"),
        (
            {"dates": ["2026-04-01 00:00:00"], "run_mode": "sideways"},
            "Workflow run run mode must be one of",
        ),
        (
            {"dates": ["2026-04-01 00:00:00"], "params": ["missing-equals"]},
            "Invalid --param value",
        ),
    ],
)
def test_backfill_validates_local_inputs_before_opening_runtime(
    monkeypatch: pytest.MonkeyPatch,
    options: dict[str, object],
    message: str,
) -> None:
    opened_runtime = False

    def unexpected_runtime(*args: object, **kwargs: object) -> None:
        nonlocal opened_runtime
        del args, kwargs
        opened_runtime = True
        message = "runtime must not open for invalid local input"
        raise AssertionError(message)

    monkeypatch.setattr(
        workflow_execution,
        "run_with_bound_domain_service_runtime",
        unexpected_runtime,
    )

    with pytest.raises(UserInputError, match=message):
        workflow_service.backfill_workflow_result(
            "daily-sync",
            **cast("Any", options),
        )

    assert opened_runtime is False


@pytest.mark.parametrize(
    ("action", "dry_run"),
    [
        ("workflow.run", False),
        ("workflow.run", True),
        ("workflow.run-task", False),
        ("workflow.run-task", True),
        ("workflow.backfill", False),
        ("workflow.backfill", True),
    ],
)
def test_execution_rejects_known_offline_workflow_before_prepare(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    action: str,
    *,
    dry_run: bool,
) -> None:
    fake_workflow_adapter.workflows[0] = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    operations = workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    def execute() -> None:
        if action == "workflow.run":
            workflow_service.run_workflow_result("daily-sync", dry_run=dry_run)
        elif action == "workflow.run-task":
            workflow_service.run_workflow_task_result(
                "daily-sync",
                task="extract",
                dry_run=dry_run,
            )
        else:
            workflow_service.backfill_workflow_result(
                "daily-sync",
                dates=["2026-04-01 00:00:00"],
                dry_run=dry_run,
            )

    with pytest.raises(InvalidStateError, match="must be online") as exc_info:
        execute()

    assert exc_info.value.details["action"] == action
    assert exc_info.value.details["current_release_state"] == "OFFLINE"
    assert exc_info.value.details["required_release_state"] == "ONLINE"
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow online daily-sync --project etl-prod`, then retry "
        "the original command."
    )
    assert operations.prepared_executions == []
    assert fake_workflow_adapter.run_calls == []
    assert fake_workflow_adapter.backfill_calls == []


def test_run_workflow_result_maps_offline_definition_to_invalid_state(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=50014,
            result_message=(
                "start workflow instance error:The workflowDefinition should be online"
            ),
        )
    }
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(InvalidStateError, match="must be online") as exc_info:
        workflow_service.run_workflow_result("daily-sync")

    assert exc_info.value.details["resource"] == "workflow"
    assert exc_info.value.details["code"] == 101
    assert exc_info.value.details["required_release_state"] == "ONLINE"
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow online 101 --project 7`, then retry the original command."
    )


def test_run_workflow_result_maps_missing_master_to_invalid_state(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    result_message = "Internal Server Error: no master server available"
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=10000,
            result_message=result_message,
        )
    }
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(InvalidStateError, match="no available master") as exc_info:
        workflow_service.run_workflow_result("daily-sync")

    assert exc_info.value.details == {
        "resource": "workflow",
        "project": "etl-prod",
        "project_code": 7,
        "code": 101,
        "name": "daily-sync",
        "operation": "workflow.run",
    }
    assert exc_info.value.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10000,
        "result_message": result_message,
    }
    assert exc_info.value.suggestion == (
        "Run `dsctl monitor server master` and wait until at least one master "
        "is listed, then retry the original command."
    )


def test_run_workflow_result_preserves_unrelated_internal_server_error(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=10000,
            result_message="Internal Server Error: failed to start workflow",
        )
    }
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(ApiResultError, match="failed to start workflow") as exc_info:
        workflow_service.run_workflow_result("daily-sync")

    assert exc_info.value.result_code == 10000


@pytest.mark.parametrize("action", ["run", "run-task", "backfill"])
def test_workflow_master_rpc_failure_requires_reconciliation(
    action: str,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    result_message = (
        "start workflow instance error:Call method to Host(ip=master, port=5678) failed"
    )
    upstream_error = ApiResultError(result_code=50014, result_message=result_message)
    fake_workflow_adapter.run_errors_by_code = {101: upstream_error}
    workflow_harness.install(context=ResourceDefaults(project="etl-prod"))

    def execute() -> None:
        if action == "run":
            workflow_service.run_workflow_result("daily-sync")
        elif action == "run-task":
            workflow_service.run_workflow_task_result("daily-sync", task="extract")
        else:
            workflow_service.backfill_workflow_result(
                "daily-sync",
                dates=["2026-04-01 00:00:00", "2026-04-02 00:00:00"],
                run_mode="parallel",
            )

    with pytest.raises(ApiTransportError, match="API-to-master RPC failed") as exc_info:
        execute()

    error = exc_info.value
    assert error.details["operation"] == f"workflow.{action}"
    assert error.details["phase"] == "master_rpc"
    assert error.details["mutation_may_have_applied"] is True
    assert error.details["request_replay_safe"] is False
    assert error.source == upstream_error.source
    assert error.suggestion is not None
    assert (
        "dsctl workflow-instance list --project 7 --workflow 101 --all"
        in error.suggestion
    )
    assert "dsctl monitor server master" in error.suggestion
    assert "Do not blindly repeat" in error.suggestion
    if action == "backfill":
        assert error.details["partial_dispatch_possible"] is True
        assert len(fake_workflow_adapter.backfill_calls) == 1
    else:
        assert len(fake_workflow_adapter.run_calls) == 1


def test_unrelated_start_failure_keeps_upstream_error(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=50014,
        result_message="start workflow instance error:invalid schedule",
    )
    fake_workflow_adapter.run_errors_by_code = {101: upstream_error}
    workflow_harness.install(context=ResourceDefaults(project="etl-prod"))

    with pytest.raises(ApiResultError) as exc_info:
        workflow_service.run_workflow_result("daily-sync")

    assert exc_info.value is upstream_error


def test_run_workflow_task_result_reports_missing_master_operation(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=10025,
            result_message="master does not exist",
        )
    }
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(InvalidStateError, match="no available master") as exc_info:
        workflow_service.run_workflow_task_result("daily-sync", task="extract")

    assert exc_info.value.details["operation"] == "workflow.run-task"
    assert exc_info.value.suggestion == (
        "Run `dsctl monitor server master` and wait until at least one master "
        "is listed, then retry `dsctl workflow run-task WORKFLOW --task TASK`."
    )


def test_parallel_backfill_missing_master_requires_dispatch_verification(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=10000,
            result_message="no master server available",
        )
    }
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(InvalidStateError, match="no available master") as exc_info:
        workflow_service.backfill_workflow_result(
            "daily-sync",
            dates=["2026-04-01 00:00:00", "2026-04-02 00:00:00"],
            run_mode="parallel",
        )

    assert exc_info.value.details["operation"] == "workflow.backfill"
    assert exc_info.value.details["partial_dispatch_possible"] is True
    assert exc_info.value.suggestion == (
        "Run `dsctl monitor server master` and wait until at least one master "
        "is listed. Part of this parallel backfill may already have been "
        "dispatched; use `dsctl workflow-instance list --project 7 "
        "--workflow 101 --all` and compare `scheduleTime` with the "
        "requested range before deciding whether to retry the original "
        "backfill command."
    )
