"""Task execution previews use the same exact wire as the eventual mutation."""

from functools import partial
from typing import TYPE_CHECKING
from urllib.parse import parse_qs

import httpx
import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping
from tests.workflow_domain_fakes import (
    _FakeWorkflowOperations,
    install_workflow_domain_runtime,
)

from dsctl.client import DolphinSchedulerClient
from dsctl.generated.workflow_profiles import WORKFLOW_PROFILE_FACTS
from dsctl.models import WorkflowSpec
from dsctl.output import CommandResult
from dsctl.services import workflow as workflow_service
from dsctl.services.workflow import execution as workflow_execution
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph
from dsctl.upstream.workflows import WorkflowAdapter

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


@pytest.fixture
def execution_reads(
    monkeypatch: pytest.MonkeyPatch,
    version: str,
) -> _FakeWorkflowOperations:
    """Keep semantic reads local while binding execution to the real wire below."""
    operations = install_workflow_domain_runtime(
        monkeypatch,
        profile=make_profile(ds_version=version),
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="etl-prod")],
        ),
        workflow_adapter=FakeWorkflowAdapter(
            workflows=[
                FakeWorkflow(
                    code=101,
                    name="daily-sync",
                    project_code_value=7,
                    global_param_map_value={"env": "prod"},
                ),
            ],
            dags={},
        ),
        task_adapter=FakeTaskAdapter(
            workflow_tasks={
                101: [FakeTaskDefinition(code=202, name="load", project_code_value=7)],
            },
        ),
    )
    if version == "1.3.9":
        graph = prepare_legacy_workflow_graph(
            WorkflowSpec.model_validate(
                {
                    "workflow": {"name": "daily-sync", "project": "etl-prod"},
                    "tasks": [
                        {"name": "load", "type": "SHELL", "command": "echo load"},
                    ],
                },
            ),
            task_id_factory=lambda name: f"tasks-{name}",
        ).materialize()
        operations.legacy_definitions[101] = (
            graph["processDefinitionJson"],
            graph["locations"],
            graph["connects"],
        )
    return operations


@pytest.mark.parametrize("action", ["run-task", "backfill-task"])
@pytest.mark.parametrize(
    ("version", "epoch"),
    [
        ("1.3.9", "legacy"),
        ("2.0.0", "process"),
        ("2.0.9", "process"),
        ("3.0.0", "process-dependent"),
        ("3.0.6", "process-dependent"),
        ("3.1.0", "process-dependent"),
        ("3.1.9", "process-dependent"),
        ("3.2.0", "process-versioned"),
        ("3.2.1", "process-versioned"),
        ("3.2.2", "process-versioned"),
        ("3.3.1", "workflow"),
        ("3.3.2", "workflow"),
        ("3.4.0", "workflow"),
        ("3.4.1", "workflow"),
        ("3.4.2", "workflow"),
        ("3.4.3", "workflow"),
    ],
)
def test_task_execution_preview_matches_exact_http_request(
    monkeypatch: pytest.MonkeyPatch,
    execution_reads: _FakeWorkflowOperations,
    version: str,
    epoch: str,
    action: str,
) -> None:
    # These finite expectations are independent of generated recipes and schemas.
    legacy = epoch == "legacy"
    workflow = epoch == "workflow"
    backfill = action == "backfill-task"
    tenant_supported = epoch in {"process-versioned", "workflow"}
    environment_code = (
        33
        if version in {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
        else 0
    )
    schedule_time = (
        '{"complementStartDate":"2026-08-01 00:00:00",'
        '"complementEndDate":"2026-08-02 00:00:00"}'
    )
    schedule_shape = WORKFLOW_PROFILE_FACTS[version]["execution_schedule_time_shape"]
    start_schedule_time = (
        "2026-08-01 00:00:00,2026-08-01 00:00:00"
        if schedule_shape == "comma-range"
        else schedule_time
    )
    monkeypatch.setattr(
        workflow_execution,
        "_workflow_run_start_process_schedule_time",
        lambda _shape: start_schedule_time,
    )
    expected_schedule_time = (
        "2026-08-01 00:00:00,2026-08-02 00:00:00"
        if backfill and schedule_shape == "comma-range"
        else schedule_time
        if backfill
        else start_schedule_time
    )
    expected_form: dict[str, str | int | bool] = {
        "scheduleTime": expected_schedule_time,
        "failureStrategy": "END",
        "startNodeList": "load" if legacy else "202",
        "taskDependType": "TASK_PRE",
        "execType": "COMPLEMENT_DATA" if backfill else "START_PROCESS",
        "warningType": "ALL",
        "warningGroupId": 17,
        "workerGroup": "workers-a",
    }
    identity_field = {
        "legacy": "processDefinitionId",
        "process": "processDefinitionCode",
        "process-dependent": "processDefinitionCode",
        "process-versioned": "processDefinitionCode",
        "workflow": "workflowDefinitionCode",
    }[epoch]
    expected_form[identity_field] = 101
    priority_field = (
        "workflowInstancePriority" if workflow else "processInstancePriority"
    )
    expected_form[priority_field] = "HIGH"
    if not legacy:
        expected_form.update(
            {
                "environmentCode": environment_code,
                "startParams": '{"env":"stage"}',
                "dryRun": 1,
            },
        )
    if tenant_supported:
        expected_form["tenantCode"] = "analytics"
    if epoch == "process-versioned":
        expected_form.update({"testFlag": 0, "version": 1})
    if backfill:
        expected_form["runMode"] = "RUN_MODE_SERIAL"
        if not legacy:
            expected_form["expectedParallelismNumber"] = 2
        if epoch in {"process-dependent", "process-versioned", "workflow"}:
            expected_form["complementDependentMode"] = "OFF_MODE"
        if tenant_supported:
            expected_form.update(
                {"allLevelDependent": False, "executionOrder": "DESC_ORDER"},
            )
    project_route = "etl-prod" if legacy else "7"
    executor_route = "start-workflow-instance" if workflow else "start-process-instance"
    expected_path = f"/projects/{project_route}/executors/{executor_route}"
    response_data = {
        "legacy": None,
        "process": None,
        "process-dependent": None,
        "process-versioned": 42,
        "workflow": [901],
    }[epoch]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": [{"id": 901}] if request.method == "GET" else response_data,
            },
        )

    execute = (
        partial(
            workflow_service.backfill_workflow_result,
            start="2026-08-01 00:00:00",
            end="2026-08-02 00:00:00",
        )
        if backfill
        else partial(workflow_service.run_workflow_task_result)
    )
    execute = partial(
        execute,
        "daily-sync",
        project="etl-prod",
        task="load",
        scope="pre",
        worker_group="workers-a",
        tenant="analytics" if tenant_supported else None,
        failure_strategy="END",
        priority="HIGH",
        warning_type="ALL",
        warning_group_id=17,
        environment_code=None if legacy else environment_code,
        params=[] if legacy else ["env=stage"],
        execution_dry_run=not legacy,
    )
    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            WorkflowAdapter.for_version(version)
            .bind(
                profile,
                http_client=client,
            )
            .workflows
        )
        monkeypatch.setattr(
            execution_reads, "prepare_execution", operations.prepare_execution
        )
        monkeypatch.setattr(
            execution_reads, "apply_execution", operations.apply_execution
        )
        monkeypatch.setattr(
            execution_reads,
            "require_execution_options",
            operations.require_execution_options,
        )
        monkeypatch.setattr(
            execution_reads,
            "normalize_expected_parallelism_number",
            operations.normalize_expected_parallelism_number,
        )

        preview = execute(dry_run=True)
        assert requests == []
        request_preview = assert_mapping(
            first_dry_run_request(assert_mapping(preview.data))
        )
        assert request_preview["method"] == "POST"
        assert request_preview["path"] == expected_path
        assert request_preview["form"] == expected_form

        applied = execute(dry_run=False)

    assert applied.resolved == preview.resolved
    dry_run_warning = (
        "dry run: no mutation was sent; lookup and verification reads may occur"
    )
    assert preview.warnings == [dry_run_warning, *applied.warnings]
    assert preview.warning_details == [
        {
            "code": "dry_run_no_mutation_sent",
            "message": dry_run_warning,
            "mutation_sent": False,
        },
        *applied.warning_details,
    ]
    _assert_execution_receipt(
        applied, requests, epoch=epoch, supports_identity=tenant_supported
    )
    assert requests[0].method == request_preview["method"]
    assert requests[0].url.path == f"/dolphinscheduler{expected_path}"
    assert requests[0].url.query == b""
    assert requests[0].headers["content-type"] == "application/x-www-form-urlencoded"
    assert parse_qs(requests[0].content.decode(), keep_blank_values=True) == {
        key: [str(value).lower() if isinstance(value, bool) else str(value)]
        for key, value in expected_form.items()
    }


def _assert_execution_receipt(
    applied: CommandResult,
    requests: list[httpx.Request],
    *,
    epoch: str,
    supports_identity: bool,
) -> None:
    expected_receipt: JsonObject = {
        "workflowInstanceIds": [901] if supports_identity else [],
        "accepted": True,
        "instanceResolution": "resolved" if supports_identity else "unavailable",
    }
    if epoch == "process-versioned":
        expected_receipt["triggerCode"] = 42
        assert requests[1].method == "GET"
        assert requests[1].url.path.endswith("/projects/7/process-instances/trigger")
        assert dict(requests[1].url.params) == {"triggerCode": "42"}
    assert applied.data == expected_receipt
    assert len(requests) == (2 if epoch == "process-versioned" else 1)
