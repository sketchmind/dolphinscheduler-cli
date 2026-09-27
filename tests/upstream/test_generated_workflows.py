from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.services._workflow.render import serialize_workflow_dag
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.wire import WireContractError
from dsctl.upstream.workflows import (
    _RECIPE_BY_VERSION,
    WorkflowAdapter,
    WorkflowDomain,
    WorkflowOperations,
    _execution_form_values,
    bind_workflow_definition_wire,
    bind_workflow_execution_wire,
)
from tests.support import make_profile


@dataclass(frozen=True)
class _CurrentUser:
    tenantCode: str = "analytics"  # noqa: N815


class _CurrentUserOperations:
    def current(self) -> _CurrentUser:
        return _CurrentUser()


def _scope() -> WorkflowScope:
    project = ProjectRef(NativeCode(101), "demo", None)
    workflow = WorkflowRef(NativeCode(202), "daily", 7)
    return WorkflowScope(
        project=project,
        workflow=workflow,
        view=WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=1,
            description=None,
            global_params="[]",
            global_param_map=None,
            create_time=None,
            update_time=None,
            user_id=1,
            user_name="admin",
            project_name="demo",
            timeout=0,
            release_state="ONLINE",
            execution_type="PARALLEL",
            include_execution_type=True,
        ),
    )


def _legacy_scope() -> WorkflowScope:
    project = ProjectRef(NativeId(101), "demo", None)
    workflow = WorkflowRef(NativeId(202), "daily", 7)
    return WorkflowScope(
        project=project,
        workflow=workflow,
        view=WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=202,
            description=None,
            global_params="[]",
            global_param_map=None,
            create_time=None,
            update_time=None,
            user_id=1,
            user_name="admin",
            project_name="demo",
            timeout=0,
            release_state="OFFLINE",
            execution_type=None,
            include_execution_type=False,
        ),
    )


def test_ds_139_legacy_create_plan_uses_string_native_wire_contract() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            201,
            json={
                "code": 0,
                "msg": "success",
                "data": {"processDefinitionId": 202},
            },
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = WorkflowDomain(operations).prepare_definition_create(
            _legacy_scope().project,
            name="daily",
            description="daily test",
            payload={
                "processDefinitionJson": '{"globalParams":[],"tasks":[],"timeout":0}',
                "locations": '{"task-a":{"x":0,"y":0}}',
                "connects": '[{"source":"task-a","target":"task-b"}]',
            },
        )

        assert requests_seen == []
        assert prepared.request.method == "POST"
        assert prepared.request.path == "/projects/demo/process/save"
        assert prepared.request.form == {
            "name": "daily",
            "description": "daily test",
            "processDefinitionJson": '{"globalParams":[],"tasks":[],"timeout":0}',
            "locations": '{"task-a":{"x":0,"y":0}}',
            "connects": '[{"source":"task-a","target":"task-b"}]',
        }

        operations.apply_create(prepared)

    assert requests_seen == [
        (
            "/dolphinscheduler/projects/demo/process/save",
            {
                "name": ["daily"],
                "description": ["daily test"],
                "processDefinitionJson": ['{"globalParams":[],"tasks":[],"timeout":0}'],
                "locations": ['{"task-a":{"x":0,"y":0}}'],
                "connects": ['[{"source":"task-a","target":"task-b"}]'],
            },
        )
    ]


def test_ds_139_legacy_update_plan_uses_id_and_string_native_wire_contract() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        assert operations.workflow_graph_family == "legacy-json"
        prepared = WorkflowDomain(operations).prepare_definition_update(
            _legacy_scope(),
            name="daily",
            description=None,
            payload={
                "processDefinitionJson": '{"globalParams":[],"tasks":[],"timeout":0}',
                "locations": '{"task-a":{"x":0,"y":0}}',
                "connects": '[{"source":"task-a","target":"task-b"}]',
            },
        )

        assert requests_seen == []
        assert prepared.request.method == "POST"
        assert prepared.request.path == "/projects/demo/process/update"
        assert prepared.request.form == {
            "name": "daily",
            "id": 202,
            "processDefinitionJson": '{"globalParams":[],"tasks":[],"timeout":0}',
            "locations": '{"task-a":{"x":0,"y":0}}',
            "connects": '[{"source":"task-a","target":"task-b"}]',
        }

        operations.apply_update(prepared)

    assert requests_seen == [
        (
            "/dolphinscheduler/projects/demo/process/update",
            {
                "name": ["daily"],
                "id": ["202"],
                "processDefinitionJson": ['{"globalParams":[],"tasks":[],"timeout":0}'],
                "locations": ['{"task-a":{"x":0,"y":0}}'],
                "connects": ['[{"source":"task-a","target":"task-b"}]'],
            },
        )
    ]


def test_code_native_workflow_operations_report_code_native_graph_family() -> None:
    profile = make_profile(ds_version="3.4.2")
    adapter = WorkflowAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: httpx.Response(500)),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        assert operations.workflow_graph_family == "code-native"


def test_definition_apply_rejects_a_prepared_graph_from_another_wire_epoch() -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    legacy_profile = make_profile(ds_version="1.3.9")
    legacy_adapter = WorkflowAdapter.for_version("1.3.9")
    legacy_http = DolphinSchedulerClient(
        legacy_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    modern_profile = make_profile(ds_version="3.4.2")
    modern_adapter = WorkflowAdapter.for_version("3.4.2")
    modern_http = DolphinSchedulerClient(
        modern_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with legacy_http, modern_http:
        legacy_operations = _operations(
            legacy_adapter,
            legacy_profile,
            legacy_http,
        )
        modern_operations = _operations(
            modern_adapter,
            modern_profile,
            modern_http,
        )
        legacy = legacy_operations.prepare_legacy_create(
            _legacy_scope().project,
            name="daily",
            description=None,
            process_definition_json='{"globalParams":[],"tasks":[],"timeout":0}',
            locations="{}",
            connects="[]",
        )
        modern = modern_operations.prepare_create(
            _scope().project,
            name="daily",
            description=None,
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type="PARALLEL",
        )
        legacy_update = legacy_operations.prepare_legacy_update(
            _legacy_scope(),
            name="daily",
            description=None,
            process_definition_json='{"globalParams":[],"tasks":[],"timeout":0}',
            locations="{}",
            connects="[]",
        )
        modern_update = modern_operations.prepare_update(
            _scope(),
            name="daily",
            description=None,
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type="PARALLEL",
            release_state="OFFLINE",
            tenant_code=None,
        )
        legacy_release = legacy_operations.prepare_release(
            _legacy_scope().project,
            workflow_code=202,
            state="ONLINE",
        )
        modern_release = modern_operations.prepare_release(
            _scope().project,
            workflow_code=202,
            state="ONLINE",
        )

        with pytest.raises(WireContractError, match="no workflow-create wire program"):
            legacy_operations.apply_create(modern)
        with pytest.raises(
            WireContractError,
            match="no legacy workflow-create wire program",
        ):
            modern_operations.apply_create(legacy)
        with pytest.raises(WireContractError, match="no workflow-update wire program"):
            legacy_operations.apply_update(modern_update)
        with pytest.raises(
            WireContractError,
            match="no legacy workflow-update wire program",
        ):
            modern_operations.apply_update(legacy_update)
        with pytest.raises(WireContractError, match=r"no code-native.*release"):
            legacy_operations.apply_release(modern_release)
        with pytest.raises(WireContractError, match="no legacy workflow-release"):
            modern_operations.apply_release(legacy_release)

    assert requests_seen == 0


def test_ds_139_legacy_definition_reads_exact_string_native_detail() -> None:
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 202,
                    "name": "daily",
                    "version": 7,
                    "releaseState": "OFFLINE",
                    "projectId": 101,
                    "description": "daily test",
                    "processDefinitionJson": (
                        '{"globalParams":[],"tasks":[],"timeout":0}'
                    ),
                    "locations": '{"task-a":{"x":0,"y":0}}',
                    "connects": '[{"source":"task-a","target":"task-b"}]',
                },
            },
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    scope = _legacy_scope()
    with http_client:
        snapshot = _operations(adapter, profile, http_client).legacy_definition(
            scope,
            action="workflow.export",
        )

    assert snapshot.scope is scope
    assert snapshot.name == "daily"
    assert snapshot.description == "daily test"
    assert snapshot.release_state == "OFFLINE"
    assert snapshot.process_definition_json == (
        '{"globalParams":[],"tasks":[],"timeout":0}'
    )
    assert snapshot.locations == '{"task-a":{"x":0,"y":0}}'
    assert snapshot.connects == '[{"source":"task-a","target":"task-b"}]'
    assert len(requests_seen) == 1
    assert requests_seen[0].method == "GET"
    assert requests_seen[0].url.path == (
        "/dolphinscheduler/projects/demo/process/select-by-id"
    )
    assert dict(requests_seen[0].url.params) == {"processId": "202"}


def test_legacy_definition_rejects_missing_native_json_and_modern_profiles() -> None:
    requests_seen = 0

    def missing_locations(request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "id": 202,
                    "name": "daily",
                    "processDefinitionJson": '{"tasks":[]}',
                    "locations": None,
                    "connects": "[]",
                },
            },
        )

    legacy_profile = make_profile(ds_version="1.3.9")
    legacy_adapter = WorkflowAdapter.for_version("1.3.9")
    legacy_http = DolphinSchedulerClient(
        legacy_profile,
        transport=httpx.MockTransport(missing_locations),
    )
    with legacy_http, pytest.raises(ApiTransportError) as exc_info:
        _operations(
            legacy_adapter,
            legacy_profile,
            legacy_http,
        ).legacy_definition(
            _legacy_scope(),
            action="workflow.describe",
        )

    modern_profile = make_profile(ds_version="3.4.2")
    modern_adapter = WorkflowAdapter.for_version("3.4.2")
    modern_http = DolphinSchedulerClient(
        modern_profile,
        transport=httpx.MockTransport(missing_locations),
    )
    with (
        modern_http,
        pytest.raises(
            WireContractError,
            match="legacy-json workflow detail",
        ),
    ):
        _operations(
            modern_adapter,
            modern_profile,
            modern_http,
        ).legacy_definition(
            _scope(),
            action="workflow.describe",
        )

    assert exc_info.value.details["field"] == "locations"
    assert requests_seen == 1


def test_ds_139_name_native_execution_plan_is_the_exact_request_applied() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = operations.prepare_execution(
            _legacy_scope(),
            schedule_time="2026-08-05 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="ignored-on-legacy",
            start_node_list=["extract", "load"],
            task_scope="self",
        )

        assert requests_seen == []
        assert prepared.request.method == "POST"
        assert prepared.request.path == (
            "/projects/demo/executors/start-process-instance"
        )
        assert prepared.request.form == {
            "processDefinitionId": 202,
            "scheduleTime": "2026-08-05 00:00:00",
            "failureStrategy": "CONTINUE",
            "startNodeList": "extract,load",
            "taskDependType": "TASK_ONLY",
            "execType": "START_PROCESS",
            "warningType": "NONE",
            "warningGroupId": 0,
            "processInstancePriority": "MEDIUM",
            "workerGroup": "default",
        }

        assert operations.apply_execution(prepared).to_data() == {
            "accepted": True,
            "workflowInstanceIds": [],
            "instanceResolution": "unavailable",
        }

    assert requests_seen == [
        (
            "/dolphinscheduler/projects/demo/executors/start-process-instance",
            {key: [str(value)] for key, value in prepared.request.form.items()},
        )
    ]


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize(
    ("warning_group_id", "expected"),
    [
        pytest.param(None, None, id="native-no-warning-group"),
        pytest.param(17, 17, id="explicit-warning-group"),
    ],
)
def test_execution_plan_projects_native_warning_group_for_every_profile(
    version: str,
    warning_group_id: int | None,
    expected: int | None,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    scope = _legacy_scope() if version == "1.3.9" else _scope()
    with http_client:
        prepared = _operations(adapter, profile, http_client).prepare_execution(
            scope,
            schedule_time="",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="default",
            warning_group_id=warning_group_id,
        )

    assert requests_seen == 0
    assert prepared.request.form is not None
    if warning_group_id is None and version.startswith(("1.3.9", "2.0.")):
        assert prepared.request.form["warningGroupId"] == 0
    elif expected is None:
        assert "warningGroupId" not in prepared.request.form
    else:
        assert prepared.request.form["warningGroupId"] == expected
    assert "timeout" not in prepared.request.form


@pytest.mark.parametrize(
    ("version", "start_nodes", "message"),
    [
        pytest.param("1.3.9", [303], "non-empty comma-free", id="legacy-code"),
        pytest.param(
            "1.3.9",
            ["extract", 303],
            "non-empty comma-free",
            id="legacy-mixed",
        ),
        pytest.param(
            "1.3.9",
            ["extract,load"],
            "non-empty comma-free",
            id="legacy-ambiguous-name",
        ),
        pytest.param("3.4.2", ["extract"], "positive task codes", id="modern-name"),
        pytest.param("3.4.2", [0], "positive task codes", id="modern-zero"),
        pytest.param(
            "3.4.2",
            [303, "extract"],
            "positive task codes",
            id="modern-mixed",
        ),
    ],
)
def test_execution_start_nodes_fail_closed_by_exact_identity_epoch(
    version: str,
    start_nodes: list[int | str],
    message: str,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    scope = _legacy_scope() if version == "1.3.9" else _scope()
    with http_client, pytest.raises(WireContractError, match=message):
        _operations(adapter, profile, http_client).prepare_execution(
            scope,
            schedule_time="2026-08-05 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
            start_node_list=cast("Any", start_nodes),
            task_scope="self",
        )

    assert requests_seen == 0


def test_code_native_execution_plan_keeps_positive_code_start_nodes() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": [901]},
        )

    profile = make_profile(ds_version="3.4.2")
    adapter = WorkflowAdapter.for_version("3.4.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = operations.prepare_execution(
            _scope(),
            schedule_time="2026-08-05 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
            start_node_list=[303],
            task_scope="self",
            warning_group_id=0,
            environment_code=-1,
        )

        assert requests_seen == []
        assert prepared.request.path == (
            "/projects/101/executors/start-workflow-instance"
        )
        assert prepared.request.form is not None
        assert prepared.request.form["startNodeList"] == "303"
        assert operations.apply_execution(prepared).workflow_instance_ids == (901,)

    assert len(requests_seen) == 1


def test_ds_139_release_plan_uses_project_name_and_integer_release_state() -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {}},
        )

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        preview = operations.prepare_release(
            _legacy_scope().project,
            workflow_code="<daily:created_process_definition_id>",
            state="ONLINE",
        )
        prepared = operations.prepare_release(
            _legacy_scope().project,
            workflow_code=202,
            state="ONLINE",
        )

        assert requests_seen == []
        assert preview.request.path == "/projects/demo/process/release"
        assert preview.request.form == {
            "processId": "<daily:created_process_definition_id>",
            "releaseState": 1,
        }
        with pytest.raises(WireContractError, match="preview workflow id"):
            operations.apply_release(preview)
        assert requests_seen == []
        assert prepared.request.path == "/projects/demo/process/release"
        assert prepared.request.form == {
            "processId": 202,
            "releaseState": 1,
        }
        operations.apply_release(prepared)

    assert requests_seen == [
        (
            "/dolphinscheduler/projects/demo/process/release",
            {"processId": ["202"], "releaseState": ["1"]},
        )
    ]


def test_ds_32_definition_and_execution_tenant_contracts_are_independent() -> None:
    for version in ("3.2.0", "3.2.1", "3.2.2"):
        recipe = _RECIPE_BY_VERSION[version]

        assert recipe.definition_tenant_code is False
        assert recipe.execution_tenant_code is True


@pytest.mark.parametrize(
    "version",
    ["2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0"],
)
def test_exact_workflow_dag_projects_task_fields_absent_from_older_models(
    version: str,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "GET"
        assert request.url.path == (
            "/dolphinscheduler/projects/101/process-definition/202"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "processDefinition": {
                        "code": 202,
                        "projectCode": 101,
                        "version": 7,
                    },
                    "processTaskRelationList": [],
                    "taskDefinitionList": [
                        {
                            "id": 1,
                            "code": 303,
                            "name": "extract",
                            "version": 4,
                            "projectCode": 101,
                            **(
                                {"taskGroupId": 12, "taskGroupPriority": 3}
                                if version.startswith(("3.0.", "3.1."))
                                else {}
                            ),
                            **(
                                {
                                    "taskExecuteType": "STREAM",
                                    "cpuQuota": 8,
                                    "memoryMax": 1024,
                                }
                                if version == "3.1.0"
                                else {}
                            ),
                        }
                    ],
                },
            },
        )

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        dag = _operations(adapter, profile, http_client).dag(
            _scope(),
            action="workflow.get",
        )

    rendered = serialize_workflow_dag(dag, attached_schedule=None)
    task = cast("dict[str, object]", rendered["tasks"][0])
    assert calls == 1
    has_task_group = version.startswith(("3.0.", "3.1."))
    assert task["taskGroupId"] == (12 if has_task_group else 0)
    assert task["taskGroupPriority"] == (3 if has_task_group else 0)
    assert task["taskExecuteType"] == ("STREAM" if version == "3.1.0" else None)
    assert task["cpuQuota"] == (8 if version == "3.1.0" else None)
    assert task["memoryMax"] == (1024 if version == "3.1.0" else None)


def test_ds_32_executor_form_keeps_required_tenant_code() -> None:
    values = _execution_form_values(
        _RECIPE_BY_VERSION["3.2.2"],
        scope=_scope(),
        schedule_time="2026-08-05 00:00:00",
        command_type="START_PROCESS",
        worker_group="default",
        tenant_code="analytics",
        start_node_list=None,
        task_scope=None,
        failure_strategy="CONTINUE",
        warning_type="NONE",
        workflow_instance_priority="MEDIUM",
        warning_group_id=0,
        environment_code=-1,
        start_params=None,
        execution_dry_run=False,
        run_mode=None,
        expected_parallelism_number=None,
        complement_dependent_mode=None,
        all_level_dependent=False,
        execution_order=None,
    )

    assert values["processDefinitionCode"] == 202
    assert values["tenantCode"] == "analytics"
    assert values["version"] == 7


@pytest.mark.parametrize(
    "version",
    tuple(version for version in TARGET_DS_VERSIONS if version != "1.3.9"),
)
def test_definition_plans_capture_without_http_for_every_code_native_profile(
    version: str,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        create = operations.prepare_create(
            _scope().project,
            name="daily",
            description="daily test",
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type="PARALLEL",
        )
        update = operations.prepare_update(
            _scope(),
            name="daily",
            description="daily test updated",
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type="PARALLEL",
            release_state="OFFLINE",
            tenant_code="analytics",
        )
        release = operations.prepare_release(
            _scope().project,
            workflow_code="<daily:created_workflow_code>",
            state="ONLINE",
        )

    assert create.request.method == "POST"
    assert update.request.method == "PUT"
    assert release.request.method == "POST"
    assert requests_seen == 0


def test_ds_32_generated_mutations_keep_definition_and_executor_tenant_contracts_separate() -> (  # noqa: E501
    None
):
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        requests_seen.append((request.url.path, form))
        workflow_result = {
            "code": 202,
            "name": "daily",
            "version": 7,
            "projectCode": 101,
            "releaseState": "ONLINE",
        }
        result: object = (
            42
            if len(requests_seen) == 3
            else [{"id": 901}, {"id": 902}]
            if request.url.path.endswith("/trigger")
            else workflow_result
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": result},
        )

    profile = make_profile(ds_version="3.2.2")
    adapter = WorkflowAdapter.for_version("3.2.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    scope = _scope()
    with http_client:
        operations = _operations(adapter, profile, http_client)
        operations.create(
            scope.project,
            name="daily",
            description="daily test",
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type=None,
        )
        operations.update(
            scope,
            name="daily",
            description="daily test updated",
            global_params="[]",
            locations="{}",
            timeout=0,
            task_relation_json="[]",
            task_definition_json="[]",
            execution_type=None,
            release_state="ONLINE",
            tenant_code="must-not-leak",
        )
        instance_ids = operations.run(
            scope,
            schedule_time="2026-08-05 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
        )

    assert len(requests_seen) == 4
    assert "tenantCode" not in requests_seen[0][1]
    assert "tenantCode" not in requests_seen[1][1]
    assert requests_seen[2][1]["tenantCode"] == ["analytics"]
    assert instance_ids.workflow_instance_ids == (901, 902)
    assert instance_ids.trigger_code == 42
    assert instance_ids.instance_resolution == "resolved"


@pytest.mark.parametrize(
    ("version", "definition_noun", "expected_version_fields"),
    [
        pytest.param(
            "2.0.9",
            "process",
            {"tenantCode": "analytics"},
            id="process-with-tenant-without-execution-type",
        ),
        pytest.param(
            "3.1.0",
            "process",
            {"tenantCode": "analytics", "executionType": "PARALLEL"},
            id="process-with-tenant-and-execution-type",
        ),
        pytest.param(
            "3.2.2",
            "process",
            {"executionType": "PARALLEL"},
            id="process-without-tenant",
        ),
        pytest.param(
            "3.3.1",
            "workflow",
            {"executionType": "PARALLEL"},
            id="workflow-vocabulary",
        ),
        pytest.param(
            "3.4.2",
            "workflow",
            {"executionType": "PARALLEL"},
            id="latest-workflow-vocabulary",
        ),
    ],
)
def test_create_plan_is_the_exact_request_applied_for_each_wire_epoch(
    version: str,
    definition_noun: str,
    expected_version_fields: dict[str, str],
) -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": 202,
                    "name": "daily",
                    "version": 7,
                    "projectCode": 101,
                    "releaseState": "OFFLINE",
                },
            },
        )

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = WorkflowDomain(operations).prepare_definition_create(
            _scope().project,
            name="daily",
            description="daily test",
            payload={
                "name": "daily",
                "description": "daily test",
                "globalParams": "[]",
                "locations": "{}",
                "timeout": 0,
                "taskRelationJson": "[]",
                "taskDefinitionJson": "[]",
                "executionType": "PARALLEL",
            },
        )

        assert requests_seen == []
        assert prepared.request.method == "POST"
        assert prepared.request.path == (f"/projects/101/{definition_noun}-definition")
        assert prepared.request.form == {
            "name": "daily",
            "description": "daily test",
            "globalParams": "[]",
            "locations": "{}",
            "timeout": 0,
            "taskRelationJson": "[]",
            "taskDefinitionJson": "[]",
            **expected_version_fields,
        }

        operations.apply_create(prepared)

    assert len(requests_seen) == 1
    actual_path, actual_form = requests_seen[0]
    assert actual_path.endswith(prepared.request.path)
    assert actual_form == {
        key: [str(value)] for key, value in prepared.request.form.items()
    }


@pytest.mark.parametrize(
    ("version", "definition_noun", "expected_version_fields"),
    [
        pytest.param(
            "2.0.9",
            "process",
            {"tenantCode": "analytics"},
            id="process-with-tenant-without-execution-type",
        ),
        pytest.param(
            "3.1.0",
            "process",
            {"tenantCode": "analytics", "executionType": "PARALLEL"},
            id="process-with-tenant-and-execution-type",
        ),
        pytest.param(
            "3.2.0",
            "process",
            {"executionType": "PARALLEL"},
            id="process-other-params-epoch",
        ),
        pytest.param(
            "3.2.1",
            "process",
            {"executionType": "PARALLEL"},
            id="process-without-update-other-params",
        ),
        pytest.param(
            "3.2.2",
            "process",
            {"executionType": "PARALLEL"},
            id="last-process-vocabulary",
        ),
        pytest.param(
            "3.3.1",
            "workflow",
            {"executionType": "PARALLEL"},
            id="workflow-vocabulary",
        ),
        pytest.param(
            "3.4.2",
            "workflow",
            {"executionType": "PARALLEL"},
            id="latest-workflow-vocabulary",
        ),
    ],
)
def test_update_plan_is_the_exact_request_applied_for_each_wire_epoch(
    version: str,
    definition_noun: str,
    expected_version_fields: dict[str, str],
) -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": 202,
                    "name": "daily",
                    "version": 8,
                    "projectCode": 101,
                    "releaseState": "OFFLINE",
                },
            },
        )

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = WorkflowDomain(operations).prepare_definition_update(
            _scope(),
            name="daily",
            description="daily test updated",
            payload={
                "name": "daily",
                "description": "daily test updated",
                "globalParams": "[]",
                "locations": "{}",
                "timeout": 0,
                "taskRelationJson": "[]",
                "taskDefinitionJson": "[]",
                "executionType": "PARALLEL",
                "releaseState": "OFFLINE",
            },
        )

        assert requests_seen == []
        assert prepared.request.method == "PUT"
        assert prepared.request.path == (
            f"/projects/101/{definition_noun}-definition/202"
        )
        assert prepared.request.form == {
            "name": "daily",
            "description": "daily test updated",
            "globalParams": "[]",
            "locations": "{}",
            "timeout": 0,
            "taskRelationJson": "[]",
            "taskDefinitionJson": "[]",
            "releaseState": "OFFLINE",
            **expected_version_fields,
        }

        operations.apply_update(prepared)

    assert len(requests_seen) == 1
    actual_path, actual_form = requests_seen[0]
    assert actual_path.endswith(prepared.request.path)
    assert actual_form == {
        key: [str(value)] for key, value in prepared.request.form.items()
    }


@pytest.mark.parametrize(
    ("version", "definition_noun"),
    [
        pytest.param("2.0.9", "process", id="process-vocabulary"),
        pytest.param("3.4.2", "workflow", id="workflow-vocabulary"),
    ],
)
def test_release_plan_uses_the_exact_definition_vocabulary_without_http(
    version: str,
    definition_noun: str,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = operations.prepare_release(
            _scope().project,
            workflow_code="<daily:created_workflow_code>",
            state="ONLINE",
        )

    assert requests_seen == 0
    assert prepared.request.method == "POST"
    assert prepared.request.path == (
        f"/projects/101/{definition_noun}-definition/"
        "<daily:created_workflow_code>/release"
    )
    assert prepared.request.form == {"releaseState": "ONLINE"}
    with pytest.raises(WireContractError, match="preview workflow code"):
        operations.apply_release(prepared)
    assert requests_seen == 0


@pytest.mark.parametrize(
    ("version", "definition_noun"),
    [
        pytest.param("2.0.9", "process", id="process-vocabulary"),
        pytest.param("3.4.2", "workflow", id="workflow-vocabulary"),
    ],
)
def test_release_plan_with_native_code_is_the_exact_request_applied(
    version: str,
    definition_noun: str,
) -> None:
    requests_seen: list[tuple[str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (
                request.url.path,
                parse_qs(request.content.decode(), keep_blank_values=True),
            )
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": True},
        )

    profile = make_profile(ds_version=version)
    adapter = WorkflowAdapter.for_version(version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        operations = _operations(adapter, profile, http_client)
        prepared = operations.prepare_release(
            _scope().project,
            workflow_code=202,
            state="ONLINE",
        )

        assert requests_seen == []
        operations.apply_release(prepared)

    assert requests_seen == [
        (
            f"/dolphinscheduler/projects/101/{definition_noun}-definition/202/release",
            {"releaseState": ["ONLINE"]},
        )
    ]


def test_expected_parallelism_boundary_is_exact_and_zero_request() -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    legacy_profile = make_profile(ds_version="1.3.9")
    legacy_adapter = WorkflowAdapter.for_version("1.3.9")
    legacy_http = DolphinSchedulerClient(
        legacy_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with legacy_http:
        legacy = _operations(legacy_adapter, legacy_profile, legacy_http)
        assert legacy.normalize_expected_parallelism_number(None) is None
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            legacy.normalize_expected_parallelism_number(4)

    modern_profile = make_profile(ds_version="2.0.0")
    modern_adapter = WorkflowAdapter.for_version("2.0.0")
    modern_http = DolphinSchedulerClient(
        modern_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with modern_http:
        modern = _operations(modern_adapter, modern_profile, modern_http)
        assert modern.normalize_expected_parallelism_number(None) == 2
        assert modern.normalize_expected_parallelism_number(4) == 4

    assert requests_seen == 0
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.backfill",
        "parameter": "expectedParallelismNumber",
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


def test_start_params_boundary_is_exact_and_zero_request() -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    legacy_profile = make_profile(ds_version="1.3.9")
    legacy_adapter = WorkflowAdapter.for_version("1.3.9")
    legacy_http = DolphinSchedulerClient(
        legacy_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with legacy_http:
        legacy = _operations(legacy_adapter, legacy_profile, legacy_http)
        assert legacy.normalize_start_params(None, action="workflow.run") is None
        with pytest.raises(UnsupportedFeatureError) as exc_info:
            legacy.normalize_start_params(
                '{"bizdate":"20260415"}',
                action="workflow.run",
            )

    modern_profile = make_profile(ds_version="2.0.0")
    modern_adapter = WorkflowAdapter.for_version("2.0.0")
    modern_http = DolphinSchedulerClient(
        modern_profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with modern_http:
        modern = _operations(modern_adapter, modern_profile, modern_http)
        assert (
            modern.normalize_start_params(
                '{"bizdate":"20260415"}',
                action="workflow.run",
            )
            == '{"bizdate":"20260415"}'
        )

    assert requests_seen == 0
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.run",
        "parameter": "startParams",
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


@pytest.mark.parametrize(
    ("options", "parameter"),
    [
        pytest.param({"tenant_code": "analytics"}, "tenantCode", id="tenant"),
        pytest.param({"environment_code": 7}, "environmentCode", id="environment"),
        pytest.param({"execution_dry_run": True}, "dryRun", id="dry-run"),
        pytest.param(
            {"complement_dependent_mode": "ALL_DEPENDENT"},
            "complementDependentMode",
            id="dependent-mode",
        ),
        pytest.param(
            {"all_level_dependent": True},
            "allLevelDependent",
            id="all-level-dependent",
        ),
        pytest.param(
            {"execution_order": "ASC_ORDER"},
            "executionOrder",
            id="execution-order",
        ),
    ],
)
def test_ds_139_execution_option_preflight_rejects_explicit_unsupported_values(
    options: dict[str, object],
    parameter: str,
) -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_client, pytest.raises(UnsupportedFeatureError) as exc_info:
        _operations(adapter, profile, http_client).require_execution_options(
            action="workflow.backfill",
            **cast("Any", options),
        )

    assert requests_seen == 0
    assert exc_info.value.details == {
        "resource": "workflow",
        "action": "workflow.backfill",
        "parameter": parameter,
        "ds_version": "1.3.9",
        "reason": "upstream_capability_absent",
    }


def test_ds_139_execution_option_preflight_accepts_implicit_defaults_and_run_mode() -> (
    None
):
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile = make_profile(ds_version="1.3.9")
    adapter = WorkflowAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_client:
        _operations(adapter, profile, http_client).require_execution_options(
            action="workflow.backfill",
            tenant_code=None,
            environment_code=None,
            execution_dry_run=False,
            complement_dependent_mode=None,
            all_level_dependent=False,
            execution_order=None,
            run_mode="RUN_MODE_SERIAL",
        )

    assert requests_seen == 0


def test_execution_option_preflight_uses_exact_recipe_boundaries() -> None:
    requests_seen = 0

    def unexpected_request(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    profile_200 = make_profile(ds_version="2.0.0")
    adapter_200 = WorkflowAdapter.for_version("2.0.0")
    http_200 = DolphinSchedulerClient(
        profile_200,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_200, pytest.raises(UnsupportedFeatureError) as exc_info:
        _operations(adapter_200, profile_200, http_200).require_execution_options(
            action="workflow.backfill",
            complement_dependent_mode="ALL_DEPENDENT",
        )

    profile_300 = make_profile(ds_version="3.0.0")
    adapter_300 = WorkflowAdapter.for_version("3.0.0")
    http_300 = DolphinSchedulerClient(
        profile_300,
        transport=httpx.MockTransport(unexpected_request),
    )
    with http_300:
        _operations(adapter_300, profile_300, http_300).require_execution_options(
            action="workflow.backfill",
            environment_code=7,
            execution_dry_run=True,
            complement_dependent_mode="ALL_DEPENDENT",
        )

    assert requests_seen == 0
    assert exc_info.value.details["parameter"] == "complementDependentMode"


def test_expected_parallelism_wire_field_starts_in_ds_2_0() -> None:
    def form_values(version: str) -> dict[str, Any]:
        return _execution_form_values(
            _RECIPE_BY_VERSION[version],
            scope=_scope(),
            schedule_time="2026-08-05 00:00:00",
            command_type="COMPLEMENT_DATA",
            worker_group="default",
            tenant_code="analytics",
            start_node_list=None,
            task_scope=None,
            failure_strategy="CONTINUE",
            warning_type="NONE",
            workflow_instance_priority="MEDIUM",
            warning_group_id=0,
            environment_code=-1,
            start_params=None,
            execution_dry_run=False,
            run_mode="RUN_MODE_PARALLEL",
            expected_parallelism_number=2,
            complement_dependent_mode=None,
            all_level_dependent=False,
            execution_order=None,
        )

    legacy = form_values("1.3.9")
    modern = form_values("2.0.0")

    assert "expectedParallelismNumber" not in legacy
    assert modern["expectedParallelismNumber"] == 2


def test_start_params_wire_field_starts_in_ds_2_0() -> None:
    def form_values(version: str) -> dict[str, Any]:
        return _execution_form_values(
            _RECIPE_BY_VERSION[version],
            scope=_scope(),
            schedule_time="2026-08-05 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
            start_node_list=None,
            task_scope=None,
            failure_strategy="CONTINUE",
            warning_type="NONE",
            workflow_instance_priority="MEDIUM",
            warning_group_id=0,
            environment_code=-1,
            start_params='{"bizdate":"20260415"}',
            execution_dry_run=False,
            run_mode=None,
            expected_parallelism_number=None,
            complement_dependent_mode=None,
            all_level_dependent=False,
            execution_order=None,
        )

    legacy = form_values("1.3.9")
    modern = form_values("2.0.0")

    assert "startParams" not in legacy
    assert modern["startParams"] == '{"bizdate":"20260415"}'


def test_workflow_operations_expose_exact_name_project_reads() -> None:
    numeric_name = ProjectRef(NativeId(9), "123", None)
    other = ProjectRef(NativeId(10), "other", None)

    class DefinitionReadsStub:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        def resolve_project_by_name(self, project_name: str) -> ProjectRef:
            self.calls.append(("resolve_project_by_name", project_name))
            return numeric_name

        def visible_project_refs(self) -> tuple[ProjectRef, ...]:
            self.calls.append(("visible_project_refs", None))
            return (numeric_name, other)

    definitions = DefinitionReadsStub()
    operations = WorkflowOperations(
        programs=cast("Any", object()),
        bindings=cast("Any", object()),
        definition_wire=cast("Any", object()),
        execution_wire=cast("Any", object()),
        definitions=cast("Any", definitions),
        current_user=cast("Any", object()),
        schedules=cast("Any", object()),
        project_preferences=None,
    )

    assert operations.resolve_project_by_name("123") is numeric_name
    assert operations.visible_project_refs() == (numeric_name, other)
    assert definitions.calls == [
        ("resolve_project_by_name", "123"),
        ("visible_project_refs", None),
    ]


def _operations(
    adapter: WorkflowAdapter,
    profile: Any,
    http_client: DolphinSchedulerClient,
) -> WorkflowOperations:
    return WorkflowOperations(
        programs=WORKFLOW_PROGRAMS.bind(
            adapter._bindings.compiled, profile, http_client=http_client
        ),
        bindings=adapter._bindings,
        definition_wire=bind_workflow_definition_wire(
            http_client,
            bindings=adapter._bindings,
        ),
        execution_wire=bind_workflow_execution_wire(
            http_client,
            bindings=adapter._bindings,
        ),
        definitions=cast("Any", object()),
        current_user=cast("Any", _CurrentUserOperations()),
        schedules=cast("Any", object()),
        project_preferences=None,
    )


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2"])
@pytest.mark.parametrize("instance_ids", [[], [901], [901, 902]])
def test_trigger_receipt_resolves_only_actual_instance_ids(
    version: str,
    instance_ids: list[int],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        data = (
            42 if request.method == "POST" else [{"id": item} for item in instance_ids]
        )
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})

    profile = make_profile(ds_version=version)
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        receipt = _operations(
            WorkflowAdapter.for_version(version), profile, client
        ).run(
            _scope(),
            schedule_time="2026-09-09 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
        )
    assert receipt.workflow_instance_ids == tuple(instance_ids)
    assert receipt.trigger_code == 42
    assert receipt.instance_resolution == ("resolved" if instance_ids else "pending")
    assert [request.method for request in requests] == ["POST", "GET"]
    assert requests[1].url.path.endswith("/projects/101/process-instances/trigger")
    assert dict(requests[1].url.params) == {"triggerCode": "42"}


def test_trigger_resolution_failure_preserves_accepted_receipt() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": 42}
            if request.method == "POST"
            else {"code": 30002, "msg": "denied"},
        )

    profile = make_profile(ds_version="3.2.2")
    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as error,
    ):
        _operations(WorkflowAdapter.for_version("3.2.2"), profile, client).run(
            _scope(),
            schedule_time="2026-09-09 00:00:00",
            command_type="START_PROCESS",
            worker_group="default",
            tenant_code="analytics",
        )
    assert error.value.details["mutation_applied"] is True
    assert error.value.details["phase"] == "instance_resolution"
    assert error.value.details["execution"] == {
        "accepted": True,
        "workflowInstanceIds": [],
        "instanceResolution": "pending",
        "triggerCode": 42,
    }
    assert methods == ["POST", "GET"]
