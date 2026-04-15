import io
import json
from typing import Any, cast
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError
from dsctl.generated.versions.ds_3_4_1.api.operations.login import LoginParams
from dsctl.generated.versions.ds_3_4_1.api.operations.project import (
    CreateProjectParams,
    QueryProjectListPagingParams,
)
from dsctl.upstream.adapters.ds_3_4_1 import DS341Adapter, _GeneratedSessionAdapter
from tests.support import make_profile


def test_adapter_bridges_generated_get_requests_through_shared_transport() -> None:
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/projects"
        assert request.url.params["pageNo"] == "1"
        assert request.url.params["pageSize"] == "20"
        assert request.headers["token"] == profile.api_token
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [
                        {"code": 7, "name": "etl-prod", "perm": 0, "defCount": 0}
                    ],
                    "total": 1,
                    "totalPage": 1,
                    "pageSize": 20,
                    "currentPage": 1,
                },
            },
        )

    adapter = DS341Adapter()
    client = adapter.create_client(
        profile,
        transport=httpx.MockTransport(handler),
    )

    payload = client.project.query_project_list_paging(
        QueryProjectListPagingParams(pageNo=1, pageSize=20)
    )

    assert payload.total == 1
    assert payload.totalList is not None
    assert payload.totalList[0].name == "etl-prod"


def test_adapter_bridges_generated_post_form_requests_through_shared_transport() -> (
    None
):
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "http://example.test/dolphinscheduler/projects"
        assert request.headers["token"] == profile.api_token
        assert request.content == b"projectName=demo&description=test+project"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": 7,
                    "name": "demo",
                    "description": "test project",
                    "perm": 0,
                    "defCount": 0,
                },
            },
        )

    adapter = DS341Adapter()
    client = adapter.create_client(
        profile,
        transport=httpx.MockTransport(handler),
    )

    project = client.project.create_project(
        CreateProjectParams(projectName="demo", description="test project")
    )

    assert project.code == 7
    assert project.name == "demo"


def test_adapter_task_type_methods_bridge_favourite_endpoint() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/favourite/taskTypes"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": [
                    {
                        "taskType": "SHELL",
                        "isCollection": True,
                        "taskCategory": "Universal",
                    },
                    {
                        "taskType": "SUB_WORKFLOW",
                        "isCollection": False,
                        "taskCategory": "Logic",
                    },
                ],
            },
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        task_types = session.task_types.list()

    assert [task_type.taskType for task_type in task_types] == [
        "SHELL",
        "SUB_WORKFLOW",
    ]
    assert requests_seen == [("GET", "/dolphinscheduler/favourite/taskTypes")]


def test_adapter_project_worker_group_methods_bridge_get_and_post() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    request_bodies: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.method == "GET":
            assert request.url.path == "/dolphinscheduler/projects/7/worker-group"
            return httpx.Response(
                200,
                json={
                    "status": "SUCCESS",
                    "msg": "success",
                    "data": [
                        {
                            "id": 11,
                            "projectCode": 7,
                            "workerGroup": "default",
                        }
                    ],
                },
            )
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/projects/7/worker-group"
        request_bodies.append(
            parse_qs(request.content.decode(), keep_blank_values=True)
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        worker_groups = session.project_worker_groups.list(project_code=7)
        session.project_worker_groups.set(
            project_code=7,
            worker_groups=["default", "gpu"],
        )
        session.project_worker_groups.set(
            project_code=7,
            worker_groups=[],
        )

    assert [worker_group.workerGroup for worker_group in worker_groups] == ["default"]
    assert request_bodies == [
        {"workerGroups": ["default,gpu"]},
        {"workerGroups": [""]},
    ]
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/worker-group"),
        ("POST", "/dolphinscheduler/projects/7/worker-group"),
        ("POST", "/dolphinscheduler/projects/7/worker-group"),
    ]


def test_adapter_workflow_lineage_methods_bridge_list_get_and_dependent_tasks() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.url.path == "/dolphinscheduler/projects/7/lineages/list":
            assert request.method == "GET"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "data": {
                            "workFlowRelationList": [
                                {
                                    "sourceWorkFlowCode": 101,
                                    "targetWorkFlowCode": 102,
                                }
                            ],
                            "workFlowRelationDetailList": [
                                {
                                    "workFlowCode": 101,
                                    "workFlowName": "daily-sync",
                                    "workFlowPublishStatus": "ONLINE",
                                    "schedulePublishStatus": 1,
                                }
                            ],
                        },
                    },
                },
            )
        if request.url.path == "/dolphinscheduler/projects/7/lineages/101":
            assert request.method == "GET"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "data": {
                            "workFlowRelationList": [
                                {
                                    "sourceWorkFlowCode": 101,
                                    "targetWorkFlowCode": 102,
                                }
                            ],
                            "workFlowRelationDetailList": [
                                {
                                    "workFlowCode": 102,
                                    "workFlowName": "quality-check",
                                    "workFlowPublishStatus": "ONLINE",
                                    "sourceWorkFlowCode": "101",
                                    "schedulePublishStatus": 0,
                                }
                            ],
                        },
                    },
                },
            )
        assert request.url.path == (
            "/dolphinscheduler/projects/7/lineages/query-dependent-tasks"
        )
        assert request.method == "GET"
        assert request.url.params["workFlowCode"] == "101"
        assert request.url.params["taskCode"] == "201"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "data": [
                        {
                            "projectCode": 7,
                            "workflowDefinitionCode": 102,
                            "workflowDefinitionName": "quality-check",
                            "taskDefinitionCode": 302,
                            "taskDefinitionName": "depends-on-extract",
                        }
                    ]
                },
            },
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        project_lineage = session.workflow_lineages.list(project_code=7)
        workflow_lineage = session.workflow_lineages.get(
            project_code=7,
            workflow_code=101,
        )
        dependent_tasks = session.workflow_lineages.query_dependent_tasks(
            project_code=7,
            workflow_code=101,
            task_code=201,
        )

    assert project_lineage is not None
    assert project_lineage.workFlowRelationList is not None
    assert project_lineage.workFlowRelationList[0].targetWorkFlowCode == 102
    assert workflow_lineage is not None
    assert workflow_lineage.workFlowRelationDetailList is not None
    assert workflow_lineage.workFlowRelationDetailList[0].sourceWorkFlowCode == "101"
    assert [task.taskDefinitionCode for task in dependent_tasks] == [302]
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/lineages/list"),
        ("GET", "/dolphinscheduler/projects/7/lineages/101"),
        ("GET", "/dolphinscheduler/projects/7/lineages/query-dependent-tasks"),
    ]


def test_adapter_task_group_methods_bridge_list_and_queue_actions() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    request_bodies: list[dict[str, list[str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.method == "GET":
            if request.url.path == "/dolphinscheduler/task-group/list-paging":
                assert request.url.params["name"] == "etl"
                assert request.url.params["status"] == "1"
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": [
                                {
                                    "id": 7,
                                    "name": "etl",
                                    "projectCode": 11,
                                    "description": "prod",
                                    "groupSize": 4,
                                    "useSize": 1,
                                    "userId": 1,
                                    "status": "YES",
                                }
                            ],
                            "total": 1,
                            "totalPage": 1,
                            "pageSize": 20,
                            "currentPage": 1,
                        },
                    },
                )
            assert (
                request.url.path
                == "/dolphinscheduler/task-group/query-list-by-group-id"
            )
            assert request.url.params["groupId"] == "7"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 31,
                                "taskId": 101,
                                "taskName": "extract",
                                "projectName": "etl-prod",
                                "projectCode": "11",
                                "workflowInstanceName": "daily-etl-1",
                                "groupId": 7,
                                "workflowInstanceId": 501,
                                "priority": 2,
                                "forceStart": 0,
                                "inQueue": 1,
                                "status": "WAIT_QUEUE",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        request_bodies.append(
            parse_qs(request.content.decode(), keep_blank_values=True)
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.task_groups.list(
            page_no=1,
            page_size=20,
            search="etl",
            status=1,
        )
        queues = session.task_groups.list_queues(
            group_id=7,
            page_no=1,
            page_size=20,
        )
        session.task_groups.close(task_group_id=7)
        session.task_groups.start(task_group_id=7)
        session.task_groups.force_start(queue_id=31)
        session.task_groups.set_queue_priority(queue_id=31, priority=5)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "etl"
    assert queues.totalList is not None
    assert queues.totalList[0].taskName == "extract"
    assert requests_seen == [
        ("GET", "/dolphinscheduler/task-group/list-paging"),
        ("GET", "/dolphinscheduler/task-group/query-list-by-group-id"),
        ("POST", "/dolphinscheduler/task-group/close-task-group"),
        ("POST", "/dolphinscheduler/task-group/start-task-group"),
        ("POST", "/dolphinscheduler/task-group/forceStart"),
        ("POST", "/dolphinscheduler/task-group/modifyPriority"),
    ]
    assert request_bodies == [
        {"id": ["7"]},
        {"id": ["7"]},
        {"queueId": ["31"]},
        {"queueId": ["31"], "priority": ["5"]},
    ]


def test_adapter_access_token_methods_bridge_crud_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.method == "GET":
            assert request.url.path == "/dolphinscheduler/access-tokens"
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "50"
            assert request.url.params["searchVal"] == "alice"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 11,
                                "userId": 1,
                                "token": "token-11",
                                "expireTime": "2026-12-31 00:00:00",
                                "userName": "alice",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 50,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/access-tokens"
        ):
            assert parse_qs(request.content.decode()) == {
                "userId": ["1"],
                "expireTime": ["2027-01-01 00:00:00"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 12,
                        "userId": 1,
                        "token": "token-12",
                        "expireTime": "2027-01-01 00:00:00",
                        "userName": "alice",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/access-tokens/generate"
        ):
            assert parse_qs(request.content.decode()) == {
                "userId": ["1"],
                "expireTime": ["2027-01-01 00:00:00"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": "generated-token-1"},
            )
        if request.method == "PUT":
            assert request.url.path == "/dolphinscheduler/access-tokens/11"
            assert parse_qs(request.content.decode()) == {
                "userId": ["1"],
                "expireTime": ["2027-02-02 00:00:00"],
                "token": ["manual-token"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 11,
                        "userId": 1,
                        "token": "manual-token",
                        "expireTime": "2027-02-02 00:00:00",
                        "userName": "alice",
                    },
                },
            )
        assert request.method == "DELETE"
        assert request.url.path == "/dolphinscheduler/access-tokens/11"
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": False},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.access_tokens.list(
            page_no=1,
            page_size=50,
            search="alice",
        )
        created = session.access_tokens.create(
            user_id=1,
            expire_time="2027-01-01 00:00:00",
        )
        generated = session.access_tokens.generate(
            user_id=1,
            expire_time="2027-01-01 00:00:00",
        )
        updated = session.access_tokens.update(
            token_id=11,
            user_id=1,
            expire_time="2027-02-02 00:00:00",
            token="manual-token",
        )
        deleted = session.access_tokens.delete(token_id=11)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].token == "token-11"
    assert created.id == 12
    assert created.token == "token-12"
    assert generated == "generated-token-1"
    assert updated.token == "manual-token"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/access-tokens"),
        ("POST", "/dolphinscheduler/access-tokens"),
        ("POST", "/dolphinscheduler/access-tokens/generate"),
        ("PUT", "/dolphinscheduler/access-tokens/11"),
        ("DELETE", "/dolphinscheduler/access-tokens/11"),
    ]


def test_adapter_cluster_methods_bridge_crud_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/cluster/list-paging"
        ):
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "50"
            assert request.url.params["searchVal"] == "k8s"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 7,
                                "code": 7,
                                "name": "k8s-prod",
                                "config": "kube-prod",
                                "workflowDefinitions": ["wf-a"],
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 50,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/cluster/query-by-code"
        ):
            cluster_code = request.url.params["clusterCode"]
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": int(cluster_code),
                        "code": int(cluster_code),
                        "name": f"cluster-{cluster_code}",
                        "config": f"kube-{cluster_code}",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/cluster/create"
        ):
            assert parse_qs(request.content.decode()) == {
                "name": ["analytics"],
                "config": ["kube-analytics"],
                "description": ["batch"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": 8},
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/cluster/update"
        ):
            assert parse_qs(request.content.decode()) == {
                "code": ["7"],
                "name": ["k8s-prod"],
                "config": ["kube-ops"],
                "description": ["primary"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": {"id": 7, "code": 7}},
            )
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/cluster/delete"
        assert parse_qs(request.content.decode()) == {"clusterCode": ["7"]}
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": True},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.clusters.list(page_no=1, page_size=50, search="k8s")
        created = session.clusters.create(
            name="analytics",
            config="kube-analytics",
            description="batch",
        )
        updated = session.clusters.update(
            code=7,
            name="k8s-prod",
            config="kube-ops",
            description="primary",
        )
        deleted = session.clusters.delete(code=7)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "k8s-prod"
    assert created.code == 8
    assert created.name == "cluster-8"
    assert updated.code == 7
    assert updated.config == "kube-7"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/cluster/list-paging"),
        ("POST", "/dolphinscheduler/cluster/create"),
        ("GET", "/dolphinscheduler/cluster/query-by-code"),
        ("POST", "/dolphinscheduler/cluster/update"),
        ("GET", "/dolphinscheduler/cluster/query-by-code"),
        ("POST", "/dolphinscheduler/cluster/delete"),
    ]


def test_adapter_audit_methods_bridge_audit_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/projects/audit/audit-log-list"
        ):
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "50"
            assert request.url.params["modelTypes"] == "Workflow,Task"
            assert request.url.params["operationTypes"] == "Create"
            assert request.url.params["startDate"] == "2026-04-11 00:00:00"
            assert request.url.params["endDate"] == "2026-04-11 23:59:59"
            assert request.url.params["userName"] == "alice"
            assert request.url.params["modelName"] == "daily"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "userName": "alice",
                                "modelType": "Workflow",
                                "modelName": "daily-etl",
                                "operation": "Create",
                                "createTime": "2026-04-11 10:00:00",
                                "description": "created workflow",
                                "detail": "{}",
                                "latency": "120",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 50,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path
            == "/dolphinscheduler/projects/audit/audit-log-model-type"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "name": "Project",
                            "child": [{"name": "Workflow", "child": None}],
                        }
                    ],
                },
            )
        assert request.method == "GET"
        assert (
            request.url.path
            == "/dolphinscheduler/projects/audit/audit-log-operation-type"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": [{"name": "Create"}],
            },
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.audits.list(
            page_no=1,
            page_size=50,
            model_types=["Workflow", "Task"],
            operation_types=["Create"],
            start_date="2026-04-11 00:00:00",
            end_date="2026-04-11 23:59:59",
            user_name="alice",
            model_name="daily",
        )
        model_types = session.audits.list_model_types()
        operation_types = session.audits.list_operation_types()

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].modelName == "daily-etl"
    assert model_types[0].name == "Project"
    assert model_types[0].child is not None
    assert model_types[0].child[0].name == "Workflow"
    assert operation_types[0].name == "Create"
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/audit/audit-log-list"),
        ("GET", "/dolphinscheduler/projects/audit/audit-log-model-type"),
        ("GET", "/dolphinscheduler/projects/audit/audit-log-operation-type"),
    ]


def test_adapter_does_not_double_unwrap_already_normalized_payloads() -> None:
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "http://example.test/dolphinscheduler/login"
        assert request.content == b"userName=demo&userPassword=secret"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": "project-code",
                    "msg": "raw message",
                    "data": "keep",
                },
            },
        )

    adapter = DS341Adapter()
    client = adapter.create_client(
        profile,
        transport=httpx.MockTransport(handler),
    )

    payload = client.login.login(LoginParams(userName="demo", userPassword="secret"))

    assert payload == {
        "code": "project-code",
        "msg": "raw message",
        "data": "keep",
    }


def test_adapter_rejects_transport_and_client_together() -> None:
    profile = make_profile()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    existing_client = DolphinSchedulerClient(profile, transport=transport)

    with pytest.raises(ValueError, match="either transport or client"):
        DS341Adapter().create_client(
            profile,
            transport=transport,
            client=existing_client,
        )


def test_generated_session_reports_contract_mismatches_as_transport_errors() -> None:
    profile = make_profile()
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )
    session = _GeneratedSessionAdapter(client, base_url=profile.api_url)
    unchecked_session = cast("Any", session)

    with pytest.raises(ApiTransportError, match="adapter contract") as exc_info:
        unchecked_session.request(
            "GET",
            f"{profile.api_url}/projects",
            {},
            timeout=1.0,
        )

    assert exc_info.value.details == {
        "method": "GET",
        "url": f"{profile.api_url}/projects",
    }


def test_adapter_project_methods_use_v2_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    v2_projects_path = "/dolphinscheduler/v2/projects"
    v2_project_detail_path = "/dolphinscheduler/v2/projects/8"

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == v2_projects_path:
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "50"
            assert request.url.params["searchVal"] == "etl"
            assert request.headers["Content-Type"] == "application/json"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {"code": 7, "name": "etl-prod", "perm": 0, "defCount": 0}
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 50,
                        "currentPage": 1,
                    },
                },
            )
        if request.method == "POST" and request.url.path == v2_projects_path:
            assert json.loads(request.content) == {
                "projectName": "demo",
                "description": "test project",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 8,
                        "name": "demo",
                        "description": "test project",
                        "perm": 0,
                        "defCount": 0,
                    },
                },
            )
        if request.method == "GET" and request.url.path == v2_project_detail_path:
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 8,
                        "name": "demo",
                        "description": "test project",
                        "perm": 0,
                        "defCount": 0,
                    },
                },
            )
        if request.method == "PUT" and request.url.path == v2_project_detail_path:
            assert json.loads(request.content) == {
                "projectName": "demo",
                "description": "updated",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 8,
                        "name": "demo",
                        "description": "updated",
                        "perm": 0,
                        "defCount": 0,
                    },
                },
            )
        if request.method == "DELETE" and request.url.path == v2_project_detail_path:
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.projects.list(
            page_no=1,
            page_size=50,
            search="etl",
        )
        created = session.projects.create(
            name="demo",
            description="test project",
        )
        fetched = session.projects.get(code=8)
        updated = session.projects.update(
            code=8,
            name="demo",
            description="updated",
        )
        deleted = session.projects.delete(code=8)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "etl-prod"
    assert created.code == 8
    assert fetched.name == "demo"
    assert updated.description == "updated"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/v2/projects"),
        ("POST", "/dolphinscheduler/v2/projects"),
        ("GET", "/dolphinscheduler/v2/projects/8"),
        ("PUT", "/dolphinscheduler/v2/projects/8"),
        ("DELETE", "/dolphinscheduler/v2/projects/8"),
    ]


def test_adapter_alert_group_methods_use_alert_group_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    list_path = "/dolphinscheduler/alert-groups"
    query_path = "/dolphinscheduler/alert-groups/query"
    detail_path = "/dolphinscheduler/alert-groups/9"

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == list_path:
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "20"
            assert request.url.params["searchVal"] == "ops"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 9,
                                "groupName": "ops",
                                "alertInstanceIds": "7,8",
                                "description": "ops alerts",
                                "createUserId": 1,
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if request.method == "POST" and request.url.path == list_path:
            assert parse_qs(request.content.decode()) == {
                "groupName": ["ops"],
                "description": ["ops alerts"],
                "alertInstanceIds": ["7,8"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 9,
                        "groupName": "ops",
                        "alertInstanceIds": "7,8",
                        "description": "ops alerts",
                        "createUserId": 1,
                    },
                },
            )
        if request.method == "POST" and request.url.path == query_path:
            assert parse_qs(request.content.decode()) == {"id": ["9"]}
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 9,
                        "groupName": "ops",
                        "alertInstanceIds": "7,8",
                        "description": "ops alerts",
                        "createUserId": 1,
                    },
                },
            )
        if request.method == "PUT" and request.url.path == detail_path:
            assert parse_qs(request.content.decode()) == {
                "groupName": ["ops-main"],
                "alertInstanceIds": ["8,9"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 9,
                        "groupName": "ops-main",
                        "alertInstanceIds": "8,9",
                        "description": None,
                        "createUserId": 1,
                    },
                },
            )
        if request.method == "DELETE" and request.url.path == detail_path:
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.alert_groups.list(
            page_no=1,
            page_size=20,
            search="ops",
        )
        created = session.alert_groups.create(
            group_name="ops",
            description="ops alerts",
            alert_instance_ids="7,8",
        )
        fetched = session.alert_groups.get(alert_group_id=9)
        updated = session.alert_groups.update(
            alert_group_id=9,
            group_name="ops-main",
            description=None,
            alert_instance_ids="8,9",
        )
        deleted = session.alert_groups.delete(alert_group_id=9)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].groupName == "ops"
    assert created.id == 9
    assert fetched.groupName == "ops"
    assert updated.groupName == "ops-main"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/alert-groups"),
        ("POST", "/dolphinscheduler/alert-groups"),
        ("POST", "/dolphinscheduler/alert-groups/query"),
        ("PUT", "/dolphinscheduler/alert-groups/9"),
        ("DELETE", "/dolphinscheduler/alert-groups/9"),
    ]


def test_adapter_environment_methods_use_legacy_environment_endpoints() -> None:  # noqa: C901
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    list_paging_path = "/dolphinscheduler/environment/list-paging"
    query_by_code_path = "/dolphinscheduler/environment/query-by-code"
    query_all_path = "/dolphinscheduler/environment/query-environment-list"
    create_path = "/dolphinscheduler/environment/create"
    update_path = "/dolphinscheduler/environment/update"
    delete_path = "/dolphinscheduler/environment/delete"

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == list_paging_path:
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "20"
            assert request.url.params["searchVal"] == "prod"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {"code": 7, "name": "prod", "description": "prod env"}
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if request.method == "POST" and request.url.path == create_path:
            form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
            assert form == {
                "name": ["qa"],
                "config": ['{"JAVA_HOME":"/opt/java"}'],
                "description": ["qa env"],
                "workerGroups": ['["default","gpu"]'],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": 11},
            )
        if request.method == "GET" and request.url.path == query_by_code_path:
            environment_code = request.url.params["environmentCode"]
            if environment_code == "7":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {"code": 7, "name": "prod", "description": "prod env"},
                    },
                )
            if environment_code == "11":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "code": 11,
                            "name": "qa",
                            "description": "qa env",
                            "config": '{"JAVA_HOME":"/opt/java"}',
                            "workerGroups": ["default", "gpu"],
                        },
                    },
                )
            if environment_code == "9":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "code": 9,
                            "name": "test",
                            "description": None,
                            "config": '{"JAVA_HOME":"/opt/java-21"}',
                            "workerGroups": [],
                        },
                    },
                )
            message = f"Unexpected environmentCode: {environment_code}"
            raise AssertionError(message)
        if request.method == "POST" and request.url.path == update_path:
            form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
            assert form == {
                "code": ["9"],
                "name": ["test"],
                "config": ['{"JAVA_HOME":"/opt/java-21"}'],
                "workerGroups": ["[]"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 9,
                        "name": "test",
                        "description": None,
                    },
                },
            )
        if request.method == "POST" and request.url.path == delete_path:
            form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
            assert form == {"environmentCode": ["9"]}
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if request.method == "GET" and request.url.path == query_all_path:
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {"code": 7, "name": "prod", "description": "prod env"},
                        {"code": 9, "name": "test", "description": "test env"},
                    ],
                },
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.environments.list(page_no=1, page_size=20, search="prod")
        environment = session.environments.get(code=7)
        created = session.environments.create(
            name="qa",
            config='{"JAVA_HOME":"/opt/java"}',
            description="qa env",
            worker_groups=["default", "gpu"],
        )
        updated = session.environments.update(
            code=9,
            name="test",
            config='{"JAVA_HOME":"/opt/java-21"}',
            description=None,
            worker_groups=[],
        )
        deleted = session.environments.delete(code=9)
        environments = session.environments.list_all()

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "prod"
    assert environment.code == 7
    assert created.code == 11
    assert created.workerGroups == ["default", "gpu"]
    assert updated.code == 9
    assert updated.config == '{"JAVA_HOME":"/opt/java-21"}'
    assert updated.workerGroups == []
    assert deleted is True
    assert [item.code for item in environments] == [7, 9]
    assert requests_seen == [
        ("GET", list_paging_path),
        ("GET", query_by_code_path),
        ("POST", create_path),
        ("GET", query_by_code_path),
        ("POST", update_path),
        ("GET", query_by_code_path),
        ("POST", delete_path),
        ("GET", query_all_path),
    ]


def test_adapter_datasource_methods_bridge_generated_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/datasources"
        ):
            assert dict(request.url.params) == {
                "searchVal": "warehouse",
                "pageNo": "1",
                "pageSize": "20",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 7,
                                "name": "warehouse",
                                "note": "main warehouse",
                                "type": "MYSQL",
                                "userId": 1,
                                "userName": "admin",
                                "connectionParams": '{"password":"******"}',
                                "createTime": "2026-04-11 10:00:00",
                                "updateTime": "2026-04-11 10:05:00",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/datasources/7"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 7,
                        "name": "warehouse",
                        "note": "main warehouse",
                        "type": "SSH",
                        "host": "db.example",
                        "port": 22,
                        "database": "warehouse",
                        "userName": "etl",
                        "password": "******",
                        "privateKey": "******",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/datasources"
        ):
            assert request.content.decode("utf-8") == (
                '{"name":"warehouse","type":"MYSQL","password":"secret"}'
            )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 7,
                        "name": "warehouse",
                        "note": "main warehouse",
                        "type": "MYSQL",
                        "userId": 1,
                        "userName": "admin",
                        "connectionParams": '{"password":"******"}',
                    },
                },
            )
        if (
            request.method == "PUT"
            and request.url.path == "/dolphinscheduler/datasources/7"
        ):
            assert request.content.decode("utf-8") == (
                '{"id":7,"name":"warehouse","type":"MYSQL","password":""}'
            )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 7,
                        "name": "warehouse",
                        "note": "main warehouse",
                        "type": "MYSQL",
                        "userId": 1,
                        "userName": "admin",
                        "connectionParams": '{"password":"******"}',
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/datasources/7/connect-test"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/datasources/7"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.datasources.list(page_no=1, page_size=20, search="warehouse")
        detail = session.datasources.get(datasource_id=7)
        created = session.datasources.create(
            payload_json='{"name":"warehouse","type":"MYSQL","password":"secret"}'
        )
        updated = session.datasources.update(
            datasource_id=7,
            payload_json='{"id":7,"name":"warehouse","type":"MYSQL","password":""}',
        )
        tested = session.datasources.connection_test(datasource_id=7)
        deleted = session.datasources.delete(datasource_id=7)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "warehouse"
    assert detail["type"] == "SSH"
    assert detail["privateKey"] == "******"
    assert created.id == 7
    assert updated.id == 7
    assert tested is True
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/datasources"),
        ("GET", "/dolphinscheduler/datasources/7"),
        ("POST", "/dolphinscheduler/datasources"),
        ("PUT", "/dolphinscheduler/datasources/7"),
        ("GET", "/dolphinscheduler/datasources/7/connect-test"),
        ("DELETE", "/dolphinscheduler/datasources/7"),
    ]


def test_adapter_datasource_authorization_methods_bridge_auth_set_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/datasources/authed-datasource"
        ):
            assert dict(request.url.params) == {"userId": "8"}
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 7,
                            "name": "warehouse",
                            "note": "main warehouse",
                            "type": "MYSQL",
                            "userId": 1,
                            "userName": "admin",
                        }
                    ],
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/users/grant-datasource"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "userId": ["8"],
                "datasourceIds": ["7,9"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": {"status": True}},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        authorized = session.datasources.authorized_for_user(user_id=8)
        granted = session.users.grant_datasources(user_id=8, datasource_ids=[7, 9])

    assert [item.id for item in authorized] == [7]
    assert granted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/datasources/authed-datasource"),
        ("POST", "/dolphinscheduler/users/grant-datasource"),
    ]


def test_adapter_namespace_methods_bridge_paging_crud_and_auth_set_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/k8s-namespace"
        ):
            assert dict(request.url.params) == {
                "searchVal": "etl",
                "pageNo": "1",
                "pageSize": "20",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 21,
                                "namespace": "etl-prod",
                                "clusterCode": 9001,
                                "clusterName": "prod-cluster",
                                "userId": 1,
                                "userName": "admin",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/k8s-namespace/authed-namespace"
        ):
            assert dict(request.url.params) == {"userId": "8"}
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 21,
                            "namespace": "etl-prod",
                            "clusterCode": 9001,
                            "clusterName": "prod-cluster",
                            "userId": 1,
                            "userName": "admin",
                        }
                    ],
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/k8s-namespace/available-list"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 22,
                            "code": 7002,
                            "namespace": "etl-staging",
                            "clusterCode": 9002,
                            "clusterName": "staging-cluster",
                            "userId": 1,
                            "userName": "admin",
                        }
                    ],
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/k8s-namespace"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "namespace": ["etl-ops"],
                "clusterCode": ["9003"],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 23,
                        "code": 7003,
                        "namespace": "etl-ops",
                        "clusterCode": 9003,
                        "userId": 1,
                        "userName": "admin",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/k8s-namespace/delete"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {"id": ["23"]}
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/users/grant-namespace"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "userId": ["8"],
                "namespaceIds": ["21,22"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": {"status": True}},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.namespaces.list(page_no=1, page_size=20, search="etl")
        authorized = session.namespaces.authorized_for_user(user_id=8)
        available = session.namespaces.available()
        created = session.namespaces.create(namespace="etl-ops", cluster_code=9003)
        deleted = session.namespaces.delete(namespace_id=23)
        granted = session.users.grant_namespaces(user_id=8, namespace_ids=[21, 22])

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].namespace == "etl-prod"
    assert [item.id for item in authorized] == [21]
    assert [item.id for item in available] == [22]
    assert created.id == 23
    assert created.namespace == "etl-ops"
    assert deleted is True
    assert granted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/k8s-namespace"),
        ("GET", "/dolphinscheduler/k8s-namespace/authed-namespace"),
        ("GET", "/dolphinscheduler/k8s-namespace/available-list"),
        ("POST", "/dolphinscheduler/k8s-namespace"),
        ("POST", "/dolphinscheduler/k8s-namespace/delete"),
        ("POST", "/dolphinscheduler/users/grant-namespace"),
    ]


def test_adapter_queue_methods_bridge_raw_paging_and_generated_crud() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    queues_state: list[dict[str, object]] = [
        {
            "id": 8,
            "queueName": "default",
            "queue": "root.default",
            "createTime": "2026-04-11 10:00:00",
            "updateTime": "2026-04-11 10:05:00",
        },
        {
            "id": 9,
            "queueName": "analytics",
            "queue": "root.analytics",
            "createTime": "2026-04-11 10:10:00",
            "updateTime": "2026-04-11 10:15:00",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/dolphinscheduler/queues":
            assert dict(request.url.params) == {
                "searchVal": "default",
                "pageNo": "1",
                "pageSize": "20",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [queues_state[0]],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/queues/list"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": queues_state},
            )
        if request.method == "POST" and request.url.path == "/dolphinscheduler/queues":
            assert parse_qs(request.content.decode("utf-8")) == {
                "queue": ["root.ops"],
                "queueName": ["ops"],
            }
            queues_state.append(
                {
                    "id": 10,
                    "queueName": "ops",
                    "queue": "root.ops",
                    "createTime": "2026-04-11 10:20:00",
                    "updateTime": "2026-04-11 10:20:00",
                }
            )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {"queueName": "ops", "queue": "root.ops"},
                },
            )
        if request.method == "PUT" and request.url.path == "/dolphinscheduler/queues/8":
            assert parse_qs(request.content.decode("utf-8")) == {
                "queue": ["root.etl"],
                "queueName": ["etl"],
            }
            queues_state[0] = {
                **queues_state[0],
                "queueName": "etl",
                "queue": "root.etl",
                "updateTime": "2026-04-11 10:30:00",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {"id": 8, "queueName": "etl", "queue": "root.etl"},
                },
            )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/queues/8"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.queues.list(page_no=1, page_size=20, search="default")
        fetched = session.queues.get(queue_id=8)
        created = session.queues.create(queue="root.ops", queue_name="ops")
        updated = session.queues.update(
            queue_id=8,
            queue="root.etl",
            queue_name="etl",
        )
        deleted = session.queues.delete(queue_id=8)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].queueName == "default"
    assert fetched.id == 8
    assert fetched.queueName == "default"
    assert created.id == 10
    assert created.queue == "root.ops"
    assert updated.id == 8
    assert updated.queueName == "etl"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/queues"),
        ("GET", "/dolphinscheduler/queues/list"),
        ("POST", "/dolphinscheduler/queues"),
        ("GET", "/dolphinscheduler/queues/list"),
        ("PUT", "/dolphinscheduler/queues/8"),
        ("GET", "/dolphinscheduler/queues/list"),
        ("DELETE", "/dolphinscheduler/queues/8"),
    ]


def test_adapter_worker_group_methods_bridge_mixed_paging_and_crud() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    config_group = {
        "id": None,
        "name": "config-default",
        "addrList": "worker-a:1234",
        "createTime": "2026-04-11 10:00:00",
        "updateTime": "2026-04-11 10:05:00",
        "description": None,
        "systemDefault": True,
    }
    ui_default = {
        "id": 8,
        "name": "default",
        "addrList": "worker-a:1234",
        "createTime": "2026-04-11 10:10:00",
        "updateTime": "2026-04-11 10:15:00",
        "description": "primary group",
        "systemDefault": False,
    }
    ui_analytics = {
        "id": 9,
        "name": "analytics",
        "addrList": "worker-b:1234",
        "createTime": "2026-04-11 10:20:00",
        "updateTime": "2026-04-11 10:25:00",
        "description": None,
        "systemDefault": False,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/worker-groups"
        ):
            if dict(request.url.params) == {
                "searchVal": "default",
                "pageNo": "1",
                "pageSize": "20",
            }:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": [config_group, ui_default],
                            "total": 1,
                            "totalPage": 1,
                            "pageSize": 20,
                            "currentPage": 1,
                        },
                    },
                )
            if dict(request.url.params) == {"pageNo": "1", "pageSize": "100"}:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": [config_group, ui_default],
                            "total": 101,
                            "pageSize": 100,
                            "currentPage": 1,
                        },
                    },
                )
            if dict(request.url.params) == {"pageNo": "2", "pageSize": "100"}:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": [config_group, ui_analytics],
                            "total": 101,
                            "pageSize": 100,
                            "currentPage": 2,
                        },
                    },
                )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/worker-groups"
        ):
            form = parse_qs(request.content.decode("utf-8"))
            if form == {
                "name": ["ops"],
                "addrList": ["worker-c:1234"],
                "description": ["ops group"],
            }:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "id": 10,
                            "name": "ops",
                            "addrList": "worker-c:1234",
                            "description": "ops group",
                            "systemDefault": False,
                        },
                    },
                )
            if form == {
                "id": ["8"],
                "name": ["etl"],
                "addrList": ["worker-d:1234"],
                "description": ["updated group"],
            }:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "id": 8,
                            "name": "etl",
                            "addrList": "worker-d:1234",
                            "description": "updated group",
                            "systemDefault": False,
                        },
                    },
                )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/worker-groups/8"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.worker_groups.list(page_no=1, page_size=20, search="default")
        worker_groups = session.worker_groups.list_all()
        fetched = session.worker_groups.get(worker_group_id=8)
        created = session.worker_groups.create(
            name="ops",
            addr_list="worker-c:1234",
            description="ops group",
        )
        updated = session.worker_groups.update(
            worker_group_id=8,
            name="etl",
            addr_list="worker-d:1234",
            description="updated group",
        )
        deleted = session.worker_groups.delete(worker_group_id=8)

    assert page.total == 1
    assert page.totalList is not None
    assert [item.name for item in page.totalList] == ["config-default", "default"]
    assert [item.addrList for item in worker_groups] == [
        "worker-a:1234",
        "worker-a:1234",
        "worker-b:1234",
    ]
    assert [item.systemDefault for item in worker_groups] == [True, False, False]
    assert fetched.id == 8
    assert fetched.name == "default"
    assert created.id == 10
    assert created.addrList == "worker-c:1234"
    assert updated.id == 8
    assert updated.name == "etl"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/worker-groups"),
        ("GET", "/dolphinscheduler/worker-groups"),
        ("GET", "/dolphinscheduler/worker-groups"),
        ("GET", "/dolphinscheduler/worker-groups"),
        ("GET", "/dolphinscheduler/worker-groups"),
        ("POST", "/dolphinscheduler/worker-groups"),
        ("POST", "/dolphinscheduler/worker-groups"),
        ("DELETE", "/dolphinscheduler/worker-groups/8"),
    ]


def test_adapter_tenant_methods_bridge_paging_and_crud() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    tenants_state: list[dict[str, object]] = [
        {
            "id": 8,
            "tenantCode": "tenant-prod",
            "description": "production tenant",
            "queueId": 11,
            "queueName": "default",
            "queue": "root.default",
            "createTime": "2026-04-11 10:00:00",
            "updateTime": "2026-04-11 10:05:00",
        },
        {
            "id": 9,
            "tenantCode": "tenant-analytics",
            "description": "analytics tenant",
            "queueId": 12,
            "queueName": "analytics",
            "queue": "root.analytics",
            "createTime": "2026-04-11 10:10:00",
            "updateTime": "2026-04-11 10:15:00",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/dolphinscheduler/tenants":
            params = dict(request.url.params)
            if params == {
                "searchVal": "tenant-prod",
                "pageNo": "1",
                "pageSize": "20",
            }:
                items = [tenants_state[0]]
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": items,
                            "total": len(items),
                            "totalPage": 1,
                            "pageSize": 20,
                            "currentPage": 1,
                        },
                    },
                )
            if params == {"pageNo": "1", "pageSize": "100"}:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": tenants_state,
                            "total": len(tenants_state),
                            "totalPage": 1,
                            "pageSize": 100,
                            "currentPage": 1,
                        },
                    },
                )
        if request.method == "POST" and request.url.path == "/dolphinscheduler/tenants":
            assert parse_qs(request.content.decode("utf-8")) == {
                "tenantCode": ["tenant-ops"],
                "queueId": ["11"],
                "description": ["ops tenant"],
            }
            created = {
                "id": 10,
                "tenantCode": "tenant-ops",
                "description": "ops tenant",
                "queueId": 11,
                "queueName": "default",
                "queue": "root.default",
                "createTime": "2026-04-11 10:20:00",
                "updateTime": "2026-04-11 10:20:00",
            }
            tenants_state.append(created)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 10,
                        "tenantCode": "tenant-ops",
                        "description": "ops tenant",
                        "queueId": 11,
                    },
                },
            )
        if (
            request.method == "PUT"
            and request.url.path == "/dolphinscheduler/tenants/8"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "tenantCode": ["tenant-prod"],
                "queueId": ["12"],
                "description": ["production tenant"],
            }
            tenants_state[0] = {
                **tenants_state[0],
                "queueId": 12,
                "queueName": "analytics",
                "queue": "root.analytics",
                "updateTime": "2026-04-11 10:30:00",
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/tenants/8"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.tenants.list(page_no=1, page_size=20, search="tenant-prod")
        fetched = session.tenants.get(tenant_id=8)
        created = session.tenants.create(
            tenant_code="tenant-ops",
            queue_id=11,
            description="ops tenant",
        )
        updated = session.tenants.update(
            tenant_id=8,
            tenant_code="tenant-prod",
            queue_id=12,
            description="production tenant",
        )
        deleted = session.tenants.delete(tenant_id=8)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].tenantCode == "tenant-prod"
    assert fetched.id == 8
    assert fetched.queueName == "default"
    assert created.id == 10
    assert created.queueName == "default"
    assert updated.id == 8
    assert updated.queueName == "analytics"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/tenants"),
        ("GET", "/dolphinscheduler/tenants"),
        ("POST", "/dolphinscheduler/tenants"),
        ("GET", "/dolphinscheduler/tenants"),
        ("PUT", "/dolphinscheduler/tenants/8"),
        ("GET", "/dolphinscheduler/tenants"),
        ("DELETE", "/dolphinscheduler/tenants/8"),
    ]


def test_adapter_monitor_server_uses_monitor_endpoint() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/monitor/MASTER"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 1,
                            "host": "master-1",
                            "port": 5678,
                            "serverDirectory": "/opt/ds/master",
                            "heartBeatInfo": "healthy",
                            "createTime": "2026-04-11 10:00:00",
                            "lastHeartbeatTime": "2026-04-11 10:05:00",
                        }
                    ],
                },
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        servers = session.monitor.list_servers(node_type="MASTER")

    assert len(servers) == 1
    assert servers[0].host == "master-1"
    assert requests_seen == [("GET", "/dolphinscheduler/monitor/MASTER")]


def test_adapter_monitor_database_uses_monitor_databases_endpoint() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/monitor/databases"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "dbType": "MYSQL",
                            "state": "YES",
                            "maxConnections": 50,
                            "maxUsedConnections": 10,
                            "threadsConnections": 4,
                            "threadsRunningConnections": 1,
                            "date": "2026-04-11 10:10:00",
                        }
                    ],
                },
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        databases = session.monitor.list_databases()

    assert len(databases) == 1
    assert databases[0].dbType is not None
    assert databases[0].dbType.value == "MYSQL"
    assert requests_seen == [("GET", "/dolphinscheduler/monitor/databases")]


def test_adapter_workflow_online_and_offline_bridge_via_release_endpoint() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-definition/101/release"
        ):
            form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
            release_state = form["releaseState"][0]
            assert release_state in {"ONLINE", "OFFLINE"}
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.workflows.online(project_code=7, workflow_code=101)
        session.workflows.offline(project_code=7, workflow_code=101)

    assert requests_seen == [
        ("POST", "/dolphinscheduler/projects/7/workflow-definition/101/release"),
        ("POST", "/dolphinscheduler/projects/7/workflow-definition/101/release"),
    ]


def test_adapter_workflow_delete_bridges_delete_endpoint() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "DELETE"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-definition/101"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.workflows.delete(project_code=7, workflow_code=101)

    assert requests_seen == [
        ("DELETE", "/dolphinscheduler/projects/7/workflow-definition/101")
    ]


def test_adapter_workflow_create_bridges_legacy_form_endpoint() -> None:
    profile = make_profile()
    global_params = json.dumps(
        [
            {
                "prop": "env",
                "value": "prod",
                "direct": "IN",
                "type": "VARCHAR",
            }
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    task_relations = json.dumps(
        [
            {
                "name": "",
                "preTaskCode": 0,
                "preTaskVersion": 0,
                "postTaskCode": 7001,
                "postTaskVersion": 1,
                "conditionType": 0,
                "conditionParams": "{}",
            }
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    task_definitions = json.dumps(
        [
            {
                "code": 7001,
                "version": 1,
                "name": "extract",
                "description": "",
                "taskType": "SHELL",
                "taskParams": json.dumps(
                    {
                        "rawScript": "echo extract",
                        "localParams": [],
                        "resourceList": [],
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "flag": "YES",
                "taskPriority": "MEDIUM",
                "workerGroup": "default",
                "environmentCode": -1,
                "failRetryTimes": 0,
                "failRetryInterval": 0,
                "timeoutFlag": "CLOSE",
                "timeoutNotifyStrategy": "",
                "timeout": 45,
                "delayTime": 0,
                "resourceIds": "",
                "taskExecuteType": "BATCH",
            }
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/projects/7/workflow-definition"
        form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
        assert form == {
            "name": ["nightly-sync"],
            "description": ["Nightly workflow"],
            "globalParams": [global_params],
            "locations": ['[{"taskCode":7001,"x":80,"y":80}]'],
            "timeout": ["45"],
            "taskRelationJson": [task_relations],
            "taskDefinitionJson": [task_definitions],
            "executionType": ["PARALLEL"],
        }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": 1},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.workflows.create(
            project_code=7,
            name="nightly-sync",
            description="Nightly workflow",
            global_params=global_params,
            locations='[{"taskCode":7001,"x":80,"y":80}]',
            timeout=45,
            task_relation_json=task_relations,
            task_definition_json=task_definitions,
            execution_type="PARALLEL",
        )


def test_adapter_workflow_update_bridges_legacy_form_endpoint() -> None:
    profile = make_profile()
    task_relations = json.dumps(
        [
            {
                "name": "",
                "preTaskCode": 0,
                "preTaskVersion": 0,
                "postTaskCode": 7001,
                "postTaskVersion": 1,
                "conditionType": 0,
                "conditionParams": "{}",
            }
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    task_definitions = json.dumps(
        [
            {
                "code": 7001,
                "version": 1,
                "name": "extract-v2",
                "description": "",
                "taskType": "SHELL",
                "taskParams": json.dumps(
                    {
                        "rawScript": "echo extract v2",
                        "localParams": [],
                        "resourceList": [],
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                "flag": "YES",
                "taskPriority": "MEDIUM",
                "workerGroup": "default",
                "environmentCode": -1,
                "failRetryTimes": 0,
                "failRetryInterval": 0,
                "timeoutFlag": "CLOSE",
                "timeoutNotifyStrategy": "",
                "timeout": 45,
                "delayTime": 0,
                "resourceIds": "",
                "taskExecuteType": "BATCH",
            }
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert (
            request.url.path == "/dolphinscheduler/projects/7/workflow-definition/101"
        )
        form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
        assert form == {
            "name": ["nightly-sync-v2"],
            "description": ["Nightly workflow v2"],
            "globalParams": ["[]"],
            "locations": ['[{"taskCode":7001,"x":80,"y":80}]'],
            "timeout": ["45"],
            "taskRelationJson": [task_relations],
            "taskDefinitionJson": [task_definitions],
            "executionType": ["PARALLEL"],
            "releaseState": ["ONLINE"],
        }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": 1},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.workflows.update(
            project_code=7,
            workflow_code=101,
            name="nightly-sync-v2",
            description="Nightly workflow v2",
            global_params="[]",
            locations='[{"taskCode":7001,"x":80,"y":80}]',
            timeout=45,
            task_relation_json=task_relations,
            task_definition_json=task_definitions,
            execution_type="PARALLEL",
            release_state="ONLINE",
        )


def test_adapter_task_update_bridges_legacy_with_upstream_endpoint() -> None:
    profile = make_profile()
    task_definition_json = json.dumps(
        {
            "name": "load",
            "description": "updated",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {"rawScript": "echo load v2"},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "YES",
            "taskPriority": "HIGH",
            "workerGroup": "default",
            "environmentCode": 0,
            "failRetryTimes": 3,
            "failRetryInterval": 0,
            "timeoutFlag": "CLOSE",
            "timeout": 0,
            "delayTime": 0,
            "taskGroupId": 0,
            "taskGroupPriority": 0,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert (
            request.url.path
            == "/dolphinscheduler/projects/7/task-definition/202/with-upstream"
        )
        form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
        assert form == {
            "taskDefinitionJsonObj": [task_definition_json],
            "upstreamCodes": ["201"],
        }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": 202},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.tasks.update(
            project_code=7,
            code=202,
            task_definition_json=task_definition_json,
            upstream_codes=[201],
        )


def test_adapter_user_methods_bridge_paging_and_raw_user_views() -> None:  # noqa: C901
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    create_password = "super" + "secret"
    update_password = "new" + "secret"
    tenants = {
        11: {"tenantCode": "tenant-prod", "queueName": "default"},
        12: {"tenantCode": "tenant-analytics", "queueName": "analytics"},
    }
    raw_users_state: list[dict[str, object]] = [
        {
            "id": 8,
            "userName": "alice",
            "email": "alice@example.com",
            "phone": "13800138000",
            "userType": "GENERAL_USER",
            "tenantId": 11,
            "queue": "",
            "state": 1,
            "timeZone": "Asia/Shanghai",
            "createTime": "2026-04-11 11:00:00",
            "updateTime": "2026-04-11 11:05:00",
        },
        {
            "id": 9,
            "userName": "bob",
            "email": "bob@example.com",
            "phone": None,
            "userType": "GENERAL_USER",
            "tenantId": 11,
            "queue": "analytics",
            "state": 0,
            "timeZone": None,
            "createTime": "2026-04-11 11:10:00",
            "updateTime": "2026-04-11 11:15:00",
        },
    ]

    def user_summary(raw_user: dict[str, object]) -> dict[str, object]:
        tenant_id = raw_user["tenantId"]
        assert isinstance(tenant_id, int)
        tenant = tenants[tenant_id]
        stored_queue = raw_user["queue"]
        effective_queue = tenant["queueName"] if stored_queue == "" else stored_queue
        return {
            "id": raw_user["id"],
            "userName": raw_user["userName"],
            "email": raw_user["email"],
            "phone": raw_user["phone"],
            "userType": raw_user["userType"],
            "tenantId": raw_user["tenantId"],
            "tenantCode": tenant["tenantCode"],
            "queueName": tenant["queueName"],
            "queue": effective_queue,
            "state": raw_user["state"],
            "createTime": raw_user["createTime"],
            "updateTime": raw_user["updateTime"],
        }

    def list_response(request: httpx.Request) -> httpx.Response | None:
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/users/get-user-info"
        ):
            current_user = dict(raw_users_state[0])
            current_tenant = tenants[11]
            current_user["tenantCode"] = current_tenant["tenantCode"]
            current_user["queueName"] = current_tenant["queueName"]
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": current_user},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/users/list"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": raw_users_state},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/users/list-all"
        ):
            enabled_users = [user for user in raw_users_state if user["state"] == 1]
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": enabled_users},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/users/list-paging"
        ):
            params = dict(request.url.params)
            search_value = params.get("searchVal")
            page_no = int(params["pageNo"])
            page_size = int(params["pageSize"])
            filtered = list(raw_users_state)
            if search_value is not None:
                filtered = [
                    user
                    for user in filtered
                    if search_value.lower() in str(user["userName"]).lower()
                ]
            items = [user_summary(user) for user in filtered]
            start = (page_no - 1) * page_size
            stop = start + page_size
            total = len(items)
            total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": items[start:stop],
                        "total": total,
                        "totalPage": total_pages,
                        "pageSize": page_size,
                        "currentPage": page_no,
                    },
                },
            )
        return None

    def create_response(request: httpx.Request) -> httpx.Response | None:
        if (
            request.method != "POST"
            or request.url.path != "/dolphinscheduler/users/create"
        ):
            return None
        assert parse_qs(request.content.decode("utf-8")) == {
            "userName": ["carol"],
            "userPassword": [create_password],
            "email": ["carol@example.com"],
            "tenantId": ["12"],
            "queue": ["analytics"],
            "state": ["1"],
        }
        created = {
            "id": 10,
            "userName": "carol",
            "email": "carol@example.com",
            "phone": None,
            "userType": "GENERAL_USER",
            "tenantId": 12,
            "queue": "analytics",
            "state": 1,
            "timeZone": None,
            "createTime": "2026-04-11 11:20:00",
            "updateTime": "2026-04-11 11:20:00",
        }
        raw_users_state.append(created)
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": created},
        )

    def update_response(request: httpx.Request) -> httpx.Response | None:
        if (
            request.method != "POST"
            or request.url.path != "/dolphinscheduler/users/update"
        ):
            return None
        assert parse_qs(request.content.decode("utf-8")) == {
            "id": ["8"],
            "userName": ["alice"],
            "userPassword": [update_password],
            "email": ["alice+ops@example.com"],
            "tenantId": ["11"],
            "phone": ["13900139000"],
            "queue": ["analytics"],
            "state": ["1"],
            "timeZone": ["UTC"],
        }
        for user in raw_users_state:
            if user["id"] != 8:
                continue
            user.update(
                {
                    "email": "alice+ops@example.com",
                    "phone": "13900139000",
                    "queue": "analytics",
                    "timeZone": "UTC",
                    "updateTime": "2026-04-11 11:25:00",
                }
            )
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": user},
            )
        message = "expected alice to exist before update"
        raise AssertionError(message)

    def delete_response(request: httpx.Request) -> httpx.Response | None:
        if (
            request.method != "POST"
            or request.url.path != "/dolphinscheduler/users/delete"
        ):
            return None
        assert parse_qs(request.content.decode("utf-8")) == {"id": ["8"]}
        raw_users_state[:] = [user for user in raw_users_state if user["id"] != 8]
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {}},
        )

    def project_permission_response(request: httpx.Request) -> httpx.Response | None:
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/users/grant-project-by-code"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "userId": ["8"],
                "projectCode": ["701"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": {"status": True}},
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/users/revoke-project"
        ):
            assert parse_qs(request.content.decode("utf-8")) == {
                "userId": ["8"],
                "projectCode": ["701"],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": {"status": True}},
            )
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        for responder in (
            list_response,
            create_response,
            update_response,
            delete_response,
            project_permission_response,
        ):
            response = responder(request)
            if response is not None:
                return response
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.users.list(page_no=1, page_size=20, search="alice")
        users = session.users.list_all()
        current = session.users.current()
        fetched = session.users.get(user_id=8)
        created = session.users.create(
            user_name="carol",
            password=create_password,
            email="carol@example.com",
            tenant_id=12,
            queue="analytics",
            state=1,
        )
        updated = session.users.update(
            user_id=8,
            user_name="alice",
            password=update_password,
            email="alice+ops@example.com",
            tenant_id=11,
            phone="13900139000",
            queue="analytics",
            state=1,
            time_zone="UTC",
        )
        deleted = session.users.delete(user_id=8)
        granted = session.users.grant_project_by_code(user_id=8, project_code=701)
        revoked = session.users.revoke_project(user_id=8, project_code=701)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].userName == "alice"
    assert [user.userName for user in users] == ["alice", "bob"]
    assert [user.storedQueue for user in users] == ["", "analytics"]
    assert current.userName == "alice"
    assert current.tenantCode == "tenant-prod"
    assert current.queueName == "default"
    assert current.timeZone == "Asia/Shanghai"
    assert fetched.userName == "alice"
    assert fetched.queue == "default"
    assert fetched.queueName == "default"
    assert fetched.timeZone == "Asia/Shanghai"
    assert created.id == 10
    assert created.tenantCode == "tenant-analytics"
    assert created.queue == "analytics"
    assert updated.email == "alice+ops@example.com"
    assert updated.queue == "analytics"
    assert updated.timeZone == "UTC"
    assert deleted is True
    assert granted is True
    assert revoked is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/users/list-paging"),
        ("GET", "/dolphinscheduler/users/list"),
        ("GET", "/dolphinscheduler/users/list-all"),
        ("GET", "/dolphinscheduler/users/list-paging"),
        ("GET", "/dolphinscheduler/users/get-user-info"),
        ("GET", "/dolphinscheduler/users/list"),
        ("GET", "/dolphinscheduler/users/list-all"),
        ("GET", "/dolphinscheduler/users/list-paging"),
        ("POST", "/dolphinscheduler/users/create"),
        ("GET", "/dolphinscheduler/users/list"),
        ("GET", "/dolphinscheduler/users/list-all"),
        ("GET", "/dolphinscheduler/users/list-paging"),
        ("POST", "/dolphinscheduler/users/update"),
        ("GET", "/dolphinscheduler/users/list"),
        ("GET", "/dolphinscheduler/users/list-all"),
        ("GET", "/dolphinscheduler/users/list-paging"),
        ("POST", "/dolphinscheduler/users/delete"),
        ("POST", "/dolphinscheduler/users/grant-project-by-code"),
        ("POST", "/dolphinscheduler/users/revoke-project"),
    ]


def test_adapter_schedule_methods_mix_v1_paging_with_v2_crud() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/projects/7/schedules"
        ):
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "20"
            assert request.url.params["workflowDefinitionCode"] == "101"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 1,
                                "workflowDefinitionCode": 101,
                                "workflowDefinitionName": "daily-sync",
                                "projectName": "etl-prod",
                                "userId": 11,
                                "warningGroupId": 0,
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/projects/7/schedules/preview"
        ):
            form = parse_qs(request.content.decode("utf-8"), strict_parsing=True)
            assert json.loads(form["schedule"][0]) == {
                "crontab": "0 0 2 * * ?",
                "startTime": "2024-01-01 00:00:00",
                "endTime": "2025-01-01 00:00:00",
                "timezoneId": "Asia/Shanghai",
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        "2024-01-01 02:00:00",
                        "2024-01-02 02:00:00",
                        "2024-01-03 02:00:00",
                        "2024-01-04 02:00:00",
                        "2024-01-05 02:00:00",
                    ],
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/schedules/1"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 1,
                        "workflowDefinitionCode": 101,
                        "workflowDefinitionName": "daily-sync",
                        "projectName": "etl-prod",
                        "crontab": "0 0 2 * * ?",
                        "timezoneId": "Asia/Shanghai",
                        "userId": 11,
                        "warningGroupId": 0,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/v2/schedules"
        ):
            assert json.loads(request.content) == {
                "workflowDefinitionCode": 101,
                "crontab": "0 0 2 * * ?",
                "startTime": "2024-01-01 00:00:00",
                "endTime": "2025-01-01 00:00:00",
                "timezoneId": "Asia/Shanghai",
                "warningGroupId": 0,
                "environmentCode": 0,
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 2,
                        "workflowDefinitionCode": 101,
                        "workflowDefinitionName": "daily-sync",
                        "projectName": "etl-prod",
                        "crontab": "0 0 2 * * ?",
                        "timezoneId": "Asia/Shanghai",
                        "userId": 11,
                        "warningGroupId": 0,
                    },
                },
            )
        if (
            request.method == "PUT"
            and request.url.path == "/dolphinscheduler/v2/schedules/1"
        ):
            assert json.loads(request.content) == {
                "crontab": "0 0 6 * * ?",
                "startTime": "2024-01-01 00:00:00",
                "endTime": "2025-01-01 00:00:00",
                "timezoneId": "Asia/Shanghai",
                "warningGroupId": 0,
                "environmentCode": 0,
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 1,
                        "workflowDefinitionCode": 101,
                        "workflowDefinitionName": "daily-sync",
                        "projectName": "etl-prod",
                        "crontab": "0 0 6 * * ?",
                        "timezoneId": "Asia/Shanghai",
                        "userId": 11,
                        "warningGroupId": 0,
                    },
                },
            )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/v2/schedules/1"
        ):
            return httpx.Response(200, json={"code": 0, "msg": "success", "data": {}})
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.schedules.list(
            project_code=7,
            workflow_code=101,
            page_no=1,
            page_size=20,
        )
        preview = session.schedules.preview(
            project_code=7,
            crontab="0 0 2 * * ?",
            start_time="2024-01-01 00:00:00",
            end_time="2025-01-01 00:00:00",
            timezone_id="Asia/Shanghai",
        )
        fetched = session.schedules.get(schedule_id=1)
        created = session.schedules.create(
            workflow_code=101,
            crontab="0 0 2 * * ?",
            start_time="2024-01-01 00:00:00",
            end_time="2025-01-01 00:00:00",
            timezone_id="Asia/Shanghai",
        )
        updated = session.schedules.update(
            schedule_id=1,
            crontab="0 0 6 * * ?",
            start_time="2024-01-01 00:00:00",
            end_time="2025-01-01 00:00:00",
            timezone_id="Asia/Shanghai",
        )
        deleted = session.schedules.delete(schedule_id=1)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].id == 1
    assert preview == [
        "2024-01-01 02:00:00",
        "2024-01-02 02:00:00",
        "2024-01-03 02:00:00",
        "2024-01-04 02:00:00",
        "2024-01-05 02:00:00",
    ]
    assert fetched.id == 1
    assert created.id == 2
    assert updated.crontab == "0 0 6 * * ?"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/schedules"),
        ("POST", "/dolphinscheduler/projects/7/schedules/preview"),
        ("GET", "/dolphinscheduler/v2/schedules/1"),
        ("POST", "/dolphinscheduler/v2/schedules"),
        ("PUT", "/dolphinscheduler/v2/schedules/1"),
        ("DELETE", "/dolphinscheduler/v2/schedules/1"),
    ]


def test_adapter_schedule_online_and_offline_bridge_via_workflow_project_code() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    online_calls = 0
    offline_calls = 0

    def schedule_payload(release_state: str) -> dict[str, object]:
        return {
            "id": 1,
            "workflowDefinitionCode": 101,
            "workflowDefinitionName": "daily-sync",
            "projectName": "etl-prod",
            "crontab": "0 0 2 * * ?",
            "timezoneId": "Asia/Shanghai",
            "userId": 11,
            "warningGroupId": 0,
            "releaseState": release_state,
        }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal online_calls, offline_calls
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/schedules/1"
        ):
            release_state = "OFFLINE"
            if online_calls > 0 and offline_calls == 0:
                release_state = "ONLINE"
            if offline_calls > 0:
                release_state = "OFFLINE"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": schedule_payload(release_state),
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/workflows/101"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 101,
                        "projectCode": 7,
                        "userId": 11,
                        "timeout": 0,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/projects/7/schedules/1/online"
        ):
            online_calls += 1
            return httpx.Response(200, json={"code": 0, "msg": "success", "data": True})
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/projects/7/schedules/1/offline"
        ):
            offline_calls += 1
            return httpx.Response(200, json={"code": 0, "msg": "success", "data": True})
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        online_schedule = session.schedules.online(schedule_id=1)
        offline_schedule = session.schedules.offline(schedule_id=1)

    assert online_schedule.releaseState is not None
    assert online_schedule.releaseState.value == "ONLINE"
    assert offline_schedule.releaseState is not None
    assert offline_schedule.releaseState.value == "OFFLINE"
    assert online_calls == 1
    assert offline_calls == 1
    assert requests_seen == [
        ("GET", "/dolphinscheduler/v2/schedules/1"),
        ("GET", "/dolphinscheduler/v2/workflows/101"),
        ("POST", "/dolphinscheduler/projects/7/schedules/1/online"),
        ("GET", "/dolphinscheduler/v2/schedules/1"),
        ("GET", "/dolphinscheduler/v2/schedules/1"),
        ("GET", "/dolphinscheduler/v2/workflows/101"),
        ("POST", "/dolphinscheduler/projects/7/schedules/1/offline"),
        ("GET", "/dolphinscheduler/v2/schedules/1"),
    ]


def test_adapter_runtime_methods_bridge_executor_and_v2_instance_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/executors/start-workflow-instance"
        ):
            payload = parse_qs(request.content.decode())
            assert payload["workflowDefinitionCode"] == ["101"]
            assert payload["failureStrategy"] == ["CONTINUE"]
            assert payload["warningType"] == ["NONE"]
            assert payload["workerGroup"] == ["analytics"]
            assert payload["tenantCode"] == ["tenant-prod"]
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": [901]},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/workflow-instances"
        ):
            assert json.loads(request.content) == {
                "pageNo": 1,
                "pageSize": 20,
                "projectName": "etl-prod",
                "state": 1,
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "id": 901,
                                "workflowDefinitionCode": 101,
                                "projectCode": 7,
                                "state": "RUNNING_EXECUTION",
                                "runTimes": 1,
                                "name": "daily-sync-901",
                                "executorId": 11,
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/workflow-instances/901"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 901,
                        "workflowDefinitionCode": 101,
                        "projectCode": 7,
                        "state": "RUNNING_EXECUTION",
                        "runTimes": 1,
                        "name": "daily-sync-901",
                        "executorId": 11,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/v2/workflow-instances/901/execute/STOP"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": None,
                },
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/v2/workflow-instances/901/execute/REPEAT_RUNNING"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if request.method == "POST" and request.url.path == (
            "/dolphinscheduler/v2/workflow-instances/901/execute/"
            "START_FAILURE_TASK_PROCESS"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/executors/execute-task"
        ):
            body = request.content.decode("utf-8")
            assert "workflowInstanceId=901" in body
            assert "startNodeList=201" in body
            assert "taskDependType=TASK_PRE" in body
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        workflow_instance_ids = session.workflows.run(
            project_code=7,
            workflow_code=101,
            worker_group="analytics",
            tenant_code="tenant-prod",
        )
        page = session.workflow_instances.list(
            page_no=1,
            page_size=20,
            project_name="etl-prod",
            state="RUNNING_EXECUTION",
        )
        instance = session.workflow_instances.get(workflow_instance_id=901)
        session.workflow_instances.stop(workflow_instance_id=901)
        session.workflow_instances.rerun(workflow_instance_id=901)
        session.workflow_instances.recover_failed(workflow_instance_id=901)
        session.workflow_instances.execute_task(
            project_code=7,
            workflow_instance_id=901,
            task_code=201,
            scope="pre",
        )

    assert list(workflow_instance_ids) == [901]
    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "daily-sync-901"
    assert instance.projectCode == 7
    assert requests_seen == [
        ("POST", "/dolphinscheduler/projects/7/executors/start-workflow-instance"),
        ("GET", "/dolphinscheduler/v2/workflow-instances"),
        ("GET", "/dolphinscheduler/v2/workflow-instances/901"),
        ("POST", "/dolphinscheduler/v2/workflow-instances/901/execute/STOP"),
        ("POST", "/dolphinscheduler/v2/workflow-instances/901/execute/REPEAT_RUNNING"),
        (
            "POST",
            "/dolphinscheduler/v2/workflow-instances/901/execute/START_FAILURE_TASK_PROCESS",
        ),
        ("POST", "/dolphinscheduler/projects/7/executors/execute-task"),
    ]


def test_adapter_workflow_instance_relation_methods_use_project_scoped_endpoints() -> (
    None
):
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-instances/query-sub-by-parent"
        ):
            assert request.url.params["taskId"] == "3003"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "dataList": {"subWorkflowInstanceId": 903},
                },
            )
        if (
            request.method == "GET"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-instances/query-parent-by-sub"
        ):
            assert request.url.params["subId"] == "903"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "dataList": {"parentWorkflowInstance": 901},
                },
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        sub_workflow = session.workflow_instances.sub_workflow_instance_by_task(
            project_code=7,
            task_instance_id=3003,
        )
        parent_workflow = session.workflow_instances.parent_instance_by_sub_workflow(
            project_code=7,
            sub_workflow_instance_id=903,
        )

    assert sub_workflow.subWorkflowInstanceId == 903
    assert parent_workflow.parentWorkflowInstance == 901
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/workflow-instances/query-sub-by-parent"),
        ("GET", "/dolphinscheduler/projects/7/workflow-instances/query-parent-by-sub"),
    ]


def test_adapter_workflow_run_can_start_from_task_scope() -> None:
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert (
            request.url.path
            == "/dolphinscheduler/projects/7/executors/start-workflow-instance"
        )
        form = parse_qs(request.content.decode("utf-8"))
        assert form["workflowDefinitionCode"] == ["101"]
        assert form["startNodeList"] == ["201"]
        assert form["taskDependType"] == ["TASK_ONLY"]
        assert form["workerGroup"] == ["analytics"]
        assert form["tenantCode"] == ["tenant-prod"]
        assert form["failureStrategy"] == ["END"]
        assert form["warningType"] == ["ALL"]
        assert form["warningGroupId"] == ["18"]
        assert form["workflowInstancePriority"] == ["HIGH"]
        assert form["environmentCode"] == ["33"]
        assert form["startParams"] == ['{"bizdate":"20260415"}']
        assert form["dryRun"] == ["1"]
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": [902]},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        workflow_instance_ids = session.workflows.run(
            project_code=7,
            workflow_code=101,
            worker_group="analytics",
            tenant_code="tenant-prod",
            start_node_list=[201],
            task_scope="self",
            failure_strategy="END",
            warning_type="ALL",
            warning_group_id=18,
            workflow_instance_priority="HIGH",
            environment_code=33,
            start_params='{"bizdate":"20260415"}',
            dry_run=True,
        )

    assert list(workflow_instance_ids) == [902]


def test_adapter_workflow_backfill_can_start_from_task_scope() -> None:
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert (
            request.url.path
            == "/dolphinscheduler/projects/7/executors/start-workflow-instance"
        )
        form = parse_qs(request.content.decode("utf-8"))
        assert form["workflowDefinitionCode"] == ["101"]
        assert form["scheduleTime"] == [
            '{"complementStartDate":"2026-04-01 00:00:00",'
            '"complementEndDate":"2026-04-02 00:00:00"}'
        ]
        assert form["execType"] == ["COMPLEMENT_DATA"]
        assert form["startNodeList"] == ["201"]
        assert form["taskDependType"] == ["TASK_POST"]
        assert form["runMode"] == ["RUN_MODE_PARALLEL"]
        assert form["expectedParallelismNumber"] == ["4"]
        assert form["complementDependentMode"] == ["ALL_DEPENDENT"]
        assert form["allLevelDependent"] == ["true"]
        assert form["executionOrder"] == ["ASC_ORDER"]
        assert form["dryRun"] == ["1"]
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": [903]},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        workflow_instance_ids = session.workflows.backfill(
            project_code=7,
            workflow_code=101,
            schedule_time=(
                '{"complementStartDate":"2026-04-01 00:00:00",'
                '"complementEndDate":"2026-04-02 00:00:00"}'
            ),
            run_mode="RUN_MODE_PARALLEL",
            expected_parallelism_number=4,
            complement_dependent_mode="ALL_DEPENDENT",
            all_level_dependent=True,
            execution_order="ASC_ORDER",
            worker_group="analytics",
            tenant_code="tenant-prod",
            start_node_list=[201],
            task_scope="post",
            dry_run=True,
        )

    assert list(workflow_instance_ids) == [903]


def test_generated_models_accept_json_data_serialized_fields_from_runtime_reads() -> (
    None
):
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    task_params = {
        "rawScript": "echo extract",
        "localParams": [],
        "resourceList": [],
    }
    relation = {
        "preTaskCode": 0,
        "preTaskVersion": 0,
        "postTaskCode": 7001,
        "postTaskVersion": 1,
        "conditionParams": {},
    }
    task_definition = {
        "code": 7001,
        "name": "extract",
        "projectCode": 7,
        "userId": 1,
        "taskType": "SHELL",
        "taskParams": task_params,
    }
    dag_data = {
        "workflowTaskRelationList": [relation],
        "taskDefinitionList": [task_definition],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/tasks/7001"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": task_definition},
            )
        if (
            request.method == "GET"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-definition/101"
        ):
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": dag_data},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/v2/workflow-instances/901"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 901,
                        "workflowDefinitionCode": 101,
                        "projectCode": 7,
                        "state": "RUNNING_EXECUTION",
                        "runTimes": 1,
                        "name": "daily-sync-901",
                        "executorId": 11,
                        "dagData": dag_data,
                    },
                },
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        task = session.tasks.get(code=7001)
        dag = session.workflows.describe(project_code=7, code=101)
        workflow_instance = session.workflow_instances.get(workflow_instance_id=901)

    assert task.taskParams == task_params
    assert dag.taskDefinitionList is not None
    assert dag.taskDefinitionList[0].taskParams == task_params
    assert dag.workflowTaskRelationList is not None
    assert dag.workflowTaskRelationList[0].conditionParams == {}
    assert workflow_instance.dagData is not None
    assert workflow_instance.dagData.taskDefinitionList is not None
    assert workflow_instance.dagData.taskDefinitionList[0].taskParams == task_params
    assert workflow_instance.dagData.workflowTaskRelationList is not None
    assert workflow_instance.dagData.workflowTaskRelationList[0].conditionParams == {}
    assert requests_seen == [
        ("GET", "/dolphinscheduler/v2/tasks/7001"),
        ("GET", "/dolphinscheduler/projects/7/workflow-definition/101"),
        ("GET", "/dolphinscheduler/v2/workflow-instances/901"),
    ]


def test_adapter_task_instance_methods_bridge_v2_and_logger_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path
            == "/dolphinscheduler/projects/7/workflow-instances/901/tasks"
        ):
            assert not request.url.params
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "dataList": {
                        "workflowInstanceState": "RUNNING_EXECUTION",
                        "taskList": [
                            {
                                "id": 3001,
                                "name": "extract",
                                "taskType": "SHELL",
                                "workflowInstanceId": 901,
                                "projectCode": 7,
                                "taskCode": 201,
                                "taskDefinitionVersion": 1,
                                "state": "RUNNING_EXECUTION",
                            },
                            {
                                "id": 3002,
                                "name": "load",
                                "taskType": "SHELL",
                                "workflowInstanceId": 901,
                                "projectCode": 7,
                                "taskCode": 202,
                                "taskDefinitionVersion": 1,
                                "state": "SUCCESS",
                            },
                        ],
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/v2/projects/7/task-instances/3001"
        ):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 3001,
                        "name": "extract",
                        "taskType": "SHELL",
                        "workflowInstanceId": 901,
                        "projectCode": 7,
                        "taskCode": 201,
                        "taskDefinitionVersion": 1,
                        "state": "RUNNING_EXECUTION",
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/log/detail"
        ):
            assert request.url.params["taskInstanceId"] == "3001"
            assert request.url.params["skipLineNum"] == "0"
            assert request.url.params["limit"] == "1000"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "lineNum": 2,
                        "message": "line-1\nline-2",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/task-instances/3001/force-success"
        ):
            return httpx.Response(200, json={"code": 0, "msg": "success"})
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/task-instances/3001/savepoint"
        ):
            return httpx.Response(200, json={"code": 0, "msg": "success"})
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/task-instances/3001/stop"
        ):
            return httpx.Response(200, json={"code": 0, "msg": "success"})
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.task_instances.list(
            workflow_instance_id=901,
            project_code=7,
            page_no=1,
            page_size=20,
            search="extract",
            state="RUNNING_EXECUTION",
        )
        task_instance = session.task_instances.get(
            project_code=7,
            task_instance_id=3001,
        )
        log = session.task_instances.log_chunk(
            task_instance_id=3001,
            skip_line_num=0,
            limit=1000,
        )
        session.task_instances.force_success(
            project_code=7,
            task_instance_id=3001,
        )
        session.task_instances.savepoint(
            project_code=7,
            task_instance_id=3001,
        )
        session.task_instances.stop(
            project_code=7,
            task_instance_id=3001,
        )

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].name == "extract"
    assert task_instance.workflowInstanceId == 901
    assert log.lineNum == 2
    assert log.message == "line-1\nline-2"
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/workflow-instances/901/tasks"),
        ("POST", "/dolphinscheduler/v2/projects/7/task-instances/3001"),
        ("GET", "/dolphinscheduler/log/detail"),
        ("POST", "/dolphinscheduler/projects/7/task-instances/3001/force-success"),
        ("POST", "/dolphinscheduler/projects/7/task-instances/3001/savepoint"),
        ("POST", "/dolphinscheduler/projects/7/task-instances/3001/stop"),
    ]


def test_adapter_resource_methods_use_resource_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/resources/base-dir"
        ):
            assert request.url.params["type"] == "FILE"
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": "/tenant/resources"},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/resources"
        ):
            assert request.url.params["fullName"] == "/tenant/resources"
            assert request.url.params["type"] == "FILE"
            assert request.url.params["searchVal"] == "demo"
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "20"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "alias": "demo.sql",
                                "fileName": "demo.sql",
                                "fullName": "/tenant/resources/demo.sql",
                                "isDirectory": False,
                                "type": "FILE",
                                "size": 20,
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 20,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/resources/download"
        ):
            assert request.url.params["fullName"] == "/tenant/resources/demo.sql"
            return httpx.Response(
                200,
                content=b"select 1;\nselect 2;\n",
                headers={"content-type": "text/plain; charset=utf-8"},
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/resources/online-create"
        ):
            assert request.content == (
                b"type=FILE&fileName=notes&suffix=txt&content=hello&"
                b"currentDir=%2Ftenant%2Fresources"
            )
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/resources/directory"
        ):
            assert request.content == (
                b"type=FILE&name=archive&currentDir=%2Ftenant%2Fresources"
            )
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/resources"
        ):
            assert request.url.params["fullName"] == "/tenant/resources/demo.sql"
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        base_dir = session.resources.base_dir()
        page = session.resources.list(
            directory="/tenant/resources",
            page_no=1,
            page_size=20,
            search="demo",
        )
        content = session.resources.view(
            full_name="/tenant/resources/demo.sql",
            skip_line_num=0,
            limit=1,
        )
        session.resources.create_from_content(
            current_dir="/tenant/resources",
            file_name="notes",
            suffix="txt",
            content="hello",
        )
        session.resources.create_directory(
            current_dir="/tenant/resources",
            name="archive",
        )
        deleted = session.resources.delete(full_name="/tenant/resources/demo.sql")

    assert base_dir == "/tenant/resources"
    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].fullName == "/tenant/resources/demo.sql"
    assert content.content == "select 1;"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/resources/base-dir"),
        ("GET", "/dolphinscheduler/resources"),
        ("GET", "/dolphinscheduler/resources/download"),
        ("POST", "/dolphinscheduler/resources/online-create"),
        ("POST", "/dolphinscheduler/resources/directory"),
        ("DELETE", "/dolphinscheduler/resources"),
    ]


def test_adapter_resource_list_sends_empty_search_value_when_omitted() -> None:
    profile = make_profile()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/dolphinscheduler/resources"
        assert request.url.params["fullName"] == "/tenant/resources"
        assert request.url.params["type"] == "FILE"
        assert request.url.params["searchVal"] == ""
        assert request.url.params["pageNo"] == "1"
        assert request.url.params["pageSize"] == "20"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [],
                    "total": 0,
                    "totalPage": 1,
                    "pageSize": 20,
                    "currentPage": 1,
                },
            },
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.resources.list(
            directory="/tenant/resources",
            page_no=1,
            page_size=20,
            search=None,
        )

    assert page.total == 0


def test_adapter_resource_upload_and_download_use_raw_transport() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/resources"
        ):
            content_type = request.headers["content-type"]
            assert "multipart/form-data" in content_type
            body = request.content
            assert b'name="type"' in body
            assert b"\r\n\r\nFILE\r\n" in body
            assert b'name="name"' in body
            assert b"\r\n\r\ndemo.sql\r\n" in body
            assert b'name="currentDir"' in body
            assert b"\r\n\r\n/tenant/resources\r\n" in body
            assert b'name="file"; filename="demo.sql"' in body
            assert b"select 1;\n" in body
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/resources/download"
        ):
            assert request.url.params["fullName"] == "/tenant/resources/demo.sql"
            return httpx.Response(
                200,
                content=b"select 1;\n",
                headers={"content-type": "text/plain"},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        session.resources.upload(
            current_dir="/tenant/resources",
            name="demo.sql",
            file=io.BytesIO(b"select 1;\n"),
        )
        download = session.resources.download(full_name="/tenant/resources/demo.sql")

    assert download.content == b"select 1;\n"
    assert download.content_type == "text/plain"
    assert requests_seen == [
        ("POST", "/dolphinscheduler/resources"),
        ("GET", "/dolphinscheduler/resources/download"),
    ]


def test_adapter_project_parameter_methods_bridge_project_scoped_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/projects/7/project-parameter"
        ):
            assert request.url.params["pageNo"] == "1"
            assert request.url.params["pageSize"] == "50"
            assert request.url.params["searchVal"] == "warehouse"
            assert request.url.params["projectParameterDataType"] == "VARCHAR"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "totalList": [
                            {
                                "code": 101,
                                "projectCode": 7,
                                "paramName": "warehouse_db",
                                "paramValue": "jdbc:mysql://warehouse",
                                "paramDataType": "VARCHAR",
                            }
                        ],
                        "total": 1,
                        "totalPage": 1,
                        "pageSize": 50,
                        "currentPage": 1,
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/projects/7/project-parameter"
        ):
            assert request.content == (
                b"projectParameterName=retry_limit&projectParameterValue=3&"
                b"projectParameterDataType=INT"
            )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "code": 102,
                        "projectCode": 7,
                        "paramName": "retry_limit",
                        "paramValue": "3",
                        "paramDataType": "INT",
                    },
                },
            )
        if (
            request.method == "POST"
            and request.url.path
            == "/dolphinscheduler/projects/7/project-parameter/delete"
        ):
            assert request.content == b"code=102"
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": None},
            )
        message = f"Unexpected request: {request.method} {request.url}"
        raise AssertionError(message)

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        page = session.project_parameters.list(
            project_code=7,
            page_no=1,
            page_size=50,
            search="warehouse",
            data_type="VARCHAR",
        )
        created = session.project_parameters.create(
            project_code=7,
            name="retry_limit",
            value="3",
            data_type="INT",
        )
        deleted = session.project_parameters.delete(project_code=7, code=102)

    assert page.total == 1
    assert page.totalList is not None
    assert page.totalList[0].paramName == "warehouse_db"
    assert created.code == 102
    assert created.paramName == "retry_limit"
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/project-parameter"),
        ("POST", "/dolphinscheduler/projects/7/project-parameter"),
        ("POST", "/dolphinscheduler/projects/7/project-parameter/delete"),
    ]


def test_adapter_project_preference_methods_bridge_project_scoped_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    get_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_calls
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.method == "GET":
            assert request.url.path == "/dolphinscheduler/projects/7/project-preference"
            get_calls += 1
            if get_calls == 1:
                return httpx.Response(
                    200,
                    json={"code": 0, "msg": "success", "data": None},
                )
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 11,
                        "code": 101,
                        "projectCode": 7,
                        "preferences": '{"taskPriority":"HIGH"}',
                        "state": 1,
                    },
                },
            )
        if request.method == "PUT":
            assert request.url.path == "/dolphinscheduler/projects/7/project-preference"
            assert parse_qs(request.content.decode()) == {
                "projectPreferences": ['{"taskPriority":"HIGH"}']
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 11,
                        "code": 101,
                        "projectCode": 7,
                        "preferences": '{"taskPriority":"HIGH"}',
                        "state": 1,
                    },
                },
            )
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/projects/7/project-preference"
        assert parse_qs(request.content.decode()) == {"state": ["0"]}
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": None})

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        missing = session.project_preferences.get(project_code=7)
        updated = session.project_preferences.update(
            project_code=7,
            preferences='{"taskPriority":"HIGH"}',
        )
        session.project_preferences.set_state(project_code=7, state=0)
        fetched = session.project_preferences.get(project_code=7)

    assert missing is None
    assert updated.projectCode == 7
    assert updated.preferences == '{"taskPriority":"HIGH"}'
    assert fetched is not None
    assert fetched.state == 1
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
        ("PUT", "/dolphinscheduler/projects/7/project-preference"),
        ("POST", "/dolphinscheduler/projects/7/project-preference"),
        ("GET", "/dolphinscheduler/projects/7/project-preference"),
    ]


def test_adapter_alert_plugin_methods_bridge_ui_plugin_and_crud_endpoints() -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []
    plugin_instance_params = json.dumps(
        [{"field": "url", "name": "url", "type": "input", "value": "hook"}],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        assert request.headers["token"] == profile.api_token
        if request.method == "GET":
            if request.url.path == "/dolphinscheduler/ui-plugins/query-by-type":
                assert request.url.params["pluginType"] == "ALERT"
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": [
                            {
                                "id": 3,
                                "pluginName": "Slack",
                                "pluginType": "ALERT",
                                "pluginParams": plugin_instance_params,
                            }
                        ],
                    },
                )
            if request.url.path == "/dolphinscheduler/ui-plugins/3":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "id": 3,
                            "pluginName": "Slack",
                            "pluginType": "ALERT",
                            "pluginParams": plugin_instance_params,
                        },
                    },
                )
            if request.url.path == "/dolphinscheduler/alert-plugin-instances":
                assert request.url.params["pageNo"] == "1"
                assert request.url.params["pageSize"] == "50"
                assert request.url.params["searchVal"] == "slack"
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "totalList": [
                                {
                                    "id": 11,
                                    "pluginDefineId": 3,
                                    "instanceName": "slack-ops",
                                    "pluginInstanceParams": plugin_instance_params,
                                    "instanceType": "ALERT",
                                    "warningType": "ALL",
                                    "alertPluginName": "Slack",
                                }
                            ],
                            "total": 1,
                            "totalPage": 1,
                            "pageSize": 50,
                            "currentPage": 1,
                        },
                    },
                )
            assert request.url.path == "/dolphinscheduler/alert-plugin-instances/list"
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": [
                        {
                            "id": 11,
                            "pluginDefineId": 3,
                            "instanceName": "slack-ops",
                            "pluginInstanceParams": plugin_instance_params,
                            "instanceType": "ALERT",
                            "warningType": "ALL",
                            "alertPluginName": "Slack",
                        }
                    ],
                },
            )
        if request.method == "POST":
            if request.url.path == "/dolphinscheduler/alert-plugin-instances":
                assert parse_qs(request.content.decode()) == {
                    "pluginDefineId": ["3"],
                    "instanceName": ["slack-ops"],
                    "pluginInstanceParams": [plugin_instance_params],
                }
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "msg": "success",
                        "data": {
                            "id": 11,
                            "pluginDefineId": 3,
                            "instanceName": "slack-ops",
                            "pluginInstanceParams": plugin_instance_params,
                        },
                    },
                )
            assert (
                request.url.path == "/dolphinscheduler/alert-plugin-instances/test-send"
            )
            assert parse_qs(request.content.decode()) == {
                "pluginDefineId": ["3"],
                "pluginInstanceParams": [plugin_instance_params],
            }
            return httpx.Response(
                200,
                json={"code": 0, "msg": "success", "data": True},
            )
        if request.method == "PUT":
            assert request.url.path == "/dolphinscheduler/alert-plugin-instances/11"
            assert parse_qs(request.content.decode()) == {
                "instanceName": ["slack-ops"],
                "pluginInstanceParams": [plugin_instance_params],
            }
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "id": 11,
                        "pluginDefineId": 3,
                        "instanceName": "slack-ops",
                        "pluginInstanceParams": plugin_instance_params,
                    },
                },
            )
        assert request.method == "DELETE"
        assert request.url.path == "/dolphinscheduler/alert-plugin-instances/11"
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": True},
        )

    adapter = DS341Adapter()
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    with http_client:
        session = adapter.bind(profile, http_client=http_client)
        plugin_defines = session.ui_plugins.list(plugin_type="ALERT")
        plugin_define = session.ui_plugins.get(plugin_id=3)
        alert_plugin_page = session.alert_plugins.list(
            page_no=1,
            page_size=50,
            search="slack",
        )
        alert_plugins = session.alert_plugins.list_all()
        created = session.alert_plugins.create(
            plugin_define_id=3,
            instance_name="slack-ops",
            plugin_instance_params=plugin_instance_params,
        )
        updated = session.alert_plugins.update(
            alert_plugin_id=11,
            instance_name="slack-ops",
            plugin_instance_params=plugin_instance_params,
        )
        tested = session.alert_plugins.test_send(
            plugin_define_id=3,
            plugin_instance_params=plugin_instance_params,
        )
        deleted = session.alert_plugins.delete(alert_plugin_id=11)

    assert [item.pluginName for item in plugin_defines] == ["Slack"]
    assert plugin_define.pluginType == "ALERT"
    assert alert_plugin_page.total == 1
    assert alert_plugin_page.totalList is not None
    assert alert_plugin_page.totalList[0].alertPluginName == "Slack"
    assert [item.instanceName for item in alert_plugins] == ["slack-ops"]
    assert created.pluginDefineId == 3
    assert updated.instanceName == "slack-ops"
    assert tested is True
    assert deleted is True
    assert requests_seen == [
        ("GET", "/dolphinscheduler/ui-plugins/query-by-type"),
        ("GET", "/dolphinscheduler/ui-plugins/3"),
        ("GET", "/dolphinscheduler/alert-plugin-instances"),
        ("GET", "/dolphinscheduler/alert-plugin-instances/list"),
        ("POST", "/dolphinscheduler/alert-plugin-instances"),
        ("PUT", "/dolphinscheduler/alert-plugin-instances/11"),
        ("POST", "/dolphinscheduler/alert-plugin-instances/test-send"),
        ("DELETE", "/dolphinscheduler/alert-plugin-instances/11"),
    ]
