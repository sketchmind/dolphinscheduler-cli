from __future__ import annotations

import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    UnsupportedFeatureError,
)
from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
from dsctl.services._whole_workflow_task_update import CodeNativeWholeWorkflowTaskUpdate
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream.task_definition_wire import TaskDefinitionAdapter
from dsctl.upstream.task_definitions import (
    TaskDefinitions,
    TaskSelector,
    TaskUpdateIntent,
)
from dsctl.upstream.task_update import compile_task_update
from dsctl.upstream.workflows import WorkflowAdapter
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Iterator

    from dsctl.services._whole_workflow_task_update import WorkflowUpdateOperations
    from dsctl.support.json_types import JsonObject


def _task(code: int, name: str) -> JsonObject:
    return {
        "id": code,
        "code": code,
        "name": name,
        "version": 3,
        "projectCode": 7,
        "description": "original",
        "taskType": "SHELL",
        "taskParams": json.dumps(
            {
                "rawScript": f"echo {name}",
                "localParams": [],
                "resourceList": [],
                "futureNested": {"keep": name},
            }
        ),
        "flag": "YES",
        "taskPriority": "MEDIUM",
        "workerGroup": "default",
        "environmentCode": -1,
        "failRetryTimes": 0,
        "failRetryInterval": 1,
        "timeoutFlag": "CLOSE",
        "timeoutNotifyStrategy": "WARN",
        "timeout": 0,
        "delayTime": 0,
        "taskGroupId": 7,
        "taskGroupPriority": 3,
        "cpuQuota": -1,
        "memoryMax": -1,
        "taskExecuteType": "BATCH",
        "resourceIds": "",
        "futureTask": {"keep": name},
    }


def _graph() -> JsonObject:
    selected = _task(202, "report")
    selected.pop("futureTask")
    return {
        "workflowDefinition": {
            "id": 17,
            "code": 101,
            "name": "daily-sync",
            "version": 5,
            "projectCode": 7,
            "releaseState": "OFFLINE",
            "executionType": "PARALLEL",
            "description": "preserve workflow",
            "globalParams": (
                '[{"prop":"day","direct":"IN","type":"VARCHAR","value":"today"}]'
            ),
            "locations": '{"futureLocation":{"keep":true}}',
            "timeout": 0,
        },
        "taskDefinitionList": [_task(201, "extract"), selected],
        "workflowTaskRelationList": [
            {
                "preTaskCode": 0,
                "preTaskVersion": 0,
                "postTaskCode": 201,
                "postTaskVersion": 3,
                "conditionType": "NONE",
                "conditionParams": "{}",
                "futureRelation": "root",
            },
            {
                "preTaskCode": 201,
                "preTaskVersion": 3,
                "postTaskCode": 202,
                "postTaskVersion": 3,
                "conditionType": "NONE",
                "conditionParams": "{}",
                "futureRelation": "selected-edge",
            },
        ],
    }


@dataclass
class _Server:
    graph: JsonObject = field(default_factory=_graph)
    sent: list[dict[str, list[str]]] = field(default_factory=list)
    lose_response: bool = False
    corrupt_readback: bool = False

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/dolphinscheduler/projects":
            return self._success(
                {
                    "totalList": [{"id": 7, "code": 7, "name": "etl-prod"}],
                    "total": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                }
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/projects/7"
        ):
            return self._success({"id": 7, "code": 7, "name": "etl-prod"})
        if request.method == "GET" and request.url.path.endswith(
            "/workflow-definition/simple-list"
        ):
            return self._success(
                [{"code": 101, "projectCode": 7, "name": "daily-sync", "version": 5}]
            )
        if request.method == "GET" and request.url.path.endswith(
            "/workflow-definition/101"
        ):
            return self._success(self.graph)
        if request.method == "GET" and request.url.path.endswith(
            "/task-definition/202"
        ):
            return self._success(self._tasks()[1])
        if request.method == "PUT" and request.url.path.endswith(
            "/workflow-definition/101"
        ):
            return self._update(request)
        message = f"unexpected {request.method} {request.url.path}"
        raise AssertionError(message)

    def _update(self, request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        self.sent.append(form)
        old_tasks = {task["code"]: task for task in self._tasks()}
        tasks = json.loads(form["taskDefinitionJson"][0])
        for task in tasks:
            old = old_tasks[task["code"]]
            task["version"] = old["version"] + (task != old)
        self.graph["taskDefinitionList"] = tasks
        relations = json.loads(form["taskRelationJson"][0])
        versions = {task["code"]: task["version"] for task in tasks}
        for relation in relations:
            relation["preTaskVersion"] = versions.get(relation["preTaskCode"], 0)
            relation["postTaskVersion"] = versions[relation["postTaskCode"]]
            if isinstance(relation["conditionType"], int):
                relation["conditionType"] = {0: "NONE", 1: "JUDGE", 2: "DELAY"}[
                    relation["conditionType"]
                ]
        self.graph["workflowTaskRelationList"] = relations
        if self.corrupt_readback:
            tasks[1]["description"] = "unexpected server value"
        if self.lose_response:
            message = "lost after application"
            raise httpx.ReadError(message, request=request)
        return self._success(self.graph["workflowDefinition"])

    def _tasks(self) -> list[JsonObject]:
        return cast("list[JsonObject]", self.graph["taskDefinitionList"])

    @staticmethod
    def _success(data: object) -> httpx.Response:
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


@contextmanager
def _module(server: _Server) -> Iterator[TaskDefinitions]:
    profile = make_profile(ds_version="3.4.3")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(server.handle)
    ) as client:
        upstream = TaskDefinitionAdapter.for_version("3.4.3").bind_task_definitions(
            profile, http_client=client
        )
        workflows = (
            WorkflowAdapter.for_version("3.4.3")
            .bind(profile, http_client=client)
            .workflows
        )
        fallback = CodeNativeWholeWorkflowTaskUpdate(
            profile_version="3.4.3",
            operations=cast("WorkflowUpdateOperations", workflows),
            catalog=get_task_authoring_catalog("3.4.3"),
            compile_update=prepare_workflow_mutation_plan,
            task_request_fields=upstream.task_definitions.top_level_field_policy.request_payload,
        )
        yield TaskDefinitions(
            profile_version="3.4.3",
            definitions=upstream.definitions,
            wire=upstream.task_definitions,
            compile_update=compile_task_update,
            whole_workflow_update=fallback,
        )


def _intent(patch: JsonObject, *fields: str) -> TaskUpdateIntent:
    return TaskUpdateIntent(
        selector=TaskSelector("etl-prod", "daily-sync", "report"),
        patch=WorkflowPatchTaskSetSpec.model_validate(patch),
        requested_fields=fields,
    )


@pytest.mark.parametrize(
    ("patch", "path", "wire", "expected"),
    [
        ({"description": "changed"}, "description", "description", "changed"),
        ({"cpu_quota": 4}, "cpu_quota", "cpuQuota", 4),
        ({"memory_max": 512}, "memory_max", "memoryMax", 512),
        ({"task_group_priority": 5}, "task_group_priority", "taskGroupPriority", 5),
        ({"task_group_id": None}, "task_group_id", "taskGroupId", 0),
        ({"task_group_id": 8}, "task_group_id", "taskGroupId", 8),
        ({"command": "echo changed"}, "command", "taskParams", None),
    ],
)
def test_exact_whole_workflow_task_overlay_preserves_unselected_native_graph(
    patch: JsonObject, path: str, wire: str, expected: object
) -> None:
    server = _Server()
    original = deepcopy(server.graph)
    with _module(server) as definitions:
        prepared = definitions.prepare_update(_intent(patch, path))
        assert server.sent == []
        outcome = definitions.apply(prepared)
    assert outcome.mutation_applied is True
    assert len(server.sent) == 1
    tasks = json.loads(server.sent[0]["taskDefinitionJson"][0])
    original_tasks = cast("list[JsonObject]", original["taskDefinitionList"])
    assert tasks[0] == original_tasks[0]
    target = tasks[1]
    if path == "command":
        params = json.loads(target[wire])
        assert params["rawScript"] == "echo changed"
        assert params["futureNested"] == {"keep": "report"}
    else:
        assert target[wire] == expected
        assert target["taskParams"] == original_tasks[1]["taskParams"]
    if path == "task_group_id":
        assert target["taskGroupPriority"] == 0
    assert server.sent[0]["globalParams"] == [
        cast("JsonObject", original["workflowDefinition"])["globalParams"]
    ]
    assert server.sent[0]["locations"] == ['{"futureLocation":{"keep":true}}']


def test_exact_whole_workflow_task_dependencies_are_saved_with_tasks() -> None:
    server = _Server()
    with _module(server) as definitions:
        prepared = definitions.prepare_update(_intent({"depends_on": []}, "depends_on"))
        definitions.apply(prepared)
    relations = json.loads(server.sent[0]["taskRelationJson"][0])
    assert {(row["preTaskCode"], row["postTaskCode"]) for row in relations} == {
        (0, 201),
        (0, 202),
    }
    assert (
        next(row for row in relations if row["postTaskCode"] == 201)["futureRelation"]
        == "root"
    )
    assert len(server.sent) == 1


def test_exact_whole_workflow_task_noop_does_not_mutate() -> None:
    server = _Server()
    with _module(server) as definitions:
        prepared = definitions.prepare_update(
            _intent({"description": "original"}, "description")
        )
        outcome = definitions.apply(prepared)
    assert outcome.mutation_applied is False
    assert server.sent == []


def test_exact_whole_workflow_task_same_group_preserves_priority_without_mutation() -> (
    None
):
    server = _Server()
    with _module(server) as definitions:
        prepared = definitions.prepare_update(
            _intent({"task_group_id": 7}, "task_group_id")
        )
        outcome = definitions.apply(prepared)
    assert outcome.mutation_applied is False
    assert server._tasks()[1]["taskGroupPriority"] == 3
    assert server.sent == []


def test_exact_whole_workflow_task_rejects_unreviewed_selected_top_level_field() -> (
    None
):
    server = _Server()
    server._tasks()[1]["unreviewedField"] = {"keep": True}
    with _module(server) as definitions, pytest.raises(UnsupportedFeatureError):
        definitions.prepare_update(_intent({"description": "changed"}, "description"))
    assert server.sent == []


def test_exact_whole_workflow_task_stale_sibling_blocks_mutation() -> None:
    server = _Server()
    with _module(server) as definitions:
        prepared = definitions.prepare_update(
            _intent({"description": "changed"}, "description")
        )
        server._tasks()[0]["futureTask"] = {"concurrent": True}
        with pytest.raises(ConflictError):
            definitions.apply(prepared)
    assert server.sent == []


@pytest.mark.parametrize("failure", ["lost", "mismatch"])
def test_exact_whole_workflow_task_uncertain_or_mismatched_write_is_once_only(
    failure: str,
) -> None:
    server = _Server(
        lose_response=failure == "lost", corrupt_readback=failure == "mismatch"
    )
    with _module(server) as definitions:
        prepared = definitions.prepare_update(
            _intent({"description": "changed"}, "description")
        )
        with pytest.raises(ApiTransportError) as exc:
            definitions.apply(prepared)
    if failure == "lost":
        assert exc.value.details["mutation_may_have_applied"] is True
        assert exc.value.details["attempts"] == 1
        assert exc.value.details["request_replay_safe"] is False
    else:
        assert exc.value.details["mutation_applied"] is True
        assert exc.value.details["mismatched_fields"] == ["description"]
    assert len(server.sent) == 1


def test_exact_whole_workflow_task_requires_offline_workflow() -> None:
    server = _Server()
    cast("JsonObject", server.graph["workflowDefinition"])["releaseState"] = "ONLINE"
    with _module(server) as definitions, pytest.raises(InvalidStateError):
        definitions.prepare_update(_intent({"description": "changed"}, "description"))
    assert server.sent == []
