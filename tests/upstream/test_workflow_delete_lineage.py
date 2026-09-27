"""Whole-definition deletion must not orphan exact 3.3.x owner lineage."""

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError
from dsctl.support.json_types import JsonObject
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.workflows import WorkflowAdapter, WorkflowDeleteLineageError
from tests.support import make_profile


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
            user_name="test",
            project_name="demo",
            timeout=0,
            release_state="OFFLINE",
            execution_type="PARALLEL",
            include_execution_type=True,
        ),
    )


def _delete(
    version: str, response: httpx.Response, requests: list[tuple[str, str]]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.method == "GET":
            assert request.url.path.endswith("/projects/101/lineages/202")
            return response
        assert request.method == "DELETE"
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": None})

    profile = make_profile(ds_version=version).model_copy(
        update={"api_retry_attempts": 1}
    )
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        operations = (
            WorkflowAdapter.for_version(version)
            .bind(profile, http_client=client)
            .workflows
        )
        operations.delete(_scope())


def _lineage(relations: list[JsonObject] | None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "code": 0,
            "msg": "success",
            "data": {
                "data": {
                    "workFlowRelationList": relations,
                    # Cross-project and stale owner rows may lack node details.
                    "workFlowRelationDetailList": [],
                },
            },
        },
    )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2"])
@pytest.mark.parametrize(
    "source", [303, 202, 0], ids=["other-workflow", "self-edge", "owner-root"]
)
def test_delete_blocks_owner_lineage_without_fetching_current_tasks(
    version: str, source: int
) -> None:
    requests: list[tuple[str, str]] = []
    response = _lineage([{"sourceWorkFlowCode": source, "targetWorkFlowCode": 202}])
    with pytest.raises(WorkflowDeleteLineageError):
        _delete(version, response, requests)
    assert [method for method, _ in requests] == ["GET"]


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2"])
@pytest.mark.parametrize(
    "relations",
    [
        [],
        [{"sourceWorkFlowCode": 202, "targetWorkFlowCode": 303}],
        [{"sourceWorkFlowCode": 0, "targetWorkFlowCode": 303}],
    ],
    ids=["no-lineage", "referenced-by-other-owner", "synthetic-root"],
)
def test_delete_without_owner_edges_uses_native_delete(
    version: str, relations: list[JsonObject]
) -> None:
    requests: list[tuple[str, str]] = []
    _delete(version, _lineage(relations), requests)
    assert [method for method, _ in requests] == ["GET", "DELETE"]


@pytest.mark.parametrize("version", ["3.2.2", "3.4.0", "3.4.1", "3.4.3"])
def test_delete_on_unaffected_profiles_needs_no_lineage_read(version: str) -> None:
    requests: list[tuple[str, str]] = []
    _delete(version, httpx.Response(500), requests)
    assert [method for method, _ in requests] == ["DELETE"]


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(503),
        httpx.Response(200, json={"code": 30001, "msg": "denied", "data": None}),
        httpx.Response(200, json={"code": 0, "msg": "success", "data": None}),
        _lineage(None),
        _lineage([{"sourceWorkFlowCode": 303}]),
        _lineage([{"sourceWorkFlowCode": -1, "targetWorkFlowCode": 202}]),
        _lineage([{"sourceWorkFlowCode": 303, "targetWorkFlowCode": 0}]),
    ],
    ids=[
        "http",
        "api",
        "null-data",
        "null-relations",
        "missing-code",
        "negative",
        "zero-target",
    ],
)
def test_delete_stops_when_lineage_cannot_be_verified(response: httpx.Response) -> None:
    requests: list[tuple[str, str]] = []
    with pytest.raises((ApiHttpError, ApiResultError, ApiTransportError)):
        _delete("3.3.1", response, requests)
    assert [method for method, _ in requests] == ["GET"]
