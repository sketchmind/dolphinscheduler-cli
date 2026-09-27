"""The public list mode resolves accepted trigger identities without pagination."""

from collections.abc import Callable

import httpx
import pytest
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.services.workflow_instance.triggers import (
    _list_by_trigger,
    list_workflow_instances_by_trigger_result,
)
from dsctl.upstream.runtime_instances import RUNTIME_INSTANCE_DOMAIN


@pytest.mark.parametrize("ids", [[], [901, 902]])
def test_trigger_list_uses_exact_program_and_reports_non_paginated_identities(
    ids: list[int],
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        data = (
            [{"id": identity} for identity in ids]
            if request.url.path.endswith("/trigger")
            else {"code": 101, "name": "demo"}
        )
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})

    profile = make_profile(ds_version="3.2.2")
    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        runtime = BoundDomainServiceRuntime(
            profile,
            ResourceDefaults(),
            client,
            RUNTIME_INSTANCE_DOMAIN.bind(profile, http_client=client),
        )
        result = _list_by_trigger(runtime, trigger_code=42, project="101")
    data = assert_mapping(result.data)
    assert data["totalList"] == [{"id": identity} for identity in ids]
    assert data["instanceResolution"] == ("resolved" if ids else "pending")
    assert "totalPage" not in data
    assert assert_mapping(result.resolved["query"])["paginated"] is False
    assert requests[-1].url.params["triggerCode"] == "42"
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize(
    "call",
    [
        lambda: list_workflow_instances_by_trigger_result(42, workflow="daily"),
        lambda: list_workflow_instances_by_trigger_result(42, search="daily"),
        lambda: list_workflow_instances_by_trigger_result(42, executor="admin"),
        lambda: list_workflow_instances_by_trigger_result(42, host="worker"),
        lambda: list_workflow_instances_by_trigger_result(
            42, start="2026-09-09 00:00:00"
        ),
        lambda: list_workflow_instances_by_trigger_result(
            42, end="2026-09-09 00:00:00"
        ),
        lambda: list_workflow_instances_by_trigger_result(42, state="FAILURE"),
        lambda: list_workflow_instances_by_trigger_result(42, page_no=2),
        lambda: list_workflow_instances_by_trigger_result(42, page_size=1),
        lambda: list_workflow_instances_by_trigger_result(42, all_pages=True),
    ],
)
def test_trigger_list_rejects_ordinary_filters_before_runtime(
    call: Callable[[], CommandResult],
) -> None:
    with pytest.raises(UserInputError, match="ordinary filters or pagination"):
        call()
