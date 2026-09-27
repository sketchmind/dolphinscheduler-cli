from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError
from dsctl.generated import task_definition_cleanup_profiles as cleanup_profiles
from dsctl.generated.task_definition_cleanup_profiles import (
    TARGET_TASK_DEFINITION_CLEANUP_VERSIONS as _CLEANUP_VERSIONS,
)
from dsctl.generated.wire_programs import workflow_runtime as compiled_workflow_runtime
from dsctl.release_gate.task_definition_cleanup import (
    CleanupTaskDetail,
    CleanupTaskHistoryPage,
    CleanupTaskPage,
    CleanupTaskRef,
    TaskDeleteAmbiguousError,
    TaskReleaseAmbiguousError,
    canonical_task_params_fingerprint,
)
from dsctl.upstream import task_definition_cleanup as cleanup_adapter
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.task_definition_cleanup import (
    TaskDefinitionCleanupAdapter,
    _load_cleanup_profile,
    _required_int,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_DIRECT_CLEANUP_VERSIONS = tuple(
    version
    for version in _CLEANUP_VERSIONS
    if cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES[version]["strategy"]
    == "direct-delete"
)


def test_cleanup_adapter_rejects_an_unreviewed_future_profile_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_data = dict(cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILE_DATA)
    profile_data["schema_version"] = 5
    monkeypatch.setattr(
        cleanup_profiles,
        "TASK_DEFINITION_CLEANUP_PROFILE_DATA",
        profile_data,
    )

    with pytest.raises(WireContractError, match="profile header drifted"):
        _load_cleanup_profile("3.1.9")


@pytest.mark.parametrize("value", [True, False, 1.0])
def test_integer_projection_rejects_bool_and_float(value: object) -> None:
    with pytest.raises(WireContractError, match="not an integer"):
        _required_int(SimpleNamespace(value=value), "value")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("total", True, id="page-total-bool"),
        pytest.param("totalPage", 1.0, id="page-count-float"),
        pytest.param("pageSize", True, id="page-size-bool"),
        pytest.param("currentPage", 1.0, id="current-page-float"),
        pytest.param("pageNo", True, id="page-number-bool"),
    ],
)
def test_list_page_rejects_non_exact_integer_metadata_at_http_boundary(
    field: str,
    value: object,
) -> None:
    profile = make_profile(ds_version="3.1.0")

    def handler(_request: httpx.Request) -> httpx.Response:
        page: dict[str, object] = {
            "totalList": [],
            "total": 0,
            "totalPage": 1,
            "pageSize": 100,
            "currentPage": 1,
            "pageNo": 1,
        }
        page[field] = value
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": page},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError):
        adapter.bind(profile, http_client=http_client).list_page(
            project_code=7001,
            execute_type="BATCH",
            page_no=1,
            page_size=100,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("taskCode", True, id="task-code-bool"),
        pytest.param("taskVersion", 1.0, id="task-version-float"),
    ],
)
def test_list_page_rejects_non_exact_row_integers_at_http_boundary(
    field: str,
    value: object,
) -> None:
    profile = make_profile(ds_version="3.1.0")

    def handler(_request: httpx.Request) -> httpx.Response:
        row: dict[str, object] = {
            "taskCode": 11,
            "taskName": "extract",
            "taskVersion": 3,
        }
        row[field] = value
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [row],
                    "total": 1,
                    "totalPage": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                    "pageNo": 1,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError):
        adapter.bind(profile, http_client=http_client).list_page(
            project_code=7001,
            execute_type="BATCH",
            page_no=1,
            page_size=100,
        )


@pytest.mark.parametrize("ds_version", ["2.0.0", "2.0.9"])
def test_20x_list_page_uses_exact_generated_legacy_query_and_projects_rows(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, dict(request.url.params))
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [
                        {"code": 11, "name": "extract", "version": 3},
                        {"code": 12, "name": "load", "version": 4},
                    ],
                    "total": 2,
                    "totalPage": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        port = adapter.bind(profile, http_client=http_client)
        page = port.list_page(
            project_code=7001,
            execute_type=None,
            page_no=1,
            page_size=100,
        )

    assert page == CleanupTaskPage(
        execute_type=None,
        requested_page=1,
        current_page=1,
        offset=0,
        page_size=100,
        total=2,
        total_pages=1,
        rows=(
            CleanupTaskRef(code=11, name="extract", version=3),
            CleanupTaskRef(code=12, name="load", version=4),
        ),
    )
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7001/task-definition",
            {"userId": "0", "pageNo": "1", "pageSize": "100"},
        )
    ]


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.0.6"])
def test_30x_list_page_uses_exact_workflow_task_query(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, dict(request.url.params))
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [],
                    "total": 0,
                    "totalPage": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                    "pageNo": 1,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = adapter.bind(profile, http_client=http_client).list_page(
            project_code=7001,
            execute_type=None,
            page_no=1,
            page_size=100,
        )

    assert page.rows == ()
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7001/task-definition",
            {"taskType": "", "pageNo": "1", "pageSize": "100"},
        )
    ]


@pytest.mark.parametrize("execute_type", ["BATCH", "STREAM"])
def test_310_list_page_uses_exact_task_execute_type_enum(
    execute_type: str,
) -> None:
    profile = make_profile(ds_version="3.1.0")
    requests_seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, dict(request.url.params))
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [
                        {
                            "taskCode": 21,
                            "taskName": "extract",
                            "taskVersion": 5,
                        }
                    ],
                    "total": 1,
                    "totalPage": 1,
                    "pageSize": 100,
                    "currentPage": 1,
                    "pageNo": 1,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        port = adapter.bind(profile, http_client=http_client)
        assert port.inventory_execute_types == ("BATCH", "STREAM")
        page = port.list_page(
            project_code=7001,
            execute_type=execute_type,
            page_no=1,
            page_size=100,
        )

    assert page.rows == (CleanupTaskRef(code=21, name="extract", version=5),)
    assert page.execute_type == execute_type
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7001/task-definition",
            {
                "taskType": "",
                "taskExecuteType": execute_type,
                "pageNo": "1",
                "pageSize": "100",
            },
        )
    ]


def test_310_list_page_rejects_unreviewed_execute_type_before_io() -> None:
    requests_seen: list[str] = []
    profile = make_profile(ds_version="3.1.0")

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return httpx.Response(200, json={})

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.0")
    with http_client, pytest.raises(WireContractError, match="inventory scope"):
        adapter.bind(profile, http_client=http_client).list_page(
            project_code=7001,
            execute_type="HYBRID",
            page_no=1,
            page_size=100,
        )

    assert requests_seen == []


def test_319_projects_workflow_binding_and_exact_task_history() -> None:
    profile = make_profile(ds_version="3.1.9")
    requests_seen: list[tuple[str, str, dict[str, str]]] = []
    raw_script = 'printf "%s\\n" "0123456789abcdef-updated-load"\n'

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, dict(request.url.params))
        )
        if request.url.path.endswith("/versions"):
            data: object = {
                "totalList": [
                    {
                        "code": 12,
                        "name": "load",
                        "version": 2,
                        "projectCode": 7001,
                        "taskType": "SHELL",
                        "flag": "YES",
                        "description": (
                            "dsctl-conformance-owner:0123456789abcdef;"
                            "resource=task;name=load"
                        ),
                        "taskParams": {
                            "rawScript": raw_script,
                            "localParams": [],
                            "resourceList": [],
                        },
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "pageSize": 100,
                "currentPage": 1,
                "pageNo": 1,
            }
        else:
            data = {
                "totalList": [
                    {
                        "taskCode": 12,
                        "taskName": "load",
                        "taskVersion": 2,
                        "processDefinitionCode": 8001,
                        "processDefinitionVersion": 1,
                        "processDefinitionName": ("dsctl-full-3-1-9-0123456789abcdef"),
                        "processReleaseState": "OFFLINE",
                    }
                ],
                "total": 1,
                "totalPage": 1,
                "pageSize": 100,
                "currentPage": 1,
                "pageNo": 1,
            }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": data},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        port = adapter.bind(profile, http_client=http_client)
        page = port.list_page(
            project_code=7001,
            execute_type="BATCH",
            page_no=1,
            page_size=100,
        )
        history = port.list_history_page(
            project_code=7001,
            code=12,
            page_no=1,
            page_size=100,
        )

    assert port.ds_version == "3.1.9"
    assert port.cleanup_strategy == "workflow-cascade-proof-only"
    assert page.rows == (
        CleanupTaskRef(
            code=12,
            name="load",
            version=2,
            workflow_code=8001,
            workflow_version=1,
            workflow_name="dsctl-full-3-1-9-0123456789abcdef",
            workflow_release_state="OFFLINE",
        ),
    )
    expected_detail = CleanupTaskDetail(
        code=12,
        name="load",
        version=2,
        project_code=7001,
        task_type="SHELL",
        description=(
            "dsctl-conformance-owner:0123456789abcdef;resource=task;name=load"
        ),
        raw_script=raw_script,
        task_params_fingerprint=canonical_task_params_fingerprint(raw_script),
    )
    assert history == CleanupTaskHistoryPage(
        requested_page=1,
        current_page=1,
        offset=0,
        page_size=100,
        total=1,
        total_pages=1,
        rows=(expected_detail,),
    )
    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7001/task-definition",
            {
                "taskType": "",
                "taskExecuteType": "BATCH",
                "pageNo": "1",
                "pageSize": "100",
            },
        ),
        (
            "GET",
            "/dolphinscheduler/projects/7001/task-definition/12/versions",
            {"pageNo": "1", "pageSize": "100"},
        ),
    ]


def test_cleanup_adapter_fails_closed_on_generated_profile_shape_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES["2.0.9"]
    monkeypatch.setitem(profile, "unexpected", True)

    with pytest.raises(WireContractError, match="profile shape drifted"):
        TaskDefinitionCleanupAdapter.for_version("2.0.9")


def test_cleanup_adapter_uses_the_generated_discriminated_recipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES["2.0.9"]
    monkeypatch.setitem(profile, "page_params_epoch", "workflow-task-search")
    monkeypatch.setitem(
        profile,
        "page_model",
        "candidate.generated.TaskPageRow",
    )
    monkeypatch.setitem(
        profile,
        "row_fields",
        ["candidateCode", "candidateName", "candidateVersion"],
    )

    projected = _load_cleanup_profile("2.0.9")

    assert projected.page_params_epoch == "workflow-task-search"
    assert projected.row_fields == (
        "candidateCode",
        "candidateName",
        "candidateVersion",
    )


def test_cleanup_adapter_consumes_structural_proof_recipe_without_field_restatement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = cleanup_profiles.TASK_DEFINITION_CLEANUP_PROFILES["3.1.9"]
    monkeypatch.setitem(
        profile,
        "workflow_binding_fields",
        [
            "candidateWorkflowCode",
            "candidateWorkflowVersion",
            "candidateWorkflowName",
            "candidateWorkflowState",
        ],
    )
    monkeypatch.setitem(
        profile,
        "history_model",
        "candidate.generated.TaskHistory",
    )

    projected = _load_cleanup_profile("3.1.9")

    assert projected.workflow_binding_fields == (
        "candidateWorkflowCode",
        "candidateWorkflowVersion",
        "candidateWorkflowName",
        "candidateWorkflowState",
    )
    assert projected.history_model == "candidate.generated.TaskHistory"


def test_cleanup_adapter_uses_the_generated_private_semantic_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cleanup_profiles,
        "TASK_DEFINITION_CLEANUP_SEMANTIC_OPERATION",
        "release-gate.task-definition.changed",
    )

    with pytest.raises(WireContractError, match="task-definition cleanup operations"):
        TaskDefinitionCleanupAdapter.for_version("3.1.0")


def test_cleanup_adapter_rejects_package_and_client_identity_drift_before_io() -> None:
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return httpx.Response(200, json={})

    with pytest.raises(WireContractError, match=r"no reviewed.*cleanup recipe"):
        TaskDefinitionCleanupAdapter.for_version("3.2.0")
    adapter_profile = make_profile(ds_version="3.1.0")
    client_profile = make_profile(ds_version="3.0.6")
    http_client = DolphinSchedulerClient(
        client_profile,
        transport=httpx.MockTransport(handler),
    )
    with (
        http_client,
        pytest.raises(WireContractError, match="client profile"),
    ):
        TaskDefinitionCleanupAdapter.for_version("3.1.0").bind(
            adapter_profile,
            http_client=http_client,
        )

    assert requests_seen == []


def test_cleanup_adapter_requires_private_semantic_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profiles = deepcopy(compiled_workflow_runtime.PROFILES)
    programs = profiles["3.1.0"]["programs"]
    assert isinstance(programs, dict)
    del programs["task_cleanup_page"]
    monkeypatch.setattr(compiled_workflow_runtime, "PROFILES", profiles)
    monkeypatch.setattr(
        cleanup_adapter, "WORKFLOW_PROGRAMS", replace(WORKFLOW_PROGRAMS)
    )

    with pytest.raises(WireContractError, match=r"profile 3\.1\.0 digest"):
        TaskDefinitionCleanupAdapter.for_version("3.1.0")


@pytest.mark.parametrize("ds_version", _CLEANUP_VERSIONS)
def test_get_detail_projects_exact_canonical_task_params(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str]] = []
    raw_script = 'printf "%s\\n" "0123456789abcdef-extract"\n'

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": 11,
                    "name": "extract",
                    "version": 3,
                    "projectCode": 7001,
                    "taskType": "SHELL",
                    "flag": "YES",
                    "description": (
                        "dsctl-conformance-owner:0123456789abcdef;"
                        "resource=task;name=extract"
                    ),
                    "taskParams": {
                        "rawScript": raw_script,
                        "localParams": [],
                        "resourceList": [],
                    },
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        detail = adapter.bind(profile, http_client=http_client).get_detail(
            project_code=7001,
            code=11,
        )

    assert detail == CleanupTaskDetail(
        code=11,
        name="extract",
        version=3,
        project_code=7001,
        task_type="SHELL",
        description=(
            "dsctl-conformance-owner:0123456789abcdef;resource=task;name=extract"
        ),
        raw_script=raw_script,
        task_params_fingerprint=canonical_task_params_fingerprint(raw_script),
    )
    assert requests_seen == [
        ("GET", "/dolphinscheduler/projects/7001/task-definition/11")
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("code", True, id="code-bool"),
        pytest.param("version", 3.0, id="version-float"),
        pytest.param("projectCode", True, id="project-code-bool"),
    ],
)
def test_get_detail_rejects_non_exact_identity_integers_at_http_boundary(
    field: str,
    value: object,
) -> None:
    profile = make_profile(ds_version="2.0.9")
    raw_script = 'printf "%s\\n" "0123456789abcdef-extract"\n'

    def handler(_request: httpx.Request) -> httpx.Response:
        detail: dict[str, object] = {
            "code": 11,
            "name": "extract",
            "version": 3,
            "projectCode": 7001,
            "taskType": "SHELL",
            "flag": "YES",
            "description": "owner",
            "taskParams": {
                "rawScript": raw_script,
                "localParams": [],
                "resourceList": [],
            },
        }
        detail[field] = value
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": detail},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("2.0.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError):
        adapter.bind(profile, http_client=http_client).get_detail(
            project_code=7001,
            code=11,
        )


@pytest.mark.parametrize(
    "task_params",
    [
        pytest.param(
            {
                "rawScript": "script",
                "localParams": [],
                "resourceList": [],
                "foreign": True,
            },
            id="extra-key",
        ),
        pytest.param(
            {
                "rawScript": "script",
                "localParams": [{"prop": "foreign"}],
                "resourceList": [],
            },
            id="non-empty-local-params",
        ),
        pytest.param(
            {
                "rawScript": 1,
                "localParams": [],
                "resourceList": [],
            },
            id="non-text-script",
        ),
        pytest.param(None, id="null"),
    ],
)
def test_get_detail_rejects_noncanonical_task_params(task_params: object) -> None:
    profile = make_profile(ds_version="3.1.0")
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "code": 11,
                    "name": "extract",
                    "version": 3,
                    "projectCode": 7001,
                    "taskType": "SHELL",
                    "flag": "YES",
                    "description": "owner",
                    "taskParams": task_params,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("3.1.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(WireContractError, match="taskParams"):
        adapter.bind(profile, http_client=http_client).get_detail(
            project_code=7001,
            code=11,
        )

    assert requests_seen == ["/dolphinscheduler/projects/7001/task-definition/11"]


@pytest.mark.parametrize(
    ("ds_version", "response_data"),
    [
        pytest.param("2.0.0", None, id="2.0.0-void"),
        *[
            pytest.param(version, None, id=f"{version}-null")
            for version in _DIRECT_CLEANUP_VERSIONS[1:]
        ],
        *[
            pytest.param(
                version,
                {"code": 9001, "projectCode": 7001},
                id=f"{version}-process-definition",
            )
            for version in _DIRECT_CLEANUP_VERSIONS[1:]
        ],
    ],
)
def test_delete_accepts_exact_null_and_non_null_generated_responses(
    ds_version: str,
    response_data: object,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": response_data},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        adapter.bind(profile, http_client=http_client).delete(
            project_code=7001,
            code=11,
        )

    assert requests_seen == [
        ("DELETE", "/dolphinscheduler/projects/7001/task-definition/11")
    ]


@pytest.mark.parametrize("ds_version", ["2.0.1", "2.0.2", "2.0.3"])
def test_pre_delete_offline_uses_exact_generated_release_request(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, request.content.decode())
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        port = adapter.bind(profile, http_client=http_client)
        assert port.pre_delete_release == "offline"
        port.release_offline(project_code=7001, code=11)

    assert requests_seen == [
        (
            "POST",
            "/dolphinscheduler/projects/7001/task-definition/11/release",
            "releaseState=OFFLINE",
        )
    ]


def test_200_cannot_release_before_delete() -> None:
    profile = make_profile(ds_version="2.0.0")
    requests_seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success"})

    adapter = TaskDefinitionCleanupAdapter.for_version("2.0.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(WireContractError, match="unavailable"):
        adapter.bind(profile, http_client=http_client).release_offline(
            project_code=7001,
            code=11,
        )

    assert requests_seen == []


def test_pre_delete_workflow_proof_uses_fresh_project_wide_page() -> None:
    profile = make_profile(ds_version="2.0.2")
    requests_seen: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(
            (request.method, request.url.path, dict(request.url.params))
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": {
                    "totalList": [],
                    "total": 0,
                    "totalPage": 0,
                    "pageSize": 100,
                    "currentPage": 1,
                },
            },
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("2.0.2")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        adapter.bind(profile, http_client=http_client).prove_no_workflows(
            project_code=7001
        )

    assert requests_seen == [
        (
            "GET",
            "/dolphinscheduler/projects/7001/process-definition",
            {"pageNo": "1", "pageSize": "100"},
        )
    ]


@pytest.mark.parametrize("ds_version", ["2.0.1", "2.0.2"])
def test_ambiguous_pre_delete_offline_is_never_retried(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        message = "release timed out"
        raise httpx.ReadTimeout(message, request=request)

    adapter = TaskDefinitionCleanupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(TaskReleaseAmbiguousError):
        adapter.bind(profile, http_client=http_client).release_offline(
            project_code=7001,
            code=11,
        )

    assert calls == 1


@pytest.mark.parametrize("failure", ["network", "response-validation"])
def test_delete_maps_only_ambiguous_transport_failures(
    failure: str,
) -> None:
    profile = make_profile(ds_version="2.0.9")

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "network":
            message = "delete timed out"
            raise httpx.ReadTimeout(message, request=request)
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": []},
        )

    adapter = TaskDefinitionCleanupAdapter.for_version("2.0.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with (
        http_client,
        pytest.raises(TaskDeleteAmbiguousError) as captured,
    ):
        adapter.bind(profile, http_client=http_client).delete(
            project_code=7001,
            code=11,
        )

    assert isinstance(captured.value.__cause__, ApiTransportError)


@pytest.mark.parametrize(
    ("status_code", "payload", "error_type"),
    [
        pytest.param(
            500,
            {"message": "server failed"},
            ApiHttpError,
            id="http-error",
        ),
        pytest.param(
            200,
            {"code": 123, "msg": "delete rejected", "data": None},
            ApiResultError,
            id="result-error",
        ),
    ],
)
def test_delete_preserves_unambiguous_remote_failures(
    status_code: int,
    payload: object,
    error_type: type[Exception],
) -> None:
    profile = make_profile(ds_version="2.0.9")
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(status_code, json=payload)
    )
    adapter = TaskDefinitionCleanupAdapter.for_version("2.0.9")
    http_client = DolphinSchedulerClient(profile, transport=transport)

    with http_client, pytest.raises(error_type):
        adapter.bind(profile, http_client=http_client).delete(
            project_code=7001,
            code=11,
        )
