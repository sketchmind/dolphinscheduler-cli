import pytest
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectWorkerGroup,
    FakeProjectWorkerGroupAdapter,
    fake_bound_domain_service_runtime,
    fake_project_definitions,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ConflictError,
    NotFoundError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.services import project_worker_group as project_worker_group_service
from dsctl.services import runtime as runtime_service
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.project_worker_groups import (
    PROJECT_WORKER_GROUP_DOMAIN,
    ProjectWorkerGroupDomain,
)


def _install_project_worker_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    project_worker_group_adapter: FakeProjectWorkerGroupAdapter,
    context: ResourceDefaults | None = None,
) -> None:
    def bound_runtime_factory(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        assert domain is PROJECT_WORKER_GROUP_DOMAIN
        return fake_bound_domain_service_runtime(
            ProjectWorkerGroupDomain(
                definitions=fake_project_definitions(project_adapter),
                worker_groups=project_worker_group_adapter,
            ),
            profile=make_profile(),
            context=context,
        )

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        bound_runtime_factory,
    )


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="other"),
        ]
    )


@pytest.fixture
def fake_project_worker_group_adapter() -> FakeProjectWorkerGroupAdapter:
    return FakeProjectWorkerGroupAdapter(
        project_worker_groups=[
            FakeProjectWorkerGroup(
                id=11,
                project_code_value=7,
                worker_group_value="default",
                create_time_value="2026-04-11 10:00:00",
                update_time_value="2026-04-11 10:00:00",
            ),
            FakeProjectWorkerGroup(
                id=21,
                project_code_value=8,
                worker_group_value="other",
            ),
        ]
    )


def test_list_project_worker_groups_result_uses_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_worker_group_adapter: FakeProjectWorkerGroupAdapter,
) -> None:
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=fake_project_worker_group_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = project_worker_group_service.list_project_worker_groups_result()
    items = _sequence(result.data)

    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "context",
        }
    }
    assert list(items) == [
        {
            "id": 11,
            "projectCode": 7,
            "workerGroup": "default",
            "createTime": "2026-04-11 10:00:00",
            "updateTime": "2026-04-11 10:00:00",
        }
    ]


def test_set_project_worker_groups_result_replaces_explicit_assignments(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_worker_group_adapter: FakeProjectWorkerGroupAdapter,
) -> None:
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=fake_project_worker_group_adapter,
    )

    result = project_worker_group_service.set_project_worker_groups_result(
        project="etl-prod",
        worker_groups=["gpu", " default ", "gpu"],
    )
    items = _sequence(result.data)

    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "flag",
        },
        "requested_worker_groups": ["gpu", "default"],
    }
    assert result.warnings == []
    assert result.warning_details == []
    assert list(items) == [
        {
            "id": 22,
            "projectCode": 7,
            "workerGroup": "gpu",
            "createTime": None,
            "updateTime": None,
        },
        {
            "id": 23,
            "projectCode": 7,
            "workerGroup": "default",
            "createTime": None,
            "updateTime": None,
        },
    ]


def test_set_project_worker_groups_result_rejects_empty_assignment_set(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=FakeProjectWorkerGroupAdapter(
            project_worker_groups=[]
        ),
    )

    with pytest.raises(
        UserInputError,
        match="requires at least one --worker-group",
    ) as exc_info:
        project_worker_group_service.set_project_worker_groups_result(
            project="etl-prod",
            worker_groups=[],
        )

    assert exc_info.value.suggestion == (
        "Supply at least one --worker-group. To remove all explicit "
        "assignments, check `dsctl capabilities --action "
        "project-worker-group.clear` before using "
        "`dsctl project-worker-group clear --force`."
    )


def test_clear_project_worker_groups_result_requires_force(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=FakeProjectWorkerGroupAdapter(
            project_worker_groups=[]
        ),
    )

    with pytest.raises(UserInputError, match="requires --force"):
        project_worker_group_service.clear_project_worker_groups_result(
            project="etl-prod",
            force=False,
        )


def test_clear_project_worker_groups_result_warns_when_used_groups_remain(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    adapter = FakeProjectWorkerGroupAdapter(
        project_worker_groups=[
            FakeProjectWorkerGroup(
                id=11,
                project_code_value=7,
                worker_group_value="default",
            )
        ],
        implicit_worker_groups_by_project={7: ["default", "gpu"]},
    )
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=adapter,
    )

    result = project_worker_group_service.clear_project_worker_groups_result(
        project="etl-prod",
        force=True,
    )
    items = _sequence(result.data)

    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": None,
            "source": "flag",
        },
        "requested_worker_groups": [],
    }
    assert result.warnings == [
        "Project still reports worker groups that are used by tasks or schedules."
    ]
    assert result.warning_details == [
        {
            "code": "project_worker_group_still_in_use",
            "message": (
                "Project still reports worker groups that are used by tasks or "
                "schedules."
            ),
            "requestedWorkerGroups": [],
            "retainedWorkerGroups": ["default", "gpu"],
        }
    ]
    assert list(items) == [
        {
            "id": None,
            "projectCode": 7,
            "workerGroup": "default",
            "createTime": None,
            "updateTime": None,
        },
        {
            "id": None,
            "projectCode": 7,
            "workerGroup": "gpu",
            "createTime": None,
            "updateTime": None,
        },
    ]


def test_set_project_worker_groups_result_translates_used_group_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    adapter = FakeProjectWorkerGroupAdapter(
        project_worker_groups=[],
        set_errors_by_project={
            7: ApiResultError(
                result_code=1402004,
                result_message="used worker groups [default,gpu] exist",
            )
        },
    )
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=adapter,
    )

    with pytest.raises(
        ConflictError,
        match="still used by tasks or schedules",
    ) as exc_info:
        project_worker_group_service.set_project_worker_groups_result(
            project="etl-prod",
            worker_groups=["default"],
        )

    assert exc_info.value.details["used_worker_groups"] == ["default", "gpu"]
    assert exc_info.value.suggestion == (
        "Update the worker-group selections in the project's tasks and "
        "schedules before removing those assignments."
    )


@pytest.mark.parametrize(
    ("result_code", "error_type", "message", "suggestion_fragment"),
    [
        (30001, PermissionDeniedError, "administrator account", "administrator"),
        (1402003, UnsupportedFeatureError, "cannot clear all", "nonempty assignment"),
        (1402002, ConflictError, "rejected", "list --project 7"),
    ],
)
def test_clear_project_worker_groups_result_translates_upstream_constraints(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    result_code: int,
    error_type: type[Exception],
    message: str,
    suggestion_fragment: str,
) -> None:
    upstream_error = ApiResultError(
        result_code=result_code,
        result_message="upstream assignment rejected",
    )
    adapter = FakeProjectWorkerGroupAdapter(
        project_worker_groups=[],
        set_errors_by_project={7: upstream_error},
    )
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=adapter,
    )

    with pytest.raises(error_type, match=message) as exc_info:
        project_worker_group_service.clear_project_worker_groups_result(
            project="etl-prod", force=True
        )

    error = exc_info.value
    assert isinstance(
        error, (PermissionDeniedError, UnsupportedFeatureError, ConflictError)
    )
    assert error.details["project_code"] == 7
    assert error.details["worker_groups"] == []
    assert error.suggestion is not None
    assert suggestion_fragment in error.suggestion
    assert error.__cause__ is upstream_error


def test_set_project_worker_groups_result_translates_missing_worker_group(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    adapter = FakeProjectWorkerGroupAdapter(
        project_worker_groups=[],
        set_errors_by_project={
            7: ApiResultError(
                result_code=1402001,
                result_message="worker group [missing] not exist",
            )
        },
    )
    _install_project_worker_group_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        project_worker_group_adapter=adapter,
    )

    with pytest.raises(
        NotFoundError,
        match=r"Worker group 'missing' was not found",
    ) as exc_info:
        project_worker_group_service.set_project_worker_groups_result(
            project="etl-prod",
            worker_groups=["missing"],
        )

    assert exc_info.value.details == {
        "resource": "worker-group",
        "name": "missing",
    }
