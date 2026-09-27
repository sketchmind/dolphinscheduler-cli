"""In-memory projects collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeIdentity,
    ProjectRef,
    ProjectView,
)
from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeProject:
    code: int
    name: str | None
    description: str | None = None
    id: int | None = None
    user_id_value: int | None = None
    user_name_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    perm: int = 0
    def_count_value: int = 0

    @property
    def userId(self) -> int | None:  # noqa: N802
        return self.user_id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def defCount(self) -> int:  # noqa: N802
        return self.def_count_value


@dataclass(frozen=True)
class FakeProjectPage(_FakePage[FakeProject]):
    pass


@dataclass
class FakeProjectAdapter:
    projects: list[FakeProject]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeProjectPage:
        filtered = list(self.projects)
        if search is not None:
            filtered = [
                project
                for project in filtered
                if project.name is not None and search.lower() in project.name.lower()
            ]
        return FakeProjectPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, code: int) -> FakeProject:
        for project in self.projects:
            if project.code == code:
                return project
        raise ApiResultError(
            result_code=10018,
            result_message=f"project code {code} not found",
        )

    def create(self, *, name: str, description: str | None = None) -> FakeProject:
        next_project_code = (
            max((project.code for project in self.projects), default=0) + 1
        )
        created = FakeProject(
            code=next_project_code,
            name=name,
            description=description,
            id=next_project_code,
        )
        self.projects.append(created)
        return created

    def update(
        self,
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> FakeProject:
        for index, project in enumerate(self.projects):
            if project.code == code:
                updated = replace(project, name=name, description=description)
                self.projects[index] = updated
                return updated
        raise ApiResultError(
            result_code=10018,
            result_message=f"project code {code} not found",
        )

    def delete(self, *, code: int) -> bool:
        for index, project in enumerate(self.projects):
            if project.code == code:
                self.projects.pop(index)
                return True
        raise ApiResultError(
            result_code=10018,
            result_message=f"project code {code} not found",
        )


@dataclass(frozen=True)
class FakeNativeProjectMutations:
    """Canonical native-identity facade over the legacy project test fake."""

    adapter: FakeProjectAdapter

    def create(self, *, name: str, description: str | None) -> ProjectView:
        return _fake_project_view(
            self.adapter.create(name=name, description=description)
        )

    def update(
        self,
        *,
        native: NativeIdentity,
        name: str,
        description: str | None,
    ) -> ProjectView:
        if not isinstance(native, NativeCode):
            message = "code-based project fake received a native id"
            raise TypeError(message)
        return _fake_project_view(
            self.adapter.update(
                code=native.value,
                name=name,
                description=description,
            )
        )

    def delete(self, *, native: NativeIdentity) -> bool:
        if not isinstance(native, NativeCode):
            message = "code-based project fake received a native id"
            raise TypeError(message)
        return self.adapter.delete(code=native.value)


def _fake_project_view(project: FakeProject) -> ProjectView:
    return ProjectView(
        ref=ProjectRef(
            native=NativeCode(project.code),
            name=project.name,
            description=project.description,
        ),
        id=project.id,
        user_id=project.userId,
        user_name=project.userName,
        create_time=project.createTime,
        update_time=project.updateTime,
        perm=project.perm,
        definition_count=project.defCount,
    )


@dataclass(frozen=True)
class FakeProjectParameter:
    code: int
    project_code_value: int
    param_name_value: str | None
    param_value_value: str | None = None
    param_data_type_value: str | None = None
    id: int | None = None
    user_id_value: int | None = None
    operator: int | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    create_user_value: str | None = None
    modify_user_value: str | None = None

    @property
    def userId(self) -> int | None:  # noqa: N802
        return self.user_id_value

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def paramName(self) -> str | None:  # noqa: N802
        return self.param_name_value

    @property
    def paramValue(self) -> str | None:  # noqa: N802
        return self.param_value_value

    @property
    def paramDataType(self) -> str | None:  # noqa: N802
        return self.param_data_type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def createUser(self) -> str | None:  # noqa: N802
        return self.create_user_value

    @property
    def modifyUser(self) -> str | None:  # noqa: N802
        return self.modify_user_value


@dataclass(frozen=True)
class FakeProjectParameterPage(_FakePage[FakeProjectParameter]):
    pass


@dataclass
class FakeProjectParameterAdapter:
    project_parameters: list[FakeProjectParameter]

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        data_type: str | None = None,
    ) -> FakeProjectParameterPage:
        filtered = [
            parameter
            for parameter in self.project_parameters
            if parameter.projectCode == project_code
        ]
        if search is not None:
            filtered = [
                parameter
                for parameter in filtered
                if parameter.paramName is not None
                and search.lower() in parameter.paramName.lower()
            ]
        if data_type is not None:
            filtered = [
                parameter
                for parameter in filtered
                if parameter.paramDataType == data_type
            ]
        return FakeProjectParameterPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, project_code: int, code: int) -> FakeProjectParameter:
        for parameter in self.project_parameters:
            if parameter.projectCode == project_code and parameter.code == code:
                return parameter
        raise ApiResultError(
            result_code=10219,
            result_message=f"project parameter code {code} not found",
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> FakeProjectParameter:
        for parameter in self.project_parameters:
            if parameter.projectCode == project_code and parameter.paramName == name:
                raise ApiResultError(
                    result_code=10218,
                    result_message=f"project parameter {name} already exists",
                )
        next_code = (
            max((parameter.code for parameter in self.project_parameters), default=0)
            + 1
        )
        created = FakeProjectParameter(
            code=next_code,
            project_code_value=project_code,
            param_name_value=name,
            param_value_value=value,
            param_data_type_value=data_type,
            id=next_code,
        )
        self.project_parameters.append(created)
        return created

    def update(
        self,
        *,
        project_code: int,
        code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> FakeProjectParameter:
        for parameter in self.project_parameters:
            if (
                parameter.projectCode == project_code
                and parameter.code != code
                and parameter.paramName == name
            ):
                raise ApiResultError(
                    result_code=10218,
                    result_message=f"project parameter {name} already exists",
                )
        for index, parameter in enumerate(self.project_parameters):
            if parameter.projectCode == project_code and parameter.code == code:
                updated = replace(
                    parameter,
                    param_name_value=name,
                    param_value_value=value,
                    param_data_type_value=data_type,
                )
                self.project_parameters[index] = updated
                return updated
        raise ApiResultError(
            result_code=10219,
            result_message=f"project parameter code {code} not found",
        )

    def delete(self, *, project_code: int, code: int) -> bool:
        for index, parameter in enumerate(self.project_parameters):
            if parameter.projectCode == project_code and parameter.code == code:
                self.project_parameters.pop(index)
                return True
        raise ApiResultError(
            result_code=10219,
            result_message=f"project parameter code {code} not found",
        )


@dataclass(frozen=True)
class FakeProjectPreference:
    code: int
    project_code_value: int
    state: int
    preferences_value: str | None = None
    id: int | None = None
    user_id_value: int | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def preferences(self) -> str | None:
        return self.preferences_value

    @property
    def userId(self) -> int | None:  # noqa: N802
        return self.user_id_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass
class FakeProjectPreferenceAdapter:
    project_preferences: list[FakeProjectPreference]
    state_updates: list[tuple[int, int]] = field(default_factory=list)

    def get(self, *, project_code: int) -> FakeProjectPreference | None:
        for project_preference in self.project_preferences:
            if project_preference.projectCode == project_code:
                return project_preference
        return None

    def update(
        self,
        *,
        project_code: int,
        preferences: str,
    ) -> FakeProjectPreference:
        for index, project_preference in enumerate(self.project_preferences):
            if project_preference.projectCode == project_code:
                updated = replace(
                    project_preference,
                    preferences_value=preferences,
                    state=1,
                )
                self.project_preferences[index] = updated
                return updated
        next_code = (
            max(
                (
                    project_preference.code
                    for project_preference in self.project_preferences
                ),
                default=0,
            )
            + 1
        )
        created = FakeProjectPreference(
            id=next_code,
            code=next_code,
            project_code_value=project_code,
            preferences_value=preferences,
            state=1,
        )
        self.project_preferences.append(created)
        return created

    def set_state(self, *, project_code: int, state: int) -> None:
        self.state_updates.append((project_code, state))
        for index, project_preference in enumerate(self.project_preferences):
            if project_preference.projectCode == project_code:
                self.project_preferences[index] = replace(
                    project_preference,
                    state=state,
                )
                return


@dataclass(frozen=True)
class FakeProjectWorkerGroup:
    project_code_value: int
    worker_group_value: str | None
    id: int | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass
class FakeProjectWorkerGroupAdapter:
    project_worker_groups: list[FakeProjectWorkerGroup]
    implicit_worker_groups_by_project: dict[int, list[str]] = field(
        default_factory=dict
    )
    set_errors_by_project: dict[int, ApiResultError] = field(default_factory=dict)

    def list(self, *, project_code: int) -> list[FakeProjectWorkerGroup]:
        explicit = [
            worker_group
            for worker_group in self.project_worker_groups
            if worker_group.projectCode == project_code
        ]
        explicit_names = {
            worker_group.workerGroup
            for worker_group in explicit
            if worker_group.workerGroup is not None
        }
        implicit = [
            FakeProjectWorkerGroup(
                project_code_value=project_code,
                worker_group_value=worker_group,
            )
            for worker_group in self.implicit_worker_groups_by_project.get(
                project_code,
                [],
            )
            if worker_group not in explicit_names
        ]
        return [*explicit, *implicit]

    def set(self, *, project_code: int, worker_groups: Sequence[str]) -> None:
        error = self.set_errors_by_project.get(project_code)
        if error is not None:
            raise error
        self.project_worker_groups = [
            worker_group
            for worker_group in self.project_worker_groups
            if worker_group.projectCode != project_code
        ]
        next_id = max(
            (worker_group.id or 0 for worker_group in self.project_worker_groups),
            default=0,
        )
        for offset, worker_group in enumerate(worker_groups, start=1):
            self.project_worker_groups.append(
                FakeProjectWorkerGroup(
                    id=next_id + offset,
                    project_code_value=project_code,
                    worker_group_value=worker_group,
                )
            )


def empty_project_parameter_adapter() -> FakeProjectParameterAdapter:
    return FakeProjectParameterAdapter(project_parameters=[])


def empty_project_preference_adapter() -> FakeProjectPreferenceAdapter:
    return FakeProjectPreferenceAdapter(project_preferences=[])


def empty_project_worker_group_adapter() -> FakeProjectWorkerGroupAdapter:
    return FakeProjectWorkerGroupAdapter(project_worker_groups=[])
