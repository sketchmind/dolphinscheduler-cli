from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.client import BinaryResponse
from dsctl.context import SessionContext
from dsctl.errors import ApiResultError
from dsctl.services.runtime import ServiceRuntime
from dsctl.support.json_types import is_json_value

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue


@dataclass(frozen=True)
class FakeEnumValue:
    value: str


@dataclass(frozen=True)
class FakeHttpClient:
    health_payload: JsonObject = field(default_factory=lambda: {"status": "UP"})

    def healthcheck(self) -> JsonObject:
        return dict(self.health_payload)


@dataclass(frozen=True)
class FakeTaskType:
    task_type_value: str | None
    is_collection_value: bool = False
    task_category_value: str | None = None

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def isCollection(self) -> bool:  # noqa: N802
        return self.is_collection_value

    @property
    def taskCategory(self) -> str | None:  # noqa: N802
        return self.task_category_value


@dataclass
class FakeTaskTypeAdapter:
    task_types: list[FakeTaskType]

    def list(self) -> list[FakeTaskType]:
        return list(self.task_types)


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
class FakeProjectPage:
    total_list_value: list[FakeProject] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeProject] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


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
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeProjectPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
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
class FakeProjectParameterPage:
    total_list_value: list[FakeProjectParameter] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeProjectParameter] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


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
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeProjectParameterPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
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


@dataclass(frozen=True)
class FakeEnvironment:
    code: int
    name: str | None
    description: str | None = None
    id: int | None = None
    config: str | None = None
    worker_groups_value: list[str] | None = None
    operator_value: int | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def workerGroups(self) -> list[str] | None:  # noqa: N802
        return self.worker_groups_value

    @property
    def operator(self) -> int | None:
        return self.operator_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeEnvironmentPage:
    total_list_value: list[FakeEnvironment] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeEnvironment] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeEnvironmentAdapter:
    environments: list[FakeEnvironment]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeEnvironmentPage:
        filtered = list(self.environments)
        if search is not None:
            filtered = [
                environment
                for environment in filtered
                if environment.name is not None
                and search.lower() in environment.name.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeEnvironmentPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeEnvironment]:
        return list(self.environments)

    def get(self, *, code: int) -> FakeEnvironment:
        for environment in self.environments:
            if environment.code == code:
                return environment
        raise ApiResultError(
            result_code=10211,
            result_message=f"environment code {code} not found",
        )

    def delete(self, *, code: int) -> bool:
        for index, environment in enumerate(self.environments):
            if environment.code == code:
                self.environments.pop(index)
                return True
        raise ApiResultError(
            result_code=10211,
            result_message=f"environment code {code} not found",
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str] | None = None,
    ) -> FakeEnvironment:
        for environment in self.environments:
            if environment.name == name:
                raise ApiResultError(
                    result_code=120002,
                    result_message=f"this environment name [{name}] already exists",
                )
        next_environment_code = (
            max((environment.code for environment in self.environments), default=0) + 1
        )
        created = FakeEnvironment(
            code=next_environment_code,
            name=name,
            config=config,
            description=description,
            worker_groups_value=(
                None if worker_groups is None else list(worker_groups)
            ),
            id=next_environment_code,
        )
        self.environments.append(created)
        return created

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str],
    ) -> FakeEnvironment:
        for environment in self.environments:
            if environment.name == name and environment.code != code:
                raise ApiResultError(
                    result_code=120002,
                    result_message=f"this environment name [{name}] already exists",
                )
        for index, environment in enumerate(self.environments):
            if environment.code == code:
                updated = replace(
                    environment,
                    name=name,
                    config=config,
                    description=description,
                    worker_groups_value=list(worker_groups),
                )
                self.environments[index] = updated
                return updated
        raise ApiResultError(
            result_code=10211,
            result_message=f"environment code {code} not found",
        )


@dataclass(frozen=True)
class FakeDataSource:
    id: int
    name: str | None
    note: str | None = None
    type_value: FakeEnumValue | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    detail_payload_value: dict[str, object] | None = None

    @property
    def type(self) -> FakeEnumValue | None:
        return self.type_value

    @property
    def userId(self) -> int:  # noqa: N802
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

    def detail_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "id": self.id,
            "name": self.name,
            "note": self.note,
            "type": None if self.type is None else self.type.value,
        }
        if self.detail_payload_value is not None:
            payload.update(self.detail_payload_value)
        return payload


@dataclass(frozen=True)
class FakeDataSourcePage:
    total_list_value: list[FakeDataSource] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeDataSource] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeDataSourceAdapter:
    datasources: list[FakeDataSource]
    connection_test_results: dict[int, bool] = field(default_factory=dict)
    authorized_by_user_id: dict[int, set[int]] = field(default_factory=dict)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeDataSourcePage:
        filtered = list(self.datasources)
        if search is not None:
            filtered = [
                datasource
                for datasource in filtered
                if datasource.name is not None
                and search.lower() in datasource.name.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeDataSourcePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, datasource_id: int) -> dict[str, object]:
        for datasource in self.datasources:
            if datasource.id == datasource_id:
                return datasource.detail_payload()
        raise ApiResultError(
            result_code=20004,
            result_message=f"datasource id {datasource_id} not found",
        )

    def authorized_for_user(self, *, user_id: int) -> Sequence[FakeDataSource]:
        authorized_ids = self.authorized_by_user_id.get(user_id, set())
        return [
            datasource
            for datasource in self.datasources
            if datasource.id in authorized_ids
        ]

    def create(self, *, payload_json: str) -> FakeDataSource:
        payload = json.loads(payload_json)
        assert isinstance(payload, dict)
        name = payload.get("name")
        datasource_type = payload.get("type")
        if not isinstance(name, str) or not isinstance(datasource_type, str):
            raise ApiResultError(
                result_code=10033,
                result_message="create datasource error",
            )
        for datasource in self.datasources:
            if datasource.name == name:
                raise ApiResultError(
                    result_code=10015,
                    result_message="data source name already exists",
                )
        next_id = max((datasource.id for datasource in self.datasources), default=0) + 1
        created = FakeDataSource(
            id=next_id,
            name=name,
            note=payload.get("note") if isinstance(payload.get("note"), str) else None,
            type_value=FakeEnumValue(datasource_type),
            detail_payload_value={"password": payload.get("password", "")},
        )
        self.datasources.append(created)
        return created

    def update(self, *, datasource_id: int, payload_json: str) -> FakeDataSource:
        payload = json.loads(payload_json)
        assert isinstance(payload, dict)
        name = payload.get("name")
        datasource_type = payload.get("type")
        if not isinstance(name, str) or not isinstance(datasource_type, str):
            raise ApiResultError(
                result_code=10034,
                result_message="update datasource error",
            )
        for index, datasource in enumerate(self.datasources):
            if datasource.id == datasource_id:
                updated = replace(
                    datasource,
                    name=name,
                    note=(
                        payload.get("note")
                        if isinstance(payload.get("note"), str)
                        else None
                    ),
                    type_value=FakeEnumValue(datasource_type),
                    detail_payload_value=dict(payload),
                )
                self.datasources[index] = updated
                return updated
        raise ApiResultError(
            result_code=20004,
            result_message=f"datasource id {datasource_id} not found",
        )

    def delete(self, *, datasource_id: int) -> bool:
        for index, datasource in enumerate(self.datasources):
            if datasource.id == datasource_id:
                self.datasources.pop(index)
                return True
        raise ApiResultError(
            result_code=20004,
            result_message=f"datasource id {datasource_id} not found",
        )

    def connection_test(self, *, datasource_id: int) -> bool:
        for datasource in self.datasources:
            if datasource.id == datasource_id:
                return self.connection_test_results.get(datasource_id, True)
        raise ApiResultError(
            result_code=20004,
            result_message=f"datasource id {datasource_id} not found",
        )


@dataclass(frozen=True)
class FakeNamespace:
    id: int
    namespace_value: str | None
    code: int | None = None
    cluster_code_value: int | None = None
    cluster_name_value: str | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def namespace(self) -> str | None:
        return self.namespace_value

    @property
    def clusterCode(self) -> int | None:  # noqa: N802
        return self.cluster_code_value

    @property
    def clusterName(self) -> str | None:  # noqa: N802
        return self.cluster_name_value

    @property
    def userId(self) -> int:  # noqa: N802
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


@dataclass(frozen=True)
class FakeNamespacePage:
    total_list_value: list[FakeNamespace] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeNamespace] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeNamespaceAdapter:
    namespaces: list[FakeNamespace]
    authorized_by_user_id: dict[int, set[int]] = field(default_factory=dict)
    available_ids: set[int] | None = None
    create_error: ApiResultError | None = None
    delete_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeNamespacePage:
        filtered = list(self.namespaces)
        if search is not None:
            filtered = [
                namespace
                for namespace in filtered
                if namespace.namespace is not None
                and search.lower() in namespace.namespace.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeNamespacePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def create(
        self,
        *,
        namespace: str,
        cluster_code: int,
    ) -> FakeNamespace:
        if self.create_error is not None:
            raise self.create_error
        for existing in self.namespaces:
            if existing.namespace == namespace and existing.clusterCode == cluster_code:
                raise ApiResultError(
                    result_code=1300002,
                    result_message=f"k8s namespace {namespace} already exists",
                )
        next_id = max((item.id for item in self.namespaces), default=0) + 1
        next_code = max((item.code or 0 for item in self.namespaces), default=0) + 1
        created = FakeNamespace(
            id=next_id,
            code=next_code,
            namespace_value=namespace,
            cluster_code_value=cluster_code,
        )
        self.namespaces.append(created)
        return created

    def available(self) -> Sequence[FakeNamespace]:
        if self.available_ids is None:
            return list(self.namespaces)
        return [
            namespace
            for namespace in self.namespaces
            if namespace.id in self.available_ids
        ]

    def delete(self, *, namespace_id: int) -> bool:
        if namespace_id in self.delete_errors_by_id:
            raise self.delete_errors_by_id[namespace_id]
        for index, namespace in enumerate(self.namespaces):
            if namespace.id == namespace_id:
                self.namespaces.pop(index)
                for authorized_ids in self.authorized_by_user_id.values():
                    authorized_ids.discard(namespace_id)
                return True
        raise ApiResultError(
            result_code=1300005,
            result_message=f"k8s namespace {namespace_id} not exists",
        )

    def authorized_for_user(self, *, user_id: int) -> Sequence[FakeNamespace]:
        authorized_ids = self.authorized_by_user_id.get(user_id, set())
        return [
            namespace for namespace in self.namespaces if namespace.id in authorized_ids
        ]


@dataclass(frozen=True)
class FakePluginDefine:
    id: int | None
    plugin_name_value: str | None
    plugin_type_value: str | None = None
    plugin_params_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def pluginName(self) -> str | None:  # noqa: N802
        return self.plugin_name_value

    @property
    def pluginType(self) -> str | None:  # noqa: N802
        return self.plugin_type_value

    @property
    def pluginParams(self) -> str | None:  # noqa: N802
        return self.plugin_params_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass
class FakeUiPluginAdapter:
    plugin_defines: list[FakePluginDefine]

    def list(self, *, plugin_type: str) -> Sequence[FakePluginDefine]:
        return [
            plugin_define
            for plugin_define in self.plugin_defines
            if plugin_define.pluginType == plugin_type
        ]

    def get(self, *, plugin_id: int) -> FakePluginDefine:
        for plugin_define in self.plugin_defines:
            if plugin_define.id == plugin_id:
                return plugin_define
        raise ApiResultError(
            result_code=110003,
            result_message=f"alert plugin define id {plugin_id} not found",
        )


@dataclass(frozen=True)
class FakeAlertPlugin:
    id: int
    plugin_define_id_value: int
    instance_name_value: str | None
    plugin_instance_params_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    instance_type_value: str | None = None
    warning_type_value: str | None = None
    alert_plugin_name_value: str | None = None

    @property
    def pluginDefineId(self) -> int:  # noqa: N802
        return self.plugin_define_id_value

    @property
    def instanceName(self) -> str | None:  # noqa: N802
        return self.instance_name_value

    @property
    def pluginInstanceParams(self) -> str | None:  # noqa: N802
        return self.plugin_instance_params_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def instanceType(self) -> str | None:  # noqa: N802
        return self.instance_type_value

    @property
    def warningType(self) -> str | None:  # noqa: N802
        return self.warning_type_value

    @property
    def alertPluginName(self) -> str | None:  # noqa: N802
        return self.alert_plugin_name_value


@dataclass(frozen=True)
class FakeAlertPluginPage:
    total_list_value: list[FakeAlertPlugin] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeAlertPlugin] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeAlertPluginAdapter:
    alert_plugins: list[FakeAlertPlugin]
    plugin_names_by_id: dict[int, str] = field(default_factory=dict)
    test_send_results_by_plugin_define_id: dict[int, bool] = field(default_factory=dict)
    delete_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)
    test_send_errors_by_plugin_define_id: dict[int, ApiResultError] = field(
        default_factory=dict
    )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeAlertPluginPage:
        filtered = list(self.alert_plugins)
        if search is not None:
            filtered = [
                alert_plugin
                for alert_plugin in filtered
                if alert_plugin.instanceName is not None
                and search.lower() in alert_plugin.instanceName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeAlertPluginPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeAlertPlugin]:
        return list(self.alert_plugins)

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> FakeAlertPlugin:
        for alert_plugin in self.alert_plugins:
            if alert_plugin.instanceName == instance_name:
                raise ApiResultError(
                    result_code=110010,
                    result_message=f"alert plugin {instance_name} already exists",
                )
        next_id = max((item.id for item in self.alert_plugins), default=0) + 1
        created = FakeAlertPlugin(
            id=next_id,
            plugin_define_id_value=plugin_define_id,
            instance_name_value=instance_name,
            plugin_instance_params_value=plugin_instance_params,
            instance_type_value="ALERT",
            warning_type_value="ALL",
            alert_plugin_name_value=self.plugin_names_by_id.get(plugin_define_id),
        )
        self.alert_plugins.append(created)
        return created

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> FakeAlertPlugin:
        for alert_plugin in self.alert_plugins:
            if (
                alert_plugin.id != alert_plugin_id
                and alert_plugin.instanceName == instance_name
            ):
                raise ApiResultError(
                    result_code=110010,
                    result_message=f"alert plugin {instance_name} already exists",
                )
        for index, alert_plugin in enumerate(self.alert_plugins):
            if alert_plugin.id == alert_plugin_id:
                updated = replace(
                    alert_plugin,
                    instance_name_value=instance_name,
                    plugin_instance_params_value=plugin_instance_params,
                )
                self.alert_plugins[index] = updated
                return updated
        raise ApiResultError(
            result_code=110007,
            result_message=f"alert plugin id {alert_plugin_id} not found",
        )

    def delete(self, *, alert_plugin_id: int) -> bool:
        if alert_plugin_id in self.delete_errors_by_id:
            raise self.delete_errors_by_id[alert_plugin_id]
        for index, alert_plugin in enumerate(self.alert_plugins):
            if alert_plugin.id == alert_plugin_id:
                self.alert_plugins.pop(index)
                return True
        raise ApiResultError(
            result_code=110006,
            result_message=f"alert plugin id {alert_plugin_id} not found",
        )

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        del plugin_instance_params
        if plugin_define_id in self.test_send_errors_by_plugin_define_id:
            raise self.test_send_errors_by_plugin_define_id[plugin_define_id]
        return self.test_send_results_by_plugin_define_id.get(plugin_define_id, True)


@dataclass(frozen=True)
class FakeAudit:
    user_name_value: str | None
    model_type_value: str | None
    model_name_value: str | None
    operation_value: str | None
    create_time_value: str | None = None
    description_value: str | None = None
    detail_value: str | None = None
    latency_value: str | None = None

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def modelType(self) -> str | None:  # noqa: N802
        return self.model_type_value

    @property
    def modelName(self) -> str | None:  # noqa: N802
        return self.model_name_value

    @property
    def operation(self) -> str | None:
        return self.operation_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def description(self) -> str | None:
        return self.description_value

    @property
    def detail(self) -> str | None:
        return self.detail_value

    @property
    def latency(self) -> str | None:
        return self.latency_value


@dataclass(frozen=True)
class FakeAuditPage:
    total_list_value: list[FakeAudit] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeAudit] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass(frozen=True)
class FakeAuditModelType:
    name: str | None
    child: list[FakeAuditModelType] | None = None


@dataclass(frozen=True)
class FakeAuditOperationType:
    name: str | None


@dataclass
class FakeAuditAdapter:
    audit_logs: list[FakeAudit]
    model_types: list[FakeAuditModelType] = field(default_factory=list)
    operation_types: list[FakeAuditOperationType] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        model_types: Sequence[str] | None = None,
        operation_types: Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        user_name: str | None = None,
        model_name: str | None = None,
    ) -> FakeAuditPage:
        filtered = list(self.audit_logs)
        if model_types is not None:
            allowed_model_types = set(model_types)
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.modelType in allowed_model_types
            ]
        if operation_types is not None:
            allowed_operation_types = set(operation_types)
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.operation in allowed_operation_types
            ]
        if start_date is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.createTime is not None
                and audit_log.createTime >= start_date
            ]
        if end_date is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.createTime is not None and audit_log.createTime <= end_date
            ]
        if user_name is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.userName is not None
                and user_name.lower() in audit_log.userName.lower()
            ]
        if model_name is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.modelName is not None
                and model_name.lower() in audit_log.modelName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeAuditPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_model_types(self) -> Sequence[FakeAuditModelType]:
        return list(self.model_types)

    def list_operation_types(self) -> Sequence[FakeAuditOperationType]:
        return list(self.operation_types)


@dataclass(frozen=True)
class FakeAlertGroup:
    id: int
    group_name_value: str | None
    alert_instance_ids_value: str | None = None
    description: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    create_user_id_value: int = 0

    @property
    def groupName(self) -> str | None:  # noqa: N802
        return self.group_name_value

    @property
    def alertInstanceIds(self) -> str | None:  # noqa: N802
        return self.alert_instance_ids_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def createUserId(self) -> int:  # noqa: N802
        return self.create_user_id_value


@dataclass(frozen=True)
class FakeAlertGroupPage:
    total_list_value: list[FakeAlertGroup] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeAlertGroup] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeAlertGroupAdapter:
    alert_groups: list[FakeAlertGroup]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeAlertGroupPage:
        filtered = list(self.alert_groups)
        if search is not None:
            filtered = [
                alert_group
                for alert_group in filtered
                if alert_group.groupName is not None
                and search.lower() in alert_group.groupName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeAlertGroupPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, alert_group_id: int) -> FakeAlertGroup:
        for alert_group in self.alert_groups:
            if alert_group.id == alert_group_id:
                return alert_group
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )

    def create(
        self,
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> FakeAlertGroup:
        for alert_group in self.alert_groups:
            if alert_group.groupName == group_name:
                raise ApiResultError(
                    result_code=10012,
                    result_message="alarm group already exists",
                )
        next_id = max((item.id for item in self.alert_groups), default=0) + 1
        created = FakeAlertGroup(
            id=next_id,
            group_name_value=group_name,
            alert_instance_ids_value=alert_instance_ids,
            description=description,
            create_user_id_value=1,
        )
        self.alert_groups.append(created)
        return created

    def update(
        self,
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str,
    ) -> FakeAlertGroup:
        for other in self.alert_groups:
            if other.id != alert_group_id and other.groupName == group_name:
                raise ApiResultError(
                    result_code=10012,
                    result_message="alarm group already exists",
                )
        for index, alert_group in enumerate(self.alert_groups):
            if alert_group.id == alert_group_id:
                updated = replace(
                    alert_group,
                    group_name_value=group_name,
                    alert_instance_ids_value=alert_instance_ids,
                    description=description,
                    create_user_id_value=1,
                )
                self.alert_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )

    def delete(self, *, alert_group_id: int) -> bool:
        if alert_group_id == 1:
            raise ApiResultError(
                result_code=130030,
                result_message="Not allow to delete the default alarm group",
            )
        for index, alert_group in enumerate(self.alert_groups):
            if alert_group.id == alert_group_id:
                self.alert_groups.pop(index)
                return True
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )


@dataclass(frozen=True)
class FakeQueue:
    id: int
    queue_name_value: str | None
    queue: str | None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def queueName(self) -> str | None:  # noqa: N802
        return self.queue_name_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeQueuePage:
    total_list_value: list[FakeQueue] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeQueue] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeQueueAdapter:
    queues: list[FakeQueue]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeQueuePage:
        filtered = list(self.queues)
        if search is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.queueName is not None
                and search.lower() in queue.queueName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeQueuePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeQueue]:
        return list(self.queues)

    def get(self, *, queue_id: int) -> FakeQueue:
        for queue in self.queues:
            if queue.id == queue_id:
                return queue
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )

    def create(self, *, queue: str, queue_name: str) -> FakeQueue:
        for existing in self.queues:
            if existing.queue == queue:
                raise ApiResultError(
                    result_code=10129,
                    result_message=f"queue value {queue} already exists",
                )
            if existing.queueName == queue_name:
                raise ApiResultError(
                    result_code=10130,
                    result_message=f"queue name {queue_name} already exists",
                )
        next_id = max((item.id for item in self.queues), default=0) + 1
        created = FakeQueue(
            id=next_id,
            queue_name_value=queue_name,
            queue=queue,
        )
        self.queues.append(created)
        return created

    def update(self, *, queue_id: int, queue: str, queue_name: str) -> FakeQueue:
        for existing in self.queues:
            if existing.id == queue_id:
                continue
            if existing.queue == queue:
                raise ApiResultError(
                    result_code=10129,
                    result_message=f"queue value {queue} already exists",
                )
            if existing.queueName == queue_name:
                raise ApiResultError(
                    result_code=10130,
                    result_message=f"queue name {queue_name} already exists",
                )
        for index, existing in enumerate(self.queues):
            if existing.id == queue_id:
                updated = replace(
                    existing,
                    queue_name_value=queue_name,
                    queue=queue,
                )
                self.queues[index] = updated
                return updated
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )

    def delete(self, *, queue_id: int) -> bool:
        for index, queue in enumerate(self.queues):
            if queue.id == queue_id:
                self.queues.pop(index)
                return True
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )


@dataclass(frozen=True)
class FakeTaskGroup:
    id: int | None
    name: str | None
    project_code_value: int
    description: str | None = None
    group_size_value: int = 1
    use_size_value: int = 0
    user_id_value: int = 1
    status_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def groupSize(self) -> int:  # noqa: N802
        return self.group_size_value

    @property
    def useSize(self) -> int:  # noqa: N802
        return self.use_size_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def status(self) -> FakeEnumValue | None:
        return self.status_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeTaskGroupPage:
    total_list_value: list[FakeTaskGroup] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeTaskGroup] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass(frozen=True)
class FakeTaskGroupQueue:
    id: int | None
    task_id_value: int
    task_name_value: str | None
    project_name_value: str | None
    project_code_value: str | None
    workflow_instance_name_value: str | None
    group_id_value: int
    workflow_instance_id_value: int | None = None
    priority: int = 0
    force_start_value: int = 0
    in_queue_value: int = 1
    status_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def taskId(self) -> int:  # noqa: N802
        return self.task_id_value

    @property
    def taskName(self) -> str | None:  # noqa: N802
        return self.task_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def projectCode(self) -> str | None:  # noqa: N802
        return self.project_code_value

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        return self.workflow_instance_name_value

    @property
    def groupId(self) -> int:  # noqa: N802
        return self.group_id_value

    @property
    def workflowInstanceId(self) -> int | None:  # noqa: N802
        return self.workflow_instance_id_value

    @property
    def forceStart(self) -> int:  # noqa: N802
        return self.force_start_value

    @property
    def inQueue(self) -> int:  # noqa: N802
        return self.in_queue_value

    @property
    def status(self) -> FakeEnumValue | None:
        return self.status_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeTaskGroupQueuePage:
    total_list_value: list[FakeTaskGroupQueue] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeTaskGroupQueue] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeTaskGroupAdapter:
    task_groups: list[FakeTaskGroup]
    task_group_queues: list[FakeTaskGroupQueue] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
        status: int | None = None,
    ) -> FakeTaskGroupPage:
        filtered = list(self.task_groups)
        if search is not None:
            filtered = [
                task_group
                for task_group in filtered
                if task_group.name is not None
                and search.lower() in task_group.name.lower()
            ]
        if status is not None:
            filtered = [
                task_group
                for task_group in filtered
                if _task_group_status_code(task_group.status) == status
            ]
        return _task_group_page(filtered, page_no=page_no, page_size=page_size)

    def list_by_project(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
    ) -> FakeTaskGroupPage:
        filtered = [
            task_group
            for task_group in self.task_groups
            if task_group.projectCode == project_code
        ]
        return _task_group_page(filtered, page_no=page_no, page_size=page_size)

    def list_all(self) -> Sequence[FakeTaskGroup]:
        return list(self.task_groups)

    def get(self, *, task_group_id: int) -> FakeTaskGroup:
        for task_group in self.task_groups:
            if task_group.id == task_group_id:
                return task_group
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> FakeTaskGroup:
        if group_size < 1:
            raise ApiResultError(
                result_code=130002,
                result_message="task group size error",
            )
        for existing in self.task_groups:
            if existing.name == name:
                raise ApiResultError(
                    result_code=130001,
                    result_message=f"task group {name} already exists",
                )
        next_id = max((item.id or 0 for item in self.task_groups), default=0) + 1
        created = FakeTaskGroup(
            id=next_id,
            name=name,
            project_code_value=project_code,
            description=description,
            group_size_value=group_size,
            status_value=FakeEnumValue("YES"),
        )
        self.task_groups.append(created)
        return created

    def update(
        self,
        *,
        task_group_id: int,
        name: str,
        description: str,
        group_size: int,
    ) -> FakeTaskGroup:
        if group_size < 1:
            raise ApiResultError(
                result_code=130002,
                result_message="task group size error",
            )
        for existing in self.task_groups:
            if existing.id == task_group_id:
                continue
            if existing.name == name:
                raise ApiResultError(
                    result_code=130001,
                    result_message=f"task group {name} already exists",
                )
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.status is not None and existing.status.value != "YES":
                    raise ApiResultError(
                        result_code=130003,
                        result_message="task group status error",
                    )
                updated = replace(
                    existing,
                    name=name,
                    description=description,
                    group_size_value=group_size,
                )
                self.task_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def close(self, *, task_group_id: int) -> None:
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.status is not None and existing.status.value == "NO":
                    raise ApiResultError(
                        result_code=130018,
                        result_message="task group already closed",
                    )
                self.task_groups[index] = replace(
                    existing,
                    status_value=FakeEnumValue("NO"),
                )
                return
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def start(self, *, task_group_id: int) -> None:
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.status is not None and existing.status.value == "YES":
                    raise ApiResultError(
                        result_code=130019,
                        result_message="task group already opened",
                    )
                self.task_groups[index] = replace(
                    existing,
                    status_value=FakeEnumValue("YES"),
                )
                return
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def list_queues(
        self,
        *,
        group_id: int,
        page_no: int,
        page_size: int,
        task_instance_name: str | None = None,
        workflow_instance_name: str | None = None,
        status: int | None = None,
    ) -> FakeTaskGroupQueuePage:
        filtered = [
            queue for queue in self.task_group_queues if queue.groupId == group_id
        ]
        if task_instance_name is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.taskName is not None
                and task_instance_name.lower() in queue.taskName.lower()
            ]
        if workflow_instance_name is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.workflowInstanceName is not None
                and workflow_instance_name.lower() in queue.workflowInstanceName.lower()
            ]
        if status is not None:
            filtered = [
                queue
                for queue in filtered
                if _task_group_queue_status_code(queue.status) == status
            ]
        return _task_group_queue_page(filtered, page_no=page_no, page_size=page_size)

    def force_start(self, *, queue_id: int) -> None:
        for index, queue in enumerate(self.task_group_queues):
            if queue.id == queue_id:
                if queue.forceStart == 1:
                    raise ApiResultError(
                        result_code=130017,
                        result_message="task group queue already start",
                    )
                self.task_group_queues[index] = replace(queue, force_start_value=1)
                return
        raise ApiResultError(
            result_code=130013,
            result_message=f"task group queue {queue_id} not found",
        )

    def set_queue_priority(self, *, queue_id: int, priority: int) -> None:
        for index, queue in enumerate(self.task_group_queues):
            if queue.id == queue_id:
                self.task_group_queues[index] = replace(queue, priority=priority)
                return
        raise ApiResultError(
            result_code=130013,
            result_message=f"task group queue {queue_id} not found",
        )


def _task_group_page(
    task_groups: list[FakeTaskGroup],
    *,
    page_no: int,
    page_size: int,
) -> FakeTaskGroupPage:
    start = (page_no - 1) * page_size
    stop = start + page_size
    total = len(task_groups)
    total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
    return FakeTaskGroupPage(
        total_list_value=task_groups[start:stop],
        total=total,
        total_page_value=total_pages,
        page_size_value=page_size,
        current_page_value=page_no,
        page_no_value=page_no,
    )


def _task_group_queue_page(
    queues: list[FakeTaskGroupQueue],
    *,
    page_no: int,
    page_size: int,
) -> FakeTaskGroupQueuePage:
    start = (page_no - 1) * page_size
    stop = start + page_size
    total = len(queues)
    total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
    return FakeTaskGroupQueuePage(
        total_list_value=queues[start:stop],
        total=total,
        total_page_value=total_pages,
        page_size_value=page_size,
        current_page_value=page_no,
        page_no_value=page_no,
    )


def _task_group_status_code(status: FakeEnumValue | None) -> int | None:
    if status is None:
        return None
    if status.value == "YES":
        return 1
    if status.value == "NO":
        return 0
    return None


def _task_group_queue_status_code(status: FakeEnumValue | None) -> int | None:
    if status is None:
        return None
    if status.value == "WAIT_QUEUE":
        return -1
    if status.value == "ACQUIRE_SUCCESS":
        return 1
    if status.value == "RELEASE":
        return 2
    return None


@dataclass(frozen=True)
class FakeResourceItem:
    alias: str | None
    full_name_value: str | None
    is_directory_value: bool
    size: int = 0
    user_name_value: str | None = None
    file_name_value: str | None = None
    type_value: FakeEnumValue | None = field(
        default_factory=lambda: FakeEnumValue("FILE")
    )
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def fileName(self) -> str | None:  # noqa: N802
        return self.file_name_value

    @property
    def fullName(self) -> str | None:  # noqa: N802
        return self.full_name_value

    @property
    def isDirectory(self) -> bool:  # noqa: N802
        return self.is_directory_value

    @property
    def type(self) -> FakeEnumValue | None:
        return self.type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeResourcePage:
    total_list_value: list[FakeResourceItem] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeResourceItem] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass(frozen=True)
class FakeResourceContent:
    content: str | None


@dataclass
class FakeResourceAdapter:
    resources: list[FakeResourceItem]
    contents_by_full_name: dict[str, bytes] = field(default_factory=dict)
    base_dir_value: str = "/tenant/resources"

    def base_dir(self) -> str:
        return self.base_dir_value

    def list(
        self,
        *,
        directory: str,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeResourcePage:
        normalized_directory = _normalize_resource_path(directory)
        filtered = [
            resource
            for resource in self.resources
            if _resource_parent(resource.fullName) == normalized_directory
        ]
        if search is not None:
            needle = search.lower()
            filtered = [
                resource
                for resource in filtered
                if resource.fileName is not None and needle in resource.fileName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeResourcePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def view(
        self,
        *,
        full_name: str,
        skip_line_num: int,
        limit: int,
    ) -> FakeResourceContent:
        item = self._require_item(full_name)
        if item.isDirectory:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        content_bytes = self.contents_by_full_name.get(
            _normalize_resource_path(full_name)
        )
        if content_bytes is None:
            raise ApiResultError(
                result_code=20012,
                result_message=f"resource file {full_name} not found",
            )
        text = content_bytes.decode("utf-8", errors="replace")
        lines = text.splitlines()
        if not lines:
            return FakeResourceContent(content=text)
        selected = lines[skip_line_num : skip_line_num + limit]
        return FakeResourceContent(content="\n".join(selected))

    def upload(
        self,
        *,
        current_dir: str,
        name: str,
        file: object,
    ) -> None:
        if not hasattr(file, "read"):
            message = "upload file must provide read()"
            raise TypeError(message)
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, name)
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20011,
                result_message=f"resource file {full_name} already exists",
            )
        file_bytes = file.read()
        if not isinstance(file_bytes, bytes):
            message = "upload file read() must return bytes"
            raise TypeError(message)
        self.resources.append(
            FakeResourceItem(
                alias=name,
                file_name_value=name,
                full_name_value=full_name,
                is_directory_value=False,
                size=len(file_bytes),
            )
        )
        self.contents_by_full_name[_normalize_resource_path(full_name)] = file_bytes

    def create_from_content(
        self,
        *,
        current_dir: str,
        file_name: str,
        suffix: str,
        content: str,
    ) -> None:
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, f"{file_name}.{suffix}")
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20011,
                result_message=f"resource file {full_name} already exists",
            )
        self.resources.append(
            FakeResourceItem(
                alias=f"{file_name}.{suffix}",
                file_name_value=f"{file_name}.{suffix}",
                full_name_value=full_name,
                is_directory_value=False,
                size=len(content.encode("utf-8")),
            )
        )
        self.contents_by_full_name[_normalize_resource_path(full_name)] = (
            content.encode("utf-8")
        )

    def create_directory(self, *, current_dir: str, name: str) -> None:
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, name)
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20005,
                result_message=f"resource {full_name} already exists",
            )
        self.resources.append(
            FakeResourceItem(
                alias=name,
                file_name_value=name,
                full_name_value=full_name,
                is_directory_value=True,
            )
        )

    def delete(self, *, full_name: str) -> bool:
        normalized_full_name = _normalize_resource_path(full_name)
        item = self._find_item(normalized_full_name)
        if item is None:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        prefix = f"{normalized_full_name}/"
        self.resources = [
            resource
            for resource in self.resources
            if resource.fullName != normalized_full_name
            and (resource.fullName is None or not resource.fullName.startswith(prefix))
        ]
        removable = [
            path
            for path in self.contents_by_full_name
            if path == normalized_full_name or path.startswith(prefix)
        ]
        for path in removable:
            self.contents_by_full_name.pop(path, None)
        return True

    def download(self, *, full_name: str) -> BinaryResponse:
        item = self._require_item(full_name)
        if item.isDirectory:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        normalized_full_name = _normalize_resource_path(full_name)
        content = self.contents_by_full_name.get(normalized_full_name)
        if content is None:
            raise ApiResultError(
                result_code=20012,
                result_message=f"resource file {full_name} not found",
            )
        return BinaryResponse(
            content=content,
            headers={"content-type": "application/octet-stream"},
            content_type="application/octet-stream",
        )

    def _find_item(self, full_name: str) -> FakeResourceItem | None:
        normalized_full_name = _normalize_resource_path(full_name)
        for resource in self.resources:
            if resource.fullName == normalized_full_name:
                return resource
        return None

    def _require_item(self, full_name: str) -> FakeResourceItem:
        resource = self._find_item(full_name)
        if resource is None:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        return resource

    def _require_directory(self, full_name: str) -> None:
        normalized_full_name = _normalize_resource_path(full_name)
        if normalized_full_name == _normalize_resource_path(self.base_dir_value):
            return
        resource = self._find_item(normalized_full_name)
        if resource is None or not resource.isDirectory:
            raise ApiResultError(
                result_code=20015,
                result_message=f"parent resource {full_name} not found",
            )


def _normalize_resource_path(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.strip()
    if normalized == "/":
        return normalized
    return normalized.rstrip("/")


def _resource_parent(full_name: str | None) -> str | None:
    normalized = _normalize_resource_path(full_name)
    if not normalized or "/" not in normalized:
        return None
    parent = normalized.rsplit("/", 1)[0]
    return parent or "/"


def _join_resource_path(directory: str, name: str) -> str:
    normalized_directory = _normalize_resource_path(directory)
    if normalized_directory == "/":
        return f"/{name}"
    return f"{normalized_directory}/{name}"


@dataclass(frozen=True)
class FakeWorkerGroup:
    id: int | None
    name: str | None
    addr_list_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    description: str | None = None
    system_default: bool = False

    @property
    def addrList(self) -> str | None:  # noqa: N802
        return self.addr_list_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def systemDefault(self) -> bool:  # noqa: N802
        return self.system_default


@dataclass(frozen=True)
class FakeWorkerGroupPage:
    total_list_value: list[FakeWorkerGroup] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeWorkerGroup] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeWorkerGroupAdapter:
    worker_groups: list[FakeWorkerGroup]
    config_worker_groups: list[FakeWorkerGroup] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeWorkerGroupPage:
        filtered_ui = [
            worker_group
            for worker_group in self.worker_groups
            if not worker_group.systemDefault
        ]
        if search is not None:
            filtered_ui = [
                worker_group
                for worker_group in filtered_ui
                if worker_group.name is not None
                and search.lower() in worker_group.name.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered_ui)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeWorkerGroupPage(
            total_list_value=list(self.config_worker_groups) + filtered_ui[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeWorkerGroup]:
        return list(self.worker_groups)

    def get(self, *, worker_group_id: int) -> FakeWorkerGroup:
        for worker_group in self.worker_groups:
            if worker_group.id == worker_group_id:
                return worker_group
        raise ApiResultError(
            result_code=1402001,
            result_message=f"worker group {worker_group_id} not exists",
        )

    def create(
        self,
        *,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> FakeWorkerGroup:
        for existing in self.worker_groups:
            if existing.id is not None and existing.name == name:
                raise ApiResultError(
                    result_code=10135,
                    result_message=f"name {name} already exists",
                )
        next_id = max((item.id or 0 for item in self.worker_groups), default=0) + 1
        created = FakeWorkerGroup(
            id=next_id,
            name=name,
            addr_list_value=addr_list,
            description=description,
            system_default=False,
        )
        self.worker_groups.append(created)
        return created

    def update(
        self,
        *,
        worker_group_id: int,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> FakeWorkerGroup:
        for existing in self.worker_groups:
            if existing.id == worker_group_id or existing.id is None:
                continue
            if existing.name == name:
                raise ApiResultError(
                    result_code=10135,
                    result_message=f"name {name} already exists",
                )
        for index, existing in enumerate(self.worker_groups):
            if existing.id == worker_group_id:
                updated = replace(
                    existing,
                    name=name,
                    addr_list_value=addr_list,
                    description=description,
                )
                self.worker_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=1402001,
            result_message=f"worker group {worker_group_id} not exists",
        )

    def delete(self, *, worker_group_id: int) -> bool:
        for index, worker_group in enumerate(self.worker_groups):
            if worker_group.id == worker_group_id:
                self.worker_groups.pop(index)
                return True
        raise ApiResultError(
            result_code=10174,
            result_message=f"worker group {worker_group_id} not exists",
        )


@dataclass(frozen=True)
class FakeTenant:
    id: int
    tenant_code_value: str | None
    description: str | None = None
    queue_id_value: int = 0
    queue_name_value: str | None = None
    queue_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def queueId(self) -> int:  # noqa: N802
        return self.queue_id_value

    @property
    def queueName(self) -> str | None:  # noqa: N802
        return self.queue_name_value

    @property
    def queue(self) -> str | None:
        return self.queue_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeTenantPage:
    total_list_value: list[FakeTenant] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeTenant] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeTenantAdapter:
    tenants: list[FakeTenant]
    queues: list[FakeQueue] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeTenantPage:
        filtered = list(self.tenants)
        if search is not None:
            filtered = [
                tenant
                for tenant in filtered
                if tenant.tenantCode is not None
                and search.lower() in tenant.tenantCode.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeTenantPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeTenant]:
        return list(self.tenants)

    def get(self, *, tenant_id: int) -> FakeTenant:
        for tenant in self.tenants:
            if tenant.id == tenant_id:
                return tenant
        raise ApiResultError(
            result_code=10017,
            result_message=f"tenant {tenant_id} not exists",
        )

    def create(
        self,
        *,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> FakeTenant:
        for existing in self.tenants:
            if existing.tenantCode == tenant_code:
                raise ApiResultError(
                    result_code=10009,
                    result_message=f"os tenant code {tenant_code} already exists",
                )
        queue = self._queue(queue_id)
        next_id = max((item.id for item in self.tenants), default=0) + 1
        created = FakeTenant(
            id=next_id,
            tenant_code_value=tenant_code,
            description=description,
            queue_id_value=queue.id,
            queue_name_value=queue.queueName,
            queue_value=queue.queue,
        )
        self.tenants.append(created)
        return created

    def update(
        self,
        *,
        tenant_id: int,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> FakeTenant:
        queue = self._queue(queue_id)
        for existing in self.tenants:
            if existing.id == tenant_id:
                continue
            if existing.tenantCode == tenant_code:
                raise ApiResultError(
                    result_code=10009,
                    result_message=f"os tenant code {tenant_code} already exists",
                )
        for index, existing in enumerate(self.tenants):
            if existing.id == tenant_id:
                updated = replace(
                    existing,
                    tenant_code_value=tenant_code,
                    description=description,
                    queue_id_value=queue.id,
                    queue_name_value=queue.queueName,
                    queue_value=queue.queue,
                )
                self.tenants[index] = updated
                return updated
        raise ApiResultError(
            result_code=10017,
            result_message=f"tenant {tenant_id} not exists",
        )

    def delete(self, *, tenant_id: int) -> bool:
        for index, tenant in enumerate(self.tenants):
            if tenant.id == tenant_id:
                self.tenants.pop(index)
                return True
        raise ApiResultError(
            result_code=10017,
            result_message=f"tenant {tenant_id} not exists",
        )

    def _queue(self, queue_id: int) -> FakeQueue:
        for queue in self.queues:
            if queue.id == queue_id:
                return queue
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )


@dataclass(frozen=True)
class FakeUser:
    id: int
    user_name_value: str | None
    email: str | None
    phone: str | None = None
    user_type_value: FakeEnumValue | None = None
    tenant_id_value: int = 0
    tenant_code_value: str | None = None
    queue_name_value: str | None = None
    queue_value: str | None = None
    state: int = 1
    time_zone_value: str | None = None
    stored_queue_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def userType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.user_type_value

    @property
    def tenantId(self) -> int:  # noqa: N802
        return self.tenant_id_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def queueName(self) -> str | None:  # noqa: N802
        return self.queue_name_value

    @property
    def queue(self) -> str | None:
        return self.queue_value

    @property
    def timeZone(self) -> str | None:  # noqa: N802
        return self.time_zone_value

    @property
    def storedQueue(self) -> str | None:  # noqa: N802
        return self.stored_queue_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeAccessToken:
    id: int | None
    user_id_value: int | None
    token: str | None
    expire_time_value: str | None
    create_time_value: str | None = None
    update_time_value: str | None = None
    user_name_value: str | None = None

    @property
    def userId(self) -> int | None:  # noqa: N802
        return self.user_id_value

    @property
    def expireTime(self) -> str | None:  # noqa: N802
        return self.expire_time_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value


@dataclass(frozen=True)
class FakeAccessTokenPage:
    total_list_value: list[FakeAccessToken] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeAccessToken] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeAccessTokenAdapter:
    access_tokens: list[FakeAccessToken]
    users: list[FakeUser] = field(default_factory=list)
    create_errors_by_user_id: dict[int, ApiResultError] = field(default_factory=dict)
    generate_errors_by_user_id: dict[int, ApiResultError] = field(default_factory=dict)
    update_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)
    delete_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeAccessTokenPage:
        filtered = list(self.access_tokens)
        if search is not None:
            search_value = search.lower()
            filtered = [
                access_token
                for access_token in filtered
                if (
                    access_token.token is not None
                    and search_value in access_token.token.lower()
                )
                or (
                    access_token.userName is not None
                    and search_value in access_token.userName.lower()
                )
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeAccessTokenPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> FakeAccessToken:
        if user_id in self.create_errors_by_user_id:
            raise self.create_errors_by_user_id[user_id]
        next_id = max((item.id or 0 for item in self.access_tokens), default=0) + 1
        created = FakeAccessToken(
            id=next_id,
            user_id_value=user_id,
            token=token or f"auto-token-{next_id}",
            expire_time_value=expire_time,
            user_name_value=self._user_name(user_id),
        )
        self.access_tokens.append(created)
        return created

    def generate(self, *, user_id: int, expire_time: str) -> str:
        del expire_time
        if user_id in self.generate_errors_by_user_id:
            raise self.generate_errors_by_user_id[user_id]
        return f"generated-token-{user_id}"

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None = None,
    ) -> FakeAccessToken:
        if token_id in self.update_errors_by_id:
            raise self.update_errors_by_id[token_id]
        for index, access_token in enumerate(self.access_tokens):
            if access_token.id == token_id:
                updated = replace(
                    access_token,
                    user_id_value=user_id,
                    expire_time_value=expire_time,
                    token=(token or f"regenerated-token-{token_id}"),
                    user_name_value=self._user_name(user_id),
                )
                self.access_tokens[index] = updated
                return updated
        raise ApiResultError(
            result_code=70015,
            result_message=f"access token {token_id} not exist",
        )

    def delete(self, *, token_id: int) -> bool:
        if token_id in self.delete_errors_by_id:
            raise self.delete_errors_by_id[token_id]
        for index, access_token in enumerate(self.access_tokens):
            if access_token.id == token_id:
                self.access_tokens.pop(index)
                return True
        raise ApiResultError(
            result_code=70015,
            result_message=f"access token {token_id} not exist",
        )

    def _user_name(self, user_id: int) -> str | None:
        for user in self.users:
            if user.id == user_id:
                return user.userName
        return None


@dataclass(frozen=True)
class FakeCluster:
    id: int
    code: int | None
    name: str | None
    config: str | None = None
    description: str | None = None
    workflow_definitions_value: list[str] | None = None
    operator: int | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def workflowDefinitions(self) -> list[str] | None:  # noqa: N802
        return self.workflow_definitions_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeClusterPage:
    total_list_value: list[FakeCluster] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeCluster] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeClusterAdapter:
    clusters: list[FakeCluster]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeClusterPage:
        filtered = list(self.clusters)
        if search is not None:
            search_value = search.lower()
            filtered = [
                cluster
                for cluster in filtered
                if cluster.name is not None and search_value in cluster.name.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeClusterPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, code: int) -> FakeCluster:
        for cluster in self.clusters:
            if cluster.code == code:
                return cluster
        raise ApiResultError(
            result_code=1200028,
            result_message=f"cluster code {code} not found",
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
    ) -> FakeCluster:
        for cluster in self.clusters:
            if cluster.name == name:
                raise ApiResultError(
                    result_code=120021,
                    result_message=f"cluster name {name} already exists",
                )
        next_code = max((cluster.code or 0 for cluster in self.clusters), default=0) + 1
        created = FakeCluster(
            id=next_code,
            code=next_code,
            name=name,
            config=config,
            description=description,
        )
        self.clusters.append(created)
        return created

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
    ) -> FakeCluster:
        for cluster in self.clusters:
            if cluster.code == code:
                continue
            if cluster.name == name:
                raise ApiResultError(
                    result_code=120021,
                    result_message=f"cluster name {name} already exists",
                )
        for index, cluster in enumerate(self.clusters):
            if cluster.code == code:
                updated = replace(
                    cluster,
                    name=name,
                    config=config,
                    description=description,
                )
                self.clusters[index] = updated
                return updated
        raise ApiResultError(
            result_code=120033,
            result_message=f"cluster code {code} not found",
        )

    def delete(self, *, code: int) -> bool:
        for index, cluster in enumerate(self.clusters):
            if cluster.code == code:
                self.clusters.pop(index)
                return True
        raise ApiResultError(
            result_code=1200028,
            result_message=f"cluster code {code} not found",
        )


@dataclass(frozen=True)
class FakeUserPage:
    total_list_value: list[FakeUser] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeUser] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeUserAdapter:
    users: list[FakeUser]
    tenants: list[FakeTenant] = field(default_factory=list)
    current_user: FakeUser | None = None
    current_user_id: int | None = None
    create_errors_by_name: dict[str, ApiResultError] | None = None
    update_errors_by_id: dict[int, ApiResultError] | None = None
    delete_errors_by_id: dict[int, ApiResultError] | None = None
    grant_project_errors_by_target: dict[tuple[int, int], ApiResultError] | None = None
    revoke_project_errors_by_target: dict[tuple[int, int], ApiResultError] | None = None
    grant_datasource_errors_by_user_id: dict[int, ApiResultError] | None = None
    grant_namespace_errors_by_user_id: dict[int, ApiResultError] | None = None
    granted_datasources_by_user_id: dict[int, set[int]] = field(default_factory=dict)
    granted_namespaces_by_user_id: dict[int, set[int]] = field(default_factory=dict)
    granted_projects_by_user_id: dict[int, set[int]] = field(default_factory=dict)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeUserPage:
        filtered = list(self.users)
        if search is not None:
            filtered = [
                user
                for user in filtered
                if user.userName is not None and search.lower() in user.userName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeUserPage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def list_all(self) -> Sequence[FakeUser]:
        return list(self.users)

    def current(self) -> FakeUser:
        if self.current_user is not None:
            return self.current_user
        if self.current_user_id is not None:
            return self.get(user_id=self.current_user_id)
        if self.users:
            return self.users[0]
        raise ApiResultError(
            result_code=10010,
            result_message="current user not exists",
        )

    def get(self, *, user_id: int) -> FakeUser:
        for user in self.users:
            if user.id == user_id:
                return user
        raise ApiResultError(
            result_code=10010,
            result_message=f"user {user_id} not exists",
        )

    def create(
        self,
        *,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None = None,
        queue: str | None = None,
        state: int,
    ) -> FakeUser:
        del password
        if (
            self.create_errors_by_name is not None
            and user_name in self.create_errors_by_name
        ):
            raise self.create_errors_by_name[user_name]
        for existing in self.users:
            if existing.userName == user_name:
                raise ApiResultError(
                    result_code=10003,
                    result_message=f"user name {user_name} already exists",
                )
        tenant = self._tenant(tenant_id)
        stored_queue = "" if queue is None else queue
        next_id = max((item.id for item in self.users), default=0) + 1
        created = FakeUser(
            id=next_id,
            user_name_value=user_name,
            email=email,
            phone=phone,
            user_type_value=FakeEnumValue("GENERAL_USER"),
            tenant_id_value=tenant.id,
            tenant_code_value=tenant.tenantCode,
            queue_name_value=tenant.queueName,
            queue_value=tenant.queueName if stored_queue == "" else stored_queue,
            state=state,
            stored_queue_value=stored_queue,
        )
        self.users.append(created)
        return created

    def update(
        self,
        *,
        user_id: int,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None,
        queue: str,
        state: int,
        time_zone: str | None = None,
    ) -> FakeUser:
        del password
        if self.update_errors_by_id is not None and user_id in self.update_errors_by_id:
            raise self.update_errors_by_id[user_id]
        tenant = self._tenant(tenant_id)
        for existing in self.users:
            if existing.id == user_id:
                continue
            if existing.userName == user_name:
                raise ApiResultError(
                    result_code=10003,
                    result_message=f"user name {user_name} already exists",
                )
        for index, existing in enumerate(self.users):
            if existing.id != user_id:
                continue
            updated = replace(
                existing,
                user_name_value=user_name,
                email=email,
                phone=phone,
                tenant_id_value=tenant.id,
                tenant_code_value=tenant.tenantCode,
                queue_name_value=tenant.queueName,
                queue_value=tenant.queueName if queue == "" else queue,
                state=state,
                time_zone_value=time_zone,
                stored_queue_value=queue,
            )
            self.users[index] = updated
            return updated
        raise ApiResultError(
            result_code=10010,
            result_message=f"user {user_id} not exists",
        )

    def delete(self, *, user_id: int) -> bool:
        if self.delete_errors_by_id is not None and user_id in self.delete_errors_by_id:
            raise self.delete_errors_by_id[user_id]
        for index, user in enumerate(self.users):
            if user.id == user_id:
                self.users.pop(index)
                return True
        raise ApiResultError(
            result_code=10010,
            result_message=f"user {user_id} not exists",
        )

    def grant_project_by_code(self, *, user_id: int, project_code: int) -> bool:
        if (
            self.grant_project_errors_by_target is not None
            and (user_id, project_code) in self.grant_project_errors_by_target
        ):
            raise self.grant_project_errors_by_target[(user_id, project_code)]
        self.get(user_id=user_id)
        self.granted_projects_by_user_id.setdefault(user_id, set()).add(project_code)
        return True

    def revoke_project(self, *, user_id: int, project_code: int) -> bool:
        if (
            self.revoke_project_errors_by_target is not None
            and (user_id, project_code) in self.revoke_project_errors_by_target
        ):
            raise self.revoke_project_errors_by_target[(user_id, project_code)]
        self.get(user_id=user_id)
        self.granted_projects_by_user_id.setdefault(user_id, set()).discard(
            project_code
        )
        return True

    def grant_datasources(
        self,
        *,
        user_id: int,
        datasource_ids: Sequence[int],
    ) -> bool:
        if (
            self.grant_datasource_errors_by_user_id is not None
            and user_id in self.grant_datasource_errors_by_user_id
        ):
            raise self.grant_datasource_errors_by_user_id[user_id]
        self.get(user_id=user_id)
        self.granted_datasources_by_user_id[user_id] = set(datasource_ids)
        return True

    def grant_namespaces(
        self,
        *,
        user_id: int,
        namespace_ids: Sequence[int],
    ) -> bool:
        if (
            self.grant_namespace_errors_by_user_id is not None
            and user_id in self.grant_namespace_errors_by_user_id
        ):
            raise self.grant_namespace_errors_by_user_id[user_id]
        self.get(user_id=user_id)
        self.granted_namespaces_by_user_id[user_id] = set(namespace_ids)
        return True

    def _tenant(self, tenant_id: int) -> FakeTenant:
        for tenant in self.tenants:
            if tenant.id == tenant_id:
                return tenant
        raise ApiResultError(
            result_code=10017,
            result_message=f"tenant {tenant_id} not exists",
        )


@dataclass(frozen=True)
class FakeMonitorServer:
    id: int
    host: str | None
    port: int
    server_directory_value: str | None = None
    heart_beat_info_value: str | None = None
    create_time_value: str | None = None
    last_heartbeat_time_value: str | None = None

    @property
    def serverDirectory(self) -> str | None:  # noqa: N802
        return self.server_directory_value

    @property
    def heartBeatInfo(self) -> str | None:  # noqa: N802
        return self.heart_beat_info_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def lastHeartbeatTime(self) -> str | None:  # noqa: N802
        return self.last_heartbeat_time_value


@dataclass(frozen=True)
class FakeMonitorDatabase:
    db_type_value: FakeEnumValue | None = None
    state_value: FakeEnumValue | None = None
    max_connections_value: int = 0
    max_used_connections_value: int = 0
    threads_connections_value: int = 0
    threads_running_connections_value: int = 0
    date_value: str | None = None

    @property
    def dbType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.db_type_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def maxConnections(self) -> int:  # noqa: N802
        return self.max_connections_value

    @property
    def maxUsedConnections(self) -> int:  # noqa: N802
        return self.max_used_connections_value

    @property
    def threadsConnections(self) -> int:  # noqa: N802
        return self.threads_connections_value

    @property
    def threadsRunningConnections(self) -> int:  # noqa: N802
        return self.threads_running_connections_value

    @property
    def date(self) -> str | None:
        return self.date_value


@dataclass
class FakeMonitorAdapter:
    servers_by_node_type: dict[str, list[FakeMonitorServer]]
    databases: list[FakeMonitorDatabase] = field(default_factory=list)

    def list_servers(self, *, node_type: str) -> Sequence[FakeMonitorServer]:
        return list(self.servers_by_node_type.get(node_type, []))

    def list_databases(self) -> Sequence[FakeMonitorDatabase]:
        return list(self.databases)


@dataclass(frozen=True)
class FakeSchedule:
    id: int | None = None
    workflow_definition_code_value: int = 0
    workflow_definition_name_value: str | None = None
    project_name_value: str | None = None
    definition_description_value: str | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    timezone_id_value: str | None = None
    crontab_value: str | None = None
    failure_strategy_value: FakeEnumValue | None = None
    warning_type_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    workflow_instance_priority_value: FakeEnumValue | None = None
    release_state_value: FakeEnumValue | None = None
    warning_group_id_value: int = 0
    worker_group_value: str | None = None
    tenant_code_value: str | None = None
    environment_code_value: int | None = None
    environment_name_value: str | None = None
    project_code_value: int = 0

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        return self.workflow_definition_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def definitionDescription(self) -> str | None:  # noqa: N802
        return self.definition_description_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def timezoneId(self) -> str | None:  # noqa: N802
        return self.timezone_id_value

    @property
    def crontab(self) -> str | None:
        return self.crontab_value

    @property
    def failureStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.failure_strategy_value

    @property
    def warningType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.warning_type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def workflowInstancePriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.workflow_instance_priority_value

    @property
    def releaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.release_state_value

    @property
    def warningGroupId(self) -> int:  # noqa: N802
        return self.warning_group_id_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def environmentName(self) -> str | None:  # noqa: N802
        return self.environment_name_value


@dataclass(frozen=True)
class FakeSchedulePage:
    total_list_value: list[FakeSchedule] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeSchedule] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass(frozen=True)
class FakeWorkflow:
    code: int
    name: str | None
    version: int | None = 1
    project_code_value: int = 0
    description: str | None = None
    global_params_value: str | None = None
    global_param_map_value: dict[str, str] | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    project_name_value: str | None = None
    timeout: int = 0
    release_state_value: FakeEnumValue | None = None
    schedule_release_state_value: FakeEnumValue | None = None
    execution_type_value: FakeEnumValue | None = None
    schedule_value: FakeSchedule | None = None
    id: int | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def globalParams(self) -> str | None:  # noqa: N802
        return self.global_params_value

    @property
    def globalParamMap(self) -> dict[str, str] | None:  # noqa: N802
        return self.global_param_map_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def releaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.release_state_value

    @property
    def scheduleReleaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.schedule_release_state_value

    @property
    def executionType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.execution_type_value

    @property
    def schedule(self) -> FakeSchedule | None:
        return self.schedule_value


@dataclass(frozen=True)
class FakeTaskDefinition:
    code: int
    name: str | None
    version: int | None = 1
    project_code_value: int = 0
    description: str | None = None
    task_type_value: str | None = None
    task_params_value: JsonValue | None = None
    user_name_value: str | None = None
    project_name_value: str | None = None
    worker_group_value: str | None = None
    fail_retry_times_value: int = 0
    fail_retry_interval_value: int = 0
    timeout: int = 0
    delay_time_value: int = 0
    resource_ids_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    modify_by_value: str | None = None
    task_group_id_value: int = 0
    task_group_priority_value: int = 0
    environment_code_value: int = 0
    task_priority_value: FakeEnumValue | None = None
    timeout_flag_value: FakeEnumValue | None = None
    timeout_notify_strategy_value: FakeEnumValue | None = None
    task_execute_type_value: FakeEnumValue | None = None
    flag_value: FakeEnumValue | None = None
    cpu_quota_value: int | None = None
    memory_max_value: int | None = None
    id: int | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def taskParams(self) -> JsonValue | None:  # noqa: N802
        return self.task_params_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def failRetryTimes(self) -> int:  # noqa: N802
        return self.fail_retry_times_value

    @property
    def failRetryInterval(self) -> int:  # noqa: N802
        return self.fail_retry_interval_value

    @property
    def delayTime(self) -> int:  # noqa: N802
        return self.delay_time_value

    @property
    def resourceIds(self) -> str | None:  # noqa: N802
        return self.resource_ids_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def modifyBy(self) -> str | None:  # noqa: N802
        return self.modify_by_value

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        return self.task_group_id_value

    @property
    def taskGroupPriority(self) -> int:  # noqa: N802
        return self.task_group_priority_value

    @property
    def environmentCode(self) -> int:  # noqa: N802
        return self.environment_code_value

    @property
    def taskPriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_priority_value

    @property
    def timeoutFlag(self) -> FakeEnumValue | None:  # noqa: N802
        return self.timeout_flag_value

    @property
    def timeoutNotifyStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.timeout_notify_strategy_value

    @property
    def taskExecuteType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_execute_type_value

    @property
    def flag(self) -> FakeEnumValue | None:
        return self.flag_value

    @property
    def cpuQuota(self) -> int | None:  # noqa: N802
        return self.cpu_quota_value

    @property
    def memoryMax(self) -> int | None:  # noqa: N802
        return self.memory_max_value


@dataclass(frozen=True)
class FakeWorkflowTaskRelation:
    pre_task_code_value: int
    post_task_code_value: int
    pre_task_version_value: int = 0
    post_task_version_value: int = 0
    condition_params_value: JsonValue | None = None

    @property
    def preTaskCode(self) -> int:  # noqa: N802
        return self.pre_task_code_value

    @property
    def postTaskCode(self) -> int:  # noqa: N802
        return self.post_task_code_value

    @property
    def preTaskVersion(self) -> int:  # noqa: N802
        return self.pre_task_version_value

    @property
    def postTaskVersion(self) -> int:  # noqa: N802
        return self.post_task_version_value

    @property
    def conditionParams(self) -> JsonValue | None:  # noqa: N802
        return self.condition_params_value


@dataclass(frozen=True)
class FakeDag:
    workflow_definition_value: FakeWorkflow | None
    task_definition_list_value: list[FakeTaskDefinition] | None
    workflow_task_relation_list_value: list[FakeWorkflowTaskRelation] | None

    @property
    def workflowDefinition(self) -> FakeWorkflow | None:  # noqa: N802
        return self.workflow_definition_value

    @property
    def taskDefinitionList(self) -> list[FakeTaskDefinition] | None:  # noqa: N802
        return self.task_definition_list_value

    @property
    def workflowTaskRelationList(self) -> list[FakeWorkflowTaskRelation] | None:  # noqa: N802
        return self.workflow_task_relation_list_value


@dataclass(frozen=True)
class FakeWorkflowLineageRelation:
    source_work_flow_code_value: int
    target_work_flow_code_value: int

    @property
    def sourceWorkFlowCode(self) -> int:  # noqa: N802
        return self.source_work_flow_code_value

    @property
    def targetWorkFlowCode(self) -> int:  # noqa: N802
        return self.target_work_flow_code_value


@dataclass(frozen=True)
class FakeWorkflowLineageDetail:
    work_flow_code_value: int
    work_flow_name_value: str | None
    work_flow_publish_status_value: str | None = None
    schedule_start_time_value: str | None = None
    schedule_end_time_value: str | None = None
    crontab_value: str | None = None
    schedule_publish_status_value: int = 0
    source_work_flow_code_value: str | None = None

    @property
    def workFlowCode(self) -> int:  # noqa: N802
        return self.work_flow_code_value

    @property
    def workFlowName(self) -> str | None:  # noqa: N802
        return self.work_flow_name_value

    @property
    def workFlowPublishStatus(self) -> str | None:  # noqa: N802
        return self.work_flow_publish_status_value

    @property
    def scheduleStartTime(self) -> str | None:  # noqa: N802
        return self.schedule_start_time_value

    @property
    def scheduleEndTime(self) -> str | None:  # noqa: N802
        return self.schedule_end_time_value

    @property
    def crontab(self) -> str | None:
        return self.crontab_value

    @property
    def schedulePublishStatus(self) -> int:  # noqa: N802
        return self.schedule_publish_status_value

    @property
    def sourceWorkFlowCode(self) -> str | None:  # noqa: N802
        return self.source_work_flow_code_value


@dataclass(frozen=True)
class FakeWorkflowLineage:
    work_flow_relation_list_value: list[FakeWorkflowLineageRelation] | None
    work_flow_relation_detail_list_value: list[FakeWorkflowLineageDetail] | None

    @property
    def workFlowRelationList(self) -> list[FakeWorkflowLineageRelation] | None:  # noqa: N802
        return self.work_flow_relation_list_value

    @property
    def workFlowRelationDetailList(self) -> list[FakeWorkflowLineageDetail] | None:  # noqa: N802
        return self.work_flow_relation_detail_list_value


@dataclass(frozen=True)
class FakeDependentLineageTask:
    project_code_value: int
    workflow_definition_code_value: int
    workflow_definition_name_value: str | None
    task_definition_code_value: int
    task_definition_name_value: str | None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        return self.workflow_definition_name_value

    @property
    def taskDefinitionCode(self) -> int:  # noqa: N802
        return self.task_definition_code_value

    @property
    def taskDefinitionName(self) -> str | None:  # noqa: N802
        return self.task_definition_name_value


@dataclass
class FakeWorkflowLineageAdapter:
    project_lineages: dict[int, FakeWorkflowLineage]
    workflow_lineages: dict[tuple[int, int], FakeWorkflowLineage]
    dependent_tasks_by_target: dict[
        tuple[int, int, int | None],
        list[FakeDependentLineageTask],
    ] = field(default_factory=dict)

    def list(self, *, project_code: int) -> FakeWorkflowLineage | None:
        return self.project_lineages.get(
            project_code,
            FakeWorkflowLineage(
                work_flow_relation_list_value=[],
                work_flow_relation_detail_list_value=[],
            ),
        )

    def get(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> FakeWorkflowLineage | None:
        return self.workflow_lineages.get(
            (project_code, workflow_code),
            FakeWorkflowLineage(
                work_flow_relation_list_value=[],
                work_flow_relation_detail_list_value=[],
            ),
        )

    def query_dependent_tasks(
        self,
        *,
        project_code: int,
        workflow_code: int,
        task_code: int | None = None,
    ) -> Sequence[FakeDependentLineageTask]:
        return list(
            self.dependent_tasks_by_target.get(
                (project_code, workflow_code, task_code),
                [],
            )
        )


@dataclass
class FakeWorkflowAdapter:
    workflows: list[FakeWorkflow]
    dags: dict[int, FakeDag]
    run_results_by_code: dict[int, Sequence[int]] | None = None
    run_errors_by_code: dict[int, ApiResultError] | None = None
    create_errors_by_name: dict[str, ApiResultError] | None = None
    update_errors_by_code: dict[int, ApiResultError] | None = None
    delete_errors_by_code: dict[int, ApiResultError] | None = None
    online_errors_by_code: dict[int, ApiResultError] | None = None
    offline_errors_by_code: dict[int, ApiResultError] | None = None
    create_calls: list[dict[str, object]] = field(default_factory=list)
    update_calls: list[dict[str, object]] = field(default_factory=list)
    run_calls: list[dict[str, object]] = field(default_factory=list)
    backfill_calls: list[dict[str, object]] = field(default_factory=list)
    release_calls: list[tuple[int, str]] = field(default_factory=list)

    def list(self, *, project_code: int) -> list[FakeWorkflow]:
        return [
            workflow
            for workflow in self.workflows
            if workflow.projectCode == project_code
        ]

    def get(self, *, code: int) -> FakeWorkflow:
        for workflow in self.workflows:
            if workflow.code == code:
                return workflow
        raise ApiResultError(
            result_code=10018,
            result_message=f"workflow code {code} not found",
        )

    def describe(self, *, project_code: int, code: int) -> FakeDag:
        workflow = self.get(code=code)
        if workflow.projectCode != project_code:
            message = f"workflow code {code} not found in project {project_code}"
            raise ApiResultError(
                result_code=10018,
                result_message=message,
            )
        dag = self.dags.get(code)
        if dag is None:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow dag {code} not found",
            )
        return dag

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> None:
        self.create_calls.append(
            {
                "project_code": project_code,
                "name": name,
                "description": description,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "execution_type": execution_type,
            }
        )
        if (
            self.create_errors_by_name is not None
            and name in self.create_errors_by_name
        ):
            raise self.create_errors_by_name[name]
        next_code = max((workflow.code for workflow in self.workflows), default=100) + 1
        created = FakeWorkflow(
            code=next_code,
            name=name,
            project_code_value=project_code,
            description=description,
            global_params_value=global_params,
            user_id_value=11,
            user_name_value="alice",
            timeout=timeout,
            execution_type_value=(
                None if execution_type is None else FakeEnumValue(execution_type)
            ),
        )
        task_definitions = [
            _fake_task_definition_from_payload(item, project_code=project_code)
            for item in _json_array(task_definition_json, label="task_definition_json")
        ]
        task_relations = [
            _fake_task_relation_from_payload(item)
            for item in _json_array(task_relation_json, label="task_relation_json")
        ]
        self.workflows.append(created)
        self.dags[next_code] = FakeDag(
            workflow_definition_value=created,
            task_definition_list_value=task_definitions,
            workflow_task_relation_list_value=task_relations,
        )

    def run(
        self,
        *,
        project_code: int,
        workflow_code: int,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        self.run_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "worker_group": worker_group,
                "tenant_code": tenant_code,
                "start_node_list": (
                    None if start_node_list is None else list(start_node_list)
                ),
                "task_scope": task_scope,
                "failure_strategy": failure_strategy,
                "warning_type": warning_type,
                "workflow_instance_priority": workflow_instance_priority,
                "warning_group_id": warning_group_id,
                "environment_code": environment_code,
                "start_params": start_params,
                "dry_run": dry_run,
            }
        )
        workflow = self.get(code=workflow_code)
        if workflow.projectCode != project_code:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow code {workflow_code} not found",
            )
        if (
            self.run_errors_by_code is not None
            and workflow_code in self.run_errors_by_code
        ):
            raise self.run_errors_by_code[workflow_code]
        if self.run_results_by_code is not None:
            return list(self.run_results_by_code.get(workflow_code, []))
        return [workflow_code * 10]

    def backfill(
        self,
        *,
        project_code: int,
        workflow_code: int,
        schedule_time: str,
        run_mode: str,
        expected_parallelism_number: int,
        complement_dependent_mode: str,
        all_level_dependent: bool,
        execution_order: str,
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        dry_run: bool = False,
    ) -> Sequence[int]:
        self.backfill_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "schedule_time": schedule_time,
                "run_mode": run_mode,
                "expected_parallelism_number": expected_parallelism_number,
                "complement_dependent_mode": complement_dependent_mode,
                "all_level_dependent": all_level_dependent,
                "execution_order": execution_order,
                "worker_group": worker_group,
                "tenant_code": tenant_code,
                "start_node_list": (
                    None if start_node_list is None else list(start_node_list)
                ),
                "task_scope": task_scope,
                "failure_strategy": failure_strategy,
                "warning_type": warning_type,
                "workflow_instance_priority": workflow_instance_priority,
                "warning_group_id": warning_group_id,
                "environment_code": environment_code,
                "start_params": start_params,
                "dry_run": dry_run,
            }
        )
        workflow = self.get(code=workflow_code)
        if workflow.projectCode != project_code:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow code {workflow_code} not found",
            )
        if (
            self.run_errors_by_code is not None
            and workflow_code in self.run_errors_by_code
        ):
            raise self.run_errors_by_code[workflow_code]
        if self.run_results_by_code is not None:
            return list(self.run_results_by_code.get(workflow_code, []))
        return [workflow_code * 10]

    def update(
        self,
        *,
        project_code: int,
        workflow_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
    ) -> None:
        self.update_calls.append(
            {
                "project_code": project_code,
                "workflow_code": workflow_code,
                "name": name,
                "description": description,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "execution_type": execution_type,
                "release_state": release_state,
            }
        )
        if (
            self.update_errors_by_code is not None
            and workflow_code in self.update_errors_by_code
        ):
            raise self.update_errors_by_code[workflow_code]

        workflow = self.get(code=workflow_code)
        if workflow.projectCode != project_code:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow code {workflow_code} not found",
            )

        effective_release_state = release_state or "OFFLINE"
        updated_schedule = workflow.schedule
        updated_schedule_release_state = workflow.scheduleReleaseState
        if (
            effective_release_state == "OFFLINE"
            and workflow.scheduleReleaseState is not None
        ):
            updated_schedule_release_state = FakeEnumValue("OFFLINE")
            if workflow.schedule is not None:
                updated_schedule = replace(
                    workflow.schedule,
                    release_state_value=FakeEnumValue("OFFLINE"),
                )

        updated_workflow = replace(
            workflow,
            name=name,
            version=(workflow.version or 1) + 1,
            description=description,
            global_params_value=global_params,
            timeout=timeout,
            update_time_value="2026-04-10 12:00:00",
            release_state_value=FakeEnumValue(effective_release_state),
            schedule_release_state_value=updated_schedule_release_state,
            execution_type_value=(
                None if execution_type is None else FakeEnumValue(execution_type)
            ),
            schedule_value=updated_schedule,
        )

        task_definitions, task_relations = _updated_fake_workflow_tasks_and_relations(
            dags=self.dags,
            workflow_code=workflow_code,
            project_code=project_code,
            task_definition_json=task_definition_json,
            task_relation_json=task_relation_json,
        )

        for index, existing in enumerate(self.workflows):
            if existing.code == workflow_code:
                self.workflows[index] = updated_workflow
                break
        self.dags[workflow_code] = FakeDag(
            workflow_definition_value=updated_workflow,
            task_definition_list_value=task_definitions,
            workflow_task_relation_list_value=task_relations,
        )

    def delete(self, *, project_code: int, workflow_code: int) -> None:
        if (
            self.delete_errors_by_code is not None
            and workflow_code in self.delete_errors_by_code
        ):
            raise self.delete_errors_by_code[workflow_code]
        for index, workflow in enumerate(self.workflows):
            if workflow.code != workflow_code or workflow.projectCode != project_code:
                continue
            self.workflows.pop(index)
            self.dags.pop(workflow_code, None)
            return
        raise ApiResultError(
            result_code=10018,
            result_message=f"workflow code {workflow_code} not found",
        )

    def online(self, *, project_code: int, workflow_code: int) -> None:
        self.release_calls.append((workflow_code, "ONLINE"))
        if (
            self.online_errors_by_code is not None
            and workflow_code in self.online_errors_by_code
        ):
            raise self.online_errors_by_code[workflow_code]
        self._set_release_state(
            project_code=project_code,
            workflow_code=workflow_code,
            release_state="ONLINE",
        )

    def offline(self, *, project_code: int, workflow_code: int) -> None:
        self.release_calls.append((workflow_code, "OFFLINE"))
        if (
            self.offline_errors_by_code is not None
            and workflow_code in self.offline_errors_by_code
        ):
            raise self.offline_errors_by_code[workflow_code]
        self._set_release_state(
            project_code=project_code,
            workflow_code=workflow_code,
            release_state="OFFLINE",
        )

    def _set_release_state(
        self,
        *,
        project_code: int,
        workflow_code: int,
        release_state: str,
    ) -> None:
        for index, workflow in enumerate(self.workflows):
            if workflow.code != workflow_code or workflow.projectCode != project_code:
                continue
            schedule = workflow.schedule
            updated_schedule = schedule
            updated_schedule_release_state = workflow.scheduleReleaseState
            if release_state == "OFFLINE" and workflow.scheduleReleaseState is not None:
                updated_schedule_release_state = FakeEnumValue("OFFLINE")
                if schedule is not None:
                    updated_schedule = replace(
                        schedule,
                        release_state_value=FakeEnumValue("OFFLINE"),
                    )
            updated = replace(
                workflow,
                release_state_value=FakeEnumValue(release_state),
                schedule_release_state_value=updated_schedule_release_state,
                schedule_value=updated_schedule,
            )
            self.workflows[index] = updated
            dag = self.dags.get(workflow_code)
            if dag is not None:
                self.dags[workflow_code] = replace(
                    dag,
                    workflow_definition_value=updated,
                )
            return
        raise ApiResultError(
            result_code=10018,
            result_message=f"workflow code {workflow_code} not found",
        )


def _json_array(value: str, *, label: str) -> list[dict[str, object]]:
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        message = f"{label} must decode to a JSON array"
        raise TypeError(message)
    items: list[dict[str, object]] = []
    for item in parsed:
        if not isinstance(item, dict):
            message = f"{label} items must be JSON objects"
            raise TypeError(message)
        items.append(dict(item))
    return items


def _fake_task_definition_from_payload(
    payload: dict[str, object],
    *,
    project_code: int,
) -> FakeTaskDefinition:
    task_priority = payload.get("taskPriority")
    timeout_flag = payload.get("timeoutFlag")
    task_execute_type = payload.get("taskExecuteType")
    return FakeTaskDefinition(
        code=_require_int(payload["code"]),
        name=str(payload["name"]),
        version=_require_int(payload.get("version", 1)),
        project_code_value=project_code,
        description=_optional_string(payload.get("description")),
        task_type_value=_optional_string(payload.get("taskType")),
        task_params_value=_optional_json_value(payload.get("taskParams")),
        worker_group_value=_optional_string(payload.get("workerGroup")),
        fail_retry_times_value=_require_int(payload.get("failRetryTimes", 0)),
        fail_retry_interval_value=_require_int(payload.get("failRetryInterval", 0)),
        timeout=_require_int(payload.get("timeout", 0)),
        delay_time_value=_require_int(payload.get("delayTime", 0)),
        resource_ids_value=_optional_string(payload.get("resourceIds")),
        environment_code_value=_require_int(payload.get("environmentCode", 0)),
        task_priority_value=_optional_enum(task_priority),
        timeout_flag_value=_optional_enum(timeout_flag),
        task_execute_type_value=_optional_enum(task_execute_type),
    )


def _fake_task_relation_from_payload(
    payload: dict[str, object],
) -> FakeWorkflowTaskRelation:
    return FakeWorkflowTaskRelation(
        pre_task_code_value=_require_int(payload["preTaskCode"]),
        post_task_code_value=_require_int(payload["postTaskCode"]),
        pre_task_version_value=_require_int(payload.get("preTaskVersion", 0)),
        post_task_version_value=_require_int(payload.get("postTaskVersion", 0)),
        condition_params_value=_optional_json_value(payload.get("conditionParams")),
    )


def _normalized_task_definition_state(task: FakeTaskDefinition) -> dict[str, object]:
    timeout = task.timeout
    task_priority = None if task.taskPriority is None else task.taskPriority.value
    timeout_flag = None if task.timeoutFlag is None else task.timeoutFlag.value
    timeout_notify_strategy = (
        None if task.timeoutNotifyStrategy is None else task.timeoutNotifyStrategy.value
    )
    task_execute_type = (
        None if task.taskExecuteType is None else task.taskExecuteType.value
    )
    flag = None if task.flag is None else task.flag.value
    return {
        "name": task.name,
        "description": "" if task.description is None else task.description,
        "taskType": task.taskType,
        "taskParams": task.taskParams,
        "workerGroup": "default" if task.workerGroup is None else task.workerGroup,
        "environmentCode": -1 if task.environmentCode <= 0 else task.environmentCode,
        "failRetryTimes": task.failRetryTimes,
        "failRetryInterval": task.failRetryInterval,
        "timeout": timeout,
        "timeoutFlag": (
            "OPEN"
            if timeout > 0 and timeout_flag is None
            else "CLOSE"
            if timeout <= 0 and timeout_flag is None
            else timeout_flag
        ),
        "timeoutNotifyStrategy": (
            "WARN"
            if timeout > 0 and timeout_notify_strategy is None
            else None
            if timeout <= 0 and timeout_notify_strategy is None
            else timeout_notify_strategy
        ),
        "delayTime": task.delayTime,
        "resourceIds": "" if task.resourceIds is None else task.resourceIds,
        "taskPriority": "MEDIUM" if task_priority is None else task_priority,
        "taskExecuteType": "BATCH" if task_execute_type is None else task_execute_type,
        "flag": "YES" if flag is None else flag,
        "taskGroupId": task.taskGroupId,
        "taskGroupPriority": task.taskGroupPriority,
        "cpuQuota": -1 if task.cpuQuota is None else task.cpuQuota,
        "memoryMax": -1 if task.memoryMax is None else task.memoryMax,
    }


def _updated_fake_workflow_tasks_and_relations(
    *,
    dags: Mapping[int, FakeDag],
    workflow_code: int,
    project_code: int,
    task_definition_json: str,
    task_relation_json: str,
) -> tuple[list[FakeTaskDefinition], list[FakeWorkflowTaskRelation]]:
    current_dag = dags.get(workflow_code)
    current_tasks = (
        [] if current_dag is None else list(current_dag.taskDefinitionList or [])
    )
    current_tasks_by_code = {task.code: task for task in current_tasks}
    task_definitions: list[FakeTaskDefinition] = []
    effective_versions_by_code: dict[int, int] = {}
    for item in _json_array(task_definition_json, label="task_definition_json"):
        candidate = _fake_task_definition_from_payload(
            item,
            project_code=project_code,
        )
        current_task = current_tasks_by_code.get(candidate.code)
        if current_task is None:
            task_definitions.append(candidate)
            effective_versions_by_code[candidate.code] = candidate.version or 1
            continue
        changed = _normalized_task_definition_state(current_task) != (
            _normalized_task_definition_state(candidate)
        )
        current_version = current_task.version or 1
        effective_version = current_version + 1 if changed else current_version
        updated_task = replace(candidate, version=effective_version)
        task_definitions.append(updated_task)
        effective_versions_by_code[updated_task.code] = effective_version
    task_relations = _updated_fake_workflow_relations(
        task_relation_json,
        effective_versions_by_code=effective_versions_by_code,
    )
    return task_definitions, task_relations


def _updated_fake_workflow_relations(
    task_relation_json: str,
    *,
    effective_versions_by_code: Mapping[int, int],
) -> list[FakeWorkflowTaskRelation]:
    task_relations: list[FakeWorkflowTaskRelation] = []
    for item in _json_array(task_relation_json, label="task_relation_json"):
        relation_payload = dict(item)
        pre_task_code = _require_int(relation_payload["preTaskCode"])
        post_task_code = _require_int(relation_payload["postTaskCode"])
        if pre_task_code != 0 and pre_task_code in effective_versions_by_code:
            relation_payload["preTaskVersion"] = effective_versions_by_code[
                pre_task_code
            ]
        if post_task_code in effective_versions_by_code:
            relation_payload["postTaskVersion"] = effective_versions_by_code[
                post_task_code
            ]
        task_relations.append(_fake_task_relation_from_payload(relation_payload))
    return task_relations


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_json_value(value: object) -> JsonValue | None:
    if value is None:
        return None
    if not is_json_value(value):
        message = "Expected a JSON-compatible fake payload value"
        raise TypeError(message)
    return value


def _optional_enum(value: object) -> FakeEnumValue | None:
    if value is None:
        return None
    return FakeEnumValue(str(value))


def _global_param_map(value: str | None) -> dict[str, str] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        return None
    global_param_map: dict[str, str] = {}
    for item in parsed:
        if not isinstance(item, dict):
            continue
        prop = item.get("prop")
        raw_value = item.get("value")
        if not isinstance(prop, str):
            continue
        global_param_map[prop] = "" if raw_value is None else str(raw_value)
    return global_param_map


def _require_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        message = "Expected an int-compatible fake payload value"
        raise TypeError(message)
    return int(value)


@dataclass
class FakeTaskAdapter:
    workflow_tasks: dict[int, list[FakeTaskDefinition]]
    update_errors_by_code: dict[int, ApiResultError] | None = None
    update_calls: list[dict[str, object]] = field(default_factory=list)

    def list(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> list[FakeTaskDefinition]:
        tasks = self.workflow_tasks.get(workflow_code, [])
        return [task for task in tasks if task.projectCode == project_code]

    def get(self, *, code: int) -> FakeTaskDefinition:
        for tasks in self.workflow_tasks.values():
            for task in tasks:
                if task.code == code:
                    return task
        raise ApiResultError(
            result_code=10018,
            result_message=f"task code {code} not found",
        )

    def update(
        self,
        *,
        project_code: int,
        code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> None:
        self.update_calls.append(
            {
                "project_code": project_code,
                "code": code,
                "task_definition_json": task_definition_json,
                "upstream_codes": list(upstream_codes),
            }
        )
        if (
            self.update_errors_by_code is not None
            and code in self.update_errors_by_code
        ):
            raise self.update_errors_by_code[code]
        payload = json.loads(task_definition_json)
        if not isinstance(payload, dict):
            message = "task_definition_json must decode to a JSON object"
            raise TypeError(message)
        for workflow_code, tasks in self.workflow_tasks.items():
            for index, task in enumerate(tasks):
                if task.code != code:
                    continue
                if task.projectCode != project_code:
                    continue
                current_task_priority = task.taskPriority
                current_timeout_flag = task.timeoutFlag
                current_timeout_notify_strategy = task.timeoutNotifyStrategy
                current_task_execute_type = task.taskExecuteType
                current_flag = task.flag
                tasks[index] = replace(
                    task,
                    name=_optional_string(payload.get("name")) or task.name,
                    description=_optional_string(payload.get("description")),
                    task_type_value=_optional_string(payload.get("taskType"))
                    or task.taskType,
                    task_params_value=_optional_json_value(payload.get("taskParams")),
                    worker_group_value=_optional_string(payload.get("workerGroup")),
                    fail_retry_times_value=_require_int(
                        payload.get("failRetryTimes", task.failRetryTimes)
                    ),
                    fail_retry_interval_value=_require_int(
                        payload.get("failRetryInterval", task.failRetryInterval)
                    ),
                    timeout=_require_int(payload.get("timeout", task.timeout)),
                    delay_time_value=_require_int(
                        payload.get("delayTime", task.delayTime)
                    ),
                    resource_ids_value=_optional_string(
                        payload.get("resourceIds", task.resourceIds)
                    ),
                    environment_code_value=_require_int(
                        payload.get("environmentCode", task.environmentCode)
                    ),
                    task_group_id_value=_require_int(
                        payload.get("taskGroupId", task.taskGroupId)
                    ),
                    task_group_priority_value=_require_int(
                        payload.get("taskGroupPriority", task.taskGroupPriority)
                    ),
                    task_priority_value=(
                        _optional_enum(payload.get("taskPriority"))
                        if "taskPriority" in payload
                        else current_task_priority
                    ),
                    timeout_flag_value=(
                        _optional_enum(payload.get("timeoutFlag"))
                        if "timeoutFlag" in payload
                        else current_timeout_flag
                    ),
                    timeout_notify_strategy_value=(
                        _optional_enum(payload.get("timeoutNotifyStrategy"))
                        if "timeoutNotifyStrategy" in payload
                        else current_timeout_notify_strategy
                    ),
                    task_execute_type_value=(
                        _optional_enum(payload.get("taskExecuteType"))
                        if "taskExecuteType" in payload
                        else current_task_execute_type
                    ),
                    flag_value=(
                        _optional_enum(payload.get("flag"))
                        if "flag" in payload
                        else current_flag
                    ),
                    cpu_quota_value=(
                        None
                        if payload.get("cpuQuota", task.cpuQuota) is None
                        else _require_int(payload.get("cpuQuota", task.cpuQuota))
                    ),
                    memory_max_value=(
                        None
                        if payload.get("memoryMax", task.memoryMax) is None
                        else _require_int(payload.get("memoryMax", task.memoryMax))
                    ),
                    version=(task.version or 0) + 1,
                )
                del workflow_code, upstream_codes
                return
        raise ApiResultError(
            result_code=10018,
            result_message=f"task code {code} not found",
        )


@dataclass(frozen=True)
class FakeWorkflowInstance:
    id: int
    workflow_definition_code_value: int | None = None
    workflow_definition_version_value: int = 0
    project_code_value: int | None = None
    dag_data_value: FakeDag | None = None
    state_value: FakeEnumValue | None = None
    recovery_value: FakeEnumValue | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    run_times_value: int = 0
    name: str | None = None
    host: str | None = None
    command_type_value: FakeEnumValue | None = None
    task_depend_type_value: FakeEnumValue | None = None
    failure_strategy_value: FakeEnumValue | None = None
    warning_type_value: FakeEnumValue | None = None
    schedule_time_value: str | None = None
    executor_id_value: int = 0
    executor_name_value: str | None = None
    tenant_code_value: str | None = None
    queue_value: str | None = None
    duration_value: str | None = None
    workflow_instance_priority_value: FakeEnumValue | None = None
    worker_group_value: str | None = None
    environment_code_value: int | None = None
    timeout: int = 0
    dry_run_value: int = 0
    restart_time_value: str | None = None

    @property
    def workflowDefinitionCode(self) -> int | None:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionVersion(self) -> int:  # noqa: N802
        return self.workflow_definition_version_value

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        return self.project_code_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def recovery(self) -> FakeEnumValue | None:
        return self.recovery_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def runTimes(self) -> int:  # noqa: N802
        return self.run_times_value

    @property
    def commandType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.command_type_value

    @property
    def taskDependType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_depend_type_value

    @property
    def failureStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.failure_strategy_value

    @property
    def warningType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.warning_type_value

    @property
    def scheduleTime(self) -> str | None:  # noqa: N802
        return self.schedule_time_value

    @property
    def executorId(self) -> int:  # noqa: N802
        return self.executor_id_value

    @property
    def executorName(self) -> str | None:  # noqa: N802
        return self.executor_name_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def queue(self) -> str | None:
        return self.queue_value

    @property
    def duration(self) -> str | None:
        return self.duration_value

    @property
    def workflowInstancePriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.workflow_instance_priority_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def dryRun(self) -> int:  # noqa: N802
        return self.dry_run_value

    @property
    def restartTime(self) -> str | None:  # noqa: N802
        return self.restart_time_value

    @property
    def dagData(self) -> FakeDag | None:  # noqa: N802
        return self.dag_data_value


@dataclass(frozen=True)
class FakeWorkflowInstanceSubWorkflow:
    sub_workflow_instance_id_value: int | None

    @property
    def subWorkflowInstanceId(self) -> int | None:  # noqa: N802
        return self.sub_workflow_instance_id_value


@dataclass(frozen=True)
class FakeWorkflowInstanceParent:
    parent_workflow_instance_value: int | None

    @property
    def parentWorkflowInstance(self) -> int | None:  # noqa: N802
        return self.parent_workflow_instance_value


@dataclass(frozen=True)
class FakeWorkflowInstancePage:
    total_list_value: list[FakeWorkflowInstance] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeWorkflowInstance] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass
class FakeWorkflowInstanceAdapter:
    workflow_instances: list[FakeWorkflowInstance]
    workflow_instance_sequences_by_id: dict[int, list[FakeWorkflowInstance]] | None = (
        None
    )
    update_errors_by_id: dict[int, ApiResultError] | None = None
    sub_workflow_instance_ids_by_task_id: dict[int, int] = field(default_factory=dict)
    parent_workflow_instance_ids_by_sub_id: dict[int, int] = field(default_factory=dict)
    update_calls: list[dict[str, object]] = field(default_factory=list)
    stopped_ids: list[int] = field(default_factory=list)
    rerun_ids: list[int] = field(default_factory=list)
    recovered_failed_ids: list[int] = field(default_factory=list)
    executed_tasks: list[tuple[int, int, str]] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        project_code: int | None = None,
        workflow_code: int | None = None,
        project_name: str | None = None,
        workflow_name: str | None = None,
        search: str | None = None,
        executor: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        state: str | None = None,
    ) -> FakeWorkflowInstancePage:
        del project_name
        filtered = list(self.workflow_instances)
        if project_code is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.projectCode == project_code
            ]
        if workflow_code is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.workflowDefinitionCode == workflow_code
            ]
        if workflow_name is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.name is not None
                and workflow_name.lower() in workflow_instance.name.lower()
            ]
        if search is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.name is not None
                and search.lower() in workflow_instance.name.lower()
            ]
        if executor is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.executorName == executor
            ]
        if host is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.host is not None
                and host.lower() in workflow_instance.host.lower()
            ]
        if start_time is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.startTime is not None
                and workflow_instance.startTime >= start_time
            ]
        if end_time is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.startTime is not None
                and workflow_instance.startTime <= end_time
            ]
        if state is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.state is not None
                and workflow_instance.state.value == state
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeWorkflowInstancePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, workflow_instance_id: int) -> FakeWorkflowInstance:
        sequence = (self.workflow_instance_sequences_by_id or {}).get(
            workflow_instance_id
        )
        if sequence:
            if len(sequence) > 1:
                return sequence.pop(0)
            return sequence[0]
        for workflow_instance in self.workflow_instances:
            if workflow_instance.id == workflow_instance_id:
                return workflow_instance
        raise ApiResultError(
            result_code=10211,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )

    def update(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None = None,
        locations: str | None = None,
        timeout: int | None = None,
        schedule_time: str | None = None,
    ) -> FakeWorkflow:
        self.update_calls.append(
            {
                "project_code": project_code,
                "workflow_instance_id": workflow_instance_id,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "sync_define": sync_define,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "schedule_time": schedule_time,
            }
        )
        if (
            self.update_errors_by_id is not None
            and workflow_instance_id in self.update_errors_by_id
        ):
            raise self.update_errors_by_id[workflow_instance_id]
        for index, workflow_instance in enumerate(self.workflow_instances):
            if workflow_instance.id != workflow_instance_id:
                continue
            if workflow_instance.projectCode != project_code:
                break
            current_dag = workflow_instance.dagData
            current_workflow = (
                None if current_dag is None else current_dag.workflowDefinition
            )
            workflow_code = workflow_instance.workflowDefinitionCode
            if current_dag is None or current_workflow is None or workflow_code is None:
                message = "workflow instance fake update requires dagData"
                raise TypeError(message)
            updated_tasks, updated_relations = (
                _updated_fake_workflow_tasks_and_relations(
                    dags={workflow_code: current_dag},
                    workflow_code=workflow_code,
                    project_code=project_code,
                    task_definition_json=task_definition_json,
                    task_relation_json=task_relation_json,
                )
            )
            current_version = (
                current_workflow.version
                or workflow_instance.workflowDefinitionVersion
                or 1
            )
            next_version = current_version + 1
            updated_workflow = replace(
                current_workflow,
                version=next_version,
                global_params_value=global_params,
                global_param_map_value=_global_param_map(global_params),
                timeout=current_workflow.timeout if timeout is None else timeout,
                update_time_value="2026-04-13 12:00:00",
            )
            updated_dag = replace(
                current_dag,
                workflow_definition_value=updated_workflow,
                task_definition_list_value=updated_tasks,
                workflow_task_relation_list_value=updated_relations,
            )
            updated_instance = replace(
                workflow_instance,
                workflow_definition_version_value=next_version,
                dag_data_value=updated_dag,
                timeout=updated_workflow.timeout,
                schedule_time_value=(
                    workflow_instance.scheduleTime
                    if schedule_time is None
                    else schedule_time
                ),
            )
            self.workflow_instances[index] = updated_instance
            sequence = (self.workflow_instance_sequences_by_id or {}).get(
                workflow_instance_id
            )
            if sequence:
                sequence[-1] = updated_instance
            return updated_workflow
        raise ApiResultError(
            result_code=10211,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )

    def parent_instance_by_sub_workflow(
        self,
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> FakeWorkflowInstanceParent:
        del project_code
        parent_workflow_instance_id = self.parent_workflow_instance_ids_by_sub_id.get(
            sub_workflow_instance_id
        )
        if parent_workflow_instance_id is None:
            raise ApiResultError(
                result_code=50007,
                result_message="sub workflow instance not found",
            )
        return FakeWorkflowInstanceParent(
            parent_workflow_instance_value=parent_workflow_instance_id
        )

    def sub_workflow_instance_by_task(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> FakeWorkflowInstanceSubWorkflow:
        del project_code
        sub_workflow_instance_id = self.sub_workflow_instance_ids_by_task_id.get(
            task_instance_id
        )
        if sub_workflow_instance_id is None:
            raise ApiResultError(
                result_code=50007,
                result_message="sub workflow instance not found",
            )
        return FakeWorkflowInstanceSubWorkflow(
            sub_workflow_instance_id_value=sub_workflow_instance_id
        )

    def stop(self, *, workflow_instance_id: int) -> None:
        self.stopped_ids.append(workflow_instance_id)
        self._set_state(workflow_instance_id=workflow_instance_id, state="READY_STOP")

    def rerun(self, *, workflow_instance_id: int) -> None:
        self.rerun_ids.append(workflow_instance_id)
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def recover_failed(self, *, workflow_instance_id: int) -> None:
        self.recovered_failed_ids.append(workflow_instance_id)
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def execute_task(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code
        self.executed_tasks.append((workflow_instance_id, task_code, scope))
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def _set_state(self, *, workflow_instance_id: int, state: str) -> None:
        for index, workflow_instance in enumerate(self.workflow_instances):
            if workflow_instance.id == workflow_instance_id:
                self.workflow_instances[index] = replace(
                    workflow_instance,
                    state_value=FakeEnumValue(state),
                )
                return
        raise ApiResultError(
            result_code=10211,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )


@dataclass(frozen=True)
class FakeTaskInstance:
    id: int
    name: str | None = None
    task_type_value: str | None = None
    workflow_instance_id_value: int = 0
    workflow_instance_name_value: str | None = None
    project_code_value: int | None = None
    task_code_value: int = 0
    task_definition_version_value: int = 0
    process_definition_name_value: str | None = None
    state_value: FakeEnumValue | None = None
    first_submit_time_value: str | None = None
    submit_time_value: str | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    host: str | None = None
    log_path_value: str | None = None
    retry_times_value: int = 0
    duration_value: str | None = None
    executor_name_value: str | None = None
    worker_group_value: str | None = None
    environment_code_value: int | None = None
    delay_time_value: int = 0
    task_params_value: str | None = None
    dry_run_value: int = 0
    task_group_id_value: int = 0
    task_execute_type_value: FakeEnumValue | None = None

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def workflowInstanceId(self) -> int:  # noqa: N802
        return self.workflow_instance_id_value

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        return self.workflow_instance_name_value

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        return self.project_code_value

    @property
    def taskCode(self) -> int:  # noqa: N802
        return self.task_code_value

    @property
    def taskDefinitionVersion(self) -> int:  # noqa: N802
        return self.task_definition_version_value

    @property
    def processDefinitionName(self) -> str | None:  # noqa: N802
        return self.process_definition_name_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def firstSubmitTime(self) -> str | None:  # noqa: N802
        return self.first_submit_time_value

    @property
    def submitTime(self) -> str | None:  # noqa: N802
        return self.submit_time_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def logPath(self) -> str | None:  # noqa: N802
        return self.log_path_value

    @property
    def retryTimes(self) -> int:  # noqa: N802
        return self.retry_times_value

    @property
    def duration(self) -> str | None:
        return self.duration_value

    @property
    def executorName(self) -> str | None:  # noqa: N802
        return self.executor_name_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def delayTime(self) -> int:  # noqa: N802
        return self.delay_time_value

    @property
    def taskParams(self) -> str | None:  # noqa: N802
        return self.task_params_value

    @property
    def dryRun(self) -> int:  # noqa: N802
        return self.dry_run_value

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        return self.task_group_id_value

    @property
    def taskExecuteType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_execute_type_value


@dataclass(frozen=True)
class FakeTaskInstancePage:
    total_list_value: list[FakeTaskInstance] | None
    total: int | None
    total_page_value: int | None
    page_size_value: int | None
    current_page_value: int | None
    page_no_value: int | None = None

    @property
    def totalList(self) -> list[FakeTaskInstance] | None:  # noqa: N802
        return self.total_list_value

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        return self.total_page_value

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        return self.page_size_value

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        return self.current_page_value

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        return self.page_no_value


@dataclass(frozen=True)
class FakeTaskLog:
    line_num_value: int
    message_value: str | None

    @property
    def lineNum(self) -> int:  # noqa: N802
        return self.line_num_value

    @property
    def message(self) -> str | None:
        return self.message_value


@dataclass
class FakeTaskInstanceAdapter:
    task_instances: list[FakeTaskInstance]
    task_instance_sequences_by_id: dict[int, list[FakeTaskInstance]] | None = None
    log_messages_by_task_instance_id: dict[int, list[str]] | None = None
    force_success_ids: list[int] = field(default_factory=list)
    savepoint_ids: list[int] = field(default_factory=list)
    stopped_ids: list[int] = field(default_factory=list)

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_instance_id: int | None = None,
        workflow_instance_name: str | None = None,
        workflow_definition_name: str | None = None,
        search: str | None = None,
        task_name: str | None = None,
        task_code: int | None = None,
        executor: str | None = None,
        state: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        task_execute_type: str | None = None,
    ) -> FakeTaskInstancePage:
        filtered = [
            task_instance
            for task_instance in self.task_instances
            if _matches_task_instance_query(
                task_instance,
                project_code=project_code,
                workflow_instance_id=workflow_instance_id,
                workflow_instance_name=workflow_instance_name,
                workflow_definition_name=workflow_definition_name,
                search=search,
                task_name=task_name,
                task_code=task_code,
                executor=executor,
                state=state,
                host=host,
                start_time=start_time,
                end_time=end_time,
                task_execute_type=task_execute_type,
            )
        ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeTaskInstancePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> FakeTaskInstance:
        sequence = (self.task_instance_sequences_by_id or {}).get(task_instance_id)
        if sequence:
            if len(sequence) > 1:
                return sequence.pop(0)
            return sequence[0]
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                return task_instance
        raise ApiResultError(
            result_code=10103,
            result_message=f"task instance id {task_instance_id} not found",
        )

    def log_chunk(
        self,
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> FakeTaskLog:
        lines = list(
            (self.log_messages_by_task_instance_id or {}).get(task_instance_id, [])
        )
        if not lines:
            lines = [
                f"task instance {task_instance_id} log line {index}"
                for index in range(1, 6)
            ]
        chunk_lines = lines[skip_line_num : skip_line_num + limit]
        return FakeTaskLog(
            line_num_value=len(chunk_lines),
            message_value="\n".join(chunk_lines),
        )

    def force_success(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.force_success_ids.append(task_instance_id)
                object.__setattr__(
                    task_instance,
                    "state_value",
                    FakeEnumValue("FORCED_SUCCESS"),
                )
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )

    def savepoint(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.savepoint_ids.append(task_instance_id)
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )

    def stop(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.stopped_ids.append(task_instance_id)
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )


def _matches_task_instance_query(
    task_instance: FakeTaskInstance,
    *,
    project_code: int,
    workflow_instance_id: int | None,
    workflow_instance_name: str | None,
    workflow_definition_name: str | None,
    search: str | None,
    task_name: str | None,
    task_code: int | None,
    executor: str | None,
    state: str | None,
    host: str | None,
    start_time: str | None,
    end_time: str | None,
    task_execute_type: str | None,
) -> bool:
    start_value = task_instance.startTime
    state_value = None if task_instance.state is None else task_instance.state.value
    execute_type_value = (
        None
        if task_instance.taskExecuteType is None
        else task_instance.taskExecuteType.value
    )
    return all(
        (
            task_instance.projectCode == project_code,
            workflow_instance_id is None
            or task_instance.workflowInstanceId == workflow_instance_id,
            workflow_instance_name is None
            or task_instance.workflowInstanceName == workflow_instance_name,
            workflow_definition_name is None
            or task_instance.processDefinitionName == workflow_definition_name,
            search is None
            or (
                task_instance.name is not None
                and search.lower() in task_instance.name.lower()
            ),
            task_name is None or task_instance.name == task_name,
            task_code is None or task_instance.taskCode == task_code,
            executor is None or task_instance.executorName == executor,
            state is None or state_value == state,
            host is None
            or (
                task_instance.host is not None
                and host.lower() in task_instance.host.lower()
            ),
            start_time is None
            or (start_value is not None and start_value >= start_time),
            end_time is None or (start_value is not None and start_value <= end_time),
            task_execute_type is None or execute_type_value == task_execute_type,
        )
    )


@dataclass
class FakeScheduleAdapter:
    schedules: list[FakeSchedule]
    preview_times_value: Sequence[str] | None = None

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_code: int | None = None,
        search: str | None = None,
    ) -> FakeSchedulePage:
        filtered = [
            schedule
            for schedule in self.schedules
            if schedule.project_code_value == project_code
        ]
        if workflow_code is not None:
            filtered = [
                schedule
                for schedule in filtered
                if schedule.workflowDefinitionCode == workflow_code
            ]
        if search is not None:
            filtered = [
                schedule
                for schedule in filtered
                if schedule.workflowDefinitionName is not None
                and search.lower() in schedule.workflowDefinitionName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = 0 if total == 0 else ((total - 1) // page_size) + 1
        return FakeSchedulePage(
            total_list_value=filtered[start:stop],
            total=total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, schedule_id: int) -> FakeSchedule:
        for schedule in self.schedules:
            if schedule.id == schedule_id:
                return schedule
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def preview(
        self,
        *,
        project_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
    ) -> Sequence[str]:
        del project_code, crontab, start_time, end_time, timezone_id
        if self.preview_times_value is not None:
            return list(self.preview_times_value)
        return [
            "2024-01-01 02:00:00",
            "2024-01-02 02:00:00",
            "2024-01-03 02:00:00",
            "2024-01-04 02:00:00",
            "2024-01-05 02:00:00",
        ]

    def create(
        self,
        *,
        workflow_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
        failure_strategy: str | None = None,
        warning_type: str | None = None,
        warning_group_id: int = 0,
        workflow_instance_priority: str | None = None,
        worker_group: str | None = None,
        tenant_code: str | None = None,
        environment_code: int = 0,
    ) -> FakeSchedule:
        next_id = max((schedule.id or 0 for schedule in self.schedules), default=0) + 1
        created = FakeSchedule(
            id=next_id,
            workflow_definition_code_value=workflow_code,
            start_time_value=start_time,
            end_time_value=end_time,
            timezone_id_value=timezone_id,
            crontab_value=crontab,
            failure_strategy_value=(
                None if failure_strategy is None else FakeEnumValue(failure_strategy)
            ),
            warning_type_value=(
                None if warning_type is None else FakeEnumValue(warning_type)
            ),
            workflow_instance_priority_value=(
                None
                if workflow_instance_priority is None
                else FakeEnumValue(workflow_instance_priority)
            ),
            release_state_value=FakeEnumValue("OFFLINE"),
            warning_group_id_value=warning_group_id,
            worker_group_value=worker_group,
            tenant_code_value=tenant_code,
            environment_code_value=environment_code,
        )
        self.schedules.append(created)
        return created

    def update(
        self,
        *,
        schedule_id: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
        failure_strategy: str | None = None,
        warning_type: str | None = None,
        warning_group_id: int = 0,
        workflow_instance_priority: str | None = None,
        worker_group: str | None = None,
        environment_code: int = 0,
    ) -> FakeSchedule:
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                updated = replace(
                    schedule,
                    start_time_value=start_time,
                    end_time_value=end_time,
                    timezone_id_value=timezone_id,
                    crontab_value=crontab,
                    failure_strategy_value=(
                        None
                        if failure_strategy is None
                        else FakeEnumValue(failure_strategy)
                    ),
                    warning_type_value=(
                        None if warning_type is None else FakeEnumValue(warning_type)
                    ),
                    workflow_instance_priority_value=(
                        None
                        if workflow_instance_priority is None
                        else FakeEnumValue(workflow_instance_priority)
                    ),
                    warning_group_id_value=warning_group_id,
                    worker_group_value=worker_group,
                    environment_code_value=environment_code,
                )
                self.schedules[index] = updated
                return updated
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def delete(self, *, schedule_id: int) -> bool:
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                self.schedules.pop(index)
                return True
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def online(self, *, schedule_id: int) -> FakeSchedule:
        return self._set_release_state(
            schedule_id=schedule_id,
            release_state="ONLINE",
        )

    def offline(self, *, schedule_id: int) -> FakeSchedule:
        return self._set_release_state(
            schedule_id=schedule_id,
            release_state="OFFLINE",
        )

    def _set_release_state(
        self,
        *,
        schedule_id: int,
        release_state: str,
    ) -> FakeSchedule:
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                updated = replace(
                    schedule,
                    release_state_value=FakeEnumValue(release_state),
                )
                self.schedules[index] = updated
                return updated
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )


@dataclass(frozen=True)
class FakeUpstreamSession:
    task_types: FakeTaskTypeAdapter
    projects: FakeProjectAdapter
    project_parameters: FakeProjectParameterAdapter
    project_preferences: FakeProjectPreferenceAdapter
    project_worker_groups: FakeProjectWorkerGroupAdapter
    access_tokens: FakeAccessTokenAdapter
    clusters: FakeClusterAdapter
    environments: FakeEnvironmentAdapter
    datasources: FakeDataSourceAdapter
    namespaces: FakeNamespaceAdapter
    ui_plugins: FakeUiPluginAdapter
    alert_plugins: FakeAlertPluginAdapter
    resources: FakeResourceAdapter
    alert_groups: FakeAlertGroupAdapter
    queues: FakeQueueAdapter
    worker_groups: FakeWorkerGroupAdapter
    task_groups: FakeTaskGroupAdapter
    tenants: FakeTenantAdapter
    users: FakeUserAdapter
    audits: FakeAuditAdapter
    monitor: FakeMonitorAdapter
    workflows: FakeWorkflowAdapter
    workflow_lineages: FakeWorkflowLineageAdapter
    tasks: FakeTaskAdapter
    schedules: FakeScheduleAdapter
    workflow_instances: FakeWorkflowInstanceAdapter
    task_instances: FakeTaskInstanceAdapter


def empty_workflow_adapter() -> FakeWorkflowAdapter:
    return FakeWorkflowAdapter(workflows=[], dags={})


def empty_workflow_lineage_adapter() -> FakeWorkflowLineageAdapter:
    return FakeWorkflowLineageAdapter(project_lineages={}, workflow_lineages={})


def empty_task_type_adapter() -> FakeTaskTypeAdapter:
    return FakeTaskTypeAdapter(task_types=[])


def empty_project_parameter_adapter() -> FakeProjectParameterAdapter:
    return FakeProjectParameterAdapter(project_parameters=[])


def empty_project_preference_adapter() -> FakeProjectPreferenceAdapter:
    return FakeProjectPreferenceAdapter(project_preferences=[])


def empty_project_worker_group_adapter() -> FakeProjectWorkerGroupAdapter:
    return FakeProjectWorkerGroupAdapter(project_worker_groups=[])


def empty_access_token_adapter() -> FakeAccessTokenAdapter:
    return FakeAccessTokenAdapter(access_tokens=[])


def empty_cluster_adapter() -> FakeClusterAdapter:
    return FakeClusterAdapter(clusters=[])


def empty_environment_adapter() -> FakeEnvironmentAdapter:
    return FakeEnvironmentAdapter(environments=[])


def empty_datasource_adapter() -> FakeDataSourceAdapter:
    return FakeDataSourceAdapter(datasources=[])


def empty_namespace_adapter() -> FakeNamespaceAdapter:
    return FakeNamespaceAdapter(namespaces=[])


def empty_ui_plugin_adapter() -> FakeUiPluginAdapter:
    return FakeUiPluginAdapter(plugin_defines=[])


def empty_alert_plugin_adapter() -> FakeAlertPluginAdapter:
    return FakeAlertPluginAdapter(alert_plugins=[])


def empty_alert_group_adapter() -> FakeAlertGroupAdapter:
    return FakeAlertGroupAdapter(alert_groups=[])


def empty_resource_adapter() -> FakeResourceAdapter:
    return FakeResourceAdapter(resources=[])


def empty_queue_adapter() -> FakeQueueAdapter:
    return FakeQueueAdapter(queues=[])


def empty_worker_group_adapter() -> FakeWorkerGroupAdapter:
    return FakeWorkerGroupAdapter(worker_groups=[])


def empty_task_group_adapter() -> FakeTaskGroupAdapter:
    return FakeTaskGroupAdapter(task_groups=[])


def empty_tenant_adapter() -> FakeTenantAdapter:
    return FakeTenantAdapter(tenants=[])


def empty_user_adapter() -> FakeUserAdapter:
    return FakeUserAdapter(
        users=[],
        current_user=FakeUser(
            id=1,
            user_name_value="current-user",
            email=None,
        ),
    )


def empty_audit_adapter() -> FakeAuditAdapter:
    return FakeAuditAdapter(audit_logs=[])


def empty_monitor_adapter() -> FakeMonitorAdapter:
    return FakeMonitorAdapter(servers_by_node_type={})


def empty_task_adapter() -> FakeTaskAdapter:
    return FakeTaskAdapter(workflow_tasks={})


def empty_schedule_adapter() -> FakeScheduleAdapter:
    return FakeScheduleAdapter(schedules=[])


def empty_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
    return FakeWorkflowInstanceAdapter(workflow_instances=[])


def empty_task_instance_adapter() -> FakeTaskInstanceAdapter:
    return FakeTaskInstanceAdapter(task_instances=[])


@contextmanager
def fake_service_runtime(
    project_adapter: FakeProjectAdapter,
    *,
    profile: ClusterProfile,
    context: SessionContext | None = None,
    http_client: FakeHttpClient | None = None,
    task_type_adapter: FakeTaskTypeAdapter | None = None,
    project_parameter_adapter: FakeProjectParameterAdapter | None = None,
    project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
    project_worker_group_adapter: FakeProjectWorkerGroupAdapter | None = None,
    access_token_adapter: FakeAccessTokenAdapter | None = None,
    cluster_adapter: FakeClusterAdapter | None = None,
    environment_adapter: FakeEnvironmentAdapter | None = None,
    datasource_adapter: FakeDataSourceAdapter | None = None,
    namespace_adapter: FakeNamespaceAdapter | None = None,
    ui_plugin_adapter: FakeUiPluginAdapter | None = None,
    alert_plugin_adapter: FakeAlertPluginAdapter | None = None,
    resource_adapter: FakeResourceAdapter | None = None,
    alert_group_adapter: FakeAlertGroupAdapter | None = None,
    queue_adapter: FakeQueueAdapter | None = None,
    worker_group_adapter: FakeWorkerGroupAdapter | None = None,
    task_group_adapter: FakeTaskGroupAdapter | None = None,
    tenant_adapter: FakeTenantAdapter | None = None,
    user_adapter: FakeUserAdapter | None = None,
    audit_adapter: FakeAuditAdapter | None = None,
    monitor_adapter: FakeMonitorAdapter | None = None,
    workflow_adapter: FakeWorkflowAdapter | None = None,
    workflow_lineage_adapter: FakeWorkflowLineageAdapter | None = None,
    task_adapter: FakeTaskAdapter | None = None,
    schedule_adapter: FakeScheduleAdapter | None = None,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter | None = None,
    task_instance_adapter: FakeTaskInstanceAdapter | None = None,
) -> Iterator[ServiceRuntime]:
    yield ServiceRuntime(
        profile=profile,
        context=context or SessionContext(),
        http_client=http_client or FakeHttpClient(),
        upstream=FakeUpstreamSession(
            task_types=task_type_adapter or empty_task_type_adapter(),
            projects=project_adapter,
            project_parameters=(
                project_parameter_adapter or empty_project_parameter_adapter()
            ),
            project_preferences=(
                project_preference_adapter or empty_project_preference_adapter()
            ),
            project_worker_groups=(
                project_worker_group_adapter or empty_project_worker_group_adapter()
            ),
            access_tokens=access_token_adapter or empty_access_token_adapter(),
            clusters=cluster_adapter or empty_cluster_adapter(),
            environments=environment_adapter or empty_environment_adapter(),
            datasources=datasource_adapter or empty_datasource_adapter(),
            namespaces=namespace_adapter or empty_namespace_adapter(),
            ui_plugins=ui_plugin_adapter or empty_ui_plugin_adapter(),
            alert_plugins=alert_plugin_adapter or empty_alert_plugin_adapter(),
            resources=resource_adapter or empty_resource_adapter(),
            alert_groups=alert_group_adapter or empty_alert_group_adapter(),
            queues=queue_adapter or empty_queue_adapter(),
            worker_groups=worker_group_adapter or empty_worker_group_adapter(),
            task_groups=task_group_adapter or empty_task_group_adapter(),
            tenants=tenant_adapter or empty_tenant_adapter(),
            users=user_adapter or empty_user_adapter(),
            audits=audit_adapter or empty_audit_adapter(),
            monitor=monitor_adapter or empty_monitor_adapter(),
            workflows=workflow_adapter or empty_workflow_adapter(),
            workflow_lineages=(
                workflow_lineage_adapter or empty_workflow_lineage_adapter()
            ),
            tasks=task_adapter or empty_task_adapter(),
            schedules=schedule_adapter or empty_schedule_adapter(),
            workflow_instances=workflow_instance_adapter
            or empty_workflow_instance_adapter(),
            task_instances=task_instance_adapter or empty_task_instance_adapter(),
        ),
    )
