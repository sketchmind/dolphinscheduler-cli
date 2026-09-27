"""In-memory datasources collaborators with explicit test-owned state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject


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
    detail_payload_value: JsonObject | None = None

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

    def detail_payload(self) -> JsonObject:
        payload: JsonObject = {
            "id": self.id,
            "name": self.name,
            "note": self.note,
            "type": None if self.type is None else self.type.value,
        }
        if self.detail_payload_value is not None:
            payload.update(self.detail_payload_value)
        return payload


@dataclass(frozen=True)
class FakeDataSourcePage(_FakePage[FakeDataSource]):
    pass


@dataclass
class FakeDataSourceAdapter:
    datasources: list[FakeDataSource]
    connection_test_results: dict[int, bool] = field(default_factory=dict)
    authorized_by_user_id: dict[int, set[int]] = field(default_factory=dict)
    blank_password_preserves_existing: bool = True
    list_error: ApiResultError | None = None
    get_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeDataSourcePage:
        if self.list_error is not None:
            raise self.list_error
        filtered = list(self.datasources)
        if search is not None:
            filtered = [
                datasource
                for datasource in filtered
                if datasource.name is not None
                and search.lower() in datasource.name.lower()
            ]
        return FakeDataSourcePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, datasource_id: int) -> JsonObject:
        if datasource_id in self.get_errors_by_id:
            raise self.get_errors_by_id[datasource_id]
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
            detail_payload_value=dict(payload),
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


def empty_datasource_adapter() -> FakeDataSourceAdapter:
    return FakeDataSourceAdapter(datasources=[])
