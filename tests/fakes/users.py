"""In-memory users collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from tests.fakes.workers import (
        FakeTenant,
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
class FakeAccessTokenPage(_FakePage[FakeAccessToken]):
    pass


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
        return FakeAccessTokenPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
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
class FakeUserPage(_FakePage[FakeUser]):
    pass


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
        return FakeUserPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
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


def empty_access_token_adapter() -> FakeAccessTokenAdapter:
    return FakeAccessTokenAdapter(access_tokens=[])


def empty_user_adapter() -> FakeUserAdapter:
    return FakeUserAdapter(
        users=[],
        current_user=FakeUser(
            id=1,
            user_name_value="current-user",
            email=None,
        ),
    )
