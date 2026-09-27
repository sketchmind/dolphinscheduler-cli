from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Protocol, TypeVar, cast

from dsctl.errors import NotFoundError
from dsctl.services import access_token as access_token_service
from dsctl.services import user as user_service
from dsctl.upstream.access_tokens import AccessTokenDomain
from dsctl.upstream.users import (
    _USER_RECIPES,
    PermissionDataSource,
    PermissionNamespace,
    PermissionProject,
    UserDomain,
)
from tests.fakes import (
    FakeAccessToken,
    FakeAccessTokenAdapter,
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeNamespace,
    FakeNamespaceAdapter,
    FakeProject,
    FakeProjectAdapter,
    FakeTenantAdapter,
    FakeUserAdapter,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pytest

    from dsctl.upstream.protocol import AccessTokenPageRecord


class _PermissionProjection(Protocol):
    @property
    def id(self) -> int:
        """Return the native permission identity."""


ProjectionT = TypeVar("ProjectionT", bound=_PermissionProjection)
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class FakeUserPermissions:
    """In-memory implementation of the user-domain permission port."""

    users: FakeUserAdapter
    projects: FakeProjectAdapter
    datasources: FakeDataSourceAdapter
    namespaces: FakeNamespaceAdapter

    def grant_project(self, *, user_id: int, selector: str) -> PermissionProject:
        project = self._project(selector)
        self.users.grant_project_by_code(user_id=user_id, project_code=project.code)
        return self._project_projection(project)

    def revoke_project(self, *, user_id: int, selector: str) -> PermissionProject:
        project = self._project(selector)
        self.users.revoke_project(user_id=user_id, project_code=project.code)
        return self._project_projection(project)

    def change_datasources(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionDataSource], list[PermissionDataSource]]:
        projections = [
            self._datasource_projection(item) for item in self.datasources.datasources
        ]
        requested = self._select_many(
            selectors,
            projections,
            name=lambda item: item.name,
        )
        final_ids = set(self.users.granted_datasources_by_user_id.get(user_id, set()))
        if grant:
            final_ids.update(item.id for item in requested)
        else:
            final_ids.difference_update(item.id for item in requested)
        self.users.grant_datasources(
            user_id=user_id,
            datasource_ids=sorted(final_ids),
        )
        return requested, [item for item in projections if item.id in final_ids]

    def change_namespaces(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionNamespace], list[PermissionNamespace]]:
        projections = [
            self._namespace_projection(item) for item in self.namespaces.namespaces
        ]
        requested = self._select_many(
            selectors,
            projections,
            name=lambda item: item.namespace,
        )
        final_ids = set(self.users.granted_namespaces_by_user_id.get(user_id, set()))
        if grant:
            final_ids.update(item.id for item in requested)
        else:
            final_ids.difference_update(item.id for item in requested)
        self.users.grant_namespaces(
            user_id=user_id,
            namespace_ids=sorted(final_ids),
        )
        return requested, [item for item in projections if item.id in final_ids]

    def _project(self, selector: str) -> FakeProject:
        numeric = int(selector) if selector.isdigit() else None
        matches = [
            project
            for project in self.projects.projects
            if project.code == numeric or project.name == selector
        ]
        if len(matches) == 1:
            return matches[0]
        message = f"Project {selector!r} was not found"
        raise NotFoundError(message)

    @staticmethod
    def _project_projection(project: FakeProject) -> PermissionProject:
        return PermissionProject(
            id=project.id or project.code,
            code=project.code,
            name=project.name or "",
            description=project.description,
        )

    @staticmethod
    def _datasource_projection(item: FakeDataSource) -> PermissionDataSource:
        return PermissionDataSource(
            id=item.id,
            name=item.name or "",
            note=item.note,
            type=item.type,
        )

    @staticmethod
    def _namespace_projection(item: FakeNamespace) -> PermissionNamespace:
        return PermissionNamespace(
            id=item.id,
            namespace=item.namespace or "",
            clusterCode=item.clusterCode,
            clusterName=item.clusterName,
        )

    @staticmethod
    def _select_many(
        selectors: Sequence[str],
        candidates: Sequence[ProjectionT],
        *,
        name: Callable[[ProjectionT], str],
    ) -> list[ProjectionT]:
        selected: dict[int, ProjectionT] = {}
        for selector in selectors:
            numeric = int(selector) if selector.isdigit() else None
            matches = [
                item
                for item in candidates
                if item.id == numeric or name(item) == selector
            ]
            if len(matches) != 1:
                message = f"Permission selector {selector!r} was not found"
                raise NotFoundError(message)
            selected[matches[0].id] = matches[0]
        return list(selected.values())


@dataclass(frozen=True)
class FakeTokenOperations:
    """In-memory implementation of the access-token persistence port."""

    adapter: FakeAccessTokenAdapter

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AccessTokenPageRecord:
        return cast(
            "AccessTokenPageRecord",
            self.adapter.list(
                page_no=page_no,
                page_size=page_size,
                search=search,
            ),
        )

    def get(self, *, token_id: int) -> FakeAccessToken:
        for token in self.adapter.access_tokens:
            if token.id == token_id:
                return token
        message = f"Access-token id {token_id} was not found"
        raise NotFoundError(
            message,
            details={"resource": "access-token", "id": token_id},
        )

    def create(
        self,
        *,
        user_id: int,
        expire_time: str,
        token: str | None,
    ) -> FakeAccessToken:
        return self.adapter.create(
            user_id=user_id,
            expire_time=expire_time,
            token=token,
        )

    def update(
        self,
        *,
        token_id: int,
        user_id: int,
        expire_time: str,
        token: str | None,
        regenerate_token: bool,
        previous_token: str | None,
    ) -> FakeAccessToken:
        del previous_token
        return self.adapter.update(
            token_id=token_id,
            user_id=user_id,
            expire_time=expire_time,
            token=None if regenerate_token else token,
        )

    def delete(self, *, token_id: int) -> bool:
        return self.adapter.delete(token_id=token_id)

    def generate(self, *, user_id: int, expire_time: str) -> str:
        return self.adapter.generate(user_id=user_id, expire_time=expire_time)


def install_user_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    user_adapter: FakeUserAdapter,
    tenant_adapter: FakeTenantAdapter,
    *,
    project_adapter: FakeProjectAdapter | None = None,
    datasource_adapter: FakeDataSourceAdapter | None = None,
    namespace_adapter: FakeNamespaceAdapter | None = None,
) -> None:
    """Bind user service entry points to one in-memory domain."""
    domain = UserDomain(
        ds_version="3.4.1",
        users=user_adapter,
        tenants=tenant_adapter,
        permissions=FakeUserPermissions(
            users=user_adapter,
            projects=project_adapter or FakeProjectAdapter(projects=[]),
            datasources=datasource_adapter or FakeDataSourceAdapter(datasources=[]),
            namespaces=namespace_adapter or FakeNamespaceAdapter(namespaces=[]),
        ),
        recipe=_USER_RECIPES["optional_entity"],
    )
    _install_domain_runner(monkeypatch, user_service, domain)


def install_access_token_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    access_token_adapter: FakeAccessTokenAdapter,
    user_adapter: FakeUserAdapter,
) -> None:
    """Bind access-token service entry points to one in-memory domain."""
    domain = AccessTokenDomain(
        ds_version="3.4.1",
        tokens=FakeTokenOperations(access_token_adapter),
        users=user_adapter,
    )
    _install_domain_runner(monkeypatch, access_token_service, domain)


def _install_domain_runner(
    monkeypatch: pytest.MonkeyPatch,
    service_module: object,
    domain: object,
) -> None:
    def run_with_domain(
        env_file: str | None,
        bound_domain: object,
        operation: Callable[..., ResultT],
        /,
        *args: object,
        **kwargs: object,
    ) -> ResultT:
        del env_file, bound_domain
        return operation(SimpleNamespace(domain=domain), *args, **kwargs)

    monkeypatch.setattr(
        service_module,
        "run_with_bound_domain_service_runtime",
        run_with_domain,
    )


__all__ = [
    "FakeTokenOperations",
    "FakeUserPermissions",
    "install_access_token_service_fakes",
    "install_user_service_fakes",
]
