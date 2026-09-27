"""In-memory workers collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from tests.fakes.queues import (
        FakeQueue,
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
class FakeEnvironmentPage(_FakePage[FakeEnvironment]):
    pass


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
        return FakeEnvironmentPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
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
class FakeWorkerGroupPage(_FakePage[FakeWorkerGroup]):
    pass


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
class FakeTenantPage(_FakePage[FakeTenant]):
    pass


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
        return FakeTenantPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
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
        current_tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> FakeTenant:
        queue = self._queue(queue_id)
        for existing in self.tenants:
            if existing.id == tenant_id:
                continue
            if existing.tenantCode == current_tenant_code:
                raise ApiResultError(
                    result_code=10009,
                    result_message=(
                        f"os tenant code {current_tenant_code} already exists"
                    ),
                )
        for index, existing in enumerate(self.tenants):
            if existing.id == tenant_id:
                updated = replace(
                    existing,
                    tenant_code_value=current_tenant_code,
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
class FakeClusterPage(_FakePage[FakeCluster]):
    pass


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
        return FakeClusterPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
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


def empty_cluster_adapter() -> FakeClusterAdapter:
    return FakeClusterAdapter(clusters=[])


def empty_environment_adapter() -> FakeEnvironmentAdapter:
    return FakeEnvironmentAdapter(environments=[])


def empty_worker_group_adapter() -> FakeWorkerGroupAdapter:
    return FakeWorkerGroupAdapter(worker_groups=[])


def empty_tenant_adapter() -> FakeTenantAdapter:
    return FakeTenantAdapter(tenants=[])
