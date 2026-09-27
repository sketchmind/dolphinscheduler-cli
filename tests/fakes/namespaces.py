"""In-memory namespaces collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeNamespace:
    id: int
    namespace_value: str | None
    code: int | None = None
    cluster_code_value: int | None = None
    cluster_name_value: str | None = None
    k8s_value: str | None = None
    limits_cpu_value: float | None = None
    limits_memory_value: int | None = None
    pod_request_cpu_value: float | None = None
    pod_request_memory_value: int | None = None
    pod_replicas_value: int | None = None
    online_job_num_value: int | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    supported_fields: frozenset[str] = frozenset(
        {
            "id",
            "code",
            "namespace",
            "clusterCode",
            "clusterName",
            "userId",
            "userName",
            "createTime",
            "updateTime",
        }
    )

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
    def k8s(self) -> str | None:
        return self.k8s_value

    @property
    def limitsCpu(self) -> float | None:  # noqa: N802
        return self.limits_cpu_value

    @property
    def limitsMemory(self) -> int | None:  # noqa: N802
        return self.limits_memory_value

    @property
    def podRequestCpu(self) -> float | None:  # noqa: N802
        return self.pod_request_cpu_value

    @property
    def podRequestMemory(self) -> int | None:  # noqa: N802
        return self.pod_request_memory_value

    @property
    def podReplicas(self) -> int | None:  # noqa: N802
        return self.pod_replicas_value

    @property
    def onlineJobNum(self) -> int | None:  # noqa: N802
        return self.online_job_num_value

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
class FakeNamespacePage(_FakePage[FakeNamespace]):
    pass


@dataclass
class FakeNamespaceAdapter:
    namespaces: list[FakeNamespace]
    authorized_by_user_id: dict[int, set[int]] = field(default_factory=dict)
    available_ids: set[int] | None = None
    create_error: ApiResultError | None = None
    delete_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)
    deletes_kubernetes_namespace: bool = False
    list_error: ApiResultError | None = None
    available_error: ApiResultError | None = None

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeNamespacePage:
        if self.list_error is not None:
            raise self.list_error
        filtered = list(self.namespaces)
        if search is not None:
            filtered = [
                namespace
                for namespace in filtered
                if namespace.namespace is not None
                and search.lower() in namespace.namespace.lower()
            ]
        return FakeNamespacePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def create(
        self,
        *,
        namespace: str,
        cluster_code: int | None = None,
        k8s: str | None = None,
        limits_cpu: float | None = None,
        limits_memory: int | None = None,
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
            k8s_value=k8s,
            limits_cpu_value=limits_cpu,
            limits_memory_value=limits_memory,
        )
        self.namespaces.append(created)
        return created

    def available(self) -> Sequence[FakeNamespace]:
        if self.available_error is not None:
            raise self.available_error
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


def empty_namespace_adapter() -> FakeNamespaceAdapter:
    return FakeNamespaceAdapter(namespaces=[])
