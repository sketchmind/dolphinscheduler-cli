from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.response_projection import (
    project_page,
    projection_error,
    require_none,
    sequence_field,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        NamespaceOperations,
        NamespacePageRecord,
        NamespaceRecord,
    )
    from dsctl.upstream.wire import CompiledWireProfile


_NAMESPACE_RESOURCE = "namespace"
_NAMESPACE_INTRODUCED_IN = "3.0.0"
_SelectorWire = Literal["k8s", "cluster-code"]
_CreateResult = Literal["none", "namespace"]
_NamespacePrimitive = Literal["page", "available", "create", "delete"]
_NAMESPACE_PROGRAMS = CompiledDomainPrograms[_NamespacePrimitive](
    name="namespace",
    schema_constant="COMPILED_NAMESPACE_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "available": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "delete": MUTATION_ONCE_REQUIRED,
    },
)


@dataclass(frozen=True)
class NamespaceDomain:
    """Caller-oriented Kubernetes namespace resource surface."""

    namespaces: NamespaceOperations


@dataclass(frozen=True)
class NamespaceSnapshot:
    """Version-neutral namespace projection consumed by stable services."""

    supported_fields: frozenset[str]
    id: int | None
    code: int | None
    namespace: str | None
    clusterCode: int | None  # noqa: N815
    clusterName: str | None  # noqa: N815
    k8s: str | None
    limitsCpu: float | None  # noqa: N815
    limitsMemory: int | None  # noqa: N815
    podRequestCpu: float | None  # noqa: N815
    podRequestMemory: int | None  # noqa: N815
    podReplicas: int | None  # noqa: N815
    onlineJobNum: int | None  # noqa: N815
    userId: int  # noqa: N815
    userName: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _NamespaceRecipe:
    selector_wire: _SelectorWire
    quotas_supported: bool
    create_result: _CreateResult
    deletes_kubernetes_namespace: bool


_K8S_QUOTA_NAMESPACE = _NamespaceRecipe(
    selector_wire="k8s",
    quotas_supported=True,
    create_result="none",
    deletes_kubernetes_namespace=True,
)
_CLUSTER_QUOTA_DESTRUCTIVE = _NamespaceRecipe(
    selector_wire="cluster-code",
    quotas_supported=True,
    create_result="none",
    deletes_kubernetes_namespace=True,
)
_CLUSTER_QUOTA_REGISTRATION_ONLY = _NamespaceRecipe(
    selector_wire="cluster-code",
    quotas_supported=True,
    create_result="none",
    deletes_kubernetes_namespace=False,
)
_CLUSTER_REGISTRATION_ONLY = _NamespaceRecipe(
    selector_wire="cluster-code",
    quotas_supported=False,
    create_result="none",
    deletes_kubernetes_namespace=False,
)
_CLUSTER_ENTITY_REGISTRATION_ONLY = _NamespaceRecipe(
    selector_wire="cluster-code",
    quotas_supported=False,
    create_result="namespace",
    deletes_kubernetes_namespace=False,
)

_NAMESPACE_RECIPES = {
    "k8s_quota_namespace": _K8S_QUOTA_NAMESPACE,
    "cluster_quota_destructive": _CLUSTER_QUOTA_DESTRUCTIVE,
    "cluster_quota_registration_only": _CLUSTER_QUOTA_REGISTRATION_ONLY,
    "cluster_registration_only": _CLUSTER_REGISTRATION_ONLY,
    "cluster_entity_registration_only": _CLUSTER_ENTITY_REGISTRATION_ONLY,
}


class NamespaceAdapter:
    """Compiled namespace adapter for every reviewed supporting profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed namespace recipe."""
        self._profile = _NAMESPACE_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> NamespaceAdapter:
        """Return one generated adapter for a reviewed supporting version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> NamespaceDomain:
        """Bind the namespace operations for this exact profile."""
        programs = _NAMESPACE_PROGRAMS.bind(
            self._profile, profile, http_client=http_client
        )
        return NamespaceDomain(
            namespaces=cast(
                "NamespaceOperations",
                _CompiledNamespaceOperations(programs, self._recipe),
            )
        )


@dataclass(frozen=True)
class _AbsentNamespaceAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> NamespaceDomain:
        del profile, http_client
        message = (
            "Kubernetes namespace management does not exist in "
            f"DolphinScheduler {self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _NAMESPACE_RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _NAMESPACE_INTRODUCED_IN,
            },
            suggestion=(
                "Use DolphinScheduler 3.0.0 or newer for Kubernetes namespace "
                "management."
            ),
        )


def _adapter_for_version(ds_version: str) -> BoundDomainAdapter[NamespaceDomain]:
    profile = _NAMESPACE_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentNamespaceAdapter(ds_version)
    if profile.status == "supported":
        return NamespaceAdapter.for_version(ds_version)
    message = f"DS {ds_version} has no reviewed namespace capability decision"
    raise WireContractError(message)


NAMESPACE_DOMAIN = BoundDomain[NamespaceDomain](
    name=_NAMESPACE_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _CompiledNamespaceOperations:
    programs: BoundCompiledPrograms[_NamespacePrimitive]
    recipe: _NamespaceRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> NamespacePageRecord:
        page = self.programs.call(
            "page", {"searchVal": search, "pageSize": page_size, "pageNo": page_no}
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_NAMESPACE_RESOURCE,
        )
        snapshots = [
            _namespace_snapshot(item, ds_version=self.ds_version) for item in items
        ]
        return cast(
            "NamespacePageRecord",
            project_page(
                page,
                snapshots,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
            ),
        )

    def available(self) -> Sequence[NamespaceRecord]:
        payload = self.programs.call("available", {})
        if not isinstance(payload, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                field="availableList",
                reason="available namespaces are not a list",
            )
        return cast(
            "Sequence[NamespaceRecord]",
            [_namespace_snapshot(item, ds_version=self.ds_version) for item in payload],
        )

    def create(
        self,
        *,
        namespace: str,
        cluster_code: int | None = None,
        k8s: str | None = None,
        limits_cpu: float | None = None,
        limits_memory: int | None = None,
    ) -> NamespaceRecord:
        form_fields = self._create_form_fields(
            namespace=namespace,
            cluster_code=cluster_code,
            k8s=k8s,
            limits_cpu=limits_cpu,
            limits_memory=limits_memory,
        )
        result = mutation_call(
            lambda: self.programs.call("create", form_fields),
            ds_version=self.ds_version,
            resource=_NAMESPACE_RESOURCE,
            operation="create",
        )
        result_id = verify_mutation(
            lambda: _validate_create_result(
                result,
                recipe=self.recipe,
                ds_version=self.ds_version,
            ),
            ds_version=self.ds_version,
            resource=_NAMESPACE_RESOURCE,
            operation="create",
            phase="mutation_response",
        )
        return cast(
            "NamespaceRecord",
            verify_mutation(
                lambda: self._find_created(
                    namespace=namespace,
                    cluster_code=cluster_code,
                    k8s=k8s,
                    namespace_id=result_id,
                    limits_cpu=limits_cpu,
                    limits_memory=limits_memory,
                ),
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                operation="create",
            ),
        )

    def delete(self, *, namespace_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call("delete", {"id": namespace_id}),
            ds_version=self.ds_version,
            resource=_NAMESPACE_RESOURCE,
            operation="delete",
        )
        verify_mutation(
            lambda: require_none(
                result,
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                field="deleteResult",
            ),
            ds_version=self.ds_version,
            resource=_NAMESPACE_RESOURCE,
            operation="delete",
            phase="mutation_response",
        )
        return True

    @property
    def deletes_kubernetes_namespace(self) -> bool:
        """Whether upstream deletion also deletes the real Kubernetes object."""
        return self.recipe.deletes_kubernetes_namespace

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def _create_form_fields(
        self,
        *,
        namespace: str,
        cluster_code: int | None,
        k8s: str | None,
        limits_cpu: float | None,
        limits_memory: int | None,
    ) -> JsonObject:
        recipe = self.recipe
        fields: JsonObject = {"namespace": namespace}
        if recipe.selector_wire == "k8s":
            if not k8s or not k8s.strip() or cluster_code is not None:
                raise _namespace_selector_error(
                    self.ds_version,
                    required="--k8s",
                    rejected="--cluster-code",
                )
            fields["k8s"] = k8s.strip()
        else:
            if cluster_code is None or cluster_code <= 0 or k8s is not None:
                raise _namespace_selector_error(
                    self.ds_version,
                    required="--cluster-code",
                    rejected="--k8s",
                )
            fields["clusterCode"] = cluster_code
        if recipe.quotas_supported:
            fields["limitsCpu"] = limits_cpu
            fields["limitsMemory"] = limits_memory
        elif limits_cpu is not None or limits_memory is not None:
            message = (
                "Namespace quota inputs are not accepted by DolphinScheduler "
                f"{self.ds_version}"
            )
            raise UserInputError(
                message,
                details={
                    "ds_version": self.ds_version,
                    "unsupported_options": [
                        option
                        for option, value in (
                            ("--limits-cpu", limits_cpu),
                            ("--limits-memory", limits_memory),
                        )
                        if value is not None
                    ],
                    "supported_through": "3.2.0",
                },
                suggestion=(
                    "Remove the quota options; manage Kubernetes ResourceQuota "
                    "outside dsctl for this DS version."
                ),
            )
        return fields

    def _find_created(
        self,
        *,
        namespace: str,
        cluster_code: int | None,
        k8s: str | None,
        namespace_id: int | None,
        limits_cpu: float | None,
        limits_memory: int | None,
    ) -> NamespaceSnapshot:
        matches = [
            cast("NamespaceSnapshot", item)
            for item in collect_pages(
                lambda *, page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=namespace,
                ),
                resource=_NAMESPACE_RESOURCE,
            )
            if item.namespace == namespace
            and (namespace_id is None or item.id == namespace_id)
            and (cluster_code is None or item.clusterCode == cluster_code)
            and (k8s is None or getattr(item, "k8s", None) == k8s)
        ]
        if len(matches) != 1:
            raise projection_error(
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                field="mutationReadback",
                reason="created namespace could not be resolved uniquely",
            )
        created = matches[0]
        if limits_cpu is not None and created.limitsCpu != limits_cpu:
            _raise_namespace_readback_mismatch(self.ds_version, "limitsCpu")
        if limits_memory is not None and created.limitsMemory != limits_memory:
            _raise_namespace_readback_mismatch(self.ds_version, "limitsMemory")
        return created


def _recipe_for_profile(profile: CompiledWireProfile) -> _NamespaceRecipe:
    try:
        return _NAMESPACE_RECIPES[profile.recipe_id or ""]
    except KeyError as exc:
        message = f"Compiled namespace recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _namespace_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> NamespaceSnapshot:
    if not isinstance(item, BaseModel):
        raise projection_error(
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            field="namespace",
            reason="namespace payload is not an exact generated model",
        )
    return NamespaceSnapshot(
        supported_fields=frozenset(type(item).model_fields),
        id=_optional_int(item, "id", ds_version=ds_version),
        code=_optional_int(item, "code", ds_version=ds_version),
        namespace=_optional_text(item, "namespace", ds_version=ds_version),
        clusterCode=_optional_int(item, "clusterCode", ds_version=ds_version),
        clusterName=_optional_text(item, "clusterName", ds_version=ds_version),
        k8s=_optional_text(item, "k8s", ds_version=ds_version),
        limitsCpu=_optional_float(item, "limitsCpu", ds_version=ds_version),
        limitsMemory=_optional_int(item, "limitsMemory", ds_version=ds_version),
        podRequestCpu=_optional_float(
            item,
            "podRequestCpu",
            ds_version=ds_version,
        ),
        podRequestMemory=_optional_int(
            item,
            "podRequestMemory",
            ds_version=ds_version,
        ),
        podReplicas=_optional_int(item, "podReplicas", ds_version=ds_version),
        onlineJobNum=_optional_int(item, "onlineJobNum", ds_version=ds_version),
        userId=_optional_int(item, "userId", ds_version=ds_version) or 0,
        userName=_optional_text(item, "userName", ds_version=ds_version),
        createTime=_optional_text(item, "createTime", ds_version=ds_version),
        updateTime=_optional_text(item, "updateTime", ds_version=ds_version),
    )


def _validate_create_result(
    result: OpaqueGeneratedValue,
    *,
    recipe: _NamespaceRecipe,
    ds_version: str,
) -> int | None:
    if recipe.create_result == "none":
        require_none(
            result,
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            field="createResult",
        )
        return None
    namespace_id = _optional_int(result, "id", ds_version=ds_version)
    if namespace_id is None:
        raise projection_error(
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            field="createResult",
            reason="namespace mutation result is missing an id",
        )
    return namespace_id


def _optional_field(item: OpaqueGeneratedValue, field: str) -> OpaqueGeneratedValue:
    return getattr(item, field, None)


def _optional_int(
    item: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> int | None:
    value = _optional_field(item, field)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=_NAMESPACE_RESOURCE,
        field=field,
        reason="payload field is not an integer or null",
    )


def _optional_float(
    item: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> float | None:
    value = _optional_field(item, field)
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    raise projection_error(
        ds_version=ds_version,
        resource=_NAMESPACE_RESOURCE,
        field=field,
        reason="payload field is not numeric or null",
    )


def _optional_text(
    item: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> str | None:
    value = _optional_field(item, field)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=_NAMESPACE_RESOURCE,
        field=field,
        reason="payload field is not text or null",
    )


def _namespace_selector_error(
    ds_version: str,
    *,
    required: str,
    rejected: str,
) -> UserInputError:
    message = f"DolphinScheduler {ds_version} requires {required} for namespace create"
    return UserInputError(
        message,
        details={
            "ds_version": ds_version,
            "required_option": required,
            "unsupported_option": rejected,
        },
        suggestion=f"Use {required} and omit {rejected}, then retry.",
    )


def _raise_namespace_readback_mismatch(ds_version: str, field: str) -> None:
    raise projection_error(
        ds_version=ds_version,
        resource=_NAMESPACE_RESOURCE,
        field=field,
        reason="readback field does not match the requested mutation",
    )


__all__ = [
    "NAMESPACE_DOMAIN",
    "NamespaceAdapter",
    "NamespaceDomain",
    "NamespaceSnapshot",
]
