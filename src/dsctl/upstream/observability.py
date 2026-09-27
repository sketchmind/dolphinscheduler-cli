from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.compiled_domain import (
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
    CompiledProgramExpectation,
)
from dsctl.upstream.response_projection import (
    optional_text_field,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        AuditModelTypeRecord,
        AuditOperations,
        AuditOperationTypeRecord,
        AuditPageRecord,
        MonitorDatabaseRecord,
        MonitorOperations,
        MonitorServerRecord,
        StringEnumValue,
    )


_AUDIT_RESOURCE = "audit"
_MONITOR_DATABASE_RESOURCE = "monitor-database"
_MONITOR_SERVER_RESOURCE = "monitor-server"

_AuditSupport = Literal["absent", "supported"]
_AuditFilterWire = Literal["singular-enums", "csv-text"]
_AuditRecordWire = Literal["legacy-resource", "canonical-model"]
_MonitorServerWire = Literal["fixed-routes", "node-type-route"]
_MonitorServerFieldWire = Literal["legacy", "canonical"]
_AuditPrimitive = Literal["page", "model_types", "operation_types"]
_AUDIT_METADATA_ABSENT = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
    }
)
_AUDIT_METADATA_READ = CompiledProgramExpectation(
    mode=READ_RETRY_OPTIONAL.mode,
    envelope=READ_RETRY_OPTIONAL.envelope,
    absent_versions=_AUDIT_METADATA_ABSENT,
)
_AUDIT_PROGRAMS = CompiledDomainPrograms[_AuditPrimitive](
    name="audit",
    schema_constant="COMPILED_AUDIT_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "model_types": _AUDIT_METADATA_READ,
        "operation_types": _AUDIT_METADATA_READ,
    },
)

_MonitorPrimitive = Literal["database", "master", "worker", "server"]
_MONITOR_FIXED_READ = CompiledProgramExpectation(
    mode=READ_RETRY_OPTIONAL.mode,
    envelope=READ_RETRY_OPTIONAL.envelope,
    absent_versions=frozenset(
        {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
    ),
)
_MONITOR_SERVER_READ = CompiledProgramExpectation(
    mode=READ_RETRY_OPTIONAL.mode,
    envelope=READ_RETRY_OPTIONAL.envelope,
    absent_versions=frozenset(
        {
            "1.3.9",
            "2.0.0",
            "2.0.1",
            "2.0.2",
            "2.0.3",
            "2.0.4",
            "2.0.5",
            "2.0.6",
            "2.0.7",
            "2.0.8",
            "2.0.9",
            "3.0.0",
            "3.0.1",
            "3.0.2",
            "3.0.3",
            "3.0.4",
            "3.0.5",
            "3.0.6",
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.8",
            "3.1.9",
            "3.2.0",
            "3.2.1",
        }
    ),
)
_MONITOR_PROGRAMS = CompiledDomainPrograms[_MonitorPrimitive](
    name="monitor",
    schema_constant="COMPILED_MONITOR_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "database": READ_RETRY_OPTIONAL,
        "master": _MONITOR_FIXED_READ,
        "worker": _MONITOR_FIXED_READ,
        "server": _MONITOR_SERVER_READ,
    },
)


@dataclass(frozen=True)
class AuditDomain:
    """Exact-version audit operations consumed by stable services."""

    audits: AuditOperations


@dataclass(frozen=True)
class MonitorDomain:
    """Exact-version monitor operations consumed by stable services."""

    monitor: MonitorOperations


@dataclass(frozen=True)
class AuditSnapshot:
    """Version-neutral projection of old and current AuditDto payloads."""

    userName: str | None  # noqa: N815
    modelType: str | None  # noqa: N815
    modelName: str | None  # noqa: N815
    operation: str | None
    createTime: str | None  # noqa: N815
    description: str | None
    detail: str | None
    latency: str | None


@dataclass(frozen=True)
class AuditModelTypeSnapshot:
    """Stable recursive audit model-type node."""

    name: str | None
    child: list[AuditModelTypeSnapshot] | None


@dataclass(frozen=True)
class AuditOperationTypeSnapshot:
    """Stable audit operation-type node."""

    name: str | None


@dataclass(frozen=True)
class MonitorServerSnapshot:
    """Stable server projection retaining every legacy worker directory."""

    id: int
    host: str | None
    port: int
    serverDirectories: list[str]  # noqa: N815
    serverDirectory: str | None  # noqa: N815
    heartBeatInfo: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    lastHeartbeatTime: str | None  # noqa: N815


@dataclass(frozen=True)
class MonitorDatabaseSnapshot:
    """Stable projection shared by MonitorRecord and DatabaseMetrics."""

    dbType: StringEnumValue | str | None  # noqa: N815
    state: StringEnumValue | str | None
    maxConnections: int  # noqa: N815
    maxUsedConnections: int  # noqa: N815
    threadsConnections: int  # noqa: N815
    threadsRunningConnections: int  # noqa: N815
    date: str | None


@dataclass(frozen=True)
class _AuditRecipe:
    support: _AuditSupport
    filter_wire: _AuditFilterWire | None
    record_wire: _AuditRecordWire | None
    metadata: bool
    model_type_values: tuple[str, ...] = ()
    operation_type_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class _MonitorRecipe:
    server_wire: _MonitorServerWire
    server_field_wire: _MonitorServerFieldWire
    node_types: tuple[str, ...]


_AUDIT_ABSENT = _AuditRecipe("absent", None, None, False)
_AUDIT_SINGULAR = _AuditRecipe(
    "supported",
    "singular-enums",
    "legacy-resource",
    False,
    ("USER_MODULE", "PROJECT_MODULE"),
    ("CREATE", "READ", "UPDATE", "DELETE"),
)
_AUDIT_CSV = _AuditRecipe("supported", "csv-text", "canonical-model", True)

_FIXED_LEGACY_MONITOR = _MonitorRecipe(
    "fixed-routes",
    "legacy",
    ("MASTER", "WORKER"),
)
_NODE_LEGACY_MONITOR = _MonitorRecipe(
    "node-type-route",
    "legacy",
    ("MASTER", "WORKER", "ALERT_SERVER"),
)
_NODE_CANONICAL_MONITOR = _MonitorRecipe(
    "node-type-route",
    "canonical",
    ("MASTER", "WORKER", "ALERT_SERVER"),
)


class AuditAdapter:
    """Bind audit operations from one exact compiled profile."""

    def __init__(self, ds_version: str) -> None:
        """Select the reviewed audit profile and its explicit recipe."""
        self._profile = _AUDIT_PROGRAMS.profile(ds_version)
        self._recipe = _audit_recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> AuditAdapter:
        """Return the adapter for one explicitly reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> AuditDomain:
        """Bind exact audit programs without changing unavailable-operation errors."""
        if self._recipe.support == "absent":
            return AuditDomain(
                audits=cast(
                    "AuditOperations",
                    _UnavailableAuditOperations(self.ds_version),
                )
            )
        programs = _AUDIT_PROGRAMS.bind(self._profile, profile, http_client=http_client)
        return AuditDomain(
            audits=cast(
                "AuditOperations", _CompiledAuditOperations(programs, self._recipe)
            )
        )


class MonitorAdapter:
    """Bind monitor server/database reads from one exact compiled profile."""

    def __init__(self, ds_version: str) -> None:
        """Select the exact programs and reviewed server projection recipe."""
        self._profile = _MONITOR_PROGRAMS.profile(ds_version)
        self._recipe = _monitor_recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> MonitorAdapter:
        """Return the adapter for one explicitly reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> MonitorDomain:
        """Bind reviewed monitor programs without loading an exact package."""
        programs = _MONITOR_PROGRAMS.bind(
            self._profile, profile, http_client=http_client
        )
        return MonitorDomain(
            monitor=cast(
                "MonitorOperations",
                _CompiledMonitorOperations(programs, self._recipe),
            )
        )


AUDIT_DOMAIN = BoundDomain[AuditDomain](
    name="audit",
    adapter_for_version=AuditAdapter.for_version,
)
MONITOR_DOMAIN = BoundDomain[MonitorDomain](
    name="monitor",
    adapter_for_version=MonitorAdapter.for_version,
)


@dataclass(frozen=True)
class _UnavailableAuditOperations:
    ds_version: str

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
    ) -> AuditPageRecord:
        del (
            page_no,
            page_size,
            model_types,
            operation_types,
            start_date,
            end_date,
            user_name,
            model_name,
        )
        raise _unsupported_audit(self.ds_version, "audit.list", introduced_in="3.0.0")

    def list_model_types(self) -> Sequence[AuditModelTypeRecord]:
        raise _unsupported_audit(
            self.ds_version,
            "audit.model-types",
            introduced_in="3.2.2",
        )

    def list_operation_types(self) -> Sequence[AuditOperationTypeRecord]:
        raise _unsupported_audit(
            self.ds_version,
            "audit.operation-types",
            introduced_in="3.2.2",
        )


@dataclass(frozen=True)
class _CompiledAuditOperations:
    programs: BoundCompiledPrograms[_AuditPrimitive]
    recipe: _AuditRecipe

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
    ) -> AuditPageRecord:
        recipe = self.recipe
        if recipe.filter_wire == "singular-enums":
            model_type = _one_legacy_filter(
                model_types,
                label="model type",
                allowed_values=recipe.model_type_values,
                ds_version=self.programs.ds_version,
            )
            operation_type = _one_legacy_filter(
                operation_types,
                label="operation type",
                allowed_values=recipe.operation_type_values,
                ds_version=self.programs.ds_version,
            )
            if model_name is not None:
                message = (
                    "--model-name is unavailable for the selected "
                    "DolphinScheduler version"
                )
                raise UserInputError(
                    message,
                    details={"selected_version": self.programs.ds_version},
                    suggestion=(
                        "Omit --model-name or use DolphinScheduler 3.2.2 or newer."
                    ),
                )
            params: JsonObject = {
                "pageNo": page_no,
                "pageSize": page_size,
                "resourceType": model_type,
                "operationType": operation_type,
                "startDate": start_date,
                "endDate": end_date,
                "userName": user_name,
            }
        else:
            params = {
                "pageNo": page_no,
                "pageSize": page_size,
                "modelTypes": _comma_join(model_types),
                "operationTypes": _comma_join(operation_types),
                "startDate": start_date,
                "endDate": end_date,
                "userName": user_name,
                "modelName": model_name,
            }
        page = self.programs.call("page", params)
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.programs.ds_version,
            resource=_AUDIT_RESOURCE,
        )
        return cast(
            "AuditPageRecord",
            project_page(
                page,
                [
                    _audit_snapshot(
                        item,
                        ds_version=self.programs.ds_version,
                        wire=cast("_AuditRecordWire", recipe.record_wire),
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.programs.ds_version,
                resource=_AUDIT_RESOURCE,
            ),
        )

    def list_model_types(self) -> Sequence[AuditModelTypeRecord]:
        self._require_metadata("audit.model-types")
        return [
            cast(
                "AuditModelTypeRecord",
                _audit_model_type_snapshot(
                    item,
                    ds_version=self.programs.ds_version,
                ),
            )
            for item in _require_list_result(
                self.programs.call("model_types", {}),
                ds_version=self.programs.ds_version,
                resource="audit-model-type",
            )
        ]

    def list_operation_types(self) -> Sequence[AuditOperationTypeRecord]:
        self._require_metadata("audit.operation-types")
        return [
            cast(
                "AuditOperationTypeRecord",
                AuditOperationTypeSnapshot(
                    name=optional_text_field(
                        item,
                        "name",
                        ds_version=self.programs.ds_version,
                        resource="audit-operation-type",
                    )
                ),
            )
            for item in _require_list_result(
                self.programs.call("operation_types", {}),
                ds_version=self.programs.ds_version,
                resource="audit-operation-type",
            )
        ]

    def _require_metadata(self, action: str) -> None:
        if self.recipe.metadata:
            return
        raise _unsupported_audit(
            self.programs.ds_version,
            action,
            introduced_in="3.2.2",
        )


@dataclass(frozen=True)
class _CompiledMonitorOperations:
    programs: BoundCompiledPrograms[_MonitorPrimitive]
    recipe: _MonitorRecipe

    def list_servers(self, *, node_type: str) -> Sequence[MonitorServerRecord]:
        recipe = self.recipe
        if node_type not in recipe.node_types:
            choices = ", ".join(
                item.lower().replace("_", "-") for item in recipe.node_types
            )
            message = (
                f"Monitor node type {node_type!r} is unavailable for the "
                "selected version"
            )
            raise UserInputError(
                message,
                details={
                    "node_type": node_type,
                    "selected_version": self.programs.ds_version,
                },
                suggestion=f"Retry with one of: {choices}.",
            )
        if recipe.server_wire == "fixed-routes":
            primitive: _MonitorPrimitive = (
                "master" if node_type == "MASTER" else "worker"
            )
            worker_collection = node_type == "WORKER"
            payload = self.programs.call(primitive, {})
        else:
            worker_collection = False
            payload = self.programs.call("server", {"nodeType": node_type})
        return [
            cast(
                "MonitorServerRecord",
                _monitor_server_snapshot(
                    item,
                    ds_version=self.programs.ds_version,
                    field_wire=recipe.server_field_wire,
                    worker_collection=worker_collection,
                ),
            )
            for item in _require_list_result(
                payload,
                ds_version=self.programs.ds_version,
                resource=_MONITOR_SERVER_RESOURCE,
            )
        ]

    def list_databases(self) -> Sequence[MonitorDatabaseRecord]:
        return [
            cast(
                "MonitorDatabaseRecord",
                _monitor_database_snapshot(
                    item,
                    ds_version=self.programs.ds_version,
                ),
            )
            for item in _require_list_result(
                self.programs.call("database", {}),
                ds_version=self.programs.ds_version,
                resource=_MONITOR_DATABASE_RESOURCE,
            )
        ]


def _audit_recipe_for_profile(profile: CompiledWireProfile) -> _AuditRecipe:
    if profile.status == "upstream_absent":
        return _AUDIT_ABSENT
    recipe = {"singular_enums": _AUDIT_SINGULAR, "csv_text": _AUDIT_CSV}.get(
        profile.recipe_id or ""
    )
    if recipe is None:
        message = f"Compiled audit recipe {profile.recipe_id!r} is unsupported"
        raise WireContractError(message)
    return recipe


def supported_monitor_server_types(ds_version: str) -> tuple[str, ...]:
    """Return CLI node choices from the exact runtime monitor recipe."""
    recipe = _monitor_recipe_for_profile(_MONITOR_PROGRAMS.profile(ds_version))
    return tuple(node_type.lower().replace("_", "-") for node_type in recipe.node_types)


def _monitor_recipe_for_profile(profile: CompiledWireProfile) -> _MonitorRecipe:
    recipe = {
        "fixed_legacy": _FIXED_LEGACY_MONITOR,
        "node_type_legacy": _NODE_LEGACY_MONITOR,
        "node_type_canonical": _NODE_CANONICAL_MONITOR,
    }.get(profile.recipe_id or "")
    if recipe is None:
        message = f"Compiled monitor recipe {profile.recipe_id!r} is unsupported"
        raise WireContractError(message)
    return recipe


def _audit_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    wire: _AuditRecordWire,
) -> AuditSnapshot:
    legacy = wire == "legacy-resource"
    return AuditSnapshot(
        userName=optional_text_field(
            item, "userName", ds_version=ds_version, resource=_AUDIT_RESOURCE
        ),
        modelType=optional_text_field(
            item,
            "resource" if legacy else "modelType",
            ds_version=ds_version,
            resource=_AUDIT_RESOURCE,
        ),
        modelName=optional_text_field(
            item,
            "resourceName" if legacy else "modelName",
            ds_version=ds_version,
            resource=_AUDIT_RESOURCE,
        ),
        operation=optional_text_field(
            item, "operation", ds_version=ds_version, resource=_AUDIT_RESOURCE
        ),
        createTime=optional_text_field(
            item,
            "time" if legacy else "createTime",
            ds_version=ds_version,
            resource=_AUDIT_RESOURCE,
        ),
        description=(
            None
            if legacy
            else optional_text_field(
                item, "description", ds_version=ds_version, resource=_AUDIT_RESOURCE
            )
        ),
        detail=(
            None
            if legacy
            else optional_text_field(
                item, "detail", ds_version=ds_version, resource=_AUDIT_RESOURCE
            )
        ),
        latency=(
            None
            if legacy
            else optional_text_field(
                item, "latency", ds_version=ds_version, resource=_AUDIT_RESOURCE
            )
        ),
    )


def _audit_model_type_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
) -> AuditModelTypeSnapshot:
    child = response_field(
        item,
        "child",
        ds_version=ds_version,
        resource="audit-model-type",
    )
    if child is not None and not isinstance(child, list):
        raise projection_error(
            ds_version=ds_version,
            resource="audit-model-type",
            field="child",
            reason="payload field is not a list or null",
        )
    return AuditModelTypeSnapshot(
        name=optional_text_field(
            item,
            "name",
            ds_version=ds_version,
            resource="audit-model-type",
        ),
        child=(
            None
            if child is None
            else [
                _audit_model_type_snapshot(value, ds_version=ds_version)
                for value in child
            ]
        ),
    )


def _monitor_server_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    field_wire: _MonitorServerFieldWire,
    worker_collection: bool,
) -> MonitorServerSnapshot:
    if field_wire == "canonical":
        server_directory = optional_text_field(
            item,
            "serverDirectory",
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
        )
        server_directories = [] if server_directory is None else [server_directory]
        heartbeat_field = "heartBeatInfo"
    elif worker_collection:
        server_directories = _directory_collection(
            item,
            ds_version=ds_version,
        )
        server_directory = (
            server_directories[0] if len(server_directories) == 1 else None
        )
        heartbeat_field = "resInfo"
    else:
        server_directory = optional_text_field(
            item,
            "zkDirectory",
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
        )
        server_directories = [] if server_directory is None else [server_directory]
        heartbeat_field = "resInfo"
    return MonitorServerSnapshot(
        id=_non_negative_int_field(
            item, "id", ds_version=ds_version, resource=_MONITOR_SERVER_RESOURCE
        ),
        host=optional_text_field(
            item, "host", ds_version=ds_version, resource=_MONITOR_SERVER_RESOURCE
        ),
        port=_non_negative_int_field(
            item, "port", ds_version=ds_version, resource=_MONITOR_SERVER_RESOURCE
        ),
        serverDirectories=server_directories,
        serverDirectory=server_directory,
        heartBeatInfo=optional_text_field(
            item,
            heartbeat_field,
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
        ),
        lastHeartbeatTime=optional_text_field(
            item,
            "lastHeartbeatTime",
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
        ),
    )


def _monitor_database_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
) -> MonitorDatabaseSnapshot:
    return MonitorDatabaseSnapshot(
        dbType=cast(
            "StringEnumValue | str | None",
            response_field(
                item,
                "dbType",
                ds_version=ds_version,
                resource=_MONITOR_DATABASE_RESOURCE,
            ),
        ),
        state=cast(
            "StringEnumValue | str | None",
            response_field(
                item,
                "state",
                ds_version=ds_version,
                resource=_MONITOR_DATABASE_RESOURCE,
            ),
        ),
        maxConnections=_non_negative_int_field(
            item,
            "maxConnections",
            ds_version=ds_version,
            resource=_MONITOR_DATABASE_RESOURCE,
        ),
        maxUsedConnections=_non_negative_int_field(
            item,
            "maxUsedConnections",
            ds_version=ds_version,
            resource=_MONITOR_DATABASE_RESOURCE,
        ),
        threadsConnections=_non_negative_int_field(
            item,
            "threadsConnections",
            ds_version=ds_version,
            resource=_MONITOR_DATABASE_RESOURCE,
        ),
        threadsRunningConnections=_non_negative_int_field(
            item,
            "threadsRunningConnections",
            ds_version=ds_version,
            resource=_MONITOR_DATABASE_RESOURCE,
        ),
        date=optional_text_field(
            item,
            "date",
            ds_version=ds_version,
            resource=_MONITOR_DATABASE_RESOURCE,
        ),
    )


def _directory_collection(item: OpaqueGeneratedValue, *, ds_version: str) -> list[str]:
    value = response_field(
        item,
        "zkDirectories",
        ds_version=ds_version,
        resource=_MONITOR_SERVER_RESOURCE,
    )
    if value is None:
        return []
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise projection_error(
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
            field="zkDirectories",
            reason="payload field is not a directory collection or null",
        )
    if any(not isinstance(directory, str) for directory in value):
        raise projection_error(
            ds_version=ds_version,
            resource=_MONITOR_SERVER_RESOURCE,
            field="zkDirectories",
            reason="directory collection contains a non-text value",
        )
    return sorted(set(value))


def _non_negative_int_field(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> int:
    value = response_field(item, name, ds_version=ds_version, resource=resource)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=name,
        reason="payload field is not a non-negative integer",
    )


def _require_list_result(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
) -> list[OpaqueGeneratedValue]:
    if isinstance(value, list):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field="result",
        reason="operation result is not a list",
    )


def _one_legacy_filter(
    selected_values: Sequence[str] | None,
    *,
    label: str,
    allowed_values: tuple[str, ...],
    ds_version: str,
) -> str | None:
    if not selected_values:
        return None
    if len(selected_values) > 1:
        option = label.replace(" ", "-")
        message = f"DolphinScheduler {ds_version} accepts only one {label} filter"
        raise UserInputError(
            message,
            details={
                "selected_version": ds_version,
                "values": list(selected_values),
            },
            suggestion=f"Pass at most one --{option} value.",
        )
    value = selected_values[0]
    if value in allowed_values:
        return value
    option = label.replace(" ", "-")
    message = f"Unsupported {label} for DolphinScheduler {ds_version}: {value}"
    raise UserInputError(
        message,
        details={"selected_version": ds_version, "value": value},
        suggestion=f"Retry --{option} with one of: {', '.join(allowed_values)}.",
    )


def _comma_join(values: Sequence[str] | None) -> str | None:
    if not values:
        return None
    return ",".join(values)


def _unsupported_audit(
    ds_version: str,
    action: str,
    *,
    introduced_in: str,
) -> UnsupportedFeatureError:
    return UnsupportedFeatureError(
        f"{action} is unavailable on DolphinScheduler {ds_version}",
        details={
            "action": action,
            "selected_version": ds_version,
            "reason": "upstream_capability_absent",
            "introduced_in": introduced_in,
        },
        suggestion=f"Use DolphinScheduler {introduced_in} or newer for {action}.",
    )


__all__ = [
    "AUDIT_DOMAIN",
    "MONITOR_DOMAIN",
    "AuditAdapter",
    "AuditDomain",
    "AuditModelTypeSnapshot",
    "AuditOperationTypeSnapshot",
    "AuditSnapshot",
    "MonitorAdapter",
    "MonitorDatabaseSnapshot",
    "MonitorDomain",
    "MonitorServerSnapshot",
    "supported_monitor_server_types",
]
