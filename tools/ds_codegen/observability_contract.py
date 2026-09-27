"""Reviewed exact-version recipes for audit and monitor commands.

The compatibility compiler consumes source-operation closures, while the
runtime adapter consumes the smaller wire recipes below.  Keeping both views
in one exact-version table prevents the stable CLI from treating the 3.4.1
audit filters or monitor routes as if they had always existed.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

TARGET_OBSERVABILITY_VERSIONS = (
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
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

MONITOR_GENERATED_OPERATIONS = (
    "monitor.database",
    "monitor.server",
)
AUDIT_SEMANTIC_OPERATIONS = (
    "audit.list",
    "audit.model-types",
    "audit.operation-types",
)

Support = Literal["supported", "absent"]
MonitorServerWire = Literal["fixed-routes", "node-type-route"]
MonitorServerFieldWire = Literal["legacy", "canonical"]
AuditFilterWire = Literal["absent", "singular-enums", "csv-text"]
AuditRecordWire = Literal["absent", "legacy-resource", "canonical-model"]
EvidenceKind = Literal["controller", "service", "build"]
TerminalReason = Literal["upstream_capability_absent"]


@dataclass(frozen=True)
class Evidence:
    """One exact upstream source coordinate supporting a reviewed decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the conventional source reference used by profile evidence."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class MonitorRecipe:
    """Wire and payload decisions for one exact monitor controller."""

    health_support: Support
    database_operation: str
    database_model: str
    server_wire: MonitorServerWire
    server_field_wire: MonitorServerFieldWire
    server_operations: tuple[str, ...]
    server_models: tuple[str, ...]
    server_enums: tuple[str, ...]
    node_types: tuple[str, ...]


@dataclass(frozen=True)
class AuditRecipe:
    """Wire and filter decisions for one exact audit controller."""

    support: Support
    filter_wire: AuditFilterWire
    record_wire: AuditRecordWire
    list_operation: str | None
    metadata_support: Support
    model_type_operation: str | None
    operation_type_operation: str | None
    model_name_filter: bool
    model_type_values: tuple[str, ...]
    operation_type_values: tuple[str, ...]
    visibility: Literal["server-defined", "admin-all-or-self"] = "server-defined"


@dataclass(frozen=True)
class ObservabilityVersionContract:
    """Reviewed audit and monitor contract for one exact DS version."""

    version: str
    monitor: MonitorRecipe
    audit: AuditRecipe


@dataclass(frozen=True)
class TerminalAbsence:
    """One stable action proven unavailable in one exact upstream release."""

    semantic_operation: str
    reason: TerminalReason
    introduced_in: str
    evidence: Evidence


_MONITOR_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/MonitorController.java"
)
_MONITOR_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/service/MonitorService.java"
)
_AUDIT_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/AuditLogController.java"
)
_API_POM = "dolphinscheduler-api/pom.xml"

_MONITOR_RECORD = "org.apache.dolphinscheduler.dao.entity.MonitorRecord"
_DATABASE_METRICS = "org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics"
_SERVER = "org.apache.dolphinscheduler.common.model.Server"
_WORKER_SERVER = "org.apache.dolphinscheduler.common.model.WorkerServerModel"
_REGISTRY_NODE_TYPE = "org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType"
_AUDIT_RESOURCE_TYPE = "org.apache.dolphinscheduler.common.enums.AuditResourceType"
_AUDIT_OPERATION_TYPE = "org.apache.dolphinscheduler.common.enums.AuditOperationType"
_PAGE_INFO = "org.apache.dolphinscheduler.api.utils.PageInfo"
_AUDIT_DTO = "org.apache.dolphinscheduler.api.dto.AuditDto"
_AUDIT_MODEL_TYPE_DTO = "org.apache.dolphinscheduler.api.dto.auditLog.AuditModelTypeDto"
_AUDIT_OPERATION_TYPE_DTO = (
    "org.apache.dolphinscheduler.api.dto.auditLog.AuditOperationTypeDto"
)

_FIXED_MONITOR_NO_HEALTH = MonitorRecipe(
    health_support="absent",
    database_operation="MonitorController.queryDatabaseState",
    database_model=_MONITOR_RECORD,
    server_wire="fixed-routes",
    server_field_wire="legacy",
    server_operations=(
        "MonitorController.listMaster",
        "MonitorController.listWorker",
    ),
    server_models=(_SERVER, _WORKER_SERVER),
    server_enums=(),
    node_types=("MASTER", "WORKER"),
)
_FIXED_MONITOR = MonitorRecipe(
    health_support="supported",
    database_operation="MonitorController.queryDatabaseState",
    database_model=_MONITOR_RECORD,
    server_wire="fixed-routes",
    server_field_wire="legacy",
    server_operations=(
        "MonitorController.listMaster",
        "MonitorController.listWorker",
    ),
    server_models=(_SERVER, _WORKER_SERVER),
    server_enums=(),
    node_types=("MASTER", "WORKER"),
)
_TYPED_FIXED_MONITOR = MonitorRecipe(
    health_support="supported",
    database_operation="MonitorController.queryDatabaseState",
    database_model=_DATABASE_METRICS,
    server_wire="fixed-routes",
    server_field_wire="legacy",
    server_operations=(
        "MonitorController.listMaster",
        "MonitorController.listWorker",
    ),
    server_models=(_SERVER, _WORKER_SERVER),
    server_enums=(),
    node_types=("MASTER", "WORKER"),
)
_LEGACY_NODE_TYPE_MONITOR = MonitorRecipe(
    health_support="supported",
    database_operation="MonitorController.queryDatabaseState",
    database_model=_DATABASE_METRICS,
    server_wire="node-type-route",
    server_field_wire="legacy",
    server_operations=("MonitorController.listServer",),
    server_models=(_SERVER,),
    server_enums=(_REGISTRY_NODE_TYPE,),
    node_types=("MASTER", "WORKER", "ALERT_SERVER"),
)
_CANONICAL_NODE_TYPE_MONITOR = MonitorRecipe(
    health_support="supported",
    database_operation="MonitorController.queryDatabaseState",
    database_model=_DATABASE_METRICS,
    server_wire="node-type-route",
    server_field_wire="canonical",
    server_operations=("MonitorController.listServer",),
    server_models=(_SERVER,),
    server_enums=(_REGISTRY_NODE_TYPE,),
    node_types=("MASTER", "WORKER", "ALERT_SERVER"),
)

_AUDIT_ABSENT = AuditRecipe(
    support="absent",
    filter_wire="absent",
    record_wire="absent",
    list_operation=None,
    metadata_support="absent",
    model_type_operation=None,
    operation_type_operation=None,
    model_name_filter=False,
    model_type_values=(),
    operation_type_values=(),
)
_AUDIT_SINGULAR = AuditRecipe(
    support="supported",
    filter_wire="singular-enums",
    record_wire="legacy-resource",
    list_operation="AuditLogController.queryAuditLogListPaging",
    metadata_support="absent",
    model_type_operation=None,
    operation_type_operation=None,
    model_name_filter=False,
    model_type_values=("USER_MODULE", "PROJECT_MODULE"),
    operation_type_values=("CREATE", "READ", "UPDATE", "DELETE"),
)
_AUDIT_CSV = AuditRecipe(
    support="supported",
    filter_wire="csv-text",
    record_wire="canonical-model",
    list_operation="AuditLogController.queryAuditLogListPaging",
    metadata_support="supported",
    model_type_operation="AuditLogController.queryAuditModelTypeList",
    operation_type_operation="AuditLogController.queryAuditOperationTypeList",
    model_name_filter=True,
    model_type_values=(),
    operation_type_values=(),
)


# Every entry is an independently reviewed exact tag.  Shared immutable recipes
# record equal contracts; they do not infer one release from a neighbouring tag.
OBSERVABILITY_CONTRACTS: dict[str, ObservabilityVersionContract] = {
    "1.3.9": ObservabilityVersionContract(
        "1.3.9", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.0": ObservabilityVersionContract(
        "2.0.0", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.1": ObservabilityVersionContract(
        "2.0.1", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.2": ObservabilityVersionContract(
        "2.0.2", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.3": ObservabilityVersionContract(
        "2.0.3", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.4": ObservabilityVersionContract(
        "2.0.4", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.5": ObservabilityVersionContract(
        "2.0.5", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.6": ObservabilityVersionContract(
        "2.0.6", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.7": ObservabilityVersionContract(
        "2.0.7", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.8": ObservabilityVersionContract(
        "2.0.8", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "2.0.9": ObservabilityVersionContract(
        "2.0.9", _FIXED_MONITOR_NO_HEALTH, _AUDIT_ABSENT
    ),
    "3.0.0": ObservabilityVersionContract("3.0.0", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.1": ObservabilityVersionContract("3.0.1", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.2": ObservabilityVersionContract("3.0.2", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.3": ObservabilityVersionContract("3.0.3", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.4": ObservabilityVersionContract("3.0.4", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.5": ObservabilityVersionContract("3.0.5", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.0.6": ObservabilityVersionContract("3.0.6", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.0": ObservabilityVersionContract("3.1.0", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.1": ObservabilityVersionContract("3.1.1", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.2": ObservabilityVersionContract("3.1.2", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.3": ObservabilityVersionContract("3.1.3", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.4": ObservabilityVersionContract("3.1.4", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.5": ObservabilityVersionContract("3.1.5", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.6": ObservabilityVersionContract("3.1.6", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.7": ObservabilityVersionContract("3.1.7", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.8": ObservabilityVersionContract("3.1.8", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.1.9": ObservabilityVersionContract("3.1.9", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.2.0": ObservabilityVersionContract("3.2.0", _FIXED_MONITOR, _AUDIT_SINGULAR),
    "3.2.1": ObservabilityVersionContract(
        "3.2.1", _TYPED_FIXED_MONITOR, _AUDIT_SINGULAR
    ),
    "3.2.2": ObservabilityVersionContract(
        "3.2.2", _LEGACY_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.3.1": ObservabilityVersionContract(
        "3.3.1", _CANONICAL_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.3.2": ObservabilityVersionContract(
        "3.3.2", _CANONICAL_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.4.0": ObservabilityVersionContract(
        "3.4.0", _CANONICAL_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.4.1": ObservabilityVersionContract(
        "3.4.1", _CANONICAL_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.4.2": ObservabilityVersionContract(
        "3.4.2", _CANONICAL_NODE_TYPE_MONITOR, _AUDIT_CSV
    ),
    "3.4.3": ObservabilityVersionContract(
        "3.4.3",
        _CANONICAL_NODE_TYPE_MONITOR,
        replace(_AUDIT_CSV, visibility="admin-all-or-self"),
    ),
}


def observability_contract(version: str) -> ObservabilityVersionContract:
    """Return one reviewed contract, rejecting unreviewed version inference."""
    try:
        return OBSERVABILITY_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed observability-domain contract"
        raise ValueError(message) from exc


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return generated operation closure for supported remote actions."""
    contract = observability_contract(version)
    sources: dict[str, tuple[str, ...]] = {
        "monitor.database": (contract.monitor.database_operation,),
        "monitor.server": contract.monitor.server_operations,
    }
    if contract.audit.list_operation is not None:
        sources["audit.list"] = (contract.audit.list_operation,)
    if contract.audit.model_type_operation is not None:
        sources["audit.model-types"] = (contract.audit.model_type_operation,)
    if contract.audit.operation_type_operation is not None:
        sources["audit.operation-types"] = (contract.audit.operation_type_operation,)
    return sources


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return exact structured type roots required by each generated action."""
    contract = observability_contract(version)
    roots: dict[str, tuple[str, ...]] = {
        "monitor.database": (contract.monitor.database_model,),
        "monitor.server": contract.monitor.server_models,
    }
    if contract.audit.list_operation is not None:
        roots["audit.list"] = (_PAGE_INFO, _AUDIT_DTO)
    if contract.audit.model_type_operation is not None:
        roots["audit.model-types"] = (_AUDIT_MODEL_TYPE_DTO,)
    if contract.audit.operation_type_operation is not None:
        roots["audit.operation-types"] = (_AUDIT_OPERATION_TYPE_DTO,)
    return roots


def semantic_operation_enum_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return exact enum roots required by each generated action."""
    contract = observability_contract(version)
    roots: dict[str, tuple[str, ...]] = dict.fromkeys(
        semantic_operation_sources(version), ()
    )
    roots["monitor.server"] = contract.monitor.server_enums
    if contract.audit.filter_wire == "singular-enums":
        roots["audit.list"] = (_AUDIT_RESOURCE_TYPE, _AUDIT_OPERATION_TYPE)
    return roots


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Return runtime-relevant exact facets for each generated action."""
    contract = observability_contract(version)
    facets: dict[str, dict[str, object]] = {
        "monitor.database": {
            "database_model": contract.monitor.database_model,
        },
        "monitor.server": {
            "server_wire": contract.monitor.server_wire,
            "server_field_wire": contract.monitor.server_field_wire,
            "node_types": list(contract.monitor.node_types),
        },
    }
    if contract.audit.list_operation is not None:
        facets["audit.list"] = {
            "filter_wire": contract.audit.filter_wire,
            "record_wire": contract.audit.record_wire,
            "model_name_filter": contract.audit.model_name_filter,
            "model_type_values": list(contract.audit.model_type_values),
            "operation_type_values": list(contract.audit.operation_type_values),
        }
    if contract.audit.visibility == "admin-all-or-self":
        facets["audit.list"]["visibility"] = contract.audit.visibility
    if contract.audit.model_type_operation is not None:
        facets["audit.model-types"] = {}
    if contract.audit.operation_type_operation is not None:
        facets["audit.operation-types"] = {}
    return facets


def action_support(version: str) -> dict[str, Support]:
    """Return stable action-level support without hiding option-level drift."""
    contract = observability_contract(version)
    return {
        "monitor.health": contract.monitor.health_support,
        "monitor.database": "supported",
        "monitor.server": "supported",
        "audit.list": contract.audit.support,
        "audit.model-types": contract.audit.metadata_support,
        "audit.operation-types": contract.audit.metadata_support,
    }


def terminal_absences(version: str) -> dict[str, TerminalAbsence]:
    """Return source-proven stable actions absent from one exact release."""
    contract = observability_contract(version)
    absences: dict[str, TerminalAbsence] = {}
    if contract.monitor.health_support == "absent":
        evidence = Evidence(
            version=version,
            kind="build",
            source=_API_POM,
            symbol="dependencies",
            conclusion=(
                "The API module predates its dolphinscheduler-meter actuator "
                "dependency and exposes no actuator health endpoint."
            ),
        )
        absences["monitor.health"] = TerminalAbsence(
            semantic_operation="monitor.health",
            reason="upstream_capability_absent",
            introduced_in="3.0.0",
            evidence=evidence,
        )
    if contract.audit.support == "absent":
        evidence = Evidence(
            version=version,
            kind="controller",
            source=(
                "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
                "api/controller"
            ),
            symbol="controller inventory",
            conclusion="The exact API controller inventory has no audit controller.",
        )
        absences["audit.list"] = TerminalAbsence(
            semantic_operation="audit.list",
            reason="upstream_capability_absent",
            introduced_in="3.0.0",
            evidence=evidence,
        )
        for action in ("audit.model-types", "audit.operation-types"):
            absences[action] = TerminalAbsence(
                semantic_operation=action,
                reason="upstream_capability_absent",
                introduced_in="3.2.2",
                evidence=evidence,
            )
    elif contract.audit.metadata_support == "absent":
        evidence = Evidence(
            version=version,
            kind="controller",
            source=_AUDIT_CONTROLLER,
            symbol="queryAuditLogListPaging",
            conclusion=(
                "The controller exposes audit paging but no model-type or "
                "operation-type discovery endpoints."
            ),
        )
        for action in ("audit.model-types", "audit.operation-types"):
            absences[action] = TerminalAbsence(
                semantic_operation=action,
                reason="upstream_capability_absent",
                introduced_in="3.2.2",
                evidence=evidence,
            )
    return absences


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return exact controller/service evidence for generated operations."""
    contract = observability_contract(version)
    evidence: dict[str, tuple[Evidence, ...]] = {
        "monitor.database": (
            Evidence(
                version,
                "controller",
                _MONITOR_CONTROLLER,
                contract.monitor.database_operation.partition(".")[2],
                "The controller defines the exact database-monitor route.",
            ),
            Evidence(
                version,
                "service",
                _MONITOR_SERVICE,
                "queryDatabaseState",
                "The service return shape determines the generated payload model.",
            ),
        ),
        "monitor.server": (
            Evidence(
                version,
                "controller",
                _MONITOR_CONTROLLER,
                ";".join(
                    operation.partition(".")[2]
                    for operation in contract.monitor.server_operations
                ),
                "The controller defines the exact fixed or node-type server routes.",
            ),
        ),
    }
    if contract.audit.list_operation is not None:
        evidence["audit.list"] = (
            Evidence(
                version,
                "controller",
                _AUDIT_CONTROLLER,
                contract.audit.list_operation.partition(".")[2],
                "The controller defines the exact audit filters and page result.",
            ),
        )
    if contract.audit.visibility == "admin-all-or-self":
        evidence["audit.list"] = (
            *evidence["audit.list"],
            Evidence(
                version,
                "service",
                "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/AuditServiceImpl.java",
                "queryLogListPaging",
                (
                    "The service restricts non-admins to loginUser.id; administrators "
                    "query all actors. Filters cannot expand that scope."
                ),
            ),
        )
    if contract.audit.model_type_operation is not None:
        evidence["audit.model-types"] = (
            Evidence(
                version,
                "controller",
                _AUDIT_CONTROLLER,
                contract.audit.model_type_operation.partition(".")[2],
                "The controller exposes the model-type discovery tree.",
            ),
        )
    if contract.audit.operation_type_operation is not None:
        evidence["audit.operation-types"] = (
            Evidence(
                version,
                "controller",
                _AUDIT_CONTROLLER,
                contract.audit.operation_type_operation.partition(".")[2],
                "The controller exposes operation-type discovery values.",
            ),
        )
    return evidence


__all__ = [
    "AUDIT_SEMANTIC_OPERATIONS",
    "MONITOR_GENERATED_OPERATIONS",
    "OBSERVABILITY_CONTRACTS",
    "TARGET_OBSERVABILITY_VERSIONS",
    "AuditRecipe",
    "Evidence",
    "MonitorRecipe",
    "ObservabilityVersionContract",
    "TerminalAbsence",
    "action_support",
    "observability_contract",
    "semantic_operation_enum_roots",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "terminal_absences",
]
