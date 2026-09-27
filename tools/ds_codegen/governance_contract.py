"""Reviewed exact-version recipes for datasources and Kubernetes namespaces.

This module is deliberately independent of the central runtime-binding tables.
It records the source facts needed to integrate the governance domain into those
tables without encoding semantic-version ranges or inferring support from a
neighbouring release.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TARGET_GOVERNANCE_VERSIONS = (
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

DATASOURCE_SEMANTIC_OPERATIONS = (
    "datasource.page",
    "datasource.get",
    "datasource.create",
    "datasource.update",
    "datasource.delete",
    "datasource.saved-test",
)
NAMESPACE_SEMANTIC_OPERATIONS = (
    "namespace.page",
    "namespace.get",
    "namespace.available",
    "namespace.create",
    "namespace.delete",
)
GOVERNANCE_SEMANTIC_OPERATIONS = (
    *DATASOURCE_SEMANTIC_OPERATIONS,
    *NAMESPACE_SEMANTIC_OPERATIONS,
)

Support = Literal["supported", "absent"]
PayloadWire = Literal["legacy-form", "typed-body", "json-string"]
MutationResult = Literal["none", "entity", "boolean"]
PasswordVisibility = Literal["clear", "masked"]
PasswordUpdate = Literal["explicit-value", "blank-preserves"]
NamespaceSelector = Literal["absent", "k8s", "cluster-code"]
EvidenceKind = Literal["controller", "ui", "snapshot"]
TerminalReason = Literal["upstream_capability_absent"]

_CLEAR_PASSWORD: PasswordVisibility = "clear"  # noqa: S105
_MASKED_PASSWORD: PasswordVisibility = "masked"  # noqa: S105
_EXPLICIT_PASSWORD_UPDATE: PasswordUpdate = "explicit-value"  # noqa: S105
_PRESERVED_BLANK_PASSWORD: PasswordUpdate = "blank-preserves"  # noqa: S105


@dataclass(frozen=True)
class Evidence:
    """One exact-version source coordinate supporting a reviewed decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the conventional source reference consumed by profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class DataSourceRecipe:
    """Wire, result, and sensitive-field decisions for one exact version."""

    payload_wire: PayloadWire
    delete_operation: str
    create_result: MutationResult
    update_result: MutationResult
    delete_result: MutationResult
    saved_test_result: MutationResult
    password_visibility: PasswordVisibility
    password_update: PasswordUpdate
    update_body_required: bool


@dataclass(frozen=True)
class NamespaceRecipe:
    """Wire and side-effect decisions for one exact namespace contract."""

    support: Support
    page_operation: str | None
    selector: NamespaceSelector
    quotas_supported: bool
    create_result: MutationResult
    creates_kubernetes_namespace_if_absent: bool
    deletes_kubernetes_namespace: bool


@dataclass(frozen=True)
class GovernanceVersionContract:
    """Reviewed governance-domain contract for one exact DS version."""

    version: str
    datasource: DataSourceRecipe
    namespace: NamespaceRecipe


@dataclass(frozen=True)
class TerminalAbsence:
    """One stable action proven terminally unavailable for an exact version."""

    semantic_operation: str
    reason: TerminalReason
    introduced_in: str
    evidence: Evidence


_DATA_SOURCE_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/DataSourceController.java"
)
_NAMESPACE_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/K8sNamespaceController.java"
)
_LEGACY_DATASOURCE_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/datasource/pages/list/_source/list.vue"
)
_DATASOURCE_UI = "dolphinscheduler-ui/src/service/modules/data-source/index.ts"
_NAMESPACE_UI = "dolphinscheduler-ui/src/service/modules/k8s-namespace/index.ts"

_PAGE_INFO_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_DATASOURCE_MODEL = "org.apache.dolphinscheduler.dao.entity.DataSource"
_NAMESPACE_MODEL = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_LEGACY_DATASOURCE_DETAIL_MODEL = "generated.view.DataSourceService_queryDataSource_map"
_DATASOURCE_DETAIL_MODEL_200 = (
    "org.apache.dolphinscheduler.common.datasource.BaseDataSourceParamDTO"
)
_DATASOURCE_DETAIL_MODEL_209 = (
    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
    "BaseDataSourceParamDTO"
)

_DATASOURCE_LEGACY = DataSourceRecipe(
    payload_wire="legacy-form",
    delete_operation="DataSourceController.delete",
    create_result="none",
    update_result="none",
    delete_result="none",
    saved_test_result="none",
    password_visibility=_CLEAR_PASSWORD,
    password_update=_EXPLICIT_PASSWORD_UPDATE,
    update_body_required=False,
)
_DATASOURCE_TYPED = DataSourceRecipe(
    payload_wire="typed-body",
    delete_operation="DataSourceController.delete",
    create_result="none",
    update_result="none",
    delete_result="none",
    saved_test_result="none",
    password_visibility=_CLEAR_PASSWORD,
    password_update=_PRESERVED_BLANK_PASSWORD,
    update_body_required=False,
)
_DATASOURCE_JSON_VOID = DataSourceRecipe(
    payload_wire="json-string",
    delete_operation="DataSourceController.deleteDataSource",
    create_result="none",
    update_result="none",
    delete_result="none",
    saved_test_result="none",
    password_visibility=_CLEAR_PASSWORD,
    password_update=_PRESERVED_BLANK_PASSWORD,
    update_body_required=False,
)
_DATASOURCE_JSON_ENTITY = DataSourceRecipe(
    payload_wire="json-string",
    delete_operation="DataSourceController.deleteDataSource",
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    saved_test_result="boolean",
    password_visibility=_MASKED_PASSWORD,
    password_update=_PRESERVED_BLANK_PASSWORD,
    update_body_required=False,
)
_DATASOURCE_342 = DataSourceRecipe(
    payload_wire="json-string",
    delete_operation="DataSourceController.deleteDataSource",
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    saved_test_result="boolean",
    password_visibility=_MASKED_PASSWORD,
    password_update=_PRESERVED_BLANK_PASSWORD,
    update_body_required=True,
)

_NAMESPACE_ABSENT = NamespaceRecipe(
    support="absent",
    page_operation=None,
    selector="absent",
    quotas_supported=False,
    create_result="none",
    creates_kubernetes_namespace_if_absent=False,
    deletes_kubernetes_namespace=False,
)
_NAMESPACE_30 = NamespaceRecipe(
    support="supported",
    page_operation="K8sNamespaceController.queryProjectListPaging",
    selector="k8s",
    quotas_supported=True,
    create_result="none",
    creates_kubernetes_namespace_if_absent=True,
    deletes_kubernetes_namespace=True,
)
_NAMESPACE_31 = NamespaceRecipe(
    support="supported",
    page_operation="K8sNamespaceController.queryNamespaceListPaging",
    selector="cluster-code",
    quotas_supported=True,
    create_result="none",
    creates_kubernetes_namespace_if_absent=True,
    deletes_kubernetes_namespace=True,
)
_NAMESPACE_320 = NamespaceRecipe(
    support="supported",
    page_operation="K8sNamespaceController.queryNamespaceListPaging",
    selector="cluster-code",
    quotas_supported=True,
    create_result="none",
    creates_kubernetes_namespace_if_absent=True,
    deletes_kubernetes_namespace=False,
)
_NAMESPACE_321 = NamespaceRecipe(
    support="supported",
    page_operation="K8sNamespaceController.queryNamespaceListPaging",
    selector="cluster-code",
    quotas_supported=False,
    create_result="none",
    creates_kubernetes_namespace_if_absent=True,
    deletes_kubernetes_namespace=False,
)
_NAMESPACE_ENTITY = NamespaceRecipe(
    support="supported",
    page_operation="K8sNamespaceController.queryNamespaceListPaging",
    selector="cluster-code",
    quotas_supported=False,
    create_result="entity",
    creates_kubernetes_namespace_if_absent=True,
    deletes_kubernetes_namespace=False,
)

# Every key is an independently reviewed upstream tag. Shared immutable values
# express equal contracts without treating equality as proof for another tag.
GOVERNANCE_CONTRACTS: dict[str, GovernanceVersionContract] = {
    "1.3.9": GovernanceVersionContract("1.3.9", _DATASOURCE_LEGACY, _NAMESPACE_ABSENT),
    "2.0.0": GovernanceVersionContract("2.0.0", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.1": GovernanceVersionContract("2.0.1", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.2": GovernanceVersionContract("2.0.2", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.3": GovernanceVersionContract("2.0.3", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.4": GovernanceVersionContract("2.0.4", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.5": GovernanceVersionContract("2.0.5", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.6": GovernanceVersionContract("2.0.6", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.7": GovernanceVersionContract("2.0.7", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.8": GovernanceVersionContract("2.0.8", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "2.0.9": GovernanceVersionContract("2.0.9", _DATASOURCE_TYPED, _NAMESPACE_ABSENT),
    "3.0.0": GovernanceVersionContract("3.0.0", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.1": GovernanceVersionContract("3.0.1", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.2": GovernanceVersionContract("3.0.2", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.3": GovernanceVersionContract("3.0.3", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.4": GovernanceVersionContract("3.0.4", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.5": GovernanceVersionContract("3.0.5", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.0.6": GovernanceVersionContract("3.0.6", _DATASOURCE_TYPED, _NAMESPACE_30),
    "3.1.0": GovernanceVersionContract("3.1.0", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.1": GovernanceVersionContract("3.1.1", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.2": GovernanceVersionContract("3.1.2", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.3": GovernanceVersionContract("3.1.3", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.4": GovernanceVersionContract("3.1.4", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.5": GovernanceVersionContract("3.1.5", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.6": GovernanceVersionContract("3.1.6", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.7": GovernanceVersionContract("3.1.7", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.8": GovernanceVersionContract("3.1.8", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.1.9": GovernanceVersionContract("3.1.9", _DATASOURCE_JSON_VOID, _NAMESPACE_31),
    "3.2.0": GovernanceVersionContract("3.2.0", _DATASOURCE_JSON_VOID, _NAMESPACE_320),
    "3.2.1": GovernanceVersionContract(
        "3.2.1", _DATASOURCE_JSON_ENTITY, _NAMESPACE_321
    ),
    "3.2.2": GovernanceVersionContract(
        "3.2.2", _DATASOURCE_JSON_ENTITY, _NAMESPACE_ENTITY
    ),
    "3.3.1": GovernanceVersionContract(
        "3.3.1", _DATASOURCE_JSON_ENTITY, _NAMESPACE_ENTITY
    ),
    "3.3.2": GovernanceVersionContract(
        "3.3.2", _DATASOURCE_JSON_ENTITY, _NAMESPACE_ENTITY
    ),
    "3.4.0": GovernanceVersionContract(
        "3.4.0", _DATASOURCE_JSON_ENTITY, _NAMESPACE_ENTITY
    ),
    "3.4.1": GovernanceVersionContract(
        "3.4.1", _DATASOURCE_JSON_ENTITY, _NAMESPACE_ENTITY
    ),
    "3.4.2": GovernanceVersionContract("3.4.2", _DATASOURCE_342, _NAMESPACE_ENTITY),
    "3.4.3": GovernanceVersionContract("3.4.3", _DATASOURCE_342, _NAMESPACE_ENTITY),
}


def governance_contract(version: str) -> GovernanceVersionContract:
    """Return one reviewed contract, rejecting unreviewed version inference."""
    try:
        return GOVERNANCE_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed governance-domain contract"
        raise ValueError(message) from exc


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return every source operation used by each stable governance action."""
    contract = governance_contract(version)
    datasource_delete = contract.datasource.delete_operation
    operations: dict[str, tuple[str, ...]] = {
        "datasource.page": ("DataSourceController.queryDataSourceListPaging",),
        "datasource.get": (
            "DataSourceController.queryDataSourceListPaging",
            "DataSourceController.queryDataSource",
        ),
        "datasource.create": (
            "DataSourceController.queryDataSourceListPaging",
            "DataSourceController.queryDataSource",
            "DataSourceController.createDataSource",
        ),
        "datasource.update": (
            "DataSourceController.queryDataSourceListPaging",
            "DataSourceController.queryDataSource",
            "DataSourceController.updateDataSource",
        ),
        "datasource.delete": (
            "DataSourceController.queryDataSourceListPaging",
            "DataSourceController.queryDataSource",
            datasource_delete,
        ),
        "datasource.saved-test": (
            "DataSourceController.queryDataSourceListPaging",
            "DataSourceController.queryDataSource",
            "DataSourceController.connectionTest",
        ),
    }
    page_operation = contract.namespace.page_operation
    if contract.namespace.support == "supported":
        if page_operation is None:
            message = f"DS {version} namespace recipe is missing its page operation"
            raise ValueError(message)
        operations.update(
            {
                "namespace.page": (page_operation,),
                "namespace.get": (page_operation,),
                "namespace.available": (
                    "K8sNamespaceController.queryAvailableNamespaceList",
                ),
                "namespace.create": (
                    page_operation,
                    "K8sNamespaceController.createNamespace",
                ),
                "namespace.delete": (
                    page_operation,
                    "K8sNamespaceController.delNamespaceById",
                ),
            }
        )
    return operations


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit model roots for the governance runtime slice."""
    sources = semantic_operation_sources(version)
    detail_model = _datasource_detail_model(version)
    roots: dict[str, tuple[str, ...]] = {
        "datasource.page": (_PAGE_INFO_MODEL, _DATASOURCE_MODEL),
        "datasource.get": (detail_model,),
        "datasource.create": (
            _PAGE_INFO_MODEL,
            _DATASOURCE_MODEL,
            detail_model,
        ),
        "datasource.update": (_DATASOURCE_MODEL, detail_model),
        "datasource.delete": (),
        "datasource.saved-test": (),
    }
    if "namespace.page" in sources:
        roots.update(
            {
                "namespace.page": (_PAGE_INFO_MODEL, _NAMESPACE_MODEL),
                "namespace.get": (_PAGE_INFO_MODEL, _NAMESPACE_MODEL),
                "namespace.available": (_NAMESPACE_MODEL,),
                "namespace.create": (_PAGE_INFO_MODEL, _NAMESPACE_MODEL),
                "namespace.delete": (),
            }
        )
    return roots


def action_support(version: str) -> dict[str, Support]:
    """Return explicit supported/absent decisions for all stable actions."""
    contract = governance_contract(version)
    support: dict[str, Support] = dict.fromkeys(
        DATASOURCE_SEMANTIC_OPERATIONS,
        "supported",
    )
    support.update(
        dict.fromkeys(NAMESPACE_SEMANTIC_OPERATIONS, contract.namespace.support)
    )
    return support


def terminal_absences(version: str) -> dict[str, TerminalAbsence]:
    """Return terminal upstream absences without converting them to build gaps."""
    contract = governance_contract(version)
    if contract.namespace.support == "supported":
        return {}
    evidence = Evidence(
        version=version,
        kind="snapshot",
        source=f"build/ds_contract/snapshots-v2/ds-{version}-contract.json",
        symbol="operations",
        conclusion=(
            "The exact source snapshot contains no K8sNamespaceController; "
            "namespace management is introduced in 3.0.0."
        ),
    )
    return {
        operation: TerminalAbsence(
            semantic_operation=operation,
            reason="upstream_capability_absent",
            introduced_in="3.0.0",
            evidence=evidence,
        )
        for operation in NAMESPACE_SEMANTIC_OPERATIONS
    }


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Project exact action-level behavior for generated profile metadata."""
    contract = governance_contract(version)
    datasource = contract.datasource
    facets: dict[str, dict[str, object]] = {
        "datasource.page": {"result": "page"},
        "datasource.get": {
            "result": "detail",
            "password_visibility": datasource.password_visibility,
        },
        "datasource.create": {
            "payload_wire": datasource.payload_wire,
            "result": datasource.create_result,
            "readback": "page-then-detail",
        },
        "datasource.update": {
            "payload_wire": datasource.payload_wire,
            "result": datasource.update_result,
            "password_update": datasource.password_update,
            "request_body_required": datasource.update_body_required,
            "readback": "detail",
        },
        "datasource.delete": {"result": datasource.delete_result},
        "datasource.saved-test": {"result": datasource.saved_test_result},
    }
    namespace = contract.namespace
    if namespace.support == "supported":
        facets.update(
            {
                "namespace.page": {"result": "page"},
                "namespace.get": {
                    "result": "detail",
                    "resolution": "exact-name-or-id-via-page",
                },
                "namespace.available": {"result": "list"},
                "namespace.create": {
                    "selector": namespace.selector,
                    "quotas_supported": namespace.quotas_supported,
                    "result": namespace.create_result,
                    "creates_kubernetes_namespace_if_absent": (
                        namespace.creates_kubernetes_namespace_if_absent
                    ),
                    "readback": "page",
                },
                "namespace.delete": {
                    "result": "none",
                    "deletes_kubernetes_namespace": (
                        namespace.deletes_kubernetes_namespace
                    ),
                },
            }
        )
    return facets


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return controller and retained-UI evidence for each supported action."""
    sources = semantic_operation_sources(version)
    datasource_ui = (
        _LEGACY_DATASOURCE_UI
        if version
        in {
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
        }
        else _DATASOURCE_UI
    )
    evidence: dict[str, tuple[Evidence, ...]] = {}
    for operation, source_operations in sources.items():
        controller_source = (
            _DATA_SOURCE_CONTROLLER
            if operation.startswith("datasource.")
            else _NAMESPACE_CONTROLLER
        )
        ui_source = (
            datasource_ui if operation.startswith("datasource.") else _NAMESPACE_UI
        )
        evidence[operation] = (
            Evidence(
                version=version,
                kind="controller",
                source=controller_source,
                symbol=";".join(item.partition(".")[2] for item in source_operations),
                conclusion="Exact controller operations used by the stable action.",
            ),
            Evidence(
                version=version,
                kind="ui",
                source=ui_source,
                symbol="request",
                conclusion="The retained UI confirms the user-facing route and inputs.",
            ),
        )
    return evidence


def _datasource_detail_model(version: str) -> str:
    if version == "1.3.9":
        return _LEGACY_DATASOURCE_DETAIL_MODEL
    if version == "2.0.0":
        return _DATASOURCE_DETAIL_MODEL_200
    return _DATASOURCE_DETAIL_MODEL_209


__all__ = [
    "DATASOURCE_SEMANTIC_OPERATIONS",
    "GOVERNANCE_CONTRACTS",
    "GOVERNANCE_SEMANTIC_OPERATIONS",
    "NAMESPACE_SEMANTIC_OPERATIONS",
    "TARGET_GOVERNANCE_VERSIONS",
    "DataSourceRecipe",
    "Evidence",
    "GovernanceVersionContract",
    "NamespaceRecipe",
    "TerminalAbsence",
    "action_support",
    "governance_contract",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "terminal_absences",
]
