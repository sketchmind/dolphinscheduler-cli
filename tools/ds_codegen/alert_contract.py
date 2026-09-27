"""Reviewed exact-version recipes for alert groups and alert plugins.

The alert domain has two real upstream boundaries that must not be hidden by
version ranges.  DolphinScheduler 1.3.9 has EMAIL/SMS alert groups but no
plugin-instance model, and the plugin test-send operation is introduced only
in 3.2.1.  Every table entry below represents a separately reviewed tag; shared
recipe values express equal contracts, not inferred inheritance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TARGET_ALERT_VERSIONS = (
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

ALERT_GROUP_SEMANTIC_OPERATIONS = (
    "alert-group.page",
    "alert-group.get",
    "alert-group.create",
    "alert-group.update",
    "alert-group.delete",
)
ALERT_PLUGIN_SEMANTIC_OPERATIONS = (
    "alert-plugin.page",
    "alert-plugin.get",
    "alert-plugin.definition.list",
    "alert-plugin.schema",
    "alert-plugin.create",
    "alert-plugin.update",
    "alert-plugin.delete",
    "alert-plugin.test",
)
ALERT_SEMANTIC_OPERATIONS = (
    *ALERT_GROUP_SEMANTIC_OPERATIONS,
    *ALERT_PLUGIN_SEMANTIC_OPERATIONS,
)

Support = Literal["supported", "limited", "absent"]
MutationResult = Literal["none", "entity", "boolean"]
GroupAssociation = Literal["legacy-alert-type", "plugin-instance-ids"]
EvidenceKind = Literal["controller", "ui", "snapshot"]
TerminalReason = Literal[
    "upstream_capability_absent",
    "upstream_capability_limited",
]


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
        """Return the source reference serialized into generated profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class AlertGroupRecipe:
    """Wire and result decisions for one exact alert-group controller."""

    association: GroupAssociation
    page_model: str
    direct_get: bool
    create_operation: str
    update_operation: str
    delete_operation: str
    create_result: MutationResult
    update_result: MutationResult
    delete_result: MutationResult


@dataclass(frozen=True)
class AlertPluginRecipe:
    """Wire and result decisions for one exact alert-plugin controller."""

    support: Support
    searchable_page: bool
    create_result: MutationResult
    update_result: MutationResult
    delete_result: MutationResult
    test_send: bool
    transient_delivery_fields: bool


@dataclass(frozen=True)
class AlertVersionContract:
    """Reviewed alert-domain contract for one exact DS version."""

    version: str
    group: AlertGroupRecipe
    plugin: AlertPluginRecipe


@dataclass(frozen=True)
class TerminalDecision:
    """One stable action proven unavailable or limited upstream."""

    semantic_operation: str
    reason: TerminalReason
    constraint: str
    evidence: Evidence


_GROUP_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/AlertGroupController.java"
)
_PLUGIN_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/AlertPluginInstanceController.java"
)
_UI_PLUGIN_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/UiPluginController.java"
)
_LEGACY_GROUP_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/"
    "warningGroups/_source/createWarning.vue"
)
_LEGACY_PLUGIN_UI = (
    "dolphinscheduler-ui/src/js/conf/home/pages/security/pages/"
    "warningInstance/_source/createWarningInstance.vue"
)
_GROUP_UI = "dolphinscheduler-ui/src/service/modules/alert-group/index.ts"
_PLUGIN_UI = "dolphinscheduler-ui/src/service/modules/alert-plugin/index.ts"

_PAGE_INFO_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_RESULT_MODEL = "org.apache.dolphinscheduler.api.utils.Result"
_ALERT_GROUP_MODEL = "org.apache.dolphinscheduler.dao.entity.AlertGroup"
_ALERT_GROUP_VO_200 = "org.apache.dolphinscheduler.dao.vo.AlertGroupVo"
_ALERT_PLUGIN_MODEL = "org.apache.dolphinscheduler.dao.entity.AlertPluginInstance"
_ALERT_PLUGIN_VO = "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO"
_PLUGIN_DEFINE_MODEL = "org.apache.dolphinscheduler.dao.entity.PluginDefine"
_PLUGIN_TYPE_ENUM = "org.apache.dolphinscheduler.common.enums.PluginType"
_ALERT_PLUGIN_INSTANCE_TYPE_ENUM = (
    "org.apache.dolphinscheduler.common.enums.AlertPluginInstanceType"
)
_WARNING_TYPE_ENUM = "org.apache.dolphinscheduler.common.enums.WarningType"
_ALERT_TYPE_ENUM = "org.apache.dolphinscheduler.common.enums.AlertType"

_GROUP_139 = AlertGroupRecipe(
    association="legacy-alert-type",
    page_model=_ALERT_GROUP_MODEL,
    direct_get=False,
    create_operation="AlertGroupController.createAlertgroup",
    update_operation="AlertGroupController.updateAlertgroup",
    delete_operation="AlertGroupController.delAlertgroupById",
    create_result="none",
    update_result="none",
    delete_result="none",
)
_GROUP_VOID_200 = AlertGroupRecipe(
    association="plugin-instance-ids",
    page_model=_ALERT_GROUP_VO_200,
    direct_get=True,
    create_operation="AlertGroupController.createAlertgroup",
    update_operation="AlertGroupController.updateAlertgroup",
    delete_operation="AlertGroupController.delAlertgroupById",
    create_result="none",
    update_result="none",
    delete_result="none",
)
_GROUP_VOID = AlertGroupRecipe(
    association="plugin-instance-ids",
    page_model=_ALERT_GROUP_MODEL,
    direct_get=True,
    create_operation="AlertGroupController.createAlertgroup",
    update_operation="AlertGroupController.updateAlertgroup",
    delete_operation="AlertGroupController.delAlertgroupById",
    create_result="none",
    update_result="none",
    delete_result="none",
)
_GROUP_CREATE_ENTITY = AlertGroupRecipe(
    association="plugin-instance-ids",
    page_model=_ALERT_GROUP_MODEL,
    direct_get=True,
    create_operation="AlertGroupController.createAlertgroup",
    update_operation="AlertGroupController.updateAlertgroup",
    delete_operation="AlertGroupController.delAlertgroupById",
    create_result="entity",
    update_result="none",
    delete_result="none",
)
_GROUP_ENTITY = AlertGroupRecipe(
    association="plugin-instance-ids",
    page_model=_ALERT_GROUP_MODEL,
    direct_get=True,
    create_operation="AlertGroupController.createAlertGroup",
    update_operation="AlertGroupController.updateAlertGroupById",
    delete_operation="AlertGroupController.deleteAlertGroupById",
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
)

_PLUGIN_ABSENT = AlertPluginRecipe(
    support="absent",
    searchable_page=False,
    create_result="none",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_PLUGIN_200 = AlertPluginRecipe(
    support="supported",
    searchable_page=False,
    create_result="none",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_PLUGIN_209 = AlertPluginRecipe(
    support="supported",
    searchable_page=True,
    create_result="none",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_PLUGIN_CREATE_ENTITY = AlertPluginRecipe(
    support="supported",
    searchable_page=True,
    create_result="entity",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_PLUGIN_TRANSIENT_ENTITY = AlertPluginRecipe(
    support="supported",
    searchable_page=True,
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    test_send=True,
    transient_delivery_fields=True,
)
_PLUGIN_ENTITY = AlertPluginRecipe(
    support="supported",
    searchable_page=True,
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    test_send=True,
    transient_delivery_fields=False,
)


ALERT_CONTRACTS: dict[str, AlertVersionContract] = {
    "1.3.9": AlertVersionContract("1.3.9", _GROUP_139, _PLUGIN_ABSENT),
    "2.0.0": AlertVersionContract("2.0.0", _GROUP_VOID_200, _PLUGIN_200),
    "2.0.1": AlertVersionContract("2.0.1", _GROUP_VOID, _PLUGIN_209),
    "2.0.2": AlertVersionContract("2.0.2", _GROUP_VOID, _PLUGIN_209),
    "2.0.3": AlertVersionContract("2.0.3", _GROUP_VOID, _PLUGIN_209),
    "2.0.4": AlertVersionContract("2.0.4", _GROUP_VOID, _PLUGIN_209),
    "2.0.5": AlertVersionContract("2.0.5", _GROUP_VOID, _PLUGIN_209),
    "2.0.6": AlertVersionContract("2.0.6", _GROUP_VOID, _PLUGIN_209),
    "2.0.7": AlertVersionContract("2.0.7", _GROUP_VOID, _PLUGIN_209),
    "2.0.8": AlertVersionContract("2.0.8", _GROUP_VOID, _PLUGIN_209),
    "2.0.9": AlertVersionContract("2.0.9", _GROUP_VOID, _PLUGIN_209),
    "3.0.0": AlertVersionContract("3.0.0", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.1": AlertVersionContract("3.0.1", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.2": AlertVersionContract("3.0.2", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.3": AlertVersionContract("3.0.3", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.4": AlertVersionContract("3.0.4", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.5": AlertVersionContract("3.0.5", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.0.6": AlertVersionContract("3.0.6", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.0": AlertVersionContract("3.1.0", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.1": AlertVersionContract("3.1.1", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.2": AlertVersionContract("3.1.2", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.3": AlertVersionContract("3.1.3", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.4": AlertVersionContract("3.1.4", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.5": AlertVersionContract("3.1.5", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.6": AlertVersionContract("3.1.6", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.7": AlertVersionContract("3.1.7", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.8": AlertVersionContract("3.1.8", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.1.9": AlertVersionContract("3.1.9", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.2.0": AlertVersionContract("3.2.0", _GROUP_CREATE_ENTITY, _PLUGIN_CREATE_ENTITY),
    "3.2.1": AlertVersionContract("3.2.1", _GROUP_ENTITY, _PLUGIN_TRANSIENT_ENTITY),
    "3.2.2": AlertVersionContract("3.2.2", _GROUP_ENTITY, _PLUGIN_TRANSIENT_ENTITY),
    "3.3.1": AlertVersionContract("3.3.1", _GROUP_ENTITY, _PLUGIN_ENTITY),
    "3.3.2": AlertVersionContract("3.3.2", _GROUP_ENTITY, _PLUGIN_ENTITY),
    "3.4.0": AlertVersionContract("3.4.0", _GROUP_ENTITY, _PLUGIN_ENTITY),
    "3.4.1": AlertVersionContract("3.4.1", _GROUP_ENTITY, _PLUGIN_ENTITY),
    "3.4.2": AlertVersionContract("3.4.2", _GROUP_ENTITY, _PLUGIN_ENTITY),
    "3.4.3": AlertVersionContract("3.4.3", _GROUP_ENTITY, _PLUGIN_ENTITY),
}


def alert_contract(version: str) -> AlertVersionContract:
    """Return one reviewed exact contract, rejecting version inference."""
    try:
        return ALERT_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed alert-domain contract"
        raise ValueError(message) from exc


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return all source operations required by each executable alert action."""
    contract = alert_contract(version)
    group = contract.group
    group_page = "AlertGroupController.listPaging"
    group_get = (
        (group_page, "AlertGroupController.queryAlertGroupById")
        if group.direct_get
        else (group_page,)
    )
    operations: dict[str, tuple[str, ...]] = {
        "alert-group.page": (group_page,),
        "alert-group.get": group_get,
        "alert-group.create": (*group_get, group.create_operation),
        "alert-group.update": (*group_get, group.update_operation),
        "alert-group.delete": (*group_get, group.delete_operation),
    }

    plugin = contract.plugin
    if plugin.support == "absent":
        return operations
    plugin_page = "AlertPluginInstanceController.listPaging"
    plugin_list = (
        "AlertPluginInstanceController."
        "getAlertPluginInstance__get_alert_plugin_instances_list"
    )
    plugin_detail = (
        "AlertPluginInstanceController."
        "getAlertPluginInstance__get_alert_plugin_instances_id"
    )
    definition_list = "UiPluginController.queryUiPluginsByType"
    definition_get = "UiPluginController.queryUiPluginDetailById"
    operations.update(
        {
            "alert-plugin.page": (plugin_page,),
            "alert-plugin.get": (plugin_page, plugin_list),
            "alert-plugin.definition.list": (definition_list,),
            "alert-plugin.schema": (definition_list, definition_get),
            "alert-plugin.create": (
                definition_list,
                definition_get,
                plugin_list,
                "AlertPluginInstanceController.createAlertPluginInstance",
            ),
            "alert-plugin.update": (
                plugin_page,
                plugin_list,
                definition_list,
                definition_get,
                *((plugin_detail,) if plugin.transient_delivery_fields else ()),
                (
                    "AlertPluginInstanceController.updateAlertPluginInstanceById"
                    if plugin.update_result == "entity"
                    else "AlertPluginInstanceController.updateAlertPluginInstance"
                ),
            ),
            "alert-plugin.delete": (
                plugin_page,
                plugin_list,
                "AlertPluginInstanceController.deleteAlertPluginInstance",
            ),
        }
    )
    if plugin.test_send:
        operations["alert-plugin.test"] = (
            plugin_page,
            plugin_list,
            "AlertPluginInstanceController.testSendAlertPluginInstance",
        )
    return operations


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit structured-model roots for the alert runtime slice."""
    contract = alert_contract(version)
    sources = semantic_operation_sources(version)
    group_models = (
        _PAGE_INFO_MODEL,
        _ALERT_GROUP_MODEL,
        *(
            (_ALERT_GROUP_VO_200,)
            if contract.group.page_model == _ALERT_GROUP_VO_200
            else ()
        ),
    )
    roots: dict[str, tuple[str, ...]] = {
        operation: group_models
        for operation in sources
        if operation.startswith("alert-group.")
    }
    for operation in sources:
        if operation.startswith("alert-plugin.definition") or operation == (
            "alert-plugin.schema"
        ):
            roots[operation] = (_PLUGIN_DEFINE_MODEL,)
        elif operation.startswith("alert-plugin."):
            roots[operation] = (
                _PAGE_INFO_MODEL,
                _ALERT_PLUGIN_MODEL,
                _ALERT_PLUGIN_VO,
                *(
                    (_PLUGIN_DEFINE_MODEL,)
                    if operation in {"alert-plugin.create", "alert-plugin.update"}
                    else ()
                ),
            )
    return roots


def semantic_operation_enum_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return exact enum roots required by selected alert requests."""
    contract = alert_contract(version)
    roots: dict[str, tuple[str, ...]] = {}
    for operation in semantic_operation_sources(version):
        enums: tuple[str, ...] = ()
        if contract.group.association == "legacy-alert-type" and operation in {
            "alert-group.create",
            "alert-group.update",
        }:
            enums = (_ALERT_TYPE_ENUM,)
        if operation in {
            "alert-plugin.definition.list",
            "alert-plugin.schema",
            "alert-plugin.create",
            "alert-plugin.update",
        }:
            enums = (_PLUGIN_TYPE_ENUM,)
        if contract.plugin.transient_delivery_fields and operation in {
            "alert-plugin.create",
            "alert-plugin.update",
        }:
            enums = (
                *enums,
                _ALERT_PLUGIN_INSTANCE_TYPE_ENUM,
                _WARNING_TYPE_ENUM,
            )
        roots[operation] = enums
    return roots


def action_support(version: str) -> dict[str, Support]:
    """Return terminal support decisions for every stable alert action."""
    contract = alert_contract(version)
    support: dict[str, Support] = dict.fromkeys(
        ALERT_GROUP_SEMANTIC_OPERATIONS,
        "supported",
    )
    support.update(
        dict.fromkeys(ALERT_PLUGIN_SEMANTIC_OPERATIONS, contract.plugin.support)
    )
    if contract.plugin.support == "supported" and not contract.plugin.test_send:
        support["alert-plugin.test"] = "absent"
    return support


def terminal_decisions(version: str) -> dict[str, TerminalDecision]:
    """Return evidence-backed zero-request terminal alert coordinates."""
    contract = alert_contract(version)
    decisions: dict[str, TerminalDecision] = {}
    if contract.plugin.support == "absent":
        evidence = Evidence(
            version=version,
            kind="snapshot",
            source=f"build/ds_contract/snapshots-v2/ds-{version}-contract.json",
            symbol="operations",
            conclusion=(
                "The exact source inventory has no AlertPluginInstanceController "
                "or UiPluginController; plugin instances are introduced in 2.0.0."
            ),
        )
        constraint = (
            "DolphinScheduler 1.3.9 predates alert-plugin definitions and "
            "instances; this action cannot be sent to that server."
        )
        for operation in ALERT_PLUGIN_SEMANTIC_OPERATIONS:
            decisions[operation] = TerminalDecision(
                semantic_operation=operation,
                reason="upstream_capability_absent",
                constraint=constraint,
                evidence=evidence,
            )
    elif not contract.plugin.test_send:
        operation = "alert-plugin.test"
        decisions[operation] = TerminalDecision(
            semantic_operation=operation,
            reason="upstream_capability_absent",
            constraint=(
                "This DolphinScheduler release predates alert-plugin test-send, "
                "introduced in 3.2.1."
            ),
            evidence=Evidence(
                version=version,
                kind="snapshot",
                source=f"build/ds_contract/snapshots-v2/ds-{version}-contract.json",
                symbol="operations",
                conclusion=(
                    "The exact source inventory contains no "
                    "AlertPluginInstanceController.testSendAlertPluginInstance."
                ),
            ),
        )
    return decisions


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return exact controller and retained-UI evidence for executable actions."""
    sources = semantic_operation_sources(version)
    evidence: dict[str, tuple[Evidence, ...]] = {}
    for operation, source_operations in sources.items():
        if operation.startswith("alert-group."):
            ui = (
                _LEGACY_GROUP_UI
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
                else _GROUP_UI
            )
        else:
            ui = (
                _LEGACY_PLUGIN_UI
                if version
                in {
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
                else _PLUGIN_UI
            )
        controller_symbols: dict[str, list[str]] = {}
        for source_operation in source_operations:
            controller_name, _, method_name = source_operation.partition(".")
            controller_symbols.setdefault(controller_name, []).append(method_name)
        controller_paths = {
            "AlertGroupController": _GROUP_CONTROLLER,
            "AlertPluginInstanceController": _PLUGIN_CONTROLLER,
            "UiPluginController": _UI_PLUGIN_CONTROLLER,
        }
        evidence[operation] = (
            *(
                Evidence(
                    version=version,
                    kind="controller",
                    source=controller_paths[controller_name],
                    symbol=";".join(methods),
                    conclusion=(
                        "Exact controller operations used by the stable action."
                    ),
                )
                for controller_name, methods in controller_symbols.items()
            ),
            Evidence(
                version=version,
                kind="ui",
                source=ui,
                symbol="request",
                conclusion=(
                    "The retained UI confirms request defaults, route use, and "
                    "the user-visible alert workflow."
                ),
            ),
        )
    return evidence


def semantic_operation_facets(version: str) -> dict[str, dict[str, object]]:
    """Project reviewed alert behavior for generated profile metadata."""
    contract = alert_contract(version)
    facets: dict[str, dict[str, object]] = {}
    for operation in semantic_operation_sources(version):
        if operation == "alert-group.page":
            facets[operation] = {"result": "page"}
        elif operation == "alert-group.get":
            facets[operation] = {
                "result": "entity",
                "direct_get": contract.group.direct_get,
            }
        elif operation.startswith("alert-group."):
            result_name = operation.rpartition(".")[2]
            facets[operation] = {
                "association": contract.group.association,
                "result": getattr(contract.group, f"{result_name}_result"),
                "verified_readback": result_name != "delete",
            }
        elif operation == "alert-plugin.page":
            facets[operation] = {
                "result": "page",
                "server_search": contract.plugin.searchable_page,
            }
        elif operation in {
            "alert-plugin.definition.list",
            "alert-plugin.schema",
        }:
            facets[operation] = {"plugin_type": "ALERT"}
        elif operation == "alert-plugin.get":
            facets[operation] = {"result": "entity", "source": "instance-list"}
        elif operation == "alert-plugin.test":
            facets[operation] = {"result": "boolean"}
        else:
            result_name = operation.rpartition(".")[2]
            facets[operation] = {
                "result": getattr(contract.plugin, f"{result_name}_result"),
                "transient_delivery_fields": (
                    contract.plugin.transient_delivery_fields
                ),
                "verified_readback": result_name != "delete",
            }
    return facets


__all__ = [
    "ALERT_CONTRACTS",
    "ALERT_GROUP_SEMANTIC_OPERATIONS",
    "ALERT_PLUGIN_SEMANTIC_OPERATIONS",
    "ALERT_SEMANTIC_OPERATIONS",
    "TARGET_ALERT_VERSIONS",
    "AlertGroupRecipe",
    "AlertPluginRecipe",
    "AlertVersionContract",
    "Evidence",
    "TerminalDecision",
    "action_support",
    "alert_contract",
    "semantic_operation_enum_roots",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "terminal_decisions",
]
