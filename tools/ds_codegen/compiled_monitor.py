"""Compile exact monitor exchanges without widening the public node catalog."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.observability_contract import (
    MONITOR_GENERATED_OPERATIONS,
    TARGET_OBSERVABILITY_VERSIONS,
    observability_contract,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from ds_codegen.observability_contract import MonitorRecipe

COMPILED_MONITOR_SCHEMA_VERSION = 1
COMPILED_MONITOR_SEMANTIC_OPERATIONS = frozenset(MONITOR_GENERATED_OPERATIONS)
_RECORD = "org.apache.dolphinscheduler.dao.entity.MonitorRecord"
_METRICS = "org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics"
_HEALTH = f"{_METRICS}.DatabaseHealthStatus"
_SERVER = "org.apache.dolphinscheduler.common.model.Server"
_WORKER = "org.apache.dolphinscheduler.common.model.WorkerServerModel"
_NODE_TYPE = "org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType"
_FLAG = "org.apache.dolphinscheduler.common.enums.Flag"
_DB_COMMON = "org.apache.dolphinscheduler.common.enums.DbType"
_DB_SPI = "org.apache.dolphinscheduler.spi.enums.DbType"
_DB_EXTERNAL = "com.baomidou.mybatisplus.annotation.DbType"
_FIRST_VERSION = frozenset({"1.3.9"})
_FIXED_VERSIONS = frozenset(
    version
    for version in TARGET_OBSERVABILITY_VERSIONS
    if observability_contract(version).monitor.server_wire == "fixed-routes"
)
_NODE_LEGACY_VERSIONS = frozenset({"3.2.2"})
_NODE_CANONICAL_VERSIONS = frozenset(
    {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)
_DB_NAMES = (
    "MYSQL",
    "POSTGRESQL",
    "HIVE",
    "SPARK",
    "CLICKHOUSE",
    "ORACLE",
    "SQLSERVER",
    "DB2",
    "PRESTO",
    "H2",
    "REDSHIFT",
    "ATHENA",
    "TRINO",
    "STARROCKS",
    "AZURESQL",
    "DAMENG",
    "OCEANBASE",
    "SSH",
    "KYUUBI",
    "DATABEND",
    "SNOWFLAKE",
    "VERTICA",
    "HANA",
    "DORIS",
)
_DB_EPOCHS = {
    "1.3.9": ("legacy", _DB_COMMON, (*_DB_NAMES[:8], "H2"), 2),
    "2.0.0": ("common", _DB_COMMON, _DB_NAMES[:10], 1),
    "2.0.1": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.2": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.3": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.4": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.5": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.6": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.7": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.8": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "2.0.9": ("spi_10", _DB_SPI, _DB_NAMES[:10], 2),
    "3.0.0": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.1": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.2": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.3": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.4": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.5": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.0.6": ("spi_11", _DB_SPI, _DB_NAMES[:11], 2),
    "3.1.0": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.1": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.2": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.3": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.4": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.5": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.6": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.7": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.8": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.1.9": ("spi_12", _DB_SPI, _DB_NAMES[:12], 2),
    "3.2.0": ("spi_24", _DB_SPI, _DB_NAMES, 2),
}
_COUNTERS = tuple(
    (name, "long", False, "0", None)
    for name in (
        "maxConnections",
        "maxUsedConnections",
        "threadsConnections",
        "threadsRunningConnections",
    )
)
_DATE = ("date", "Date", True, None, None)
_SERVER_PREFIX = (
    ("id", "int", False, "0", None),
    ("host", "String", True, None, None),
    ("port", "int", False, "0", None),
)
_SERVER_TIMES = (
    ("createTime", "Date", True, None, None),
    ("lastHeartbeatTime", "Date", True, None, None),
)
_CODE_FIELDS = (("code", "int", ("EnumValue",)), ("descp", "String", ()))
_NODE_FIELDS = (("name", "String", ()), ("registryPath", "String", ()))
_LEGACY_NODE_VALUES = (
    ("ALL_SERVERS", ("nodes", "/nodes")),
    ("MASTER", ("Master", "/nodes/master")),
    ("MASTER_NODE_LOCK", ("MasterNodeLock", "/lock/master-node")),
    ("MASTER_FAILOVER_LOCK", ("MasterFailoverLock", "/lock/master-failover")),
    (
        "MASTER_TASK_GROUP_COORDINATOR_LOCK",
        ("TaskGroupCoordinatorLock", "/lock/master-task-group-coordinator"),
    ),
    ("WORKER", ("Worker", "/nodes/worker")),
    ("ALERT_SERVER", ("AlertServer", "/nodes/alert-server")),
    ("ALERT_LOCK", ("AlertNodeLock", "/lock/alert")),
)
_CANONICAL_NODE_VALUES = (
    ("FAILOVER_FINISH_NODES", ("FailoverFinishNodes", "/nodes/failover-finish-nodes")),
    (
        "GLOBAL_MASTER_FAILOVER_LOCK",
        ("GlobalMasterFailoverLock", "/lock/global-master-failover"),
    ),
    ("MASTER", ("Master", "/nodes/master")),
    ("MASTER_FAILOVER_LOCK", ("MasterFailoverLock", "/lock/master-failover")),
    ("MASTER_COORDINATOR", ("MasterCoordinator", "/nodes/master-coordinator")),
    (
        "MASTER_TASK_GROUP_COORDINATOR_LOCK",
        ("TaskGroupCoordinatorLock", "/lock/master-task-group-coordinator"),
    ),
    (
        "MASTER_SERIAL_COORDINATOR_LOCK",
        ("SerialWorkflowCoordinator", "/lock/master-serial-workflow-coordinator"),
    ),
    ("WORKER", ("Worker", "/nodes/worker")),
    ("ALERT_SERVER", ("AlertServer", "/nodes/alert-server")),
    ("ALERT_HA_LEADER", ("AlertHALeader", "/nodes/alert-server-ha-leader")),
)

_PRIMITIVES = (
    *(
        CompiledPrimitive(
            name=name,
            requests=tuple(
                CompiledRequestEpoch(
                    method="GET",
                    path=path,
                    channel="query",
                    request_schema="empty",
                    request_model="MonitorEmptyParams",
                    request_fields=(),
                    required_fields=frozenset(),
                    versions=versions,
                )
                for path, versions in (
                    (legacy_path, _FIRST_VERSION),
                    (modern_path, present_versions - _FIRST_VERSION),
                )
            ),
            result_envelope="optional",
            absent_versions=frozenset(TARGET_OBSERVABILITY_VERSIONS) - present_versions,
        )
        for name, legacy_path, modern_path, present_versions in (
            (
                "database",
                "monitor/database",
                "monitor/databases",
                frozenset(TARGET_OBSERVABILITY_VERSIONS),
            ),
            ("master", "monitor/master/list", "monitor/masters", _FIXED_VERSIONS),
            ("worker", "monitor/worker/list", "monitor/workers", _FIXED_VERSIONS),
        )
    ),
    CompiledPrimitive(
        name="server",
        requests=tuple(
            CompiledRequestEpoch(
                method="GET",
                path="monitor/{nodeType}",
                channel="path",
                request_schema=f"node_type_{epoch}",
                request_model=f"MonitorNodeType{epoch.title()}Params",
                request_fields=("nodeType",),
                path_fields=("nodeType",),
                required_fields=frozenset({"nodeType"}),
                versions=versions,
            )
            for epoch, versions in (
                ("legacy", _NODE_LEGACY_VERSIONS),
                ("canonical", _NODE_CANONICAL_VERSIONS),
            )
        ),
        result_envelope="optional",
        absent_versions=_FIXED_VERSIONS,
    ),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "MonitorController.queryDatabaseState": "database",
        "MonitorController.listMaster": "master",
        "MonitorController.listWorker": "worker",
        "MonitorController.listServer": "server",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    recipe = observability_contract(snapshot.ds_version).monitor
    expected_source = (
        (recipe.database_operation,)
        if primitive == "database"
        else recipe.server_operations
    )
    if operation.operation_id not in expected_source:
        message = "compiled monitor reviewed source recipe changed"
        raise ValueError(message)
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled monitor {primitive} exchange projection changed"
        raise ValueError(message)
    fields = tuple(
        (item.wire_name, item.java_type, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    )
    if fields != ((("nodeType", _NODE_TYPE, None),) if primitive == "server" else ()):
        message = f"compiled monitor {primitive} request type or default changed"
        raise ValueError(message)
    if primitive == "database":
        schema = _database_epoch(snapshot, operation, recipe)
        return CompiledResponsePolicy(codec=schema, schema=schema, capture=[])
    schema = _server_epoch(snapshot, operation, primitive, recipe)
    codec = (
        f"server_{recipe.server_field_wire}"
        if primitive == "server"
        else f"{primitive}_{'initial' if snapshot.ds_version == '1.3.9' else 'fixed'}"
    )
    return CompiledResponsePolicy(codec=codec, schema=schema, capture=[])


def _database_epoch(
    snapshot: ContractSnapshot, operation: OperationSpec, recipe: MonitorRecipe
) -> str:
    if operation.logical_return_type != f"List<{recipe.database_model}>":
        message = "compiled monitor database response changed"
        raise ValueError(message)
    if recipe.database_model == _RECORD and snapshot.ds_version in _DB_EPOCHS:
        epoch, db_type, names, columns = _DB_EPOCHS[snapshot.ds_version]
        values = tuple(
            (
                name,
                (str(_DB_NAMES.index(name)),)
                + ((name.lower(),) if columns == 2 else ()),
            )
            for name in names
        )
        _require_enum(snapshot, db_type, _CODE_FIELDS[:columns], values)
        _require_enum(
            snapshot, _FLAG, _CODE_FIELDS, (("NO", ("0", "no")), ("YES", ("1", "yes")))
        )
        state = _FLAG
    elif recipe.database_model == _METRICS and snapshot.ds_version not in _DB_EPOCHS:
        epoch, db_type, state = "metrics", _DB_EXTERNAL, _HEALTH
        _require_enum(snapshot, _HEALTH, (), (("YES", ()), ("NO", ())))
        # MyBatis owns this external enum; preserve the existing opaque boundary.
        declared = (
            {item.import_path for item in snapshot.enums}
            | {item.import_path for item in snapshot.models}
            | {item.import_path for item in snapshot.dtos}
        )
        if _DB_EXTERNAL in declared:
            message = "compiled monitor external database type became declared"
            raise ValueError(message)
    else:
        message = "compiled monitor reviewed database model changed"
        raise ValueError(message)
    expected = (
        ("dbType", db_type, True, None, None),
        ("state", state, True, None, None),
        *_COUNTERS,
        _DATE,
    )
    model = require_model(snapshot, recipe.database_model, domain="monitor")
    if model.extends is not None or model_field_facts(model) != expected:
        message = "compiled monitor database response fields changed"
        raise ValueError(message)
    return f"database_{epoch}"


def _server_epoch(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    primitive: str,
    recipe: MonitorRecipe,
) -> str:
    if primitive in {"master", "worker"}:
        if (
            recipe.server_wire != "fixed-routes"
            or recipe.server_field_wire != "legacy"
            or recipe.server_models != (_SERVER, _WORKER)
            or recipe.server_enums
            or recipe.node_types != ("MASTER", "WORKER")
        ):
            message = "compiled monitor fixed server recipe changed"
            raise ValueError(message)
        model = _WORKER if primitive == "worker" else _SERVER
        root = (
            "Collection"
            if primitive == "worker" and recipe.database_model == _RECORD
            else "List"
        )
        directory = (
            ("zkDirectories", "Set<String>")
            if primitive == "worker"
            else ("zkDirectory", "String")
        )
        heartbeat = "resInfo"
        schema = "worker" if primitive == "worker" else "server_legacy"
    else:
        if (
            recipe.server_wire != "node-type-route"
            or recipe.server_models != (_SERVER,)
            or recipe.server_enums != (_NODE_TYPE,)
            or recipe.node_types != ("MASTER", "WORKER", "ALERT_SERVER")
        ):
            message = "compiled monitor node-type recipe changed"
            raise ValueError(message)
        values: tuple[tuple[str, tuple[str, ...]], ...]
        if (
            snapshot.ds_version in _NODE_LEGACY_VERSIONS
            and recipe.server_field_wire == "legacy"
        ):
            values, directory, heartbeat = (
                _LEGACY_NODE_VALUES,
                ("zkDirectory", "String"),
                "resInfo",
            )
        elif (
            snapshot.ds_version in _NODE_CANONICAL_VERSIONS
            and recipe.server_field_wire == "canonical"
        ):
            values, directory, heartbeat = (
                _CANONICAL_NODE_VALUES,
                ("serverDirectory", "String"),
                "heartBeatInfo",
            )
        else:
            message = "compiled monitor node-type field epoch changed"
            raise ValueError(message)
        _require_enum(snapshot, _NODE_TYPE, _NODE_FIELDS, values)
        model, root, schema = _SERVER, "List", f"server_{recipe.server_field_wire}"
    if operation.logical_return_type != f"{root}<{model}>":
        message = "compiled monitor server response root changed"
        raise ValueError(message)
    expected = (
        *_SERVER_PREFIX,
        (*directory, True, None, None),
        (heartbeat, "String", True, None, None),
        *_SERVER_TIMES,
    )
    source_model = require_model(snapshot, model, domain="monitor")
    if source_model.extends is not None or model_field_facts(source_model) != expected:
        message = "compiled monitor server response fields changed"
        raise ValueError(message)
    return schema


def _require_enum(
    snapshot: ContractSnapshot,
    import_path: str,
    fields: tuple[tuple[str, str, tuple[str, ...]], ...],
    values: tuple[tuple[str, tuple[str, ...]], ...],
) -> None:
    matches = [item for item in snapshot.enums if item.import_path == import_path]
    if (
        len(matches) != 1
        or matches[0].json_value_field is not None
        or tuple(
            (item.name, item.java_type, tuple(item.annotations))
            for item in matches[0].fields
        )
        != fields
        or tuple((item.name, tuple(item.arguments)) for item in matches[0].values)
        != values
    ):
        message = f"compiled monitor enum changed: {import_path}"
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    if codecs.get("database") not in {
        "database_legacy",
        "database_common",
        "database_spi_10",
        "database_spi_11",
        "database_spi_12",
        "database_spi_24",
        "database_metrics",
    }:
        message = "compiled monitor database recipe is unsupported"
        raise ValueError(message)
    servers = {name: value for name, value in codecs.items() if name != "database"}
    if servers in (
        {"master": "master_initial", "worker": "worker_initial"},
        {"master": "master_fixed", "worker": "worker_fixed"},
    ):
        return "fixed_legacy"
    if codecs.get("database") == "database_metrics":
        if servers == {"server": "server_legacy"}:
            return "node_type_legacy"
        if servers == {"server": "server_canonical"}:
            return "node_type_canonical"
    message = f"compiled monitor recipe is unsupported: {codecs!r}"
    raise ValueError(message)


MONITOR_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="monitor",
    schema_constant="COMPILED_MONITOR_SCHEMA_VERSION",
    schema_version=COMPILED_MONITOR_SCHEMA_VERSION,
    semantic_operations=COMPILED_MONITOR_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = ["COMPILED_MONITOR_SCHEMA_VERSION", "MONITOR_COMPILED_DOMAIN"]
