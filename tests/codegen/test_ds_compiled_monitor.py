from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import (
    load_schema_pool,
    replace_operation,
    response_adapter,
)

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_monitor import MONITOR_COMPILED_DOMAIN
from ds_codegen.observability_contract import observability_contract
from ds_codegen.runtime_bundles import (
    _COMPILED_DOMAINS,
    _COMPILED_OPERATION_DEPENDENCIES,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet, CompiledRequest
    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from ds_codegen.observability_contract import ObservabilityVersionContract
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract
_DOMAINS = (MONITOR_COMPILED_DOMAIN,)
_RECORD = "org.apache.dolphinscheduler.dao.entity.MonitorRecord"
_METRICS = "org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics"
_SERVER = "org.apache.dolphinscheduler.common.model.Server"
_WORKER = "org.apache.dolphinscheduler.common.model.WorkerServerModel"
_NODE_TYPE = "org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType"
_DATABASE_SCHEMAS = (
    "database_legacy",
    "database_common",
    "database_spi_10",
    "database_spi_11",
    "database_spi_12",
    "database_spi_24",
    "database_metrics",
)


@pytest.fixture(scope="module")
def compiled_monitor(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(exact_runtime_bundles, _DOMAINS)


def test_monitor_preserves_exact_ownership_absence_and_recipes(
    compiled_monitor: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_monitor.plan("monitor")
    assert tuple(item.recipe_id for item in plan.profiles) == (
        *("fixed_legacy",) * 30,
        "node_type_legacy",
        *("node_type_canonical",) * 6,
    )
    assert (len(plan.requests), len(plan.responses), len(plan.codecs)) == (3, 10, 13)
    assert sum(len(item.programs) for item in plan.profiles) == 104
    assert {item.schema for item in plan.responses} == {
        *_DATABASE_SCHEMAS,
        "worker",
        "server_legacy",
        "server_canonical",
    }
    expected_databases = (
        "database_legacy",
        "database_common",
        *("database_spi_10",) * 9,
        *("database_spi_11",) * 7,
        *("database_spi_12",) * 10,
        "database_spi_24",
        *("database_metrics",) * 8,
    )
    assert (
        tuple(dict(item.programs)["database"].codec for item in plan.profiles)
        == expected_databases
    )
    for index, (original, legacy, profile) in enumerate(
        zip(
            exact_runtime_bundles,
            compiled_monitor.legacy_bundles,
            plan.profiles,
            strict=True,
        )
    ):
        expected = {"MonitorController.queryDatabaseState"}
        expected.update(
            {"MonitorController.listMaster", "MonitorController.listWorker"}
            if index < 30
            else {"MonitorController.listServer"}
        )
        assert {item.source_operation for _, item in profile.programs} == expected
        assert profile.status == "supported"
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        assert all(item.result_envelope == "optional" for _, item in profile.programs)
        assert {item.operation_id for item in original.snapshot.operations} - {
            item.operation_id for item in legacy.snapshot.operations
        } == expected
        assert not any(
            item.controller == "MonitorController"
            for item in legacy.snapshot.operations
        )
        assert not {_RECORD, _METRICS, _SERVER, _WORKER} & {
            item.import_path for item in legacy.snapshot.models
        }
        # The shared slice retains its complete enum metadata catalog.
        assert legacy.snapshot.enums == original.snapshot.enums
    assert MONITOR_COMPILED_DOMAIN.semantic_operations == {
        "monitor.database",
        "monitor.server",
    }


def test_monitor_does_not_change_previous_thirteen_compiled_plans(
    compiled_monitor: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    compiled_all_domains: CompiledDomainSet,
) -> None:
    previous_names = {
        "cluster",
        "environment",
        "worker_group",
        "alert_group",
        "tenant",
        "queue",
        "alert_plugin",
        "access_token",
        "namespace",
        "task_group",
        "user",
        "audit",
        "task_type",
    }
    previous = tuple(item for item in _COMPILED_DOMAINS if item.name in previous_names)
    assert {item.name for item in previous} == previous_names
    baseline = compile_domains(
        exact_runtime_bundles,
        previous,
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )
    combined = compiled_all_domains
    for plan in baseline.plans:
        assert combined.plan(plan.definition.name) == plan
    assert combined.plan("monitor") == compiled_monitor.plan("monitor")


@pytest.mark.parametrize(
    ("schema", "members", "other_epoch_member"),
    [
        (
            "node_type_legacy",
            (
                "ALL_SERVERS",
                "MASTER",
                "MASTER_NODE_LOCK",
                "MASTER_FAILOVER_LOCK",
                "MASTER_TASK_GROUP_COORDINATOR_LOCK",
                "WORKER",
                "ALERT_SERVER",
                "ALERT_LOCK",
            ),
            "ALERT_HA_LEADER",
        ),
        (
            "node_type_canonical",
            (
                "FAILOVER_FINISH_NODES",
                "GLOBAL_MASTER_FAILOVER_LOCK",
                "MASTER",
                "MASTER_FAILOVER_LOCK",
                "MASTER_COORDINATOR",
                "MASTER_TASK_GROUP_COORDINATOR_LOCK",
                "MASTER_SERIAL_COORDINATOR_LOCK",
                "WORKER",
                "ALERT_SERVER",
                "ALERT_HA_LEADER",
            ),
            "ALL_SERVERS",
        ),
    ],
)
def test_monitor_request_keeps_full_native_enum_and_required_path(
    compiled_monitor: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    members: tuple[str, ...],
    other_epoch_member: str,
) -> None:
    request = next(
        item
        for item in compiled_monitor.plan("monitor").requests
        if item.schema == schema
    )
    model = _request_model(request, monkeypatch)
    for member in members:
        assert model.model_validate({"nodeType": member}).model_dump(mode="json") == {
            "nodeType": member
        }
    for payload in (
        {},
        {"nodeType": None},
        {"nodeType": 1},
        {"nodeType": "../MASTER"},
        {"nodeType": other_epoch_member},
        {"nodeType": "MASTER", "extra": 1},
    ):
        with pytest.raises(ValidationError):
            model.model_validate(payload)


@pytest.mark.parametrize("schema", _DATABASE_SCHEMAS)
def test_monitor_database_schemas_keep_defaults_and_external_opaque_boundary(
    compiled_monitor: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    adapter = response_adapter(compiled_monitor.plan("monitor"), schema, monkeypatch)
    assert adapter.dump_python(adapter.validate_python([{}]), mode="json") == [
        {
            "dbType": None,
            "state": None,
            "maxConnections": 0,
            "maxUsedConnections": 0,
            "threadsConnections": 0,
            "threadsRunningConnections": 0,
            "date": None,
        }
    ]
    row = {
        "dbType": "MYSQL",
        "state": "YES",
        "maxConnections": 12,
        "maxUsedConnections": 8,
        "threadsConnections": 2,
        "threadsRunningConnections": 1,
        "date": "2026-09-05 12:00:00",
    }
    assert adapter.dump_python(adapter.validate_python([row]), mode="json") == [row]
    with pytest.raises(ValidationError):
        adapter.validate_python([{**row, "state": "UNKNOWN"}])
    if schema == "database_metrics":
        opaque = {**row, "dbType": {"future": [1, None]}}
        assert adapter.dump_python(adapter.validate_python([opaque]), mode="json") == [
            opaque
        ]
    else:
        with pytest.raises(ValidationError):
            adapter.validate_python([{**row, "dbType": {"future": []}}])
        with pytest.raises(ValidationError):
            adapter.validate_python([{**row, "dbType": "FUTURE_DB"}])


@pytest.mark.parametrize(
    ("schema", "directory", "value", "heartbeat"),
    [
        ("worker", "zkDirectories", ["/b", "/a"], "resInfo"),
        ("server_legacy", "zkDirectory", "/a", "resInfo"),
        ("server_canonical", "serverDirectory", "/a", "heartBeatInfo"),
    ],
)
def test_monitor_server_schemas_preserve_all_fields_and_collection_shape(
    compiled_monitor: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    directory: str,
    value: str | list[str],
    heartbeat: str,
) -> None:
    adapter = response_adapter(compiled_monitor.plan("monitor"), schema, monkeypatch)
    row = {
        "id": 1,
        "host": "host",
        "port": 1234,
        directory: value,
        heartbeat: "{}",
        "createTime": "created",
        "lastHeartbeatTime": "heartbeat",
    }
    assert adapter.dump_python(adapter.validate_python([row]), mode="json") == [row]
    empty = {
        "id": 0,
        "host": None,
        "port": 0,
        directory: None,
        heartbeat: None,
        "createTime": None,
        "lastHeartbeatTime": None,
    }
    assert adapter.dump_python(adapter.validate_python([{}]), mode="json") == [empty]
    with pytest.raises(ValidationError):
        adapter.validate_python(
            [{**row, directory: "wrong" if schema == "worker" else ["wrong"]}]
        )


@pytest.mark.parametrize(
    "drift", ["route", "method", "projection", "root", "type", "default", "required"]
)
def test_monitor_rejects_native_exchange_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    operation = _operation(
        exact_runtime_bundles, "3.2.2", "MonitorController.listServer"
    )
    if drift == "route":
        changed = replace(operation, path="monitor/{different}")
    elif drift == "method":
        changed = replace(operation, http_method="DELETE")
    elif drift == "projection":
        changed = replace(operation, response_projection="single_data")
    elif drift == "root":
        changed = replace(operation, logical_return_type=f"Collection<{_SERVER}>")
    else:
        parameter = next(
            item for item in operation.parameters if item.wire_name == "nodeType"
        )
        if drift == "type":
            field = replace(parameter, java_type="String")
        elif drift == "default":
            field = replace(parameter, default_value="MASTER")
        else:
            field = replace(parameter, required=False)
        changed = replace(
            operation,
            parameters=[
                field if item == parameter else item for item in operation.parameters
            ],
        )
    with pytest.raises(ValueError):
        compile_domains(
            replace_operation(exact_runtime_bundles, "3.2.2", changed), _DOMAINS
        )


@pytest.mark.parametrize("drift", ["known_route", "worker_root", "missing_source"])
def test_monitor_rejects_epoch_substitution_and_missing_owned_source(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], drift: str
) -> None:
    if drift == "known_route":
        operation = _operation(
            exact_runtime_bundles, "1.3.9", "MonitorController.queryDatabaseState"
        )
        bundles = replace_operation(
            exact_runtime_bundles, "1.3.9", replace(operation, path="monitor/databases")
        )
    elif drift == "worker_root":
        operation = _operation(
            exact_runtime_bundles, "3.2.1", "MonitorController.listWorker"
        )
        bundles = replace_operation(
            exact_runtime_bundles,
            "3.2.1",
            replace(operation, logical_return_type=f"Collection<{_WORKER}>"),
        )
    else:
        snapshot = _snapshot(exact_runtime_bundles, "3.2.2")
        bundles = _replace_snapshot(
            exact_runtime_bundles,
            replace(
                snapshot,
                operations=[
                    item
                    for item in snapshot.operations
                    if item.operation_id != "MonitorController.listServer"
                ],
            ),
        )
    with pytest.raises(ValueError):
        compile_domains(bundles, _DOMAINS)


@pytest.mark.parametrize("path", [_METRICS, _SERVER])
def test_monitor_rejects_new_response_inheritance(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], path: str
) -> None:
    snapshot = _snapshot(exact_runtime_bundles, "3.2.1")
    model = next(item for item in snapshot.models if item.import_path == path)
    snapshot = replace(
        snapshot,
        models=[
            replace(item, extends=_WORKER) if item == model else item
            for item in snapshot.models
        ],
    )
    with pytest.raises(ValueError, match="response fields"):
        compile_domains(_replace_snapshot(exact_runtime_bundles, snapshot), _DOMAINS)


@pytest.mark.parametrize(
    ("version", "path", "field", "change"),
    [
        ("1.3.9", _RECORD, "maxConnections", "default"),
        ("3.2.1", _METRICS, "dbType", "type"),
        ("3.2.2", _METRICS, "state", "nullable"),
        ("3.2.1", _WORKER, "zkDirectories", "factory"),
        ("3.2.2", _SERVER, "resInfo", "wire"),
        ("3.3.1", _SERVER, "heartBeatInfo", "wire"),
    ],
)
def test_monitor_rejects_complete_response_field_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    path: str,
    field: str,
    change: str,
) -> None:
    snapshot = _snapshot(exact_runtime_bundles, version)
    model = next(item for item in snapshot.models if item.import_path == path)
    original = next(item for item in model.fields if item.wire_name == field)
    if change == "default":
        changed_field = replace(original, default_value="1")
    elif change == "type":
        changed_field = replace(original, java_type="String")
    elif change == "nullable":
        changed_field = replace(original, nullable=False)
    elif change == "factory":
        changed_field = replace(original, default_factory="list")
    else:
        changed_field = replace(original, wire_name="changed")
    changed = replace(
        model,
        fields=[changed_field if item == original else item for item in model.fields],
    )
    snapshot = replace(
        snapshot,
        models=[changed if item == model else item for item in snapshot.models],
    )
    with pytest.raises(ValueError, match="response fields"):
        compile_domains(_replace_snapshot(exact_runtime_bundles, snapshot), _DOMAINS)


@pytest.mark.parametrize(
    ("version", "path", "change"),
    [
        ("1.3.9", "org.apache.dolphinscheduler.common.enums.DbType", "member"),
        ("2.0.0", "org.apache.dolphinscheduler.common.enums.Flag", "arguments"),
        ("3.2.1", f"{_METRICS}.DatabaseHealthStatus", "member"),
        ("3.2.2", _NODE_TYPE, "arguments"),
        ("3.2.2", _NODE_TYPE, "metadata"),
        ("3.3.1", _NODE_TYPE, "json_value"),
    ],
)
def test_monitor_rejects_response_and_request_enum_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    path: str,
    change: str,
) -> None:
    snapshot = _snapshot(exact_runtime_bundles, version)
    enum = next(item for item in snapshot.enums if item.import_path == path)
    if change == "json_value":
        changed = replace(enum, json_value_field="name")
    elif change == "metadata":
        changed = replace(
            enum, fields=[replace(enum.fields[0], java_type="int"), *enum.fields[1:]]
        )
    else:
        value = (
            replace(enum.values[0], name="CHANGED")
            if change == "member"
            else replace(
                enum.values[0], arguments=["CHANGED", *enum.values[0].arguments[1:]]
            )
        )
        changed = replace(enum, values=[value, *enum.values[1:]])
    snapshot = replace(
        snapshot, enums=[changed if item == enum else item for item in snapshot.enums]
    )
    with pytest.raises(ValueError):
        compile_domains(_replace_snapshot(exact_runtime_bundles, snapshot), _DOMAINS)


@pytest.mark.parametrize("selector", ["empty", "unknown", "absent", "overlap", "wrong"])
def test_monitor_rejects_invalid_or_misassigned_request_epoch_versions(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    selector: str,
) -> None:
    server = MONITOR_COMPILED_DOMAIN.primitives[-1]
    legacy, canonical = server.requests
    versions = {
        "empty": frozenset(),
        "unknown": frozenset({"9.9.9"}),
        "absent": frozenset({"3.2.1"}),
        "overlap": frozenset({"3.2.2", "3.3.1"}),
        "wrong": frozenset({"3.3.1"}),
    }[selector]
    requests = (replace(legacy, versions=versions), canonical)
    definition = replace(
        MONITOR_COMPILED_DOMAIN,
        primitives=(
            *MONITOR_COMPILED_DOMAIN.primitives[:-1],
            replace(server, requests=requests),
        ),
    )
    with pytest.raises(ValueError):
        compile_domains(exact_runtime_bundles, (definition,))


def test_monitor_rejects_reviewed_node_catalog_disagreement(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def changed(version: str) -> ObservabilityVersionContract:
        contract = observability_contract(version)
        return (
            replace(
                contract,
                monitor=replace(contract.monitor, node_types=("MASTER", "WORKER")),
            )
            if version == "3.2.2"
            else contract
        )

    monkeypatch.setattr("ds_codegen.compiled_monitor.observability_contract", changed)
    with pytest.raises(ValueError, match="node-type recipe"):
        compile_domains(exact_runtime_bundles, _DOMAINS)


def _request_model(
    request: CompiledRequest, monkeypatch: pytest.MonkeyPatch
) -> type[BaseParamsModel]:
    name = f"dsctl.generated.wire_programs._test_monitor_request_{request.schema}"
    load_schema_pool(request.pool_modules, monkeypatch)
    module = ModuleType(name)
    module.__package__ = "dsctl.generated.wire_programs"
    if request.module_name is not None:
        module.__package__ += f".{request.module_name.rpartition('.')[0]}"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, name, module)
    exec(  # noqa: S102 - compile only in-memory renderer output under test
        compile(request.content or request.source, f"<{name}>", "exec"),
        module.__dict__,
    )
    return cast("type[BaseParamsModel]", getattr(module, request.class_name))


def _operation(
    bundles: tuple[RuntimeBundle, ...], version: str, source: str
) -> OperationSpec:
    return next(
        item
        for item in _snapshot(bundles, version).operations
        if item.operation_id == source
    )


def _snapshot(bundles: tuple[RuntimeBundle, ...], version: str) -> ContractSnapshot:
    return next(item.snapshot for item in bundles if item.spec.version == version)


def _replace_snapshot(
    bundles: tuple[RuntimeBundle, ...], snapshot: ContractSnapshot
) -> tuple[RuntimeBundle, ...]:
    return tuple(
        replace(item, snapshot=snapshot)
        if item.spec.version == snapshot.ds_version
        else item
        for item in bundles
    )
