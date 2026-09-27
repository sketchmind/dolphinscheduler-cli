"""Compile exact datasource exchanges while preserving native payload recipes."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_DATASOURCE_SCHEMA_VERSION = 1
COMPILED_DATASOURCE_SEMANTIC_OPERATIONS = frozenset(
    {
        "datasource.page",
        "datasource.get",
        "datasource.create",
        "datasource.update",
        "datasource.delete",
        "datasource.saved-test",
    }
)
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_ENTITY = "org.apache.dolphinscheduler.dao.entity.DataSource"
_LEGACY_DETAIL = "generated.view.DataSourceService_queryDataSource_map"
_COMMON_DTO = "org.apache.dolphinscheduler.common.datasource.BaseDataSourceParamDTO"
_PLUGIN_DTO = (
    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
    "BaseDataSourceParamDTO"
)
_COMMON_DB = "org.apache.dolphinscheduler.common.enums.DbType"
_SPI_DB = "org.apache.dolphinscheduler.spi.enums.DbType"
_CONNECT = "org.apache.dolphinscheduler.common.enums.DbConnectType"
_LEGACY = frozenset({"1.3.9"})
_MODERN = frozenset(REVIEWED_DS_VERSIONS) - _LEGACY
_ANNOTATED = frozenset(REVIEWED_DS_VERSIONS[REVIEWED_DS_VERSIONS.index("3.2.0") :])
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
    "ZEPPELIN",
    "SAGEMAKER",
    "K8S",
    "ALIYUN_SERVERLESS_SPARK",
    "DOLPHINDB",
)
_FORM_FIELDS = (
    "name",
    "note",
    "type",
    "host",
    "port",
    "database",
    "principal",
    "userName",
    "password",
    "connectType",
    "other",
)
_DTO_FIELDS = (
    ("id", "Integer", True, None, None),
    ("name", "String", True, None, None),
    ("note", "String", True, None, None),
    ("host", "String", True, None, None),
    ("port", "Integer", True, None, None),
    ("database", "String", True, None, None),
    ("userName", "String", True, None, None),
    ("password", "String", True, None, None),
    ("other", "Map<String, String>", True, None, None),
)
_NULLABLE_PAGE = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_STRICT_PAGE_FIELDS = frozenset(
    {"total", "totalPage", "pageSize", "currentPage", "pageNo"}
)


@dataclass(frozen=True)
class _Epoch:
    name: str
    versions: frozenset[str]
    recipe: str
    db_count: int
    db_arguments: int
    strict_page: bool = False


# Full enum closures, not just the datasource fields, distinguish these epochs.
_EPOCHS = (
    _Epoch("legacy", _LEGACY, "legacy_form", 9, 2),
    _Epoch("common", frozenset({"2.0.0"}), "typed_body", 10, 1, True),
    _Epoch(
        "spi_10",
        frozenset(
            {
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
        ),
        "typed_body",
        10,
        2,
        True,
    ),
    _Epoch(
        "spi_11",
        frozenset({"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}),
        "typed_body",
        11,
        2,
        True,
    ),
    _Epoch(
        "spi_12",
        frozenset(
            {
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
            }
        ),
        "json_void",
        12,
        2,
        True,
    ),
    _Epoch("spi_24", frozenset({"3.2.0"}), "json_void", 24, 2),
    _Epoch("spi_26", frozenset({"3.2.1"}), "json_entity", 26, 2),
    _Epoch("spi_26_named", frozenset({"3.2.2"}), "json_entity", 26, 3),
    _Epoch(
        "spi_29",
        frozenset({"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}),
        "json_entity",
        29,
        3,
    ),
)
_BY_VERSION = {version: epoch for epoch in _EPOCHS for version in epoch.versions}
_TEXT_VERSIONS = frozenset(
    version
    for epoch in _EPOCHS
    if epoch.recipe.startswith("json_")
    for version in epoch.versions
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.controller != "DataSourceController":
        return None
    if operation.method_name == "queryDataSource":
        return "get_legacy" if operation.http_method == "POST" else "get"
    if operation.method_name == "delete":
        return "delete_legacy" if operation.http_method == "GET" else "delete"
    return {
        "queryDataSourceListPaging": "page",
        "createDataSource": "create",
        "updateDataSource": "update",
        "deleteDataSource": "delete",
        "connectionTest": "connection_test",
    }.get(operation.method_name)


def _codecs(epoch: _Epoch) -> dict[str, str]:
    codecs = {"page": f"page_{epoch.name}"}
    if epoch.recipe == "legacy_form":
        return {
            **codecs,
            "get_legacy": "get_legacy",
            "create": "create_legacy",
            "update": "update_legacy",
            "delete_legacy": "delete_legacy",
            "connection_test": "connection_test_legacy",
        }
    codecs["get"] = f"get_{epoch.name}"
    mutation = epoch.name if epoch.recipe != "json_void" else "text_void"
    codecs.update(create=f"create_{mutation}", update=f"update_{mutation}")
    boolean = "bool" if epoch.recipe == "json_entity" else "void"
    codecs.update(
        delete=f"delete_{boolean}", connection_test=f"connection_test_{boolean}"
    )
    return codecs


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    epoch = _BY_VERSION[snapshot.ds_version]
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled datasource {primitive} exchange projection changed"
        raise ValueError(message)
    _require_request(snapshot.ds_version, operation, primitive, epoch)
    db_type = _require_db_enum(snapshot, epoch)
    codec = _codecs(epoch)[primitive]
    logical = operation.logical_return_type
    if primitive == "page":
        expected = f"{_PAGE}<{_ENTITY}>"
        if epoch.recipe == "legacy_form":
            expected = "PageInfo<DataSource>"
        _require_page(snapshot, epoch)
        _require_entity(snapshot, epoch, db_type)
        schema = codec
    elif primitive == "get_legacy":
        expected = "DataSourceService_queryDataSource_map"
        _require_legacy_detail(snapshot)
        schema = codec
    elif primitive == "get":
        expected = _COMMON_DTO if epoch.name == "common" else _PLUGIN_DTO
        _require_dto(snapshot, expected, db_type)
        schema = codec
    elif primitive in {"create", "update"} and epoch.recipe == "json_entity":
        expected = _ENTITY
        _require_entity(snapshot, epoch, db_type)
        schema = f"entity_{epoch.name}"
    elif primitive in {"delete", "connection_test"} and epoch.recipe == "json_entity":
        if logical != "Boolean":
            message = f"compiled datasource {primitive} boolean response changed"
            raise ValueError(message)
        return CompiledResponsePolicy(
            codec=codec, schema="boolean", capture=True, scalar_annotation="bool"
        )
    else:
        if logical != "Void":
            message = f"compiled datasource {primitive} void response changed"
            raise ValueError(message)
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if logical != expected:
        message = f"compiled datasource {primitive} response type changed"
        raise ValueError(message)
    return CompiledResponsePolicy(codec=codec, schema=schema, capture={})


def _require_request(
    version: str, operation: OperationSpec, primitive: str, epoch: _Epoch
) -> None:
    required = True if version in _ANNOTATED else None
    expected: dict[str, tuple[str, bool | None]] = {}
    if primitive == "page":
        expected = {
            "searchVal": ("String", False),
            "pageNo": ("Integer", required),
            "pageSize": ("Integer", required),
        }
    elif primitive in {
        "get",
        "get_legacy",
        "delete",
        "delete_legacy",
        "connection_test",
    }:
        expected = {"id": ("int", required)}
    elif epoch.recipe == "legacy_form":
        expected = dict.fromkeys(_FORM_FIELDS, ("String", None))
        expected.update(
            note=("String", False),
            type=(_COMMON_DB, None),
            connectType=(_CONNECT, None),
        )
        if primitive == "update":
            expected["id"] = ("int", None)
    else:
        if primitive == "update":
            expected["id"] = ("Integer", required)
        if epoch.recipe == "typed_body":
            dto_type = _COMMON_DTO if epoch.name == "common" else _PLUGIN_DTO
            expected["dataSourceParam"] = (dto_type, None)
        else:
            body_required = required
            if primitive == "update" and version not in {"3.4.2", "3.4.3"}:
                body_required = None
            expected["jsonStr"] = ("String", body_required)
    actual = {
        item.wire_name: (item.java_type, item.required, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if actual != {name: (*facts, None) for name, facts in expected.items()}:
        message = (
            f"compiled datasource {primitive} request type, "
            "requiredness or default changed"
        )
        raise ValueError(message)


def _require_db_enum(snapshot: ContractSnapshot, epoch: _Epoch) -> str:
    import_path = _COMMON_DB if epoch.name in {"legacy", "common"} else _SPI_DB
    names = _DB_NAMES[: epoch.db_count]
    if epoch.name == "legacy":
        names = (*_DB_NAMES[:8], "H2")
    values = []
    for name in names:
        arguments = [str(_DB_NAMES.index(name))]
        if epoch.db_arguments >= 2:
            arguments.append(name.lower())
        if epoch.db_arguments == 3:
            arguments.append(name.lower().replace("_", " "))
        values.append((name, tuple(arguments)))
    fields: list[tuple[str, str, tuple[str, ...]]] = [("code", "int", ("EnumValue",))]
    if epoch.db_arguments == 3:
        fields.append(("name", "String", ()))
    if epoch.db_arguments >= 2:
        fields.append(("descp", "String", ()))
    enums = [item for item in snapshot.enums if item.import_path == import_path]
    if (
        len(enums) != 1
        or enums[0].json_value_field is not None
        or (
            [
                (item.name, item.java_type, tuple(item.annotations))
                for item in enums[0].fields
            ]
            != fields
            or [(item.name, tuple(item.arguments)) for item in enums[0].values]
            != values
        )
    ):
        message = "compiled datasource DbType closure changed"
        raise ValueError(message)
    return import_path


def _require_dto(snapshot: ContractSnapshot, import_path: str, db_type: str) -> None:
    model = require_model(snapshot, import_path, domain="datasource")
    if model.extends is not None or model_field_facts(model) != (
        *_DTO_FIELDS,
        ("type", db_type, True, None, None),
    ):
        message = "compiled datasource DTO fields changed"
        raise ValueError(message)


def _require_entity(snapshot: ContractSnapshot, epoch: _Epoch, db_type: str) -> None:
    identity: tuple[str, str, bool, str | None, str | None] = (
        "id",
        "Integer",
        True,
        None,
        None,
    )
    if epoch.recipe in {"legacy_form", "typed_body"}:
        identity = ("id", "int", False, "0", None)
    expected = (
        identity,
        ("userId", "int", False, "0", None),
        ("userName", "String", True, None, None),
        ("name", "String", True, None, None),
        ("note", "String", True, None, None),
        ("type", db_type, True, None, None),
        ("connectionParams", "String", True, None, None),
        ("createTime", "Date", True, None, None),
        ("updateTime", "Date", True, None, None),
    )
    model = require_model(snapshot, _ENTITY, domain="datasource")
    if model.extends is not None or model_field_facts(model) != expected:
        message = "compiled datasource entity fields changed"
        raise ValueError(message)


def _require_page(snapshot: ContractSnapshot, epoch: _Epoch) -> None:
    expected: tuple[tuple[str, str, bool, str | None, str | None], ...] = _NULLABLE_PAGE
    if epoch.recipe == "legacy_form":
        expected = (*_NULLABLE_PAGE[:3], _NULLABLE_PAGE[4])
    elif epoch.recipe != "typed_body":
        expected = (("totalList", "List<T>", False, None, "list"), *_NULLABLE_PAGE[1:])
    model = require_model(snapshot, _PAGE, domain="datasource")
    strict = frozenset(
        cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE, ())
    )
    if (
        model.extends is not None
        or model_field_facts(model) != expected
        or strict != (_STRICT_PAGE_FIELDS if epoch.strict_page else frozenset())
    ):
        message = "compiled datasource page fields or integer policy changed"
        raise ValueError(message)


def _require_legacy_detail(snapshot: ContractSnapshot) -> None:
    expected = (
        *((name, "String", True, None, None) for name in ("name", "note", "type")),
        ("connectType", _CONNECT, True, None, None),
        *(
            (name, "String", True, None, None)
            for name in (
                "host",
                "port",
                "principal",
                "database",
                "userName",
                "password",
            )
        ),
        ("other", "Map<String, String>", True, None, None),
    )
    model = require_model(snapshot, _LEGACY_DETAIL, domain="datasource")
    if model.extends is not None or model_field_facts(model) != expected:
        message = "compiled datasource legacy detail fields changed"
        raise ValueError(message)
    enums = [item for item in snapshot.enums if item.import_path == _CONNECT]
    if (
        len(enums) != 1
        or enums[0].json_value_field is not None
        or (
            [
                (item.name, item.java_type, tuple(item.annotations))
                for item in enums[0].fields
            ]
            != [("code", "int", ("EnumValue",)), ("descp", "String", ())]
            or [(item.name, tuple(item.arguments)) for item in enums[0].values]
            != [
                ("ORACLE_SERVICE_NAME", ("0", "Oracle Service Name")),
                ("ORACLE_SID", ("1", "Oracle SID")),
            ]
        )
    ):
        message = "compiled datasource DbConnectType closure changed"
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    for epoch in _EPOCHS:
        if codecs == _codecs(epoch):
            return epoch.recipe
    message = f"compiled datasource recipe is unsupported: {codecs!r}"
    raise ValueError(message)


_ID_REQUEST = CompiledRequestEpoch(
    method="GET",
    path="datasources/{id}",
    channel="path",
    request_schema="id",
    request_model="DataSourceIdParams",
    request_fields=("id",),
    path_fields=("id",),
    required_fields=frozenset({"id"}),
    versions=_MODERN,
)


def _mutation_requests(
    action: Literal["create", "update"],
) -> tuple[CompiledRequestEpoch, ...]:
    updating = action == "update"
    title = "Update" if updating else "Create"
    method: Literal["POST", "PUT"] = "PUT" if updating else "POST"
    path = "datasources/{id}" if updating else "datasources"
    ids = ("id",) if updating else ()
    return (
        CompiledRequestEpoch(
            method="POST",
            path=f"datasources/{action}",
            channel="form",
            request_schema=f"{action}_legacy",
            request_model=f"DataSource{title}LegacyParams",
            request_fields=(*ids, *_FORM_FIELDS),
            required_fields=frozenset((*ids, *_FORM_FIELDS)) - {"note"},
            versions=_LEGACY,
        ),
        *(
            CompiledRequestEpoch(
                method=method,
                path=path,
                channel="path_json" if updating else "json",
                request_schema=f"{action}_{epoch.name}",
                request_model=f"DataSource{title}BodyParams",
                request_fields=(*ids, "dataSourceParam"),
                path_fields=ids,
                required_fields=frozenset((*ids, "dataSourceParam")),
                versions=epoch.versions,
            )
            for epoch in _EPOCHS
            if epoch.recipe == "typed_body"
        ),
        CompiledRequestEpoch(
            method=method,
            path=path,
            channel="path_json_text" if updating else "json_text",
            request_schema=f"{action}_text",
            request_model=f"DataSource{title}TextParams",
            request_fields=(*ids, "jsonStr"),
            path_fields=ids,
            required_fields=frozenset((*ids, "jsonStr")),
            versions=_TEXT_VERSIONS,
        ),
    )


DATASOURCE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="datasource",
    schema_constant="COMPILED_DATASOURCE_SCHEMA_VERSION",
    schema_version=COMPILED_DATASOURCE_SCHEMA_VERSION,
    semantic_operations=COMPILED_DATASOURCE_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=(
        CompiledPrimitive(
            name="page",
            requests=tuple(
                CompiledRequestEpoch(
                    method="GET",
                    path=path,
                    channel="query",
                    request_schema="page",
                    request_model="DataSourcePageParams",
                    request_fields=("searchVal", "pageNo", "pageSize"),
                    required_fields=frozenset({"pageNo", "pageSize"}),
                    versions=versions,
                )
                for path, versions in (
                    ("datasources/list-paging", _LEGACY),
                    ("datasources", _MODERN),
                )
            ),
            result_envelope="optional",
        ),
        CompiledPrimitive(
            name="get_legacy",
            requests=(
                replace(
                    _ID_REQUEST,
                    method="POST",
                    path="datasources/update-ui",
                    channel="form",
                    path_fields=(),
                    versions=_LEGACY,
                ),
            ),
            result_envelope="required",
            absent_versions=_MODERN,
        ),
        CompiledPrimitive(
            name="get",
            requests=(_ID_REQUEST,),
            result_envelope="optional",
            absent_versions=_LEGACY,
        ),
        CompiledPrimitive(
            name="delete_legacy",
            requests=(
                replace(
                    _ID_REQUEST,
                    path="datasources/delete",
                    channel="query",
                    path_fields=(),
                    versions=_LEGACY,
                ),
            ),
            result_envelope="optional",
            absent_versions=_MODERN,
        ),
        CompiledPrimitive(
            name="delete",
            requests=(replace(_ID_REQUEST, method="DELETE"),),
            result_envelope="required",
            absent_versions=_LEGACY,
        ),
        CompiledPrimitive(
            name="create",
            requests=_mutation_requests("create"),
            result_envelope="required",
        ),
        CompiledPrimitive(
            name="update",
            requests=_mutation_requests("update"),
            result_envelope="required",
        ),
        CompiledPrimitive(
            name="connection_test",
            requests=(
                replace(
                    _ID_REQUEST,
                    path="datasources/connect-by-id",
                    channel="query",
                    path_fields=(),
                    versions=_LEGACY,
                ),
                replace(_ID_REQUEST, path="datasources/{id}/connect-test"),
            ),
            result_envelope="optional",
        ),
    ),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_DATASOURCE_SCHEMA_VERSION",
    "COMPILED_DATASOURCE_SEMANTIC_OPERATIONS",
    "DATASOURCE_COMPILED_DOMAIN",
]
