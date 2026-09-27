from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    SQL_INLINE_FACET,
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_VERSION = "1.3.9"
_TASK_TYPE = "SQL"
_FINGERPRINT = "sha256:bd6c715a5d026a06791e042fd286df60b4e0c52116438f5b3a3d8f60c507ce19"
_REFS = TaskRefIndex.from_code_by_name({})
_DATASOURCE_TYPES = {
    "MYSQL",
    "POSTGRESQL",
    "HIVE",
    "SPARK",
    "CLICKHOUSE",
    "ORACLE",
    "SQLSERVER",
    "DB2",
}
_SCALAR_TYPES = {
    "VARCHAR",
    "INTEGER",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "BOOLEAN",
}
_CANONICAL_FIELDS = {
    "type",
    "datasource",
    "sql",
    "sqlType",
    "displayRows",
    "connParams",
    "preStatements",
    "postStatements",
    "limit",
    "localParams",
}
_CANONICAL_FIELD_PATHS = {
    "task_params.type",
    "task_params.datasource",
    "task_params.sql",
    "task_params.sqlType",
    "task_params.displayRows",
    "task_params.connParams",
    "task_params.preStatements[]",
    "task_params.postStatements[]",
    "task_params.limit",
    "task_params.localParams[]",
    "task_params.localParams[].prop",
    "task_params.localParams[].direct",
    "task_params.localParams[].type",
    "task_params.localParams[].value",
}
_FIXED_NATIVE_FIELDS: YamlObject = {
    "sendEmail": False,
    "udfs": "",
    "showType": "TABLE",
    "title": "",
    "receivers": "",
    "receiversCc": "",
}


def _canonical(**overrides: YamlValue) -> YamlObject:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 7,
        "sql": "select 1;",
        "sqlType": 0,
        "displayRows": 10,
        "connParams": "",
        "preStatements": [],
        "postStatements": [],
        "limit": 0,
        "localParams": [],
    }
    params.update(overrides)
    return params


def _native(**overrides: YamlValue) -> YamlObject:
    params = {**_canonical(), **_FIXED_NATIVE_FIELDS}
    params.update(overrides)
    return params


def _spec(params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(_VERSION)
    return validate_workflow_document(
        {
            "workflow": {"name": "sql-139-authoring"},
            "tasks": [
                {
                    "name": "query-one",
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled(params: YamlObject) -> YamlObject:
    payload = prepare_legacy_workflow_graph(
        _spec(params),
        task_id_factory=lambda _task_name: "tasks-sql",
    ).materialize()
    process = json.loads(payload["processDefinitionJson"])
    native_params = process["tasks"][0]["params"]
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encode(params: YamlObject) -> YamlObject:
    encoded = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", encoded.task_params)


def _decode(params: YamlObject) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    return (
        cast("YamlObject", decoded.task.task_params),
        decoded.reencode_source,
    )


def _legacy_graph(native_params: YamlObject) -> DecodedLegacyWorkflowGraph:
    return decode_legacy_workflow_graph(
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "tasks-sql",
                        "name": "query-one",
                        "type": _TASK_TYPE,
                        "description": "Original SQL task",
                        "params": native_params,
                        "preTasks": [],
                    }
                ],
                "timeout": 0,
                "tenantId": -1,
            }
        ),
        locations=json.dumps(
            {
                "tasks-sql": {
                    "name": "query-one",
                    "targetarr": "",
                    "nodenumber": 0,
                    "x": 0,
                    "y": 0,
                }
            }
        ),
        connects="[]",
    )


def _exported_params(graph: DecodedLegacyWorkflowGraph) -> YamlObject:
    document = graph.workflow_document(name="sql-139-roundtrip")
    tasks = document["tasks"]
    assert isinstance(tasks, list)
    task = tasks[0]
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _recompiled_params(
    graph: DecodedLegacyWorkflowGraph,
    *,
    spec: WorkflowSpec | None = None,
) -> YamlObject:
    selected = (
        graph.to_workflow_spec(name="sql-139-roundtrip") if spec is None else spec
    )
    payload = prepare_legacy_workflow_graph(selected, baseline=graph).materialize()
    process = json.loads(payload["processDefinitionJson"])
    params = process["tasks"][0]["params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _task_params_json_schema() -> YamlObject:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    return cast("YamlObject", task_params)


def _object_member(value: YamlObject, key: str) -> YamlObject:
    """Narrow one asserted object-valued schema member for static checking."""
    member = value[key]
    assert isinstance(member, dict)
    return member


def _list_member(value: YamlObject, key: str) -> list[YamlValue]:
    """Narrow one asserted list-valued schema member for static checking."""
    member = value[key]
    assert isinstance(member, list)
    return member


def test_sql_139_minimal_workflow_compiles_closed_legacy_wire() -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert catalog.supports_typed_authoring("SQL") is True
    assert catalog.require_facet("SQL", SQL_INLINE_FACET).typed_create is True

    spec = validate_workflow_document(
        {
            "workflow": {"name": "sql-139-authoring"},
            "tasks": [
                {
                    "name": "query-one",
                    "type": "SQL",
                    "task_params": {
                        "type": "MYSQL",
                        "datasource": 7,
                        "sql": "select 1;",
                        "sqlType": 0,
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    payload = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _task_name: "tasks-sql",
    ).materialize()
    process = json.loads(payload["processDefinitionJson"])
    task = cast("YamlObject", process["tasks"][0])

    assert task["type"] == "SQL"
    assert task["params"] == {
        "type": "MYSQL",
        "datasource": 7,
        "sql": "select 1;",
        "sqlType": 0,
        "sendEmail": False,
        "displayRows": 10,
        "udfs": "",
        "showType": "TABLE",
        "connParams": "",
        "preStatements": [],
        "postStatements": [],
        "title": "",
        "receivers": "",
        "receiversCc": "",
        "limit": 0,
        "localParams": [],
    }


def test_sql_139_exact_source_fact_has_reviewed_membership() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review
    membership = catalog.require_facet(_TASK_TYPE, SQL_INLINE_FACET)

    assert _TASK_TYPE in catalog.upstream_task_types
    assert _TASK_TYPE in catalog.reviewed_typed_task_types
    assert len(catalog.reviewed_typed_task_types) == 11
    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.common.task.sql.SqlParameters"
    )
    assert fact.registration_kind == "legacy_switch"
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.review == "legacy-1.3.9-sql-safe-inline-subset"
    assert review.cli_model == "Sql139InlineTaskParamsSpec"
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.profile_version == _VERSION
    assert membership.contract.review == review.review
    assert membership.contract.params_model is not None
    assert membership.contract.params_model.__name__ == review.cli_model
    assert membership.typed_create is membership.typed_edit is True
    assert membership.opaque_create is membership.opaque_edit is True
    assert membership.opaque_preserve is True


def test_sql_139_catalog_schema_exposes_only_the_closed_canonical_surface() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_type_schema_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_paths = {path for path in fields if path.startswith("task_params.")}

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert task_param_paths == _CANONICAL_FIELD_PATHS
    assert set(fields["task_params.type"]["choices"]) == _DATASOURCE_TYPES
    assert fields["task_params.sqlType"]["choices"] == ["0", "1"]
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]
    assert set(fields["task_params.localParams[].type"]["choices"]) == _SCALAR_TYPES
    for compiler_owned in (
        "sendEmail",
        "udfs",
        "showType",
        "title",
        "receivers",
        "receiversCc",
        "groupId",
        "varPool",
    ):
        assert f"task_params.{compiler_owned}" not in fields


def test_sql_139_schema_discloses_every_unconditional_fixed_native_field() -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    rules = result.data["state_rules"]
    assert isinstance(rules, list)
    fixed_rule = next(
        rule
        for rule in rules
        if isinstance(rule, dict)
        and rule.get("when") == "all exact 1.3.9 SQL typed authoring"
    )

    assert fixed_rule["condition_paths"] == []
    assert fixed_rule["compile_policy"] == {
        "task_params.sendEmail": "send compiler-owned false",
        "task_params.udfs": "send compiler-owned empty text",
        "task_params.showType": "send compiler-owned TABLE",
        "task_params.title": "send compiler-owned empty text",
        "task_params.receivers": "send compiler-owned empty text",
        "task_params.receiversCc": "send compiler-owned empty text",
    }


def test_sql_139_json_schema_is_closed_and_matches_the_exact_subset() -> None:
    task_params = _task_params_json_schema()
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _CANONICAL_FIELDS
    assert set(required) == {"type", "datasource", "sql", "sqlType"}
    definitions = _object_member(task_params, "$defs")
    assert (
        set(_list_member(_object_member(definitions, "Sql139DatasourceType"), "enum"))
        == _DATASOURCE_TYPES
    )
    assert (
        set(_list_member(_object_member(definitions, "DataType"), "enum"))
        == _SCALAR_TYPES
    )
    assert _list_member(_object_member(definitions, "Direct"), "enum") == ["IN"]
    assert _list_member(_object_member(properties, "sqlType"), "enum") == [0, 1]
    datasource = _object_member(properties, "datasource")
    datasource_variants = _list_member(datasource, "anyOf")
    datasource_schemas: dict[str, YamlObject] = {}
    for datasource_variant in datasource_variants:
        assert isinstance(datasource_variant, dict)
        datasource_type = datasource_variant["type"]
        assert isinstance(datasource_type, str)
        datasource_schemas[datasource_type] = datasource_variant
    assert set(datasource_schemas) == {"integer", "string"}
    assert datasource_schemas["integer"]["minimum"] == 1
    assert datasource_schemas["string"]["pattern"] == r"\S"
    assert _object_member(properties, "displayRows")["minimum"] == 0
    assert _object_member(properties, "limit")["minimum"] == 0


@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_sql_139_templates_validate_and_compile_the_closed_wire(variant: str) -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_template_result(
        _TASK_TYPE,
        variant=None if variant in {"minimal", "params"} else variant,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    metadata = result.data["template"]
    assert isinstance(metadata, dict)
    document = yaml.safe_load(
        parameter_example_yaml(_TASK_TYPE, _VERSION)
        if variant == "params"
        else cast("str", result.data["yaml"])
    )
    assert isinstance(document, dict)

    assert metadata["kind"] == "typed"
    assert metadata["variants"] == []
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"sql-139-{variant}"},
            "tasks": [document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    payload = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _task_name: "tasks-sql",
    ).materialize()
    process = json.loads(payload["processDefinitionJson"])
    params = process["tasks"][0]["params"]

    assert set(params) == _CANONICAL_FIELDS | set(_FIXED_NATIVE_FIELDS)
    assert params["sendEmail"] is False
    assert params["udfs"] == ""
    assert params["showType"] == "TABLE"
    assert "groupId" not in params
    assert "varPool" not in params
    if variant == "params":
        assert params["localParams"] == [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${system.biz.date}",
            }
        ]


def test_sql_139_canonical_native_canonical_roundtrip_has_typed_provenance() -> None:
    canonical = _canonical(
        preStatements=["set names utf8mb4"],
        postStatements=["select 2"],
        limit=25,
        localParams=[
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "DATE",
                "value": "2026-08-30",
            }
        ],
    )

    native = _encode(canonical)
    decoded, provenance = _decode(native)

    assert native == {**canonical, **_FIXED_NATIVE_FIELDS}
    assert decoded == canonical
    assert provenance is ProjectionSource.TYPED_AUTHORING
    assert _encode(decoded) == native

    graph = _legacy_graph(native)
    assert graph.tasks[0].task_params_reencode_source is (
        ProjectionSource.TYPED_AUTHORING
    )
    assert _exported_params(graph) == canonical
    assert _recompiled_params(graph) == native


def test_sql_139_hive_conn_params_roundtrip_without_rewriting() -> None:
    canonical = _canonical(
        type="HIVE",
        connParams="principal=hive/_HOST@EXAMPLE.COM;user=ds",
    )

    native = _compiled(canonical)
    decoded, provenance = _decode(native)

    assert native["connParams"] == canonical["connParams"]
    assert decoded == canonical
    assert provenance is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    ("conn_params", "message"),
    [
        ("principal=one;principal=two", "unique"),
        ("key=", "blank"),
        ("=value", "blank"),
        ("key=value=extra", "key=value"),
        ("key=value;", "key=value"),
    ],
)
def test_sql_139_hive_rejects_invalid_conn_params(
    conn_params: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _spec(_canonical(type="HIVE", connParams=conn_params))


def test_sql_139_non_hive_rejects_conn_params() -> None:
    with pytest.raises(ValueError, match="only when type is HIVE"):
        _spec(_canonical(connParams="user=ds"))


def test_sql_139_accepts_all_nine_unique_in_scalar_parameters() -> None:
    local_params: list[YamlValue] = [
        {
            "prop": f"p_{data_type.lower()}",
            "direct": "IN",
            "type": data_type,
            "value": "literal",
        }
        for data_type in sorted(_SCALAR_TYPES)
    ]
    canonical = _canonical(localParams=local_params)

    spec = _spec(canonical)
    native = _compiled(canonical)

    assert spec.tasks[0].task_params == canonical
    assert native["localParams"] == local_params


@pytest.mark.parametrize("direct", ["OUT", "FUTURE_DIRECTION"])
def test_sql_139_rejects_non_in_parameters(direct: str) -> None:
    with pytest.raises((UnsupportedFeatureError, ValueError), match=r"unsupported|IN"):
        _spec(
            _canonical(
                localParams=[
                    {
                        "prop": "result",
                        "direct": direct,
                        "type": "VARCHAR",
                        "value": "",
                    }
                ]
            )
        )


@pytest.mark.parametrize("data_type", ["LIST", "FILE", "FUTURE_TYPE"])
def test_sql_139_rejects_non_scalar_parameter_types(data_type: str) -> None:
    with pytest.raises(
        (UnsupportedFeatureError, ValueError),
        match=r"unsupported|scalar|Input should be",
    ):
        _spec(
            _canonical(
                localParams=[
                    {
                        "prop": "value",
                        "direct": "IN",
                        "type": data_type,
                        "value": "literal",
                    }
                ]
            )
        )


def test_sql_139_rejects_duplicate_local_parameter_props() -> None:
    with pytest.raises(ValueError, match="unique"):
        _spec(
            _canonical(
                localParams=[
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "DATE",
                        "value": "2026-08-30",
                    },
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "duplicate",
                    },
                ]
            )
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"type": "H2"}, "Input should be"),
        ({"sqlType": 2}, "0 or 1|Input should be"),
        ({"sqlType": "0"}, "integer"),
        ({"sqlType": True}, "integer"),
        ({"sql": ""}, "blank"),
        ({"sql": " \t\n"}, "blank"),
        ({"preStatements": [""]}, "must not be blank"),
        ({"postStatements": ["  "]}, "must not be blank"),
    ],
    ids=(
        "h2",
        "unknown-sql-type",
        "string-sql-type",
        "boolean-sql-type",
        "empty-sql",
        "whitespace-sql",
        "blank-pre-statement",
        "blank-post-statement",
    ),
)
def test_sql_139_invalid_typed_values_fail_closed(
    overrides: dict[str, YamlValue],
    message: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _spec(_canonical(**overrides))


@pytest.mark.parametrize(
    "compiler_owned",
    [
        "sendEmail",
        "udfs",
        "showType",
        "title",
        "receivers",
        "receiversCc",
        "groupId",
        "varPool",
    ],
)
def test_sql_139_rejects_authored_compiler_owned_or_modern_fields(
    compiler_owned: str,
) -> None:
    value: YamlValue = False if compiler_owned == "sendEmail" else "authored"
    if compiler_owned == "varPool":
        value = []
    with pytest.raises(
        (UnsupportedFeatureError, ValueError),
        match=r"unsupported fields|unsupported",
    ):
        _spec(_canonical(**{compiler_owned: value}))


@pytest.mark.parametrize(
    "native",
    [
        _native(sendEmail=True),
        _native(sendEmail=None),
        _native(udfs="12"),
        _native(showType="ATTACHMENT"),
        _native(title="Daily result"),
        _native(receivers="owner@example.com"),
        _native(groupId=3),
        _native(varPool=[]),
        _native(futureField={"nested": ["preserve", None]}),
        _native(localParams=[{"prop": "result", "direct": "OUT"}]),
    ],
    ids=(
        "email-enabled",
        "email-null-default",
        "udf",
        "attachment-display",
        "mail-title",
        "mail-recipient",
        "modern-group-id",
        "modern-var-pool",
        "future-field",
        "out-parameter",
    ),
)
def test_sql_139_richer_native_state_stays_opaque_and_reencodes_losslessly(
    native: YamlObject,
) -> None:
    decoded, provenance = _decode(native)
    reencoded = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(decoded)),
        refs=_REFS,
        source=provenance,
    )

    assert decoded == native
    assert provenance is ProjectionSource.OPAQUE_PRESERVE
    assert reencoded.task_params == native


def test_sql_139_integer_zero_send_email_stays_strictly_opaque_and_lossless() -> None:
    native = _native(sendEmail=0)

    decoded, provenance = _decode(native)
    reencoded = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(decoded)),
        refs=_REFS,
        source=provenance,
    )

    assert provenance is ProjectionSource.OPAQUE_PRESERVE
    assert type(decoded["sendEmail"]) is int
    assert decoded["sendEmail"] == 0
    assert reencoded.task_params == native
    assert type(reencoded.task_params["sendEmail"]) is int


def test_sql_139_complete_native_package_is_explicit_opaque_create() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    native = _native(receivers="owner@example.com")

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    spec = validate_workflow_document(
        {
            "workflow": {"name": "sql-139-opaque-create"},
            "tasks": [
                {
                    "name": "query-one",
                    "type": _TASK_TYPE,
                    "task_params": native,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    assert _compiled(cast("YamlObject", spec.tasks[0].task_params)) == native


def test_sql_139_changed_complete_native_package_is_explicit_opaque_edit() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    baseline_native = _native(receivers="old@example.com")
    edited_native = _native(receivers="new@example.com")
    graph = _legacy_graph(baseline_native)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "sql-139-opaque-edit"},
            "tasks": [
                {
                    "name": "query-one",
                    "type": _TASK_TYPE,
                    "task_params": edited_native,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    payload = prepare_legacy_workflow_graph(spec, baseline=graph).materialize()
    process = json.loads(payload["processDefinitionJson"])

    assert process["tasks"][0]["params"] == edited_native


def test_sql_139_richer_native_metadata_only_edit_is_lossless() -> None:
    native = _native(
        sendEmail=True,
        title="Daily result",
        receivers="owner@example.com",
        futureField={"nested": ["preserve", {"value": True}]},
    )
    graph = _legacy_graph(native)
    baseline = graph.to_workflow_spec(name="sql-139-roundtrip")
    current = baseline.tasks[0]
    edited = baseline.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"description": "Metadata-only edit"},
                    deep=True,
                )
            ]
        },
        deep=True,
    )

    assert graph.tasks[0].task_params_reencode_source is (
        ProjectionSource.OPAQUE_PRESERVE
    )
    assert _exported_params(graph) == native
    assert _recompiled_params(graph, spec=edited) == native
