from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowTaskRelation,
)

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskGraphContext,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_TASK_TYPE = "BLOCKING"
_FACET = "BLOCKING/same_workflow_state_gate"
_TYPED_VERSIONS = (
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_REFS = TaskRefIndex.from_code_by_name({"upstream": 7_001})
_GRAPH_CONTEXT = TaskGraphContext(
    task_code=7_002,
    relation_edges=frozenset({(7_001, 7_002)}),
)


def _canonical_params(
    *,
    alert: bool = False,
    opportunity: str = "BlockingOnSuccess",
    outer_relation: str = "AND",
    group_relation: str = "OR",
    status: str = "SUCCESS",
) -> YamlObject:
    return {
        "blockingOpportunity": opportunity,
        "alertWhenBlocking": alert,
        "dependence": {
            "relation": outer_relation,
            "dependTaskList": [
                {
                    "relation": group_relation,
                    "dependItemList": [
                        {"task": "upstream", "status": status},
                    ],
                }
            ],
        },
    }


def _native_params(
    *,
    alert: bool = False,
    opportunity: str = "BlockingOnSuccess",
    outer_relation: str = "AND",
    group_relation: str = "OR",
    status: str = "SUCCESS",
) -> YamlObject:
    return {
        "blockingOpportunity": opportunity,
        "alertWhenBlocking": alert,
        "dependence": {
            "relation": outer_relation,
            "dependTaskList": [
                {
                    "relation": group_relation,
                    "dependItemList": [
                        {"depTaskCode": 7_001, "status": status},
                    ],
                }
            ],
        },
    }


def _invalid_canonical_params(case: str) -> YamlObject:
    params = _canonical_params()
    dependence = cast("YamlObject", params["dependence"])
    groups = cast("list[YamlValue]", dependence["dependTaskList"])
    group = cast("YamlObject", groups[0])
    predicates = cast("list[YamlValue]", group["dependItemList"])
    predicate = cast("YamlObject", predicates[0])
    mutators: dict[str, Callable[[], None]] = {
        "opportunity": lambda: params.__setitem__("blockingOpportunity", "SUCCESS"),
        "outer-relation": lambda: dependence.__setitem__("relation", "XOR"),
        "group-relation": lambda: group.__setitem__("relation", "XOR"),
        "status": lambda: predicate.__setitem__("status", "KILL"),
        "empty-groups": lambda: dependence.__setitem__("dependTaskList", []),
        "empty-items": lambda: group.__setitem__("dependItemList", []),
        "outer-extra": lambda: params.__setitem__("localParams", []),
        "dependence-extra": lambda: dependence.__setitem__("futureField", True),
        "group-extra": lambda: group.__setitem__("futureField", True),
        "predicate-extra": lambda: predicate.__setitem__("cycle", "day"),
    }
    try:
        mutate = mutators[case]
    except KeyError as exc:  # pragma: no cover - test helper misuse
        raise AssertionError(case) from exc
    mutate()
    return params


def _fake_dag(
    native: YamlObject,
    *,
    relations: tuple[tuple[int, int], ...] = ((7_001, 7_002),),
) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="blocking-export",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=7_001,
                name="upstream",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value="SHELL",
                task_params_value=json.dumps(
                    {"rawScript": "echo ready", "localParams": [], "resourceList": []}
                ),
                worker_group_value="default",
                timeout=3600,
                is_cache_value=FakeEnumValue("NO"),
            ),
            FakeTaskDefinition(
                code=7_002,
                name="gate",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value=_TASK_TYPE,
                task_params_value=json.dumps(native),
                worker_group_value="default",
                timeout=3600,
                is_cache_value=FakeEnumValue("NO"),
            ),
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(pre_task_code, post_task_code)
            for pre_task_code, post_task_code in relations
        ],
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_blocking_catalog_exposes_one_same_workflow_state_gate(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    surface = get_task_authoring_surface(version).blocking
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert profile.category == "Logic"
    assert profile.kind == "typed"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert membership.contract.params_model is not None
    assert membership.contract.params_model.__name__ == "BlockingTaskParamsSpec"
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert surface.available is True
    assert surface.standby_transition == (
        "KILL" if version in {"3.0.0", "3.0.6"} else "PAUSE"
    )
    assert surface.pause_kill_behavior == (
        "task-local-state"
        if version in {"3.0.0", "3.0.6", "3.1.0", "3.1.9"}
        else "warn-no-op"
    )


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_blocking_typed_authoring_fails_closed_when_upstream_is_absent(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    surface = get_task_authoring_surface(version).blocking

    assert _TASK_TYPE not in catalog.upstream_task_types
    assert surface.available is False
    assert surface.standby_transition is None
    assert surface.pause_kill_behavior is None
    with pytest.raises(UnsupportedFeatureError) as captured:
        catalog.normalize_task_params(
            _TASK_TYPE,
            _canonical_params(),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    assert str(captured.value) == (
        f"BLOCKING typed authoring is unsupported for DolphinScheduler {version}."
    )
    assert captured.value.details["selected_version"] == version
    assert captured.value.details["task_type"] == _TASK_TYPE


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_blocking_projector_fails_closed_when_upstream_is_absent(
    version: str,
) -> None:
    with pytest.raises(
        TaskParameterProjectionError,
        match="does not exist",
    ) as captured:
        encode_task_parameters(
            version=version,
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", _canonical_params()),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "encode",
        "task_type": _TASK_TYPE,
        "field": "task.type",
        "reason": "task-type-absent-in-version",
    }


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_blocking_has_no_public_raw_opaque_authoring_selector(
    intent: TaskAuthoringIntent,
) -> None:
    version = "3.2.2"
    native = _native_params()
    native["futureTaskField"] = {"native": True}

    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog(version).normalize_task_params(
            _TASK_TYPE,
            native,
            intent=intent,
        )

    assert str(captured.value) == (
        f"BLOCKING opaque authoring is unsupported for DolphinScheduler {version}."
    )
    assert captured.value.details == {
        "selected_version": version,
        "task_type": _TASK_TYPE,
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {version} policy permits only opaque "
            "preservation for BLOCKING."
        ),
    }


def test_blocking_schema_is_closed_and_declares_nonempty_dependency_groups() -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog("3.0.0"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["blockingOpportunity", "dependence"]
    assert set(task_params["properties"]) == {
        "blockingOpportunity",
        "alertWhenBlocking",
        "dependence",
    }
    assert task_params["properties"]["blockingOpportunity"]["enum"] == [
        "BlockingOnSuccess",
        "BlockingOnFailed",
    ]
    assert task_params["properties"]["alertWhenBlocking"] == {
        "default": False,
        "description": (
            "Request a blocking alert record for the workflow warningGroupId after "
            "the gate matches; delivery requires separately configured alert "
            "infrastructure."
        ),
        "title": "Alertwhenblocking",
        "type": "boolean",
    }

    definitions = task_params["$defs"]
    dependency = next(
        definition
        for definition in definitions.values()
        if "dependTaskList" in definition.get("properties", {})
    )
    group = next(
        definition
        for definition in definitions.values()
        if "dependItemList" in definition.get("properties", {})
    )
    assert dependency["properties"]["dependTaskList"]["minItems"] == 1
    assert group["properties"]["dependItemList"]["minItems"] == 1


def test_blocking_schema_uses_blocking_domain_language() -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog("3.2.2"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert "CONDITIONS" not in json.dumps(task_params)


@pytest.mark.parametrize(
    ("version", "standby_state", "cancel_behavior"),
    [
        ("3.0.0", "kill", "changes only the local task state"),
        ("3.2.2", "pause", "warns and performs no task-specific action"),
    ],
)
def test_blocking_schema_and_template_disclose_exact_runtime_boundaries(
    version: str,
    standby_state: str,
    cancel_behavior: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    schema = task_type_schema_result(_TASK_TYPE, catalog=catalog)
    template = task_template_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(schema.data, dict)
    assert isinstance(template.data, dict)
    guidance = " ".join(
        [
            *(
                str(field.get("description", ""))
                for field in schema.data["fields"]
                if isinstance(field, dict)
            ),
            cast("str", template.data["yaml"]),
        ]
    ).lower()

    for phrase in (
        "rest-only",
        "no blocking authoring form",
        "task itself completes success",
        "does not trigger task retry",
        "ready_block",
        "active and retry",
        standby_state,
        cancel_behavior,
        "warninggroupid",
        "does not validate",
        "info",
        "not secret storage",
        "master-local",
        "no worker",
        "structured output",
        "durable application id",
        "dedicated failover resume",
        "re-evaluate",
        "repeat the alert",
    ):
        assert phrase in guidance


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_blocking_minimal_template_validates_and_compiles_exact_wire(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(cast("str", result.data["yaml"]))
    assert isinstance(task, dict)
    assert task["task_params"] == _canonical_params(group_relation="AND")
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"blocking-template-{version}"},
            "tasks": [
                {"name": "upstream", "type": "SHELL", "command": "echo ready"},
                task,
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([10_001, 10_002])
    definitions = {
        definition["name"]: definition
        for definition in json.loads(payload["taskDefinitionJson"])
    }
    upstream_code = cast("int", definitions["upstream"]["code"])

    expected = _native_params(group_relation="AND")
    expected["dependence"] = {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "AND",
                "dependItemList": [
                    {"depTaskCode": upstream_code, "status": "SUCCESS"},
                ],
            }
        ],
    }
    assert (
        json.loads(definitions["block-on-upstream-success"]["taskParams"]) == expected
    )


@pytest.mark.parametrize(
    ("field", "canonical_field"),
    [
        ("depend_task_list", "dependTaskList"),
        ("depend_item_list", "dependItemList"),
    ],
)
def test_blocking_nested_canonical_fields_are_alias_only(
    field: str,
    canonical_field: str,
) -> None:
    params = _canonical_params()
    dependence = cast("YamlObject", params["dependence"])
    if field == "depend_task_list":
        dependence[field] = dependence.pop("dependTaskList")
    else:
        groups = cast("list[YamlValue]", dependence["dependTaskList"])
        group = cast("YamlObject", groups[0])
        group[field] = group.pop("dependItemList")

    with pytest.raises(ValueError, match=canonical_field):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("value", [0, 1, "false", "true", None])
def test_blocking_alert_flag_is_a_strict_boolean(value: YamlValue) -> None:
    params = _canonical_params()
    params["alertWhenBlocking"] = value

    with pytest.raises(ValueError, match="alertWhenBlocking"):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("opportunity", "blockingOpportunity"),
        ("outer-relation", "dependence.relation"),
        ("group-relation", "relation"),
        ("status", "status"),
        ("empty-groups", "dependTaskList"),
        ("empty-items", "dependItemList"),
        ("outer-extra", "localParams"),
        ("dependence-extra", "futureField"),
        ("group-extra", "futureField"),
        ("predicate-extra", "cycle"),
    ],
)
def test_blocking_typed_shape_fails_closed_without_opaque_downgrade(
    case: str,
    message: str,
) -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    params = _invalid_canonical_params(case)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=params,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises(ValueError, match=message):
        catalog.normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_blocking_workflow_compile_binds_predicate_and_dag_edge() -> None:
    catalog = get_task_authoring_catalog("3.0.0")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "blocking-gate"},
            "tasks": [
                {"name": "upstream", "type": "SHELL", "command": "echo ready"},
                {
                    "name": "gate",
                    "type": _TASK_TYPE,
                    "task_params": _canonical_params(alert=True),
                },
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([10_001, 10_002])
    definitions = {
        definition["name"]: definition
        for definition in json.loads(payload["taskDefinitionJson"])
    }
    upstream_code = cast("int", definitions["upstream"]["code"])
    gate_code = cast("int", definitions["gate"]["code"])
    native = json.loads(definitions["gate"]["taskParams"])

    assert native == {
        "blockingOpportunity": "BlockingOnSuccess",
        "alertWhenBlocking": True,
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "OR",
                    "dependItemList": [
                        {"depTaskCode": upstream_code, "status": "SUCCESS"},
                    ],
                }
            ],
        },
    }
    relations = cast("list[YamlObject]", json.loads(payload["taskRelationJson"]))
    assert any(
        relation["preTaskCode"] == upstream_code
        and relation["postTaskCode"] == gate_code
        for relation in relations
    )


@pytest.mark.parametrize(
    ("task_ref", "message"),
    [
        ("missing", "unknown task 'missing'"),
        ("gate", "cannot reference itself"),
    ],
)
def test_blocking_graph_references_fail_closed(
    task_ref: str,
    message: str,
) -> None:
    params = _canonical_params()
    dependence = cast("YamlObject", params["dependence"])
    groups = cast("list[YamlValue]", dependence["dependTaskList"])
    group = cast("YamlObject", groups[0])
    predicates = cast("list[YamlValue]", group["dependItemList"])
    predicate = cast("YamlObject", predicates[0])
    predicate["task"] = task_ref
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "blocking-gate"},
            "tasks": [
                {"name": "upstream", "type": "SHELL", "command": "echo ready"},
                {"name": "gate", "type": _TASK_TYPE, "task_params": params},
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match=message):
        prepare_workflow_create_compilation(spec, catalog=catalog)


def test_blocking_logical_predecessor_participates_in_cycle_detection() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "blocking-cycle"},
            "tasks": [
                {
                    "name": "upstream",
                    "type": "SHELL",
                    "command": "echo ready",
                    "depends_on": ["gate"],
                },
                {
                    "name": "gate",
                    "type": _TASK_TYPE,
                    "task_params": _canonical_params(),
                },
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match="dependency cycle"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


@pytest.mark.parametrize("task_name", ["${upstream}", "$[upstream]"])
def test_blocking_rejects_placeholder_task_references(task_name: str) -> None:
    params = _canonical_params()
    dependence = cast("YamlObject", params["dependence"])
    groups = cast("list[YamlValue]", dependence["dependTaskList"])
    group = cast("YamlObject", groups[0])
    predicates = cast("list[YamlValue]", group["dependItemList"])
    predicate = cast("YamlObject", predicates[0])
    predicate["task"] = task_name

    with pytest.raises(ValueError, match="placeholders"):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_blocking_exact_native_wire_has_a_typed_fixed_point(version: str) -> None:
    native = _native_params(alert=True)
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
        graph_context=_GRAPH_CONTEXT,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical_params(alert=True)
    assert (
        encode_task_parameters(
            version=version,
            task_type=_TASK_TYPE,
            task_params=decoded.task.task_params,
            refs=_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )


def test_blocking_native_decode_needs_relation_evidence_to_claim_typed() -> None:
    native = _native_params()

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native


def test_blocking_cyclic_native_relation_evidence_cannot_claim_typed() -> None:
    native = _native_params()

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
        graph_context=TaskGraphContext(
            task_code=7_002,
            relation_edges=frozenset({(7_001, 7_002), (7_002, 7_001)}),
        ),
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native


def test_blocking_failed_opportunity_and_both_relation_levels_project_exactly() -> None:
    canonical = _canonical_params(
        alert=True,
        opportunity="BlockingOnFailed",
        outer_relation="OR",
        group_relation="AND",
        status="FAILURE",
    )
    native = _native_params(
        alert=True,
        opportunity="BlockingOnFailed",
        outer_relation="OR",
        group_relation="AND",
        status="FAILURE",
    )

    encoded = encode_task_parameters(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", canonical),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=encoded.task_params,
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
        graph_context=_GRAPH_CONTEXT,
    )

    assert encoded.task_params == native
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == canonical


def test_blocking_richer_native_state_remains_lossless_opaque_preservation() -> None:
    native = _native_params()
    native["futureTaskField"] = {"native": True}
    dependence = cast("YamlObject", native["dependence"])
    groups = cast("list[YamlValue]", dependence["dependTaskList"])
    group = cast("YamlObject", groups[0])
    predicates = cast("list[YamlValue]", group["dependItemList"])
    predicate = cast("YamlObject", predicates[0])
    predicate["cycle"] = "day"

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type=_TASK_TYPE,
            task_params=decoded.task.task_params,
            refs=_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )


def test_blocking_java_field_name_is_not_the_rest_wire_key() -> None:
    native = _native_params(alert=True)
    native["isAlertWhenBlocking"] = native.pop("alertWhenBlocking")

    preserved = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert preserved.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert preserved.task.task_params == native
    with pytest.raises(
        TaskParameterProjectionError,
        match="isAlertWhenBlocking",
    ):
        decode_task_parameters_with_provenance(
            version="3.2.2",
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", deepcopy(native)),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_native_params(), _canonical_params(), ProjectionSource.TYPED_AUTHORING),
        (
            {**_native_params(), "futureTaskField": {"preserve": True}},
            {**_native_params(), "futureTaskField": {"preserve": True}},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_blocking_live_baseline_export_and_unchanged_update_keep_exact_wire(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    project = ResolvedProject(code=7, name="analytics", description=None)
    dag = _fake_dag(native)

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        preserved_projection_sources=baseline.projection_sources,
        catalog=catalog,
    ).materialize([])
    definitions = {
        definition["name"]: definition
        for definition in json.loads(payload["taskDefinitionJson"])
    }

    assert baseline.projection_sources["gate"] is source
    assert baseline.spec.tasks[1].task_params == exported
    assert document["tasks"][1]["task_params"] == exported
    assert json.loads(definitions["gate"]["taskParams"]) == native


def test_blocking_native_without_matching_relation_stays_opaque_and_unchanged() -> None:
    native = _native_params()
    catalog = get_task_authoring_catalog("3.2.2")
    baseline = workflow_live_baseline(
        _fake_dag(native, relations=()),
        project=ResolvedProject(code=7, name="analytics", description=None),
        catalog=catalog,
    )

    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        preserved_projection_sources=baseline.projection_sources,
        catalog=catalog,
    ).materialize([])
    definitions = {
        definition["name"]: definition
        for definition in json.loads(payload["taskDefinitionJson"])
    }
    relations = cast("list[YamlObject]", json.loads(payload["taskRelationJson"]))

    assert baseline.projection_sources["gate"] is ProjectionSource.OPAQUE_PRESERVE
    assert baseline.spec.tasks[1].task_params == native
    assert json.loads(definitions["gate"]["taskParams"]) == native
    assert not any(
        relation["preTaskCode"] == 7_001 and relation["postTaskCode"] == 7_002
        for relation in relations
    )


def test_blocking_opaque_metadata_patch_preserves_every_native_field() -> None:
    native = _native_params()
    native["futureTaskField"] = {"preserve": True}
    dependence = cast("YamlObject", native["dependence"])
    groups = cast("list[YamlValue]", dependence["dependTaskList"])
    group = cast("YamlObject", groups[0])
    predicates = cast("list[YamlValue]", group["dependItemList"])
    predicate = cast("YamlObject", predicates[0])
    predicate["futurePredicateField"] = ["native"]
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "gate"},
                            "set": {"description": "metadata only"},
                        }
                    ]
                }
            }
        }
    ).patch
    plan = prepare_workflow_mutation_plan(
        _fake_dag(native),
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog("3.2.2"),
    )
    definitions = {
        definition["name"]: definition
        for definition in json.loads(plan.compilation.preview()["taskDefinitionJson"])
    }

    assert definitions["gate"]["description"] == "metadata only"
    assert json.loads(definitions["gate"]["taskParams"]) == native
