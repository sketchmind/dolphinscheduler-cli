"""Compile the exact nullable project-preference singleton and state mutation."""

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
from ds_codegen.runtime_contract import PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_PROJECT_PREFERENCE_SCHEMA_VERSION = 1
COMPILED_PROJECT_PREFERENCE_SEMANTIC_OPERATIONS = frozenset(
    {
        "project-preference.get",
        "project-preference.update",
        "project-preference.enable",
        "project-preference.disable",
        PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,
    }
)
_ABSENT_VERSIONS = frozenset(
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
    }
)
_ENTITY = "org.apache.dolphinscheduler.dao.entity.ProjectPreference"
_PATH = "projects/{projectCode}/project-preference"
_FIELDS = (
    ("id", "Integer", True, None, None),
    ("code", "long", False, "0", None),
    ("projectCode", "long", False, "0", None),
    ("preferences", "String", True, None, None),
    ("userId", "Integer", True, None, None),
    ("state", "int", False, "0", None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)


def _classify(operation: OperationSpec) -> str | None:
    return {
        "ProjectPreferenceController.queryProjectPreferenceByProjectCode": "get",
        "ProjectPreferenceController.updateProjectPreference": "update",
        "ProjectPreferenceController.enableProjectPreference": "set_state",
    }.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    logical = {
        "get": "Optional<ProjectPreference>",
        "update": _ENTITY,
        "set_state": "Void",
    }[primitive]
    expected: dict[str, tuple[str, str | None]] = {"projectCode": ("long", None)}
    if primitive == "update":
        expected["projectPreferences"] = ("String", None)
    elif primitive == "set_state":
        expected["state"] = ("int", None)
    actual = {
        item.wire_name: (item.java_type, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if actual != expected:
        message = (
            f"compiled project_preference {primitive} request type or default changed"
        )
        raise ValueError(message)
    if (
        operation.logical_return_type != logical
        or operation.response_projection != "direct"
        or operation.consumes
    ):
        message = f"compiled project_preference {primitive} response changed"
        raise ValueError(message)
    if primitive == "set_state":
        return CompiledResponsePolicy(codec="set_state", schema=None, capture=None)
    entity = require_model(snapshot, _ENTITY, domain="project_preference")
    if entity.extends is not None or model_field_facts(entity) != _FIELDS:
        message = "compiled project_preference entity fields changed"
        raise ValueError(message)
    return CompiledResponsePolicy(codec=primitive, schema=primitive, capture={})


def _recipe(codecs: Mapping[str, str]) -> str:
    if codecs == {"get": "get", "update": "update", "set_state": "set_state"}:
        return "singleton"
    message = f"compiled project_preference recipe is unsupported: {codecs!r}"
    raise ValueError(message)


PROJECT_PREFERENCE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="project_preference",
    schema_constant="COMPILED_PROJECT_PREFERENCE_SCHEMA_VERSION",
    schema_version=COMPILED_PROJECT_PREFERENCE_SCHEMA_VERSION,
    semantic_operations=COMPILED_PROJECT_PREFERENCE_SEMANTIC_OPERATIONS,
    absent_versions=_ABSENT_VERSIONS,
    primitives=(
        CompiledPrimitive(
            name="get",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=_PATH,
                    channel="path",
                    request_schema="project",
                    request_model="ProjectPreferenceProjectParams",
                    request_fields=("projectCode",),
                    path_fields=("projectCode",),
                    required_fields=frozenset({"projectCode"}),
                ),
            ),
            result_envelope="optional",
        ),
        *(
            CompiledPrimitive(
                name=name,
                requests=(
                    CompiledRequestEpoch(
                        method="PUT" if name == "update" else "POST",
                        path=_PATH,
                        channel="path_form",
                        request_schema=name,
                        request_model=f"ProjectPreference{model}Params",
                        request_fields=("projectCode", field),
                        path_fields=("projectCode",),
                        required_fields=frozenset({"projectCode", field}),
                    ),
                ),
                result_envelope="required",
            )
            for name, model, field in (
                ("update", "Update", "projectPreferences"),
                ("set_state", "State", "state"),
            )
        ),
    ),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = [
    "COMPILED_PROJECT_PREFERENCE_SCHEMA_VERSION",
    "PROJECT_PREFERENCE_COMPILED_DOMAIN",
]
