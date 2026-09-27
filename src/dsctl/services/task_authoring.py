from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from dataclasses import field as dataclass_field
from difflib import get_close_matches
from typing import TYPE_CHECKING, TypedDict, cast

from pydantic import TypeAdapter

from dsctl.errors import UserInputError
from dsctl.models.common import Direct, Priority
from dsctl.models.task_spec import (
    JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN,
    JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
    JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN,
    JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
    JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN,
    JUPYTER_SECRET_PARAMETER_NAMES,
    K8S_LITERAL_JSON_SCHEMA_PATTERN,
    K8S_NAMESPACE_JSON_SCHEMA_PATTERN,
    K8S_NODE_SELECTOR_INTEGER_JSON_SCHEMA_PATTERN,
    K8S_NODE_SELECTOR_LABEL_VALUE_JSON_SCHEMA_PATTERN,
    K8S_TASK_NAME_JSON_SCHEMA_PATTERN,
    K8S_TASK_NAME_MAX_LENGTH,
    K8S_VALUE_JSON_SCHEMA_PATTERN,
    ZEPPELIN_ID_JSON_SCHEMA_PATTERN,
    ZEPPELIN_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
    ZEPPELIN_PARAMETER_VALUE_JSON_SCHEMA_PATTERN,
    ZEPPELIN_REST_ENDPOINT_JSON_SCHEMA_PATTERN,
    TaskRunFlag,
    canonical_task_type,
    task_params_model_for_type,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference
from dsctl.models.workflow_spec import COMMAND_TASK_TYPES
from dsctl.output import CommandResult, require_json_object, require_json_value
from dsctl.services import _task_templates
from dsctl.services._discovery_commands import (
    render_discovery_command,
    render_discovery_reference,
)
from dsctl.services._workflow.authoring import load_selected_task_authoring_catalog
from dsctl.services.enums import supported_enum_member_values
from dsctl.services.task_authoring_catalog import TaskAuthoringField, model_field
from dsctl.services.task_authoring_catalog.parameter_guidance import (
    switch_parameter_guidance as _switch_parameter_guidance,
)
from dsctl.services.task_authoring_catalog.templates import (
    generic_task_runtime_limitation,
)
from dsctl.upstream.task_settings import task_node_native_name

if TYPE_CHECKING:
    from collections.abc import Sequence
    from enum import Enum

    from dsctl.models.task_spec import TaskParamsSpec
    from dsctl.services.task_authoring_catalog import (
        TaskAuthoringCatalog,
        TaskTypeAuthoringProfile,
    )
    from dsctl.support.yaml_io import JsonObject, JsonValue


class TaskAuthoringFieldData(TypedDict, total=False):
    """One authoring field accepted by workflow task YAML."""

    path: str
    type: str
    required: bool
    default: JsonValue
    choices: list[str]
    active_when: str
    choice_source: str
    choice_value: str
    related_commands: list[str]
    compile_path: str
    description: str


@dataclass(slots=True)
class _TaskParamsSchemaFieldTree:
    """Recursive exact-authoring field selection for one task schema."""

    children: dict[str, _TaskParamsSchemaFieldTree] = dataclass_field(
        default_factory=dict,
    )


class TaskAuthoringStateRuleData(TypedDict, total=False):
    """One task-type-specific field state rule."""

    when: str
    condition_paths: list[str]
    active_paths: list[str]
    inactive_paths: list[str]
    compile_policy: dict[str, str]
    description: str


class TaskAuthoringChoiceSourceData(TypedDict, total=False):
    """How to discover valid values for an authoring field."""

    path: str
    command: str
    value: str
    description: str
    related_commands: list[str]


class TaskAuthoringCompileMappingData(TypedDict):
    """How an authoring field maps to the DS create/update payload."""

    authoring_path: str
    ds_payload_path: str
    description: str


class TaskTypeSummaryRowData(TypedDict, total=False):
    """Compact row for task-type get output."""

    kind: str
    name: str
    summary: str
    command: str


class TaskTypeSummaryData(TypedDict):
    """Local authoring summary for one DS task type."""

    task_type: str
    category: str
    kind: str
    variants: list[str]
    payload_modes: list[str]
    required_paths: list[str]
    required_paths_by_payload_mode: dict[str, list[str]]
    template_command: str
    raw_template_command: str
    schema_command: str
    template_index_command: str
    parameter_command: str
    choice_sources: list[TaskAuthoringChoiceSourceData]
    workflow_usage: dict[str, str]
    rows: list[TaskTypeSummaryRowData]


class TaskTypeAuthoringSchemaData(TypedDict):
    """Legacy full local authoring contract for one DS task type."""

    task_type: str
    category: str
    kind: str
    schema: JsonObject
    fields: list[TaskAuthoringFieldData]
    state_rules: list[TaskAuthoringStateRuleData]
    choice_sources: list[TaskAuthoringChoiceSourceData]
    compile_mappings: list[TaskAuthoringCompileMappingData]
    template_command: str
    raw_template_command: str


@dataclass(frozen=True)
class _TaskTypeAuthoringContract:
    """Canonical facts projected into bounded or legacy task schema views."""

    task_type: str
    category: str
    kind: str
    params_model: type[TaskParamsSpec] | None
    parameter_data_types: tuple[str, ...]
    fields: list[TaskAuthoringFieldData]
    state_rules: list[TaskAuthoringStateRuleData]
    env_file: str | None = None


_TASK_TYPE_SCHEMA_VERSION = 2
_COMPILE_MAPPING_DESCRIPTION = (
    "Compiled by workflow create/edit before sending DS REST form fields."
)
_SCRIPT_TASK_TYPES = frozenset({*COMMAND_TASK_TYPES, "REMOTESHELL"})


def task_type_summary_result(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return a local authoring summary for one DS task type."""
    selected_catalog = _selected_catalog(catalog, env_file=env_file)
    normalized = require_supported_authoring_task_type(
        task_type,
        catalog=selected_catalog,
        env_file=env_file,
    )
    data = task_type_summary_data(
        normalized, catalog=selected_catalog, env_file=env_file
    )
    warnings, warning_details = _generic_task_warnings(
        normalized,
        catalog=selected_catalog,
    )
    return CommandResult(
        data=require_json_object(data, label="task type summary data"),
        resolved={"task_type": normalized},
        warnings=warnings,
        warning_details=warning_details,
    )


def task_type_schema_result(
    task_type: str,
    *,
    field: str | None = None,
    json_schema: bool = False,
    compile_mappings: bool = False,
    full: bool = False,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> CommandResult:
    """Return one bounded authoring view or the explicit legacy full contract."""
    selected_catalog = _selected_catalog(catalog, env_file=env_file)
    selected_field = _normalize_schema_field(field)
    view = _task_type_schema_view(
        field=selected_field,
        json_schema=json_schema,
        compile_mappings=compile_mappings,
        full=full,
    )
    normalized = require_supported_authoring_task_type(
        task_type,
        catalog=selected_catalog,
        env_file=env_file,
    )
    contract = _task_type_authoring_contract(
        normalized,
        catalog=selected_catalog,
        env_file=env_file,
    )
    data = _project_task_type_schema(
        contract,
        view=view,
        field=selected_field,
    )
    warnings, warning_details = _generic_task_warnings(
        normalized,
        catalog=selected_catalog,
    )
    resolved: JsonObject = {"task_type": normalized, "view": view}
    if selected_field is not None:
        resolved["field"] = selected_field
    return CommandResult(
        data=require_json_object(data, label="task type authoring schema data"),
        resolved=resolved,
        warnings=warnings,
        warning_details=warning_details,
    )


def task_type_summary_data(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> TaskTypeSummaryData:
    """Build the compact task authoring summary for one supported task type."""
    selected_catalog = _selected_catalog(catalog, env_file=env_file)
    normalized = require_supported_authoring_task_type(
        task_type,
        catalog=selected_catalog,
        env_file=env_file,
    )
    metadata = _task_templates.task_template_metadata(catalog=selected_catalog)[
        normalized
    ]
    fields = _fields_for(normalized, catalog=selected_catalog, env_file=env_file)
    state_rules = _state_rules_for(normalized, catalog=selected_catalog)
    required_paths_by_payload_mode = _required_paths_by_payload_mode(
        normalized,
        fields,
    )
    mode_specific_paths = {
        path for paths in required_paths_by_payload_mode.values() for path in paths
    }
    conditionally_inactive_paths = {
        path for rule in state_rules for path in rule.get("inactive_paths", [])
    }
    required_paths = [
        field["path"]
        for field in fields
        if field.get("required") is True
        and field["path"] not in conditionally_inactive_paths
        and field["path"] not in mode_specific_paths
        and not (required_paths_by_payload_mode and field["path"] == "task_params")
    ]
    return TaskTypeSummaryData(
        task_type=normalized,
        category=metadata["category"],
        kind=metadata["kind"],
        variants=metadata["variants"],
        payload_modes=metadata["payload_modes"],
        required_paths=required_paths,
        required_paths_by_payload_mode=required_paths_by_payload_mode,
        template_command=render_discovery_command(
            "template.task", values={"task_type": normalized}, env_file=env_file
        ),
        raw_template_command=render_discovery_command(
            "template.task",
            values={"task_type": normalized, "raw": True},
            env_file=env_file,
        ),
        schema_command=render_discovery_command(
            "task-type.schema", values={"task_type": normalized}, env_file=env_file
        ),
        template_index_command=render_discovery_command(
            "template.task", env_file=env_file
        ),
        parameter_command=render_discovery_command(
            "template.params", env_file=env_file
        ),
        choice_sources=_choice_sources_from_fields(fields, env_file=env_file),
        workflow_usage=_workflow_usage_for(normalized, catalog=selected_catalog),
        rows=_summary_rows(normalized, catalog=selected_catalog, env_file=env_file),
    )


def _workflow_usage_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> dict[str, str]:
    usage = {
        "paste_into": "workflow YAML tasks[]",
        "validate": "dsctl lint workflow FILE",
        "dry_run": "dsctl workflow create --file FILE --dry-run",
    }
    if task_type == "SUB_WORKFLOW":
        usage["child_parameters"] = _task_templates.nested_workflow_parameter_guidance(
            catalog
        )
    return usage


def _required_paths_by_payload_mode(
    task_type: str,
    fields: Sequence[TaskAuthoringFieldData],
) -> dict[str, list[str]]:
    """Return leaf requirements that depend on a script payload mode."""
    if task_type not in _SCRIPT_TASK_TYPES:
        return {}
    task_params_paths = [
        field["path"]
        for field in fields
        if field.get("required") is True and field["path"].startswith("task_params.")
    ]
    if task_type in COMMAND_TASK_TYPES:
        return {
            "command": ["command"],
            "task_params": task_params_paths,
        }
    return {"task_params": task_params_paths}


def _task_type_authoring_contract(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
    env_file: str | None = None,
) -> _TaskTypeAuthoringContract:
    """Build canonical authoring facts once before selecting a representation."""
    metadata = _task_templates.task_template_metadata(catalog=catalog)[task_type]
    fields = _fields_for(task_type, catalog=catalog, env_file=env_file)
    state_rules = _state_rules_for(task_type, catalog=catalog)
    params_model = None
    if catalog.supports_typed_authoring(task_type):
        params_model = (
            catalog.require_task_type(task_type).default.contract.params_model
            if task_type in catalog.entries
            else task_params_model_for_type(task_type)
        )
    entry = catalog.entries.get(task_type)
    contract_parameter_data_types = (
        None if entry is None else entry.default.contract.parameter_data_types
    )
    return _TaskTypeAuthoringContract(
        task_type=task_type,
        category=metadata["category"],
        kind=metadata["kind"],
        params_model=params_model,
        parameter_data_types=(
            _profile_parameter_data_type_values(catalog)
            if contract_parameter_data_types is None
            else contract_parameter_data_types
        ),
        fields=fields,
        state_rules=state_rules,
        env_file=env_file,
    )


def _normalize_schema_field(field: str | None) -> str | None:
    if field is None:
        return None
    normalized = field.strip()
    if normalized:
        return normalized
    message = "--field must include an authoring field path."
    raise UserInputError(
        message,
        details={"field": field},
        suggestion="Remove --field or pass a path from `dsctl task-type schema TYPE`.",
    )


def _task_type_schema_view(
    *,
    field: str | None,
    json_schema: bool,
    compile_mappings: bool,
    full: bool,
) -> str:
    selected = [
        flag
        for flag, enabled in (
            ("--field", field is not None),
            ("--json-schema", json_schema),
            ("--compile-mappings", compile_mappings),
            ("--full", full),
        )
        if enabled
    ]
    if len(selected) > 1:
        message = "Task type schema view selectors cannot be combined."
        raise UserInputError(
            message,
            details={
                "constraint": "at_most_one_of",
                "selected": selected,
            },
            suggestion=(
                "Use only one of --field, --json-schema, --compile-mappings, or --full."
            ),
        )
    if field is not None:
        return "field"
    if json_schema:
        return "json_schema"
    if compile_mappings:
        return "compile_mappings"
    if full:
        return "full"
    return "fields"


def _project_task_type_schema(
    contract: _TaskTypeAuthoringContract,
    *,
    view: str,
    field: str | None,
) -> JsonObject:
    if view == "full":
        return require_json_object(
            _legacy_task_type_schema_data(contract),
            label="legacy task type authoring schema data",
        )

    data = _bounded_task_type_schema_data(contract)
    if view == "fields":
        data["fields"] = cast("JsonValue", contract.fields)
        data["state_rules"] = cast("JsonValue", contract.state_rules)
    elif view == "field" and field is not None:
        selected = _require_authoring_field(contract, field)
        data["fields"] = cast("JsonValue", [selected])
        data["state_rules"] = cast(
            "JsonValue",
            _state_rules_for_field(contract.state_rules, field),
        )
    elif view == "json_schema":
        data["schema"] = _json_schema_for(
            contract.task_type,
            fields=contract.fields,
            params_model=contract.params_model,
            parameter_data_types=contract.parameter_data_types,
            env_file=contract.env_file,
        )
    elif view == "compile_mappings":
        mappings = _compile_mappings_from_fields(contract.fields)
        data["compile_mapping_policy"] = _COMPILE_MAPPING_DESCRIPTION
        data["compile_mappings"] = cast(
            "JsonValue",
            [
                {
                    "authoring_path": mapping["authoring_path"],
                    "ds_payload_path": mapping["ds_payload_path"],
                }
                for mapping in mappings
            ],
        )
    else:
        message = f"Unsupported internal task type schema view '{view}'"
        raise RuntimeError(message)
    return data


def _bounded_task_type_schema_data(
    contract: _TaskTypeAuthoringContract,
) -> JsonObject:
    return {
        "schema_version": _TASK_TYPE_SCHEMA_VERSION,
        "task_type": contract.task_type,
        "category": contract.category,
        "kind": contract.kind,
        "links": {
            "fields": render_discovery_command(
                "task-type.schema",
                values={"task_type": contract.task_type},
                env_file=contract.env_file,
            ),
            "field": render_discovery_command(
                "task-type.schema",
                values={"task_type": contract.task_type, "field": "FIELD_PATH"},
                env_file=contract.env_file,
            ),
            "json_schema": render_discovery_command(
                "task-type.schema",
                values={"task_type": contract.task_type, "json-schema": True},
                env_file=contract.env_file,
            ),
            "compile_mappings": render_discovery_command(
                "task-type.schema",
                values={"task_type": contract.task_type, "compile-mappings": True},
                env_file=contract.env_file,
            ),
            "full": render_discovery_command(
                "task-type.schema",
                values={"task_type": contract.task_type, "full": True},
                env_file=contract.env_file,
            ),
            "template": render_discovery_command(
                "template.task",
                values={"task_type": contract.task_type},
                env_file=contract.env_file,
            ),
            "raw_template": render_discovery_command(
                "template.task",
                values={"task_type": contract.task_type, "raw": True},
                env_file=contract.env_file,
            ),
        },
    }


def _legacy_task_type_schema_data(
    contract: _TaskTypeAuthoringContract,
) -> TaskTypeAuthoringSchemaData:
    legacy_fields: list[TaskAuthoringFieldData] = []
    for field in contract.fields:
        legacy_field = dict(field)
        legacy_field.pop("choice_value", None)
        legacy_fields.append(cast("TaskAuthoringFieldData", legacy_field))
    choice_sources = _choice_sources_from_fields(
        contract.fields, env_file=contract.env_file
    )
    compile_mappings = _compile_mappings_from_fields(contract.fields)
    schema = deepcopy(
        _json_schema_for(
            contract.task_type,
            fields=legacy_fields,
            params_model=contract.params_model,
            parameter_data_types=contract.parameter_data_types,
            env_file=contract.env_file,
        )
    )
    metadata_value = schema.get("x-dsctl")
    if not isinstance(metadata_value, dict):
        message = "task authoring JSON Schema is missing x-dsctl metadata"
        raise TypeError(message)
    metadata = dict(metadata_value)
    metadata["state_rules"] = cast("JsonValue", contract.state_rules)
    metadata["choice_sources"] = cast("JsonValue", choice_sources)
    metadata["compile_mappings"] = cast("JsonValue", compile_mappings)
    schema["x-dsctl"] = metadata
    return TaskTypeAuthoringSchemaData(
        task_type=contract.task_type,
        category=contract.category,
        kind=contract.kind,
        schema=schema,
        fields=legacy_fields,
        state_rules=contract.state_rules,
        choice_sources=choice_sources,
        compile_mappings=compile_mappings,
        template_command=render_discovery_command(
            "template.task",
            values={"task_type": contract.task_type},
            env_file=contract.env_file,
        ),
        raw_template_command=render_discovery_command(
            "template.task",
            values={"task_type": contract.task_type, "raw": True},
            env_file=contract.env_file,
        ),
    )


def _require_authoring_field(
    contract: _TaskTypeAuthoringContract,
    field_path: str,
) -> TaskAuthoringFieldData:
    fields_by_path = {field["path"]: field for field in contract.fields}
    selected = fields_by_path.get(field_path)
    if selected is not None:
        return selected

    open_plugin_field = contract.kind == "generic" and field_path.startswith(
        "task_params."
    )
    candidates: list[dict[str, str]] = []
    if not open_plugin_field:
        candidates.extend(
            {
                "path": candidate_path,
                "command": render_discovery_command(
                    "task-type.schema",
                    values={"task_type": contract.task_type, "field": candidate_path},
                    env_file=contract.env_file,
                ),
            }
            for candidate_path in get_close_matches(
                field_path,
                list(fields_by_path),
                n=3,
                cutoff=0.45,
            )
        )
    details: JsonObject = {
        "task_type": contract.task_type,
        "field": field_path,
        "available_count": len(fields_by_path),
        "candidates": cast("JsonValue", candidates),
        "discovery_command": render_discovery_command(
            "task-type.schema",
            values={"task_type": contract.task_type},
            env_file=contract.env_file,
        ),
    }
    if open_plugin_field:
        details["open_task_params"] = True
        suggestion = (
            "This generic task type accepts plugin-defined task_params; inspect an "
            "exported workflow or the upstream plugin contract."
        )
    elif candidates:
        suggestion = (
            f"Retry with `{candidates[0]['command']}`, or inspect the bounded "
            "field catalog."
        )
    else:
        suggestion = (
            f"Run `{details['discovery_command']}` to inspect the "
            "bounded field catalog."
        )
    message = f"Unknown {contract.task_type} authoring field '{field_path}'."
    raise UserInputError(
        message,
        details=details,
        suggestion=suggestion,
    )


def _state_rules_for_field(
    rules: Sequence[TaskAuthoringStateRuleData],
    field_path: str,
) -> list[TaskAuthoringStateRuleData]:
    return [rule for rule in rules if _state_rule_mentions_field(rule, field_path)]


def _state_rule_mentions_field(
    rule: TaskAuthoringStateRuleData,
    field_path: str,
) -> bool:
    related_paths = [
        *rule.get("condition_paths", []),
        *rule.get("active_paths", []),
        *rule.get("inactive_paths", []),
        *rule.get("compile_policy", {}),
    ]
    normalized_field = _normalize_authoring_path(field_path)
    return any(
        normalized_field == normalized_path
        or normalized_field.startswith(f"{normalized_path}.")
        or normalized_path.startswith(f"{normalized_field}.")
        for path in related_paths
        if (normalized_path := _normalize_authoring_path(path))
    )


def _normalize_authoring_path(path: str) -> str:
    """Normalize array markers while retaining dotted field boundaries."""
    return path.replace("[]", "")


def require_supported_authoring_task_type(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | None = None,
) -> str:
    """Normalize and validate one task type for local authoring commands."""
    selected_catalog = _selected_catalog(catalog, env_file=env_file)
    normalized = canonical_task_type(task_type)
    supported = _task_templates.supported_task_template_types(catalog=selected_catalog)
    if normalized in supported:
        return normalized
    message = f"Unsupported task type '{task_type}'."
    discovery_command = render_discovery_command("template.task", env_file=env_file)
    raise UserInputError(
        message,
        details={
            "task_type": task_type,
            "available_task_types_count": len(supported),
            "selected_version": selected_catalog.profile_version,
            "discovery_command": discovery_command,
        },
        suggestion=f"Run `{discovery_command}` to inspect supported task types.",
    )


def _selected_catalog(
    catalog: TaskAuthoringCatalog | None,
    *,
    env_file: str | None = None,
) -> TaskAuthoringCatalog:
    if catalog is not None:
        return catalog
    return load_selected_task_authoring_catalog(env_file)


def _fields_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
    env_file: str | None = None,
) -> list[TaskAuthoringFieldData]:
    specific = _task_specific_fields(task_type, catalog=catalog)
    owned = {field.path for field in specific}
    fields = [
        cast("TaskAuthoringFieldData", field.to_data())
        for field in (
            *(
                field
                for field in _common_fields(task_type, catalog=catalog)
                if field.path not in owned
            ),
            *specific,
        )
    ]
    fields = _project_exact_task_fields(
        fields,
        catalog=catalog,
        task_type=task_type,
    )
    for field in fields:
        source = field.get("choice_source")
        if isinstance(source, str):
            choice_value = _choice_source_value(field["path"], source)
            if choice_value is not None:
                field["choice_value"] = choice_value
            field["choice_source"] = render_discovery_reference(
                source, env_file=env_file
            )
        related_commands = field.get("related_commands")
        if isinstance(related_commands, list):
            field["related_commands"] = [
                render_discovery_reference(command, env_file=env_file)
                for command in related_commands
            ]
    return fields


def _project_exact_task_fields(
    fields: list[TaskAuthoringFieldData],
    *,
    catalog: TaskAuthoringCatalog,
    task_type: str,
) -> list[TaskAuthoringFieldData]:
    """Bind stable authoring fields to the selected workflow task-node epoch."""
    surface = catalog.authoring_surface.task_node
    projected: list[TaskAuthoringFieldData] = []
    entry = catalog.entries.get(task_type)
    legacy_conditions_split = (
        task_type == "CONDITIONS" and catalog.profile_version == "1.3.9"
    )
    allow_local_out_without_var_pool = bool(
        entry is not None and entry.default.contract.allow_local_out_without_var_pool
    )
    for original in fields:
        path = original["path"]
        if path in surface.unavailable_fields:
            continue
        if (
            path.startswith("task_params.varPool")
            and not catalog.parameter_semantics.output.var_pool_transport
        ):
            continue
        field = dict(original)
        if legacy_conditions_split and path == "task_params":
            field.pop("compile_path", None)
            field["description"] = (
                "Canonical CONDITIONS intent projected across the outer legacy "
                "TaskNode dependence and conditionResult fields; native params is "
                "fixed to an empty object."
            )
        if (
            path == "task_params.localParams[].direct"
            and not catalog.parameter_semantics.output.var_pool_transport
            and not allow_local_out_without_var_pool
        ):
            field["choices"] = [Direct.IN.value]
        compile_path = field.get("compile_path")
        if isinstance(compile_path, str) and surface.wire == "legacy-process-json":
            field["compile_path"] = _legacy_task_compile_path(compile_path)
        projected.append(cast("TaskAuthoringFieldData", field))
    return projected


def _legacy_task_compile_path(modern_path: str) -> str:
    """Translate one task-definition mapping to the 1.3.9 process JSON wire."""
    if modern_path == "workflowTaskRelationList":
        return "processDefinitionJson.tasks[].preTasks"
    prefix = "taskDefinitionJson[]."
    if not modern_path.startswith(prefix):
        return modern_path
    suffix = modern_path.removeprefix(prefix)
    head, separator, tail = suffix.partition(".")
    native_head = {
        "name": "name",
        "taskType": "type",
        "description": "description",
        "taskParams": "params",
        "flag": "runFlag",
        "workerGroup": "workerGroup",
        "taskPriority": "taskInstancePriority",
        "failRetryTimes": "maxRetryTimes",
        "failRetryInterval": "retryInterval",
        "timeout": "timeout.interval",
        "timeoutNotifyStrategy": "timeout.strategy",
    }.get(head, head)
    native_suffix = native_head
    if separator:
        native_suffix = f"{native_head}.{tail}"
    return f"processDefinitionJson.tasks[].{native_suffix}"


def _common_fields(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskAuthoringField, ...]:
    payload_rule = (
        "required when command is absent"
        if task_type in COMMAND_TASK_TYPES
        else "required"
    )
    fields = [
        TaskAuthoringField(
            "name",
            "string",
            required=True,
            compile_path="taskDefinitionJson[].name",
            description="Task name unique inside one workflow YAML document.",
        ),
        TaskAuthoringField(
            "type",
            "string",
            required=True,
            default=task_type,
            choices=(task_type,),
            compile_path="taskDefinitionJson[].taskType",
            description="DS-native task type.",
        ),
        TaskAuthoringField(
            "description",
            "string",
            default="",
            compile_path=f"taskDefinitionJson[].{task_node_native_name('description')}",
            description="Optional task description.",
        ),
        TaskAuthoringField(
            "task_params",
            "object",
            required=task_type not in COMMAND_TASK_TYPES,
            active_when=payload_rule,
            compile_path="taskDefinitionJson[].taskParams",
            description="DS task plugin payload for this task type.",
        ),
    ]
    if task_type in COMMAND_TASK_TYPES:
        fields.append(
            TaskAuthoringField(
                "command",
                "string",
                required=False,
                active_when="allowed when task_params is absent",
                compile_path="taskDefinitionJson[].taskParams.rawScript",
                description="Shortcut for simple script-like tasks.",
            )
        )
    fields.extend(
        (
            TaskAuthoringField(
                "flag",
                "enum",
                default=TaskRunFlag.YES.value,
                choices=_profile_enum_values(catalog, "flag"),
                compile_path=f"taskDefinitionJson[].{task_node_native_name('flag')}",
                description="Whether DS should run this task.",
            ),
            TaskAuthoringField(
                "worker_group",
                "string",
                default="default",
                choice_source="dsctl worker-group list",
                related_commands=("dsctl worker-group list",),
                compile_path=f"taskDefinitionJson[].{task_node_native_name('worker_group')}",
                description="Worker group used to dispatch the task.",
            ),
            TaskAuthoringField(
                "environment_code",
                "integer",
                choice_source="dsctl environment list",
                related_commands=(
                    "dsctl environment list",
                    "dsctl template environment",
                    "dsctl environment create --name NAME --config-file env.sh",
                ),
                compile_path=f"taskDefinitionJson[].{task_node_native_name('environment_code')}",
                description="Optional environment code bound to worker execution.",
            ),
            TaskAuthoringField(
                "task_group_id",
                "integer",
                choice_source="dsctl task-group list",
                related_commands=(
                    "dsctl task-group list",
                    "dsctl task-group create --name NAME --group-size N",
                ),
                compile_path=f"taskDefinitionJson[].{task_node_native_name('task_group_id')}",
                description="Optional DS task-group id for resource throttling.",
            ),
            TaskAuthoringField(
                "task_group_priority",
                "integer",
                default=0,
                active_when="valid only when task_group_id is set",
                compile_path=f"taskDefinitionJson[].{task_node_native_name('task_group_priority')}",
                description="Priority inside the selected task group.",
            ),
            TaskAuthoringField(
                "priority",
                "enum",
                default=Priority.MEDIUM.value,
                choices=_profile_enum_values(catalog, "priority"),
                compile_path=f"taskDefinitionJson[].{task_node_native_name('priority')}",
                description="DS task priority.",
            ),
            TaskAuthoringField(
                "retry.times",
                "integer",
                default=0,
                compile_path=f"taskDefinitionJson[].{task_node_native_name('retry.times')}",
                description="Retry count after task failure.",
            ),
            TaskAuthoringField(
                "retry.interval",
                "integer",
                default=0,
                compile_path=f"taskDefinitionJson[].{task_node_native_name('retry.interval')}",
                description="Retry interval in minutes.",
            ),
            TaskAuthoringField(
                "timeout",
                "integer",
                default=0,
                compile_path=f"taskDefinitionJson[].{task_node_native_name('timeout')}",
                description="Timeout in minutes; 0 disables timeout handling.",
            ),
            TaskAuthoringField(
                "timeout_notify_strategy",
                "enum",
                choices=_profile_optional_enum_values(
                    catalog,
                    "task-timeout-strategy",
                ),
                active_when="requires timeout > 0",
                compile_path=f"taskDefinitionJson[].{task_node_native_name('timeout_notify_strategy')}",
                description="Timeout warning/failure behavior.",
            ),
            TaskAuthoringField(
                "delay",
                "integer",
                default=0,
                compile_path=f"taskDefinitionJson[].{task_node_native_name('delay')}",
                description="Delay execution by this many minutes.",
            ),
            TaskAuthoringField(
                "cpu_quota",
                "integer",
                compile_path=f"taskDefinitionJson[].{task_node_native_name('cpu_quota')}",
                description="Optional CPU quota; -1 follows DS default behavior.",
            ),
            TaskAuthoringField(
                "memory_max",
                "integer",
                compile_path=f"taskDefinitionJson[].{task_node_native_name('memory_max')}",
                description="Optional memory limit; -1 follows DS default behavior.",
            ),
            TaskAuthoringField(
                "depends_on[]",
                "string",
                default=[],
                choice_source="other tasks in the same workflow YAML",
                compile_path="workflowTaskRelationList",
                description="Upstream task names for ordinary DAG edges.",
            ),
        )
    )
    return tuple(fields)


def _task_specific_fields(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskAuthoringField, ...]:
    if not catalog.supports_typed_authoring(task_type):
        return ()
    entry = catalog.entries.get(task_type)
    if entry is not None:
        return _materialized_task_fields(task_type, entry=entry, catalog=catalog)
    model = task_params_model_for_type(task_type)
    return tuple(
        field.bind_model(model)
        for field in _model_only_task_fields(task_type, catalog=catalog)
    )


def _model_only_task_fields(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskAuthoringField, ...]:
    """Describe reviewed model-only families without granting facet membership."""
    if task_type in {"SHELL", "PYTHON"}:
        return _script_fields(task_type=task_type, catalog=catalog)
    if task_type == "REMOTESHELL":
        return _remote_shell_fields(catalog=catalog)
    if task_type == "HTTP":
        return _http_fields(catalog=catalog)
    if task_type == "SUB_WORKFLOW":
        return _sub_workflow_fields(catalog=catalog)
    if task_type == "SWITCH":
        return _switch_fields(catalog=catalog)
    if task_type == "CONDITIONS":
        return _conditions_fields(catalog=catalog)
    return ()


def _materialized_task_fields(
    task_type: str,
    *,
    entry: TaskTypeAuthoringProfile,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskAuthoringField, ...]:
    """Project one reviewed facet contract into its exact field inventory."""
    contract = entry.default.contract
    params_model = contract.params_model
    model_aliases = (
        {
            field.alias or field_name
            for field_name, field in params_model.model_fields.items()
        }
        if params_model is not None
        else set()
    )
    parameter_fields = (
        _parameter_fields(
            catalog=catalog,
            parameter_data_types=contract.parameter_data_types,
        )
        if "localParams" in model_aliases
        else ()
    )
    if "varPool" not in model_aliases:
        parameter_fields = tuple(
            field
            for field in parameter_fields
            if not field.path.startswith("task_params.varPool")
        )
    if contract.parameter_directions is not None:
        parameter_fields = tuple(
            replace(field, choices=contract.parameter_directions)
            if field.path == "task_params.localParams[].direct"
            else field
            for field in parameter_fields
        )
    if task_type == "EMR":
        parameter_fields = _project_emr_parameter_fields(
            parameter_fields,
            parameter_substitution=catalog.authoring_surface.emr.parameter_substitution,
        )
    if task_type == "EMR_SERVERLESS":
        parameter_fields = _project_emr_serverless_parameter_fields(parameter_fields)
    if task_type == "HIVECLI":
        parameter_fields = _project_hive_cli_parameter_fields(parameter_fields)
    if task_type == "PROCEDURE":
        parameter_fields = tuple(
            replace(
                field,
                description=(
                    "Derived PROCEDURE runtime output pool; authored value "
                    "must remain empty."
                ),
            )
            if field.path == "task_params.varPool[]"
            else field
            for field in parameter_fields
        )
    owned = {field.path for field in contract.fields}
    return (
        *contract.fields,
        *(field for field in parameter_fields if field.path not in owned),
    )


def _project_emr_parameter_fields(
    fields: Sequence[TaskAuthoringField],
    *,
    parameter_substitution: bool,
) -> tuple[TaskAuthoringField, ...]:
    descriptions = {
        "task_params.localParams[]": (
            "Task-local IN/VARCHAR properties available to EMR JSON substitution."
            if parameter_substitution
            else (
                "Task-local IN/VARCHAR properties; this release does not "
                "substitute them into EMR JSON."
            )
        ),
        "task_params.localParams[].prop": (
            "Property name referenced as ${name} in EMR JSON at runtime."
            if parameter_substitution
            else (
                "Task-local property name; this release does not substitute "
                "${name} into EMR JSON."
            )
        ),
        "task_params.localParams[].direct": "EMR authored parameters accept IN only.",
        "task_params.localParams[].type": (
            "EMR authored parameters accept VARCHAR only."
        ),
        "task_params.localParams[].value": (
            "Value substituted before AWS parses the EMR JSON text."
            if parameter_substitution
            else (
                "Stored task-local value; this release does not substitute it "
                "into EMR JSON."
            )
        ),
    }
    return tuple(
        replace(
            field,
            description=descriptions.get(field.path, field.description),
            related_commands=tuple(
                command
                for command in field.related_commands
                if command != "dsctl template params --topic output"
            ),
        )
        for field in fields
    )


def _project_hive_cli_parameter_fields(
    fields: Sequence[TaskAuthoringField],
) -> tuple[TaskAuthoringField, ...]:
    descriptions = {
        "task_params.localParams[]": (
            "Portable unique IN/VARCHAR properties substituted in Hive SQL only."
        ),
        "task_params.localParams[].prop": (
            "Property name referenced as ${name} in hiveSqlScript; HIVECLI options "
            "remain literal."
        ),
        "task_params.localParams[].direct": (
            "HIVECLI inline typed authoring accepts IN only."
        ),
        "task_params.localParams[].type": (
            "HIVECLI inline typed authoring accepts VARCHAR only."
        ),
        "task_params.localParams[].value": (
            "Value substituted into hiveSqlScript, never hiveCliOptions."
        ),
    }
    return tuple(
        replace(
            field,
            description=descriptions.get(field.path, field.description),
            related_commands=tuple(
                command
                for command in field.related_commands
                if command != "dsctl template params --topic output"
            ),
        )
        for field in fields
    )


def _project_emr_serverless_parameter_fields(
    fields: Sequence[TaskAuthoringField],
) -> tuple[TaskAuthoringField, ...]:
    descriptions = {
        "task_params.localParams[]": (
            "Unique IN/VARCHAR properties substituted only in startJobRunRequestJson."
        ),
        "task_params.localParams[].prop": (
            "Property name referenced as ${name} in startJobRunRequestJson."
        ),
        "task_params.localParams[].direct": (
            "EMR_SERVERLESS typed authoring accepts IN only."
        ),
        "task_params.localParams[].type": (
            "EMR_SERVERLESS typed authoring accepts VARCHAR only."
        ),
        "task_params.localParams[].value": (
            "Raw value substituted into startJobRunRequestJson before AWS parsing."
        ),
    }
    return tuple(
        replace(
            field,
            description=descriptions.get(field.path, field.description),
            related_commands=tuple(
                command
                for command in field.related_commands
                if command != "dsctl template params --topic output"
            ),
        )
        for field in fields
    )


def _script_fields(
    *,
    task_type: str,
    catalog: TaskAuthoringCatalog,
) -> tuple[TaskAuthoringField, ...]:
    legacy_python = task_type == "PYTHON" and catalog.profile_version == "1.3.9"
    raw_script_description = (
        _python_139_runtime_guidance()
        if legacy_python
        else "Script body executed by the worker."
    )
    return (
        model_field(
            "task_params.rawScript",
            active_when="required when task_params is used instead of command",
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description=raw_script_description,
        ),
        *_parameter_fields(catalog=catalog),
        *_resource_fields(
            description="Attached DS resources used by the script.",
            empty_only=(catalog.profile_version == "1.3.9"),
        ),
    )


def _python_139_runtime_guidance() -> str:
    """Describe exact legacy Python execution, logging, and recovery behavior."""
    return (
        "Python source executed by the worker. DolphinScheduler 1.3.9 performs "
        "placeholder substitution after CRLF-to-LF normalization, writes the "
        "substituted script as UTF-8, and selects the interpreter from PYTHON_HOME "
        "with a python fallback. Upstream logs full task params, the original and "
        "substituted script, the command, and stdout/stderr at INFO. These fields "
        "are not secret storage; dsctl does not redact them. Execution is a local "
        "process with best-effort cancellation, no structured output, and no "
        "worker-failover resume. A retry reruns the whole script, so side effects "
        "can repeat. Typed resourceList must stay empty because the legacy "
        "full-name resource path bypasses positive-ID permission checks; existing "
        "native resource attachments are unchanged/export opaque state."
    )


def _remote_shell_fields(
    *, catalog: TaskAuthoringCatalog
) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.rawScript",
            compile_path="taskDefinitionJson[].taskParams.rawScript",
            description="Remote shell script body.",
        ),
        model_field(
            "task_params.type",
            "enum",
            model_default=True,
            choices=("SSH",),
            compile_path="taskDefinitionJson[].taskParams.type",
            description="Remote connection mode used by the DS plugin.",
        ),
        model_field(
            "task_params.datasource",
            "integer|string",
            required=True,
            choices=(),
            choice_source="dsctl datasource list",
            related_commands=(
                "dsctl datasource list",
                "dsctl datasource get DATASOURCE",
                "dsctl datasource test DATASOURCE",
                "dsctl template datasource",
            ),
            compile_path="taskDefinitionJson[].taskParams.datasource",
            description=(
                "Positive datasource id or exact name containing remote shell "
                "connection settings."
            ),
        ),
        *_parameter_fields(catalog=catalog),
    )


def _http_fields(*, catalog: TaskAuthoringCatalog) -> tuple[TaskAuthoringField, ...]:
    runtime_guidance = _http_runtime_guidance(catalog)
    body_fields = (
        (
            model_field(
                "task_params.httpBody",
                default="",
                active_when="usually used with POST or PUT",
                compile_path="taskDefinitionJson[].taskParams.httpBody",
                description="HTTP request body.",
            ),
        )
        if catalog.authoring_surface.http.request_body
        else ()
    )
    return (
        model_field(
            "task_params.url",
            compile_path="taskDefinitionJson[].taskParams.url",
            description=f"HTTP URL. {runtime_guidance}".rstrip(),
        ),
        model_field(
            "task_params.httpMethod",
            compile_path="taskDefinitionJson[].taskParams.httpMethod",
            description="HTTP request method.",
        ),
        model_field(
            "task_params.httpParams[]",
            default=[],
            compile_path="taskDefinitionJson[].taskParams.httpParams",
            description="HTTP query parameters or headers.",
        ),
        model_field(
            "task_params.httpParams[].prop",
            compile_path="taskDefinitionJson[].taskParams.httpParams[].prop",
            required=False,
            description="HTTP parameter or header name.",
        ),
        model_field(
            "task_params.httpParams[].httpParametersType",
            compile_path="taskDefinitionJson[].taskParams.httpParams[].httpParametersType",
            required=False,
            description="Whether an item is a query parameter or header.",
        ),
        model_field(
            "task_params.httpParams[].value",
            compile_path="taskDefinitionJson[].taskParams.httpParams[].value",
            required=False,
            description="HTTP parameter or header value.",
        ),
        *body_fields,
        model_field(
            "task_params.httpCheckCondition",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.httpCheckCondition",
            description="HTTP success check strategy.",
        ),
        model_field(
            "task_params.condition",
            default="",
            active_when="used when httpCheckCondition requires a custom condition",
            compile_path="taskDefinitionJson[].taskParams.condition",
            description="Custom HTTP check expression.",
        ),
        model_field(
            "task_params.connectTimeout",
            default=10000,
            compile_path="taskDefinitionJson[].taskParams.connectTimeout",
            description="Connection timeout in milliseconds.",
        ),
        *_parameter_fields(catalog=catalog),
    )


def _http_runtime_guidance(catalog: TaskAuthoringCatalog) -> str:
    """Describe the exact legacy HTTP substitution and recovery boundary."""
    if catalog.profile_version != "1.3.9":
        return ""
    return (
        "DolphinScheduler applies prepared placeholder substitution to the URL and "
        "each HTTP property. Upstream logs complete task params, substituted "
        "request params and properties, the configured URL, status, and the full "
        "response body at INFO. Task fields are not secret storage; dsctl does not "
        "redact them. There is no reliable cancel or worker-failover resume. Retry "
        "reissues the whole HTTP request, so POST, PUT, and DELETE side effects can "
        "duplicate."
    )


def _sub_workflow_fields(
    *, catalog: TaskAuthoringCatalog
) -> tuple[TaskAuthoringField, ...]:
    if catalog.profile_version == "1.3.9":
        return (
            model_field(
                "task_params.childWorkflowName",
                required=True,
                choice_source="dsctl workflow list --project PROJECT",
                related_commands=(
                    "dsctl workflow list --project PROJECT",
                    "dsctl workflow get WORKFLOW --project PROJECT",
                ),
                compile_path=("taskDefinitionJson[].taskParams.processDefinitionId"),
                description=(
                    "Same-project child workflow name resolved by the service to "
                    "the exact DolphinScheduler 1.3.9 processDefinitionId."
                ),
            ),
        )
    native_code_field = catalog.authoring_surface.nested_workflow.native_code_field
    parameter_fields = tuple(
        _sub_workflow_compatibility_field(field, catalog=catalog)
        for field in _parameter_fields(catalog=catalog, include_resources=True)
    )
    return (
        model_field(
            "task_params.workflowDefinitionCode",
            required=True,
            choice_source="dsctl workflow list --project PROJECT",
            related_commands=(
                "dsctl workflow list --project PROJECT",
                "dsctl workflow get WORKFLOW --project PROJECT",
            ),
            compile_path=f"taskDefinitionJson[].taskParams.{native_code_field}",
            description="Child workflow definition code.",
        ),
        *parameter_fields,
    )


def _sub_workflow_compatibility_field(
    field: TaskAuthoringField,
    *,
    catalog: TaskAuthoringCatalog,
) -> TaskAuthoringField:
    context_command = ("dsctl template params --topic context",)
    nested = catalog.parameter_semantics.nested_workflow
    if field.path == "task_params.localParams[]":
        if nested.task_local_params_role == "select-parent-global":
            description = (
                "DS-native task-local properties selecting matching parent workflow "
                "globals supplied to the child workflow."
            )
        elif nested.task_local_params_role == "select-parent-task-varpool":
            description = (
                "DS-native task-local properties selecting parent task varPool values "
                "supplied to the child workflow."
            )
        else:
            source_instruction = (
                "Pass parent-specific values as startup parameters."
                if nested.child_input_sources == ("parent-startup",)
                else (
                    "Supply parent-specific values via the parent "
                    "workflow.global_params or startup parameters."
                )
            )
            description = (
                "DS-native task-local properties. In DS "
                f"{catalog.profile_version} they do not become child workflow inputs. "
                f"{source_instruction}"
            )
        return replace(
            field,
            related_commands=context_command,
            description=description,
        )
    if field.path.startswith("task_params.localParams[]."):
        if nested.task_local_params_role == "ignored":
            description = (
                "Compatibility-only SUB_WORKFLOW localParams field; these entries "
                "do not become child workflow inputs in DS "
                f"{catalog.profile_version}."
            )
        else:
            role = nested.task_local_params_role.replace("-", " ")
            description = (
                f"DS-native SUB_WORKFLOW localParams field; its exact role in DS "
                f"{catalog.profile_version} is {role}."
            )
        return replace(
            field,
            related_commands=context_command,
            description=description,
        )
    if field.path == "task_params.varPool[]":
        return replace(
            field,
            related_commands=context_command,
            description=(
                "Compatibility-only task-definition field; this is not the "
                "parent workflow-instance varPool forwarded to the child. Keep "
                "it empty in new SUB_WORKFLOW YAML."
            ),
        )
    if field.path.startswith("task_params.resourceList"):
        return replace(
            field,
            choice_source=None,
            related_commands=(),
            description=(
                f"DS round-trip-only {nested.task_type} resource field; the "
                f"{catalog.profile_version} plugin does not load task resources. "
                "Keep it empty."
            ),
        )
    return field


def _switch_fields(*, catalog: TaskAuthoringCatalog) -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.switchResult.dependTaskList[]",
            default=[],
            compile_path="taskDefinitionJson[].taskParams.switchResult.dependTaskList",
            description="Ordered conditional branches.",
        ),
        model_field(
            "task_params.switchResult.dependTaskList[].condition",
            description="Branch condition expression. "
            + _switch_parameter_guidance(catalog.profile_version),
        ),
        model_field(
            "task_params.switchResult.dependTaskList[].nextNode",
            choice_source="other tasks in the same workflow YAML",
            compile_path=(
                "taskDefinitionJson[].taskParams.switchResult.dependTaskList[].nextNode"
            ),
            description="Downstream task name for this branch; compiled to task code.",
        ),
        model_field(
            "task_params.switchResult.nextNode",
            choice_source="other tasks in the same workflow YAML",
            compile_path="taskDefinitionJson[].taskParams.switchResult.nextNode",
            description="Default downstream task name; compiled to task code.",
        ),
        *(
            replace(
                field,
                description=field.description
                + " "
                + _switch_parameter_guidance(catalog.profile_version),
            )
            if field.path in {"task_params.localParams[]", "task_params.varPool[]"}
            else field
            for field in _parameter_fields(catalog=catalog)
        ),
    )


def _conditions_fields(
    *, catalog: TaskAuthoringCatalog
) -> tuple[TaskAuthoringField, ...]:
    legacy_split_wire = catalog.profile_version == "1.3.9"
    params_prefix = (
        "processDefinitionJson.tasks[]"
        if legacy_split_wire
        else "taskDefinitionJson[].taskParams"
    )
    predicate_ref = "depTasks" if legacy_split_wire else "depTaskCode"
    runtime_guidance = (
        " DolphinScheduler 1.3.9 evaluates same-process task-name predicates "
        "on the master and stores dependence/conditionResult beside params in "
        "TaskNode. It logs task names, expected/actual states, and the result at "
        "INFO. Task fields are not secret storage; dsctl does not redact them. "
        "There is no worker, structured output, remote application id, or durable "
        "failover resume; retry/failover can reevaluate persisted state."
        if legacy_split_wire
        else ""
    )
    parameter_fields: tuple[TaskAuthoringField, ...] = (
        () if legacy_split_wire else _parameter_fields(catalog=catalog)
    )
    return (
        model_field(
            "task_params.dependence.relation",
            compile_path=f"{params_prefix}.dependence.relation",
            description=(
                "Top-level relation for upstream status checks." + runtime_guidance
            ),
        ),
        model_field(
            "task_params.dependence.dependTaskList[]",
            description="Groups of upstream task status checks.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].relation",
            compile_path=(f"{params_prefix}.dependence.dependTaskList[].relation"),
            description="Relation inside one upstream status-check group.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[]",
            description="One local upstream task status check.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[].task",
            choice_source="other tasks in the same workflow YAML",
            compile_path=(
                f"{params_prefix}.dependence.dependTaskList[]."
                f"dependItemList[].{predicate_ref}"
            ),
            description=(
                "Task name in the same workflow tasks[] list whose terminal state "
                "is checked. dsctl automatically adds its edge into this CONDITIONS "
                "node; no duplicate depends_on is needed."
            ),
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[].status",
            compile_path=(
                f"{params_prefix}.dependence.dependTaskList[].dependItemList[].status"
            ),
            description="Expected terminal state of the local upstream task.",
        ),
        model_field(
            "task_params.conditionResult.successNode[]",
            choice_source="other tasks in the same workflow YAML",
            compile_path=f"{params_prefix}.conditionResult.successNode",
            description=(
                "Task names in the same workflow tasks[] list selected when "
                "conditions succeed. dsctl automatically adds edges from this "
                "CONDITIONS node to these targets; no duplicate depends_on is needed."
            ),
        ),
        model_field(
            "task_params.conditionResult.failedNode[]",
            choice_source="other tasks in the same workflow YAML",
            compile_path=f"{params_prefix}.conditionResult.failedNode",
            description=(
                "Task names in the same workflow tasks[] list selected when "
                "conditions fail. dsctl automatically adds edges from this "
                "CONDITIONS node to these targets; no duplicate depends_on is needed."
            ),
        ),
        *parameter_fields,
    )


def _resource_fields(
    *,
    description: str = "Attached DS resources.",
    empty_only: bool = False,
) -> tuple[TaskAuthoringField, ...]:
    container = TaskAuthoringField(
        "task_params.resourceList[]",
        "object",
        default=[],
        related_commands=(
            ()
            if empty_only
            else (
                "dsctl resource list",
                "dsctl resource upload --file FILE",
            )
        ),
        compile_path="taskDefinitionJson[].taskParams.resourceList",
        description=(
            "Must stay empty for typed 1.3.9 script authoring; native resource "
            "attachments remain unchanged/export opaque state because the legacy "
            "full-name path bypasses positive-ID permission checks."
            if empty_only
            else description
        ),
    )
    if empty_only:
        return (container,)
    return (
        container,
        TaskAuthoringField(
            "task_params.resourceList[].resourceName",
            "string",
            choice_source="dsctl resource list",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.resourceList[].resourceName",
            description=(
                "FILE-root-relative DS resource path, retaining one leading slash; "
                "the compiler resolves it to the exact resource wire identity."
            ),
        ),
    )


def _parameter_fields(
    *,
    catalog: TaskAuthoringCatalog,
    include_resources: bool = False,
    parameter_data_types: Sequence[str] | None = None,
) -> tuple[TaskAuthoringField, ...]:
    selected_parameter_data_types = (
        _profile_parameter_data_type_values(catalog)
        if parameter_data_types is None
        else tuple(parameter_data_types)
    )
    fields = [
        TaskAuthoringField(
            "task_params.localParams[]",
            "object",
            default=[],
            related_commands=(
                "dsctl template params --topic property",
                "dsctl template params --topic built-in",
                "dsctl template params --topic output",
            ),
            compile_path="taskDefinitionJson[].taskParams.localParams",
            description="Task-local DS Property entries.",
        ),
        TaskAuthoringField(
            "task_params.localParams[].prop",
            "string",
            compile_path="taskDefinitionJson[].taskParams.localParams[].prop",
            description="Parameter name referenced as ${name} at runtime.",
        ),
        TaskAuthoringField(
            "task_params.localParams[].direct",
            "enum",
            choices=_enum_values(Direct),
            compile_path="taskDefinitionJson[].taskParams.localParams[].direct",
            description="Parameter direction.",
        ),
        TaskAuthoringField(
            "task_params.localParams[].type",
            "enum",
            choices=selected_parameter_data_types,
            compile_path="taskDefinitionJson[].taskParams.localParams[].type",
            description="Parameter data type.",
        ),
        TaskAuthoringField(
            "task_params.localParams[].value",
            "string",
            compile_path="taskDefinitionJson[].taskParams.localParams[].value",
            description="Optional parameter value or DS expression.",
        ),
        TaskAuthoringField(
            "task_params.varPool[]",
            "object",
            default=[],
            related_commands=("dsctl template params --topic output",),
            compile_path="taskDefinitionJson[].taskParams.varPool",
            description=(
                "Runtime output parameter pool; usually empty in authored YAML."
            ),
        ),
    ]
    if include_resources:
        fields.extend(_resource_fields())
    return tuple(fields)


def _state_rules_for(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> list[TaskAuthoringStateRuleData]:
    entry = catalog.entries.get(task_type)
    if entry is not None:
        return [
            cast("TaskAuthoringStateRuleData", rule.to_data())
            for rule in entry.default.contract.state_rules
        ]
    if task_type in COMMAND_TASK_TYPES:
        return [
            {
                "when": "command is set",
                "condition_paths": ["command"],
                "active_paths": ["command"],
                "inactive_paths": ["task_params"],
                "compile_policy": {
                    "command": "compile to task_params.rawScript",
                    "task_params.localParams": "send []",
                    "task_params.resourceList": "send []",
                },
                "description": "Command shorthand is for simple script-like tasks.",
            },
            {
                "when": "task_params is set",
                "condition_paths": ["task_params"],
                "active_paths": ["task_params"],
                "inactive_paths": ["command"],
                "compile_policy": {},
                "description": (
                    "Use task_params for resources, localParams, and plugin fields."
                ),
            },
        ]
    return []


def _choice_sources_from_fields(
    fields: Sequence[TaskAuthoringFieldData],
    *,
    env_file: str | None = None,
) -> list[TaskAuthoringChoiceSourceData]:
    rows: list[TaskAuthoringChoiceSourceData] = []
    seen: set[str] = set()
    for field in fields:
        command = field.get("choice_source")
        if not isinstance(command, str):
            continue
        path = field["path"]
        if path in seen:
            continue
        seen.add(path)
        row: TaskAuthoringChoiceSourceData = {
            "path": path,
            "command": command,
            "description": _choice_source_description(path, command, env_file=env_file),
        }
        choice_value = field.get("choice_value")
        if isinstance(choice_value, str):
            row["value"] = choice_value
        related_commands = field.get("related_commands")
        if isinstance(related_commands, list) and related_commands:
            row["related_commands"] = related_commands
        rows.append(row)
    return rows


def _choice_source_value(path: str, command: str) -> str | None:
    if "same workflow YAML" in command:
        return "task.name"
    if path.endswith("scriptResource"):
        return "fullName minus resolved.directory, retaining one leading slash"
    if path.endswith(("resourceName", "mainJar", "configResource")):
        return "fullName relative to the FILE root, retaining one leading slash"
    suffix_values = {
        "worker_group": "name",
        "environment_code": "code",
        "namespace": "namespace",
        "cluster": "name",
        "task_group_id": "id",
        "groupId": "id",
        "datasource": "name or id",
        "childWorkflowName": "name",
        "projectName": "name",
        "workflowName": "name",
        "taskName": "name",
        "depTaskName": "name",
        "workflowDefinitionCode": "code",
        "processDefinitionCode": "code",
        "projectCode": "code",
        "definitionCode": "code",
        "depTaskCode": "code",
    }
    for suffix, value in suffix_values.items():
        if path.endswith(suffix):
            return value
    if "enum list" in command:
        return "name"
    return None


def _choice_source_description(
    path: str, command: str, *, env_file: str | None = None
) -> str:
    if "same workflow YAML" in command:
        return "Choose from other task names in the current workflow YAML."
    if path.endswith("scriptResource"):
        return (
            f"Run `{command}`, remove its `resolved.directory` FILE-root prefix "
            f"from the selected file `fullName`, retain one leading slash, and "
            f"use that stable base-relative path as {path}."
        )
    if path.endswith(("resourceName", "mainJar", "configResource")):
        root_command = render_discovery_command("resource.list", env_file=env_file)
        return (
            f"Run `{root_command}` without --dir for the FILE root in "
            "`resolved.directory`, then navigate directories and select a file "
            "`fullName`. Remove that FILE-root prefix and retain one leading "
            f"slash for {path}; when the root is /, fullName is already relative. "
            "Upload the file first when it is missing."
        )
    if path.endswith("datasource"):
        return (
            f"Run `{command}` and use the exact datasource `name` or positive "
            f"`id` for {path}."
        )
    if path.endswith("depTaskCode"):
        return (
            f"Run `{command}` and use the task `code`; use 0 when "
            "dependentType targets the whole workflow."
        )
    return f"Run `{command}` and use the indicated value for {path}."


def _compile_mappings_from_fields(
    fields: Sequence[TaskAuthoringFieldData],
) -> list[TaskAuthoringCompileMappingData]:
    mappings: dict[str, str] = {}
    for field in fields:
        compile_path = field.get("compile_path")
        if isinstance(compile_path, str):
            mappings[field["path"]] = compile_path
    return [
        {
            "authoring_path": authoring_path,
            "ds_payload_path": ds_payload_path,
            "description": (_COMPILE_MAPPING_DESCRIPTION),
        }
        for authoring_path, ds_payload_path in mappings.items()
    ]


def _json_schema_for(
    task_type: str,
    *,
    fields: Sequence[TaskAuthoringFieldData],
    params_model: type[TaskParamsSpec] | None,
    parameter_data_types: Sequence[str],
    env_file: str | None = None,
) -> JsonObject:
    task_params_schema = _task_params_json_schema(
        task_type,
        params_model=params_model,
    )
    _project_task_params_schema(task_params_schema, fields=fields)
    _project_parameter_data_type_schema(
        task_params_schema,
        parameter_data_types=parameter_data_types,
    )
    _project_task_params_choice_schemas(task_params_schema, fields=fields)
    if task_type == "HTTP":
        task_params_schema["additionalProperties"] = False
    _project_exact_legacy_task_params_schema(
        task_type,
        task_params_schema,
        fields=fields,
    )
    if task_type == "EMR":
        _project_emr_task_params_schema(task_params_schema, fields=fields)
    if task_type == "JUPYTER":
        _project_jupyter_task_params_schema(task_params_schema)
    if task_type == "ZEPPELIN":
        _project_zeppelin_task_params_schema(task_params_schema, fields=fields)
    if task_type == "K8S":
        _project_k8s_task_params_schema(task_params_schema, fields=fields)
    _project_sagemaker_task_params_schema(
        task_type,
        task_params_schema,
        fields=fields,
    )
    properties = _top_level_json_schema_properties(fields)
    _project_k8s_task_name_schema(
        task_type,
        properties,
        params_model=params_model,
    )
    properties["type"] = {"const": task_type, "type": "string"}
    properties["task_params"] = {"$ref": "#/$defs/task_params"}
    required = ["name", "type"]
    if task_type not in COMMAND_TASK_TYPES:
        required.append("task_params")
    schema: JsonObject = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{task_type} task authoring schema",
        "type": "object",
        "properties": cast("JsonValue", properties),
        "required": required,
        "$defs": {
            "task_params": task_params_schema,
        },
        "x-dsctl": {
            "task_type": task_type,
            "template_command": render_discovery_command(
                "template.task", values={"task_type": task_type}, env_file=env_file
            ),
            "raw_template_command": render_discovery_command(
                "template.task",
                values={"task_type": task_type, "raw": True},
                env_file=env_file,
            ),
            "lint_command_pattern": "dsctl lint workflow FILE",
            # JSON-schema compatibility alias; remove with a breaking contract.
            "lint_command": "dsctl lint workflow FILE",
        },
    }
    if task_type in COMMAND_TASK_TYPES:
        schema["oneOf"] = [
            {"required": ["command"], "not": {"required": ["task_params"]}},
            {"required": ["task_params"], "not": {"required": ["command"]}},
        ]
    return require_json_object(schema, label="task authoring json schema")


def _project_exact_legacy_task_params_schema(
    task_type: str,
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Close reviewed 1.3.9 parameter subsets selected by their exact fields."""
    if _is_legacy_script_139_schema(task_type, fields=fields):
        _project_legacy_script_139_schema(schema)
    elif task_type == "CONDITIONS" and _is_legacy_conditions_139_schema(fields):
        _project_legacy_conditions_139_schema(schema)
    elif task_type == "SUB_WORKFLOW" and _is_legacy_sub_workflow_139_schema(fields):
        _project_legacy_sub_workflow_139_schema(schema)


def _is_legacy_script_139_schema(
    task_type: str,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> bool:
    """Recognize the only script epoch without the varPool authoring field."""
    return task_type in {"PYTHON", "SHELL"} and not any(
        field["path"].startswith("task_params.varPool") for field in fields
    )


def _is_legacy_sub_workflow_139_schema(
    fields: Sequence[TaskAuthoringFieldData],
) -> bool:
    """Recognize the name-resolved 1.3.9 nested-workflow contract."""
    return any(field["path"] == "task_params.childWorkflowName" for field in fields)


_LEGACY_SCRIPT_139_BLANK_CHARACTERS = (
    r"\x09-\x0d\x1c-\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000"
)
_LEGACY_SCRIPT_139_NONBLANK_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S]*[^{_LEGACY_SCRIPT_139_BLANK_CHARACTERS}])"
    r"[\s\S]+(?![\s\S])"
)


def _project_legacy_sub_workflow_139_schema(schema: JsonObject) -> None:
    """Close the exact 1.3.9 name selector and make it required."""
    schema["additionalProperties"] = False
    schema["required"] = ["childWorkflowName"]
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    child_workflow_name = properties.get("childWorkflowName")
    if not isinstance(child_workflow_name, dict):
        return
    child_workflow_name.pop("anyOf", None)
    child_workflow_name.pop("default", None)
    child_workflow_name["type"] = "string"
    child_workflow_name["minLength"] = 1


def _project_legacy_script_139_schema(schema: JsonObject) -> None:
    """Close 1.3.9 script params and require an empty resource list."""
    schema["additionalProperties"] = False
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    raw_script = properties.get("rawScript")
    if isinstance(raw_script, dict):
        raw_script["pattern"] = _LEGACY_SCRIPT_139_NONBLANK_JSON_SCHEMA_PATTERN
    resource_list = properties.get("resourceList")
    if not isinstance(resource_list, dict):
        return
    resource_list["items"] = False
    resource_list["maxItems"] = 0
    resource_list["description"] = (
        "Must be empty for typed 1.3.9 script authoring. Native resource "
        "attachments are unchanged/export opaque because the legacy full-name "
        "wire bypasses positive-ID permission checks."
    )
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        return
    global_param = definitions.get("GlobalParamSpec")
    if not isinstance(global_param, dict):
        return
    parameter_properties = global_param.get("properties")
    if not isinstance(parameter_properties, dict):
        return
    prop = parameter_properties.get("prop")
    if isinstance(prop, dict):
        prop["pattern"] = _LEGACY_SCRIPT_139_NONBLANK_JSON_SCHEMA_PATTERN


def _is_legacy_conditions_139_schema(
    fields: Sequence[TaskAuthoringFieldData],
) -> bool:
    """Recognize CONDITIONS whose exact wire is split across legacy TaskNode."""
    return any(
        field.get("compile_path") == "processDefinitionJson.tasks[].dependence.relation"
        for field in fields
    )


def _project_legacy_conditions_139_schema(schema: JsonObject) -> None:
    """Close the split-wire subset and expose its exact graph cardinalities."""
    schema["additionalProperties"] = False
    for array_path in (
        ("dependence", "dependTaskList"),
        ("dependence", "dependTaskList[]", "dependItemList"),
    ):
        node = _task_params_schema_node(schema, array_path)
        if node is not None:
            node["minItems"] = 1
    for outcome in ("successNode", "failedNode"):
        node = _task_params_schema_node(schema, ("conditionResult", outcome))
        if node is not None:
            node["minItems"] = 1
            node["maxItems"] = 1
            node["description"] = (
                "Exactly one distinct same-workflow branch task name is required "
                "by the reviewed DolphinScheduler 1.3.9 CONDITIONS subset."
            )
    for name_path in (
        (
            "dependence",
            "dependTaskList[]",
            "dependItemList[]",
            "task",
        ),
        ("conditionResult", "successNode[]"),
        ("conditionResult", "failedNode[]"),
    ):
        node = _task_params_schema_node(schema, name_path)
        if node is not None:
            node["pattern"] = _LEGACY_SCRIPT_139_NONBLANK_JSON_SCHEMA_PATTERN


def _project_parameter_data_type_schema(
    schema: JsonObject,
    *,
    parameter_data_types: Sequence[str],
) -> None:
    """Narrow the broad typed enum to the selected exact DS profile."""
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        return
    data_type = definitions.get("DataType")
    if not isinstance(data_type, dict):
        return
    data_type["enum"] = list(parameter_data_types)


def _project_task_params_choice_schemas(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Apply exact field choices to refs in the Pydantic-generated schema."""
    for field in fields:
        path = field["path"]
        choices = field.get("choices")
        if not path.startswith("task_params.") or not isinstance(choices, list):
            continue
        node = _task_params_schema_node(
            schema,
            path.removeprefix("task_params.").split("."),
        )
        if node is None:
            continue
        target = _task_params_schema_ref_target(node, root=schema)
        target["enum"] = _json_schema_choice_values(field, choices=choices)


def _json_schema_choice_values(
    field: TaskAuthoringFieldData,
    *,
    choices: Sequence[str],
) -> list[JsonValue]:
    """Keep projected choice values compatible with the field's JSON type."""
    if field["type"] == "integer":
        try:
            return [int(choice) for choice in choices]
        except ValueError as exc:
            message = f"Integer authoring choices must be integers: {field['path']}"
            raise TypeError(message) from exc
    return list(choices)


def _task_params_schema_node(
    root: JsonObject,
    segments: Sequence[str],
) -> JsonObject | None:
    current = root
    for segment in segments:
        current = _task_params_schema_ref_target(current, root=root)
        properties = current.get("properties")
        if not isinstance(properties, dict):
            return None
        name = segment.removesuffix("[]")
        child = properties.get(name)
        if not isinstance(child, dict):
            return None
        current = child
        if segment.endswith("[]"):
            current = _task_params_schema_ref_target(current, root=root)
            items = current.get("items")
            if not isinstance(items, dict):
                return None
            current = items
    return current


def _task_params_schema_ref_target(
    node: JsonObject,
    *,
    root: JsonObject,
) -> JsonObject:
    ref = node.get("$ref")
    # Older supported Pydantic releases wrap refs with defaults in one allOf.
    # Only unwrap that representation, never a multi-branch intersection.
    if not isinstance(ref, str):
        all_of = node.get("allOf")
        if (
            isinstance(all_of, list)
            and len(all_of) == 1
            and isinstance(all_of[0], dict)
            and set(all_of[0]) == {"$ref"}
        ):
            ref = all_of[0]["$ref"]
    definitions = root.get("$defs")
    if not isinstance(ref, str) or not isinstance(definitions, dict):
        return node
    target = definitions.get(ref.rsplit("/", maxsplit=1)[-1])
    return target if isinstance(target, dict) else node


def _project_emr_task_params_schema(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Encode EMR's exact discriminator and mutually exclusive payloads."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    program_field = next(
        (field for field in fields if field["path"] == "task_params.programType"),
        None,
    )
    program_types = None if program_field is None else program_field.get("choices")
    if not isinstance(program_types, list):
        return
    for field_name in ("jobFlowDefineJson", "stepsDefineJson"):
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        field_schema.pop("anyOf", None)
        field_schema.pop("default", None)
        field_schema["type"] = "string"
    variants: list[JsonObject] = []
    for program_type in program_types:
        active_field, inactive_field = (
            ("jobFlowDefineJson", "stepsDefineJson")
            if program_type == "RUN_JOB_FLOW"
            else ("stepsDefineJson", "jobFlowDefineJson")
        )
        if active_field not in properties:
            continue
        variant: JsonObject = {
            "properties": {"programType": {"const": program_type}},
            "required": ["programType", active_field],
        }
        if inactive_field in properties:
            variant["not"] = {"required": [inactive_field]}
        variants.append(variant)
    schema["oneOf"] = variants


def _project_jupyter_task_params_schema(schema: JsonObject) -> None:
    """Expose the portable shell-safe notebook constraints to schema clients."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    typed_properties = cast("JsonObject", properties)
    _project_jupyter_required_string_schemas(typed_properties)
    _project_jupyter_optional_token_schemas(schema, typed_properties)
    _project_jupyter_parameters_schema(typed_properties)


def _project_jupyter_required_string_schemas(properties: JsonObject) -> None:
    """Project the preinstalled environment and absolute notebook paths."""
    required_patterns = {
        "condaEnvName": JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN,
        "inputNotePath": JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
        "outputNotePath": JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
    }
    for field_name, pattern in required_patterns.items():
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        field_schema["type"] = "string"
        field_schema["minLength"] = 1
        field_schema["pattern"] = pattern


def _project_jupyter_optional_token_schemas(
    schema: JsonObject,
    properties: JsonObject,
) -> None:
    """Project shell-safe optional kernel and engine values."""
    for field_name in ("kernel", "engine"):
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        target = _task_params_schema_ref_target(field_schema, root=schema)
        alternatives = target.get("anyOf")
        if not isinstance(alternatives, list):
            continue
        for alternative in alternatives:
            if isinstance(alternative, dict) and alternative.get("type") == "string":
                alternative["pattern"] = JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN
                alternative["minLength"] = 1


def _project_jupyter_parameters_schema(properties: JsonObject) -> None:
    """Project literal token maps and the bounded secret-name denylist."""
    parameters_schema = properties.get("parameters")
    if not isinstance(parameters_schema, dict):
        return
    parameters_schema["propertyNames"] = {
        "type": "string",
        "pattern": JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
        "not": {"enum": sorted(JUPYTER_SECRET_PARAMETER_NAMES)},
    }
    value_schema = parameters_schema.get("additionalProperties")
    if isinstance(value_schema, dict):
        value_schema["pattern"] = JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN


def _project_zeppelin_task_params_schema(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Require the one connection field selected by the exact Zeppelin profile."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    typed_properties = cast("JsonObject", properties)
    _project_zeppelin_identity_schemas(typed_properties)
    active_field = _zeppelin_active_connection_field(fields)
    _project_zeppelin_active_connection_schema(
        typed_properties,
        active_field=active_field,
    )
    _project_zeppelin_required_connection(schema, active_field=active_field)
    _project_zeppelin_parameters_schema(typed_properties, fields=fields)


def _project_zeppelin_identity_schemas(properties: JsonObject) -> None:
    """Expose the exact portable URL-segment constraint to schema consumers."""
    for field_name in ("noteId", "paragraphId"):
        identity_schema = properties.get(field_name)
        if not isinstance(identity_schema, dict):
            continue
        identity_schema["type"] = "string"
        identity_schema["minLength"] = 1
        identity_schema["pattern"] = ZEPPELIN_ID_JSON_SCHEMA_PATTERN


def _zeppelin_active_connection_field(
    fields: Sequence[TaskAuthoringFieldData],
) -> str | None:
    """Return the sole native connection field selected by this exact profile."""
    mode_field = next(
        (field for field in fields if field["path"] == "task_params.connectionMode"),
        None,
    )
    modes = None if mode_field is None else mode_field.get("choices")
    if not isinstance(modes, list) or len(modes) != 1:
        return None
    return {
        "WORKER_CONFIG": None,
        "REST_ENDPOINT": "restEndpoint",
        "DATASOURCE": "datasource",
    }.get(modes[0])


def _project_zeppelin_required_connection(
    schema: JsonObject,
    *,
    active_field: str | None,
) -> None:
    """Require the profile-selected native connection field when one exists."""
    required = schema.get("required")
    if not isinstance(required, list):
        required = []
        schema["required"] = required
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    if active_field in properties and active_field not in required:
        required.append(active_field)


def _project_zeppelin_active_connection_schema(
    properties: JsonObject,
    *,
    active_field: str | None,
) -> None:
    """Remove nullable model artifacts from the exact active connection field."""
    if active_field == "restEndpoint":
        projected_schema: JsonObject = {
            "type": "string",
            "minLength": 1,
            "pattern": ZEPPELIN_REST_ENDPOINT_JSON_SCHEMA_PATTERN,
        }
    elif active_field == "datasource":
        field_schema = properties.get(active_field)
        if isinstance(field_schema, dict):
            _project_datasource_reference_schema(field_schema)
        return
    else:
        return
    field_schema = properties.get(active_field)
    if not isinstance(field_schema, dict):
        return
    field_schema.pop("anyOf", None)
    field_schema.pop("default", None)
    field_schema.update(projected_schema)


def _project_datasource_reference_schema(field_schema: JsonObject) -> None:
    """Keep active datasource inputs aligned with the canonical name/id model."""
    for keyword in ("anyOf", "default", "type", "exclusiveMinimum"):
        field_schema.pop(keyword, None)
    field_schema.update(
        require_json_object(
            TypeAdapter(DatasourceReference).json_schema(),
            label="datasource reference schema",
        )
    )


def _project_zeppelin_parameters_schema(
    properties: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Expose literal parameter-key constraints and the 3.0.x empty-map epoch."""
    parameters_field = next(
        (field for field in fields if field["path"] == "task_params.parameters"),
        None,
    )
    parameters_schema = properties.get("parameters")
    if not isinstance(parameters_schema, dict):
        return
    parameters_schema["propertyNames"] = {
        "type": "string",
        "pattern": ZEPPELIN_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
    }
    value_schema = parameters_schema.get("additionalProperties")
    if isinstance(value_schema, dict):
        value_schema["pattern"] = ZEPPELIN_PARAMETER_VALUE_JSON_SCHEMA_PATTERN
    if parameters_field is not None and "compile_path" not in parameters_field:
        parameters_schema["maxProperties"] = 0


def _project_k8s_task_params_schema(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Close active K8S connections and selector requirements by exact epoch."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    typed_properties = cast("JsonObject", properties)
    _project_required_task_param_fields(schema, fields=fields)
    _project_k8s_active_connection_schemas(typed_properties)
    _project_k8s_field_defaults(typed_properties, fields=fields)
    _project_k8s_argv_schema(typed_properties)
    _project_k8s_node_selector_schema(schema)
    _compact_k8s_schema_guidance(schema)
    _project_k8s_runtime_validations(schema, fields=fields)


def _project_k8s_task_name_schema(
    task_type: str,
    properties: JsonObject,
    *,
    params_model: type[TaskParamsSpec] | None,
) -> None:
    """Publish the exact outer task-name limit used by the K8S runtime identity."""
    if task_type != "K8S" or params_model is None:
        return
    name = properties.get("name")
    if not isinstance(name, dict):
        return
    # The exact pattern is the useful machine contract here; the generic
    # top-level description is redundant with the field catalog and would push
    # this already-rich schema past the compact-envelope budget.
    name.pop("description", None)
    name["pattern"] = K8S_TASK_NAME_JSON_SCHEMA_PATTERN
    name["maxLength"] = K8S_TASK_NAME_MAX_LENGTH


def _project_required_task_param_fields(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Require every selected top-level field advertised as required."""
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    required = schema.get("required")
    if not isinstance(required, list):
        required = []
        schema["required"] = required
    for field in fields:
        path = field["path"]
        field_name = path.removeprefix("task_params.")
        if (
            field.get("required") is True
            and "." not in field_name
            and "[]" not in field_name
            and field_name in properties
            and field_name not in required
        ):
            required.append(field_name)


def _project_sagemaker_task_params_schema(
    task_type: str,
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Publish the exact required datasource reference in datasource epochs."""
    if task_type != "SAGEMAKER":
        return
    _project_required_task_param_fields(schema, fields=fields)
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return
    datasource = properties.get("datasource")
    if not isinstance(datasource, dict):
        return
    _project_datasource_reference_schema(datasource)


def _project_k8s_active_connection_schemas(properties: JsonObject) -> None:
    """Remove nullable model artifacts from selected connection fields."""
    projected: dict[str, JsonObject] = {
        "namespace": {
            "type": "string",
            "minLength": 1,
            "pattern": K8S_NAMESPACE_JSON_SCHEMA_PATTERN,
        },
        "cluster": {
            "type": "string",
            "minLength": 1,
            "pattern": K8S_LITERAL_JSON_SCHEMA_PATTERN,
        },
    }
    for field_name, exact_schema in projected.items():
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        field_schema.pop("anyOf", None)
        field_schema.pop("default", None)
        field_schema.update(exact_schema)
    datasource = properties.get("datasource")
    if isinstance(datasource, dict):
        _project_datasource_reference_schema(datasource)


def _project_k8s_field_defaults(
    properties: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Publish only defaults that exact normalization actually materializes."""
    defaults = {
        field["path"].removeprefix("task_params."): deepcopy(field["default"])
        for field in fields
        if field["path"].startswith("task_params.")
        and "." not in field["path"].removeprefix("task_params.")
        and "[]" not in field["path"]
        and "default" in field
    }
    for field_name, field_schema in properties.items():
        if not isinstance(field_schema, dict):
            continue
        if field_name in defaults:
            field_schema["default"] = defaults[field_name]
        else:
            field_schema.pop("default", None)


def _project_k8s_argv_schema(properties: JsonObject) -> None:
    """Project the runtime literal-string gate into command and args items."""
    for field_name in ("command", "args"):
        field_schema = properties.get(field_name)
        if not isinstance(field_schema, dict):
            continue
        items = field_schema.get("items")
        if isinstance(items, dict):
            items["pattern"] = K8S_VALUE_JSON_SCHEMA_PATTERN


def _compact_k8s_schema_guidance(value: JsonValue) -> None:
    """Keep the machine view bounded; detailed guidance lives in field views."""
    if isinstance(value, dict):
        value.pop("description", None)
        for child in value.values():
            _compact_k8s_schema_guidance(child)
    elif isinstance(value, list):
        for child in value:
            _compact_k8s_schema_guidance(child)


def _project_k8s_runtime_validations(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Name exact cross-item rules that JSON Schema cannot express."""
    field_paths = {field["path"] for field in fields}
    validations = ["environment names must be unique"]
    if any(path.startswith("task_params.outputs") for path in field_paths):
        validations.append(
            "output names must be unique and disjoint from environment names"
        )
    if any(path.startswith("task_params.customizedLabels") for path in field_paths):
        validations.append("customizedLabels label keys must be unique")
    schema["x-dsctl-runtime-validations"] = validations


def _project_k8s_node_selector_schema(schema: JsonObject) -> None:
    """Express exact label-value and operator cardinality constraints."""
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        return
    selector = definitions.get("K8sNodeSelectorSpec")
    if not isinstance(selector, dict):
        return
    properties = selector.get("properties")
    if not isinstance(properties, dict):
        return
    values = properties.get("values")
    if not isinstance(values, dict):
        return
    items = values.get("items")
    if not isinstance(items, dict):
        items = {"type": "string"}
        values["items"] = items
    items.pop("pattern", None)
    values["uniqueItems"] = True
    root_properties = schema.get("properties")
    if isinstance(root_properties, dict):
        node_selectors = root_properties.get("nodeSelectors")
        if isinstance(node_selectors, dict):
            node_selectors["uniqueItems"] = True
    selector["allOf"] = [
        {
            "if": {
                "properties": {"operator": {"enum": ["In", "NotIn"]}},
                "required": ["operator"],
            },
            "then": {
                "properties": {
                    "values": {
                        "minItems": 1,
                        "items": {
                            "type": "string",
                            "pattern": (
                                K8S_NODE_SELECTOR_LABEL_VALUE_JSON_SCHEMA_PATTERN
                            ),
                        },
                    }
                }
            },
        },
        {
            "if": {
                "properties": {"operator": {"enum": ["Exists", "DoesNotExist"]}},
                "required": ["operator"],
            },
            "then": {"properties": {"values": {"maxItems": 0}}},
        },
        {
            "if": {
                "properties": {"operator": {"enum": ["Gt", "Lt"]}},
                "required": ["operator"],
            },
            "then": {
                "properties": {
                    "values": {
                        "minItems": 1,
                        "maxItems": 1,
                        "items": {
                            "type": "string",
                            "pattern": K8S_NODE_SELECTOR_INTEGER_JSON_SCHEMA_PATTERN,
                        },
                    }
                }
            },
        },
    ]


def _task_params_json_schema(
    task_type: str,
    *,
    params_model: type[TaskParamsSpec] | None,
) -> JsonObject:
    if params_model is None:
        return {
            "type": "object",
            "additionalProperties": True,
            "description": "Generic DS-native task_params object for this plugin.",
        }
    schema = params_model.model_json_schema(
        by_alias=True,
        ref_template="#/$defs/task_params/$defs/{model}",
    )
    return require_json_object(schema, label=f"{task_type} task params schema")


def _project_task_params_schema(
    schema: JsonObject,
    *,
    fields: Sequence[TaskAuthoringFieldData],
) -> None:
    """Prune the broad typed model to the selected exact authoring field tree."""
    field_tree = _TaskParamsSchemaFieldTree()
    for field in fields:
        path = field["path"]
        if not path.startswith("task_params."):
            continue
        current = field_tree
        for segment in path.removeprefix("task_params.").split("."):
            name = segment.removesuffix("[]")
            current = current.children.setdefault(name, _TaskParamsSchemaFieldTree())
    _prune_task_params_schema_node(
        schema,
        field_tree=field_tree,
        root=schema,
        visited=set(),
    )


def _prune_task_params_schema_node(
    node: JsonObject,
    *,
    field_tree: _TaskParamsSchemaFieldTree,
    root: JsonObject,
    visited: set[tuple[str, int]],
) -> None:
    if not field_tree.children:
        return
    _prune_task_params_schema_ref(
        node,
        field_tree=field_tree,
        root=root,
        visited=visited,
    )
    _prune_task_params_schema_properties(
        node,
        field_tree=field_tree,
        root=root,
        visited=visited,
    )
    _prune_task_params_schema_children(
        node,
        field_tree=field_tree,
        root=root,
        visited=visited,
    )


def _prune_task_params_schema_ref(
    node: JsonObject,
    *,
    field_tree: _TaskParamsSchemaFieldTree,
    root: JsonObject,
    visited: set[tuple[str, int]],
) -> None:
    ref = node.get("$ref")
    definitions = root.get("$defs")
    if not isinstance(ref, str) or not isinstance(definitions, dict):
        return
    definition_name = ref.rsplit("/", maxsplit=1)[-1]
    marker = (definition_name, id(field_tree))
    definition = definitions.get(definition_name)
    if marker in visited or not isinstance(definition, dict):
        return
    visited.add(marker)
    _prune_task_params_schema_node(
        definition,
        field_tree=field_tree,
        root=root,
        visited=visited,
    )


def _prune_task_params_schema_properties(
    node: JsonObject,
    *,
    field_tree: _TaskParamsSchemaFieldTree,
    root: JsonObject,
    visited: set[tuple[str, int]],
) -> None:
    properties = node.get("properties")
    if not isinstance(properties, dict):
        return
    for property_name in tuple(properties):
        if property_name not in field_tree.children:
            properties.pop(property_name)
            continue
        property_schema = properties[property_name]
        subtree = field_tree.children[property_name]
        if isinstance(property_schema, dict) and subtree.children:
            _prune_task_params_schema_node(
                property_schema,
                field_tree=subtree,
                root=root,
                visited=visited,
            )
    required = node.get("required")
    if isinstance(required, list):
        node["required"] = [name for name in required if name in properties]


def _prune_task_params_schema_children(
    node: JsonObject,
    *,
    field_tree: _TaskParamsSchemaFieldTree,
    root: JsonObject,
    visited: set[tuple[str, int]],
) -> None:
    items = node.get("items")
    if isinstance(items, dict):
        _prune_task_params_schema_node(
            items,
            field_tree=field_tree,
            root=root,
            visited=visited,
        )
    for union_key in ("anyOf", "oneOf", "allOf"):
        variants = node.get(union_key)
        if not isinstance(variants, list):
            continue
        for variant in variants:
            if isinstance(variant, dict):
                _prune_task_params_schema_node(
                    variant,
                    field_tree=field_tree,
                    root=root,
                    visited=visited,
                )


def _field_json_schema(field: TaskAuthoringFieldData) -> JsonObject:
    schema: JsonObject = {"description": field["description"]}
    field_type = field["type"]
    if field_type in {"string", "integer", "boolean", "object"}:
        schema["type"] = field_type
    elif field_type == "enum":
        schema["type"] = "string"
    elif field_type.startswith("list"):
        schema["type"] = "array"
    if "choices" in field:
        schema["enum"] = list(field["choices"])
    if "default" in field:
        schema["default"] = field["default"]
    metadata: JsonObject = {}
    for key in (
        "active_when",
        "choice_source",
        "choice_value",
        "related_commands",
        "compile_path",
    ):
        value = field.get(key)
        if value is not None:
            metadata[key] = require_json_value(
                value,
                label=f"task authoring field metadata {field['path']}.{key}",
            )
    if metadata:
        schema["x-dsctl"] = metadata
    return schema


def _top_level_json_schema_properties(
    fields: Sequence[TaskAuthoringFieldData],
) -> JsonObject:
    properties: JsonObject = {}
    for field in fields:
        path = field["path"]
        if path == "task_params" or path.startswith("task_params."):
            continue
        _insert_authoring_field_schema(properties, path=path, field=field)
    return properties


def _insert_authoring_field_schema(
    properties: JsonObject,
    *,
    path: str,
    field: TaskAuthoringFieldData,
) -> None:
    current = properties
    segments = path.split(".")
    for index, segment in enumerate(segments):
        is_array = segment.endswith("[]")
        name = segment.removesuffix("[]")
        if index == len(segments) - 1:
            current[name] = (
                _array_field_json_schema(field)
                if is_array
                else _field_json_schema(field)
            )
            return
        current = _nested_authoring_properties(
            current,
            name=name,
            is_array=is_array,
            path=path,
        )


def _nested_authoring_properties(
    properties: JsonObject,
    *,
    name: str,
    is_array: bool,
    path: str,
) -> JsonObject:
    existing = properties.get(name)
    if existing is None:
        nested: JsonObject = {"type": "object", "properties": {}}
        node: JsonObject = {"type": "array", "items": nested} if is_array else nested
        properties[name] = node
    elif isinstance(existing, dict):
        node = existing
    else:
        message = f"task authoring schema path conflicts at {path}"
        raise TypeError(message)

    target = node.get("items") if is_array else node
    if not isinstance(target, dict):
        message = f"task authoring schema path has invalid container at {path}"
        raise TypeError(message)
    nested_properties = target.get("properties")
    if not isinstance(nested_properties, dict):
        message = f"task authoring schema path has no object properties at {path}"
        raise TypeError(message)
    return nested_properties


def _array_field_json_schema(field: TaskAuthoringFieldData) -> JsonObject:
    item_schema = _field_json_schema(field)
    array_schema: JsonObject = {
        "type": "array",
        "items": item_schema,
    }
    for key in ("description", "default", "x-dsctl"):
        value = item_schema.pop(key, None)
        if value is not None:
            if key == "default" and isinstance(value, tuple):
                value = list(value)
            array_schema[key] = value
    return array_schema


def _summary_rows(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
    env_file: str | None = None,
) -> list[TaskTypeSummaryRowData]:
    metadata = _task_templates.task_template_metadata(catalog=catalog)[task_type]
    rows: list[TaskTypeSummaryRowData] = [
        {
            "kind": "command",
            "name": "schema",
            "summary": "Bounded field contract, state rules, and value discovery.",
            "command": render_discovery_command(
                "task-type.schema", values={"task_type": task_type}, env_file=env_file
            ),
        },
        {
            "kind": "command",
            "name": "json-schema",
            "summary": "Nested validation schema without repeated authoring metadata.",
            "command": render_discovery_command(
                "task-type.schema",
                values={"task_type": task_type, "json-schema": True},
                env_file=env_file,
            ),
        },
        {
            "kind": "command",
            "name": "compile-mappings",
            "summary": "Authoring paths mapped to DS REST payload paths.",
            "command": render_discovery_command(
                "task-type.schema",
                values={"task_type": task_type, "compile-mappings": True},
                env_file=env_file,
            ),
        },
        {
            "kind": "command",
            "name": "full-schema",
            "summary": "Expanded compatibility contract for audits and generators.",
            "command": render_discovery_command(
                "task-type.schema",
                values={"task_type": task_type, "full": True},
                env_file=env_file,
            ),
        },
        {
            "kind": "command",
            "name": "template",
            "summary": "Default task YAML fragment.",
            "command": render_discovery_command(
                "template.task", values={"task_type": task_type}, env_file=env_file
            ),
        },
        {
            "kind": "command",
            "name": "raw-template",
            "summary": "Copyable YAML fragment without the JSON envelope.",
            "command": render_discovery_command(
                "template.task",
                values={"task_type": task_type, "raw": True},
                env_file=env_file,
            ),
        },
    ]
    rows.extend(
        {
            "kind": "variant",
            "name": variant,
            "summary": metadata["variant_summaries"][variant],
            "command": render_discovery_command(
                "template.task",
                values={"task_type": task_type, "variant": variant},
                env_file=env_file,
            ),
        }
        for variant in metadata["variants"]
    )
    return rows


def _generic_task_warnings(
    task_type: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[list[str], list[JsonObject]]:
    if _task_templates.task_template_kind(task_type, catalog=catalog) != "generic":
        return [], []
    message = (
        f"{task_type} has a generic task_params template; inspect upstream plugin "
        "payloads or an exported workflow before production use."
    )
    limitation = generic_task_runtime_limitation(task_type, catalog=catalog)
    if limitation is not None:
        message += " " + limitation
    return [
        message,
    ], [
        {
            "code": "generic_task_template",
            "task_type": task_type,
            "message": message,
        }
    ]


def _enum_values(enum_type: type[Enum]) -> tuple[str, ...]:
    values: list[str] = []
    for item in enum_type:
        value = getattr(item, "value", item)
        values.append(str(value))
    return tuple(values)


def _profile_enum_values(
    catalog: TaskAuthoringCatalog,
    enum_name: str,
) -> tuple[str, ...]:
    return supported_enum_member_values(
        enum_name,
        ds_version=catalog.profile_version,
    )


def _profile_parameter_data_type_values(
    catalog: TaskAuthoringCatalog,
) -> tuple[str, ...]:
    values = _profile_enum_values(catalog, "data-type")
    if frozenset(values) != catalog.parameter_data_types:
        message = (
            "Task authoring catalog parameter types must match the exact "
            "generated DataType enum"
        )
        raise ValueError(message)
    return values


def _profile_optional_enum_values(
    catalog: TaskAuthoringCatalog,
    enum_name: str,
) -> tuple[str, ...]:
    try:
        return _profile_enum_values(catalog, enum_name)
    except UserInputError as exc:
        if exc.details.get("reason") == "upstream_capability_absent":
            return ()
        raise


__all__ = [
    "TaskAuthoringChoiceSourceData",
    "TaskAuthoringCompileMappingData",
    "TaskAuthoringFieldData",
    "TaskAuthoringStateRuleData",
    "TaskTypeAuthoringSchemaData",
    "TaskTypeSummaryData",
    "require_supported_authoring_task_type",
    "task_type_schema_result",
    "task_type_summary_data",
    "task_type_summary_result",
]
