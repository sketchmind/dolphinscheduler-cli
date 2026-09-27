"""Canonical invocation facts shared by CLI parsing, help, schema, and rendering."""

from dsctl._command_catalog import COMMANDS
from dsctl._command_contract_model import (
    LOCAL_CONFIGURATION_WRITE,
    MISSING_DEFAULT,
    NO_EFFECTS,
    REMOTE_READ,
    REMOTE_READ_LOCAL_FILE_WRITE,
    REMOTE_WRITE,
    REMOTE_WRITE_DRY_RUN_READ,
    CommandBindingError,
    CommandCatalog,
    CommandContract,
    CommandContractError,
    CommandDryRunEffects,
    CommandEffects,
    GlobalOptionContract,
    InputContract,
    LocalEffect,
    MissingDefault,
    PathRules,
    RemoteEffect,
    ValueResolution,
    semantic_value_name,
)

_GLOBAL_OPTIONS = (
    GlobalOptionContract(
        input=InputContract(
            name="context",
            parameter_name="context_name",
            kind="option",
            value_type="string",
            description="Select a saved context; mutually exclusive with --env-file.",
            parse_default=None,
            value_name="NAME",
        ),
        arity=1,
        example="dsctl --context production project list",
        schema_order=-1,
        invocation_order=-1,
    ),
    GlobalOptionContract(
        input=InputContract(
            name="env-file",
            kind="option",
            value_type="path",
            description=(
                "Select an isolated dotenv profile; mutually exclusive with --context."
            ),
            parse_default=None,
            value_name="PATH",
            path_rules=PathRules(
                exists=True,
                file_okay=True,
                dir_okay=False,
                readable=True,
                resolve_path=True,
            ),
        ),
        arity=1,
        example="dsctl --env-file cluster.env context",
        schema_order=0,
        invocation_order=0,
    ),
    GlobalOptionContract(
        input=InputContract(
            name="format",
            parameter_name="output_format",
            kind="option",
            value_type="string",
            description="Result format: json, json-compact, table or tsv.",
            parse_default="json",
            choices=("json", "json-compact", "table", "tsv"),
            normalization="lowercase",
            value_name="FORMAT",
        ),
        arity=1,
        example="dsctl --format table project list",
        schema_order=1,
        invocation_order=1,
    ),
    GlobalOptionContract(
        input=InputContract(
            name="columns",
            kind="option",
            value_type="string",
            description="Select comma-separated data fields, e.g. id,name,state.",
            parse_default=None,
            value_name="FIELDS",
        ),
        arity=1,
        example=(
            "dsctl --columns id,name,state workflow-instance list --project etl-prod"
        ),
        schema_order=2,
        invocation_order=3,
    ),
)

COMMAND_CATALOG = CommandCatalog(global_options=_GLOBAL_OPTIONS, commands=COMMANDS)

__all__ = [
    "COMMAND_CATALOG",
    "LOCAL_CONFIGURATION_WRITE",
    "MISSING_DEFAULT",
    "NO_EFFECTS",
    "REMOTE_READ",
    "REMOTE_READ_LOCAL_FILE_WRITE",
    "REMOTE_WRITE",
    "REMOTE_WRITE_DRY_RUN_READ",
    "CommandBindingError",
    "CommandCatalog",
    "CommandContract",
    "CommandContractError",
    "CommandDryRunEffects",
    "CommandEffects",
    "GlobalOptionContract",
    "InputContract",
    "LocalEffect",
    "MissingDefault",
    "PathRules",
    "RemoteEffect",
    "ValueResolution",
    "semantic_value_name",
]
