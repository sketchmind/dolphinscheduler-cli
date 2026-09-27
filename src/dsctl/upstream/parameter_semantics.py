from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.generated.workflow_profiles import WORKFLOW_PROFILE_FACTS
from dsctl.upstream.enums import get_enum_spec
from dsctl.upstream.registry import normalize_version

StartupWire = Literal["absent", "startParams"]
StartupKeyScope = Literal[
    "absent",
    "declared-workflow-globals",
    "any-key-as-varchar",
]
SetValueParser = Literal[
    "absent",
    "dollar-line-start",
    "dollar-or-hash-line-start",
    "dollar-or-hash-stream",
]
DownstreamBinding = Literal["absent", "implicit", "declared-in-only"]
SameNameUpstreamWinner = Literal[
    "n/a",
    "earliest-nonempty",
    "latest-nonempty",
]
NestedWorkflowTaskType = Literal["SUB_PROCESS", "SUB_WORKFLOW"]
NestedTaskLocalParamsRole = Literal[
    "ignored",
    "select-parent-global",
    "select-parent-task-varpool",
]
NestedInputSource = Literal[
    "parent-global",
    "parent-startup",
    "parent-varpool",
]
NestedInputPrecedence = Literal[
    "child-global",
    "parent-global",
    "parent-startup",
    "parent-varpool",
]
NestedOutputContract = Literal[
    "absent",
    "task-local-out",
    "task-local-out-on-cancel",
    "child-global-out",
]
ResolutionSource = Literal[
    "context",
    "startup",
    "local",
    "global",
    "project",
    "builtin",
]


@dataclass(frozen=True, slots=True)
class StartupParamsSemantics:
    """Exact executor/startup-merge behavior for one profile epoch."""

    wire: StartupWire
    key_scope: StartupKeyScope


@dataclass(frozen=True, slots=True)
class TaskOutputSemantics:
    """Exact worker-output parsing and downstream binding behavior."""

    var_pool_transport: bool
    set_value_parser: SetValueParser
    downstream_binding: DownstreamBinding
    same_name_upstream_winner: SameNameUpstreamWinner


@dataclass(frozen=True, slots=True)
class ParameterResolutionSemantics:
    """Exact parameter sources and effective runtime precedence."""

    project_parameters: bool
    effective_precedence: tuple[ResolutionSource, ...]


@dataclass(frozen=True, slots=True)
class NestedWorkflowSemantics:
    """Exact parent-to-child parameter behavior for one nested-workflow epoch."""

    task_type: NestedWorkflowTaskType
    task_local_params_role: NestedTaskLocalParamsRole
    child_input_sources: tuple[NestedInputSource, ...]
    child_input_precedence: tuple[NestedInputPrecedence, ...]
    child_output_contract: NestedOutputContract


@dataclass(frozen=True, slots=True)
class ParameterSemanticsProfile:
    """Reviewed runtime parameter semantics bound to one exact DS release."""

    version: str
    startup: StartupParamsSemantics
    allowed_property_types: frozenset[str]
    output: TaskOutputSemantics
    resolution: ParameterResolutionSemantics
    nested_workflow: NestedWorkflowSemantics


_STARTUP_ABSENT = StartupParamsSemantics("absent", "absent")
_STARTUP_DECLARED_GLOBALS = StartupParamsSemantics(
    "startParams",
    "declared-workflow-globals",
)
_STARTUP_ANY_KEY = StartupParamsSemantics("startParams", "any-key-as-varchar")

_OUTPUT_ABSENT = TaskOutputSemantics(False, "absent", "absent", "n/a")
_OUTPUT_DOLLAR_LINE = TaskOutputSemantics(
    True,
    "dollar-line-start",
    "implicit",
    "earliest-nonempty",
)
_OUTPUT_DOLLAR_HASH_LINE = TaskOutputSemantics(
    True,
    "dollar-or-hash-line-start",
    "implicit",
    "earliest-nonempty",
)
_OUTPUT_STREAM_IMPLICIT = TaskOutputSemantics(
    True,
    "dollar-or-hash-stream",
    "implicit",
    "earliest-nonempty",
)
_OUTPUT_STREAM_DECLARED_IN = TaskOutputSemantics(
    True,
    "dollar-or-hash-stream",
    "declared-in-only",
    "latest-nonempty",
)

_RESOLUTION_139 = ParameterResolutionSemantics(
    False,
    ("local", "global", "builtin"),
)
_RESOLUTION_20 = ParameterResolutionSemantics(
    False,
    ("local", "startup", "global", "context", "builtin"),
)
_RESOLUTION_30 = ParameterResolutionSemantics(
    False,
    ("local", "context", "startup", "global", "builtin"),
)
_RESOLUTION_320 = ParameterResolutionSemantics(
    True,
    ("local", "context", "startup", "global", "project", "builtin"),
)
_RESOLUTION_321 = ParameterResolutionSemantics(
    True,
    ("startup", "local", "context", "global", "project", "builtin"),
)
_RESOLUTION_33 = ParameterResolutionSemantics(
    True,
    ("context", "startup", "local", "global", "project", "builtin"),
)

_NESTED_139 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "ignored",
    ("parent-global",),
    ("child-global", "parent-global"),
    "absent",
)
_NESTED_200 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "select-parent-global",
    ("parent-global",),
    ("child-global", "parent-global"),
    "absent",
)
_NESTED_209 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "select-parent-global",
    ("parent-global",),
    ("parent-global", "child-global"),
    "task-local-out-on-cancel",
)
_NESTED_203 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "select-parent-global",
    ("parent-global",),
    ("parent-global", "child-global"),
    "absent",
)
_NESTED_30 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "select-parent-task-varpool",
    ("parent-global", "parent-varpool"),
    ("parent-varpool", "parent-global", "child-global"),
    "task-local-out",
)
_NESTED_32 = NestedWorkflowSemantics(
    "SUB_PROCESS",
    "select-parent-task-varpool",
    ("parent-global", "parent-varpool"),
    ("parent-varpool", "parent-global", "child-global"),
    "child-global-out",
)
_NESTED_33 = NestedWorkflowSemantics(
    "SUB_WORKFLOW",
    "ignored",
    ("parent-startup",),
    ("parent-startup", "child-global"),
    "child-global-out",
)
_NESTED_34 = NestedWorkflowSemantics(
    "SUB_WORKFLOW",
    "ignored",
    ("parent-global", "parent-startup", "parent-varpool"),
    ("parent-varpool", "parent-startup", "parent-global", "child-global"),
    "child-global-out",
)


@dataclass(frozen=True, slots=True)
class _ProfileEpoch:
    startup: StartupParamsSemantics
    output: TaskOutputSemantics
    resolution: ParameterResolutionSemantics
    nested_workflow: NestedWorkflowSemantics


_EPOCHS: dict[str, _ProfileEpoch] = {
    "1.3.9": _ProfileEpoch(
        _STARTUP_ABSENT,
        _OUTPUT_ABSENT,
        _RESOLUTION_139,
        _NESTED_139,
    ),
    "2.0.0": _ProfileEpoch(
        _STARTUP_DECLARED_GLOBALS,
        _OUTPUT_DOLLAR_LINE,
        _RESOLUTION_20,
        _NESTED_200,
    ),
    **{
        version: _ProfileEpoch(
            _STARTUP_DECLARED_GLOBALS,
            _OUTPUT_DOLLAR_LINE,
            _RESOLUTION_20,
            _NESTED_200,
        )
        for version in ("2.0.1", "2.0.2")
    },
    **{
        version: _ProfileEpoch(
            _STARTUP_DECLARED_GLOBALS,
            _OUTPUT_DOLLAR_LINE,
            _RESOLUTION_20,
            _NESTED_203,
        )
        for version in ("2.0.3", "2.0.4", "2.0.5", "2.0.6")
    },
    "2.0.7": _ProfileEpoch(
        _STARTUP_DECLARED_GLOBALS,
        _OUTPUT_DOLLAR_LINE,
        _RESOLUTION_20,
        _NESTED_209,
    ),
    "2.0.8": _ProfileEpoch(
        _STARTUP_ANY_KEY,
        _OUTPUT_DOLLAR_LINE,
        _RESOLUTION_20,
        _NESTED_209,
    ),
    "2.0.9": _ProfileEpoch(
        _STARTUP_ANY_KEY,
        _OUTPUT_DOLLAR_LINE,
        _RESOLUTION_20,
        _NESTED_209,
    ),
    **{
        version: _ProfileEpoch(
            _STARTUP_ANY_KEY,
            _OUTPUT_DOLLAR_HASH_LINE,
            _RESOLUTION_30,
            _NESTED_30,
        )
        for version in (
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
        )
    },
    "3.2.0": _ProfileEpoch(
        _STARTUP_ANY_KEY,
        _OUTPUT_DOLLAR_HASH_LINE,
        _RESOLUTION_320,
        _NESTED_32,
    ),
    **{
        version: _ProfileEpoch(
            _STARTUP_ANY_KEY,
            _OUTPUT_STREAM_IMPLICIT,
            _RESOLUTION_321,
            _NESTED_32,
        )
        for version in ("3.2.1", "3.2.2")
    },
    **{
        version: _ProfileEpoch(
            _STARTUP_ANY_KEY,
            _OUTPUT_STREAM_DECLARED_IN,
            _RESOLUTION_33,
            _NESTED_33,
        )
        for version in ("3.3.1", "3.3.2")
    },
    **{
        version: _ProfileEpoch(
            _STARTUP_ANY_KEY,
            _OUTPUT_STREAM_DECLARED_IN,
            _RESOLUTION_33,
            _NESTED_34,
        )
        for version in ("3.4.0", "3.4.1", "3.4.2", "3.4.3")
    },
}

if tuple(_EPOCHS) != TARGET_DS_VERSIONS:
    message = "Parameter semantics must cover every exact DS profile in order"
    raise RuntimeError(message)


@cache
def get_parameter_semantics(version: str) -> ParameterSemanticsProfile:
    """Return reviewed parameter semantics for one exact DS release."""
    normalized = normalize_version(version)
    epoch = _EPOCHS.get(normalized)
    if epoch is None:
        supported = ", ".join(TARGET_DS_VERSIONS)
        message = (
            f"No exact parameter semantics profile for {version!r}; "
            f"supported versions: {supported}"
        )
        raise ValueError(message)
    generated_start_params = bool(WORKFLOW_PROFILE_FACTS[normalized]["start_params"])
    reviewed_start_params = epoch.startup.wire == "startParams"
    if generated_start_params != reviewed_start_params:
        message = f"Parameter semantics startParams review is stale for DS {normalized}"
        raise RuntimeError(message)
    enum_spec = get_enum_spec(normalized, "data-type")
    if enum_spec is None:
        message = f"Exact DolphinScheduler {normalized} data-type enum is missing"
        raise RuntimeError(message)
    return ParameterSemanticsProfile(
        version=normalized,
        startup=epoch.startup,
        allowed_property_types=frozenset(
            str(member.value) for member in enum_spec.members
        ),
        output=epoch.output,
        resolution=epoch.resolution,
        nested_workflow=epoch.nested_workflow,
    )


__all__ = [
    "NestedWorkflowSemantics",
    "ParameterResolutionSemantics",
    "ParameterSemanticsProfile",
    "StartupParamsSemantics",
    "TaskOutputSemantics",
    "get_parameter_semantics",
]
