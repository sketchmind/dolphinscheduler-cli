"""Admit bounded reads without asserting an exact server release.

Candidate membership comes from discovery, never from this policy. Each action
has an explicit transitive wire closure; equal wire shapes alone do not confer
support. Reviewed exact action decisions, selectors, projection recipes and enum
members must agree as well. Execution still uses an unchanged exact compiler
profile, whose version is an internal artifact selector, not server identity.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, NoReturn

from dsctl.client import ReadExecutionPolicy
from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS, VERSION_PROFILES
from dsctl.upstream._compiled_project import PROJECT_PROGRAMS
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.code_native_reads import _READ_SPEC_BY_VERSION
from dsctl.upstream.enums import get_enum_spec
from dsctl.upstream.instance_time_filters import (
    InstanceTimeFilterContract,
    instance_time_filter_contract,
)
from dsctl.upstream.users import _USER_PROGRAMS, _recipe_for_profile

if TYPE_CHECKING:
    from dsctl.generated.runtime_instance_profiles import RuntimeInstanceProfile
    from dsctl.support.json_types import JsonValue
    from dsctl.upstream._compiled_project import ProjectPrimitive
    from dsctl.upstream._compiled_workflow_runtime import WorkflowPrimitive
    from dsctl.upstream.code_native_reads import _ReadVersionSpec
    from dsctl.upstream.enums import EnumMemberSpec
    from dsctl.upstream.wire import (
        CompiledWireProgram,
        WireExecutionMode,
        WireResultEnvelope,
    )


# Explicit source review boundary, independent of shared codec/recipe identities.
# API Status.java supplies the read permission/not-found meanings; source-era
# renames remain independently reviewed in the exact action ledger. In modern
# releases codes 10008, 10018, 10020, 10190, 10203, 30001, 30002, 50001 and 50003
# retain their read meanings across the process->workflow rename.
# Exact controller/type/selector evidence is in
# VERSION_PROFILES[*].build_decisions; runtime_instances.py, code_native_reads.py
# and DefinitionReads own the reviewed local projections and scoped lookups.
# No future tag, authoring action or implicit version range is admitted. Legacy
# identities and projections have a separate signature, even for equal schemas.
_REVIEWED_VERSIONS = frozenset(
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
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)

# Reviewed Status.java subsets, read directly in each exact source checkout:
# dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/enums/Status.java.
# PROJECT_NOT_FOUNT -> PROJECT_NOT_FOUND and PROCESS_* -> WORKFLOW_* are name
# changes with unchanged meanings. Additional status members remain separate
# closures: sharing codecs never silently fills these historical absences.
_ERROR_CODE_CLOSURES = {
    version: codes
    for versions, codes in (
        (
            (
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
            ),
            (10001, 10008, 10018, 10020, 10103, 30001, 30002, 50001, 50003),
        ),
        (
            (
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
            ),
            (10001, 10008, 10018, 10020, 10103, 10190, 30001, 30002, 50001, 50003),
        ),
        (
            (
                "3.2.0",
                "3.2.1",
                "3.2.2",
                "3.3.1",
                "3.3.2",
                "3.4.0",
                "3.4.1",
                "3.4.2",
                "3.4.3",
            ),
            (
                10001,
                10008,
                10018,
                10020,
                10103,
                10190,
                10203,
                30001,
                30002,
                50001,
                50003,
            ),
        ),
    )
    for version in versions
}


@dataclass(frozen=True)
class ReadCompatibilityPlan:
    """One invocation's internal execution selection and enforced read scope."""

    candidate_versions: tuple[str, ...]
    action: str
    execution_version: str
    policy: ReadExecutionPolicy


@dataclass(frozen=True)
class _ReadReview:
    project: tuple[ProjectPrimitive, ...] = ()
    workflow: tuple[WorkflowPrimitive, ...] = ()
    dependencies: tuple[str, ...] = ()
    projection: Literal[
        "project", "definition", "schedule", "runtime", "log", "identity"
    ] = "project"


@dataclass(frozen=True)
class _ProgramSignature:
    source_operation: str
    execution_mode: WireExecutionMode
    result_envelope: WireResultEnvelope
    request_schema: str
    response: str
    response_schema: str | None
    method: str
    path: str
    channel: str
    path_encoding: str | None
    field_bindings: tuple[tuple[str, str], ...]
    path_fields: tuple[str, ...]
    file_fields: tuple[str, ...]
    response_projection: str
    response_transport: str


@dataclass(frozen=True)
class _SemanticSignature:
    operation: str
    selector_json: str
    effective_wire: str
    consumed_projection: str
    preservation: str


@dataclass(frozen=True)
class _EnumSignature:
    value_type: str
    members: tuple[EnumMemberSpec, ...]


@dataclass(frozen=True)
class _ProjectionSignature:
    identity: str | None = None
    has_time_zone: bool | None = None
    log_epoch: str | None = None
    log_first_page_header: bool | None = None
    read_spec: _ReadVersionSpec | None = None
    runtime: RuntimeInstanceProfile | None = None
    schedule_recipe: str | None = None
    runtime_enums: tuple[_EnumSignature | None, ...] = ()


@dataclass(frozen=True)
class _ReadSignature:
    programs: tuple[_ProgramSignature, ...]
    semantics: tuple[_SemanticSignature, ...]
    projection: _ProjectionSignature
    error_codes: tuple[int, ...]
    time_filter: InstanceTimeFilterContract | None


_PROJECT_LOOKUP: tuple[ProjectPrimitive, ...] = ("page", "get")
_DEFINITION_LOOKUP: tuple[WorkflowPrimitive, ...] = (
    "definition_refs",
    "definition_get",
)
_PROJECT_ACTIONS = ("project.list", "project.get")
_DEFINITION_ACTIONS = (*_PROJECT_ACTIONS, "workflow.get")
_REVIEWS = {
    "project.list": _ReadReview(project=("page",)),
    "project.get": _ReadReview(project=_PROJECT_LOOKUP, dependencies=("project.list",)),
    "workflow.list": _ReadReview(
        _PROJECT_LOOKUP, ("definition_page",), _PROJECT_ACTIONS, "definition"
    ),
    "workflow.get": _ReadReview(
        _PROJECT_LOOKUP,
        (*_DEFINITION_LOOKUP, "schedule_page"),
        (*_PROJECT_ACTIONS, "schedule.list"),
        "definition",
    ),
    "schedule.list": _ReadReview(
        _PROJECT_LOOKUP,
        (*_DEFINITION_LOOKUP, "schedule_page"),
        _DEFINITION_ACTIONS,
        "schedule",
    ),
    "schedule.get": _ReadReview(
        ("page",), ("schedule_page",), ("project.list", "schedule.list"), "schedule"
    ),
    "workflow-instance.list": _ReadReview(
        _PROJECT_LOOKUP,
        (*_DEFINITION_LOOKUP, "instance_page"),
        _DEFINITION_ACTIONS,
        "runtime",
    ),
    "workflow-instance.get": _ReadReview(
        _PROJECT_LOOKUP, ("instance_get",), _PROJECT_ACTIONS, "runtime"
    ),
    "workflow-instance.digest": _ReadReview(
        _PROJECT_LOOKUP,
        ("instance_get", "task_instance_page"),
        (*_PROJECT_ACTIONS, "workflow-instance.get", "task-instance.list"),
        "runtime",
    ),
    "task-instance.list": _ReadReview(
        _PROJECT_LOOKUP,
        ("instance_get", "task_instance_page"),
        (*_PROJECT_ACTIONS, "workflow-instance.get"),
        "runtime",
    ),
    "task-instance.get": _ReadReview(
        _PROJECT_LOOKUP,
        ("instance_get", "task_instance_page"),
        (*_PROJECT_ACTIONS, "workflow-instance.get", "task-instance.list"),
        "runtime",
    ),
    "task-instance.log": _ReadReview(workflow=("task_log",), projection="log"),
    "doctor": _ReadReview(projection="identity"),
}
READ_COMPATIBILITY_ACTIONS = frozenset(_REVIEWS)


def build_read_compatibility_plan(
    candidate_versions: tuple[str, ...],
    *,
    action: str,
    compatible_operations: tuple[str, ...],
) -> ReadCompatibilityPlan:
    """Require independently reviewed, equal closures across every candidate."""
    review = _REVIEWS.get(action)
    if review is None:
        _reject(action, candidate_versions, "action_not_reviewed_for_compatibility")
    if not candidate_versions or not set(candidate_versions) <= _REVIEWED_VERSIONS:
        _reject(action, candidate_versions, "candidate_not_reviewed_for_compatibility")
    versions = tuple(v for v in TARGET_DS_VERSIONS if v in candidate_versions)
    signatures = [_signature(version, action, review) for version in versions]
    if any(signature != signatures[0] for signature in signatures[1:]):
        _reject(action, versions, "read_contracts_differ")
    execution_version = versions[0]
    programs = _programs(execution_version, review)
    missing = sorted(
        {program.source_operation for program in programs} - set(compatible_operations)
    )
    if missing:
        message = "The server description does not prove this read operation closure."
        raise UnsupportedFeatureError(
            message,
            details={
                "action": action,
                "reason": "read_operation_not_observed",
                "missing_operations": missing,
                "candidate_versions": list(versions),
            },
            suggestion=(
                "Set DS_VERSION to the verified exact server version before retrying."
            ),
        )
    return ReadCompatibilityPlan(
        candidate_versions=versions,
        action=action,
        execution_version=execution_version,
        policy=ReadExecutionPolicy(
            action=action,
            program_fingerprints=frozenset(program.fingerprint for program in programs),
        ),
    )


def available_read_actions(
    candidate_versions: tuple[str, ...],
    *,
    compatible_operations: tuple[str, ...],
) -> frozenset[str]:
    """Report only fully admitted actions; broken compiler invariants still fail."""
    admitted = set()
    for action in READ_COMPATIBILITY_ACTIONS:
        try:
            build_read_compatibility_plan(
                candidate_versions,
                action=action,
                compatible_operations=compatible_operations,
            )
        except UnsupportedFeatureError:
            continue
        admitted.add(action)
    return frozenset(admitted)


def _signature(version: str, action: str, review: _ReadReview) -> _ReadSignature:
    programs = _programs(version, review)
    if any(program.execution_mode.mutates for program in programs):
        _reject(action, (version,), "read_closure_contains_mutation")
    return _ReadSignature(
        tuple(_program_signature(program) for program in programs),
        _semantic_signature(version, (action, *review.dependencies)),
        _projection_signature(version, review),
        _ERROR_CODE_CLOSURES[version],
        instance_time_filter_contract(version, "workflow-instance.list")
        if action == "workflow-instance.list"
        else instance_time_filter_contract(version, "task-instance.list")
        if action == "task-instance.list"
        else None,
    )


def _programs(version: str, review: _ReadReview) -> tuple[CompiledWireProgram, ...]:
    result = tuple(PROJECT_PROGRAMS.profile(version).program(p) for p in review.project)
    result += tuple(
        WORKFLOW_PROGRAMS.profile(version).program(p) for p in review.workflow
    )
    if review.projection == "identity":
        result += (_USER_PROGRAMS.profile(version).program("current"),)
    # Legacy workflow name resolution exhausts pages instead of the code-native
    # reference inventory. Pre-3.2 schedule reads also enumerate definitions to
    # supply the controller's required per-workflow schedule filter.
    if (version == "1.3.9" and "definition_refs" in review.workflow) or (
        review.projection == "schedule"
        and (
            version == "1.3.9"
            or _READ_SPEC_BY_VERSION[version].projection.schedule_filter_required
        )
    ):
        profile = WORKFLOW_PROGRAMS.profile(version)
        result += tuple(
            profile.program(p) for p in ("definition_page", "definition_refs")
        )
    return result


def _program_signature(program: CompiledWireProgram) -> _ProgramSignature:
    codec = program.codec
    return _ProgramSignature(
        program.source_operation,
        program.execution_mode,
        program.result_envelope,
        # The complete executable encoder is represented by its schema digest
        # and fields below. Source-bound codec digests may differ even when
        # those executable records and the independently reviewed semantics do
        # not. Exact fingerprints remain enforced for dispatch, below this gate.
        codec.request_schema_fingerprint,
        codec.response_fingerprint,
        codec.response_schema_fingerprint,
        codec.method,
        codec.path,
        codec.channel,
        codec.path_encoding,
        codec.field_bindings,
        codec.path_fields,
        codec.file_fields,
        codec.response_projection,
        codec.response_transport,
    )


def _semantic_signature(
    version: str, actions: tuple[str, ...]
) -> tuple[_SemanticSignature, ...]:
    profile = _mapping(VERSION_PROFILES[version])
    capabilities = _mapping(profile["actions"])
    decisions = _mapping(profile["build_decisions"])
    signature: list[_SemanticSignature] = []
    for action in actions:
        capability = _mapping(capabilities[action])
        if capability.get("availability") != "supported":
            _reject(action, (version,), "candidate_action_unsupported")
        matches = [
            (name, _mapping(value))
            for name, value in decisions.items()
            if _mapping(value).get("stable_action") == action
        ]
        if not matches:
            _reject(action, (version,), "read_semantic_evidence_missing")
        for name, decision in matches:
            if decision.get("build_status") != "accepted" or not decision.get(
                "evidence_sources"
            ):
                _reject(action, (version,), "read_semantic_evidence_missing")
            fingerprints = _mapping(decision["fingerprints"])
            signature.append(
                _SemanticSignature(
                    name,
                    # Preserve the complete generated selector boundary value,
                    # including future fields, without leaking JSON into runtime
                    # policy state. Object key order never changes semantics.
                    json.dumps(decision["selector_semantics"], sort_keys=True),
                    _text(fingerprints["effective_wire"]),
                    _text(fingerprints["consumed_projection"]),
                    _text(fingerprints["preservation"]),
                )
            )
    return tuple(signature)


def _projection_signature(version: str, review: _ReadReview) -> _ProjectionSignature:
    kind = review.projection
    if kind == "project":
        return _ProjectionSignature(
            identity="id-native" if version == "1.3.9" else "code-native"
        )
    if kind == "identity":
        return _ProjectionSignature(
            has_time_zone=_recipe_for_profile(
                _USER_PROGRAMS.profile(version)
            ).has_time_zone
        )
    # Environment execution policy does not change a read request or decoder.
    recipe = replace(
        RUNTIME_INSTANCE_PROFILES[version],
        schedule_forwards_environment=False,
        task_inherits_workflow_environment=False,
    )
    if kind == "log":
        return _ProjectionSignature(
            log_epoch=recipe.log_epoch,
            log_first_page_header=recipe.log_first_page_header,
        )
    if version == "1.3.9":
        return _ProjectionSignature(identity="legacy-id-native-read-v1", runtime=recipe)
    read_spec = _READ_SPEC_BY_VERSION[version]
    if kind == "schedule":
        return _ProjectionSignature(
            read_spec=read_spec,
            schedule_recipe=WORKFLOW_PROGRAMS.profile(version).recipe_id,
        )
    if kind == "runtime":
        return _ProjectionSignature(
            read_spec=read_spec,
            runtime=recipe,
            runtime_enums=_runtime_enum_signature(version),
        )
    return _ProjectionSignature(read_spec=read_spec)


def _runtime_enum_signature(version: str) -> tuple[_EnumSignature | None, ...]:
    enums: list[_EnumSignature | None] = []
    for name in (
        "workflow-execution-status",
        "task-execution-status",
        "task-execute-type",
    ):
        if (
            name == "task-execute-type"
            and not RUNTIME_INSTANCE_PROFILES[version].has_task_execute_type
        ):
            enums.append(None)
            continue
        spec = get_enum_spec(version, name)
        if spec is None and name in {
            "workflow-execution-status",
            "task-execution-status",
        }:
            spec = get_enum_spec(version, "execution-status")
        if spec is None:
            _reject(name, (version,), "read_enum_evidence_missing")
        enums.append(_EnumSignature(spec.value_type, spec.members))
    return tuple(enums)


def _mapping(value: JsonValue) -> Mapping[str, JsonValue]:
    """Read an object only at the generated JSON profile ledger boundary."""
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        message = "Read compatibility requires a complete exact profile ledger"
        raise TypeError(message)
    return value


def _text(value: JsonValue) -> str:
    if not isinstance(value, str) or not value:
        message = "Read compatibility requires exact profile fingerprint text"
        raise TypeError(message)
    return value


def _reject(action: str, versions: tuple[str, ...], reason: str) -> NoReturn:
    message = "The discovered contracts do not admit this read compatibility action."
    raise UnsupportedFeatureError(
        message,
        details={
            "action": action,
            "candidate_versions": list(versions),
            "reason": reason,
        },
        suggestion=(
            "Set DS_VERSION to the verified exact server version before retrying."
        ),
    )


__all__ = [
    "READ_COMPATIBILITY_ACTIONS",
    "ReadCompatibilityPlan",
    "available_read_actions",
    "build_read_compatibility_plan",
]
