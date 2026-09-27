"""Validate ordered scenario proofs, negative paths, mutation readback and cleanup."""

from __future__ import annotations

import re

from live_gate.conformance_evidence.types import (
    _TaskDefinitionCleanupContract,
    _TraceEntry,
)
from live_gate.conformance_evidence.values import (
    _text_sequence,
)
from live_gate.evidence_values import required_bool as _boolean
from live_gate.evidence_values import required_int as _integer
from live_gate.evidence_values import required_list as _sequence
from live_gate.evidence_values import required_mapping_view as _mapping
from live_gate.evidence_values import required_text as _text

_ARGV_FLAG = re.compile(r"--[a-z][a-z0-9-]*\Z")


_ARGV_METAVAR = re.compile(r"[A-Z][A-Z0-9_]*\Z")


def _validate_operation_trace(
    value: object,
    *,
    bundle_name: str,
    ds_version: str,
    required_actions: tuple[str, ...],
    required_outcomes: tuple[str, ...],
    task_cleanup: _TaskDefinitionCleanupContract,
    dependency_update_upstream_limited_versions: frozenset[str],
) -> None:
    entries = tuple(
        _trace_entry(raw_entry)
        for raw_entry in _sequence(value, label="operation_trace")
    )
    if not entries:
        message = "operation_trace must not be empty"
        raise ValueError(message)
    if tuple(entry.sequence for entry in entries) != tuple(range(1, len(entries) + 1)):
        message = "operation trace sequence must be contiguous and ordered"
        raise ValueError(message)
    _validate_trace_preflight(entries, required_actions=required_actions)
    requires_task_reconciliation = (
        bundle_name == "full_core/v1"
        and ds_version in task_cleanup.full_core_reconciliation_versions
    )
    auxiliary_actions = (
        frozenset(
            {
                task_cleanup.semantic_operation.removesuffix(".cleanup") + ".prove",
                task_cleanup.semantic_operation,
            }
        )
        if requires_task_reconciliation
        else frozenset()
    )
    trace_action_domain = {
        *required_actions,
        *auxiliary_actions,
        "version",
        "capabilities",
    }
    unexpected_actions = sorted(
        {entry.action for entry in entries} - trace_action_domain
    )
    if unexpected_actions:
        message = (
            "operation trace contains actions outside the named bundle: "
            + ", ".join(unexpected_actions)
        )
        raise ValueError(message)
    successful_actions = {entry.action for entry in entries if entry.ok}
    missing_actions = sorted(set(required_actions) - successful_actions)
    if missing_actions:
        message = (
            "operation trace is missing successful required actions: "
            + ", ".join(missing_actions)
        )
        raise ValueError(message)
    observed_outcomes = tuple(
        outcome for entry in entries for outcome in entry.outcomes
    )
    if observed_outcomes != required_outcomes:
        message = "operation trace outcomes differ from the scenario"
        raise ValueError(message)
    stable_negative_outcomes = {
        "conflict": any(
            not entry.ok
            and entry.action == "project.create"
            and entry.error_type == "conflict"
            and {"stable-conflict", "actionable-suggestion"} <= set(entry.assertions)
            for entry in entries
        ),
        "not_found": any(
            not entry.ok
            and entry.action == "project.get"
            and entry.error_type == "not_found"
            and "stable-not-found" in entry.assertions
            for entry in entries
        ),
    }
    missing_negatives = sorted(
        name for name, observed in stable_negative_outcomes.items() if not observed
    )
    if missing_negatives:
        message = (
            "operation trace lacks required stable negative outcomes: "
            + ", ".join(missing_negatives)
        )
        raise ValueError(message)
    if bundle_name == "full_core/v1":
        _validate_full_scenario_trace(
            entries,
            ds_version=ds_version,
            scenario_start=len(required_actions) + 1,
            task_cleanup=(task_cleanup if requires_task_reconciliation else None),
            dependency_update_upstream_limited=(
                ds_version in dependency_update_upstream_limited_versions
            ),
        )


def _validate_full_scenario_trace(
    entries: tuple[_TraceEntry, ...],
    *,
    ds_version: str,
    scenario_start: int,
    task_cleanup: _TaskDefinitionCleanupContract | None,
    dependency_update_upstream_limited: bool,
) -> None:
    task_reconciliation_strategy = (
        None if task_cleanup is None else task_cleanup.strategies.get(ds_version)
    )
    if task_cleanup is not None and task_reconciliation_strategy is None:
        message = "full scenario task reconciliation strategy is missing"
        raise ValueError(message)
    workflow_list_shape = (
        "workflow list --project PROJECT --search WORKFLOW_NAME "
        "--page-no PAGE_NO --page-size PAGE_SIZE"
    )
    _validate_full_failed_trace_entries(
        entries,
        dependency_update_upstream_limited=dependency_update_upstream_limited,
    )
    _validate_full_mutation_stages(entries)
    cursor = _validate_full_pre_workflow_trace(entries, start=scenario_start)
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="pre-create workflow absence",
        action="workflow.list",
        argv_shape=workflow_list_shape,
        assertions=("pre-create-workflow-absent",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow create dry run",
        action="workflow.create",
        argv_shape="workflow create --file WORKFLOW_FILE --project PROJECT --dry-run",
        assertions=("installed-exact-create-plan", "dry-run-no-request-sent"),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow create dry-run absence",
        action="workflow.list",
        argv_shape=workflow_list_shape,
        assertions=("create-dry-run-left-workflow-absent",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow create apply",
        action="workflow.create",
        argv_shape="workflow create --file WORKFLOW_FILE --project PROJECT",
        assertions=(
            "gate-owned-workflow-created",
            "native-workflow-code-captured",
            "workflow-offline",
        ),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="created workflow list identity",
        action="workflow.list",
        argv_shape=workflow_list_shape,
        assertions=("gate-owned-workflow-listed", "native-workflow-code-matched"),
    )
    for selector_kind in ("name", "native"):
        cursor = _require_full_trace_stage(
            entries,
            start=cursor,
            label=f"created workflow {selector_kind} identity",
            action="workflow.get",
            argv_shape="workflow get WORKFLOW --project PROJECT",
            assertions=(
                "gate-owned-workflow-matched",
                "native-workflow-code-matched",
                "workflow-offline",
            ),
        )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="created workflow baseline",
        ds_version=ds_version,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow duplicate conflict",
        action="workflow.create",
        argv_shape="workflow create --file WORKFLOW_FILE --project PROJECT",
        ok=False,
        error_type="conflict",
        assertions=("stable-conflict",),
    )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="duplicate workflow conflict",
        ds_version=ds_version,
        describe_assertions=("duplicate-workflow-state-unchanged",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="missing workflow negative",
        action="workflow.get",
        argv_shape="workflow get MISSING_WORKFLOW --project PROJECT",
        ok=False,
        error_type="not_found",
        assertions=("stable-not-found", "missing-workflow-nonmutating"),
    )
    if dependency_update_upstream_limited:
        cursor = _require_full_trace_stage(
            entries,
            start=cursor,
            label="upstream-limited dependency rejection",
            action="task.update",
            argv_shape=(
                "task update TASK --project PROJECT --workflow WORKFLOW "
                "--set DEPENDS_ON --dry-run"
            ),
            ok=False,
            error_type="unsupported_feature",
            assertions=(
                "dependency-update-upstream-limited",
                "pre-io-no-mutation",
            ),
        )
        cursor = _require_full_dag_readback(
            entries,
            start=cursor,
            label="rejected dependency update",
            ds_version=ds_version,
        )
    elif any(
        "dependency-update-upstream-limited" in entry.assertions for entry in entries
    ):
        message = "full scenario trace contains an unsupported dependency claim"
        raise ValueError(message)
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="task update dry run",
        action="task.update",
        argv_shape=(
            "task update TASK --project PROJECT --workflow WORKFLOW "
            "--set COMMAND --dry-run"
        ),
        assertions=("installed-exact-task-update-plan", "dry-run-no-request-sent"),
    )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="task update dry-run nonmutation",
        ds_version=ds_version,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="task update apply",
        action="task.update",
        argv_shape=(
            "task update TASK --project PROJECT --workflow WORKFLOW --set COMMAND"
        ),
        assertions=(
            "command-updated-exactly",
            (
                "native-task-id-preserved"
                if ds_version == "1.3.9"
                else "task-version-advanced"
            ),
            "non-owned-task-state-preserved",
        ),
    )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="task update apply readback",
        ds_version=ds_version,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow edit dry run",
        action="workflow.edit",
        argv_shape=(
            "workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE --dry-run"
        ),
        assertions=(
            "installed-exact-workflow-edit-plan",
            "description-only-diff",
            "dry-run-no-request-sent",
        ),
    )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="workflow edit dry-run preservation",
        ds_version=ds_version,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow edit apply",
        action="workflow.edit",
        argv_shape="workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE",
        assertions=("description-updated-exactly",),
    )
    cursor = _require_full_dag_readback(
        entries,
        start=cursor,
        label="workflow edit apply preservation",
        ds_version=ds_version,
    )
    cursor = _require_full_task_reconciliation_proof(
        entries,
        start=cursor,
        task_cleanup=task_cleanup,
        strategy=task_reconciliation_strategy,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow delete",
        action="workflow.delete",
        argv_shape="workflow delete WORKFLOW --project PROJECT --force",
        assertions=("gate-owned-workflow-deleted",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="workflow absence",
        action="workflow.list",
        argv_shape=workflow_list_shape,
        assertions=("workflow-leftovers-zero",),
        outcomes=(
            "gate-owned-workflow-round-trip",
            "workflow-dag-cross-checked",
            "task-update-round-trip",
            "workflow-edit-round-trip",
        ),
    )
    cursor = _require_full_task_reconciliation_cleanup(
        entries,
        start=cursor,
        ds_version=ds_version,
        task_cleanup=task_cleanup,
        strategy=task_reconciliation_strategy,
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="project delete after workflow",
        action="project.delete",
        argv_shape="project delete PROJECT --force",
        assertions=("gate-owned-project-deleted-after-workflow",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="deleted project absence",
        action="project.get",
        argv_shape="project get PROJECT",
        ok=False,
        error_type="not_found",
        assertions=("stable-not-found", "deleted-project-absent"),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="full cleanup zero",
        action="project.list",
        argv_shape=(
            "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=("bounded-exact-name-search",),
        outcomes=("full-cleanup-zero", "negative-errors-translated"),
    )
    cursor = _require_full_external_fixture_trace(
        entries,
        start=cursor,
        observed_outcome=None,
        label="post-scenario external fixture",
    )
    if cursor != len(entries):
        message = "full scenario trace contains entries outside its exact grammar"
        raise ValueError(message)


def _require_full_task_reconciliation_proof(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
    task_cleanup: _TaskDefinitionCleanupContract | None,
    strategy: str | None,
) -> int:
    if strategy is None:
        return start
    if task_cleanup is None:
        message = f"{strategy} task reconciliation contract is missing"
        raise ValueError(message)
    assertions: tuple[str, ...]
    if strategy == "direct-delete":
        label = "project-wide task-definition proof"
        assertions = (
            "project-wide-task-inventory-proven",
            "exact-two-owned-tasks",
            "zero-remote-mutations",
        )
    elif strategy == "workflow-cascade-proof-only":
        label = "workflow-bound task-definition proof"
        assertions = (
            "workflow-bound-task-inventory-proven",
            "batch-and-stream-inventories-proven",
            "task-history-lineage-proven",
            "exact-two-owned-tasks",
            "zero-remote-mutations",
        )
    else:
        message = f"task reconciliation strategy {strategy!r} is not recognized"
        raise ValueError(message)
    return _require_full_trace_stage(
        entries,
        start=start,
        label=label,
        action=task_cleanup.semantic_operation.removesuffix(".cleanup") + ".prove",
        argv_shape="release-gate task-definition prove",
        assertions=assertions,
    )


def _require_full_task_reconciliation_cleanup(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
    ds_version: str,
    task_cleanup: _TaskDefinitionCleanupContract | None,
    strategy: str | None,
) -> int:
    if strategy is None:
        return _require_full_task_absence(entries, start=start)
    if task_cleanup is None:
        message = f"{strategy} task reconciliation contract is missing"
        raise ValueError(message)
    if strategy == "direct-delete":
        assertions: tuple[str, ...]
        if ds_version in task_cleanup.pre_delete_release_versions:
            assertions = (
                "exact-two-online-tasks-released",
                "exact-two-owned-tasks-deleted",
                "fresh-zero-reconciliation",
                "remote-mutations-four",
            )
        else:
            assertions = (
                "exact-two-owned-tasks-deleted",
                "fresh-zero-reconciliation",
                "remote-mutations-two",
            )
        return _require_full_trace_stage(
            entries,
            start=start,
            label="project-wide task-definition cleanup",
            action=task_cleanup.semantic_operation,
            argv_shape="release-gate task-definition cleanup",
            assertions=assertions,
        )
    if strategy != "workflow-cascade-proof-only":
        message = f"task reconciliation strategy {strategy!r} is not recognized"
        raise ValueError(message)
    cursor = _require_full_trace_stage(
        entries,
        start=start,
        label="post-cascade task-definition reconciliation",
        action=task_cleanup.semantic_operation,
        argv_shape="release-gate task-definition cleanup",
        assertions=(
            "post-cascade-zero-reconciliation",
            "observed-zero-tasks",
            "deleted-zero-tasks",
            "remaining-zero-tasks",
            "zero-remote-mutations",
        ),
    )
    return _require_full_task_absence(entries, start=cursor)


def _validate_full_failed_trace_entries(
    entries: tuple[_TraceEntry, ...],
    *,
    dependency_update_upstream_limited: bool,
) -> None:
    allowed = {
        (
            "project.create",
            "project create --name PROJECT_NAME --description DESCRIPTION",
            "conflict",
            ("stable-conflict", "actionable-suggestion"),
        ),
        (
            "workflow.create",
            "workflow create --file WORKFLOW_FILE --project PROJECT",
            "conflict",
            ("stable-conflict",),
        ),
        (
            "workflow.get",
            "workflow get MISSING_WORKFLOW --project PROJECT",
            "not_found",
            ("stable-not-found", "missing-workflow-nonmutating"),
        ),
        (
            "task.list",
            "task list --project PROJECT --workflow WORKFLOW",
            "not_found",
            ("stable-not-found", "task-leftovers-zero-with-workflow-absent"),
        ),
        (
            "project.get",
            "project get PROJECT",
            "not_found",
            ("stable-not-found", "deleted-project-absent"),
        ),
    }
    if dependency_update_upstream_limited:
        allowed.add(
            (
                "task.update",
                (
                    "task update TASK --project PROJECT --workflow WORKFLOW "
                    "--set DEPENDS_ON --dry-run"
                ),
                "unsupported_feature",
                ("dependency-update-upstream-limited", "pre-io-no-mutation"),
            )
        )
    unexpected = [
        entry
        for entry in entries
        if not entry.ok
        and (
            entry.action,
            entry.argv_shape,
            entry.error_type,
            entry.assertions,
        )
        not in allowed
    ]
    if unexpected:
        contains_dependency_rejection = any(
            entry.action == "task.update" and "DEPENDS_ON" in entry.argv_shape
            for entry in unexpected
        )
        if contains_dependency_rejection:
            message = (
                "full trace dependency rejection is not pre-I/O exact"
                if dependency_update_upstream_limited
                else "full trace dependency rejection contradicts generated policy"
            )
            raise ValueError(message)
        message = "full scenario trace contains unexpected failed I/O"
        raise ValueError(message)


def _validate_full_pre_workflow_trace(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
) -> int:
    project_list_shape = (
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE"
    )
    cursor = _require_full_trace_stage(
        entries,
        start=start,
        label="doctor preflight",
        action="doctor",
        argv_shape="doctor",
        assertions=("api-ready", "current-user-bound", "principal-hmac-matched"),
        outcomes=("bound-current-user", "capability-preflight-complete"),
    )
    cursor = _require_full_external_fixture_trace(
        entries,
        start=cursor,
        observed_outcome="external-fixture-cross-checked",
        label="pre-scenario external fixture",
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="project create apply",
        action="project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        assertions=("gate-owned-project-created", "native-identity-captured"),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="project duplicate conflict",
        action="project.create",
        argv_shape="project create --name PROJECT_NAME --description DESCRIPTION",
        ok=False,
        error_type="conflict",
        assertions=("stable-conflict", "actionable-suggestion"),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="created project list",
        action="project.list",
        argv_shape=project_list_shape,
        assertions=("bounded-exact-name-search",),
    )
    for label in ("name", "native"):
        cursor = _require_full_trace_stage(
            entries,
            start=cursor,
            label=f"created project {label} read",
            action="project.get",
            argv_shape="project get PROJECT",
            assertions=("gate-owned-project-matched", "native-identity-matched"),
        )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label="project update apply",
        action="project.update",
        argv_shape="project update PROJECT --description DESCRIPTION",
        assertions=("description-marker-updated",),
    )
    return _require_full_trace_stage(
        entries,
        start=cursor,
        label="project update readback",
        action="project.get",
        argv_shape="project get PROJECT",
        assertions=("updated-description-read-back",),
        outcomes=("gate-owned-project-round-trip",),
    )


def _require_full_external_fixture_trace(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
    observed_outcome: str | None,
    label: str,
) -> int:
    project_list_shape = (
        "project list --search PROJECT_NAME --page-no PAGE_NO --page-size PAGE_SIZE"
    )
    workflow_list_shape = (
        "workflow list --project PROJECT --search WORKFLOW_NAME "
        "--page-no PAGE_NO --page-size PAGE_SIZE"
    )
    cursor = _require_full_trace_stage(
        entries,
        start=start,
        label=f"{label} project list",
        action="project.list",
        argv_shape=project_list_shape,
        assertions=("bounded-exact-name-search",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} project name read",
        action="project.get",
        argv_shape="project get PROJECT",
        assertions=("external-project-matched",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} project native read",
        action="project.get",
        argv_shape="project get PROJECT",
        assertions=("external-project-native-identity-matched",),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} workflow list",
        action="workflow.list",
        argv_shape=workflow_list_shape,
        assertions=("external-workflow-native-identity-matched",),
    )
    for selector_kind in ("name", "native"):
        cursor = _require_full_trace_stage(
            entries,
            start=cursor,
            label=f"{label} workflow {selector_kind} read",
            action="workflow.get",
            argv_shape="workflow get WORKFLOW --project PROJECT",
            assertions=("external-workflow-and-schedule-matched",),
        )
    return _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} schedule cross-check",
        action="schedule.list",
        argv_shape=(
            "schedule list --project PROJECT --workflow WORKFLOW "
            "--page-no PAGE_NO --page-size PAGE_SIZE"
        ),
        assertions=("external-schedule-matched",),
        outcomes=() if observed_outcome is None else (observed_outcome,),
    )


def _validate_full_mutation_stages(entries: tuple[_TraceEntry, ...]) -> None:
    mutation_actions = {
        "project.create",
        "project.delete",
        "project.update",
        "task.update",
        "workflow.create",
        "workflow.delete",
        "workflow.edit",
    }
    observed = tuple(
        (entry.action, entry.argv_shape)
        for entry in entries
        if entry.ok
        and entry.action in mutation_actions
        and "--dry-run" not in entry.argv_shape.split()
    )
    expected = (
        (
            "project.create",
            "project create --name PROJECT_NAME --description DESCRIPTION",
        ),
        ("project.update", "project update PROJECT --description DESCRIPTION"),
        (
            "workflow.create",
            "workflow create --file WORKFLOW_FILE --project PROJECT",
        ),
        (
            "task.update",
            "task update TASK --project PROJECT --workflow WORKFLOW --set COMMAND",
        ),
        (
            "workflow.edit",
            "workflow edit WORKFLOW --project PROJECT --patch PATCH_FILE",
        ),
        (
            "workflow.delete",
            "workflow delete WORKFLOW --project PROJECT --force",
        ),
        ("project.delete", "project delete PROJECT --force"),
    )
    if observed != expected:
        message = "full scenario trace mutation stages must equal its fixed seven"
        raise ValueError(message)


def _require_full_task_absence(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
) -> int:
    argv_shape = "task list --project PROJECT --workflow WORKFLOW"
    variants = (
        (True, None, ("task-leftovers-zero-with-workflow-absent",)),
        (
            False,
            "not_found",
            ("stable-not-found", "task-leftovers-zero-with-workflow-absent"),
        ),
    )
    for ok, error_type, assertions in variants:
        try:
            return _require_full_trace_stage(
                entries,
                start=start,
                label="task absence",
                action="task.list",
                argv_shape=argv_shape,
                ok=ok,
                error_type=error_type,
                assertions=assertions,
            )
        except ValueError:
            continue
    message = "full scenario trace lacks its fixed task absence stage"
    raise ValueError(message)


def _require_full_dag_readback(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
    label: str,
    ds_version: str,
    describe_assertions: tuple[str, ...] = (),
) -> int:
    cursor = _require_full_trace_stage(
        entries,
        start=start,
        label=f"{label} describe",
        action="workflow.describe",
        argv_shape="workflow describe WORKFLOW --project PROJECT",
        assertions=(
            "two-task-dag-matched",
            "relation-endpoints-coherent",
            "workflow-offline",
            *describe_assertions,
        ),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} digest",
        action="workflow.digest",
        argv_shape="workflow digest WORKFLOW --project PROJECT",
        assertions=(
            "digest-counts-match-describe",
            "digest-topology-match-describe",
        ),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} export",
        action="workflow.export",
        argv_shape="workflow export WORKFLOW --project PROJECT",
        assertions=("raw-yaml-dag-matches-describe", "raw-body-not-recorded"),
    )
    cursor = _require_full_trace_stage(
        entries,
        start=cursor,
        label=f"{label} task list",
        action="task.list",
        argv_shape="task list --project PROJECT --workflow WORKFLOW",
        assertions=(
            "two-task-list-matched-dag",
            (
                "native-task-ids-coherent"
                if ds_version == "1.3.9"
                else "task-versions-coherent"
            ),
        ),
    )
    for task_index in range(2):
        selector_kinds = ("name",) if ds_version == "1.3.9" else ("name", "native")
        for selector_kind in selector_kinds:
            cursor = _require_full_trace_stage(
                entries,
                start=cursor,
                label=f"{label} task {task_index + 1} {selector_kind} readback",
                action="task.get",
                argv_shape="task get TASK --project PROJECT --workflow WORKFLOW",
                assertions=(
                    "task-get-matched-list-and-dag",
                    f"selector-{selector_kind}-matched",
                ),
            )
    return cursor


def _require_full_trace_stage(
    entries: tuple[_TraceEntry, ...],
    *,
    start: int,
    label: str,
    action: str,
    argv_shape: str,
    assertions: tuple[str, ...],
    ok: bool = True,
    error_type: str | None = None,
    outcomes: tuple[str, ...] = (),
) -> int:
    if start < len(entries):
        entry = entries[start]
        if (
            entry.action == action
            and entry.argv_shape == argv_shape
            and entry.ok is ok
            and entry.error_type == error_type
            and entry.assertions == assertions
            and entry.outcomes == outcomes
        ):
            return start + 1
    message = f"full scenario trace lacks its fixed {label} stage"
    raise ValueError(message)


def _validate_trace_preflight(
    entries: tuple[_TraceEntry, ...],
    *,
    required_actions: tuple[str, ...],
) -> None:
    preflight_size = len(required_actions) + 1
    if len(entries) < preflight_size:
        message = "operation trace is missing its version/capability preflight"
        raise ValueError(message)
    version = entries[0]
    if (
        version.action != "version"
        or not version.ok
        or version.argv_shape != "version"
        or version.outcomes
        or version.assertions != ("selected-contract-and-family-matched",)
    ):
        message = "operation trace version preflight is invalid"
        raise ValueError(message)
    capabilities = entries[1:preflight_size]
    observed_subjects = tuple(entry.subject_action for entry in capabilities)
    if observed_subjects != required_actions:
        message = "operation trace capability preflight does not cover every action"
        raise ValueError(message)
    if any(
        entry.action != "capabilities"
        or not entry.ok
        or entry.argv_shape != "capabilities --action ACTION"
        or entry.outcomes
        or entry.assertions != ("supported",)
        for entry in capabilities
    ):
        message = "operation trace capability preflight is invalid"
        raise ValueError(message)


def _trace_entry(value: object) -> _TraceEntry:
    entry = _mapping(value, label="operation trace entry")
    required = {
        "sequence",
        "action",
        "argv_shape",
        "exit_code",
        "ok",
        "assertions",
        "outcomes",
    }
    allowed = {*required, "error_type", "subject_action"}
    missing = required - entry.keys()
    extra = entry.keys() - allowed
    if missing or extra:
        message = "operation trace entry fields differ"
        raise ValueError(message)
    action = _text(entry.get("action"), label="trace action")
    has_subject_action = "subject_action" in entry
    if action == "capabilities" and not has_subject_action:
        message = "capabilities trace entry requires subject_action"
        raise ValueError(message)
    if action != "capabilities" and has_subject_action:
        message = "only capabilities trace entries may define subject_action"
        raise ValueError(message)
    subject_action = (
        None
        if not has_subject_action
        else _text(entry.get("subject_action"), label="trace subject_action")
    )
    ok = _boolean(entry.get("ok"), label="trace ok")
    has_error_type = "error_type" in entry
    if not ok and not has_error_type:
        message = "failed trace entry requires error_type"
        raise ValueError(message)
    if ok and has_error_type:
        message = "successful trace entry cannot contain error_type"
        raise ValueError(message)
    exit_code = _integer(entry.get("exit_code"), label="trace exit_code")
    if ok != (exit_code == 0):
        message = "trace ok state must match exit_code"
        raise ValueError(message)
    argv_shape = _text(entry.get("argv_shape"), label="trace argv_shape")
    if "<" in argv_shape or ">" in argv_shape:
        message = "trace argv_shape must use metavariables without angle brackets"
        raise ValueError(message)
    _validate_argv_shape(argv_shape, action=action)
    error_type = (
        None
        if not has_error_type
        else _text(entry.get("error_type"), label="trace error_type")
    )
    return _TraceEntry(
        sequence=_integer(entry.get("sequence"), label="trace sequence"),
        action=action,
        argv_shape=argv_shape,
        exit_code=exit_code,
        ok=ok,
        assertions=tuple(
            _text_sequence(entry.get("assertions"), label="trace assertions")
        ),
        outcomes=tuple(
            _text_sequence(
                entry.get("outcomes"),
                label="trace outcomes",
                allow_empty=True,
            )
        ),
        error_type=error_type,
        subject_action=subject_action,
    )


def _validate_argv_shape(value: str, *, action: str) -> None:
    tokens = value.split()
    action_tokens = action.split(".")
    if tokens[: len(action_tokens)] != action_tokens:
        message = "trace argv_shape must begin with its stable action path"
        raise ValueError(message)
    for token in tokens[len(action_tokens) :]:
        if _ARGV_FLAG.fullmatch(token) is not None:
            continue
        if _ARGV_METAVAR.fullmatch(token) is not None:
            continue
        message = "trace argv_shape values must use uppercase metavariables"
        raise ValueError(message)
