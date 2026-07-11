from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

Constraint = dict[str, object]


def _fields(kind: str, *fields: str) -> Constraint:
    return {"kind": kind, "fields": list(fields)}


def _alternatives(kind: str, *alternatives: Sequence[str]) -> Constraint:
    return {
        "kind": kind,
        "alternatives": [list(alternative) for alternative in alternatives],
    }


def _requires(if_present: str, *fields: str) -> Constraint:
    return {
        "kind": "requires",
        "if_present": if_present,
        "fields": list(fields),
    }


def _requires_when_absent(if_absent: str, *fields: str) -> Constraint:
    return {
        "kind": "requires",
        "if_absent": if_absent,
        "fields": list(fields),
    }


def _requires_any(*fields: str, if_present: str | None = None) -> Constraint:
    constraint = _fields("requires_any", *fields)
    if if_present is not None:
        constraint["if_present"] = if_present
    return constraint


def _forbids(if_present: str, *fields: str) -> Constraint:
    return {
        "kind": "forbids",
        "if_present": if_present,
        "fields": list(fields),
    }


_WORKFLOW_TARGET_ACTIONS = (
    "workflow.get",
    "workflow.export",
    "workflow.describe",
    "workflow.digest",
    "workflow.edit",
    "workflow.online",
    "workflow.offline",
    "workflow.run",
    "workflow.run-task",
    "workflow.backfill",
    "workflow.delete",
    "workflow.lineage.get",
    "workflow.lineage.dependent-tasks",
)
_WORKFLOW_EXPLICIT_PROJECT_CONSTRAINT = (_requires("--project", "WORKFLOW"),)

_SCHEDULE_MUTATION_FIELDS = (
    "--cron",
    "--start",
    "--end",
    "--timezone",
    "--failure-strategy",
    "--warning-type",
    "--warning-group-id",
    "--priority",
    "--worker-group",
    "--environment-code",
)
_FORCE_REQUIRED_ACTIONS = (
    "environment.delete",
    "cluster.delete",
    "datasource.delete",
    "namespace.delete",
    "resource.delete",
    "queue.delete",
    "worker-group.delete",
    "alert-plugin.delete",
    "alert-group.delete",
    "tenant.delete",
    "user.delete",
    "access-token.delete",
    "project.delete",
    "project-parameter.delete",
    "project-worker-group.clear",
    "schedule.delete",
    "workflow.delete",
)

ACTION_CONSTRAINTS: dict[str, tuple[Constraint, ...]] = {
    **dict.fromkeys(_WORKFLOW_TARGET_ACTIONS, _WORKFLOW_EXPLICIT_PROJECT_CONSTRAINT),
    "use.clear": (_fields("requires_all", "--clear"),),
    "schema": (
        _fields(
            "at_most_one_of",
            "--group",
            "--command",
            "--list-groups",
            "--list-commands",
        ),
        _forbids("--full", "--list-groups", "--list-commands"),
    ),
    "capabilities": (_fields("at_most_one_of", "--summary", "--section", "--full"),),
    "task-type.schema": (
        _fields(
            "at_most_one_of",
            "--field",
            "--json-schema",
            "--compile-mappings",
            "--full",
        ),
    ),
    "environment.create": (_fields("exactly_one_of", "--config", "--config-file"),),
    "environment.update": (
        _fields("at_most_one_of", "--config", "--config-file"),
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields("at_most_one_of", "--worker-group", "--clear-worker-groups"),
        _fields(
            "requires_any",
            "--name",
            "--config",
            "--config-file",
            "--description",
            "--clear-description",
            "--worker-group",
            "--clear-worker-groups",
        ),
    ),
    "cluster.create": (_fields("exactly_one_of", "--config", "--config-file"),),
    "cluster.update": (
        _fields("at_most_one_of", "--config", "--config-file"),
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields(
            "requires_any",
            "--name",
            "--config",
            "--config-file",
            "--description",
            "--clear-description",
        ),
    ),
    "queue.update": (_fields("requires_any", "--queue-name", "--queue"),),
    "worker-group.update": (
        _fields("at_most_one_of", "--addr", "--clear-addrs"),
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields(
            "requires_any",
            "--name",
            "--addr",
            "--clear-addrs",
            "--description",
            "--clear-description",
        ),
    ),
    "task-group.list": (_forbids("--project", "--search", "--status"),),
    "task-group.update": (
        _fields(
            "requires_any",
            "--name",
            "--group-size",
            "--description",
            "--clear-description",
        ),
    ),
    "project-preference.update": (
        _fields("exactly_one_of", "--preferences-json", "--file"),
    ),
    "alert-plugin.create": (
        _fields("exactly_one_of", "--param", "--params-json", "--file"),
    ),
    "alert-plugin.update": (
        _fields("at_most_one_of", "--param", "--params-json", "--file"),
        _fields("requires_any", "--name", "--param", "--params-json", "--file"),
    ),
    "alert-group.update": (
        _fields("at_most_one_of", "--instance-id", "--clear-instance-ids"),
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields(
            "requires_any",
            "--name",
            "--instance-id",
            "--clear-instance-ids",
            "--description",
            "--clear-description",
        ),
    ),
    "tenant.update": (
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields(
            "requires_any",
            "--tenant-code",
            "--queue",
            "--description",
            "--clear-description",
        ),
    ),
    "user.update": (
        _fields("at_most_one_of", "--phone", "--clear-phone"),
        _fields("at_most_one_of", "--queue", "--clear-queue"),
        _fields(
            "requires_any",
            "--user-name",
            "--password",
            "--email",
            "--tenant",
            "--state",
            "--phone",
            "--clear-phone",
            "--queue",
            "--clear-queue",
            "--time-zone",
        ),
    ),
    "access-token.update": (
        _fields("at_most_one_of", "--token", "--regenerate-token"),
        _fields(
            "requires_any",
            "--user",
            "--expire-time",
            "--token",
            "--regenerate-token",
        ),
    ),
    "project.update": (
        _fields("at_most_one_of", "--description", "--clear-description"),
        _fields("requires_any", "--name", "--description", "--clear-description"),
    ),
    "project-parameter.update": (
        _fields("requires_any", "--name", "--value", "--data-type"),
    ),
    "project-worker-group.set": (_fields("requires_all", "--worker-group"),),
    "workflow.edit": (
        *_WORKFLOW_EXPLICIT_PROJECT_CONSTRAINT,
        _fields("exactly_one_of", "--patch", "--file"),
        _requires("--file", "WORKFLOW"),
    ),
    "workflow.backfill": (
        *_WORKFLOW_EXPLICIT_PROJECT_CONSTRAINT,
        _alternatives(
            "at_least_one_of",
            ("--date",),
            ("--start", "--end"),
        ),
        _fields("all_or_none", "--start", "--end"),
        _forbids("--date", "--start", "--end"),
    ),
    "workflow-instance.list": (
        _requires("--search", "--project"),
        _requires("--executor", "--project"),
    ),
    "workflow-instance.edit": (_fields("exactly_one_of", "--patch", "--file"),),
    "schedule.list": (_fields("at_most_one_of", "--workflow", "--search"),),
    "schedule.preview": (
        _alternatives(
            "exactly_one_of",
            ("SCHEDULE_ID",),
            ("--cron", "--start", "--end", "--timezone"),
        ),
        _forbids(
            "SCHEDULE_ID",
            "--project",
            "--cron",
            "--start",
            "--end",
            "--timezone",
        ),
    ),
    "schedule.explain": (
        _forbids("SCHEDULE_ID", "--workflow", "--project", "--tenant-code"),
        _requires_when_absent(
            "SCHEDULE_ID",
            "--cron",
            "--start",
            "--end",
            "--timezone",
        ),
        _requires_any(*_SCHEDULE_MUTATION_FIELDS, if_present="SCHEDULE_ID"),
        _requires("--project", "--workflow"),
    ),
    "schedule.update": (_fields("requires_any", *_SCHEDULE_MUTATION_FIELDS),),
    "template.task": (_requires("--raw", "TASK_TYPE"),),
}

for _force_action in _FORCE_REQUIRED_ACTIONS:
    ACTION_CONSTRAINTS[_force_action] = (
        *ACTION_CONSTRAINTS.get(_force_action, ()),
        _fields("requires_all", "--force"),
    )


def constraints_for_action(action: str) -> list[Constraint]:
    """Return stable cross-field constraints mirrored from runtime validation."""
    return [dict(constraint) for constraint in ACTION_CONSTRAINTS.get(action, ())]


def constrained_actions() -> tuple[str, ...]:
    """Return actions with explicit cross-field runtime constraints."""
    return tuple(ACTION_CONSTRAINTS)


__all__ = ["constrained_actions", "constraints_for_action"]
