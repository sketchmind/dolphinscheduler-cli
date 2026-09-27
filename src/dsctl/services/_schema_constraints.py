from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.pagination import DEFAULT_PAGE_SIZE
from dsctl.upstream.schedules import schedule_contract_features
from dsctl.upstream.worker_groups import worker_group_same_name_update_limitation
from dsctl.upstream.workflows import workflow_execution_schedule_time_shape

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


_SCHEDULE_MUTATION_FIELDS = (
    "--cron",
    "--start",
    "--end",
    "--timezone",
    "--missed-fire-policy",
    "--failure-strategy",
    "--warning-type",
    "--warning-group-id",
    "--priority",
    "--worker-group",
    "--environment-code",
)
_VERSIONED_CONSTRAINT_ACTIONS = (
    "schedule.preview",
    "schedule.explain",
    "schedule.update",
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
    "context.update": (
        _fields("at_most_one_of", "--project", "--clear-project"),
        _fields("requires_any", "--file", "--project", "--clear-project"),
    ),
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
    "capabilities": (
        _fields("at_most_one_of", "--summary", "--section", "--full", "--action"),
    ),
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
        _fields("at_most_one_of", "--description", "--clear-description"),
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
            "--queue",
            "--description",
            "--clear-description",
        ),
    ),
    "user.create": (_fields("exactly_one_of", "--password", "--password-file"),),
    "user.update": (
        _fields("at_most_one_of", "--password", "--password-file"),
        _fields("at_most_one_of", "--phone", "--clear-phone"),
        _fields("at_most_one_of", "--queue", "--clear-queue"),
        _fields(
            "requires_any",
            "--user-name",
            "--password",
            "--password-file",
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
    "workflow.edit": (_fields("exactly_one_of", "--patch", "--file"),),
    "workflow.backfill": (
        _alternatives(
            "at_least_one_of",
            ("--date",),
            ("--start", "--end"),
        ),
        _fields("all_or_none", "--start", "--end"),
        _forbids("--date", "--start", "--end"),
    ),
    "workflow-instance.list": (
        _forbids(
            "--trigger-code",
            "--workflow",
            "--search",
            "--executor",
            "--host",
            "--start",
            "--end",
            "--state",
            "--all",
        ),
        {
            "kind": "requires_default",
            "if_present": "--trigger-code",
            "defaults": {"--page-no": 1, "--page-size": DEFAULT_PAGE_SIZE},
        },
    ),
    "workflow-instance.edit": (_fields("exactly_one_of", "--patch", "--file"),),
    "task-instance.log": (_forbids("--tail", "--start-line", "--limit"),),
    "schedule.list": (_fields("at_most_one_of", "--workflow", "--search"),),
    "template.task": (_requires("--raw", "TASK_TYPE"),),
}

for _force_action in _FORCE_REQUIRED_ACTIONS:
    ACTION_CONSTRAINTS[_force_action] = (
        *ACTION_CONSTRAINTS.get(_force_action, ()),
        _fields("requires_all", "--force"),
    )


def constraints_for_action(action: str, *, ds_version: str) -> list[Constraint]:
    """Return selected-version constraints mirrored from runtime validation."""
    if action == "worker-group.update":
        constraints = [dict(item) for item in ACTION_CONSTRAINTS[action]]
        limitation = worker_group_same_name_update_limitation(ds_version)
        if limitation is not None:
            constraints.append(
                {
                    "kind": "requires_changed_value",
                    "fields": ["--name"],
                    "relative_to": "resolved_worker_group.name",
                    "reason": limitation,
                }
            )
        return constraints
    if (
        action == "workflow.backfill"
        and workflow_execution_schedule_time_shape(ds_version) == "comma-range"
    ):
        return [_fields("requires_all", "--start", "--end")]
    if action == "alert-group.update" and ds_version == "1.3.9":
        return [
            _fields("at_most_one_of", "--description", "--clear-description"),
            _fields(
                "requires_any",
                "--name",
                "--group-type",
                "--description",
                "--clear-description",
            ),
        ]
    schedule_constraints = _schedule_constraints_for_version(
        action,
        ds_version=ds_version,
    )
    if schedule_constraints is not None:
        return schedule_constraints
    return [dict(constraint) for constraint in ACTION_CONSTRAINTS.get(action, ())]


def _schedule_constraints_for_version(
    action: str,
    *,
    ds_version: str,
) -> list[Constraint] | None:
    if action not in {"schedule.preview", "schedule.explain", "schedule.update"}:
        return None
    features = schedule_contract_features(ds_version)
    timing_fields = ["--cron", "--start", "--end"]
    mutation_fields = [
        field
        for field in _SCHEDULE_MUTATION_FIELDS
        if (field != "--timezone" or features.timezone)
        and (field != "--environment-code" or features.environment)
        and (field != "--missed-fire-policy" or features.missed_fire_policy)
    ]
    if features.timezone:
        timing_fields.append("--timezone")

    if action == "schedule.preview":
        return [
            _alternatives(
                "exactly_one_of",
                ("SCHEDULE_ID",),
                tuple(timing_fields),
            ),
            _forbids("SCHEDULE_ID", *timing_fields),
        ]
    if action == "schedule.explain":
        create_only = ["--workflow"]
        if features.tenant:
            create_only.append("--tenant-code")
        return [
            _forbids("SCHEDULE_ID", *create_only),
            _requires_when_absent("SCHEDULE_ID", "--workflow", *timing_fields),
            _requires_any(*mutation_fields, if_present="SCHEDULE_ID"),
        ]
    return [_fields("requires_any", *mutation_fields)]


def constrained_actions() -> tuple[str, ...]:
    """Return actions with explicit cross-field runtime constraints."""
    return (*ACTION_CONSTRAINTS, *_VERSIONED_CONSTRAINT_ACTIONS)


__all__ = ["constrained_actions", "constraints_for_action"]
