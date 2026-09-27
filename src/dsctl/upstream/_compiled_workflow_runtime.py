"""Shared exact native programs; callers retain their focused workflow domains."""

from dataclasses import replace
from typing import Literal

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    CompiledDomainPrograms,
)
from dsctl.upstream.wire import WireResultEnvelope

WorkflowPrimitive = Literal[
    "definition_refs",
    "definition_page",
    "definition_get",
    "definition_create",
    "definition_update",
    "definition_delete",
    "definition_release",
    "workflow_execute",
    "task_code_allocate",
    "task_get",
    "task_update",
    "task_cleanup_page",
    "task_cleanup_history",
    "task_cleanup_release",
    "task_cleanup_delete",
    "schedule_page",
    "schedule_create",
    "schedule_update",
    "schedule_delete",
    "schedule_online",
    "schedule_offline",
    "schedule_preview",
    "instance_page",
    "instance_get",
    "instance_trigger",
    "instance_parent",
    "instance_sub",
    "instance_update_legacy",
    "instance_update",
    "instance_control",
    "instance_execute_task",
    "task_instance_page",
    "task_instance_force_success",
    "task_instance_savepoint",
    "task_instance_stop",
    "task_log",
    "lineage_list",
    "lineage_get",
    "lineage_dependent_tasks",
]

_LEGACY = frozenset({"1.3.9"})
_VERSIONS = frozenset(TARGET_DS_VERSIONS)
_PRE_310 = frozenset(
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
    }
)
_PRE_320 = _PRE_310 | {
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
_CLEANUP = frozenset(
    {
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
_MUTATION_OPTIONAL = replace(
    MUTATION_ONCE_REQUIRED, envelope=WireResultEnvelope.OPTIONAL
)

WORKFLOW_PROGRAMS = CompiledDomainPrograms[WorkflowPrimitive](
    name="workflow_runtime",
    schema_constant="COMPILED_WORKFLOW_RUNTIME_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "definition_refs": READ_RETRY_OPTIONAL,
        "definition_page": READ_RETRY_OPTIONAL,
        "definition_get": READ_RETRY_OPTIONAL,
        "definition_create": _MUTATION_OPTIONAL,
        "definition_update": _MUTATION_OPTIONAL,
        "definition_delete": MUTATION_ONCE_REQUIRED,
        "definition_release": _MUTATION_OPTIONAL,
        "workflow_execute": _MUTATION_OPTIONAL,
        "task_code_allocate": replace(READ_RETRY_OPTIONAL, absent_versions=_LEGACY),
        "task_get": replace(READ_RETRY_OPTIONAL, absent_versions=_LEGACY),
        "task_update": replace(
            _MUTATION_OPTIONAL,
            absent_versions=_LEGACY | {"2.0.0", "2.0.1", "2.0.2", "2.0.3", "3.4.3"},
        ),
        "task_cleanup_page": replace(
            READ_RETRY_OPTIONAL, absent_versions=_VERSIONS - _CLEANUP
        ),
        "task_cleanup_history": replace(
            READ_RETRY_OPTIONAL,
            absent_versions=_VERSIONS
            - {"3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8", "3.1.9"},
        ),
        "task_cleanup_release": replace(
            _MUTATION_OPTIONAL,
            absent_versions=_VERSIONS - {"2.0.1", "2.0.2", "2.0.3"},
        ),
        "task_cleanup_delete": replace(
            _MUTATION_OPTIONAL,
            absent_versions=_VERSIONS
            - (
                _CLEANUP
                - {"3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8", "3.1.9"}
            ),
        ),
        "schedule_page": READ_RETRY_OPTIONAL,
        "schedule_create": MUTATION_ONCE_REQUIRED,
        "schedule_update": MUTATION_ONCE_REQUIRED,
        "schedule_delete": MUTATION_ONCE_REQUIRED,
        "schedule_online": MUTATION_ONCE_REQUIRED,
        "schedule_offline": MUTATION_ONCE_REQUIRED,
        "schedule_preview": MUTATION_ONCE_REQUIRED,
        "instance_page": READ_RETRY_OPTIONAL,
        "instance_get": READ_RETRY_OPTIONAL,
        "instance_trigger": replace(READ_RETRY_OPTIONAL, absent_versions=_PRE_320),
        "instance_parent": READ_RETRY_OPTIONAL,
        "instance_sub": READ_RETRY_OPTIONAL,
        "instance_update_legacy": replace(
            _MUTATION_OPTIONAL, absent_versions=_VERSIONS - _LEGACY
        ),
        "instance_update": replace(MUTATION_ONCE_REQUIRED, absent_versions=_LEGACY),
        "instance_control": MUTATION_ONCE_REQUIRED,
        "instance_execute_task": replace(
            MUTATION_ONCE_REQUIRED, absent_versions=_PRE_320
        ),
        "task_instance_page": READ_RETRY_OPTIONAL,
        "task_instance_force_success": replace(
            MUTATION_ONCE_REQUIRED, absent_versions=_LEGACY
        ),
        "task_instance_savepoint": replace(
            MUTATION_ONCE_REQUIRED, absent_versions=_PRE_310
        ),
        "task_instance_stop": replace(MUTATION_ONCE_REQUIRED, absent_versions=_PRE_310),
        "task_log": READ_RETRY_OPTIONAL,
        "lineage_list": replace(READ_RETRY_OPTIONAL, absent_versions=_LEGACY),
        "lineage_get": replace(READ_RETRY_OPTIONAL, absent_versions=_LEGACY),
        "lineage_dependent_tasks": replace(
            READ_RETRY_OPTIONAL, absent_versions=_PRE_320 | {"3.2.0", "3.2.1"}
        ),
    },
)
