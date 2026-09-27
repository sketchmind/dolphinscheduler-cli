from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_definition_wire import task_update_contract_features
from dsctl.upstream.task_settings import task_node_native_name

if TYPE_CHECKING:
    from dsctl.services.task_authoring_catalog.catalog import TaskAuthoringCatalog


_TASK_RUNTIME_EXAMPLES = (
    ("flag", "NO"),
    ("environment_code", "42"),
    ("task_group_id", "12"),
    ("task_group_priority", "0"),
    ("timeout_notify_strategy", "WARN"),
    ("cpu_quota", "50"),
    ("memory_max", "1024"),
)


def task_template_with_runtime_controls(base: str) -> str:
    """Append the stable task-runtime block to one task template body."""
    return f"{base}{_task_runtime_controls_comment_block()}delay: 0\ndepends_on: []\n"


def _task_runtime_controls_comment_block() -> str:
    examples = "".join(
        f"# {field}: {value}\n" for field, value in _TASK_RUNTIME_EXAMPLES
    )
    return f"# Optional task runtime controls:\n{examples}"


def project_task_runtime_template(
    yaml_text: str,
    *,
    catalog: TaskAuthoringCatalog,
) -> str:
    """Keep only runtime examples representable by the selected task payload."""
    surface = catalog.authoring_surface.task_node
    unavailable = set(surface.unavailable_fields)
    if surface.wire == "task-definition-json":
        request_fields = task_update_contract_features(
            catalog.profile_version
        ).request_fields
        runtime_fields = ("delay", *(field for field, _ in _TASK_RUNTIME_EXAMPLES))
        unavailable.update(
            field
            for field in runtime_fields
            if task_node_native_name(field) not in request_fields
        )
    return "".join(
        (
            line.rstrip("\n") + "  # Minutes; 0 disables task timeout.\n"
            if line.startswith("timeout:") and "#" not in line
            else line
        )
        for line in yaml_text.splitlines(keepends=True)
        if line.removeprefix("# ").partition(":")[0] not in unavailable
    )


def generic_task_runtime_limitation(
    task_type: str, *, catalog: TaskAuthoringCatalog
) -> str | None:
    """Explain a known runtime exclusion without changing opaque authoring policy."""
    if task_type != "FLINK_STREAM" or catalog.supports_typed_authoring(task_type):
        return None
    reason = catalog.authoring_surface.flink_stream_inline_sql.exclusion_reason
    return {
        "stream-executor-service-not-supported": (
            "The stream execution REST entry point returns Not supported."
        ),
        "broken-unconditional-main-jar-no-review": (
            "This plugin retains the missing-mainJar runtime failure."
        ),
    }.get(reason or "", reason)
