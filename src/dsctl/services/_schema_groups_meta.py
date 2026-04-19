from __future__ import annotations

from dsctl.services._schema_primitives import argument, command, group
from dsctl.services.enums import supported_enum_choices


def enum_group() -> dict[str, object]:
    """Build the enum command group schema."""
    return group(
        "enum",
        summary="Discover generated DolphinScheduler enums.",
        commands=[
            command(
                "names",
                action="enum.names",
                summary="List supported generated enum discovery names.",
            ),
            command(
                "list",
                action="enum.list",
                summary="List the members of one supported generated enum.",
                arguments=[
                    argument(
                        "enum",
                        value_type="string",
                        description="Stable enum discovery name.",
                        choices=supported_enum_choices(),
                        discovery_command="dsctl enum names",
                    )
                ],
            ),
        ],
    )


def task_type_group() -> dict[str, object]:
    """Build the task-type command group schema."""
    return group(
        "task-type",
        summary="Discover DS task types and local task authoring contracts.",
        commands=[
            command(
                "list",
                action="task-type.list",
                summary=(
                    "List live DS task types, categories, favourite flags, and "
                    "CLI authoring coverage."
                ),
            ),
            command(
                "get",
                action="task-type.get",
                summary="Summarize the local authoring contract for one task type.",
                arguments=[
                    argument(
                        "task_type",
                        value_type="string",
                        description="Task type to inspect.",
                        discovery_command="dsctl template task",
                    )
                ],
            ),
            command(
                "schema",
                action="task-type.schema",
                summary=(
                    "Print the full local authoring schema for one task type, "
                    "including fields, state rules, choices, and compile mapping."
                ),
                arguments=[
                    argument(
                        "task_type",
                        value_type="string",
                        description=(
                            "Task type whose authoring schema should be printed."
                        ),
                        discovery_command="dsctl template task",
                    )
                ],
            ),
        ],
    )


def lint_group() -> dict[str, object]:
    """Build the lint command group schema."""
    return group(
        "lint",
        summary="Run local design-time checks without contacting DolphinScheduler.",
        commands=[
            command(
                "workflow",
                action="lint.workflow",
                summary=(
                    "Lint one workflow YAML file using the local spec and "
                    "compile pipeline."
                ),
                arguments=[
                    argument(
                        "file",
                        value_type="path",
                        description="Workflow YAML file to lint.",
                    )
                ],
            )
        ],
    )
