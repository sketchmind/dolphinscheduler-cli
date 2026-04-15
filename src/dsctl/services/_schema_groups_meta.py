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
                "list",
                action="enum.list",
                summary="List the members of one supported generated enum.",
                arguments=[
                    argument(
                        "enum",
                        value_type="string",
                        description="Stable enum discovery name.",
                        choices=supported_enum_choices(),
                    )
                ],
            )
        ],
    )


def task_type_group() -> dict[str, object]:
    """Build the task-type command group schema."""
    return group(
        "task-type",
        summary="Discover DolphinScheduler task types for the current runtime.",
        commands=[
            command(
                "list",
                action="task-type.list",
                summary=("List DS task types plus the current user's favourite flags."),
            )
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
