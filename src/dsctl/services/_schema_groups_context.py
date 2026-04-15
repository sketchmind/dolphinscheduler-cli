from __future__ import annotations

from dsctl.services._schema_primitives import (
    argument,
    command,
    group,
    option,
    project_option,
    use_target_options,
)
from dsctl.services.pagination import DEFAULT_PAGE_SIZE


def use_group() -> dict[str, object]:
    """Build the persisted-context command group schema."""
    return group(
        "use",
        summary="Set or clear persisted CLI context.",
        group_action={
            "action": "use.clear",
            "summary": (
                "Clear all persisted context in one scope when called as use --clear."
            ),
            "options": [
                option(
                    "clear",
                    value_type="boolean",
                    description="Clear the selected context scope.",
                    default=False,
                ),
                option(
                    "scope",
                    value_type="string",
                    description="Persisted context layer to update.",
                    default="project",
                    choices=["project", "user"],
                ),
            ],
        },
        commands=[
            command(
                "project",
                action="use.project",
                summary="Set or clear the project context.",
                arguments=[
                    argument(
                        "name",
                        value_type="string",
                        description="Project name to persist. Required unless --clear.",
                        required=False,
                        selector="opaque_name",
                    )
                ],
                options=use_target_options(
                    clear_help=(
                        "Clear the stored project context and any workflow context "
                        "beneath it."
                    )
                ),
            ),
            command(
                "workflow",
                action="use.workflow",
                summary="Set or clear the workflow context.",
                arguments=[
                    argument(
                        "name",
                        value_type="string",
                        description=(
                            "Workflow name to persist. Required unless --clear."
                        ),
                        required=False,
                        selector="opaque_name",
                    )
                ],
                options=use_target_options(
                    clear_help="Clear the stored workflow context."
                ),
            ),
        ],
    )


def project_group() -> dict[str, object]:
    """Build the project command group schema."""
    return group(
        "project",
        summary="Manage DolphinScheduler projects.",
        commands=[
            command(
                "list",
                action="project.list",
                summary=(
                    "List projects with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter projects by name using the upstream search value."
                        ),
                    ),
                    option(
                        "page-no",
                        value_type="integer",
                        description="Page number to fetch when not using --all.",
                        default=1,
                    ),
                    option(
                        "page-size",
                        value_type="integer",
                        description="Page size to request from the upstream API.",
                        default=DEFAULT_PAGE_SIZE,
                    ),
                    option(
                        "all",
                        value_type="boolean",
                        description="Fetch all remaining pages up to the safety limit.",
                        default=False,
                    ),
                ],
            ),
            command(
                "get",
                action="project.get",
                summary="Get one project by name or code.",
                arguments=[
                    argument(
                        "project",
                        value_type="string",
                        description="Project name or numeric code.",
                        selector="name_or_code",
                    )
                ],
            ),
            command(
                "create",
                action="project.create",
                summary="Create a project.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Project name.",
                        required=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional project description.",
                    ),
                ],
            ),
            command(
                "update",
                action="project.update",
                summary="Update a project.",
                arguments=[
                    argument(
                        "project",
                        value_type="string",
                        description="Project name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated project name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated project description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored project description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="project.delete",
                summary="Delete a project.",
                arguments=[
                    argument(
                        "project",
                        value_type="string",
                        description="Project name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm project deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def project_parameter_group() -> dict[str, object]:
    """Build the project-parameter command group schema."""
    return group(
        "project-parameter",
        summary="Manage DolphinScheduler project parameters.",
        commands=[
            command(
                "list",
                action="project-parameter.list",
                summary="List project parameters inside one selected project.",
                options=[
                    project_option(),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter project parameters by name using the upstream "
                            "search value."
                        ),
                    ),
                    option(
                        "data-type",
                        value_type="string",
                        description=(
                            "Filter project parameters by DS projectParameterDataType."
                        ),
                    ),
                    option(
                        "page-no",
                        value_type="integer",
                        description="Page number to fetch when not using --all.",
                        default=1,
                    ),
                    option(
                        "page-size",
                        value_type="integer",
                        description="Page size to request from the upstream API.",
                        default=DEFAULT_PAGE_SIZE,
                    ),
                    option(
                        "all",
                        value_type="boolean",
                        description="Fetch all remaining pages up to the safety limit.",
                        default=False,
                    ),
                ],
            ),
            command(
                "get",
                action="project-parameter.get",
                summary="Get one project parameter by name or code.",
                arguments=[
                    argument(
                        "project-parameter",
                        value_type="string",
                        description="Project parameter name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[project_option()],
            ),
            command(
                "create",
                action="project-parameter.create",
                summary="Create one project parameter.",
                options=[
                    project_option(),
                    option(
                        "name",
                        value_type="string",
                        description="Project parameter name.",
                        required=True,
                    ),
                    option(
                        "value",
                        value_type="string",
                        description="Project parameter value.",
                        required=True,
                    ),
                    option(
                        "data-type",
                        value_type="string",
                        description="DS projectParameterDataType value.",
                        default="VARCHAR",
                    ),
                ],
            ),
            command(
                "update",
                action="project-parameter.update",
                summary="Update one project parameter.",
                arguments=[
                    argument(
                        "project-parameter",
                        value_type="string",
                        description="Project parameter name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated parameter name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "value",
                        value_type="string",
                        description=(
                            "Updated parameter value. Omit to keep the current value."
                        ),
                    ),
                    option(
                        "data-type",
                        value_type="string",
                        description="Updated DS projectParameterDataType value.",
                    ),
                ],
            ),
            command(
                "delete",
                action="project-parameter.delete",
                summary="Delete one project parameter.",
                arguments=[
                    argument(
                        "project-parameter",
                        value_type="string",
                        description="Project parameter name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    project_option(),
                    option(
                        "force",
                        value_type="boolean",
                        description=(
                            "Confirm project parameter deletion without prompting."
                        ),
                        default=False,
                    ),
                ],
            ),
        ],
    )


def project_preference_group() -> dict[str, object]:
    """Build the project-preference command group schema."""
    return group(
        "project-preference",
        summary=(
            "Manage the singleton DolphinScheduler project preference as a "
            "project-level default-value source."
        ),
        commands=[
            command(
                "get",
                action="project-preference.get",
                summary=("Get the selected project preference default-value source."),
                options=[project_option()],
            ),
            command(
                "update",
                action="project-preference.update",
                summary=(
                    "Create or update the selected project preference "
                    "default-value source from one JSON object."
                ),
                options=[
                    project_option(),
                    option(
                        "preferences-json",
                        value_type="string",
                        description=(
                            "Inline JSON object used as the DS project preference "
                            "payload."
                        ),
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one JSON object file for the DS project "
                            "preference payload."
                        ),
                        value_name="PATH",
                    ),
                ],
            ),
            command(
                "enable",
                action="project-preference.enable",
                summary="Enable the selected project preference default-value source.",
                options=[project_option()],
            ),
            command(
                "disable",
                action="project-preference.disable",
                summary="Disable the selected project preference default-value source.",
                options=[project_option()],
            ),
        ],
    )


def project_worker_group_group() -> dict[str, object]:
    """Build the project-worker-group command group schema."""
    return group(
        "project-worker-group",
        summary="Manage DolphinScheduler project worker-group assignments.",
        commands=[
            command(
                "list",
                action="project-worker-group.list",
                summary=(
                    "List the worker groups currently reported for one selected "
                    "project."
                ),
                options=[project_option()],
            ),
            command(
                "set",
                action="project-worker-group.set",
                summary=(
                    "Replace the explicit worker-group assignment set for one "
                    "selected project."
                ),
                options=[
                    project_option(),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group to keep assigned to this project. Repeat "
                            "as needed."
                        ),
                        multiple=True,
                    ),
                ],
            ),
            command(
                "clear",
                action="project-worker-group.clear",
                summary=(
                    "Clear the explicit worker-group assignment set for one "
                    "selected project."
                ),
                options=[
                    project_option(),
                    option(
                        "force",
                        value_type="boolean",
                        description=(
                            "Confirm removal of all explicit project worker-group "
                            "assignments."
                        ),
                        default=False,
                    ),
                ],
            ),
        ],
    )
