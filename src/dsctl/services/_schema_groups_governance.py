from __future__ import annotations

from dsctl.services._schema_primitives import argument, command, group, option
from dsctl.services.pagination import DEFAULT_PAGE_SIZE


def env_group() -> dict[str, object]:
    """Build the environment command group schema."""
    return group(
        "env",
        summary="Manage DolphinScheduler environments.",
        commands=[
            command(
                "list",
                action="env.list",
                summary=(
                    "List environments with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter environments by name using the upstream search "
                            "value."
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
                action="env.get",
                summary="Get one environment by name or code.",
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description="Environment name or numeric code.",
                        selector="name_or_code",
                    )
                ],
            ),
            command(
                "create",
                action="env.create",
                summary="Create one environment.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Environment name.",
                        required=True,
                    ),
                    option(
                        "config",
                        value_type="string",
                        description="Environment config payload.",
                        required=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional environment description.",
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group to bind to this environment. Repeat as "
                            "needed."
                        ),
                        multiple=True,
                    ),
                ],
            ),
            command(
                "update",
                action="env.update",
                summary="Update one environment by name or code.",
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description="Environment name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated environment name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "config",
                        value_type="string",
                        description=(
                            "Updated environment config. Omit to keep the current "
                            "config."
                        ),
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated environment description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored environment description.",
                        default=False,
                    ),
                    option(
                        "worker-group",
                        value_type="string",
                        description=(
                            "Worker group to bind to this environment. Repeat as "
                            "needed."
                        ),
                        multiple=True,
                    ),
                    option(
                        "clear-worker-groups",
                        value_type="boolean",
                        description="Clear all bound worker groups.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="env.delete",
                summary="Delete one environment by name or code.",
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description="Environment name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm environment deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def cluster_group() -> dict[str, object]:
    """Build the cluster command group schema."""
    return group(
        "cluster",
        summary="Manage DolphinScheduler clusters.",
        commands=[
            command(
                "list",
                action="cluster.list",
                summary=(
                    "List clusters with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter clusters by name using the upstream search value."
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
                action="cluster.get",
                summary="Get one cluster by name or code.",
                arguments=[
                    argument(
                        "cluster",
                        value_type="string",
                        description="Cluster name or numeric code.",
                        selector="name_or_code",
                    )
                ],
            ),
            command(
                "create",
                action="cluster.create",
                summary="Create one cluster.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Cluster name.",
                        required=True,
                    ),
                    option(
                        "config",
                        value_type="string",
                        description="Cluster config payload.",
                        required=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional cluster description.",
                    ),
                ],
            ),
            command(
                "update",
                action="cluster.update",
                summary="Update one cluster by name or code.",
                arguments=[
                    argument(
                        "cluster",
                        value_type="string",
                        description="Cluster name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated cluster name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "config",
                        value_type="string",
                        description=(
                            "Updated cluster config. Omit to keep the current config."
                        ),
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated cluster description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored cluster description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="cluster.delete",
                summary="Delete one cluster by name or code.",
                arguments=[
                    argument(
                        "cluster",
                        value_type="string",
                        description="Cluster name or numeric code.",
                        selector="name_or_code",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm cluster deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def datasource_group() -> dict[str, object]:
    """Build the datasource command group schema."""
    return group(
        "datasource",
        summary="Manage DolphinScheduler datasources.",
        commands=[
            command(
                "list",
                action="datasource.list",
                summary=(
                    "List datasources with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter datasources by name using the upstream search "
                            "value."
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
                action="datasource.get",
                summary="Get one datasource by name or id.",
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description="Datasource name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="datasource.create",
                summary="Create one datasource from a JSON payload file.",
                options=[
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one DS-native datasource JSON payload file."
                        ),
                        required=True,
                        value_name="PATH",
                    )
                ],
            ),
            command(
                "update",
                action="datasource.update",
                summary="Update one datasource from a JSON payload file.",
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description="Datasource name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one DS-native datasource JSON payload file."
                        ),
                        required=True,
                        value_name="PATH",
                    )
                ],
            ),
            command(
                "delete",
                action="datasource.delete",
                summary="Delete one datasource by name or id.",
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description="Datasource name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm datasource deletion without prompting.",
                        default=False,
                    )
                ],
            ),
            command(
                "test",
                action="datasource.test",
                summary="Run one datasource connection test by name or id.",
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description="Datasource name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
        ],
    )


def resource_group() -> dict[str, object]:
    """Build the resource command group schema."""
    return group(
        "resource",
        summary="Manage DolphinScheduler file resources.",
        commands=[
            command(
                "list",
                action="resource.list",
                summary=(
                    "List file resources inside one DS directory with optional "
                    "filtering and pagination controls."
                ),
                options=[
                    option(
                        "dir",
                        value_type="string",
                        description=(
                            "DS directory fullName path. Defaults to the upstream "
                            "base directory."
                        ),
                    ),
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter resource names by the upstream search value."
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
                "view",
                action="resource.view",
                summary="View one text content window for one resource file.",
                arguments=[
                    argument(
                        "resource",
                        value_type="string",
                        description="DS resource fullName path.",
                        selector="resource_path",
                    )
                ],
                options=[
                    option(
                        "skip-line-num",
                        value_type="integer",
                        description="Number of lines to skip before returning content.",
                        default=0,
                    ),
                    option(
                        "limit",
                        value_type="integer",
                        description="Maximum number of lines to fetch.",
                        default=100,
                    ),
                ],
            ),
            command(
                "upload",
                action="resource.upload",
                summary="Upload one local file into one DS directory.",
                options=[
                    option(
                        "file",
                        value_type="path",
                        description="Local file path to upload.",
                        required=True,
                        value_name="PATH",
                    ),
                    option(
                        "dir",
                        value_type="string",
                        description=(
                            "Destination DS directory fullName path. Defaults to the "
                            "upstream base directory."
                        ),
                    ),
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Override the remote leaf file name. Defaults to the "
                            "local file name."
                        ),
                    ),
                ],
            ),
            command(
                "create",
                action="resource.create",
                summary="Create one text resource from inline content.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Remote leaf file name, including the file extension."
                        ),
                        required=True,
                    ),
                    option(
                        "content",
                        value_type="string",
                        description=(
                            "Inline text content to write into the remote resource "
                            "file."
                        ),
                        required=True,
                    ),
                    option(
                        "dir",
                        value_type="string",
                        description=(
                            "Destination DS directory fullName path. Defaults to the "
                            "upstream base directory."
                        ),
                    ),
                ],
            ),
            command(
                "mkdir",
                action="resource.mkdir",
                summary="Create one directory inside one DS resource directory.",
                arguments=[
                    argument(
                        "name",
                        value_type="string",
                        description="Leaf directory name to create.",
                    )
                ],
                options=[
                    option(
                        "dir",
                        value_type="string",
                        description=(
                            "Parent DS directory fullName path. Defaults to the "
                            "upstream base directory."
                        ),
                    )
                ],
            ),
            command(
                "download",
                action="resource.download",
                summary="Download one remote resource to one local file path.",
                arguments=[
                    argument(
                        "resource",
                        value_type="string",
                        description="DS resource fullName path.",
                        selector="resource_path",
                    )
                ],
                options=[
                    option(
                        "output",
                        value_type="path",
                        description=(
                            "Local output file path or existing directory. Defaults "
                            "to the current working directory plus the remote leaf "
                            "name."
                        ),
                        value_name="PATH",
                    ),
                    option(
                        "overwrite",
                        value_type="boolean",
                        description="Replace an existing local output file.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="resource.delete",
                summary="Delete one resource.",
                arguments=[
                    argument(
                        "resource",
                        value_type="string",
                        description="DS resource fullName path.",
                        selector="resource_path",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm resource deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def namespace_group() -> dict[str, object]:
    """Build the namespace command group schema."""
    return group(
        "namespace",
        summary="Manage DolphinScheduler namespaces.",
        commands=[
            command(
                "list",
                action="namespace.list",
                summary=(
                    "List namespaces with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter namespaces by namespace name using the upstream "
                            "search value."
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
                action="namespace.get",
                summary="Get one namespace by name or id.",
                arguments=[
                    argument(
                        "namespace",
                        value_type="string",
                        description="Namespace name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "available",
                action="namespace.available",
                summary="List namespaces available to the current login user.",
            ),
            command(
                "create",
                action="namespace.create",
                summary="Create one namespace.",
                options=[
                    option(
                        "namespace",
                        value_type="string",
                        description="Namespace name.",
                        required=True,
                    ),
                    option(
                        "cluster-code",
                        value_type="integer",
                        description="Owning cluster code.",
                        required=True,
                    ),
                ],
            ),
            command(
                "delete",
                action="namespace.delete",
                summary="Delete one namespace by name or id.",
                arguments=[
                    argument(
                        "namespace",
                        value_type="string",
                        description="Namespace name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm namespace deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def queue_group() -> dict[str, object]:
    """Build the queue command group schema."""
    return group(
        "queue",
        summary="Manage DolphinScheduler queues.",
        commands=[
            command(
                "list",
                action="queue.list",
                summary="List queues with optional filtering and pagination controls.",
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter queues by queue name using the upstream search "
                            "value."
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
                action="queue.get",
                summary="Get one queue by name or id.",
                arguments=[
                    argument(
                        "queue",
                        value_type="string",
                        description="Queue name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="queue.create",
                summary="Create one queue.",
                options=[
                    option(
                        "queue-name",
                        value_type="string",
                        description="Human-facing queue name.",
                        required=True,
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description="Underlying DolphinScheduler queue value.",
                        required=True,
                    ),
                ],
            ),
            command(
                "update",
                action="queue.update",
                summary="Update one queue by name or id.",
                arguments=[
                    argument(
                        "queue",
                        value_type="string",
                        description="Queue name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "queue-name",
                        value_type="string",
                        description=(
                            "Updated queue name. Omit to keep the current queue name."
                        ),
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description=(
                            "Updated queue value. Omit to keep the current queue value."
                        ),
                    ),
                ],
            ),
            command(
                "delete",
                action="queue.delete",
                summary="Delete one queue by name or id.",
                arguments=[
                    argument(
                        "queue",
                        value_type="string",
                        description="Queue name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm queue deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def worker_group_group() -> dict[str, object]:
    """Build the worker-group command group schema."""
    return group(
        "worker-group",
        summary="Manage DolphinScheduler worker groups.",
        commands=[
            command(
                "list",
                action="worker-group.list",
                summary=(
                    "List worker groups with optional filtering and pagination "
                    "controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter UI worker groups by name using the upstream "
                            "search value."
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
                action="worker-group.get",
                summary="Get one worker group by name or id.",
                arguments=[
                    argument(
                        "worker_group",
                        value_type="string",
                        description="Worker-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="worker-group.create",
                summary="Create one worker group.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Worker-group name.",
                        required=True,
                    ),
                    option(
                        "addr",
                        value_type="string",
                        description=(
                            "Worker address to include in addrList. Repeat as needed."
                        ),
                        multiple=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional worker-group description.",
                    ),
                ],
            ),
            command(
                "update",
                action="worker-group.update",
                summary="Update one worker group by name or id.",
                arguments=[
                    argument(
                        "worker_group",
                        value_type="string",
                        description="Worker-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated worker-group name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "addr",
                        value_type="string",
                        description=(
                            "Replacement worker address list. Repeat as needed."
                        ),
                        multiple=True,
                    ),
                    option(
                        "clear-addrs",
                        value_type="boolean",
                        description="Clear the current addrList.",
                        default=False,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated worker-group description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the current worker-group description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="worker-group.delete",
                summary="Delete one worker group by name or id.",
                arguments=[
                    argument(
                        "worker_group",
                        value_type="string",
                        description="Worker-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm worker-group deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def task_group_group() -> dict[str, object]:
    """Build the task-group command group schema."""
    return group(
        "task-group",
        summary="Manage DolphinScheduler task groups.",
        commands=[
            command(
                "list",
                action="task-group.list",
                summary=(
                    "List task groups with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "project",
                        value_type="string",
                        description="Project name or code for project-scoped listing.",
                    ),
                    option(
                        "search",
                        value_type="string",
                        description="Filter task groups by task-group name.",
                    ),
                    option(
                        "status",
                        value_type="string",
                        description="Filter task groups by status: open or closed.",
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
                action="task-group.get",
                summary="Get one task group by name or id.",
                arguments=[
                    argument(
                        "task_group",
                        value_type="string",
                        description="Task-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="task-group.create",
                summary="Create one task group.",
                options=[
                    option(
                        "project",
                        value_type="string",
                        description=(
                            "Project name or code. Falls back to stored "
                            "project context."
                        ),
                    ),
                    option(
                        "name",
                        value_type="string",
                        description="Task-group name.",
                        required=True,
                    ),
                    option(
                        "group-size",
                        value_type="integer",
                        description="Task-group capacity.",
                        required=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional task-group description.",
                    ),
                ],
            ),
            command(
                "update",
                action="task-group.update",
                summary="Update one task group by name or id.",
                arguments=[
                    argument(
                        "task_group",
                        value_type="string",
                        description="Task-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Updated task-group name.",
                    ),
                    option(
                        "group-size",
                        value_type="integer",
                        description="Updated task-group capacity.",
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated task-group description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored task-group description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "close",
                action="task-group.close",
                summary="Close one task group by name or id.",
                arguments=[
                    argument(
                        "task_group",
                        value_type="string",
                        description="Task-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "start",
                action="task-group.start",
                summary="Start one task group by name or id.",
                arguments=[
                    argument(
                        "task_group",
                        value_type="string",
                        description="Task-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            group(
                "queue",
                summary="Manage DolphinScheduler task-group queues.",
                commands=[
                    command(
                        "list",
                        action="task-group.queue.list",
                        summary="List queue rows for one task group.",
                        arguments=[
                            argument(
                                "task_group",
                                value_type="string",
                                description="Task-group name or numeric id.",
                                selector="name_or_id",
                            )
                        ],
                        options=[
                            option(
                                "task-instance",
                                value_type="string",
                                description="Filter by task-instance name.",
                            ),
                            option(
                                "workflow-instance",
                                value_type="string",
                                description="Filter by workflow-instance name.",
                            ),
                            option(
                                "status",
                                value_type="string",
                                description=(
                                    "Filter by queue status: WAIT_QUEUE, "
                                    "ACQUIRE_SUCCESS, or RELEASE."
                                ),
                            ),
                            option(
                                "page-no",
                                value_type="integer",
                                description=(
                                    "Page number to fetch when not using --all."
                                ),
                                default=1,
                            ),
                            option(
                                "page-size",
                                value_type="integer",
                                description=(
                                    "Page size to request from the upstream API."
                                ),
                                default=DEFAULT_PAGE_SIZE,
                            ),
                            option(
                                "all",
                                value_type="boolean",
                                description=(
                                    "Fetch all remaining pages up to the safety limit."
                                ),
                                default=False,
                            ),
                        ],
                    ),
                    command(
                        "force-start",
                        action="task-group.queue.force-start",
                        summary="Force-start one waiting task-group queue row.",
                        arguments=[
                            argument(
                                "queue_id",
                                value_type="integer",
                                description="Numeric task-group queue id.",
                                selector="id",
                            )
                        ],
                    ),
                    command(
                        "set-priority",
                        action="task-group.queue.set-priority",
                        summary="Set one task-group queue priority.",
                        arguments=[
                            argument(
                                "queue_id",
                                value_type="integer",
                                description="Numeric task-group queue id.",
                                selector="id",
                            )
                        ],
                        options=[
                            option(
                                "priority",
                                value_type="integer",
                                description="Updated queue priority.",
                                required=True,
                            )
                        ],
                    ),
                ],
            ),
        ],
    )


def alert_plugin_group() -> dict[str, object]:
    """Build the alert-plugin command group schema."""
    return group(
        "alert-plugin",
        summary="Manage DolphinScheduler alert plugin instances.",
        commands=[
            command(
                "list",
                action="alert-plugin.list",
                summary=(
                    "List alert-plugin instances with optional filtering and "
                    "pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter alert-plugin instances by instance name using "
                            "the upstream search value."
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
                action="alert-plugin.get",
                summary="Get one alert-plugin instance by name or id.",
                arguments=[
                    argument(
                        "alert-plugin",
                        value_type="string",
                        description="Alert-plugin instance name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "schema",
                action="alert-plugin.schema",
                summary="Get one alert UI plugin definition by name or id.",
                arguments=[
                    argument(
                        "plugin",
                        value_type="string",
                        description="Alert UI plugin definition name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="alert-plugin.create",
                summary="Create one alert-plugin instance.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Alert-plugin instance name.",
                        required=True,
                    ),
                    option(
                        "plugin",
                        value_type="string",
                        description="Alert UI plugin definition name or numeric id.",
                        required=True,
                        selector="name_or_id",
                    ),
                    option(
                        "params-json",
                        value_type="string",
                        description="DS-native alert-plugin UI params JSON array.",
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one DS-native alert-plugin UI params JSON file."
                        ),
                        value_name="PATH",
                    ),
                ],
            ),
            command(
                "update",
                action="alert-plugin.update",
                summary="Update one alert-plugin instance by name or id.",
                arguments=[
                    argument(
                        "alert-plugin",
                        value_type="string",
                        description="Alert-plugin instance name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Updated alert-plugin instance name.",
                    ),
                    option(
                        "params-json",
                        value_type="string",
                        description=(
                            "Replacement DS-native alert-plugin UI params JSON array."
                        ),
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one replacement DS-native alert-plugin UI "
                            "params JSON file."
                        ),
                        value_name="PATH",
                    ),
                ],
            ),
            command(
                "delete",
                action="alert-plugin.delete",
                summary="Delete one alert-plugin instance by name or id.",
                arguments=[
                    argument(
                        "alert-plugin",
                        value_type="string",
                        description="Alert-plugin instance name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm alert-plugin deletion without prompting.",
                        default=False,
                    )
                ],
            ),
            command(
                "test",
                action="alert-plugin.test",
                summary="Send one test alert using one alert-plugin instance.",
                arguments=[
                    argument(
                        "alert-plugin",
                        value_type="string",
                        description="Alert-plugin instance name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
        ],
    )


def alert_group_group() -> dict[str, object]:
    """Build the alert-group command group schema."""
    return group(
        "alert-group",
        summary="Manage DolphinScheduler alert groups.",
        commands=[
            command(
                "list",
                action="alert-group.list",
                summary=(
                    "List alert groups with optional filtering and pagination controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter alert groups by group name using the upstream "
                            "search value."
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
                action="alert-group.get",
                summary="Get one alert group by name or id.",
                arguments=[
                    argument(
                        "alert-group",
                        value_type="string",
                        description="Alert-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="alert-group.create",
                summary="Create one alert group.",
                options=[
                    option(
                        "name",
                        value_type="string",
                        description="Alert-group name.",
                        required=True,
                    ),
                    option(
                        "instance-id",
                        value_type="integer",
                        description=(
                            "Alert plugin instance id to bind to this group. "
                            "Repeat as needed."
                        ),
                        multiple=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional alert-group description.",
                    ),
                ],
            ),
            command(
                "update",
                action="alert-group.update",
                summary="Update one alert group by name or id.",
                arguments=[
                    argument(
                        "alert-group",
                        value_type="string",
                        description="Alert-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "name",
                        value_type="string",
                        description=(
                            "Updated alert-group name. Omit to keep the current name."
                        ),
                    ),
                    option(
                        "instance-id",
                        value_type="integer",
                        description=(
                            "Alert plugin instance id to bind to this group. "
                            "Repeat as needed."
                        ),
                        multiple=True,
                    ),
                    option(
                        "clear-instance-ids",
                        value_type="boolean",
                        description="Clear all bound alert plugin instance ids.",
                        default=False,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated alert-group description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored alert-group description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="alert-group.delete",
                summary="Delete one alert group by name or id.",
                arguments=[
                    argument(
                        "alert-group",
                        value_type="string",
                        description="Alert-group name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm alert-group deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def tenant_group() -> dict[str, object]:
    """Build the tenant command group schema."""
    return group(
        "tenant",
        summary="Manage DolphinScheduler tenants.",
        commands=[
            command(
                "list",
                action="tenant.list",
                summary="List tenants with optional filtering and pagination controls.",
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter tenants by tenant code using the upstream search "
                            "value."
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
                action="tenant.get",
                summary="Get one tenant by code or id.",
                arguments=[
                    argument(
                        "tenant",
                        value_type="string",
                        description="Tenant code or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="tenant.create",
                summary="Create one tenant.",
                options=[
                    option(
                        "tenant-code",
                        value_type="string",
                        description="Tenant code.",
                        required=True,
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description="Queue name or numeric id to bind to this tenant.",
                        required=True,
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Optional tenant description.",
                    ),
                ],
            ),
            command(
                "update",
                action="tenant.update",
                summary="Update one tenant by code or id.",
                arguments=[
                    argument(
                        "tenant",
                        value_type="string",
                        description="Tenant code or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "tenant-code",
                        value_type="string",
                        description=(
                            "Updated tenant code. Omit to keep the current tenant code."
                        ),
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description=(
                            "Updated queue name or numeric id. Omit to keep the "
                            "current queue."
                        ),
                    ),
                    option(
                        "description",
                        value_type="string",
                        description="Updated tenant description.",
                    ),
                    option(
                        "clear-description",
                        value_type="boolean",
                        description="Clear the stored tenant description.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="tenant.delete",
                summary="Delete one tenant by code or id.",
                arguments=[
                    argument(
                        "tenant",
                        value_type="string",
                        description="Tenant code or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm tenant deletion without prompting.",
                        default=False,
                    )
                ],
            ),
        ],
    )


def user_group() -> dict[str, object]:
    """Build the user command group schema."""
    return group(
        "user",
        summary="Manage DolphinScheduler users.",
        commands=[
            command(
                "list",
                action="user.list",
                summary="List users with optional filtering and pagination controls.",
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter users by user name using the upstream search value."
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
                action="user.get",
                summary="Get one user by user name or id.",
                arguments=[
                    argument(
                        "user",
                        value_type="string",
                        description="User name or numeric id.",
                        selector="name_or_id",
                    )
                ],
            ),
            command(
                "create",
                action="user.create",
                summary="Create one user.",
                options=[
                    option(
                        "user-name",
                        value_type="string",
                        description="User name.",
                        required=True,
                    ),
                    option(
                        "password",
                        value_type="string",
                        description="Plain-text user password.",
                        required=True,
                    ),
                    option(
                        "email",
                        value_type="string",
                        description="User email.",
                        required=True,
                    ),
                    option(
                        "tenant",
                        value_type="string",
                        description="Tenant code or numeric id.",
                        required=True,
                    ),
                    option(
                        "state",
                        value_type="integer",
                        description="User state. Use 1 for enabled and 0 for disabled.",
                        required=True,
                        choices=[0, 1],
                    ),
                    option(
                        "phone",
                        value_type="string",
                        description="Optional user phone.",
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description="Optional queue-name override stored on the user.",
                    ),
                ],
            ),
            command(
                "update",
                action="user.update",
                summary="Update one user by user name or id.",
                arguments=[
                    argument(
                        "user",
                        value_type="string",
                        description="User name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "user-name",
                        value_type="string",
                        description="Updated user name.",
                    ),
                    option(
                        "password",
                        value_type="string",
                        description="Updated plain-text user password.",
                    ),
                    option(
                        "email",
                        value_type="string",
                        description="Updated user email.",
                    ),
                    option(
                        "tenant",
                        value_type="string",
                        description="Updated tenant code or numeric id.",
                    ),
                    option(
                        "state",
                        value_type="integer",
                        description=(
                            "Updated user state. Use 1 for enabled and 0 for disabled."
                        ),
                        choices=[0, 1],
                    ),
                    option(
                        "phone",
                        value_type="string",
                        description="Updated user phone.",
                    ),
                    option(
                        "clear-phone",
                        value_type="boolean",
                        description="Clear the stored user phone.",
                        default=False,
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description="Updated queue-name override stored on the user.",
                    ),
                    option(
                        "clear-queue",
                        value_type="boolean",
                        description="Clear the stored queue-name override.",
                        default=False,
                    ),
                    option(
                        "time-zone",
                        value_type="string",
                        description="Updated IANA time zone.",
                    ),
                ],
            ),
            command(
                "delete",
                action="user.delete",
                summary="Delete one user by user name or id.",
                arguments=[
                    argument(
                        "user",
                        value_type="string",
                        description="User name or numeric id.",
                        selector="name_or_id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm user deletion without prompting.",
                        default=False,
                    )
                ],
            ),
            group(
                "grant",
                summary="Grant DolphinScheduler user permissions.",
                commands=[
                    command(
                        "project",
                        action="user.grant.project",
                        summary="Grant one project to one user with write permission.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            ),
                            argument(
                                "project",
                                value_type="string",
                                description="Project name or numeric code.",
                                selector="name_or_code",
                            ),
                        ],
                    ),
                    command(
                        "datasource",
                        action="user.grant.datasource",
                        summary="Grant one or more datasources to one user.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            )
                        ],
                        options=[
                            option(
                                "datasource",
                                value_type="string",
                                description=(
                                    "Datasource name or numeric id. Repeat to grant "
                                    "multiple datasources."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                            )
                        ],
                    ),
                    command(
                        "namespace",
                        action="user.grant.namespace",
                        summary="Grant one or more namespaces to one user.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            )
                        ],
                        options=[
                            option(
                                "namespace",
                                value_type="string",
                                description=(
                                    "Namespace name or numeric id. Repeat to grant "
                                    "multiple namespaces."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                            )
                        ],
                    ),
                ],
            ),
            group(
                "revoke",
                summary="Revoke DolphinScheduler user permissions.",
                commands=[
                    command(
                        "project",
                        action="user.revoke.project",
                        summary="Revoke one project from one user.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            ),
                            argument(
                                "project",
                                value_type="string",
                                description="Project name or numeric code.",
                                selector="name_or_code",
                            ),
                        ],
                    ),
                    command(
                        "datasource",
                        action="user.revoke.datasource",
                        summary="Revoke one or more datasources from one user.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            )
                        ],
                        options=[
                            option(
                                "datasource",
                                value_type="string",
                                description=(
                                    "Datasource name or numeric id. Repeat to revoke "
                                    "multiple datasources."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                            )
                        ],
                    ),
                    command(
                        "namespace",
                        action="user.revoke.namespace",
                        summary="Revoke one or more namespaces from one user.",
                        arguments=[
                            argument(
                                "user",
                                value_type="string",
                                description="User name or numeric id.",
                                selector="name_or_id",
                            )
                        ],
                        options=[
                            option(
                                "namespace",
                                value_type="string",
                                description=(
                                    "Namespace name or numeric id. Repeat to revoke "
                                    "multiple namespaces."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                            )
                        ],
                    ),
                ],
            ),
        ],
    )


def access_token_group() -> dict[str, object]:
    """Build the access-token command group schema."""
    return group(
        "access-token",
        summary="Manage DolphinScheduler access tokens.",
        commands=[
            command(
                "list",
                action="access-token.list",
                summary=(
                    "List access tokens with optional filtering and pagination "
                    "controls."
                ),
                options=[
                    option(
                        "search",
                        value_type="string",
                        description=(
                            "Filter access tokens using the upstream search value."
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
                action="access-token.get",
                summary="Get one access token by numeric id.",
                arguments=[
                    argument(
                        "access-token",
                        value_type="integer",
                        description="Access-token id.",
                        selector="id",
                    )
                ],
            ),
            command(
                "create",
                action="access-token.create",
                summary="Create one access token.",
                options=[
                    option(
                        "user",
                        value_type="string",
                        description="User name or numeric id.",
                        selector="name_or_id",
                        required=True,
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description="Token expiration time.",
                        required=True,
                    ),
                    option(
                        "token",
                        value_type="string",
                        description=(
                            "Optional token string. Omit to let DS generate one."
                        ),
                    ),
                ],
            ),
            command(
                "update",
                action="access-token.update",
                summary="Update one access token by numeric id.",
                arguments=[
                    argument(
                        "access-token",
                        value_type="integer",
                        description="Access-token id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "user",
                        value_type="string",
                        description="Updated user name or numeric id.",
                        selector="name_or_id",
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description="Updated token expiration time.",
                    ),
                    option(
                        "token",
                        value_type="string",
                        description="Updated token string.",
                    ),
                    option(
                        "regenerate-token",
                        value_type="boolean",
                        description="Ask DS to generate a fresh token string.",
                        default=False,
                    ),
                ],
            ),
            command(
                "delete",
                action="access-token.delete",
                summary="Delete one access token by numeric id.",
                arguments=[
                    argument(
                        "access-token",
                        value_type="integer",
                        description="Access-token id.",
                        selector="id",
                    )
                ],
                options=[
                    option(
                        "force",
                        value_type="boolean",
                        description="Confirm access-token deletion without prompting.",
                        default=False,
                    )
                ],
            ),
            command(
                "generate",
                action="access-token.generate",
                summary="Generate one token string without persisting it.",
                options=[
                    option(
                        "user",
                        value_type="string",
                        description="User name or numeric id.",
                        selector="name_or_id",
                        required=True,
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description="Token expiration time.",
                        required=True,
                    ),
                ],
            ),
        ],
    )
