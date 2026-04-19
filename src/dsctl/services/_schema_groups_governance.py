from __future__ import annotations

from dsctl.services._schema_primitives import argument, command, group, option
from dsctl.services.datasource_payload import datasource_payload_command_data
from dsctl.services.pagination import DEFAULT_PAGE_SIZE


def env_group() -> dict[str, object]:
    """Build the environment command group schema."""
    return group(
        "environment",
        summary="Manage DolphinScheduler environments.",
        commands=[
            command(
                "list",
                action="environment.list",
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
                action="environment.get",
                summary="Get one environment by name or code.",
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description=(
                            "Environment name or numeric code. Run `dsctl "
                            "environment list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl environment list",
                    )
                ],
            ),
            command(
                "create",
                action="environment.create",
                summary="Create one environment; pass --config or --config-file.",
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
                        description=(
                            "Inline DS environment shell/export config. Prefer "
                            "--config-file for multiline configs."
                        ),
                        examples=["export JAVA_HOME=/opt/java"],
                        discovery_command="dsctl template environment",
                    ),
                    option(
                        "config-file",
                        value_type="path",
                        description=(
                            "Path to a DS environment shell/export config file."
                        ),
                        discovery_command="dsctl template environment",
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
                            "needed; run `dsctl worker-group list` to discover "
                            "values."
                        ),
                        multiple=True,
                        discovery_command="dsctl worker-group list",
                    ),
                ],
            ),
            command(
                "update",
                action="environment.update",
                summary=(
                    "Update one environment by name or code; config may come "
                    "from --config-file."
                ),
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description=(
                            "Environment name or numeric code. Run `dsctl "
                            "environment list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl environment list",
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
                            "Updated inline DS environment shell/export config. "
                            "Omit to keep the current config; prefer --config-file "
                            "for multiline configs."
                        ),
                        examples=["export JAVA_HOME=/opt/java"],
                        discovery_command="dsctl template environment",
                    ),
                    option(
                        "config-file",
                        value_type="path",
                        description=(
                            "Path to an updated DS environment shell/export config "
                            "file. Omit both config options to keep the current "
                            "config."
                        ),
                        discovery_command="dsctl template environment",
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
                            "needed; run `dsctl worker-group list` to discover "
                            "values."
                        ),
                        multiple=True,
                        discovery_command="dsctl worker-group list",
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
                action="environment.delete",
                summary="Delete one environment by name or code.",
                arguments=[
                    argument(
                        "environment",
                        value_type="string",
                        description=(
                            "Environment name or numeric code. Run `dsctl "
                            "environment list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl environment list",
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
                        description=(
                            "Cluster name or numeric code. Run `dsctl cluster "
                            "list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl cluster list",
                    )
                ],
            ),
            command(
                "create",
                action="cluster.create",
                summary="Create one cluster; pass --config or --config-file.",
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
                        description=(
                            "Inline DS cluster config JSON. Prefer --config-file "
                            "for multiline Kubernetes configs."
                        ),
                        discovery_command="dsctl template cluster",
                    ),
                    option(
                        "config-file",
                        value_type="path",
                        description="Path to one DS cluster config JSON file.",
                        discovery_command="dsctl template cluster",
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
                        description=(
                            "Cluster name or numeric code. Run `dsctl cluster "
                            "list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl cluster list",
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
                            "Updated inline DS cluster config JSON. Omit to keep "
                            "the current config; prefer --config-file for "
                            "multiline Kubernetes configs."
                        ),
                        discovery_command="dsctl template cluster",
                    ),
                    option(
                        "config-file",
                        value_type="path",
                        description=(
                            "Path to an updated DS cluster config JSON file. Omit "
                            "both config options to keep the current config."
                        ),
                        discovery_command="dsctl template cluster",
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
                        description=(
                            "Cluster name or numeric code. Run `dsctl cluster "
                            "list` to discover values."
                        ),
                        selector="name_or_code",
                        discovery_command="dsctl cluster list",
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
        summary=(
            "Manage DolphinScheduler datasources. Create/update use DS-native "
            "JSON payload files."
        ),
        commands=[
            command(
                "list",
                action="datasource.list",
                summary="List datasource identities and summary fields.",
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
                        description=(
                            "Datasource name or numeric id. Run `dsctl "
                            "datasource list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl datasource list",
                    )
                ],
            ),
            command(
                "create",
                action="datasource.create",
                summary="Create one datasource from a JSON payload file.",
                payload=datasource_payload_command_data(),
                options=[
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one DS-native datasource JSON payload file."
                        ),
                        required=True,
                        value_name="PATH",
                        discovery_command="dsctl template datasource",
                    )
                ],
            ),
            command(
                "update",
                action="datasource.update",
                summary="Update one datasource from a JSON payload file.",
                payload=datasource_payload_command_data(),
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description=(
                            "Datasource name or numeric id. Run `dsctl "
                            "datasource list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl datasource list",
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
                        discovery_command="dsctl template datasource",
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
                        description=(
                            "Datasource name or numeric id. Run `dsctl "
                            "datasource list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl datasource list",
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
                summary="Run one datasource connection test after create or update.",
                arguments=[
                    argument(
                        "datasource",
                        value_type="string",
                        description=(
                            "Datasource name or numeric id. Run `dsctl "
                            "datasource list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl datasource list",
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
                            "base directory; run `dsctl resource list` to discover "
                            "paths."
                        ),
                        discovery_command="dsctl resource list",
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
                        description=(
                            "DS resource fullName path. Run "
                            "`dsctl resource list --dir DIR` to discover paths."
                        ),
                        selector="resource_path",
                        discovery_command="dsctl resource list --dir DIR",
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
                            "upstream base directory; run `dsctl resource list` to "
                            "discover paths."
                        ),
                        discovery_command="dsctl resource list",
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
                            "file. For local files, use "
                            "`dsctl resource upload --file PATH`."
                        ),
                        required=True,
                    ),
                    option(
                        "dir",
                        value_type="string",
                        description=(
                            "Destination DS directory fullName path. Defaults to the "
                            "upstream base directory; run `dsctl resource list` to "
                            "discover paths."
                        ),
                        discovery_command="dsctl resource list",
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
                            "upstream base directory; run `dsctl resource list` to "
                            "discover paths."
                        ),
                        discovery_command="dsctl resource list",
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
                        description=(
                            "DS resource fullName path. Run "
                            "`dsctl resource list --dir DIR` to discover paths."
                        ),
                        selector="resource_path",
                        discovery_command="dsctl resource list --dir DIR",
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
                        description=(
                            "DS resource fullName path. Run "
                            "`dsctl resource list --dir DIR` to discover paths."
                        ),
                        selector="resource_path",
                        discovery_command="dsctl resource list --dir DIR",
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
                        description=(
                            "Namespace name or numeric id. Run `dsctl namespace "
                            "list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl namespace list",
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
                        description=(
                            "Owning cluster code. Run `dsctl cluster list` "
                            "to discover codes."
                        ),
                        required=True,
                        discovery_command="dsctl cluster list",
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
                        description=(
                            "Namespace name or numeric id. Run `dsctl namespace "
                            "list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl namespace list",
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
                        description=(
                            "Queue name or numeric id. Run `dsctl queue list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl queue list",
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
                        description=(
                            "Human-facing DS queue name used as the selector label."
                        ),
                        required=True,
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description="Underlying YARN queue value stored in DS.",
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
                        description=(
                            "Queue name or numeric id. Run `dsctl queue list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl queue list",
                    )
                ],
                options=[
                    option(
                        "queue-name",
                        value_type="string",
                        description=(
                            "Updated human-facing DS queue name. Omit to keep the "
                            "current queue name."
                        ),
                    ),
                    option(
                        "queue",
                        value_type="string",
                        description=(
                            "Updated underlying YARN queue value. Omit to keep the "
                            "current queue value."
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
                        description=(
                            "Queue name or numeric id. Run `dsctl queue list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl queue list",
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
                        description=(
                            "Worker-group name or numeric id. Run `dsctl "
                            "worker-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl worker-group list",
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
                            "Worker server address to include in addrList. Repeat "
                            "as needed; run `dsctl monitor server worker` to "
                            "discover workers."
                        ),
                        multiple=True,
                        discovery_command="dsctl monitor server worker",
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
                        description=(
                            "Worker-group name or numeric id. Run `dsctl "
                            "worker-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl worker-group list",
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
                            "Replacement worker address list. Repeat as needed; "
                            "run `dsctl monitor server worker` to discover workers."
                        ),
                        multiple=True,
                        discovery_command="dsctl monitor server worker",
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
                        description=(
                            "Worker-group name or numeric id. Run `dsctl "
                            "worker-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl worker-group list",
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
                        description=(
                            "Project name or code for project-scoped listing. "
                            "Run `dsctl project list` to discover values."
                        ),
                        discovery_command="dsctl project list",
                    ),
                    option(
                        "search",
                        value_type="string",
                        description="Filter task groups by task-group name.",
                    ),
                    option(
                        "status",
                        value_type="string",
                        description="Filter task groups by status.",
                        choices=["open", "closed", "1", "0"],
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
                        description=(
                            "Task-group name or numeric id. Run `dsctl "
                            "task-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl task-group list",
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
                            "project context; run `dsctl project list` to "
                            "discover values."
                        ),
                        discovery_command="dsctl project list",
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
                        description=(
                            "Task-group name or numeric id. Run `dsctl "
                            "task-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl task-group list",
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
                        description=(
                            "Task-group name or numeric id. Run `dsctl "
                            "task-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl task-group list",
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
                        description=(
                            "Task-group name or numeric id. Run `dsctl "
                            "task-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl task-group list",
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
                                description=(
                                    "Task-group name or numeric id. Run `dsctl "
                                    "task-group list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl task-group list",
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
                                description="Filter by task-group queue status.",
                                choices=[
                                    "WAIT_QUEUE",
                                    "ACQUIRE_SUCCESS",
                                    "RELEASE",
                                    "-1",
                                    "1",
                                    "2",
                                ],
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
                                description=(
                                    "Numeric task-group queue id. Run "
                                    "`dsctl task-group queue list TASK_GROUP` "
                                    "to discover ids."
                                ),
                                selector="id",
                                discovery_command=(
                                    "dsctl task-group queue list TASK_GROUP"
                                ),
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
                                description=(
                                    "Numeric task-group queue id. Run "
                                    "`dsctl task-group queue list TASK_GROUP` "
                                    "to discover ids."
                                ),
                                selector="id",
                                discovery_command=(
                                    "dsctl task-group queue list TASK_GROUP"
                                ),
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
                        description=(
                            "Alert-plugin instance name or numeric id. Run "
                            "`dsctl alert-plugin list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin list",
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
                        description=(
                            "Alert UI plugin definition name or numeric id. Run "
                            "`dsctl alert-plugin definition list` to discover "
                            "values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin definition list",
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
                        description=(
                            "Alert UI plugin definition name or numeric id. Run "
                            "`dsctl alert-plugin definition list` to discover "
                            "values."
                        ),
                        required=True,
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin definition list",
                    ),
                    option(
                        "params-json",
                        value_type="string",
                        description=(
                            "DS-native alert-plugin UI params JSON array. Run "
                            "`dsctl alert-plugin schema PLUGIN` to inspect fields."
                        ),
                        discovery_command="dsctl alert-plugin schema PLUGIN",
                    ),
                    option(
                        "param",
                        value_type="string",
                        description=(
                            "Alert-plugin UI param in KEY=VALUE form. Repeat for "
                            "multiple fields; run "
                            "`dsctl alert-plugin schema PLUGIN` to inspect keys."
                        ),
                        value_name="KEY=VALUE",
                        multiple=True,
                        discovery_command="dsctl alert-plugin schema PLUGIN",
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one DS-native alert-plugin UI params JSON file. "
                            "Run `dsctl alert-plugin schema PLUGIN` to inspect "
                            "fields."
                        ),
                        value_name="PATH",
                        discovery_command="dsctl alert-plugin schema PLUGIN",
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
                        description=(
                            "Alert-plugin instance name or numeric id. Run "
                            "`dsctl alert-plugin list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin list",
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
                        discovery_command="dsctl alert-plugin schema PLUGIN",
                    ),
                    option(
                        "param",
                        value_type="string",
                        description=(
                            "Replacement alert-plugin UI param in KEY=VALUE form. "
                            "Repeat for multiple fields; omitted fields keep "
                            "current values. Run "
                            "`dsctl alert-plugin schema PLUGIN` to inspect keys."
                        ),
                        value_name="KEY=VALUE",
                        multiple=True,
                        discovery_command="dsctl alert-plugin schema PLUGIN",
                    ),
                    option(
                        "file",
                        value_type="path",
                        description=(
                            "Path to one replacement DS-native alert-plugin UI "
                            "params JSON file. Run "
                            "`dsctl alert-plugin schema PLUGIN` to inspect fields."
                        ),
                        value_name="PATH",
                        discovery_command="dsctl alert-plugin schema PLUGIN",
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
                        description=(
                            "Alert-plugin instance name or numeric id. Run "
                            "`dsctl alert-plugin list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin list",
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
                        description=(
                            "Alert-plugin instance name or numeric id. Run "
                            "`dsctl alert-plugin list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-plugin list",
                    )
                ],
            ),
            group(
                "definition",
                summary=(
                    "Discover supported alert-plugin definitions, not configured "
                    "alert-plugin instances."
                ),
                commands=[
                    command(
                        "list",
                        action="alert-plugin.definition.list",
                        summary=(
                            "List alert-plugin definitions supported by the "
                            "current DolphinScheduler runtime."
                        ),
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
                        description=(
                            "Alert-group name or numeric id. Run `dsctl "
                            "alert-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-group list",
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
                            "Repeat as needed; run `dsctl alert-plugin list` "
                            "to discover ids."
                        ),
                        multiple=True,
                        discovery_command="dsctl alert-plugin list",
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
                        description=(
                            "Alert-group name or numeric id. Run `dsctl "
                            "alert-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-group list",
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
                            "Repeat as needed; run `dsctl alert-plugin list` "
                            "to discover ids."
                        ),
                        multiple=True,
                        discovery_command="dsctl alert-plugin list",
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
                        description=(
                            "Alert-group name or numeric id. Run `dsctl "
                            "alert-group list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl alert-group list",
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
                        description=(
                            "Tenant code or numeric id. Run `dsctl tenant list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl tenant list",
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
                        description=(
                            "Queue name or numeric id to bind to this tenant. "
                            "Run `dsctl queue list` to discover values."
                        ),
                        required=True,
                        discovery_command="dsctl queue list",
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
                        description=(
                            "Tenant code or numeric id. Run `dsctl tenant list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl tenant list",
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
                            "Updated queue name or numeric id. Run `dsctl queue "
                            "list` to discover values; omit to keep the current "
                            "queue."
                        ),
                        discovery_command="dsctl queue list",
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
                        description=(
                            "Tenant code or numeric id. Run `dsctl tenant list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl tenant list",
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
                        description=(
                            "User name or numeric id. Run `dsctl user list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl user list",
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
                        description=(
                            "Tenant code or numeric id. Run `dsctl tenant list` "
                            "to discover values."
                        ),
                        required=True,
                        discovery_command="dsctl tenant list",
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
                        description=(
                            "Optional queue-name override stored on the user. "
                            "Run `dsctl queue list` to discover queue names."
                        ),
                        discovery_command="dsctl queue list",
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
                        description=(
                            "User name or numeric id. Run `dsctl user list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl user list",
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
                        description=(
                            "Updated tenant code or numeric id. Run `dsctl tenant "
                            "list` to discover values."
                        ),
                        discovery_command="dsctl tenant list",
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
                        description=(
                            "Updated queue-name override stored on the user. "
                            "Run `dsctl queue list` to discover queue names."
                        ),
                        discovery_command="dsctl queue list",
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
                        description=(
                            "User name or numeric id. Run `dsctl user list` "
                            "to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl user list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            ),
                            argument(
                                "project",
                                value_type="string",
                                description=(
                                    "Project name or numeric code. Run `dsctl "
                                    "project list` to discover values."
                                ),
                                selector="name_or_code",
                                discovery_command="dsctl project list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            )
                        ],
                        options=[
                            option(
                                "datasource",
                                value_type="string",
                                description=(
                                    "Datasource name or numeric id. Repeat to grant "
                                    "multiple datasources; run `dsctl datasource "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                                discovery_command="dsctl datasource list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            )
                        ],
                        options=[
                            option(
                                "namespace",
                                value_type="string",
                                description=(
                                    "Namespace name or numeric id. Repeat to grant "
                                    "multiple namespaces; run `dsctl namespace "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                                discovery_command="dsctl namespace list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            ),
                            argument(
                                "project",
                                value_type="string",
                                description=(
                                    "Project name or numeric code. Run `dsctl "
                                    "project list` to discover values."
                                ),
                                selector="name_or_code",
                                discovery_command="dsctl project list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            )
                        ],
                        options=[
                            option(
                                "datasource",
                                value_type="string",
                                description=(
                                    "Datasource name or numeric id. Repeat to revoke "
                                    "multiple datasources; run `dsctl datasource "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                                discovery_command="dsctl datasource list",
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
                                description=(
                                    "User name or numeric id. Run `dsctl user "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                discovery_command="dsctl user list",
                            )
                        ],
                        options=[
                            option(
                                "namespace",
                                value_type="string",
                                description=(
                                    "Namespace name or numeric id. Repeat to revoke "
                                    "multiple namespaces; run `dsctl namespace "
                                    "list` to discover values."
                                ),
                                selector="name_or_id",
                                multiple=True,
                                required=True,
                                discovery_command="dsctl namespace list",
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
                        description=(
                            "Access-token id. Run `dsctl access-token list` "
                            "to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl access-token list",
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
                        description=(
                            "User name or numeric id. Run `dsctl user list` to "
                            "discover values."
                        ),
                        selector="name_or_id",
                        required=True,
                        discovery_command="dsctl user list",
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description=(
                            "Token expiration time, for example '2027-01-01 00:00:00'."
                        ),
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
                        description=(
                            "Access-token id. Run `dsctl access-token list` "
                            "to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl access-token list",
                    )
                ],
                options=[
                    option(
                        "user",
                        value_type="string",
                        description=(
                            "Updated user name or numeric id. Run `dsctl user "
                            "list` to discover values."
                        ),
                        selector="name_or_id",
                        discovery_command="dsctl user list",
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description=(
                            "Updated token expiration time, for example "
                            "'2027-01-01 00:00:00'."
                        ),
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
                        description=(
                            "Access-token id. Run `dsctl access-token list` "
                            "to discover values."
                        ),
                        selector="id",
                        discovery_command="dsctl access-token list",
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
                        description=(
                            "User name or numeric id. Run `dsctl user list` to "
                            "discover values."
                        ),
                        selector="name_or_id",
                        required=True,
                        discovery_command="dsctl user list",
                    ),
                    option(
                        "expire-time",
                        value_type="string",
                        description=(
                            "Token expiration time, for example '2027-01-01 00:00:00'."
                        ),
                        required=True,
                    ),
                ],
            ),
        ],
    )
