"""Command-facing workflow operations."""

from dsctl.services.workflow.create import (
    create_workflow_result,
)
from dsctl.services.workflow.edit import (
    edit_workflow_result,
)
from dsctl.services.workflow.execution import (
    backfill_workflow_result,
    run_workflow_result,
    run_workflow_task_result,
)
from dsctl.services.workflow.lifecycle import (
    delete_workflow_result,
    offline_workflow_result,
    online_workflow_result,
)
from dsctl.services.workflow.reads import (
    describe_workflow_result,
    digest_workflow_result,
    export_workflow_yaml_result,
    get_workflow_result,
    list_workflows_result,
)

__all__ = [
    "backfill_workflow_result",
    "create_workflow_result",
    "delete_workflow_result",
    "describe_workflow_result",
    "digest_workflow_result",
    "edit_workflow_result",
    "export_workflow_yaml_result",
    "get_workflow_result",
    "list_workflows_result",
    "offline_workflow_result",
    "online_workflow_result",
    "run_workflow_result",
    "run_workflow_task_result",
]
