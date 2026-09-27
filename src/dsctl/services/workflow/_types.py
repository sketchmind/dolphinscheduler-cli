from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict, TypeVar

from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
)
from dsctl.upstream.workflows import WorkflowDomain

if TYPE_CHECKING:
    from dsctl.services._schedule_support import (
        ScheduleConfirmationData,
    )
    from dsctl.services._workflow.render import (
        WorkflowData,
    )
    from dsctl.services.schedule_analysis import SchedulePreviewData


WorkflowRunTaskScope = Literal["self", "pre", "post"]


WorkflowBackfillTimeMode = Literal["range", "dates"]


WorkflowServiceRuntime = BoundDomainServiceRuntime[WorkflowDomain]


INTERNAL_SERVER_ERROR_ARGS = 10000


WORKFLOW_DEFINITION_NOT_EXIST = 50003


_LEGACY_PROCESS_INSTANCE_NOT_EXIST = 50001


WORKFLOW_DEFINITION_NOT_RELEASE = 50004


WORKFLOW_DEFINITION_NOT_ALLOWED_EDIT = 50008


START_WORKFLOW_INSTANCE_ERROR = 50014


PROJECT_NOT_FOUND = 10018


_PROJECT_NOT_EXIST = 10190


WORKFLOW_DEFINITION_NAME_EXIST = 10168


TASK_NAME_DUPLICATE_ERROR = 10198


DATA_IS_NOT_VALID = 50017


WORKFLOW_NODE_HAS_CYCLE = 50019


WORKFLOW_NODE_S_PARAMETER_INVALID = 50020


TASK_DEFINE_NOT_EXIST = 50030


CHECK_WORKFLOW_TASK_RELATION_ERROR = 50036


_USER_NO_OPERATION_PERMISSION = 30001


_USER_NO_OPERATION_PROJECT_PERMISSION = 30002


_WORKFLOW_CREATE_REVIEW_SUGGESTION = (
    "Lint the same workflow file and repeat the create command with --dry-run "
    "to inspect the workflow spec and compiled DS-native payload before retrying."
)


_WORKFLOW_EDIT_DRY_RUN_SUGGESTION = (
    "Retry the original workflow edit command with --dry-run to inspect the "
    "compiled diff and DS-native payload before sending it again."
)


_WORKFLOW_RUN_FAILURE_STRATEGIES = ("CONTINUE", "END")


_WORKFLOW_RUN_WARNING_TYPES = ("NONE", "SUCCESS", "FAILURE", "ALL")


_WORKFLOW_RUN_PRIORITIES = ("HIGHEST", "HIGH", "MEDIUM", "LOW", "LOWEST")


_WORKFLOW_BACKFILL_RUN_MODES = ("RUN_MODE_SERIAL", "RUN_MODE_PARALLEL")


_WORKFLOW_BACKFILL_COMPLEMENT_DEPENDENT_MODES = ("OFF_MODE", "ALL_DEPENDENT")


_WORKFLOW_BACKFILL_EXECUTION_ORDERS = ("DESC_ORDER", "ASC_ORDER")


_PreparedWorkflowT = TypeVar("_PreparedWorkflowT")


class WorkflowYamlExportData(TypedDict):
    """YAML export payload kept inside the standard JSON envelope."""

    yaml: str


class WorkflowRunTaskDependencyWarningDetail(TypedDict):
    """Structured warning for task-scoped workflow starts."""

    code: str
    message: str
    blocking: bool
    scope: str
    dependent_resolution: str


class WorkflowExecutionDryRunWarningDetail(TypedDict):
    """Structured warning for DS server-side workflow execution dry-run."""

    code: str
    message: str
    blocking: bool
    request_sent: bool


class DeleteWorkflowData(TypedDict):
    """Stable payload emitted for one workflow deletion."""

    deleted: bool
    workflow: WorkflowData


class WorkflowCreateScheduleDryRunData(TypedDict):
    """Dry-run metadata for one inline schedule block."""

    schedule_preview: SchedulePreviewData
    schedule_confirmation: ScheduleConfirmationData


class WorkflowEditConstraintData(TypedDict):
    """Structured workflow edit precondition or side-effect."""

    code: str
    message: str
    blocking: bool
    current_release_state: str | None
    required_release_state: str | None
    current_schedule_release_state: str | None


class WorkflowEditScheduleImpactData(TypedDict):
    """Structured schedule-impact note for workflow edit."""

    code: str
    message: str
    desired_workflow_release_state: str | None
    current_schedule_release_state: str | None


class WorkflowReleaseWarningDetail(TypedDict):
    """Structured warning emitted by workflow release actions."""

    code: str
    message: str
    action: Literal["online", "offline"]
    workflow_release_state: str | None
    schedule_release_state: str | None


class WorkflowEditNoChangeWarningDetail(TypedDict):
    """Structured warning emitted when one workflow patch changes nothing."""

    code: str
    message: str
    no_change: bool
    request_sent: bool
