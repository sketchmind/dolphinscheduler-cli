from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from dsctl.cli_surface import TASK_RESOURCE
from dsctl.errors import ApiTransportError, InvalidStateError
from dsctl.models.workflow_patch import validate_workflow_patch_document
from dsctl.upstream.definition_models import NativeCode
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_definition_wire import task_update_contract_features
from dsctl.upstream.wire import WireContractError
from dsctl.upstream.workflow_graph_preservation import (
    WHOLE_WORKFLOW_TASK_REQUEST_FIELDS,
    preserved_task_update_arguments,
    workflow_graph_fingerprint,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.models.workflow_patch import (
        WorkflowPatchSpec,
        WorkflowPatchTaskSetSpec,
    )
    from dsctl.output import JsonObject
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.definition_models import WorkflowScope
    from dsctl.upstream.protocol import WorkflowDagRecord
    from dsctl.upstream.resolver import ResolvedProject
    from dsctl.upstream.task_definitions import (
        PreparedWholeWorkflowTaskMutation as PreparedWholeWorkflowTaskMutationToken,
    )
    from dsctl.upstream.wire import WireRequest


class PreparedWorkflowUpdate(Protocol):
    """Exact workflow prepared call consumed by the fallback strategy."""

    @property
    def request(self) -> WireRequest:
        """Return the captured exact workflow request."""
        ...


class WorkflowUpdateOperations(Protocol):
    """Narrow exact workflow seam needed by one task-update epoch."""

    @property
    def ds_version(self) -> str:
        """Return the bound DolphinScheduler profile version."""
        ...

    def prepare_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> PreparedWorkflowUpdate:
        """Capture one exact whole-workflow update without transport."""
        ...

    def apply_update(self, prepared: PreparedWorkflowUpdate) -> None:
        """Execute one captured whole-workflow update exactly once."""
        ...


class WorkflowMutationCompiler(Protocol):
    """Pure workflow compiler injected from the stable mutation module."""

    def __call__(
        self,
        dag: WorkflowDagRecord,
        *,
        project: ResolvedProject,
        patch: WorkflowPatchSpec,
        release_state: str | None,
        catalog: TaskAuthoringCatalog | None = None,
    ) -> WorkflowMutationPlan:
        """Compile a complete graph update from one canonical task patch."""
        ...


@dataclass(frozen=True)
class PreparedWholeWorkflowTaskMutation:
    """One exact whole-definition task update captured before mutation."""

    profile_version: str
    request: WireRequest
    initial_graph_fingerprint: str
    has_changes: bool
    _prepared_update: PreparedWorkflowUpdate = field(repr=False, compare=False)
    _operations: WorkflowUpdateOperations = field(repr=False, compare=False)


@dataclass(frozen=True)
class CodeNativeWholeWorkflowTaskUpdate:
    """Adapt task intent through a reviewed exact atomic workflow update."""

    profile_version: str
    operations: WorkflowUpdateOperations
    catalog: TaskAuthoringCatalog
    compile_update: WorkflowMutationCompiler
    task_request_fields: frozenset[str]

    def __post_init__(self) -> None:
        """Keep this exceptional recipe exact and profile-bound."""
        if not task_update_contract_features(
            self.profile_version
        ).whole_workflow_update:
            message = (
                "The selected exact task contract has no whole-workflow update strategy"
            )
            raise ValueError(message)
        if self.operations.ds_version != self.profile_version:
            message = "Whole-workflow task operations do not match the profile"
            raise ValueError(message)
        missing = WHOLE_WORKFLOW_TASK_REQUEST_FIELDS.difference(
            self.task_request_fields
        )
        if missing:
            message = (
                "Whole-workflow task recipe is missing exact request fields: "
                f"{sorted(missing)!r}"
            )
            raise ValueError(message)

    def prepare(
        self,
        scope: WorkflowScope,
        *,
        dag: WorkflowDagRecord,
        dag_raw: JsonObject,
        project: ResolvedProject,
        task_name: str,
        patch: WorkflowPatchTaskSetSpec,
        requested_fields: Sequence[str],
    ) -> PreparedWholeWorkflowTaskMutation:
        """Compile the stable patch and capture its exact process update call."""
        self._require_code_scope(scope)
        release_state = _workflow_release_state(scope, dag=dag)
        if release_state == "ONLINE":
            message = "Task update requires the containing workflow to be offline"
            raise InvalidStateError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "selected_version": self.profile_version,
                    "project_code": scope.project.native.value,
                    "workflow_code": scope.workflow.native.value,
                    "task": task_name,
                    "release_state": release_state,
                    "mutation_applied": False,
                },
                suggestion=(
                    "Bring the workflow offline before retrying `task update`."
                ),
            )
        current_group_id = next(
            (
                task.taskGroupId
                for task in dag.taskDefinitionList or ()
                if task.name == task_name
            ),
            None,
        )
        mutation = _task_patch(
            task_name, patch=patch, current_group_id=current_group_id
        )
        plan = self.compile_update(
            dag,
            project=project,
            patch=mutation,
            release_state=release_state,
            catalog=self.catalog,
        )
        if plan.compilation.required_task_code_count:
            message = "Task update unexpectedly required a new task identity"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "selected_version": self.profile_version,
                    "project_code": scope.project.native.value,
                    "workflow_code": scope.workflow.native.value,
                    "task": task_name,
                    "mutation_applied": False,
                },
            )
        compiled = plan.compilation.materialize(())
        features = task_update_contract_features(self.profile_version)
        arguments = preserved_task_update_arguments(
            compiled,
            dag=dag,
            dag_raw=dag_raw,
            task_name=task_name,
            requested_fields=requested_fields,
            workflow_field=features.dag_workflow_field,
            relation_field=features.dag_relation_field,
        )
        prepared = self.operations.prepare_update(scope, **arguments)
        return PreparedWholeWorkflowTaskMutation(
            profile_version=self.profile_version,
            request=prepared.request,
            initial_graph_fingerprint=self.fingerprint(dag, dag_raw=dag_raw),
            has_changes=plan.has_changes,
            _prepared_update=prepared,
            _operations=self.operations,
        )

    def fingerprint(
        self,
        dag: WorkflowDagRecord,
        *,
        dag_raw: JsonObject,
    ) -> str:
        """Hash the lossless raw DAG, falling back only for local test doubles."""
        features = task_update_contract_features(self.profile_version)
        return workflow_graph_fingerprint(
            dag,
            dag_raw=dag_raw,
            workflow_field=features.dag_workflow_field,
            relation_field=features.dag_relation_field,
        )

    def apply(self, prepared: PreparedWholeWorkflowTaskMutationToken) -> None:
        """Apply exactly the prepared request after ownership validation."""
        if not isinstance(prepared, PreparedWholeWorkflowTaskMutation):
            message = "Prepared whole-workflow task update has an invalid token"
            raise WireContractError(message)
        if (
            prepared.profile_version != self.profile_version
            or prepared._operations is not self.operations
        ):
            message = "Prepared whole-workflow task update belongs to another recipe"
            raise WireContractError(message)
        self.operations.apply_update(prepared._prepared_update)

    @staticmethod
    def _require_code_scope(scope: WorkflowScope) -> None:
        if isinstance(scope.project.native, NativeCode) and isinstance(
            scope.workflow.native,
            NativeCode,
        ):
            return
        message = (
            "Whole-workflow task update requires code-native "
            "project and workflow identities"
        )
        raise WireContractError(message)


def _task_patch(
    task_name: str,
    *,
    patch: WorkflowPatchTaskSetSpec,
    current_group_id: int | None,
) -> WorkflowPatchSpec:
    task_set = patch.model_dump(mode="python", exclude_unset=True)
    if (
        "task_group_id" in patch.model_fields_set
        and "task_group_priority" not in patch.model_fields_set
    ):
        if patch.task_group_id is None:
            task_set["task_group_priority"] = None
        elif patch.task_group_id != current_group_id:
            task_set["task_group_priority"] = 0
    return validate_workflow_patch_document(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": task_name},
                            "set": task_set,
                        }
                    ]
                }
            }
        }
    ).patch


def _workflow_release_state(
    scope: WorkflowScope,
    *,
    dag: WorkflowDagRecord,
) -> str | None:
    if scope.view.release_state is not None:
        return scope.view.release_state
    workflow = dag.workflowDefinition
    return None if workflow is None else enum_value(workflow.releaseState)


__all__ = [
    "CodeNativeWholeWorkflowTaskUpdate",
    "PreparedWholeWorkflowTaskMutation",
]
