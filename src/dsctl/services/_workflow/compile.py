from __future__ import annotations

from collections import deque
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from pydantic import ValidationError

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.models.common import first_validation_error_message
from dsctl.models.task_spec import (
    KUBEFLOW_WORKFLOW_INSTANCE_PARAMETER_NAME,
    KubeflowTfjobManifestTaskParamsSpec,
    TaskTimeoutNotifyStrategy,
)
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.output import require_json_object
from dsctl.services._task_code_allocation import preview_task_codes
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    default_task_authoring_catalog,
)
from dsctl.upstream.kubeflow_manifest import (
    KubeflowTfjobIdentityTemplate,
    kubeflow_tfjob_identity_template,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_definition_wire import requires_task_cache_preservation
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskResourceRefIndex,
    TaskWorkflowRefIndex,
)
from dsctl.upstream.task_parameter_projection.resource_info import task_file_uses_id
from dsctl.upstream.task_references import task_references
from dsctl.upstream.wire import WireContractError
from dsctl.upstream.workflow_graph import (
    WorkflowCreatePayload,
    WorkflowUpdatePayload,
    render_workflow_graph,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.services._workflow.identity import WorkflowTaskIdentity
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.support.yaml_io import JsonObject


_WORKFLOW_AUTHORING_SUGGESTION = (
    "Run `dsctl task-type list` to identify the relevant task type, inspect its "
    "schema, and lint the same workflow file before retrying."
)


_WORKFLOW_GRAPH_REVIEW_SUGGESTION = (
    "Fix task names and references in the workflow input, then retry the "
    "current operation."
)
_PREVIEW_RESOURCE_ID_BASE = 2_000_000_000
_PREVIEW_WORKFLOW_CODE_BASE = 3_000_000_000


_WorkflowPayloadT = TypeVar(
    "_WorkflowPayloadT",
    bound=WorkflowCreatePayload | WorkflowUpdatePayload,
)


@dataclass(frozen=True)
class _TaskResourceRequirements:
    """Verified FILE names split by whether the exact task wire needs an id."""

    full_names: tuple[str, ...]
    id_required_full_names: tuple[str, ...]


@dataclass(frozen=True)
class PreparedWorkflowCompilation(Generic[_WorkflowPayloadT]):
    """Validated workflow plan that can bind preview or persistent task codes."""

    _spec: WorkflowSpec
    _active_task_identities: tuple[tuple[str, WorkflowTaskIdentity], ...]
    _unavailable_task_identities: tuple[WorkflowTaskIdentity, ...]
    _missing_task_names: tuple[str, ...]
    _edges: tuple[tuple[str, str], ...]
    _levels: tuple[tuple[str, int], ...]
    _task_cache_requested: bool
    _profile_version: str
    _projection_sources: tuple[tuple[str, ProjectionSource], ...]
    _include_release_state: bool
    _release_state: str | None
    _required_resource_full_names: tuple[str, ...]
    _required_child_workflow_names: tuple[str, ...]
    _preview_resource_refs: TaskResourceRefIndex
    _preview_workflow_refs: TaskWorkflowRefIndex
    _preview_task_codes: tuple[int, ...]
    _preview_payload: _WorkflowPayloadT

    @property
    def required_task_code_count(self) -> int:
        """Return the number of task identities still required by this plan."""
        return len(self._missing_task_names)

    @property
    def existing_task_codes(self) -> tuple[int, ...]:
        """Codes retained from the source DAG, excluding newly allocated tasks."""
        return tuple(identity.code for _, identity in self._active_task_identities)

    def task_codes_by_name(self, task_codes: Sequence[int]) -> dict[str, int]:
        """Resolve retained and allocated codes for post-mutation verification."""
        codes, _ = _task_identity_maps_from_allocated_codes(
            self._spec.tasks,
            active_task_identities=dict(self._active_task_identities),
            missing_task_names=self._missing_task_names,
            allocated_task_codes=_validated_allocated_task_codes(
                task_codes,
                required_count=len(self._missing_task_names),
                existing_codes=set(self.existing_task_codes),
            ),
        )
        return codes

    @property
    def edges(self) -> tuple[tuple[str, str], ...]:
        """Return the canonical validated workflow graph edges."""
        return self._edges

    @property
    def required_resource_full_names(self) -> tuple[str, ...]:
        """Return exact task FILE identities that must be remotely resolved."""
        return self._required_resource_full_names

    @property
    def projection_sources(self) -> dict[str, ProjectionSource]:
        """Return the frozen per-task typed-or-preserve projection policy."""
        return dict(self._projection_sources)

    @property
    def required_child_workflow_names(self) -> tuple[str, ...]:
        """Return canonical child workflows that require same-project resolution."""
        return self._required_child_workflow_names

    def preview(
        self,
        *,
        main_task_ids: Mapping[int, int] | None = None,
        resource_refs: TaskResourceRefIndex | None = None,
        workflow_refs: TaskWorkflowRefIndex | None = None,
    ) -> _WorkflowPayloadT:
        """Materialize the plan with deterministic, non-persistent task codes."""
        if (
            main_task_ids is not None
            or resource_refs is not None
            or workflow_refs is not None
        ):
            return cast(
                "_WorkflowPayloadT",
                self._materialize(
                    self._preview_task_codes,
                    main_task_ids=main_task_ids,
                    resource_refs=(
                        self._preview_resource_refs
                        if resource_refs is None
                        else resource_refs
                    ),
                    workflow_refs=(
                        self._preview_workflow_refs
                        if workflow_refs is None
                        else workflow_refs
                    ),
                ),
            )
        return cast("_WorkflowPayloadT", dict(self._preview_payload))

    def materialize(
        self,
        task_codes: Sequence[int],
        *,
        main_task_ids: Mapping[int, int] | None = None,
        resource_refs: TaskResourceRefIndex | None = None,
        workflow_refs: TaskWorkflowRefIndex | None = None,
    ) -> _WorkflowPayloadT:
        """Materialize the plan with caller-provided persistent task codes."""
        return cast(
            "_WorkflowPayloadT",
            self._materialize(
                task_codes,
                main_task_ids=main_task_ids,
                resource_refs=resource_refs,
                workflow_refs=workflow_refs,
            ),
        )

    def _materialize(
        self,
        task_codes: Sequence[int],
        *,
        main_task_ids: Mapping[int, int] | None,
        resource_refs: TaskResourceRefIndex | None,
        workflow_refs: TaskWorkflowRefIndex | None,
    ) -> WorkflowCreatePayload | WorkflowUpdatePayload:
        """Bind task and resource identities after proving required coverage."""
        selected_resource_refs = _require_task_resource_refs(
            resource_refs,
            required_full_names=self._required_resource_full_names,
        )
        selected_workflow_refs = _require_task_workflow_refs(
            workflow_refs,
            required_names=self._required_child_workflow_names,
        )
        if main_task_ids is not None and (
            self._profile_version != "3.1.0"
            or set(main_task_ids) != set(self.existing_task_codes)
            or any(
                type(value) is not int or value <= 0 for value in main_task_ids.values()
            )
            or len(set(main_task_ids.values())) != len(main_task_ids)
        ):
            message = "Main task ids do not match the existing DS 3.1.0 task codes"
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        return _materialize_prepared_workflow_payload(
            spec=self._spec,
            active_task_identities=dict(self._active_task_identities),
            unavailable_task_identities=self._unavailable_task_identities,
            missing_task_names=self._missing_task_names,
            edges=self._edges,
            levels=self._levels,
            task_cache_requested=self._task_cache_requested,
            profile_version=self._profile_version,
            projection_sources=dict(self._projection_sources),
            include_release_state=self._include_release_state,
            release_state=self._release_state,
            task_codes=task_codes,
            main_task_ids=main_task_ids,
            resource_refs=selected_resource_refs,
            workflow_refs=selected_workflow_refs,
        )


def prepare_workflow_create_compilation(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> PreparedWorkflowCompilation[WorkflowCreatePayload]:
    """Prepare one create plan without allocating persistent task identities."""
    return _prepare_workflow_compilation(
        spec,
        active_task_identities={},
        unavailable_task_identities=(),
        authored_task_names=(),
        runtime_authored_task_names=(),
        workflow_global_params_authored=False,
        workflow_runtime_authored=False,
        preserved_projection_sources={},
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_CREATE,
        include_release_state=False,
        release_state=None,
    )


def validate_workflow_create_constraints(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
) -> None:
    """Validate local execution rules without binding remote task references."""
    validated_spec = _validated_workflow_spec(
        spec, catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
    )
    projection_sources = _task_projection_sources(
        validated_spec.tasks,
        catalog=catalog,
        requested_intent=TaskAuthoringIntent.TYPED_CREATE,
        authored_task_names=(),
        preserved_projection_sources={},
    )
    _validate_runtime_constraints(
        validated_spec,
        profile_version=catalog.profile_version,
        projection_sources=projection_sources,
        constrained_task_names={task.name for task in validated_spec.tasks},
        workflow_global_params_authored=True,
    )


def prepare_preserved_workflow_update_compilation(
    spec: WorkflowSpec,
    *,
    release_state: str | None,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    unavailable_task_identities: Collection[WorkflowTaskIdentity],
    authored_task_names: Collection[str] = (),
    runtime_authored_task_names: Collection[str] | None = None,
    workflow_global_params_authored: bool = False,
    workflow_runtime_authored: bool = False,
    preserved_projection_sources: Mapping[str, ProjectionSource] | None = None,
    catalog: TaskAuthoringCatalog | None = None,
) -> PreparedWorkflowCompilation[WorkflowUpdatePayload]:
    """Prepare an update whose authored changes were already authorized."""
    return cast(
        "PreparedWorkflowCompilation[WorkflowUpdatePayload]",
        _prepare_workflow_compilation(
            spec,
            active_task_identities=active_task_identities,
            unavailable_task_identities=unavailable_task_identities,
            authored_task_names=authored_task_names,
            runtime_authored_task_names=(
                authored_task_names
                if runtime_authored_task_names is None
                else runtime_authored_task_names
            ),
            workflow_global_params_authored=workflow_global_params_authored,
            workflow_runtime_authored=workflow_runtime_authored,
            preserved_projection_sources=(
                {}
                if preserved_projection_sources is None
                else preserved_projection_sources
            ),
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
            include_release_state=True,
            release_state=release_state,
        ),
    )


def preflight_kubeflow_runtime_activation(
    spec: WorkflowSpec,
    *,
    projection_sources: Mapping[str, ProjectionSource],
) -> None:
    """Recheck KUBEFLOW runtime safety before activating a live workflow."""
    _validate_kubeflow_runtime_constraints(
        spec,
        projection_sources=projection_sources,
        constrained_task_names={task.name for task in spec.tasks},
        workflow_global_params_authored=False,
    )


def preflight_datax_runtime_activation(
    spec: WorkflowSpec,
    *,
    profile_version: str,
) -> None:
    """Reject visible DataX shell-argument forwarding before activation."""
    _validate_datax_runtime_constraints(
        spec,
        profile_version=profile_version,
        constrained_task_names={task.name for task in spec.tasks},
        workflow_global_params_authored=True,
    )


def preflight_seatunnel_runtime_activation(
    spec: WorkflowSpec,
    *,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
) -> None:
    """Recheck SeaTunnel parameter isolation before activating a workflow."""
    _validate_seatunnel_runtime_constraints(
        spec,
        profile_version=profile_version,
        projection_sources=projection_sources,
        constrained_task_names={task.name for task in spec.tasks},
        workflow_global_params_authored=True,
    )


def _prepare_workflow_compilation(
    spec: WorkflowSpec,
    *,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    unavailable_task_identities: Collection[WorkflowTaskIdentity],
    authored_task_names: Collection[str],
    runtime_authored_task_names: Collection[str],
    workflow_global_params_authored: bool,
    workflow_runtime_authored: bool,
    preserved_projection_sources: Mapping[str, ProjectionSource],
    catalog: TaskAuthoringCatalog | None,
    intent: TaskAuthoringIntent,
    include_release_state: bool,
    release_state: str | None,
) -> PreparedWorkflowCompilation[WorkflowCreatePayload | WorkflowUpdatePayload]:
    """Prepare one validated graph and eagerly prove its preview materialization."""
    selected_catalog = _selected_catalog(catalog)
    validated_spec = _validated_workflow_spec(
        spec,
        catalog=selected_catalog,
        intent=intent,
    )
    active_identities = dict(active_task_identities)
    unavailable_identities = tuple(unavailable_task_identities)
    _validate_prepared_task_identities(
        validated_spec.tasks,
        active_task_identities=active_identities,
        unavailable_task_identities=unavailable_identities,
    )
    missing_task_names = tuple(
        task.name for task in validated_spec.tasks if task.name not in active_identities
    )
    edges = workflow_edges(
        validated_spec.tasks,
        allow_native_code_refs=intent is TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    levels = _task_levels(validated_spec.tasks, edges=edges)
    task_cache_requested = _task_cache_requested(selected_catalog)
    projection_sources = _task_projection_sources(
        validated_spec.tasks,
        catalog=selected_catalog,
        requested_intent=intent,
        authored_task_names=authored_task_names,
        preserved_projection_sources=preserved_projection_sources,
    )
    constrained_runtime_task_names = (
        {task.name for task in validated_spec.tasks}
        if (
            intent is not TaskAuthoringIntent.OPAQUE_PRESERVE
            or workflow_runtime_authored
        )
        else runtime_authored_task_names
    )
    _validate_runtime_constraints(
        validated_spec,
        profile_version=selected_catalog.profile_version,
        projection_sources=projection_sources,
        constrained_task_names=constrained_runtime_task_names,
        workflow_global_params_authored=(
            intent is not TaskAuthoringIntent.OPAQUE_PRESERVE
            or workflow_global_params_authored
        ),
    )
    _validate_task_cache_identities(
        validated_spec.tasks,
        active_task_identities=active_identities,
        task_cache_requested=task_cache_requested,
    )
    resource_requirements = _task_resource_requirements(
        validated_spec.tasks,
        profile_version=selected_catalog.profile_version,
        projection_sources=projection_sources,
    )
    required_resource_full_names = resource_requirements.full_names
    preview_resource_refs = _preview_task_resource_refs(
        required_resource_full_names,
        id_required_full_names=resource_requirements.id_required_full_names,
    )
    required_child_workflow_names = _required_dynamic_child_workflow_names(
        validated_spec.tasks,
        projection_sources=projection_sources,
    )
    preview_workflow_refs = _preview_task_workflow_refs(required_child_workflow_names)
    preview_codes = tuple(preview_task_codes(len(missing_task_names)))
    preview_payload = _materialize_prepared_workflow_payload(
        spec=validated_spec,
        active_task_identities=active_identities,
        unavailable_task_identities=unavailable_identities,
        missing_task_names=missing_task_names,
        edges=tuple(edges),
        levels=tuple(levels.items()),
        task_cache_requested=task_cache_requested,
        profile_version=selected_catalog.profile_version,
        projection_sources=projection_sources,
        include_release_state=include_release_state,
        release_state=release_state,
        task_codes=preview_codes,
        resource_refs=preview_resource_refs,
        workflow_refs=preview_workflow_refs,
    )
    return PreparedWorkflowCompilation(
        _spec=validated_spec,
        _active_task_identities=tuple(active_identities.items()),
        _unavailable_task_identities=unavailable_identities,
        _missing_task_names=missing_task_names,
        _edges=tuple(edges),
        _levels=tuple(levels.items()),
        _task_cache_requested=task_cache_requested,
        _profile_version=selected_catalog.profile_version,
        _projection_sources=tuple(projection_sources.items()),
        _include_release_state=include_release_state,
        _release_state=release_state,
        _required_resource_full_names=required_resource_full_names,
        _required_child_workflow_names=required_child_workflow_names,
        _preview_resource_refs=preview_resource_refs,
        _preview_workflow_refs=preview_workflow_refs,
        _preview_task_codes=preview_codes,
        _preview_payload=preview_payload,
    )


def _materialize_prepared_workflow_payload(
    *,
    spec: WorkflowSpec,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    unavailable_task_identities: Sequence[WorkflowTaskIdentity],
    missing_task_names: Sequence[str],
    edges: Sequence[tuple[str, str]],
    levels: Sequence[tuple[str, int]],
    task_cache_requested: bool,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    include_release_state: bool,
    release_state: str | None,
    task_codes: Sequence[int],
    main_task_ids: Mapping[int, int] | None = None,
    resource_refs: TaskResourceRefIndex,
    workflow_refs: TaskWorkflowRefIndex,
) -> WorkflowCreatePayload | WorkflowUpdatePayload:
    reserved_task_codes = {
        *(identity.code for identity in active_task_identities.values()),
        *(identity.code for identity in unavailable_task_identities),
    }
    validated_task_codes = _validated_allocated_task_codes(
        task_codes,
        required_count=len(missing_task_names),
        existing_codes=reserved_task_codes,
    )
    task_code_map, task_versions = _task_identity_maps_from_allocated_codes(
        spec.tasks,
        active_task_identities=active_task_identities,
        missing_task_names=missing_task_names,
        allocated_task_codes=validated_task_codes,
    )
    payload = render_workflow_graph(
        spec,
        task_codes=task_code_map,
        task_versions=task_versions,
        main_task_ids=main_task_ids,
        task_cache_by_name={
            name: identity.is_cache for name, identity in active_task_identities.items()
        },
        edges=list(edges),
        levels=dict(levels),
        task_cache_requested=task_cache_requested,
        profile_version=profile_version,
        projection_sources=projection_sources,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    if include_release_state:
        return {**payload, "releaseState": release_state}
    return payload


def _validated_workflow_spec(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog | None,
    intent: TaskAuthoringIntent,
) -> WorkflowSpec:
    context = workflow_authoring_context(catalog=catalog, intent=intent)
    try:
        return validate_workflow_document(
            spec.model_dump(mode="python", exclude_none=False),
            authoring_context=context,
        )
    except ValidationError as exc:
        raise UserInputError(
            first_validation_error_message(exc),
            suggestion=_WORKFLOW_AUTHORING_SUGGESTION,
        ) from exc


def _selected_catalog(
    catalog: TaskAuthoringCatalog | None,
) -> TaskAuthoringCatalog:
    return default_task_authoring_catalog() if catalog is None else catalog


def _task_resource_requirements(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
) -> _TaskResourceRequirements:
    """Collect typed task FILE verification and id needs in task order."""
    file_uses_id = task_file_uses_id(profile_version)
    pytorch_uses_resource_id = (
        get_task_authoring_surface(profile_version).pytorch.wire_epoch
        == "positive-resource-id-python-home"
    )
    names: list[str] = []
    seen: set[str] = set()
    id_names: list[str] = []
    seen_id_names: set[str] = set()
    for task in tasks:
        if projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING:
            continue
        task_type = task.type.upper()
        id_required = (
            pytorch_uses_resource_id
            if task_type == "PYTORCH"
            else task_type == "WATERDROP" or file_uses_id
        )
        for full_name in _typed_task_resource_names(task):
            if full_name not in seen:
                seen.add(full_name)
                names.append(full_name)
            if id_required and full_name not in seen_id_names:
                seen_id_names.add(full_name)
                id_names.append(full_name)
    return _TaskResourceRequirements(
        full_names=tuple(names),
        id_required_full_names=tuple(id_names),
    )


def _typed_task_resource_names(task: WorkflowTaskSpec) -> tuple[str, ...]:
    """Read canonical FILE fields only after typed task normalization."""
    params = task.task_params
    if not isinstance(params, Mapping):
        return ()
    if task.type.upper() in {"SHELL", "PYTHON"}:
        resources = params.get("resourceList", [])
        if isinstance(resources, list):
            return tuple(
                name
                for resource in resources
                if isinstance(resource, Mapping)
                and isinstance(name := resource.get("resourceName"), str)
            )
        return ()
    field = {
        "JAVA": "mainJar",
        "MR": "mainJar",
        "PYTORCH": "scriptResource",
        "WATERDROP": "configResource",
    }.get(task.type.upper())
    if field is None:
        return ()
    name = params.get(field)
    if not isinstance(name, str):
        message = f"{task.type} task '{task.name}' is missing canonical {field}"
        raise UserInputError(message, suggestion=_WORKFLOW_AUTHORING_SUGGESTION)
    return (name,)


def _preview_task_resource_refs(
    full_names: Sequence[str],
    *,
    id_required_full_names: Sequence[str],
) -> TaskResourceRefIndex:
    """Create preview ids only for exact task wires that require positive ids."""
    return TaskResourceRefIndex.from_resolved_files(
        full_names,
        id_by_full_name={
            full_name: _PREVIEW_RESOURCE_ID_BASE + index
            for index, full_name in enumerate(id_required_full_names, start=1)
        },
        wire_full_name_by_full_name={
            full_name: f"/__dsctl_preview__/resources{full_name}"
            for full_name in full_names
        },
    )


def _require_task_resource_refs(
    resource_refs: TaskResourceRefIndex | None,
    *,
    required_full_names: Sequence[str],
) -> TaskResourceRefIndex:
    """Prevent any persistent materialization from using offline preview ids."""
    if not required_full_names:
        return (
            TaskResourceRefIndex.from_id_by_full_name({})
            if resource_refs is None
            else resource_refs
        )
    if resource_refs is None:
        message = "Workflow materialization requires resolved task resource identities"
        raise UserInputError(
            message,
            details={"resource_full_names": list(required_full_names)},
            suggestion=(
                "Resolve every typed task resource through the selected "
                "DolphinScheduler "
                "resource profile before retrying the workflow mutation."
            ),
        )
    missing = [
        full_name
        for full_name in required_full_names
        if full_name not in resource_refs.verified_full_names
    ]
    if missing:
        message = "Workflow materialization is missing task resource identities"
        raise UserInputError(
            message,
            details={"resource_full_names": missing},
            suggestion=(
                "Resolve every typed task resource through the selected "
                "DolphinScheduler "
                "resource profile before retrying the workflow mutation."
            ),
        )
    return resource_refs


def _required_dynamic_child_workflow_names(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    projection_sources: Mapping[str, ProjectionSource],
) -> tuple[str, ...]:
    """Collect typed DYNAMIC child names without interpreting preserved native state."""
    names: list[str] = []
    seen: set[str] = set()
    for task in tasks:
        if (
            task.type.upper() != "DYNAMIC"
            or projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING
            or not isinstance(task.task_params, Mapping)
        ):
            continue
        name = task.task_params.get("childWorkflowName")
        if not isinstance(name, str):
            message = (
                f"DYNAMIC task '{task.name}' is missing canonical childWorkflowName"
            )
            raise UserInputError(message, suggestion=_WORKFLOW_AUTHORING_SUGGESTION)
        if name not in seen:
            seen.add(name)
            names.append(name)
    return tuple(names)


def _preview_task_workflow_refs(
    names: Sequence[str],
) -> TaskWorkflowRefIndex:
    """Create deterministic offline-only child codes for eager preview validation."""
    return TaskWorkflowRefIndex.from_code_by_name(
        {
            name: _PREVIEW_WORKFLOW_CODE_BASE + index
            for index, name in enumerate(names, start=1)
        }
    )


def _require_task_workflow_refs(
    workflow_refs: TaskWorkflowRefIndex | None,
    *,
    required_names: Sequence[str],
) -> TaskWorkflowRefIndex:
    """Require persistent same-project identities for typed DYNAMIC materialization."""
    if not required_names:
        return (
            TaskWorkflowRefIndex.from_code_by_name({})
            if workflow_refs is None
            else workflow_refs
        )
    if workflow_refs is None:
        message = "Workflow materialization requires resolved child workflow identities"
        raise UserInputError(
            message,
            details={"child_workflow_names": list(required_names)},
            suggestion=(
                "Resolve every DYNAMIC childWorkflowName in the selected project "
                "before retrying the workflow mutation."
            ),
        )
    missing = [
        name for name in required_names if name not in workflow_refs.code_by_name
    ]
    if missing:
        message = "Workflow materialization is missing child workflow identities"
        raise UserInputError(
            message,
            details={"child_workflow_names": missing},
            suggestion=(
                "Resolve every DYNAMIC childWorkflowName in the selected project "
                "before retrying the workflow mutation."
            ),
        )
    return workflow_refs


def _task_projection_sources(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    catalog: TaskAuthoringCatalog,
    requested_intent: TaskAuthoringIntent,
    authored_task_names: Collection[str],
    preserved_projection_sources: Mapping[str, ProjectionSource],
) -> dict[str, ProjectionSource]:
    """Freeze each task's reviewed typed-or-opaque wire projection policy."""
    authored_names = frozenset(authored_task_names)
    task_names = {task.name for task in tasks}
    if unknown_names := authored_names - task_names:
        message = (
            "Authored task projection names were absent from the compiled workflow: "
            f"{', '.join(sorted(unknown_names))}"
        )
        raise ValueError(message)
    preserved_sources = dict(preserved_projection_sources)
    if unknown_preserved_names := set(preserved_sources) - task_names:
        message = (
            "Preserved task projection names were absent from the compiled workflow: "
            f"{', '.join(sorted(unknown_preserved_names))}"
        )
        raise ValueError(message)
    sources: dict[str, ProjectionSource] = {}
    for task in tasks:
        if (
            requested_intent is TaskAuthoringIntent.OPAQUE_PRESERVE
            and task.name not in authored_names
        ):
            sources[task.name] = preserved_sources.get(
                task.name,
                ProjectionSource.OPAQUE_PRESERVE,
            )
            continue
        task_intent = (
            TaskAuthoringIntent.TYPED_EDIT
            if requested_intent is TaskAuthoringIntent.OPAQUE_PRESERVE
            else requested_intent
        )
        effective_intent = catalog.effective_authoring_intent(
            task.type,
            requested=task_intent,
            task_params=task.task_params,
        )
        sources[task.name] = (
            ProjectionSource.TYPED_AUTHORING
            if effective_intent
            in {
                TaskAuthoringIntent.TYPED_CREATE,
                TaskAuthoringIntent.TYPED_EDIT,
            }
            else ProjectionSource.OPAQUE_PRESERVE
        )
    return sources


def _task_cache_requested(catalog: TaskAuthoringCatalog) -> bool:
    try:
        return requires_task_cache_preservation(catalog.profile_version)
    except WireContractError as exc:
        message = "Task-definition profile is invalid during workflow compilation"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE},
        ) from exc


def _validate_runtime_constraints(
    spec: WorkflowSpec,
    *,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    constrained_task_names: Collection[str],
    workflow_global_params_authored: bool,
) -> None:
    """Share execution constraints between local lint and wire compilation."""
    _validate_dynamic_runtime_constraints(spec, projection_sources=projection_sources)
    _validate_pytorch_runtime_constraints(
        spec,
        profile_version=profile_version,
        projection_sources=projection_sources,
        constrained_task_names=constrained_task_names,
    )
    _validate_datax_runtime_constraints(
        spec,
        profile_version=profile_version,
        constrained_task_names=constrained_task_names,
        workflow_global_params_authored=workflow_global_params_authored,
    )
    _validate_seatunnel_runtime_constraints(
        spec,
        profile_version=profile_version,
        projection_sources=projection_sources,
        constrained_task_names=constrained_task_names,
        workflow_global_params_authored=workflow_global_params_authored,
    )
    _validate_kubeflow_runtime_constraints(
        spec,
        projection_sources=projection_sources,
        constrained_task_names=constrained_task_names,
        workflow_global_params_authored=workflow_global_params_authored,
    )


def _workflow_global_parameter_names(spec: WorkflowSpec) -> set[str]:
    """Return author-controlled workflow-global names from either YAML form."""
    global_params = spec.workflow.global_params
    return (
        set(global_params)
        if isinstance(global_params, Mapping)
        else {
            parameter.prop
            for parameter in (() if global_params is None else global_params)
        }
    )


def _validate_dynamic_runtime_constraints(
    spec: WorkflowSpec,
    *,
    projection_sources: Mapping[str, ProjectionSource],
) -> None:
    """Reject static DYNAMIC states that exact 3.2.2 cannot execute honestly."""
    global_names = _workflow_global_parameter_names(spec)
    for task in spec.tasks:
        if (
            task.type.upper() != "DYNAMIC"
            or projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING
        ):
            continue
        if task.retry.times != 0:
            message = (
                f"DYNAMIC task '{task.name}' retry.times must be 0 because exact "
                "3.2.2 task retry does not reset failed child workflows"
            )
            raise UserInputError(
                message,
                details={
                    "resource": WORKFLOW_RESOURCE,
                    "task": task.name,
                    "field": "retry.times",
                    "reason": "dynamic-task-retry-does-not-reset-children",
                },
                suggestion=(
                    "Set retry.times to 0 and handle reruns at the workflow level "
                    "after reviewing child side effects."
                ),
            )
        child_workflow_name = (
            task.task_params.get("childWorkflowName")
            if isinstance(task.task_params, Mapping)
            else None
        )
        if child_workflow_name == spec.workflow.name:
            message = (
                f"DYNAMIC task '{task.name}' cannot target its containing workflow "
                f"'{spec.workflow.name}'"
            )
            raise UserInputError(
                message,
                details={
                    "resource": WORKFLOW_RESOURCE,
                    "task": task.name,
                    "field": "task_params.childWorkflowName",
                    "reason": "dynamic-child-workflow-self-reference",
                },
                suggestion="Choose a different same-project child workflow and retry.",
            )
        parameter_name = (
            task.task_params.get("parameterName")
            if isinstance(task.task_params, Mapping)
            else None
        )
        if isinstance(parameter_name, str) and parameter_name in global_names:
            message = (
                f"DYNAMIC task '{task.name}' parameterName '{parameter_name}' "
                "collides with a parent workflow global parameter"
            )
            raise UserInputError(
                message,
                details={
                    "resource": WORKFLOW_RESOURCE,
                    "task": task.name,
                    "field": "task_params.parameterName",
                    "parameter_name": parameter_name,
                    "reason": "dynamic-parent-global-overrides-fanout-value",
                },
                suggestion=(
                    "Rename the DYNAMIC parameter or the parent workflow global, "
                    "then retry."
                ),
            )


def _validate_datax_runtime_constraints(
    spec: WorkflowSpec,
    *,
    profile_version: str,
    constrained_task_names: Collection[str],
    workflow_global_params_authored: bool,
) -> None:
    """Reject authored globals that the reviewed DataX executor shell-forwards.

    Startup parameters and worker-prepared values remain runtime prerequisites;
    an empty local workflow cannot prove that the eventual prepared map is empty.
    """
    surface = get_task_authoring_surface(profile_version).datax
    if not surface.typed_custom_json_available or not surface.prepared_params_forwarded:
        return
    global_names = _workflow_global_parameter_names(spec)
    if not global_names:
        return
    for task in spec.tasks:
        if task.type.upper() != "DATAX" or not (
            task.name in constrained_task_names or workflow_global_params_authored
        ):
            continue
        names = ", ".join(sorted(global_names))
        message = (
            f"DATAX task '{task.name}' requires a parameter-free workflow on "
            f"exact {profile_version}; workflow globals would be forwarded as "
            f"unsafe shell-built -p -D arguments: {names}"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "workflow.global_params",
                "reason": "datax-workflow-parameters-outside-typed-facet",
            },
            suggestion=(
                "Remove workflow.global_params or isolate this DataX task in a "
                "parameter-free workflow. Also ensure startup and worker-prepared "
                "parameters are absent; local validation cannot establish that."
            ),
        )


def _validate_seatunnel_runtime_constraints(
    spec: WorkflowSpec,
    *,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    constrained_task_names: Collection[str],
    workflow_global_params_authored: bool,
) -> None:
    """Keep automatic SeaTunnel CLI variable forwarding outside typed intent."""
    surface = get_task_authoring_surface(profile_version).seatunnel
    if not surface.available:
        return
    constrained_names = frozenset(constrained_task_names)
    unknown_names = constrained_names.difference(task.name for task in spec.tasks)
    if unknown_names:
        names = ", ".join(sorted(unknown_names))
        message = f"SEATUNNEL runtime constraints received unknown task names: {names}"
        raise ValueError(message)
    runtime_context_authored = (
        bool(constrained_names) or workflow_global_params_authored
    )
    if not runtime_context_authored:
        return
    global_names = _workflow_global_parameter_names(spec)
    for task in spec.tasks:
        if task.type.upper() != "SEATUNNEL":
            continue
        task_runtime_authored = (
            task.name in constrained_names or workflow_global_params_authored
        )
        if not task_runtime_authored:
            continue
        if projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING:
            _reject_seatunnel_opaque_runtime_edit(task.name)
        if surface.workflow_parameter_forwarding == "none" or not global_names:
            continue
        names = ", ".join(sorted(global_names))
        message = (
            f"SEATUNNEL task '{task.name}' requires a parameter-free workflow on "
            f"exact {profile_version}; workflow globals would be forwarded as "
            f"out-of-facet shell arguments: {names}"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "workflow.global_params",
                "reason": "seatunnel-workflow-parameters-outside-typed-facet",
            },
            suggestion=(
                "Remove workflow.global_params or move this job to an isolated "
                "parameter-free workflow, then retry."
            ),
        )


def _reject_seatunnel_opaque_runtime_edit(task_name: str) -> None:
    message = (
        f"SEATUNNEL task '{task_name}' has richer opaque state whose execution "
        "semantics cannot be safely edited or activated"
    )
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "task": task_name,
            "reason": "seatunnel-opaque-runtime-edit-closed",
        },
        suggestion=(
            "Leave task and workflow execution settings unchanged, or replace the "
            "task with the closed SEATUNNEL/literal_local_config_job shape."
        ),
    )


def _validate_pytorch_runtime_constraints(
    spec: WorkflowSpec,
    *,
    profile_version: str,
    projection_sources: Mapping[str, ProjectionSource],
    constrained_task_names: Collection[str],
) -> None:
    """Protect PYTORCH preservation and unsafe exact 3.3.x timeout paths."""
    surface = get_task_authoring_surface(profile_version).pytorch
    constrained_names = frozenset(constrained_task_names)
    unknown_names = constrained_names.difference(task.name for task in spec.tasks)
    if unknown_names:
        names = ", ".join(sorted(unknown_names))
        message = f"PYTORCH runtime constraints received unknown task names: {names}"
        raise ValueError(message)
    for task in spec.tasks:
        if task.type.upper() != "PYTORCH" or task.name not in constrained_names:
            continue
        if projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING:
            _reject_pytorch_opaque_runtime_edit(task.name)
        if surface.stop_mode != "plugin-cancel-noop" or task.timeout == 0:
            continue
        message = (
            f"PYTORCH task '{task.name}' timeout must be 0 on exact "
            f"{profile_version} because the executor can block before timeout "
            "cancellation and the plugin cancel path is a no-op"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "timeout",
                "reason": "pytorch-timeout-cannot-reliably-stop-process",
            },
            suggestion=(
                "Set timeout to 0 and enforce an external deadline around an "
                "idempotent script if one is required."
            ),
        )


def _reject_pytorch_opaque_runtime_edit(task_name: str) -> None:
    message = (
        f"PYTORCH task '{task_name}' has richer opaque PYTORCH state whose "
        "execution-affecting fields cannot be edited safely"
    )
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "task": task_name,
            "reason": "pytorch-opaque-runtime-edit-unsafe",
        },
        suggestion=(
            "Leave the task and workflow execution settings unchanged; only "
            "metadata-only edits can preserve this server-backed PYTORCH state."
        ),
    )


def _validate_kubeflow_runtime_constraints(
    spec: WorkflowSpec,
    *,
    projection_sources: Mapping[str, ProjectionSource],
    constrained_task_names: Collection[str],
    workflow_global_params_authored: bool,
) -> None:
    """Keep TFJob identity stable for failover without pretending retry is safe."""
    identity_parameter = KUBEFLOW_WORKFLOW_INSTANCE_PARAMETER_NAME
    global_names = _workflow_global_parameter_names(spec)
    constrained_names = frozenset(constrained_task_names)
    unknown_names = constrained_names.difference(task.name for task in spec.tasks)
    if unknown_names:
        names = ", ".join(sorted(unknown_names))
        message = f"KUBEFLOW runtime constraints received unknown task names: {names}"
        raise ValueError(message)
    runtime_context_authored = bool(constrained_names) or (
        workflow_global_params_authored
    )
    task_by_target: dict[KubeflowTfjobIdentityTemplate, str] = {}
    for task in spec.tasks:
        if task.type.upper() != "KUBEFLOW":
            continue
        if projection_sources[task.name] is not ProjectionSource.TYPED_AUTHORING:
            if runtime_context_authored:
                _reject_kubeflow_opaque_runtime_edit(task.name)
            continue
        if runtime_context_authored:
            _validate_kubeflow_task_runtime(
                task,
                global_names=global_names,
                identity_parameter=identity_parameter,
            )
        target = _kubeflow_target_identity(task)
        conflicting_task = task_by_target.get(target)
        if conflicting_task is not None and runtime_context_authored:
            _reject_duplicate_kubeflow_target(
                task_name=task.name,
                conflicting_task=conflicting_task,
                target=target,
            )
        task_by_target[target] = task.name


def _reject_kubeflow_opaque_runtime_edit(task_name: str) -> None:
    message = (
        f"KUBEFLOW task '{task_name}' has richer opaque state and cannot accept "
        "execution-affecting edits"
    )
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "task": task_name,
            "reason": "kubeflow-opaque-runtime-edit-closed",
        },
        suggestion=(
            "Restore the task and workflow runtime fields from the live server "
            "baseline, or replace the task with the closed typed "
            "KUBEFLOW/tfjob_manifest shape. Richer opaque state supports only "
            "unchanged/export preservation and metadata-only edits."
        ),
    )


def _validate_kubeflow_task_runtime(
    task: WorkflowTaskSpec,
    *,
    global_names: Collection[str],
    identity_parameter: str,
) -> None:
    """Validate one typed KUBEFLOW task when execution semantics are authored."""
    if task.retry.times != 0:
        message = (
            f"KUBEFLOW task '{task.name}' retry.times must be 0 because no "
            "resource identity is both failover-stable and retry-unique"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "retry.times",
                "reason": "kubeflow-task-retry-cannot-preserve-identity",
            },
            suggestion=(
                "Set retry.times to 0. For a fresh TFJob attempt, start the "
                "workflow definition with `dsctl workflow run`; do not use "
                "workflow-instance rerun, recover-failed, or execute-task, which "
                "reuse the existing workflow instance identity."
            ),
        )
    if task.timeout <= 0:
        message = (
            f"KUBEFLOW task '{task.name}' timeout must be a positive number of "
            "minutes because unknown TFJob status can poll indefinitely"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "timeout",
                "reason": "kubeflow-watcher-needs-finite-timeout",
            },
            suggestion="Set timeout to a positive number of minutes, then retry.",
        )
    if task.timeout_notify_strategy not in {
        TaskTimeoutNotifyStrategy.FAILED,
        TaskTimeoutNotifyStrategy.WARNFAILED,
    }:
        message = (
            f"KUBEFLOW task '{task.name}' timeout_notify_strategy must be FAILED "
            "or WARNFAILED so the timeout terminates the watcher"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "timeout_notify_strategy",
                "reason": "kubeflow-watcher-needs-terminating-timeout",
            },
            suggestion=(
                "Set timeout_notify_strategy to FAILED or WARNFAILED, then retry."
            ),
        )
    if identity_parameter in global_names:
        message = (
            f"KUBEFLOW task '{task.name}' cannot use workflow.global_params name "
            f"'{identity_parameter}' because it shadows the built-in "
            "workflow-instance identity"
        )
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "workflow.global_params",
                "parameter_name": identity_parameter,
                "reason": "kubeflow-workflow-instance-id-shadowed",
            },
            suggestion="Remove or rename that workflow global parameter, then retry.",
        )


def _kubeflow_target_identity(
    task: WorkflowTaskSpec,
) -> KubeflowTfjobIdentityTemplate:
    """Validate canonical manifest semantics and return one named identity."""
    try:
        task_params = KubeflowTfjobManifestTaskParamsSpec.model_validate(
            task.task_params
        )
        return kubeflow_tfjob_identity_template(
            task_params.yaml_content,
            namespace=task_params.namespace,
            cluster=task_params.cluster,
        )
    except ValidationError as exc:
        reason = first_validation_error_message(exc)
        message = f"KUBEFLOW task '{task.name}' is invalid: {reason}"
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "task_params",
                "reason": "invalid-kubeflow-canonical-value",
            },
            suggestion=_WORKFLOW_AUTHORING_SUGGESTION,
        ) from exc
    except ValueError as exc:
        message = f"KUBEFLOW task '{task.name}' is invalid: {exc}"
        raise UserInputError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "task": task.name,
                "field": "task_params.yamlContent",
                "reason": "invalid-kubeflow-manifest",
            },
            suggestion=_WORKFLOW_AUTHORING_SUGGESTION,
        ) from exc


def _reject_duplicate_kubeflow_target(
    *,
    task_name: str,
    conflicting_task: str,
    target: KubeflowTfjobIdentityTemplate,
) -> None:
    message = (
        f"KUBEFLOW task '{task_name}' targets the same TFJob identity as task "
        f"'{conflicting_task}'"
    )
    raise UserInputError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "task": task_name,
            "conflicting_task": conflicting_task,
            "field": "task_params.yamlContent.metadata.name",
            "cluster": target.cluster,
            "namespace": target.namespace,
            "name": target.name_template,
            "reason": "kubeflow-duplicate-workflow-resource-identity",
        },
        suggestion=(
            "Give each KUBEFLOW task a distinct metadata.name prefix or target "
            "cluster/namespace, then retry."
        ),
    )


def _validate_prepared_task_identities(
    tasks: list[WorkflowTaskSpec],
    *,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    unavailable_task_identities: Sequence[WorkflowTaskIdentity],
) -> None:
    task_names = {task.name for task in tasks}
    unknown_names = sorted(set(active_task_identities).difference(task_names))
    if unknown_names:
        message = "Workflow compilation received an unknown active task identity"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE, "tasks": unknown_names},
        )

    active_identity_by_code: dict[int, WorkflowTaskIdentity] = {}
    for name, identity in active_task_identities.items():
        _validate_prepared_task_identity(identity, task=name, state="active")
        if identity.code in active_identity_by_code:
            message = "Workflow compilation received a duplicate active task code"
            raise ApiTransportError(
                message,
                details={"resource": WORKFLOW_RESOURCE, "task": name},
            )
        active_identity_by_code[identity.code] = identity

    unavailable_identity_by_code: dict[int, WorkflowTaskIdentity] = {}
    for identity in unavailable_task_identities:
        _validate_prepared_task_identity(
            identity,
            task=None,
            state="unavailable",
        )
        previous = unavailable_identity_by_code.get(identity.code)
        if previous is not None and previous != identity:
            message = "Workflow compilation received conflicting unavailable identities"
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        active = active_identity_by_code.get(identity.code)
        if active is not None:
            message = (
                "Workflow compilation received a task identity that is both active "
                "and unavailable"
            )
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        unavailable_identity_by_code[identity.code] = identity


def _validate_prepared_task_identity(
    identity: WorkflowTaskIdentity,
    *,
    task: str | None,
    state: str,
) -> None:
    details: JsonObject = {
        "resource": WORKFLOW_RESOURCE,
        "identity_state": state,
    }
    if task is not None:
        details["task"] = task
    if (
        isinstance(identity.code, bool)
        or not isinstance(identity.code, int)
        or identity.code <= 0
        or isinstance(identity.version, bool)
        or not isinstance(identity.version, int)
        or identity.version <= 0
        or identity.is_cache not in {None, "YES", "NO"}
    ):
        message = "Workflow compilation received an invalid task identity"
        raise ApiTransportError(message, details=details)


def _validate_task_cache_identities(
    tasks: list[WorkflowTaskSpec],
    *,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    task_cache_requested: bool,
) -> None:
    if not task_cache_requested:
        return
    for task in tasks:
        identity = active_task_identities.get(task.name)
        if identity is None or identity.is_cache in {"YES", "NO"}:
            continue
        message = "Workflow DAG payload was missing required task cache state"
        raise ApiTransportError(
            message,
            details={"resource": WORKFLOW_RESOURCE, "task": task.name},
        )


def _task_identity_maps_from_allocated_codes(
    tasks: list[WorkflowTaskSpec],
    *,
    active_task_identities: Mapping[str, WorkflowTaskIdentity],
    missing_task_names: Sequence[str],
    allocated_task_codes: Sequence[int],
) -> tuple[dict[str, int], dict[str, int]]:
    task_codes: dict[str, int] = {}
    task_versions: dict[str, int] = {}
    allocated_code_by_name = dict(
        zip(missing_task_names, allocated_task_codes, strict=True)
    )
    for task in tasks:
        identity = active_task_identities.get(task.name)
        if identity is None:
            task_codes[task.name] = allocated_code_by_name[task.name]
            task_versions[task.name] = 1
            continue
        task_codes[task.name] = identity.code
        task_versions[task.name] = identity.version
    return task_codes, task_versions


def _validated_allocated_task_codes(
    values: Sequence[int],
    *,
    required_count: int,
    existing_codes: set[int],
) -> list[int]:
    task_codes = list(values)
    if len(task_codes) != required_count:
        message = (
            f"Task code allocator returned {len(task_codes)} task codes "
            f"when {required_count} were required"
        )
        raise ApiTransportError(message)
    if any(
        isinstance(code, bool) or not isinstance(code, int) or code <= 0
        for code in task_codes
    ):
        message = "Task code allocator values must be positive integers"
        raise ApiTransportError(message)
    if len(set(task_codes)) != len(task_codes):
        message = "Task code allocator contained duplicate task codes"
        raise ApiTransportError(message)
    if existing_codes.intersection(task_codes):
        message = "Task code allocator collided with an existing task code"
        raise ApiTransportError(message)
    return task_codes


def workflow_edges(
    tasks: list[WorkflowTaskSpec],
    *,
    allow_native_code_refs: bool = False,
) -> list[tuple[str, str]]:
    """Build the DAG edge list implied by task dependencies and logic branches."""
    task_names = {task.name for task in tasks}
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for task in tasks:
        for dependency in task.depends_on:
            _append_workflow_edge(
                predecessor=dependency,
                successor=task.name,
                task_name=task.name,
                label="depends_on",
                task_names=task_names,
                edges=edges,
                seen=seen,
                allow_native_code_refs=allow_native_code_refs,
            )
        if task.task_params is None:
            continue
        payload = require_json_object(
            task.task_params,
            label=f"workflow task params for '{task.name}'",
        )
        for reference in task_references(task.type.upper(), payload):
            if not isinstance(reference.value, str):
                continue
            predecessor, successor = (
                (reference.value, task.name)
                if reference.role == "predecessor"
                else (task.name, reference.value)
            )
            _append_workflow_edge(
                predecessor=predecessor,
                successor=successor,
                task_name=task.name,
                label=reference.field,
                task_names=task_names,
                edges=edges,
                seen=seen,
                allow_native_code_refs=allow_native_code_refs,
            )
    return edges


def _append_workflow_edge(
    *,
    predecessor: str,
    successor: str,
    task_name: str,
    label: str,
    task_names: set[str],
    edges: list[tuple[str, str]],
    seen: set[tuple[str, str]],
    allow_native_code_refs: bool,
) -> None:
    if allow_native_code_refs and _contains_unresolved_native_code_ref(
        predecessor,
        successor,
        label=label,
        task_names=task_names,
    ):
        # Opaque legacy logic-task payloads can retain decimal-string codes.
        # Their authoritative edges already come from workflow relations.
        return
    if predecessor not in task_names:
        message = f"Task '{task_name}' depends on unknown task '{predecessor}'"
        raise UserInputError(message, suggestion=_WORKFLOW_GRAPH_REVIEW_SUGGESTION)
    if successor not in task_names:
        message = f"Task '{task_name}' references unknown task '{successor}' in {label}"
        raise UserInputError(message, suggestion=_WORKFLOW_GRAPH_REVIEW_SUGGESTION)
    if predecessor == successor:
        message = f"Task '{task_name}' cannot reference itself in {label}"
        raise UserInputError(message, suggestion=_WORKFLOW_GRAPH_REVIEW_SUGGESTION)
    edge = (predecessor, successor)
    if edge in seen:
        return
    seen.add(edge)
    edges.append(edge)


def preserved_workflow_edges(
    tasks: list[WorkflowTaskSpec],
) -> list[tuple[str, str]]:
    """Build edges while retaining unrepresentable legacy native code refs."""
    return workflow_edges(tasks, allow_native_code_refs=True)


def _is_unresolved_native_code_ref(
    value: str,
    *,
    task_names: set[str],
) -> bool:
    return value not in task_names and value.isdecimal()


def _contains_unresolved_native_code_ref(
    predecessor: str,
    successor: str,
    *,
    label: str,
    task_names: set[str],
) -> bool:
    if not label.startswith("task_params."):
        return False
    return _is_unresolved_native_code_ref(
        predecessor,
        task_names=task_names,
    ) or _is_unresolved_native_code_ref(successor, task_names=task_names)


def _task_levels(
    tasks: list[WorkflowTaskSpec],
    *,
    edges: list[tuple[str, str]],
) -> dict[str, int]:
    dependents: dict[str, list[str]] = {task.name: [] for task in tasks}
    indegree: dict[str, int] = {task.name: 0 for task in tasks}
    for predecessor, successor in edges:
        dependents[predecessor].append(successor)
        indegree[successor] += 1

    order: list[str] = []
    queue = deque(task.name for task in tasks if indegree[task.name] == 0)
    while queue:
        current = queue.popleft()
        order.append(current)
        for dependent in dependents[current]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(tasks):
        message = "Workflow tasks contain a dependency cycle"
        raise UserInputError(
            message,
            suggestion=_WORKFLOW_GRAPH_REVIEW_SUGGESTION,
        )

    levels: dict[str, int] = {}
    predecessors_by_task: dict[str, list[str]] = {task.name: [] for task in tasks}
    for predecessor, successor in edges:
        predecessors_by_task[successor].append(predecessor)
    for name in order:
        dependencies = predecessors_by_task[name]
        if not dependencies:
            levels[name] = 0
            continue
        levels[name] = max(levels[dependency] + 1 for dependency in dependencies)
    return levels
