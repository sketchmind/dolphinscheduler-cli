from __future__ import annotations

import json
from collections import defaultdict, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from math import isfinite
from types import MappingProxyType
from typing import TypedDict, TypeGuard, cast
from uuid import uuid4

from dsctl.models.common import Direct, GlobalParamSpec, YamlObject
from dsctl.models.task_spec import (
    DEPENDENT_BASE_MONTH_DATE_VALUES,
    DEPENDENT_DAY_DATE_VALUES,
    DEPENDENT_HOUR_DATE_VALUES,
    DEPENDENT_WEEK_DATE_VALUES,
    Dependent139TaskParamsSpec,
    Sql139InlineTaskParamsSpec,
    SubWorkflowTaskParamsSpec,
    contains_ds_parameter_placeholder,
    normalize_typed_task_params,
)
from dsctl.models.workflow_spec import (
    WorkflowAuthoringContext,
    WorkflowSpec,
    WorkflowTaskSpec,
    validate_workflow_document,
    validate_workflow_task_document,
)
from dsctl.support.json_types import JsonObject, JsonValue, is_json_value
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
    is_sql_139_complete_native_package,
)

_EMPTY_TASK_REFS = TaskRefIndex.from_code_by_name({})
_SHARED_PROJECTED_LEGACY_TASK_TYPES = frozenset(
    {"DATAX", "HTTP", "MR", "PROCEDURE", "PYTHON", "SHELL", "SQL"}
)
_REVIEWED_MR_OPAQUE_PROGRAM_TYPE = "SCALA"


@dataclass(frozen=True, slots=True)
class LegacyWorkflowRefIndex:
    """Verified same-project workflow names bound to exact 1.3.9 ids."""

    _id_by_name: Mapping[str, int]

    @classmethod
    def empty(cls) -> LegacyWorkflowRefIndex:
        """Return an index that authorizes no legacy workflow references."""
        return cls(MappingProxyType({}))

    @classmethod
    def from_id_by_name(
        cls,
        id_by_name: Mapping[str, int],
    ) -> LegacyWorkflowRefIndex:
        """Freeze verified name/id pairs while rejecting ambiguous identities."""
        normalized: dict[str, int] = {}
        names_by_id: dict[int, str] = {}
        for name, workflow_id in id_by_name.items():
            if not isinstance(name, str) or not name.strip() or name != name.strip():
                message = "Legacy child workflow names must be nonblank normalized text"
                raise LegacyWorkflowGraphError(message)
            if (
                isinstance(workflow_id, bool)
                or not isinstance(workflow_id, int)
                or workflow_id <= 0
            ):
                message = f"Legacy child workflow '{name}' has an invalid native id"
                raise LegacyWorkflowGraphError(message)
            previous_name = names_by_id.get(workflow_id)
            if previous_name is not None and previous_name != name:
                message = (
                    f"Legacy workflow id {workflow_id} resolves to both "
                    f"'{previous_name}' and '{name}'"
                )
                raise LegacyWorkflowGraphError(message)
            normalized[name] = workflow_id
            names_by_id[workflow_id] = name
        return cls(MappingProxyType(normalized))

    def id_for_name(self, name: str) -> int | None:
        """Return the verified native id for one canonical child selector."""
        return self._id_by_name.get(name)

    def name_for_id(self, workflow_id: int) -> str | None:
        """Return the canonical name for one verified native id."""
        return next(
            (
                name
                for name, candidate_id in self._id_by_name.items()
                if candidate_id == workflow_id
            ),
            None,
        )


_EMPTY_WORKFLOW_REFS = LegacyWorkflowRefIndex.empty()


@dataclass(frozen=True, slots=True)
class LegacyDependentRefIndex:
    """Verified canonical names bound to exact 1.3.9 DEPENDENT native identities."""

    _native_by_selector: Mapping[
        tuple[str, str, str | None],
        tuple[int, int, str],
    ]
    _selector_by_native: Mapping[
        tuple[int, int, str],
        tuple[str, str, str | None],
    ]

    @classmethod
    def empty(cls) -> LegacyDependentRefIndex:
        """Return an index that authorizes no legacy dependent references."""
        return cls(MappingProxyType({}), MappingProxyType({}))

    @classmethod
    def from_native_by_selector(
        cls,
        native_by_selector: Mapping[
            tuple[str, str, str | None],
            tuple[int, int, str],
        ],
    ) -> LegacyDependentRefIndex:
        """Freeze a bijection between literal selectors and resolved IDs."""
        normalized: dict[tuple[str, str, str | None], tuple[int, int, str]] = {}
        inverse: dict[tuple[int, int, str], tuple[str, str, str | None]] = {}
        for selector, native in native_by_selector.items():
            project_name, workflow_name, task_name = selector
            if any(
                not isinstance(name, str) or not name or name.strip() != name
                for name in (project_name, workflow_name)
            ):
                message = "Legacy DEPENDENT project/workflow names must be normalized"
                raise LegacyWorkflowGraphError(message)
            if task_name is not None and (
                not isinstance(task_name, str)
                or not task_name
                or task_name.strip() != task_name
                or task_name == "ALL"
            ):
                message = "Legacy DEPENDENT task names must be literal and not ALL"
                raise LegacyWorkflowGraphError(message)
            project_id, definition_id, dep_tasks = native
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in (project_id, definition_id)
            ):
                message = "Legacy DEPENDENT native IDs must be positive integers"
                raise LegacyWorkflowGraphError(message)
            expected_dep_tasks = "ALL" if task_name is None else task_name
            if dep_tasks != expected_dep_tasks:
                message = (
                    "Legacy DEPENDENT depTasks must be ALL for a workflow or the "
                    "exact task selector"
                )
                raise LegacyWorkflowGraphError(message)
            previous_selector = inverse.get(native)
            if previous_selector is not None and previous_selector != selector:
                message = "Legacy DEPENDENT native identity is ambiguous"
                raise LegacyWorkflowGraphError(message)
            normalized[selector] = native
            inverse[native] = selector
        return cls(
            MappingProxyType(normalized),
            MappingProxyType(inverse),
        )

    def native_for_selector(
        self,
        project_name: str,
        workflow_name: str,
        task_name: str | None,
    ) -> tuple[int, int, str] | None:
        """Return the frozen native identity for one canonical selector."""
        return self._native_by_selector.get(
            (project_name, workflow_name, task_name),
        )

    def selector_for_native(
        self,
        project_id: int,
        definition_id: int,
        dep_tasks: str,
    ) -> tuple[str, str, str | None] | None:
        """Return names only when the exact native triple was verified."""
        return self._selector_by_native.get(
            (project_id, definition_id, dep_tasks),
        )


_EMPTY_DEPENDENT_REFS = LegacyDependentRefIndex.empty()
_LEGACY_DEPENDENT_DATE_VALUES_BY_CYCLE = MappingProxyType(
    {
        "hour": DEPENDENT_HOUR_DATE_VALUES,
        "day": DEPENDENT_DAY_DATE_VALUES,
        "week": DEPENDENT_WEEK_DATE_VALUES,
        "month": DEPENDENT_BASE_MONTH_DATE_VALUES,
    }
)


def _allow_preserved_task_type(_task_type: str) -> None:
    """Allow a server-originated legacy task type during baseline rebuilding."""


def _allow_preserved_task_identity(_task_type: str, _task_name: str) -> None:
    """Leave server-originated legacy task identities unchanged."""


def _preserve_task_params(_task_type: str, task_params: YamlObject) -> YamlObject:
    """Detach one native parameter object without typed normalization."""
    return cast("YamlObject", _thaw(task_params))


def _accept_preserved_global_params(_params: list[GlobalParamSpec]) -> None:
    """Leave already-decoded server globals to the workflow model's shape checks."""


def _normalize_legacy_sql_params(
    task_type: str,
    task_params: YamlObject,
) -> YamlObject:
    """Keep decoded typed SQL on the exact 1.3.9 canonical model."""
    if task_type != "SQL":
        message = "Exact legacy SQL authoring context received a non-SQL task"
        raise ValueError(message)
    return normalize_typed_task_params(Sql139InlineTaskParamsSpec, task_params)


def _normalize_legacy_sub_workflow_params(
    task_type: str,
    task_params: YamlObject,
) -> YamlObject:
    """Keep decoded exact 1.3.9 child-name intent on its canonical model."""
    if task_type != "SUB_WORKFLOW":
        message = "Exact legacy SUB_WORKFLOW context received another task type"
        raise ValueError(message)
    normalized = normalize_typed_task_params(SubWorkflowTaskParamsSpec, task_params)
    if set(normalized) != {"childWorkflowName"}:
        message = (
            "DolphinScheduler 1.3.9 SUB_WORKFLOW requires only "
            "task_params.childWorkflowName"
        )
        raise ValueError(message)
    return normalized


def _normalize_legacy_dependent_params(
    task_type: str,
    task_params: YamlObject,
) -> YamlObject:
    """Keep decoded exact 1.3.9 name intent on its closed canonical model."""
    if task_type != "DEPENDENT":
        message = "Exact legacy DEPENDENT context received another task type"
        raise ValueError(message)
    return normalize_typed_task_params(Dependent139TaskParamsSpec, task_params)


_LEGACY_OPAQUE_AUTHORING_CONTEXT = WorkflowAuthoringContext(
    authorize_task_type=_allow_preserved_task_type,
    validate_task_identity=_allow_preserved_task_identity,
    normalize_task_params=_preserve_task_params,
    validate_global_params=_accept_preserved_global_params,
    schedule_timezone_supported=False,
)
_LEGACY_SQL_AUTHORING_CONTEXT = WorkflowAuthoringContext(
    authorize_task_type=_allow_preserved_task_type,
    validate_task_identity=_allow_preserved_task_identity,
    normalize_task_params=_normalize_legacy_sql_params,
    validate_global_params=_accept_preserved_global_params,
    schedule_timezone_supported=False,
)
_LEGACY_SUB_WORKFLOW_AUTHORING_CONTEXT = WorkflowAuthoringContext(
    authorize_task_type=_allow_preserved_task_type,
    validate_task_identity=_allow_preserved_task_identity,
    normalize_task_params=_normalize_legacy_sub_workflow_params,
    validate_global_params=_accept_preserved_global_params,
    schedule_timezone_supported=False,
)
_LEGACY_DEPENDENT_AUTHORING_CONTEXT = WorkflowAuthoringContext(
    authorize_task_type=_allow_preserved_task_type,
    validate_task_identity=_allow_preserved_task_identity,
    normalize_task_params=_normalize_legacy_dependent_params,
    validate_global_params=_accept_preserved_global_params,
    schedule_timezone_supported=False,
)


class LegacyWorkflowGraphError(ValueError):
    """Raised when a DolphinScheduler 1.3.9 graph is not self-consistent."""


class LegacyWorkflowGraphPayload(TypedDict):
    """The three string-native graph fields accepted by DS 1.3.9."""

    processDefinitionJson: str
    locations: str
    connects: str


@dataclass(frozen=True, slots=True)
class PreparedLegacyWorkflowGraph:
    """A DS 1.3.9 graph whose generated string identities are already frozen."""

    _process_definition_json: str
    _locations: str
    _connects: str
    _task_ids: tuple[tuple[str, str], ...]
    _edges: tuple[tuple[str, str], ...]
    _required_task_id_count: int

    @property
    def required_task_id_count(self) -> int:
        """Return how many native task ids were generated while preparing."""
        return self._required_task_id_count

    @property
    def task_ids(self) -> tuple[tuple[str, str], ...]:
        """Return the frozen task-name to native-id bindings."""
        return self._task_ids

    @property
    def edges(self) -> tuple[tuple[str, str], ...]:
        """Return the canonical name-native graph edges."""
        return self._edges

    def preview(self) -> LegacyWorkflowGraphPayload:
        """Return a copy of the exact string-native payload for dry-run output."""
        return {
            "processDefinitionJson": self._process_definition_json,
            "locations": self._locations,
            "connects": self._connects,
        }

    def materialize(self) -> LegacyWorkflowGraphPayload:
        """Return the same frozen payload for a mutating request."""
        return self.preview()


@dataclass(frozen=True, slots=True)
class DecodedLegacyTask:
    """One canonical task projection with its original DS 1.3.9 fields."""

    id: str
    name: str
    type: str
    depends_on: tuple[str, ...]
    document: Mapping[str, JsonValue]
    native_fields: Mapping[str, JsonValue]
    task_params_reencode_source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE


@dataclass(frozen=True, slots=True)
class DecodedLegacyWorkflowGraph:
    """Validated projection of the three redundant DS 1.3.9 graph strings."""

    tasks: tuple[DecodedLegacyTask, ...]
    edges: tuple[tuple[str, str], ...]
    global_params: tuple[JsonValue, ...]
    timeout: int
    tenant_id: int | None
    native_process_fields: Mapping[str, JsonValue]
    native_locations: Mapping[str, JsonValue]

    @property
    def task_ids(self) -> tuple[tuple[str, str], ...]:
        """Return task-name to native string-id bindings in source order."""
        return tuple((task.name, task.id) for task in self.tasks)

    def workflow_document(
        self,
        *,
        name: str,
        project: str | None = None,
        description: str | None = None,
        release_state: str = "OFFLINE",
    ) -> JsonObject:
        """Project the legacy graph into the canonical workflow YAML shape."""
        workflow: JsonObject = {
            "name": name,
            "timeout": self.timeout,
            "global_params": [_thaw(value) for value in self.global_params],
            "release_state": release_state,
        }
        if project is not None:
            workflow["project"] = project
        if description is not None:
            workflow["description"] = description
        return {
            "workflow": workflow,
            "tasks": [_thaw(task.document) for task in self.tasks],
        }

    def to_workflow_spec(
        self,
        *,
        name: str,
        project: str | None = None,
        description: str | None = None,
        release_state: str = "OFFLINE",
    ) -> WorkflowSpec:
        """Build the canonical validated model needed by workflow services."""
        document = self.workflow_document(
            name=name,
            project=project,
            description=description,
            release_state=release_state,
        )
        validated_tasks = [
            validate_workflow_task_document(
                cast("YamlObject", _thaw(task.document)),
                authoring_context=_decoded_task_authoring_context(task),
            )
            for task in self.tasks
        ]
        return validate_workflow_document(
            cast(
                "YamlObject",
                {
                    "workflow": document["workflow"],
                    "tasks": [
                        task.model_dump(mode="json", exclude_none=False)
                        for task in validated_tasks
                    ],
                },
            ),
            authoring_context=_LEGACY_OPAQUE_AUTHORING_CONTEXT,
        )


def _decoded_task_authoring_context(
    task: DecodedLegacyTask,
) -> WorkflowAuthoringContext | None:
    """Select exact validation without reinterpreting decoded legacy params."""
    if task.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE:
        return _LEGACY_OPAQUE_AUTHORING_CONTEXT
    if task.type == "SQL":
        return _LEGACY_SQL_AUTHORING_CONTEXT
    if task.type == "SUB_WORKFLOW":
        return _LEGACY_SUB_WORKFLOW_AUTHORING_CONTEXT
    if task.type == "DEPENDENT":
        return _LEGACY_DEPENDENT_AUTHORING_CONTEXT
    return None


def decode_legacy_workflow_graph(
    process_definition_json: str,
    locations: str,
    connects: str,
    *,
    workflow_refs: LegacyWorkflowRefIndex = _EMPTY_WORKFLOW_REFS,
    dependent_refs: LegacyDependentRefIndex = _EMPTY_DEPENDENT_REFS,
    resource_refs: TaskResourceRefIndex | None = None,
) -> DecodedLegacyWorkflowGraph:
    """Decode and cross-check the three DS 1.3.9 native graph strings."""
    process_data = _json_object(process_definition_json, label="processDefinitionJson")
    raw_tasks = process_data.get("tasks")
    if not _is_json_sequence(raw_tasks):
        msg = "processDefinitionJson.tasks must be an array"
        raise LegacyWorkflowGraphError(msg)

    decoded_tasks: list[DecodedLegacyTask] = []
    names: set[str] = set()
    ids: set[str] = set()
    for index, raw_task in enumerate(raw_tasks):
        task = _json_mapping(raw_task, label=f"processDefinitionJson.tasks[{index}]")
        task_id = _required_text(task, "id", label=f"task[{index}]")
        task_name = _required_text(task, "name", label=f"task[{index}]")
        task_type = _required_text(task, "type", label=f"task[{index}]")
        if task_name in names:
            msg = f"Native task name '{task_name}' is duplicated"
            raise LegacyWorkflowGraphError(msg)
        if task_id in ids:
            msg = f"Native task id '{task_id}' is duplicated"
            raise LegacyWorkflowGraphError(msg)
        if "code" in task or "version" in task:
            message = (
                f"Native task '{task_name}' contains modern code/version "
                "identity fields"
            )
            raise LegacyWorkflowGraphError(message)
        names.add(task_name)
        ids.add(task_id)
        predecessors = _pre_task_names(task.get("preTasks"), task_name=task_name)
        (
            document,
            task_params_reencode_source,
            canonical_task_type,
        ) = _canonical_task_document(
            task,
            name=task_name,
            task_type=task_type,
            predecessors=predecessors,
            workflow_refs=workflow_refs,
            dependent_refs=dependent_refs,
            resource_refs=resource_refs,
        )
        decoded_tasks.append(
            DecodedLegacyTask(
                id=task_id,
                name=task_name,
                type=canonical_task_type,
                depends_on=predecessors,
                document=_freeze_mapping(document),
                native_fields=_freeze_mapping(dict(task)),
                task_params_reencode_source=task_params_reencode_source,
            )
        )

    task_by_name = {task.name: task for task in decoded_tasks}
    task_by_id = {task.id: task for task in decoded_tasks}
    pre_edges: list[tuple[str, str]] = []
    for decoded_task in decoded_tasks:
        for predecessor in decoded_task.depends_on:
            if predecessor not in task_by_name:
                message = (
                    f"Native task '{decoded_task.name}' depends on unknown task "
                    f"'{predecessor}'"
                )
                raise LegacyWorkflowGraphError(message)
            pre_edges.append((predecessor, decoded_task.name))
    _require_unique_edges(pre_edges, label="task preTasks")
    decoded_tasks = _project_exact_legacy_conditions_tasks(
        decoded_tasks,
        edges=pre_edges,
    )

    location_data = _json_object(locations, label="locations")
    location_edges = _location_edges(location_data, task_by_id=task_by_id)
    connect_data = _json_array(connects, label="connects")
    connect_edges = _connect_edges(connect_data, task_by_id=task_by_id)
    _require_same_graph(pre_edges, connect_edges, source="connects")
    _require_same_graph(pre_edges, location_edges, source="locations.targetarr")

    global_params = process_data.get("globalParams", [])
    if not _is_json_sequence(global_params):
        msg = "processDefinitionJson.globalParams must be an array"
        raise LegacyWorkflowGraphError(msg)
    timeout = _non_negative_int(process_data.get("timeout", 0), label="timeout")
    tenant_value = process_data.get("tenantId")
    tenant_id = None if tenant_value is None else _tenant_id(tenant_value)
    native_process_fields = {
        key: value
        for key, value in process_data.items()
        if key not in {"tasks", "globalParams", "timeout", "tenantId"}
    }
    return DecodedLegacyWorkflowGraph(
        tasks=tuple(decoded_tasks),
        edges=tuple(pre_edges),
        global_params=tuple(_freeze(value) for value in global_params),
        timeout=timeout,
        tenant_id=tenant_id,
        native_process_fields=_freeze_mapping(native_process_fields),
        native_locations=_freeze_mapping(location_data),
    )


def legacy_safe_sub_process_definition_ids(
    graph: DecodedLegacyWorkflowGraph,
) -> tuple[int, ...]:
    """Return IDs from exact id-only SUB_PROCESS packages eligible for reversal."""
    workflow_ids: list[int] = []
    seen: set[int] = set()
    for task in graph.tasks:
        if task.native_fields.get("type") != "SUB_PROCESS":
            continue
        params = _embedded_json(
            task.native_fields.get("params"),
            label=f"task '{task.name}'.params",
        )
        if not isinstance(params, Mapping) or set(params) != {"processDefinitionId"}:
            continue
        workflow_id = params.get("processDefinitionId")
        if (
            isinstance(workflow_id, bool)
            or not isinstance(workflow_id, int)
            or workflow_id <= 0
        ):
            continue
        if workflow_id not in seen:
            seen.add(workflow_id)
            workflow_ids.append(workflow_id)
    return tuple(workflow_ids)


def legacy_safe_dependent_targets(
    graph: DecodedLegacyWorkflowGraph,
) -> tuple[tuple[int, int, str], ...]:
    """Inventory exact native DEPENDENT targets eligible for reverse binding."""
    targets: list[tuple[int, int, str]] = []
    seen: set[tuple[int, int, str]] = set()
    for task in graph.tasks:
        if task.type != "DEPENDENT":
            continue
        task_targets = _safe_legacy_dependent_task_targets(task.native_fields)
        if task_targets is None:
            continue
        for target in task_targets:
            if target in seen:
                continue
            seen.add(target)
            targets.append(target)
    return tuple(targets)


def _safe_legacy_dependent_task_targets(
    task: Mapping[str, JsonValue],
) -> tuple[tuple[int, int, str], ...] | None:
    params = _embedded_json(task.get("params"), label="DEPENDENT task params")
    dependence = _embedded_json(
        task.get("dependence"),
        label="DEPENDENT task dependence",
    )
    if (
        params != {}
        or not isinstance(dependence, Mapping)
        or set(dependence)
        != {
            "relation",
            "dependTaskList",
        }
    ):
        return None
    if dependence.get("relation") not in {"AND", "OR"}:
        return None
    groups = dependence.get("dependTaskList")
    if not _is_json_sequence(groups) or not groups:
        return None
    targets: list[tuple[int, int, str]] = []
    for raw_group in groups:
        group_targets = _safe_legacy_dependent_group_targets(raw_group)
        if group_targets is None:
            return None
        targets.extend(group_targets)
    return tuple(targets)


def _safe_legacy_dependent_group_targets(
    raw_group: JsonValue,
) -> tuple[tuple[int, int, str], ...] | None:
    if not isinstance(raw_group, Mapping) or set(raw_group) != {
        "relation",
        "dependItemList",
    }:
        return None
    if raw_group.get("relation") not in {"AND", "OR"}:
        return None
    items = raw_group.get("dependItemList")
    if not _is_json_sequence(items) or not items:
        return None
    targets: list[tuple[int, int, str]] = []
    for raw_item in items:
        target = _safe_legacy_dependent_item_target(raw_item)
        if target is None:
            return None
        targets.append(target)
    return tuple(targets)


def _safe_legacy_dependent_item_target(
    raw_item: JsonValue,
) -> tuple[int, int, str] | None:
    if not isinstance(raw_item, Mapping) or set(raw_item) != {
        "projectId",
        "definitionId",
        "depTasks",
        "cycle",
        "dateValue",
    }:
        return None
    project_id = raw_item.get("projectId")
    definition_id = raw_item.get("definitionId")
    dep_tasks = raw_item.get("depTasks")
    cycle = raw_item.get("cycle")
    date_value = raw_item.get("dateValue")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in (project_id, definition_id)
    ):
        return None
    if (
        not isinstance(dep_tasks, str)
        or not dep_tasks
        or dep_tasks.strip() != dep_tasks
        or contains_ds_parameter_placeholder(dep_tasks)
    ):
        return None
    if (
        not isinstance(cycle, str)
        or not isinstance(date_value, str)
        or date_value not in _LEGACY_DEPENDENT_DATE_VALUES_BY_CYCLE.get(cycle, ())
    ):
        return None
    return cast("int", project_id), cast("int", definition_id), dep_tasks


def legacy_runtime_nested_workflow_definition_ids(
    graph: DecodedLegacyWorkflowGraph,
) -> tuple[int, ...]:
    """Return every ID followed by exact 1.3.9 ``recurseFindSubProcessId``.

    Upstream checks each task's native ``params`` object for the
    ``processDefinitionId`` key without checking task type.  The audit mirrors
    that behavior so an opaque or richer task cannot hide a runtime edge.
    """
    workflow_ids: list[int] = []
    seen: set[int] = set()
    for task in graph.tasks:
        params = _embedded_json(
            task.native_fields.get("params"),
            label=f"task '{task.name}'.params",
        )
        if not isinstance(params, Mapping):
            message = f"Native task '{task.name}' params must be an object"
            raise LegacyWorkflowGraphError(message)
        if "processDefinitionId" not in params:
            continue
        raw_workflow_id = params.get("processDefinitionId")
        if raw_workflow_id is None:
            continue
        workflow_id = _legacy_runtime_workflow_id(
            raw_workflow_id,
            task_name=task.name,
        )
        if workflow_id is None:
            continue
        if workflow_id not in seen:
            seen.add(workflow_id)
            workflow_ids.append(workflow_id)
    return tuple(workflow_ids)


def prepared_legacy_runtime_nested_workflow_definition_ids(
    compilation: PreparedLegacyWorkflowGraph,
) -> tuple[int, ...]:
    """Return exact runtime edges from one already-frozen legacy compilation."""
    payload = compilation.preview()
    graph = decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
    )
    return legacy_runtime_nested_workflow_definition_ids(graph)


def _legacy_runtime_workflow_id(
    value: JsonValue,
    *,
    task_name: str,
) -> int | None:
    """Mirror both FastJSON integer passes used by the 1.3.9 runtime."""
    try:
        condition_id = _fastjson_cast_to_integer(value)
    except (OverflowError, ValueError):
        pass
    else:
        if condition_id is None:
            return None
        try:
            workflow_id = _fastjson_cast_to_bean_integer(value)
        except (OverflowError, ValueError):
            pass
        else:
            if workflow_id is not None and workflow_id > 0:
                return workflow_id
    message = f"Native task '{task_name}' has an invalid runtime processDefinitionId"
    raise LegacyWorkflowGraphError(message)


def _fastjson_cast_to_integer(value: JsonValue) -> int | None:
    """Implement the JSON-relevant branches of FastJSON 1.2.75 castToInt."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return _java_int_value(value)
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError
        # ``compilation.preview`` serializes this binary float with Python's
        # shortest round-tripping decimal spelling. FastJSON parses that final
        # wire spelling as BigDecimal, so cast the same spelling before
        # applying Java's low-32-bit intValue behavior.
        return _java_int_value(int(Decimal(str(value))))
    if isinstance(value, str):
        return _fastjson_integer_from_text(value)
    if isinstance(value, Mapping):
        return _fastjson_integer_from_mapping(value)
    raise ValueError


def _fastjson_cast_to_bean_integer(value: JsonValue) -> int | None:
    """Mirror IntegerCodec's ordered root-map pass for the bean field."""
    if not isinstance(value, Mapping):
        return _fastjson_cast_to_integer(value)
    if set(value) != {"andIncrement", "andDecrement"}:
        raise ValueError
    # IntegerCodec reparses an object-valued Integer into JSONObject(true), so
    # TypeUtils skips the first wire-ordered value and casts the second. Nested
    # objects return to the ordinary JSONObject/HashMap cast path.
    return _fastjson_cast_to_integer(tuple(value.values())[1])


def _fastjson_integer_from_text(value: str) -> int | None:
    if value in {"", "null", "NULL"}:
        return None
    normalized = value.replace(",", "")
    dot_index = normalized.rfind(".")
    if dot_index >= 0 and all(
        character == "0" for character in normalized[dot_index + 1 :]
    ):
        normalized = normalized[:dot_index]
    if not _is_java_decimal_integer(normalized):
        raise ValueError
    candidate = int(normalized, 10)
    if candidate < -(2**31) or candidate > 2**31 - 1:
        raise OverflowError
    return candidate


def _fastjson_integer_from_mapping(value: Mapping[str, JsonValue]) -> int | None:
    if set(value) != {"andIncrement", "andDecrement"}:
        raise ValueError
    # JSONObject uses HashMap here. These fixed keys occupy buckets 5 and 0,
    # so TypeUtils skips andDecrement and casts andIncrement second.
    return _fastjson_cast_to_integer(value["andIncrement"])


def _java_int_value(value: int) -> int:
    """Return Java Number.intValue's signed low 32 bits."""
    narrowed = value & 0xFFFF_FFFF
    return narrowed if narrowed < 2**31 else narrowed - 2**32


def _is_java_decimal_integer(value: str) -> bool:
    """Match ``Integer.parseInt`` text syntax without Python-only leniency."""
    digits = value[1:] if value[:1] in {"+", "-"} else value
    return bool(digits) and all(
        ord(character) <= 0xFFFF and character.isdecimal() for character in digits
    )


def prepare_legacy_workflow_lint_graph(
    spec: WorkflowSpec,
) -> PreparedLegacyWorkflowGraph:
    """Build a deterministic local-only legacy preview below the wire seam.

    Exact 1.3.9 authoring ultimately needs server database IDs for nested
    workflows and typed MR resources. Local lint has no server identity
    context, so the legacy compiler owns isolated preview bindings that are
    used only to validate the canonical graph and are never exposed as or sent
    as native identities.
    """
    task_ids = {
        task.name: f"lint-task-{index}"
        for index, task in enumerate(spec.tasks, start=1)
    }
    child_names = tuple(
        dict.fromkeys(
            child_name
            for task in spec.tasks
            if task.type == "SUB_WORKFLOW" and task.task_params is not None
            if isinstance(
                (child_name := task.task_params.get("childWorkflowName")),
                str,
            )
        )
    )
    workflow_refs = LegacyWorkflowRefIndex.from_id_by_name(
        {
            child_name: preview_id
            for preview_id, child_name in enumerate(child_names, start=1)
        }
    )
    dependent_selectors = _legacy_dependent_selectors(spec)
    project_ids = {
        project_name: project_id
        for project_id, project_name in enumerate(
            dict.fromkeys(selector[0] for selector in dependent_selectors),
            start=1,
        )
    }
    definition_ids = {
        workflow_selector: definition_id
        for definition_id, workflow_selector in enumerate(
            dict.fromkeys(
                (project_name, workflow_name)
                for project_name, workflow_name, _task_name in dependent_selectors
            ),
            start=1,
        )
    }
    dependent_refs = LegacyDependentRefIndex.from_native_by_selector(
        {
            selector: (
                project_ids[selector[0]],
                definition_ids[(selector[0], selector[1])],
                "ALL" if selector[2] is None else selector[2],
            )
            for selector in dependent_selectors
        }
    )
    mr_resource_names = tuple(
        dict.fromkeys(
            main_jar
            for task in spec.tasks
            if task.type == "MR" and isinstance(task.task_params, Mapping)
            if task.task_params.get("programType") != _REVIEWED_MR_OPAQUE_PROGRAM_TYPE
            if isinstance((main_jar := task.task_params.get("mainJar")), str)
        )
    )
    resource_refs = TaskResourceRefIndex.from_id_by_full_name(
        {
            full_name: preview_id
            for preview_id, full_name in enumerate(mr_resource_names, start=1)
        }
    )
    return prepare_legacy_workflow_graph(
        spec,
        task_id_factory=task_ids.__getitem__,
        workflow_refs=workflow_refs,
        dependent_refs=dependent_refs,
        resource_refs=resource_refs,
    )


def _legacy_dependent_selectors(
    spec: WorkflowSpec,
) -> tuple[tuple[str, str, str | None], ...]:
    """Collect normalized canonical selectors for isolated lint bindings."""
    selectors: list[tuple[str, str, str | None]] = []
    for task in spec.tasks:
        selectors.extend(_legacy_dependent_task_selectors(task))
    return tuple(dict.fromkeys(selectors))


def _legacy_dependent_task_selectors(
    task: WorkflowTaskSpec,
) -> tuple[tuple[str, str, str | None], ...]:
    if task.type != "DEPENDENT" or task.task_params is None:
        return ()
    dependence = task.task_params.get("dependence")
    if not isinstance(dependence, Mapping):
        return ()
    groups = dependence.get("dependTaskList")
    if not _is_json_sequence(groups):
        return ()
    selectors: list[tuple[str, str, str | None]] = []
    for raw_group in groups:
        if not isinstance(raw_group, Mapping):
            continue
        items = raw_group.get("dependItemList")
        if not _is_json_sequence(items):
            continue
        for raw_item in items:
            selector = _legacy_dependent_item_selector(raw_item)
            if selector is not None:
                selectors.append(selector)
    return tuple(selectors)


def _legacy_dependent_item_selector(
    raw_item: JsonValue,
) -> tuple[str, str, str | None] | None:
    if not isinstance(raw_item, Mapping):
        return None
    project_name = raw_item.get("projectName")
    workflow_name = raw_item.get("workflowName")
    task_name = raw_item.get("taskName")
    if not isinstance(project_name, str) or not isinstance(workflow_name, str):
        return None
    if task_name is not None and not isinstance(task_name, str):
        return None
    return project_name, workflow_name, task_name


def prepare_legacy_workflow_graph(
    spec: WorkflowSpec,
    *,
    task_id_factory: Callable[[str], str] | None = None,
    baseline: DecodedLegacyWorkflowGraph | None = None,
    workflow_refs: LegacyWorkflowRefIndex = _EMPTY_WORKFLOW_REFS,
    dependent_refs: LegacyDependentRefIndex = _EMPTY_DEPENDENT_REFS,
    resource_refs: TaskResourceRefIndex | None = None,
) -> PreparedLegacyWorkflowGraph:
    """Compile canonical workflow intent into the DS 1.3.9 native graph strings."""
    validate_legacy_workflow_constraints(spec)
    _require_opaque_conditions_baseline_unchanged(spec.tasks, baseline=baseline)
    _require_opaque_dependent_baseline_unchanged(spec.tasks, baseline=baseline)
    baseline_tasks = (
        {} if baseline is None else {task.name: task for task in baseline.tasks}
    )
    edges = _workflow_edges(spec.tasks, baseline_tasks=baseline_tasks)
    task_ids, generated_task_id_count = _allocate_task_ids(
        spec.tasks,
        task_id_factory=(
            _new_legacy_task_id if task_id_factory is None else task_id_factory
        ),
        baseline_tasks=baseline_tasks,
    )
    id_by_name = dict(task_ids)
    levels = _task_levels(spec.tasks, edges)
    native_tasks = [
        _native_task(
            task,
            task_id=id_by_name[task.name],
            predecessors=tuple(
                predecessor
                for predecessor, successor in edges
                if successor == task.name
            ),
            preserved=baseline_tasks.get(task.name),
            workflow_refs=workflow_refs,
            dependent_refs=dependent_refs,
            resource_refs=resource_refs,
        )
        for task in spec.tasks
    ]
    process_data = _preserved_process_data(baseline)
    process_data.update(
        {
            "globalParams": _global_params(spec),
            "tasks": native_tasks,
            "timeout": spec.workflow.timeout,
            "tenantId": (
                -1
                if baseline is None or baseline.tenant_id is None
                else baseline.tenant_id
            ),
        }
    )
    locations = _locations(
        spec.tasks,
        id_by_name=id_by_name,
        edges=edges,
        levels=levels,
        baseline=baseline,
    )
    connects = [
        {
            "endPointSourceId": id_by_name[predecessor],
            "endPointTargetId": id_by_name[successor],
        }
        for predecessor, successor in edges
    ]
    return PreparedLegacyWorkflowGraph(
        _process_definition_json=_json_text(process_data),
        _locations=_json_text(locations),
        _connects=_json_text(connects),
        _task_ids=task_ids,
        _edges=edges,
        _required_task_id_count=generated_task_id_count,
    )


def _new_legacy_task_id(_task_name: str) -> str:
    """Generate one opaque task identity in the native graph-owning module."""
    return f"tasks-{uuid4().hex}"


def validate_legacy_workflow_constraints(spec: WorkflowSpec) -> None:
    """Reject settings the exact legacy graph cannot represent before binding."""
    if spec.workflow.execution_type.value != "PARALLEL":
        message = (
            "workflow.execution_type is not representable by DolphinScheduler "
            "1.3.9; only PARALLEL matches its native graph execution."
        )
        raise LegacyWorkflowGraphError(message)
    for task in spec.tasks:
        unsupported = {
            "environment_code": task.environment_code,
            "task_group_id": task.task_group_id,
            "task_group_priority": task.task_group_priority,
            "delay": task.delay or None,
            "cpu_quota": task.cpu_quota,
            "memory_max": task.memory_max,
        }
        for field, value in unsupported.items():
            if value is None:
                continue
            message = (
                f"Task '{task.name}' field '{field}' is not representable by "
                "DolphinScheduler 1.3.9."
            )
            raise LegacyWorkflowGraphError(message)


def _require_opaque_conditions_baseline_unchanged(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    baseline: DecodedLegacyWorkflowGraph | None,
) -> None:
    """Keep unprojectable split CONDITIONS state on metadata-only edits."""
    if baseline is None:
        return
    opaque_conditions = tuple(
        task
        for task in baseline.tasks
        if task.type == "CONDITIONS"
        and task.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    if not opaque_conditions:
        return

    current_by_name = {task.name: task for task in tasks}
    baseline_names = {task.name for task in baseline.tasks}
    current_edges = {
        (predecessor, task.name) for task in tasks for predecessor in task.depends_on
    }
    if set(current_by_name) != baseline_names or current_edges != set(baseline.edges):
        message = (
            "Server-originated opaque CONDITIONS requires unchanged task names "
            "and topology because its split outer TaskNode references are not "
            "represented in standalone YAML"
        )
        raise LegacyWorkflowGraphError(message)

    for preserved in opaque_conditions:
        task = current_by_name[preserved.name]
        if any(
            (
                task.type != preserved.type,
                task.task_params != _canonical_preserved_task_params(preserved),
                task.command is not None,
            )
        ):
            message = (
                f"Server-originated opaque CONDITIONS task '{task.name}' requires "
                "an unchanged payload because its split outer TaskNode state is "
                "not represented in standalone YAML"
            )
            raise LegacyWorkflowGraphError(message)


def _require_opaque_dependent_baseline_unchanged(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    baseline: DecodedLegacyWorkflowGraph | None,
) -> None:
    """Preserve richer split DEPENDENT state while allowing unrelated topology."""
    if baseline is None:
        return
    opaque_dependents = tuple(
        task
        for task in baseline.tasks
        if task.type == "DEPENDENT"
        and task.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    if not opaque_dependents:
        return
    current_by_name = {task.name: task for task in tasks}
    for preserved in opaque_dependents:
        task = current_by_name.get(preserved.name)
        if task is not None and not any(
            (
                task.type != preserved.type,
                task.task_params != _canonical_preserved_task_params(preserved),
                task.command != preserved.document.get("command"),
            )
        ):
            continue
        message = (
            f"Server-originated opaque DEPENDENT task '{preserved.name}' requires "
            "its name, type, task_params, and command to remain unchanged because "
            "its split outer TaskNode state is not represented in standalone YAML"
        )
        raise LegacyWorkflowGraphError(message)


def _allocate_task_ids(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    task_id_factory: Callable[[str], str],
    baseline_tasks: Mapping[str, DecodedLegacyTask],
) -> tuple[tuple[tuple[str, str], ...], int]:
    bindings: list[tuple[str, str]] = []
    seen: set[str] = set()
    generated_count = 0
    for task in tasks:
        preserved = baseline_tasks.get(task.name)
        if preserved is None:
            task_id = task_id_factory(task.name)
            generated_count += 1
        else:
            task_id = preserved.id
        if not isinstance(task_id, str) or not task_id.strip():
            message = f"Native task id factory returned an invalid id for '{task.name}'"
            raise LegacyWorkflowGraphError(message)
        if task_id in seen:
            message = f"Native task id '{task_id}' is duplicated"
            raise LegacyWorkflowGraphError(message)
        seen.add(task_id)
        bindings.append((task.name, task_id))
    return tuple(bindings), generated_count


def _workflow_edges(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    baseline_tasks: Mapping[str, DecodedLegacyTask],
) -> tuple[tuple[str, str], ...]:
    task_names = {task.name for task in tasks}
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for task in tasks:
        for predecessor in task.depends_on:
            _append_legacy_workflow_edge(
                edges,
                seen=seen,
                task_names=task_names,
                predecessor=predecessor,
                successor=task.name,
                label="depends_on",
            )
    _extend_legacy_conditions_edges(
        tasks,
        baseline_tasks=baseline_tasks,
        task_names=task_names,
        edges=edges,
        seen=seen,
    )
    return tuple(edges)


def _extend_legacy_conditions_edges(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    baseline_tasks: Mapping[str, DecodedLegacyTask],
    task_names: set[str],
    edges: list[tuple[str, str]],
    seen: set[tuple[str, str]],
) -> None:
    typed_conditions: list[
        tuple[WorkflowTaskSpec, tuple[str, ...], tuple[str, ...]]
    ] = []
    for task in tasks:
        if task.type.upper() != "CONDITIONS" or task.task_params is None:
            continue
        params = task.task_params
        preserved = baseline_tasks.get(task.name)
        _params, source = _task_params_for_encode(task, preserved=preserved)
        if source is ProjectionSource.OPAQUE_PRESERVE and preserved is not None:
            # A server-originated opaque 1.3.9 CONDITIONS task carries its graph
            # and split state in the preserved TaskNode, not canonical params.
            continue
        dependence, condition_result = _encode_legacy_conditions_params(
            params,
            task_name=task.name,
        )
        predicates = _legacy_condition_predicate_names(dependence)
        branches = _legacy_condition_branch_names(condition_result)
        extra_predecessors = set(task.depends_on).difference(predicates)
        if extra_predecessors:
            names = ", ".join(sorted(extra_predecessors))
            message = (
                f"CONDITIONS task '{task.name}' has depends_on tasks outside its "
                f"predicate tree: {names}"
            )
            raise LegacyWorkflowGraphError(message)
        for predecessor in predicates:
            _append_legacy_workflow_edge(
                edges,
                seen=seen,
                task_names=task_names,
                predecessor=predecessor,
                successor=task.name,
                label="task_params.dependence",
            )
        for successor in branches:
            _append_legacy_workflow_edge(
                edges,
                seen=seen,
                task_names=task_names,
                predecessor=task.name,
                successor=successor,
                label="task_params.conditionResult",
            )
        typed_conditions.append((task, predicates, branches))

    for task, _predicates, branch_names in typed_conditions:
        actual_successors = {
            successor for predecessor, successor in edges if predecessor == task.name
        }
        expected_successors = set(branch_names)
        if actual_successors != expected_successors:
            extras = ", ".join(sorted(actual_successors - expected_successors))
            message = (
                f"CONDITIONS task '{task.name}' may route only to its success and "
                "failure branches"
            )
            if extras:
                message = f"{message}; extra successors: {extras}"
            raise LegacyWorkflowGraphError(message)


def _append_legacy_workflow_edge(
    edges: list[tuple[str, str]],
    *,
    seen: set[tuple[str, str]],
    task_names: set[str],
    predecessor: str,
    successor: str,
    label: str,
) -> None:
    if predecessor not in task_names:
        message = (
            f"Task '{successor}' references unknown task '{predecessor}' in {label}"
        )
        raise LegacyWorkflowGraphError(message)
    if successor not in task_names:
        message = (
            f"Task '{predecessor}' references unknown task '{successor}' in {label}"
        )
        raise LegacyWorkflowGraphError(message)
    if predecessor == successor:
        message = f"Task '{successor}' cannot reference itself in {label}"
        raise LegacyWorkflowGraphError(message)
    edge = (predecessor, successor)
    if edge not in seen:
        seen.add(edge)
        edges.append(edge)


def _encode_legacy_conditions_params(
    params: Mapping[str, JsonValue],
    *,
    task_name: str,
) -> tuple[JsonObject, JsonObject]:
    """Project canonical 1.3.9 CONDITIONS params into split TaskNode fields."""
    exact = _legacy_conditions_object(
        params,
        label=f"CONDITIONS task '{task_name}' task_params",
        keys={"dependence", "conditionResult"},
    )
    dependence = _project_legacy_conditions_dependence(
        exact["dependence"],
        input_ref="task",
        output_ref="depTasks",
        label=f"CONDITIONS task '{task_name}'.dependence",
    )
    condition_result = _project_legacy_conditions_result(
        exact["conditionResult"],
        label=f"CONDITIONS task '{task_name}'.conditionResult",
    )
    return dependence, condition_result


def _decode_legacy_conditions_params(
    task: Mapping[str, JsonValue],
    *,
    params: JsonObject,
    task_name: str,
) -> JsonObject | None:
    """Return the exact canonical split-wire subset or keep native state opaque."""
    if params:
        return None
    try:
        raw_dependence = _embedded_json(
            task.get("dependence"),
            label=f"task '{task_name}'.dependence",
        )
        raw_result = _embedded_json(
            task.get("conditionResult"),
            label=f"task '{task_name}'.conditionResult",
        )
        dependence = _project_legacy_conditions_dependence(
            raw_dependence,
            input_ref="depTasks",
            output_ref="task",
            label=f"task '{task_name}'.dependence",
        )
        condition_result = _project_legacy_conditions_result(
            raw_result,
            label=f"task '{task_name}'.conditionResult",
        )
    except LegacyWorkflowGraphError:
        return None
    return {
        "dependence": dependence,
        "conditionResult": condition_result,
    }


def _project_legacy_conditions_dependence(
    value: JsonValue,
    *,
    input_ref: str,
    output_ref: str,
    label: str,
) -> JsonObject:
    dependence = _legacy_conditions_object(
        value,
        label=label,
        keys={"relation", "dependTaskList"},
    )
    relation = _legacy_conditions_relation(
        dependence["relation"],
        label=f"{label}.relation",
    )
    groups = _legacy_conditions_list(
        dependence["dependTaskList"],
        label=f"{label}.dependTaskList",
    )
    projected_groups: list[JsonValue] = []
    for group_index, raw_group in enumerate(groups):
        group_label = f"{label}.dependTaskList[{group_index}]"
        group = _legacy_conditions_object(
            raw_group,
            label=group_label,
            keys={"relation", "dependItemList"},
        )
        items = _legacy_conditions_list(
            group["dependItemList"],
            label=f"{group_label}.dependItemList",
        )
        projected_items: list[JsonValue] = []
        for item_index, raw_item in enumerate(items):
            item_label = f"{group_label}.dependItemList[{item_index}]"
            item = _legacy_conditions_object(
                raw_item,
                label=item_label,
                keys={input_ref, "status"},
            )
            status = item["status"]
            if status not in {"SUCCESS", "FAILURE"}:
                message = f"{item_label}.status must be SUCCESS or FAILURE"
                raise LegacyWorkflowGraphError(message)
            projected_items.append(
                {
                    output_ref: _legacy_conditions_name(
                        item[input_ref],
                        label=f"{item_label}.{input_ref}",
                    ),
                    "status": status,
                }
            )
        projected_groups.append(
            {
                "relation": _legacy_conditions_relation(
                    group["relation"],
                    label=f"{group_label}.relation",
                ),
                "dependItemList": projected_items,
            }
        )
    return {"relation": relation, "dependTaskList": projected_groups}


def _project_legacy_conditions_result(
    value: JsonValue,
    *,
    label: str,
) -> JsonObject:
    result = _legacy_conditions_object(
        value,
        label=label,
        keys={"successNode", "failedNode"},
    )
    projected: JsonObject = {}
    for outcome in ("successNode", "failedNode"):
        nodes = _legacy_conditions_list(
            result[outcome],
            label=f"{label}.{outcome}",
            exact_length=1,
        )
        projected[outcome] = [
            _legacy_conditions_name(nodes[0], label=f"{label}.{outcome}[0]")
        ]
    success = cast("list[JsonValue]", projected["successNode"])[0]
    failure = cast("list[JsonValue]", projected["failedNode"])[0]
    if success == failure:
        message = f"{label} successNode and failedNode must name different tasks"
        raise LegacyWorkflowGraphError(message)
    return projected


def _legacy_conditions_object(
    value: JsonValue | Mapping[str, JsonValue],
    *,
    label: str,
    keys: set[str],
) -> JsonObject:
    if not isinstance(value, Mapping):
        message = f"{label} must be an object"
        raise LegacyWorkflowGraphError(message)
    actual_keys = set(value)
    if actual_keys != keys:
        names = ", ".join(sorted(actual_keys.symmetric_difference(keys)))
        message = f"{label} has missing or unsupported fields: {names}"
        raise LegacyWorkflowGraphError(message)
    return dict(value)


def _legacy_conditions_list(
    value: JsonValue,
    *,
    label: str,
    exact_length: int | None = None,
) -> tuple[JsonValue, ...]:
    if not _is_json_sequence(value):
        message = f"{label} must be an array"
        raise LegacyWorkflowGraphError(message)
    items = tuple(value)
    if exact_length is None and not items:
        message = f"{label} must not be empty"
        raise LegacyWorkflowGraphError(message)
    if exact_length is not None and len(items) != exact_length:
        message = f"{label} must contain exactly {exact_length} task"
        raise LegacyWorkflowGraphError(message)
    return items


def _legacy_conditions_relation(value: JsonValue, *, label: str) -> str:
    if not isinstance(value, str) or value not in {"AND", "OR"}:
        message = f"{label} must be AND or OR"
        raise LegacyWorkflowGraphError(message)
    return value


def _legacy_conditions_name(value: JsonValue, *, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        message = f"{label} must be a non-empty exact task name"
        raise LegacyWorkflowGraphError(message)
    return value


def _legacy_condition_predicate_names(
    dependence: Mapping[str, JsonValue],
) -> tuple[str, ...]:
    names: list[str] = []
    groups = cast("Sequence[JsonValue]", dependence["dependTaskList"])
    for raw_group in groups:
        group = cast("Mapping[str, JsonValue]", raw_group)
        items = cast("Sequence[JsonValue]", group["dependItemList"])
        for raw_item in items:
            item = cast("Mapping[str, JsonValue]", raw_item)
            names.append(cast("str", item["depTasks"]))
    return tuple(dict.fromkeys(names))


def _legacy_condition_branch_names(
    condition_result: Mapping[str, JsonValue],
) -> tuple[str, str]:
    success = cast("Sequence[JsonValue]", condition_result["successNode"])[0]
    failure = cast("Sequence[JsonValue]", condition_result["failedNode"])[0]
    return cast("str", success), cast("str", failure)


def _project_exact_legacy_conditions_tasks(
    tasks: list[DecodedLegacyTask],
    *,
    edges: Sequence[tuple[str, str]],
) -> list[DecodedLegacyTask]:
    """Keep only graph-consistent split CONDITIONS nodes in typed provenance."""
    task_names = {task.name for task in tasks}
    projected: list[DecodedLegacyTask] = []
    for task in tasks:
        if (
            task.type != "CONDITIONS"
            or task.task_params_reencode_source is not ProjectionSource.TYPED_AUTHORING
        ):
            projected.append(task)
            continue
        raw_params = task.document.get("task_params")
        if not isinstance(raw_params, Mapping):
            projected.append(_opaque_legacy_conditions_task(task))
            continue
        try:
            dependence, condition_result = _encode_legacy_conditions_params(
                raw_params,
                task_name=task.name,
            )
        except LegacyWorkflowGraphError:
            projected.append(_opaque_legacy_conditions_task(task))
            continue
        predicates = set(_legacy_condition_predicate_names(dependence))
        branches = set(_legacy_condition_branch_names(condition_result))
        actual_predecessors = set(task.depends_on)
        actual_successors = {
            successor for predecessor, successor in edges if predecessor == task.name
        }
        references = predicates | branches
        if (
            task.name in references
            or not references.issubset(task_names)
            or actual_predecessors != predicates
            or actual_successors != branches
        ):
            projected.append(_opaque_legacy_conditions_task(task))
            continue
        projected.append(task)
    return projected


def _opaque_legacy_conditions_task(task: DecodedLegacyTask) -> DecodedLegacyTask:
    params = _preserved_task_params(task.native_fields)
    if params is None:
        params = {}
    document_value = _thaw(task.document)
    if not isinstance(document_value, Mapping):
        message = f"Native CONDITIONS task '{task.name}' document is invalid"
        raise LegacyWorkflowGraphError(message)
    document = dict(document_value)
    document["task_params"] = params
    return replace(
        task,
        document=_freeze_mapping(document),
        task_params_reencode_source=ProjectionSource.OPAQUE_PRESERVE,
    )


def _task_levels(
    tasks: Sequence[WorkflowTaskSpec],
    edges: Sequence[tuple[str, str]],
) -> dict[str, int]:
    predecessors: dict[str, list[str]] = defaultdict(list)
    successors: dict[str, list[str]] = defaultdict(list)
    for predecessor, successor in edges:
        predecessors[successor].append(predecessor)
        successors[predecessor].append(successor)
    remaining = {task.name: len(predecessors[task.name]) for task in tasks}
    ready = deque(task.name for task in tasks if remaining[task.name] == 0)
    levels: dict[str, int] = {}
    while ready:
        task_name = ready.popleft()
        levels[task_name] = max(
            (levels[predecessor] + 1 for predecessor in predecessors[task_name]),
            default=0,
        )
        for successor in successors[task_name]:
            remaining[successor] -= 1
            if remaining[successor] == 0:
                ready.append(successor)
    if len(levels) != len(tasks):
        msg = "Workflow graph contains a dependency cycle"
        raise LegacyWorkflowGraphError(msg)
    return levels


def _native_task(
    task: WorkflowTaskSpec,
    *,
    task_id: str,
    predecessors: tuple[str, ...],
    preserved: DecodedLegacyTask | None,
    workflow_refs: LegacyWorkflowRefIndex,
    dependent_refs: LegacyDependentRefIndex,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    params, source = _task_params_for_encode(task, preserved=preserved)
    (
        params,
        native_task_type,
        conditions_fields,
        dependent_field,
    ) = _project_legacy_task_wire(
        task,
        params=params,
        source=source,
        preserved=preserved,
        workflow_refs=workflow_refs,
        dependent_refs=dependent_refs,
        resource_refs=resource_refs,
    )
    if preserved is None:
        native: JsonObject = {}
    else:
        thawed = _thaw(preserved.native_fields)
        if not isinstance(thawed, Mapping):
            msg = f"Native task '{task.name}' preservation payload is invalid"
            raise LegacyWorkflowGraphError(msg)
        native = dict(thawed)
    native.pop("code", None)
    native.pop("version", None)
    description_key = (
        "desc"
        if preserved is not None
        and "desc" in preserved.native_fields
        and "description" not in preserved.native_fields
        else "description"
    )
    native.pop("desc", None)
    native.pop("description", None)
    native.update(
        {
            "id": task_id,
            "name": task.name,
            "type": native_task_type,
            description_key: task.description or "",
            "runFlag": "NORMAL" if task.flag.value == "YES" else "FORBIDDEN",
        }
    )
    native.setdefault("dependence", {})
    native.update(
        {
            "maxRetryTimes": task.retry.times,
            "retryInterval": (
                1
                if task.retry.times == 0 and task.retry.interval == 0
                else task.retry.interval
            ),
            "params": params,
            "preTasks": list(predecessors),
            "taskInstancePriority": task.priority.value,
            "workerGroup": task.worker_group or "default",
            "timeout": _native_timeout(task),
        }
    )
    if conditions_fields is not None:
        native["dependence"], native["conditionResult"] = conditions_fields
    if dependent_field is not None:
        native["dependence"] = dependent_field
    return native


def _project_legacy_task_wire(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    source: ProjectionSource,
    preserved: DecodedLegacyTask | None,
    workflow_refs: LegacyWorkflowRefIndex,
    dependent_refs: LegacyDependentRefIndex,
    resource_refs: TaskResourceRefIndex | None,
) -> tuple[
    JsonValue,
    str,
    tuple[JsonObject, JsonObject] | None,
    JsonObject | None,
]:
    """Select one exact task-family projection before common TaskNode assembly."""
    if task.type in _SHARED_PROJECTED_LEGACY_TASK_TYPES:
        return (
            _encode_legacy_supported_task_params(
                task,
                params=params,
                source=source,
                resource_refs=resource_refs,
            ),
            task.type,
            None,
            None,
        )
    if task.type == "CONDITIONS":
        projected_params, fields = _project_legacy_conditions_wire(
            task,
            params=params,
            source=source,
            preserved=preserved,
        )
        return projected_params, task.type, fields, None
    if task.type == "SUB_WORKFLOW":
        return (
            _encode_legacy_sub_workflow_params(
                task,
                params=params,
                workflow_refs=workflow_refs,
            ),
            "SUB_PROCESS",
            None,
            None,
        )
    if task.type == "DEPENDENT":
        projected_params, dependence = _project_legacy_dependent_wire(
            task,
            params=params,
            source=source,
            preserved=preserved,
            dependent_refs=dependent_refs,
        )
        return projected_params, task.type, None, dependence
    return params, task.type, None, None


def _project_legacy_conditions_wire(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    source: ProjectionSource,
    preserved: DecodedLegacyTask | None,
) -> tuple[JsonValue, tuple[JsonObject, JsonObject] | None]:
    if source is ProjectionSource.OPAQUE_PRESERVE and preserved is not None:
        preserved_params = _preserved_task_params(preserved.native_fields)
        if preserved_params is None:
            message = f"Native CONDITIONS task '{task.name}' params are invalid"
            raise LegacyWorkflowGraphError(message)
        return preserved_params, None
    if not isinstance(params, Mapping):
        message = "CONDITIONS task params must be an object"
        raise LegacyWorkflowGraphError(message)
    return {}, _encode_legacy_conditions_params(dict(params), task_name=task.name)


def _project_legacy_dependent_wire(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    source: ProjectionSource,
    preserved: DecodedLegacyTask | None,
    dependent_refs: LegacyDependentRefIndex,
) -> tuple[JsonValue, JsonObject | None]:
    if source is ProjectionSource.OPAQUE_PRESERVE and preserved is not None:
        preserved_params = _preserved_task_params(preserved.native_fields)
        if preserved_params is None:
            message = f"Native DEPENDENT task '{task.name}' params are invalid"
            raise LegacyWorkflowGraphError(message)
        return preserved_params, None
    return {}, _encode_legacy_dependent_params(
        task,
        params=params,
        dependent_refs=dependent_refs,
    )


def _encode_legacy_supported_task_params(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    source: ProjectionSource,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Encode one task family handled by the shared exact projector."""
    if not isinstance(params, Mapping):
        message = f"{task.type} task params must be an object"
        raise LegacyWorkflowGraphError(message)
    return encode_task_parameters(
        version="1.3.9",
        task_type=task.type,
        task_params=dict(params),
        refs=_EMPTY_TASK_REFS,
        source=source,
        resource_refs=resource_refs,
    ).task_params


def _encode_legacy_sub_workflow_params(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    workflow_refs: LegacyWorkflowRefIndex,
) -> JsonObject:
    """Project one verified child name to the 1.3.9 database-ID wire."""
    if not isinstance(params, Mapping) or set(params) != {"childWorkflowName"}:
        message = (
            f"SUB_WORKFLOW task '{task.name}' requires only "
            "task_params.childWorkflowName on DolphinScheduler 1.3.9"
        )
        raise LegacyWorkflowGraphError(message)
    child_workflow = params.get("childWorkflowName")
    if not isinstance(child_workflow, str) or not child_workflow:
        message = f"SUB_WORKFLOW task '{task.name}' childWorkflowName is invalid"
        raise LegacyWorkflowGraphError(message)
    process_definition_id = workflow_refs.id_for_name(child_workflow)
    if process_definition_id is None:
        message = (
            f"SUB_WORKFLOW task '{task.name}' childWorkflowName "
            f"'{child_workflow}' has not been resolved in the parent project"
        )
        raise LegacyWorkflowGraphError(message)
    return {"processDefinitionId": process_definition_id}


def _encode_legacy_dependent_params(
    task: WorkflowTaskSpec,
    *,
    params: JsonValue,
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject:
    """Project canonical names into the split 1.3.9 DEPENDENT outer wire."""
    if not isinstance(params, Mapping):
        message = (
            f"DEPENDENT task '{task.name}' requires only task_params.dependence "
            "on DolphinScheduler 1.3.9"
        )
        raise LegacyWorkflowGraphError(message)
    try:
        normalized = normalize_typed_task_params(
            Dependent139TaskParamsSpec,
            cast("YamlObject", dict(params)),
        )
    except ValueError as error:
        message = f"DEPENDENT task '{task.name}' params are invalid: {error}"
        raise LegacyWorkflowGraphError(message) from error
    dependence = cast("Mapping[str, JsonValue]", normalized["dependence"])
    raw_groups = cast("Sequence[JsonValue]", dependence["dependTaskList"])
    return {
        "relation": dependence["relation"],
        "dependTaskList": [
            _encode_legacy_dependent_group(
                cast("Mapping[str, JsonValue]", raw_group),
                task=task,
                dependent_refs=dependent_refs,
            )
            for raw_group in raw_groups
        ],
    }


def _encode_legacy_dependent_group(
    group: Mapping[str, JsonValue],
    *,
    task: WorkflowTaskSpec,
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject:
    raw_items = cast("Sequence[JsonValue]", group["dependItemList"])
    return {
        "relation": group["relation"],
        "dependItemList": [
            _encode_legacy_dependent_item(
                cast("Mapping[str, JsonValue]", raw_item),
                task=task,
                dependent_refs=dependent_refs,
            )
            for raw_item in raw_items
        ],
    }


def _encode_legacy_dependent_item(
    item: Mapping[str, JsonValue],
    *,
    task: WorkflowTaskSpec,
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject:
    project_name = cast("str", item["projectName"])
    workflow_name = cast("str", item["workflowName"])
    task_name = cast("str | None", item.get("taskName"))
    native = dependent_refs.native_for_selector(
        project_name,
        workflow_name,
        task_name,
    )
    if native is None:
        message = (
            f"DEPENDENT task '{task.name}' target "
            f"'{project_name}/{workflow_name}' has not been resolved"
        )
        raise LegacyWorkflowGraphError(message)
    project_id, definition_id, dep_tasks = native
    return {
        "projectId": project_id,
        "definitionId": definition_id,
        "depTasks": dep_tasks,
        "cycle": item["cycle"],
        "dateValue": item["dateValue"],
    }


def _task_params_for_encode(
    task: WorkflowTaskSpec,
    *,
    preserved: DecodedLegacyTask | None,
) -> tuple[JsonValue, ProjectionSource]:
    if task.command is not None:
        if preserved is None or preserved.type != task.type:
            return (
                {
                    "resourceList": [],
                    "localParams": [],
                    "rawScript": task.command,
                },
                ProjectionSource.TYPED_AUTHORING,
            )
        if preserved.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE:
            preserved_params = _preserved_task_params(preserved.native_fields)
        else:
            preserved_params = _canonical_preserved_task_params(preserved)
        if preserved_params is None:
            preserved_params = {"resourceList": [], "localParams": []}
        return (
            {**preserved_params, "rawScript": task.command},
            preserved.task_params_reencode_source,
        )

    params = task.task_params
    if task.type == "MR" and isinstance(params, Mapping):
        program_type = params.get("programType")
        if (
            isinstance(program_type, str)
            and program_type == _REVIEWED_MR_OPAQUE_PROGRAM_TYPE
        ):
            # SCALA is the catalog's reviewed native MR escape mode. JAVA
            # remains the typed canonical facet and therefore still fails
            # closed when its package is not the exact three-field intent.
            return dict(params), ProjectionSource.OPAQUE_PRESERVE
    if (
        task.type == "SQL"
        and isinstance(params, Mapping)
        and is_sql_139_complete_native_package(params)
    ):
        # A complete native package is the exact-profile selector for explicit
        # opaque create/edit. Partial typed payloads still fail closed instead
        # of silently downgrading to preservation.
        return dict(params), ProjectionSource.OPAQUE_PRESERVE
    if (
        preserved is not None
        and preserved.type == task.type
        and params == _canonical_preserved_task_params(preserved)
    ):
        return params, preserved.task_params_reencode_source
    return params, ProjectionSource.TYPED_AUTHORING


def _canonical_preserved_task_params(
    preserved: DecodedLegacyTask,
) -> JsonObject | None:
    value = preserved.document.get("task_params")
    if not isinstance(value, Mapping):
        return None
    thawed = _thaw(value)
    if not isinstance(thawed, Mapping):
        return None
    return dict(thawed)


def _preserved_task_params(
    preserved: Mapping[str, JsonValue],
) -> JsonObject | None:
    value = preserved.get("params")
    if isinstance(value, str):
        value = _embedded_json(value, label="preserved task params")
    if not isinstance(value, Mapping):
        return None
    thawed = _thaw(value)
    if not isinstance(thawed, Mapping):
        return None
    return dict(thawed)


def _native_timeout(task: WorkflowTaskSpec) -> JsonObject:
    if task.timeout == 0:
        return {"strategy": "", "interval": None, "enable": False}
    strategy = (
        "WARN"
        if task.timeout_notify_strategy is None
        else task.timeout_notify_strategy.value
    )
    if strategy == "WARNFAILED":
        strategy = "WARN,FAILED"
    return {"strategy": strategy, "interval": task.timeout, "enable": True}


def _global_params(spec: WorkflowSpec) -> list[JsonObject]:
    global_params = spec.workflow.global_params
    if global_params is None:
        return []
    if isinstance(global_params, Mapping):
        return [
            {
                "prop": prop,
                "direct": Direct.IN.value,
                "type": "VARCHAR",
                "value": value,
            }
            for prop, value in global_params.items()
        ]
    return [_global_param(parameter) for parameter in global_params]


def _global_param(parameter: GlobalParamSpec) -> JsonObject:
    result: JsonObject = {
        "prop": parameter.prop,
        "direct": parameter.direct.value,
        "type": parameter.type.value,
    }
    if parameter.value is not None:
        result["value"] = parameter.value
    return result


def _locations(
    tasks: Sequence[WorkflowTaskSpec],
    *,
    id_by_name: Mapping[str, str],
    edges: Sequence[tuple[str, str]],
    levels: Mapping[str, int],
    baseline: DecodedLegacyWorkflowGraph | None,
) -> JsonObject:
    level_rows: dict[int, int] = defaultdict(int)
    result: JsonObject = {}
    for task in tasks:
        level = levels[task.name]
        row = level_rows[level]
        level_rows[level] += 1
        predecessor_ids = [
            id_by_name[predecessor]
            for predecessor, successor in edges
            if successor == task.name
        ]
        outgoing_count = sum(
            predecessor == task.name for predecessor, _successor in edges
        )
        task_id = id_by_name[task.name]
        preserved = None if baseline is None else baseline.native_locations.get(task_id)
        if isinstance(preserved, Mapping):
            thawed_location = _thaw(preserved)
            if not isinstance(thawed_location, Mapping):
                message = f"Native location '{task_id}' preservation is invalid"
                raise LegacyWorkflowGraphError(message)
            native_location: JsonObject = dict(thawed_location)
        else:
            native_location = {"x": level * 300, "y": row * 120}
        native_location.update(
            {
                "name": task.name,
                "targetarr": ",".join(predecessor_ids),
                "nodenumber": outgoing_count,
            }
        )
        if preserved is None:
            native_location = {
                "name": native_location["name"],
                "targetarr": native_location["targetarr"],
                "nodenumber": native_location["nodenumber"],
                "x": native_location["x"],
                "y": native_location["y"],
            }
        result[task_id] = native_location
    return result


def _preserved_process_data(
    baseline: DecodedLegacyWorkflowGraph | None,
) -> JsonObject:
    if baseline is None:
        return {}
    thawed = _thaw(baseline.native_process_fields)
    if not isinstance(thawed, Mapping):
        msg = "Native process preservation payload is invalid"
        raise LegacyWorkflowGraphError(msg)
    return dict(thawed)


def _json_text(value: JsonValue) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_object(text: str, *, label: str) -> JsonObject:
    value = _json_value(text, label=label)
    if not isinstance(value, Mapping):
        msg = f"{label} must be a JSON object"
        raise LegacyWorkflowGraphError(msg)
    return dict(value)


def _json_array(text: str, *, label: str) -> tuple[JsonValue, ...]:
    value = _json_value(text, label=label)
    if not _is_json_sequence(value):
        msg = f"{label} must be a JSON array"
        raise LegacyWorkflowGraphError(msg)
    return tuple(value)


def _json_value(text: str, *, label: str) -> JsonValue:
    try:
        value: JsonValue = json.loads(text)
    except (json.JSONDecodeError, TypeError) as exc:
        msg = f"{label} must contain valid JSON"
        raise LegacyWorkflowGraphError(msg) from exc
    if not is_json_value(value):
        msg = f"{label} must contain JSON-compatible data"
        raise LegacyWorkflowGraphError(msg)
    return value


def _json_mapping(value: JsonValue, *, label: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping):
        msg = f"{label} must be a JSON object"
        raise LegacyWorkflowGraphError(msg)
    return value


def _required_text(
    value: Mapping[str, JsonValue],
    field: str,
    *,
    label: str,
) -> str:
    candidate = value.get(field)
    if not isinstance(candidate, str) or not candidate.strip():
        msg = f"{label}.{field} must be non-empty text"
        raise LegacyWorkflowGraphError(msg)
    return candidate


def _pre_task_names(value: JsonValue, *, task_name: str) -> tuple[str, ...]:
    parsed = _embedded_json(value, label=f"task '{task_name}'.preTasks")
    if not _is_json_sequence(parsed):
        msg = f"Native task '{task_name}' preTasks must be an array"
        raise LegacyWorkflowGraphError(msg)
    predecessors: list[str] = []
    for predecessor in parsed:
        if not isinstance(predecessor, str) or not predecessor.strip():
            msg = f"Native task '{task_name}' preTasks must contain task names"
            raise LegacyWorkflowGraphError(msg)
        predecessors.append(predecessor)
    return tuple(predecessors)


def _embedded_json(value: JsonValue, *, label: str) -> JsonValue:
    if not isinstance(value, str):
        return value
    try:
        parsed: JsonValue = json.loads(value)
    except json.JSONDecodeError as exc:
        msg = f"{label} must contain valid JSON"
        raise LegacyWorkflowGraphError(msg) from exc
    if not is_json_value(parsed):
        msg = f"{label} must contain JSON-compatible data"
        raise LegacyWorkflowGraphError(msg)
    return parsed


def _canonical_task_document(
    task: Mapping[str, JsonValue],
    *,
    name: str,
    task_type: str,
    predecessors: tuple[str, ...],
    workflow_refs: LegacyWorkflowRefIndex,
    dependent_refs: LegacyDependentRefIndex,
    resource_refs: TaskResourceRefIndex | None,
) -> tuple[JsonObject, ProjectionSource, str]:
    canonical_task_type, canonical_params, reencode_source = (
        _canonical_legacy_task_params(
            task,
            name=name,
            task_type=task_type,
            workflow_refs=workflow_refs,
            dependent_refs=dependent_refs,
            resource_refs=resource_refs,
        )
    )
    run_flag = task.get("runFlag", "NORMAL")
    if run_flag == "NORMAL":
        canonical_flag = "YES"
    elif run_flag == "FORBIDDEN":
        canonical_flag = "NO"
    else:
        msg = f"Native task '{name}' has unknown runFlag '{run_flag}'"
        raise LegacyWorkflowGraphError(msg)
    retry_times = _non_negative_int(
        task.get("maxRetryTimes", 0),
        label=f"task '{name}'.maxRetryTimes",
    )
    retry_interval = _non_negative_int(
        task.get("retryInterval", 0),
        label=f"task '{name}'.retryInterval",
    )
    if retry_times == 0 and retry_interval == 1:
        retry_interval = 0
    priority = task.get("taskInstancePriority", "MEDIUM")
    if not isinstance(priority, str) or not priority:
        msg = f"Native task '{name}' taskInstancePriority must be text"
        raise LegacyWorkflowGraphError(msg)
    worker_group = task.get("workerGroup", "default")
    if not isinstance(worker_group, str) or not worker_group:
        msg = f"Native task '{name}' workerGroup must be text"
        raise LegacyWorkflowGraphError(msg)
    document: JsonObject = {
        "name": name,
        "type": canonical_task_type,
        "task_params": canonical_params,
        "flag": canonical_flag,
        "worker_group": worker_group,
        "priority": priority,
        "retry": {"times": retry_times, "interval": retry_interval},
        "depends_on": list(predecessors),
    }
    description = task.get("description", task.get("desc"))
    if description is not None:
        if not isinstance(description, str):
            msg = f"Native task '{name}' description must be text"
            raise LegacyWorkflowGraphError(msg)
        document["description"] = description
        # Keep canonical field order stable even though JSON objects are unordered.
        document = {
            "name": document["name"],
            "type": document["type"],
            "description": document["description"],
            "task_params": document["task_params"],
            "flag": document["flag"],
            "worker_group": document["worker_group"],
            "priority": document["priority"],
            "retry": document["retry"],
            "depends_on": document["depends_on"],
        }
    timeout = _canonical_timeout(task.get("timeout"), task_name=name)
    document.update(timeout)
    return document, reencode_source, canonical_task_type


def _canonical_legacy_task_params(
    task: Mapping[str, JsonValue],
    *,
    name: str,
    task_type: str,
    workflow_refs: LegacyWorkflowRefIndex,
    dependent_refs: LegacyDependentRefIndex,
    resource_refs: TaskResourceRefIndex | None,
) -> tuple[str, JsonObject, ProjectionSource]:
    params = _embedded_json(task.get("params"), label=f"task '{name}'.params")
    if not isinstance(params, Mapping):
        msg = f"Native task '{name}' params must be an object"
        raise LegacyWorkflowGraphError(msg)
    canonical_params = dict(params)
    canonical_task_type = task_type
    reencode_source = ProjectionSource.OPAQUE_PRESERVE
    if task_type in _SHARED_PROJECTED_LEGACY_TASK_TYPES:
        decoded_params = decode_task_parameters_with_provenance(
            version="1.3.9",
            task_type=task_type,
            task_params=canonical_params,
            refs=_EMPTY_TASK_REFS,
            source=ProjectionSource.OPAQUE_PRESERVE,
            resource_refs=resource_refs,
        )
        canonical_params = decoded_params.task.task_params
        reencode_source = decoded_params.reencode_source
    elif task_type == "CONDITIONS":
        projected_conditions = _decode_legacy_conditions_params(
            task,
            params=canonical_params,
            task_name=name,
        )
        if projected_conditions is not None:
            canonical_params = projected_conditions
            reencode_source = ProjectionSource.TYPED_AUTHORING
    elif task_type == "DEPENDENT":
        projected_dependent = _decode_legacy_dependent_params(
            task,
            params=canonical_params,
            dependent_refs=dependent_refs,
        )
        if projected_dependent is not None:
            canonical_params = projected_dependent
            reencode_source = ProjectionSource.TYPED_AUTHORING
    elif task_type == "SUB_PROCESS":
        process_definition_id = canonical_params.get("processDefinitionId")
        child_workflow_name = (
            workflow_refs.name_for_id(process_definition_id)
            if set(canonical_params) == {"processDefinitionId"}
            and isinstance(process_definition_id, int)
            and not isinstance(process_definition_id, bool)
            and process_definition_id > 0
            else None
        )
        if child_workflow_name is not None:
            canonical_task_type = "SUB_WORKFLOW"
            canonical_params = {"childWorkflowName": child_workflow_name}
            reencode_source = ProjectionSource.TYPED_AUTHORING
    return canonical_task_type, canonical_params, reencode_source


def _decode_legacy_dependent_params(
    task: Mapping[str, JsonValue],
    *,
    params: Mapping[str, JsonValue],
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject | None:
    """Reverse-project only the closed split 1.3.9 DEPENDENT package."""
    if params:
        return None
    raw_dependence = _embedded_json(
        task.get("dependence"),
        label="DEPENDENT task dependence",
    )
    if not isinstance(raw_dependence, Mapping) or set(raw_dependence) != {
        "relation",
        "dependTaskList",
    }:
        return None
    raw_groups = raw_dependence.get("dependTaskList")
    if not _is_json_sequence(raw_groups) or not raw_groups:
        return None
    canonical_groups: list[JsonValue] = []
    for raw_group in raw_groups:
        canonical_group = _decode_legacy_dependent_group(
            raw_group,
            dependent_refs=dependent_refs,
        )
        if canonical_group is None:
            return None
        canonical_groups.append(canonical_group)
    canonical: JsonObject = {
        "dependence": {
            "relation": raw_dependence.get("relation"),
            "dependTaskList": canonical_groups,
        }
    }
    try:
        normalized = normalize_typed_task_params(
            Dependent139TaskParamsSpec,
            cast("YamlObject", canonical),
        )
    except ValueError:
        return None
    return cast("JsonObject", normalized)


def _decode_legacy_dependent_group(
    raw_group: JsonValue,
    *,
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject | None:
    if not isinstance(raw_group, Mapping) or set(raw_group) != {
        "relation",
        "dependItemList",
    }:
        return None
    raw_items = raw_group.get("dependItemList")
    if not _is_json_sequence(raw_items) or not raw_items:
        return None
    canonical_items: list[JsonValue] = []
    for raw_item in raw_items:
        canonical_item = _decode_legacy_dependent_item(
            raw_item,
            dependent_refs=dependent_refs,
        )
        if canonical_item is None:
            return None
        canonical_items.append(canonical_item)
    return {
        "relation": raw_group.get("relation"),
        "dependItemList": canonical_items,
    }


def _decode_legacy_dependent_item(
    raw_item: JsonValue,
    *,
    dependent_refs: LegacyDependentRefIndex,
) -> JsonObject | None:
    target = _safe_legacy_dependent_item_target(raw_item)
    if target is None or not isinstance(raw_item, Mapping):
        return None
    project_id, definition_id, dep_tasks = target
    selector = dependent_refs.selector_for_native(
        project_id,
        definition_id,
        dep_tasks,
    )
    if selector is None:
        return None
    project_name, workflow_name, task_name = selector
    canonical_item: JsonObject = {
        "dependentType": (
            "DEPENDENT_ON_WORKFLOW" if task_name is None else "DEPENDENT_ON_TASK"
        ),
        "projectName": project_name,
        "workflowName": workflow_name,
        "cycle": raw_item.get("cycle"),
        "dateValue": raw_item.get("dateValue"),
    }
    if task_name is not None:
        canonical_item["taskName"] = task_name
    return canonical_item


def _canonical_timeout(value: JsonValue, *, task_name: str) -> JsonObject:
    if value is None:
        return {}
    parsed = _embedded_json(value, label=f"task '{task_name}'.timeout")
    if not isinstance(parsed, Mapping):
        msg = f"Native task '{task_name}' timeout must be an object"
        raise LegacyWorkflowGraphError(msg)
    enabled = parsed.get("enable", False)
    if not isinstance(enabled, bool):
        msg = f"Native task '{task_name}' timeout.enable must be boolean"
        raise LegacyWorkflowGraphError(msg)
    if not enabled:
        return {}
    interval = _non_negative_int(
        parsed.get("interval"),
        label=f"task '{task_name}'.timeout.interval",
    )
    strategy = parsed.get("strategy", "WARN")
    if strategy == "WARN,FAILED":
        strategy = "WARNFAILED"
    if strategy not in {"WARN", "FAILED", "WARNFAILED"}:
        msg = f"Native task '{task_name}' timeout.strategy is invalid"
        raise LegacyWorkflowGraphError(msg)
    return {"timeout": interval, "timeout_notify_strategy": strategy}


def _location_edges(
    locations: Mapping[str, JsonValue],
    *,
    task_by_id: Mapping[str, DecodedLegacyTask],
) -> list[tuple[str, str]]:
    location_ids = set(locations)
    task_ids = set(task_by_id)
    if location_ids != task_ids:
        missing = sorted(task_ids - location_ids)
        extra = sorted(location_ids - task_ids)
        msg = (
            f"locations task ids conflict with tasks (missing={missing}, extra={extra})"
        )
        raise LegacyWorkflowGraphError(msg)
    edges: list[tuple[str, str]] = []
    for task_id, task in task_by_id.items():
        location = _json_mapping(
            locations[task_id],
            label=f"locations.{task_id}",
        )
        location_name = location.get("name")
        if location_name is not None and location_name != task.name:
            msg = f"locations.{task_id}.name conflicts with native task '{task.name}'"
            raise LegacyWorkflowGraphError(msg)
        predecessor_ids = _target_ids(
            location.get("targetarr", ""),
            label=f"locations.{task_id}.targetarr",
        )
        for predecessor_id in predecessor_ids:
            predecessor = task_by_id.get(predecessor_id)
            if predecessor is None:
                msg = (
                    f"locations.{task_id}.targetarr references unknown id "
                    f"'{predecessor_id}'"
                )
                raise LegacyWorkflowGraphError(msg)
            edges.append((predecessor.name, task.name))
    _require_unique_edges(edges, label="locations.targetarr")
    outgoing_counts = {
        task.name: sum(predecessor == task.name for predecessor, _ in edges)
        for task in task_by_id.values()
    }
    for task_id, task in task_by_id.items():
        location = _json_mapping(
            locations[task_id],
            label=f"locations.{task_id}",
        )
        node_number = location.get("nodenumber")
        if (
            node_number is not None
            and _non_negative_int(
                node_number,
                label=f"locations.{task_id}.nodenumber",
            )
            != outgoing_counts[task.name]
        ):
            msg = f"locations.{task_id}.nodenumber conflicts with graph edges"
            raise LegacyWorkflowGraphError(msg)
    return edges


def _target_ids(value: JsonValue, *, label: str) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(candidate for candidate in value.split(",") if candidate)
    if _is_json_sequence(value) and all(isinstance(item, str) for item in value):
        return tuple(cast("str", item) for item in value)
    msg = f"{label} must be comma-separated task ids"
    raise LegacyWorkflowGraphError(msg)


def _connect_edges(
    connects: Sequence[JsonValue],
    *,
    task_by_id: Mapping[str, DecodedLegacyTask],
) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for index, value in enumerate(connects):
        connect = _json_mapping(value, label=f"connects[{index}]")
        source_id = _required_text(
            connect,
            "endPointSourceId",
            label=f"connects[{index}]",
        )
        target_id = _required_text(
            connect,
            "endPointTargetId",
            label=f"connects[{index}]",
        )
        source = task_by_id.get(source_id)
        target = task_by_id.get(target_id)
        if source is None or target is None:
            msg = f"connects[{index}] references an unknown native task id"
            raise LegacyWorkflowGraphError(msg)
        edges.append((source.name, target.name))
    _require_unique_edges(edges, label="connects")
    return edges


def _require_unique_edges(edges: Sequence[tuple[str, str]], *, label: str) -> None:
    if len(set(edges)) != len(edges):
        msg = f"{label} contains duplicate graph edges"
        raise LegacyWorkflowGraphError(msg)


def _require_same_graph(
    expected: Sequence[tuple[str, str]],
    actual: Sequence[tuple[str, str]],
    *,
    source: str,
) -> None:
    if set(expected) != set(actual):
        msg = f"{source} conflicts with task preTasks graph"
        raise LegacyWorkflowGraphError(msg)


def _non_negative_int(value: JsonValue, *, label: str) -> int:
    if isinstance(value, bool):
        msg = f"{label} must be a non-negative integer"
        raise LegacyWorkflowGraphError(msg)
    if isinstance(value, int):
        candidate = value
    elif isinstance(value, str) and value.isdecimal():
        candidate = int(value)
    else:
        msg = f"{label} must be a non-negative integer"
        raise LegacyWorkflowGraphError(msg)
    if candidate < 0:
        msg = f"{label} must be a non-negative integer"
        raise LegacyWorkflowGraphError(msg)
    return candidate


def _tenant_id(value: JsonValue) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < -1:
        msg = "tenantId must be -1 or a non-negative integer"
        raise LegacyWorkflowGraphError(msg)
    return value


def _is_json_sequence(value: JsonValue) -> TypeGuard[Sequence[JsonValue]]:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _freeze_mapping(value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    return MappingProxyType({key: _freeze(item) for key, item in value.items()})


def _freeze(value: JsonValue) -> JsonValue:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if _is_json_sequence(value):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: JsonValue | Mapping[str, JsonValue]) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if _is_json_sequence(value):
        return [_thaw(item) for item in value]
    return value
