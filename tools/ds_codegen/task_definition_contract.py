"""Reviewed exact-version task-definition recipes for DolphinScheduler.

The stable task surface is deliberately derived from two upstream truths:

* workflow DAG detail owns task membership and dependency relations;
* task-definition detail owns the current task payload used for a safe update
  once code/version task definitions exist.

DolphinScheduler 1.3.9 predates the code/version task-definition model.  Its
workflow JSON identifies ``TaskNode`` values with strings, so the stable task
surface is compiled through a sibling graph-backed recipe without inventing
integer ``code``/``version`` identity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ds_codegen.profile_ledger import select_exact_versions

TARGET_TASK_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

TASK_SEMANTIC_OPERATIONS = (
    "task.list",
    "task.get",
    "task.update",
)
TASK_DEFINITION_PROFILE_SCHEMA_VERSION = 5
GENERATED_TASK_DEFINITION_PROFILE_PATH = Path("generated/task_definition_profiles.py")

IdentityWire = Literal["legacy-string", "code-version"]
DagFamily = Literal["legacy-process-json", "process", "workflow"]
EvidenceKind = Literal["controller", "model", "service", "ui"]


@dataclass(frozen=True)
class Evidence:
    """One exact source coordinate used by a task-definition decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the conventional source reference consumed by profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class TaskTopLevelFieldPolicy:
    """Explicit semantic classification of one detail response model."""

    request_payload: frozenset[str]
    server_managed: frozenset[str]
    response_derived: frozenset[str]
    relation_projection: frozenset[str]
    opaque_preservation: frozenset[str]

    def __post_init__(self) -> None:
        """Reject ambiguous categories and invalid preservation claims."""
        categories = (
            self.request_payload,
            self.server_managed,
            self.response_derived,
            self.relation_projection,
        )
        classified: set[str] = set()
        for category in categories:
            overlap = classified.intersection(category)
            if overlap:
                message = (
                    "Task top-level field policy contains overlapping categories: "
                    f"{sorted(overlap)!r}"
                )
                raise ValueError(message)
            classified.update(category)
        invalid = self.opaque_preservation.difference(self.request_payload)
        if invalid:
            message = (
                "Opaque-preservation fields must belong to request payload: "
                f"{sorted(invalid)!r}"
            )
            raise ValueError(message)

    @property
    def classified_fields(self) -> frozenset[str]:
        """Return every generated response field assigned an explicit role."""
        return frozenset().union(
            self.request_payload,
            self.server_managed,
            self.response_derived,
            self.relation_projection,
        )


@dataclass(frozen=True)
class TaskDefinitionRecipe:
    """Wire, projection, and preservation decisions for one exact release."""

    executable: bool
    graph_backed: bool
    identity_wire: IdentityWire
    dag_family: DagFamily
    dag_operation: str | None
    dag_model: str
    workflow_field: str
    relation_field: str | None
    detail_operation: str | None
    detail_model: str
    update_operation: str | None
    update_executable: bool
    whole_workflow_update: bool
    dependency_update: bool
    requires_unique_workflow_binding: bool
    update_response_optional: bool
    request_fields: frozenset[str]
    computed_response_fields: frozenset[str]
    preserve_is_cache: bool

    def __post_init__(self) -> None:
        """Reject update guards that are not paired with a standalone mutation."""
        if not self.requires_unique_workflow_binding:
            if self.update_executable and self.whole_workflow_update:
                message = (
                    "A task recipe cannot select standalone and whole-workflow "
                    "mutation together"
                )
                raise ValueError(message)
            return
        if self.update_executable and not self.dependency_update:
            return
        message = (
            "Unique workflow binding requires an executable standalone task update"
        )
        raise ValueError(message)


@dataclass(frozen=True)
class TaskDefinitionVersionContract:
    """Reviewed task-definition contract for one exact DS version."""

    version: str
    task: TaskDefinitionRecipe


_TASK_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/TaskDefinitionController.java"
)
_PROCESS_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ProcessDefinitionController.java"
)
_WORKFLOW_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/WorkflowDefinitionController.java"
)
_TASK_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/service/impl/TaskDefinitionServiceImpl.java"
)
_PROCESS_SERVICE = (
    "dolphinscheduler-service/src/main/java/org/apache/dolphinscheduler/"
    "service/process/ProcessService.java"
)
_PROCESS_DEFINITION_SERVICE = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/service/impl/ProcessDefinitionServiceImpl.java"
)
_LEGACY_TASK_MODEL_SOURCE = (
    "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
    "common/model/TaskNode.java"
)
_LEGACY_UI = "dolphinscheduler-ui/src/js/conf/home/store/dag/actions.js"
_TASK_UI_20 = (
    "dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/taskDefinition/index.vue"
)
_TASK_UI = "dolphinscheduler-ui/src/service/modules/task-definition/index.ts"
_WORKFLOW_TASK_UI = (
    "dolphinscheduler-ui/src/views/projects/workflow/components/dag/use-task-edit.ts"
)
_PROCESS_WORKFLOW_REFS_OPERATION = (
    "ProcessDefinitionController.queryProcessDefinitionSimpleList"
)

_RESULT_MODEL = "org.apache.dolphinscheduler.api.utils.Result"
_DAG_MODEL = "org.apache.dolphinscheduler.dao.entity.DagData"
_LEGACY_TASK_MODEL = "org.apache.dolphinscheduler.common.model.TaskNode"
_ENTITY_TASK_MODEL = "org.apache.dolphinscheduler.dao.entity.TaskDefinition"
_TASK_VO_OLD = "org.apache.dolphinscheduler.api.vo.TaskDefinitionVo"
_TASK_VO = "org.apache.dolphinscheduler.api.vo.TaskDefinitionVO"

_BASE_REQUEST_FIELDS = frozenset(
    {
        "name",
        "description",
        "taskType",
        "taskParams",
        "flag",
        "taskPriority",
        "workerGroup",
        "environmentCode",
        "failRetryTimes",
        "failRetryInterval",
        "timeoutFlag",
        "timeoutNotifyStrategy",
        "timeout",
        "delayTime",
        "resourceIds",
    }
)
_TASK_GROUP_FIELDS = frozenset({"taskGroupId", "taskGroupPriority"})
_RESOURCE_LIMIT_FIELDS = frozenset({"cpuQuota", "memoryMax", "taskExecuteType"})
_SERVER_MANAGED_FIELDS = frozenset(
    {
        "id",
        "code",
        "version",
        "projectCode",
        "userId",
        "createTime",
        "updateTime",
    }
)
_RESPONSE_DERIVED_FIELDS = frozenset(
    {
        "taskParamList",
        "taskParamMap",
        "userName",
        "projectName",
        "modifyBy",
    }
)
# Exact tags 2.0.0 through 3.2.2 expose the unignored Jackson property at
# dolphinscheduler-dao/.../TaskDefinition.java#getDependence. It computes only
# ``dependence`` from taskParams; the getter is removed at the reviewed 3.3.1
# boundary and the property is never accepted by task mutations.
_DEPENDENCE_COMPUTED_RESPONSE_FIELDS = frozenset({"dependence"})


def _recipe(
    *,
    dag_family: Literal["process", "workflow"],
    detail_model: str,
    update_with_upstream: bool,
    request_fields: frozenset[str],
    computed_response_fields: frozenset[str] = frozenset(),
    relation_field: str | None = None,
    preserve_is_cache: bool = False,
    update_response_optional: bool = False,
    update_executable: bool = True,
    whole_workflow_update: bool = False,
    whole_workflow_dependency_update: bool = False,
    requires_unique_workflow_binding: bool = False,
) -> TaskDefinitionRecipe:
    dag_operation = (
        "ProcessDefinitionController.queryProcessDefinitionByCode"
        if dag_family == "process"
        else "WorkflowDefinitionController.queryWorkflowDefinitionByCode"
    )
    return TaskDefinitionRecipe(
        executable=True,
        graph_backed=False,
        identity_wire="code-version",
        dag_family=dag_family,
        dag_operation=dag_operation,
        dag_model=_DAG_MODEL,
        workflow_field=(
            "processDefinition" if dag_family == "process" else "workflowDefinition"
        ),
        relation_field=relation_field,
        detail_operation="TaskDefinitionController.queryTaskDefinitionDetail",
        detail_model=detail_model,
        update_operation=(
            "TaskDefinitionController.updateTaskWithUpstream"
            if update_with_upstream
            else "TaskDefinitionController.updateTaskDefinition"
        ),
        update_executable=update_executable,
        whole_workflow_update=whole_workflow_update,
        dependency_update=update_with_upstream or whole_workflow_dependency_update,
        requires_unique_workflow_binding=requires_unique_workflow_binding,
        update_response_optional=update_response_optional,
        request_fields=request_fields,
        computed_response_fields=computed_response_fields,
        preserve_is_cache=preserve_is_cache,
    )


_LEGACY_139 = TaskDefinitionRecipe(
    executable=False,
    graph_backed=True,
    identity_wire="legacy-string",
    dag_family="legacy-process-json",
    dag_operation=None,
    dag_model="org.apache.dolphinscheduler.dao.entity.ProcessDefinition",
    workflow_field="processDefinitionJson",
    relation_field=None,
    detail_operation=None,
    detail_model=_LEGACY_TASK_MODEL,
    update_operation=None,
    update_executable=False,
    whole_workflow_update=True,
    dependency_update=False,
    requires_unique_workflow_binding=False,
    update_response_optional=False,
    request_fields=frozenset(),
    computed_response_fields=frozenset(),
    preserve_is_cache=False,
)
_ENTITY_200 = _recipe(
    dag_family="process",
    detail_model=_ENTITY_TASK_MODEL,
    update_with_upstream=False,
    request_fields=_BASE_REQUEST_FIELDS,
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
    update_executable=False,
    whole_workflow_update=True,
    whole_workflow_dependency_update=True,
)
# DS 2.0.3 safely saves changed task definitions with the complete graph, but
# its unchanged-task relation comparison compares the submitted graph to itself.
_ENTITY_203 = replace(_ENTITY_200, dependency_update=False)
_ENTITY_209 = _recipe(
    dag_family="process",
    detail_model=_ENTITY_TASK_MODEL,
    update_with_upstream=False,
    request_fields=_BASE_REQUEST_FIELDS,
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
    requires_unique_workflow_binding=True,
)
_ENTITY_30 = _recipe(
    dag_family="process",
    detail_model=_ENTITY_TASK_MODEL,
    update_with_upstream=False,
    requires_unique_workflow_binding=True,
    request_fields=_BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS,
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
)
_ENTITY_31 = _recipe(
    dag_family="process",
    detail_model=_ENTITY_TASK_MODEL,
    update_with_upstream=False,
    requires_unique_workflow_binding=True,
    request_fields=(_BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS),
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
)
_OLD_VO_319 = _recipe(
    dag_family="process",
    detail_model=_TASK_VO_OLD,
    update_with_upstream=False,
    request_fields=(_BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS),
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
    relation_field="processTaskRelationList",
    requires_unique_workflow_binding=True,
)
_OLD_VO_320 = _recipe(
    dag_family="process",
    detail_model=_TASK_VO_OLD,
    update_with_upstream=False,
    request_fields=(
        _BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS | {"isCache"}
    ),
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
    relation_field="processTaskRelationList",
    preserve_is_cache=True,
    requires_unique_workflow_binding=True,
)
_VO_32 = _recipe(
    dag_family="process",
    detail_model=_TASK_VO,
    update_with_upstream=True,
    request_fields=(
        _BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS | {"isCache"}
    ),
    computed_response_fields=_DEPENDENCE_COMPUTED_RESPONSE_FIELDS,
    relation_field="processTaskRelationList",
    preserve_is_cache=True,
)
_VO_33 = _recipe(
    dag_family="workflow",
    detail_model=_TASK_VO,
    update_with_upstream=True,
    request_fields=(_BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS),
    relation_field="workflowTaskRelationList",
)
_VO_341 = _recipe(
    dag_family="workflow",
    detail_model=_TASK_VO,
    update_with_upstream=True,
    request_fields=(_BASE_REQUEST_FIELDS | _TASK_GROUP_FIELDS | _RESOURCE_LIMIT_FIELDS),
    relation_field="workflowTaskRelationList",
    update_response_optional=True,
)
_VO_WORKFLOW_UPDATE = replace(
    _VO_33,
    update_operation=None,
    update_executable=False,
    whole_workflow_update=True,
    dependency_update=True,
)


TASK_DEFINITION_CONTRACTS: dict[str, TaskDefinitionVersionContract] = {
    "1.3.9": TaskDefinitionVersionContract("1.3.9", _LEGACY_139),
    "2.0.0": TaskDefinitionVersionContract("2.0.0", _ENTITY_200),
    "2.0.1": TaskDefinitionVersionContract("2.0.1", _ENTITY_200),
    "2.0.2": TaskDefinitionVersionContract("2.0.2", _ENTITY_200),
    "2.0.3": TaskDefinitionVersionContract("2.0.3", _ENTITY_203),
    "2.0.4": TaskDefinitionVersionContract("2.0.4", _ENTITY_209),
    "2.0.5": TaskDefinitionVersionContract("2.0.5", _ENTITY_209),
    "2.0.6": TaskDefinitionVersionContract("2.0.6", _ENTITY_209),
    "2.0.7": TaskDefinitionVersionContract("2.0.7", _ENTITY_209),
    "2.0.8": TaskDefinitionVersionContract("2.0.8", _ENTITY_209),
    "2.0.9": TaskDefinitionVersionContract("2.0.9", _ENTITY_209),
    "3.0.0": TaskDefinitionVersionContract("3.0.0", _ENTITY_30),
    "3.0.1": TaskDefinitionVersionContract("3.0.1", _ENTITY_30),
    "3.0.2": TaskDefinitionVersionContract("3.0.2", _ENTITY_30),
    "3.0.3": TaskDefinitionVersionContract("3.0.3", _ENTITY_30),
    "3.0.4": TaskDefinitionVersionContract("3.0.4", _ENTITY_30),
    "3.0.5": TaskDefinitionVersionContract("3.0.5", _ENTITY_30),
    "3.0.6": TaskDefinitionVersionContract("3.0.6", _ENTITY_30),
    "3.1.0": TaskDefinitionVersionContract("3.1.0", _ENTITY_31),
    "3.1.1": TaskDefinitionVersionContract("3.1.1", _ENTITY_31),
    "3.1.2": TaskDefinitionVersionContract("3.1.2", _ENTITY_31),
    "3.1.3": TaskDefinitionVersionContract("3.1.3", _OLD_VO_319),
    "3.1.4": TaskDefinitionVersionContract("3.1.4", _OLD_VO_319),
    "3.1.5": TaskDefinitionVersionContract("3.1.5", _OLD_VO_319),
    "3.1.6": TaskDefinitionVersionContract("3.1.6", _OLD_VO_319),
    "3.1.7": TaskDefinitionVersionContract("3.1.7", _OLD_VO_319),
    "3.1.8": TaskDefinitionVersionContract("3.1.8", _OLD_VO_319),
    "3.1.9": TaskDefinitionVersionContract("3.1.9", _OLD_VO_319),
    "3.2.0": TaskDefinitionVersionContract("3.2.0", _OLD_VO_320),
    "3.2.1": TaskDefinitionVersionContract("3.2.1", _VO_32),
    "3.2.2": TaskDefinitionVersionContract("3.2.2", _VO_32),
    "3.3.1": TaskDefinitionVersionContract("3.3.1", _VO_33),
    "3.3.2": TaskDefinitionVersionContract("3.3.2", _VO_33),
    "3.4.0": TaskDefinitionVersionContract("3.4.0", _VO_33),
    "3.4.1": TaskDefinitionVersionContract("3.4.1", _VO_341),
    "3.4.2": TaskDefinitionVersionContract("3.4.2", _VO_33),
    "3.4.3": TaskDefinitionVersionContract("3.4.3", _VO_WORKFLOW_UPDATE),
}


def task_definition_contract(version: str) -> TaskDefinitionVersionContract:
    """Return one reviewed contract without neighbouring-version inference."""
    try:
        return TASK_DEFINITION_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed task-definition contract"
        raise ValueError(message) from exc


def task_top_level_field_policy(version: str) -> TaskTopLevelFieldPolicy:
    """Return the exact response-field classification for one executable recipe."""
    recipe = task_definition_contract(version).task
    if not recipe.executable:
        message = f"DS {version} has no executable task-definition field policy"
        raise ValueError(message)
    relation = (
        frozenset({recipe.relation_field})
        if recipe.relation_field is not None
        else frozenset()
    )
    opaque = {"taskParams"}
    if recipe.preserve_is_cache:
        opaque.add("isCache")
    return TaskTopLevelFieldPolicy(
        request_payload=recipe.request_fields,
        server_managed=_SERVER_MANAGED_FIELDS,
        response_derived=(_RESPONSE_DERIVED_FIELDS | recipe.computed_response_fields),
        relation_projection=relation,
        opaque_preservation=frozenset(opaque),
    )


def task_definition_profile_data(
    versions: tuple[str, ...] | None = None,
) -> dict[str, object]:
    """Project reviewed task contracts into the publishable runtime seam."""
    selected = select_exact_versions(
        TARGET_TASK_VERSIONS, versions, label="task_definition reviews"
    )
    profiles: dict[str, object] = {}
    for version in selected:
        recipe = task_definition_contract(version).task
        profile: dict[str, object] = {
            "executable": recipe.executable,
            "graph_backed": recipe.graph_backed,
            "identity_wire": recipe.identity_wire,
            "dag_family": recipe.dag_family,
            "dag_operation": recipe.dag_operation,
            "dag_model": recipe.dag_model,
            "workflow_field": recipe.workflow_field,
            "relation_field": recipe.relation_field,
            "detail_operation": recipe.detail_operation,
            "detail_model": recipe.detail_model,
            "update_operation": recipe.update_operation,
            "update_executable": recipe.update_executable,
            "whole_workflow_update": recipe.whole_workflow_update,
            "dependency_update": recipe.dependency_update,
            "requires_unique_workflow_binding": (
                recipe.requires_unique_workflow_binding
            ),
            "update_response_optional": recipe.update_response_optional,
            "request_fields": sorted(recipe.request_fields),
            "computed_response_fields": sorted(recipe.computed_response_fields),
            "preserve_is_cache": recipe.preserve_is_cache,
        }
        if recipe.executable:
            policy = task_top_level_field_policy(version)
            profile["field_policy"] = {
                "request_payload": sorted(policy.request_payload),
                "server_managed": sorted(policy.server_managed),
                "response_derived": sorted(policy.response_derived),
                "relation_projection": sorted(policy.relation_projection),
                "opaque_preservation": sorted(policy.opaque_preservation),
            }
        profiles[version] = profile
    return {
        "schema_version": TASK_DEFINITION_PROFILE_SCHEMA_VERSION,
        "target_versions": list(selected),
        "profiles": profiles,
    }


def render_task_definition_profiles(data: dict[str, object] | None = None) -> str:
    """Render the runtime projection without duplicating reviewed decisions."""
    projected = task_definition_profile_data() if data is None else data
    raw_versions = projected["target_versions"]
    raw_profiles = projected["profiles"]
    if not isinstance(raw_versions, list) or not isinstance(raw_profiles, dict):
        message = "task-definition profile projection has an invalid shape"
        raise TypeError(message)
    profile_pool: list[object] = []
    profile_indexes: dict[str, int] = {}
    profile_specs: dict[str, int] = {}
    for raw_version in raw_versions:
        if not isinstance(raw_version, str):
            message = "task-definition profile version must be a string"
            raise TypeError(message)
        profile = raw_profiles[raw_version]
        key = json.dumps(profile, sort_keys=True, separators=(",", ":"))
        profile_index = profile_indexes.get(key)
        if profile_index is None:
            profile_index = len(profile_pool)
            profile_indexes[key] = profile_index
            profile_pool.append(profile)
        profile_specs[raw_version] = profile_index
    compact = {
        "schema_version": projected["schema_version"],
        "target_versions": raw_versions,
        "profile_pool": profile_pool,
        "profile_specs": profile_specs,
    }
    payload = json.dumps(
        compact,
        ensure_ascii=True,
        indent=2,
        separators=(",", ": "),
    )
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "import json as _json",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "_TASK_DEFINITION_PROFILE_JSON = r'''",
            payload,
            "'''",
            "TASK_DEFINITION_PROFILE_DATA = _json.loads(",
            "    _TASK_DEFINITION_PROFILE_JSON",
            ")",
            "TASK_DEFINITION_PROFILE_SCHEMA_VERSION = (",
            "    TASK_DEFINITION_PROFILE_DATA['schema_version']",
            ")",
            "TARGET_TASK_DEFINITION_VERSIONS = tuple(",
            "    TASK_DEFINITION_PROFILE_DATA['target_versions']",
            ")",
            "_TASK_DEFINITION_PROFILE_POOL = (",
            "    TASK_DEFINITION_PROFILE_DATA['profile_pool']",
            ")",
            "TASK_DEFINITION_PROFILES = {",
            "    _version: dict(_TASK_DEFINITION_PROFILE_POOL[_profile_index])",
            "    for _version, _profile_index in (",
            "        TASK_DEFINITION_PROFILE_DATA['profile_specs'].items()",
            "    )",
            "}",
            "",
        )
    )


def write_task_definition_profiles(
    output_root: Path, *, versions: tuple[str, ...] | None = None
) -> Path:
    """Write the generated runtime projection beside exact wire bundles."""
    output_path = output_root / GENERATED_TASK_DEFINITION_PROFILE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_task_definition_profiles(task_definition_profile_data(versions)),
        encoding="utf-8",
    )
    return output_path


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return the exact source closure used by each stable task action."""
    recipe = task_definition_contract(version).task
    if recipe.graph_backed:
        detail = "ProcessDefinitionController.queryProcessDefinitionById"
        return {
            "task.list": (detail,),
            "task.get": (detail,),
            "task.update": (
                detail,
                "ProcessDefinitionController.updateProcessDefinition",
            ),
        }
    if not recipe.executable:
        return dict.fromkeys(TASK_SEMANTIC_OPERATIONS, ())
    dag_operation = recipe.dag_operation
    detail_operation = recipe.detail_operation
    update_operation = recipe.update_operation
    if (
        dag_operation is None
        or detail_operation is None
        or (update_operation is None and recipe.update_executable)
    ):
        message = f"DS {version} executable task recipe is incomplete"
        raise ValueError(message)
    operations = {
        "task.list": (dag_operation,),
        "task.get": (dag_operation, detail_operation),
        "task.update": (
            dag_operation,
            detail_operation,
            *((update_operation,) if update_operation is not None else ()),
            *(
                (_PROCESS_WORKFLOW_REFS_OPERATION,)
                if recipe.requires_unique_workflow_binding
                else ()
            ),
        ),
    }
    if not recipe.update_executable:
        operations["task.update"] = (
            (
                dag_operation,
                detail_operation,
                (
                    "WorkflowDefinitionController.updateWorkflowDefinition"
                    if recipe.dag_family == "workflow"
                    else "ProcessDefinitionController.updateProcessDefinition"
                ),
            )
            if recipe.whole_workflow_update
            else ()
        )
    return operations


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit response roots needed by the generated runtime slice."""
    recipe = task_definition_contract(version).task
    if recipe.graph_backed:
        legacy_roots = (
            _RESULT_MODEL,
            "org.apache.dolphinscheduler.dao.entity.ProcessDefinition",
            _LEGACY_TASK_MODEL,
        )
        return dict.fromkeys(TASK_SEMANTIC_OPERATIONS, legacy_roots)
    if not recipe.executable:
        return dict.fromkeys(TASK_SEMANTIC_OPERATIONS, ())
    roots_by_operation = {
        "task.list": (_RESULT_MODEL, recipe.dag_model),
        "task.get": (_RESULT_MODEL, recipe.dag_model, recipe.detail_model),
        "task.update": (_RESULT_MODEL, recipe.dag_model, recipe.detail_model),
    }
    if not recipe.update_executable and not recipe.whole_workflow_update:
        roots_by_operation["task.update"] = ()
    return roots_by_operation


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return exact controller/model/UI evidence for each stable action."""
    recipe = task_definition_contract(version).task
    if recipe.graph_backed:
        identity = Evidence(
            version,
            "model",
            _LEGACY_TASK_MODEL_SOURCE,
            "TaskNode.id/preTasks",
            (
                "TaskNode uses string id and task-name dependencies, so the "
                "stable task surface preserves native identity without code/version"
            ),
        )
        read = Evidence(
            version,
            "controller",
            _PROCESS_CONTROLLER,
            "queryProcessDefinitionById",
            "processDefinitionJson owns the complete embedded TaskNode graph",
        )
        legacy_ui_evidence = Evidence(
            version,
            "ui",
            _LEGACY_UI,
            "tasks/updateDefinition",
            "the official editor reads and writes tasks through the whole definition",
        )
        return {
            "task.list": (identity, read, legacy_ui_evidence),
            "task.get": (identity, read, legacy_ui_evidence),
            "task.update": (
                identity,
                read,
                Evidence(
                    version,
                    "controller",
                    _PROCESS_CONTROLLER,
                    "updateProcessDefinition",
                    (
                        "whole-definition update safely preserves graph-bound "
                        "task identity"
                    ),
                ),
                legacy_ui_evidence,
            ),
        }
    if not recipe.executable:
        message = f"DS {version} has no reviewed executable task recipe"
        raise ValueError(message)

    if version in {"2.0.0", "2.0.1"}:
        ui = _LEGACY_UI
    elif version in {
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        ui = _TASK_UI_20
    else:
        ui = _WORKFLOW_TASK_UI if recipe.dag_family == "workflow" else _TASK_UI
    dag_controller = (
        _PROCESS_CONTROLLER if recipe.dag_family == "process" else _WORKFLOW_CONTROLLER
    )
    dependency_conclusion = (
        "task payload update is executable, but dependency changes are rejected "
        "before transport because this endpoint cannot express upstream codes"
        if not recipe.dependency_update
        else "workflow DAG is the dependency truth and update carries upstream codes"
    )
    operation_evidence: dict[str, tuple[Evidence, ...]] = {
        "task.list": (
            Evidence(
                version,
                "controller",
                dag_controller,
                recipe.dag_operation or "",
                "workflow DAG owns task membership",
            ),
            Evidence(version, "ui", ui, "taskDefinitionList", "UI reads DAG tasks"),
        ),
        "task.get": (
            Evidence(
                version,
                "controller",
                _TASK_CONTROLLER,
                recipe.detail_operation or "",
                "task detail owns the current update payload",
            ),
            Evidence(version, "ui", ui, "taskDefinitionJsonObj", "UI task shape"),
        ),
        "task.update": (
            Evidence(
                version,
                "controller",
                _TASK_CONTROLLER,
                recipe.update_operation or "",
                dependency_conclusion,
            ),
            Evidence(version, "ui", ui, "taskDefinitionJsonObj", "UI task shape"),
        ),
    }
    if recipe.whole_workflow_update:
        operation_evidence["task.update"] = (
            Evidence(
                version,
                "controller",
                dag_controller,
                (
                    "updateWorkflowDefinition"
                    if recipe.dag_family == "workflow"
                    else "updateProcessDefinition"
                ),
                (
                    "the complete definition endpoint owns task and "
                    "relation mutation together"
                ),
            ),
            Evidence(
                version,
                "service",
                (
                    _PROCESS_DEFINITION_SERVICE.replace(
                        "ProcessDefinitionServiceImpl", "WorkflowDefinitionServiceImpl"
                    )
                    if recipe.dag_family == "workflow"
                    else _PROCESS_DEFINITION_SERVICE
                ),
                "updateDagDefine",
                (
                    "the whole-workflow transaction saves task and relation "
                    "versions coherently"
                ),
            ),
            Evidence(
                version,
                "ui",
                ui,
                "updateDefinition",
                "the official DAG editor submits the complete definition graph",
            ),
        )
    if version in {
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        operation_evidence["task.update"] = (
            Evidence(
                version,
                "controller",
                _TASK_CONTROLLER,
                recipe.update_operation or "",
                "standalone task update delegates its transaction to the task service",
            ),
            Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskDefinition/updateDag",
                (
                    "ordinary task updates advance the process definition and pass "
                    "the new task log into relation persistence"
                ),
            ),
            Evidence(
                version,
                "service",
                _PROCESS_SERVICE,
                "saveTaskRelation",
                (
                    "relation persistence rewrites the matching pre/post task "
                    "version while retaining the existing relation set"
                ),
            ),
            Evidence(version, "ui", ui, "updateTaskDefinition", "UI task editor"),
        )
    if recipe.requires_unique_workflow_binding:
        if version in {
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.8",
            "3.1.9",
        }:
            rejected_route = Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskWithUpstream",
                (
                    "the dependency route removes every valid upstream code from "
                    "its working set before that same now-empty set guards "
                    "updateDag, so process and relation-log advancement is "
                    "unreachable"
                ),
            )
        elif version in {
            "3.0.0",
            "3.0.1",
            "3.0.2",
            "3.0.3",
            "3.0.4",
            "3.0.5",
            "3.0.6",
            "3.1.0",
        }:
            rejected_route = Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskWithUpstream",
                (
                    "dependency edits mutate a copied relation list but updateDag "
                    "receives the original list, so additions and removals are "
                    "not reliably persisted"
                ),
            )
        elif version in {"2.0.4", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9"}:
            rejected_route = Evidence(
                version,
                "controller",
                _TASK_CONTROLLER,
                "updateTaskDefinition",
                (
                    "the standalone endpoint accepts task fields only and has "
                    "no dependency-edit input"
                ),
            )
        elif version == "3.2.0":
            rejected_route = Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskWithUpstream/updateUpstreamTask",
                (
                    "the dependency route updates current upstream relations but "
                    "does not call updateDag, so it does not advance the process "
                    "definition and relation-log versions"
                ),
            )
        else:  # pragma: no cover - generated contract invariant
            message = f"DS {version} has an unreviewed guarded task-update recipe"
            raise ValueError(message)
        operation_evidence["task.update"] = (
            Evidence(
                version,
                "controller",
                _TASK_CONTROLLER,
                recipe.update_operation or "",
                (
                    "ordinary task fields use the standalone update endpoint; "
                    "dependency edits are rejected before transport"
                ),
            ),
            Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskDefinition/updateDag",
                (
                    "standalone update selects one process code from the first "
                    "upstream relation before rewriting that process DAG"
                ),
            ),
            rejected_route,
            Evidence(
                version,
                "controller",
                _PROCESS_CONTROLLER,
                ("queryProcessDefinitionSimpleList/queryProcessDefinitionByCode"),
                (
                    "the generated precondition exhausts the project workflow "
                    "inventory and requires task membership and relation references "
                    "to bind only the selected workflow"
                ),
            ),
            Evidence(version, "ui", ui, "taskDefinitionJsonObj", "UI task shape"),
        )
    if not recipe.update_executable and not recipe.whole_workflow_update:
        conclusion = (
            "the standalone task endpoint increments the task version without "
            "rewriting workflow relation version references, while workflow "
            "reads resolve the exact relation-bound task version"
        )
        operation_evidence["task.update"] = (
            Evidence(
                version,
                "service",
                _TASK_SERVICE,
                "updateTaskDefinition",
                conclusion,
            ),
            Evidence(
                version,
                "service",
                _PROCESS_SERVICE,
                "getTaskNodeListByDefinition",
                conclusion,
            ),
            Evidence(
                version,
                "ui",
                _LEGACY_UI,
                "updateDefinition",
                "UI updates tasks through the full process-definition mutation",
            ),
        )
    return operation_evidence


def semantic_operation_facets(version: str) -> dict[str, tuple[str, ...]]:
    """Return compact reviewed facets for compatibility and audit output."""
    recipe = task_definition_contract(version).task
    facets: tuple[str, ...]
    if recipe.graph_backed:
        shared = (
            "identity:legacy-string",
            "dag:legacy-process-json",
            "execution:supported",
        )
        return {
            "task.list": (*shared, "selector:enumeration"),
            "task.get": (*shared, "selector:exact-name"),
            "task.update": (
                *shared,
                "selector:exact-name",
                "mutation:whole-workflow",
            ),
        }
    if not recipe.executable:
        facets = (
            "identity:legacy-string",
            "execution:terminal",
            "reason:lossy-code-version-projection",
        )
    else:
        shared_facets = (
            "identity:code-version",
            f"dag:{recipe.dag_family}",
            f"detail-model:{recipe.detail_model.rpartition('.')[2]}",
            f"dependency-update:{str(recipe.dependency_update).lower()}",
            (
                "workflow-binding:single"
                if recipe.requires_unique_workflow_binding
                else "workflow-binding:unrestricted"
            ),
            f"preserve-is-cache:{str(recipe.preserve_is_cache).lower()}",
        )
        facets_by_operation: dict[str, tuple[str, ...]] = dict.fromkeys(
            TASK_SEMANTIC_OPERATIONS,
            (*shared_facets, "execution:supported"),
        )
        if not recipe.update_executable and recipe.whole_workflow_update:
            facets_by_operation["task.update"] = (
                *shared_facets,
                "execution:supported",
                "mutation:whole-workflow",
                (
                    "standalone-task-update:absent"
                    if recipe.update_operation is None
                    else "standalone-task-update:unsafe"
                ),
            )
        elif not recipe.update_executable:
            facets_by_operation["task.update"] = (
                *shared_facets,
                "execution:terminal",
                "reason:workflow-relation-version-not-updated",
            )
        return facets_by_operation
    return dict.fromkeys(TASK_SEMANTIC_OPERATIONS, facets)


__all__ = [
    "GENERATED_TASK_DEFINITION_PROFILE_PATH",
    "TARGET_TASK_VERSIONS",
    "TASK_DEFINITION_CONTRACTS",
    "TASK_DEFINITION_PROFILE_SCHEMA_VERSION",
    "TASK_SEMANTIC_OPERATIONS",
    "Evidence",
    "TaskDefinitionRecipe",
    "TaskDefinitionVersionContract",
    "TaskTopLevelFieldPolicy",
    "render_task_definition_profiles",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "task_definition_contract",
    "task_definition_profile_data",
    "task_top_level_field_policy",
    "write_task_definition_profiles",
]
