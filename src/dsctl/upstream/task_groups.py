from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiTransportError, NotFoundError, UnsupportedFeatureError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.response_projection import (
    optional_int_field,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    require_none,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        TaskGroupOperations,
        TaskGroupPageRecord,
        TaskGroupQueuePageRecord,
        TaskGroupRecord,
    )
    from dsctl.upstream.wire import CompiledWireProfile


_RESOURCE = "task-group"
_INTRODUCED_IN = "3.0.0"
_Primitive = Literal[
    "page",
    "project_page",
    "create",
    "update",
    "close",
    "start",
    "queue_page",
    "force_start",
    "priority",
]
_TASK_GROUP_PROGRAMS = CompiledDomainPrograms[_Primitive](
    name="task_group",
    schema_constant="COMPILED_TASK_GROUP_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "project_page": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "update": MUTATION_ONCE_REQUIRED,
        "close": MUTATION_ONCE_REQUIRED,
        "start": MUTATION_ONCE_REQUIRED,
        "queue_page": READ_RETRY_OPTIONAL,
        "force_start": MUTATION_ONCE_REQUIRED,
        "priority": MUTATION_ONCE_REQUIRED,
    },
)

_MutationResult = Literal["none", "entity"]
_QueueIdentity = Literal["process", "workflow"]


@dataclass(frozen=True)
class TaskGroupDomain:
    """Project resolution plus one exact task-group/queue lifecycle."""

    definitions: DefinitionReads
    task_groups: TaskGroupOperations


@dataclass(frozen=True)
class TaskGroupSnapshot:
    """Version-neutral task-group projection consumed by stable services."""

    id: int
    name: str | None
    projectCode: int  # noqa: N815
    description: str | None
    groupSize: int  # noqa: N815
    useSize: int  # noqa: N815
    userId: int  # noqa: N815
    status: str | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class TaskGroupQueueSnapshot:
    """Version-neutral task-group queue projection for stable output."""

    id: int
    taskId: int | None  # noqa: N815
    taskName: str | None  # noqa: N815
    projectName: str | None  # noqa: N815
    projectCode: str | None  # noqa: N815
    workflowInstanceName: str | None  # noqa: N815
    groupId: int  # noqa: N815
    workflowInstanceId: int | None  # noqa: N815
    priority: int
    forceStart: int  # noqa: N815
    inQueue: int | None  # noqa: N815
    status: str | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _Recipe:
    queue_filter_name: str
    queue_identity: _QueueIdentity
    queue_in_queue_projected: bool
    mutation_result: _MutationResult
    numeric_group_status: bool


_LEGACY_QUEUE_VOID = _Recipe(
    queue_filter_name="processInstanceName",
    queue_identity="process",
    queue_in_queue_projected=False,
    mutation_result="none",
    numeric_group_status=True,
)
_RENAMED_QUEUE_VOID = _Recipe(
    queue_filter_name="processInstanceName",
    queue_identity="process",
    queue_in_queue_projected=True,
    mutation_result="none",
    numeric_group_status=False,
)
_RENAMED_QUEUE_ENTITY = _Recipe(
    queue_filter_name="processInstanceName",
    queue_identity="process",
    queue_in_queue_projected=True,
    mutation_result="entity",
    numeric_group_status=False,
)
_WORKFLOW_QUEUE_ENTITY = _Recipe(
    queue_filter_name="workflowInstanceName",
    queue_identity="workflow",
    queue_in_queue_projected=True,
    mutation_result="entity",
    numeric_group_status=False,
)

_RECIPES = {
    "legacy_queue_void": _LEGACY_QUEUE_VOID,
    "renamed_queue_void": _RENAMED_QUEUE_VOID,
    "renamed_queue_entity": _RENAMED_QUEUE_ENTITY,
    "workflow_queue_entity": _WORKFLOW_QUEUE_ENTITY,
}


class TaskGroupAdapter:
    """Compiled task-group adapter for every supporting exact profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed task-group recipe."""
        self._profile = _TASK_GROUP_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> TaskGroupAdapter:
        """Return the exact adapter for one reviewed source version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskGroupDomain:
        """Bind project discovery and the exact task-group operations."""
        definitions = (
            CodeNativeReadAdapter.for_version(self.ds_version)
            .bind_read(
                profile,
                http_client=http_client,
            )
            .definitions
        )
        return TaskGroupDomain(
            definitions=definitions,
            task_groups=cast(
                "TaskGroupOperations",
                _Operations(
                    _TASK_GROUP_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                    self._recipe,
                ),
            ),
        )


@dataclass(frozen=True)
class _AbsentAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskGroupDomain:
        del profile, http_client
        message = f"Task groups do not exist in DolphinScheduler {self.ds_version}."
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _INTRODUCED_IN,
            },
            suggestion="Use DolphinScheduler 3.0.0 or newer for task groups.",
        )


def _adapter_for_version(ds_version: str) -> BoundDomainAdapter[TaskGroupDomain]:
    profile = _TASK_GROUP_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentAdapter(ds_version)
    if profile.status == "supported":
        return TaskGroupAdapter.for_version(ds_version)
    message = f"DS {ds_version} has no reviewed task-group capability decision"
    raise WireContractError(message)


TASK_GROUP_DOMAIN = BoundDomain[TaskGroupDomain](
    name=_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _Operations:
    programs: BoundCompiledPrograms[_Primitive]
    recipe: _Recipe

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
        status: int | None = None,
    ) -> TaskGroupPageRecord:
        return self._page(
            "page",
            {
                "name": search,
                "status": status,
                "pageNo": page_no,
                "pageSize": page_size,
            },
            page_no=page_no,
            page_size=page_size,
        )

    def list_by_project(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
    ) -> TaskGroupPageRecord:
        return self._page(
            "project_page",
            {"projectCode": project_code, "pageNo": page_no, "pageSize": page_size},
            page_no=page_no,
            page_size=page_size,
        )

    def list_all(self) -> Sequence[TaskGroupRecord]:
        return collect_pages(self.list, resource=_RESOURCE)

    def get(self, *, task_group_id: int) -> TaskGroupRecord:
        for task_group in self.list_all():
            if task_group.id == task_group_id:
                return task_group
        message = f"Task-group id {task_group_id} was not found"
        raise NotFoundError(
            message,
            details={"resource": _RESOURCE, "id": task_group_id},
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroupRecord:
        result = mutation_call(
            lambda: self.programs.call(
                "create",
                {
                    "name": name,
                    "projectCode": project_code,
                    "description": description,
                    "groupSize": group_size,
                },
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="create",
        )
        return self._verified_readback(
            operation="create",
            result=result,
            task_group_id=None,
            project_code=project_code,
            name=name,
            description=description,
            group_size=group_size,
        )

    def update(
        self,
        *,
        task_group_id: int,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroupRecord:
        result = mutation_call(
            lambda: self.programs.call(
                "update",
                {
                    "id": task_group_id,
                    "name": name,
                    "description": description,
                    "groupSize": group_size,
                },
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="update",
        )
        return self._verified_readback(
            operation="update",
            result=result,
            task_group_id=task_group_id,
            project_code=project_code,
            name=name,
            description=description,
            group_size=group_size,
        )

    def close(self, *, task_group_id: int) -> TaskGroupRecord:
        return self._set_open_state(task_group_id=task_group_id, open_state=False)

    def start(self, *, task_group_id: int) -> TaskGroupRecord:
        return self._set_open_state(task_group_id=task_group_id, open_state=True)

    def list_queues(
        self,
        *,
        group_id: int,
        page_no: int,
        page_size: int,
        task_instance_name: str | None = None,
        workflow_instance_name: str | None = None,
        status: int | None = None,
    ) -> TaskGroupQueuePageRecord:
        values: JsonObject = {
            "groupId": group_id,
            "taskInstanceName": task_instance_name,
            self.recipe.queue_filter_name: workflow_instance_name,
            "status": status,
            "pageNo": page_no,
            "pageSize": page_size,
        }
        page = self.programs.call("queue_page", values)
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="task-group-queue",
        )
        return cast(
            "TaskGroupQueuePageRecord",
            project_page(
                page,
                [
                    _queue_snapshot(
                        item,
                        ds_version=self.ds_version,
                        identity=self.recipe.queue_identity,
                        in_queue_projected=self.recipe.queue_in_queue_projected,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource="task-group-queue",
            ),
        )

    def force_start(self, *, queue_id: int) -> None:
        result = mutation_call(
            lambda: self.programs.call("force_start", {"queueId": queue_id}),
            ds_version=self.ds_version,
            resource="task-group-queue",
            operation="force-start",
        )
        require_none(
            result,
            ds_version=self.ds_version,
            resource="task-group-queue",
            field="forceStartResult",
        )

    def set_queue_priority(self, *, queue_id: int, priority: int) -> None:
        result = mutation_call(
            lambda: self.programs.call(
                "priority", {"queueId": queue_id, "priority": priority}
            ),
            ds_version=self.ds_version,
            resource="task-group-queue",
            operation="set-priority",
        )
        require_none(
            result,
            ds_version=self.ds_version,
            resource="task-group-queue",
            field="setPriorityResult",
        )

    def _page(
        self,
        primitive: Literal["page", "project_page"],
        fields: JsonObject,
        *,
        page_no: int,
        page_size: int,
    ) -> TaskGroupPageRecord:
        page = self.programs.call(primitive, fields)
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        return cast(
            "TaskGroupPageRecord",
            project_page(
                page,
                [
                    _task_group_snapshot(
                        item,
                        ds_version=self.ds_version,
                        numeric_status=self.recipe.numeric_group_status,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
        )

    def _set_open_state(
        self,
        *,
        task_group_id: int,
        open_state: bool,
    ) -> TaskGroupRecord:
        operation: Literal["start", "close"] = "start" if open_state else "close"
        result = mutation_call(
            lambda: self.programs.call(operation, {"id": task_group_id}),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation=operation,
        )

        def verify() -> TaskGroupRecord:
            require_none(
                result,
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field=f"{operation}Result",
            )
            refreshed = self.get(task_group_id=task_group_id)
            expected = "YES" if open_state else "NO"
            if refreshed.status != expected:
                message = "Task-group state readback did not match the mutation"
                raise ApiTransportError(
                    message,
                    details={
                        "task_group_id": task_group_id,
                        "expected_status": expected,
                        "actual_status": refreshed.status,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation=operation,
        )

    def _verified_readback(
        self,
        *,
        operation: str,
        result: OpaqueGeneratedValue,
        task_group_id: int | None,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> TaskGroupRecord:
        def verify() -> TaskGroupRecord:
            _require_mutation_result(
                result,
                expected=self.recipe.mutation_result,
                operation=operation,
                ds_version=self.ds_version,
            )
            candidates = collect_pages(
                lambda page_no, page_size: self.list_by_project(
                    project_code=project_code,
                    page_no=page_no,
                    page_size=page_size,
                ),
                resource=_RESOURCE,
            )
            matches = [
                item
                for item in candidates
                if item.name == name
                and item.projectCode == project_code
                and (task_group_id is None or item.id == task_group_id)
            ]
            if len(matches) != 1:
                message = "Task-group mutation readback did not return one exact row"
                raise ApiTransportError(
                    message,
                    details={
                        "task_group_id": task_group_id,
                        "project_code": project_code,
                        "name": name,
                        "match_count": len(matches),
                    },
                )
            refreshed = matches[0]
            if (
                refreshed.description != description
                or refreshed.groupSize != group_size
            ):
                message = "Task-group mutation readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={
                        "task_group_id": refreshed.id,
                        "expected_description": description,
                        "actual_description": refreshed.description,
                        "expected_group_size": group_size,
                        "actual_group_size": refreshed.groupSize,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation=operation,
        )


def _recipe_for_profile(profile: CompiledWireProfile) -> _Recipe:
    try:
        return _RECIPES[profile.recipe_id or ""]
    except KeyError as exc:
        message = f"Compiled task-group recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _task_group_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    numeric_status: bool,
) -> TaskGroupSnapshot:
    raw_status = response_field(
        item,
        "status",
        ds_version=ds_version,
        resource=_RESOURCE,
    )
    status = (
        _numeric_flag(raw_status, ds_version=ds_version)
        if numeric_status
        else _enum_text(raw_status, ds_version=ds_version, field="status")
    )
    return TaskGroupSnapshot(
        id=_positive_field(item, "id", ds_version=ds_version),
        name=optional_text_field(
            item,
            "name",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        projectCode=_positive_field(item, "projectCode", ds_version=ds_version),
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        groupSize=_positive_field(item, "groupSize", ds_version=ds_version),
        useSize=_non_negative_field(item, "useSize", ds_version=ds_version),
        userId=_positive_field(item, "userId", ds_version=ds_version),
        status=status,
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
    )


def _queue_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    identity: _QueueIdentity,
    in_queue_projected: bool,
) -> TaskGroupQueueSnapshot:
    if identity == "process":
        workflow_name_field = "processInstanceName"
        workflow_id_field = "processId"
    else:
        workflow_name_field = "workflowInstanceName"
        workflow_id_field = "workflowInstanceId"
    raw_workflow_id = optional_int_field(
        item,
        workflow_id_field,
        ds_version=ds_version,
        resource="task-group-queue",
    )
    return TaskGroupQueueSnapshot(
        id=_positive_field(
            item,
            "id",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        # The native paging SELECT omits task_id; Java's primitive defaults to
        # zero. Preserve a real ID when supplied, but never expose zero as one.
        taskId=_non_negative_field(
            item,
            "taskId",
            ds_version=ds_version,
            resource="task-group-queue",
        )
        or None,
        taskName=optional_text_field(
            item,
            "taskName",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        projectName=optional_text_field(
            item,
            "projectName",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        projectCode=optional_text_field(
            item,
            "projectCode",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        workflowInstanceName=optional_text_field(
            item,
            workflow_name_field,
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        groupId=_positive_field(
            item,
            "groupId",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        workflowInstanceId=(
            raw_workflow_id
            if raw_workflow_id is not None and raw_workflow_id > 0
            else None
        ),
        priority=_non_negative_field(
            item,
            "priority",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        forceStart=_non_negative_field(
            item,
            "forceStart",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        inQueue=(
            _non_negative_field(
                item,
                "inQueue",
                ds_version=ds_version,
                resource="task-group-queue",
            )
            if in_queue_projected
            else None
        ),
        status=_enum_text(
            response_field(
                item,
                "status",
                ds_version=ds_version,
                resource="task-group-queue",
            ),
            ds_version=ds_version,
            field="status",
            resource="task-group-queue",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="task-group-queue",
        ),
    )


def _positive_field(
    item: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
    resource: str = _RESOURCE,
) -> int:
    return positive_int(
        response_field(item, field, ds_version=ds_version, resource=resource),
        ds_version=ds_version,
        resource=resource,
        field=field,
    )


def _non_negative_field(
    item: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
    resource: str = _RESOURCE,
) -> int:
    value = response_field(item, field, ds_version=ds_version, resource=resource)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="field must be a non-negative integer",
    )


def _numeric_flag(value: OpaqueGeneratedValue, *, ds_version: str) -> str | None:
    if value is None:
        return None
    if value == 0:
        return "NO"
    if value == 1:
        return "YES"
    raise projection_error(
        ds_version=ds_version,
        resource=_RESOURCE,
        field="status",
        reason="numeric Flag value must be 0 or 1",
    )


def _enum_text(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    field: str,
    resource: str = _RESOURCE,
) -> str | None:
    if value is None:
        return None
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, str):
        return enum_value
    if isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="enum field must be text or a generated string enum",
    )


def _require_mutation_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _MutationResult,
    operation: str,
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "entity" and result is not None:
        return
    raise projection_error(
        ds_version=ds_version,
        resource=_RESOURCE,
        field=f"{operation}Result",
        reason=f"expected {expected} mutation result",
    )


__all__ = [
    "TASK_GROUP_DOMAIN",
    "TaskGroupAdapter",
    "TaskGroupDomain",
    "TaskGroupQueueSnapshot",
    "TaskGroupSnapshot",
]
