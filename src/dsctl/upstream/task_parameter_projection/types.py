from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol

from dsctl.errors import UserInputError
from dsctl.models.task_spec import (
    DYNAMIC_MAX_WORKFLOW_CODE,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from dsctl.support.json_types import JsonObject

ProjectionDirection = Literal["encode", "decode"]


class ProjectionSource(StrEnum):
    """Declare whether task params are authored semantics or opaque evidence."""

    TYPED_AUTHORING = "typed-authoring"
    OPAQUE_PRESERVE = "opaque-preserve"


class TaskParameterProjectionError(UserInputError):
    """Reject one task-parameter value that an exact wire cannot represent."""

    def __init__(
        self,
        message: str,
        *,
        version: str,
        direction: ProjectionDirection,
        task_type: str,
        field: str,
        reason: str,
    ) -> None:
        """Attach stable exact-profile and field context to the user error."""
        super().__init__(
            message,
            details={
                "version": version,
                "direction": direction,
                "task_type": task_type,
                "field": field,
                "reason": reason,
            },
            suggestion=(
                "Review the selected DolphinScheduler version's typed task schema, "
                "or preserve the task opaquely without changing task_params."
            ),
        )


@dataclass(frozen=True, slots=True)
class TaskRefIndex:
    """Immutable bijection between canonical local task names and native codes."""

    code_by_name: Mapping[str, int]
    name_by_code: Mapping[int, str]

    def __post_init__(self) -> None:
        """Copy, validate, and freeze both sides of the task-reference index."""
        codes = dict(self.code_by_name)
        names = dict(self.name_by_code)
        valid_names = all(
            isinstance(name, str) and bool(name.strip()) and name == name.strip()
            for name in codes
        )
        valid_codes = all(
            isinstance(code, int)
            and not isinstance(code, bool)
            and 0 < code <= DYNAMIC_MAX_WORKFLOW_CODE
            for code in codes.values()
        )
        inverse = {code: name for name, code in codes.items()}
        if (
            not valid_names
            or not valid_codes
            or len(inverse) != len(codes)
            or inverse != names
        ):
            message = "Task references must form one positive name/code bijection"
            raise ValueError(message)
        object.__setattr__(self, "code_by_name", MappingProxyType(codes))
        object.__setattr__(self, "name_by_code", MappingProxyType(names))

    @classmethod
    def from_code_by_name(cls, values: Mapping[str, int]) -> TaskRefIndex:
        """Build a validated index from the compiler's canonical name map."""
        codes = dict(values)
        names = {code: name for name, code in codes.items()}
        return cls(code_by_name=codes, name_by_code=names)


@dataclass(frozen=True, slots=True)
class TaskGraphContext:
    """Native relation evidence for one task being decoded from a workflow."""

    task_code: int
    relation_edges: frozenset[tuple[int, int]]

    def __post_init__(self) -> None:
        """Require positive native task identities and freeze copied edges."""
        valid_task_code = (
            isinstance(self.task_code, int)
            and not isinstance(self.task_code, bool)
            and self.task_code > 0
        )
        edges = frozenset(self.relation_edges)
        valid_edges = all(
            isinstance(predecessor, int)
            and not isinstance(predecessor, bool)
            and predecessor > 0
            and isinstance(successor, int)
            and not isinstance(successor, bool)
            and successor > 0
            for predecessor, successor in edges
        )
        if not valid_task_code or not valid_edges:
            message = "Task graph context requires positive native task codes"
            raise ValueError(message)
        object.__setattr__(self, "relation_edges", edges)

    def preserves_predecessors(self, predecessor_codes: frozenset[int]) -> bool:
        """Return whether existing acyclic edges already encode all predecessors."""
        if not predecessor_codes or self.task_code in predecessor_codes:
            return False
        required_edges = frozenset(
            (predecessor, self.task_code) for predecessor in predecessor_codes
        )
        if not required_edges.issubset(self.relation_edges):
            return False
        return not any(
            self._has_path(self.task_code, predecessor)
            for predecessor in predecessor_codes
        )

    def _has_path(self, start: int, target: int) -> bool:
        """Detect whether one existing native relation path reaches a task."""
        successors: dict[int, set[int]] = {}
        for predecessor, successor in self.relation_edges:
            successors.setdefault(predecessor, set()).add(successor)
        pending = list(successors.get(start, ()))
        visited = {start}
        while pending:
            current = pending.pop()
            if current == target:
                return True
            if current in visited:
                continue
            visited.add(current)
            pending.extend(successors.get(current, ()))
        return False


@dataclass(frozen=True, slots=True)
class TaskResourceRefIndex:
    """Immutable verified FILE names plus exact ids where the wire exposes them."""

    id_by_full_name: Mapping[str, int]
    full_name_by_id: Mapping[int, str]
    wire_full_name_by_full_name: Mapping[str, str]
    verified_full_names: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        """Copy, validate, and freeze both resource-identity directions."""
        ids = dict(self.id_by_full_name)
        names = dict(self.full_name_by_id)
        wire_names = dict(self.wire_full_name_by_full_name)
        verified_names = frozenset(self.verified_full_names)
        valid_names = all(
            isinstance(full_name, str)
            and bool(full_name.strip())
            and full_name == full_name.strip()
            for full_name in verified_names
        )
        valid_ids = all(
            isinstance(resource_id, int)
            and not isinstance(resource_id, bool)
            and resource_id > 0
            for resource_id in ids.values()
        )
        inverse = {resource_id: full_name for full_name, resource_id in ids.items()}
        valid_wire_names = all(
            isinstance(full_name, str)
            and bool(full_name.strip())
            and full_name == full_name.strip()
            for full_name in wire_names.values()
        )
        if (
            not valid_names
            or not valid_ids
            or not valid_wire_names
            or len(inverse) != len(ids)
            or inverse != names
            or not set(ids).issubset(verified_names)
            or set(wire_names) != set(verified_names)
        ):
            message = (
                "Task resources must carry one exact wire fullName per verified "
                "canonical fullName, and any ids must form one positive bijection"
            )
            raise ValueError(message)
        object.__setattr__(self, "id_by_full_name", MappingProxyType(ids))
        object.__setattr__(self, "full_name_by_id", MappingProxyType(names))
        object.__setattr__(
            self,
            "wire_full_name_by_full_name",
            MappingProxyType(wire_names),
        )
        object.__setattr__(self, "verified_full_names", verified_names)

    @classmethod
    def from_id_by_full_name(
        cls,
        values: Mapping[str, int],
    ) -> TaskResourceRefIndex:
        """Build a validated task-resource index from resolved exact identities."""
        ids = dict(values)
        names = {resource_id: full_name for full_name, resource_id in ids.items()}
        return cls(
            id_by_full_name=ids,
            full_name_by_id=names,
            wire_full_name_by_full_name={name: name for name in ids},
            verified_full_names=frozenset(ids),
        )

    @classmethod
    def from_resolved_files(
        cls,
        full_names: Iterable[str],
        *,
        id_by_full_name: Mapping[str, int],
        wire_full_name_by_full_name: Mapping[str, str],
    ) -> TaskResourceRefIndex:
        """Build refs from verified FILE names and optional exact positive ids."""
        verified_names = tuple(full_names)
        ids = dict(id_by_full_name)
        names = {resource_id: full_name for full_name, resource_id in ids.items()}
        return cls(
            id_by_full_name=ids,
            full_name_by_id=names,
            wire_full_name_by_full_name=wire_full_name_by_full_name,
            verified_full_names=frozenset(verified_names),
        )


@dataclass(frozen=True, slots=True)
class TaskWorkflowRefIndex:
    """Immutable exact child-workflow name/positive-code bijection."""

    code_by_name: Mapping[str, int]
    name_by_code: Mapping[int, str]

    def __post_init__(self) -> None:
        """Copy, validate, and freeze both workflow-identity directions."""
        codes = dict(self.code_by_name)
        names = dict(self.name_by_code)
        valid_names = all(
            isinstance(name, str) and bool(name.strip()) and name == name.strip()
            for name in codes
        )
        valid_codes = all(
            isinstance(code, int) and not isinstance(code, bool) and code > 0
            for code in codes.values()
        )
        inverse = {code: name for name, code in codes.items()}
        if (
            not valid_names
            or not valid_codes
            or len(inverse) != len(codes)
            or inverse != names
        ):
            message = "Task workflows must form one positive name/code bijection"
            raise ValueError(message)
        object.__setattr__(self, "code_by_name", MappingProxyType(codes))
        object.__setattr__(self, "name_by_code", MappingProxyType(names))

    @classmethod
    def from_code_by_name(
        cls,
        values: Mapping[str, int],
    ) -> TaskWorkflowRefIndex:
        """Build a validated child-workflow index from resolved identities."""
        codes = dict(values)
        names = {code: name for name, code in codes.items()}
        return cls(code_by_name=codes, name_by_code=names)


@dataclass(frozen=True, slots=True)
class ProjectedTask:
    """One exact task type and an isolated JSON-boundary parameter object."""

    task_type: str
    _task_params: JsonObject = field(repr=False)

    def __post_init__(self) -> None:
        """Detach the stored result from the projector's working object."""
        object.__setattr__(self, "_task_params", deepcopy(self._task_params))

    @property
    def task_params(self) -> JsonObject:
        """Return an isolated copy suitable for the next boundary."""
        return deepcopy(self._task_params)


@dataclass(frozen=True, slots=True)
class DecodedTaskParameters:
    """Decoded task plus its required semantics-preserving re-encode source."""

    task: ProjectedTask
    reencode_source: ProjectionSource


class _ParameterCodec(Protocol):
    def __call__(self, payload: JsonObject, *, version: str) -> JsonObject: ...


class _ResourceCodec(Protocol):
    def __call__(
        self,
        payload: JsonObject,
        *,
        version: str,
        resource_refs: TaskResourceRefIndex | None,
    ) -> JsonObject: ...


class _WorkflowCodec(Protocol):
    def __call__(
        self,
        payload: JsonObject,
        *,
        version: str,
        workflow_refs: TaskWorkflowRefIndex | None,
    ) -> JsonObject: ...


class _LocalReferenceCodec(Protocol):
    def __call__(
        self, payload: JsonObject, *, version: str, refs: TaskRefIndex
    ) -> JsonObject: ...


class _AvailabilityGuard(Protocol):
    def __call__(self, *, version: str, direction: ProjectionDirection) -> None: ...


class _NativeDecoder(Protocol):
    def __call__(
        self, payload: JsonObject, *, version: str
    ) -> DecodedTaskParameters | None: ...


class _ResourceNativeDecoder(Protocol):
    def __call__(
        self,
        payload: JsonObject,
        *,
        version: str,
        resource_refs: TaskResourceRefIndex | None,
    ) -> DecodedTaskParameters: ...


class _WorkflowNativeDecoder(Protocol):
    def __call__(
        self,
        payload: JsonObject,
        *,
        version: str,
        workflow_refs: TaskWorkflowRefIndex | None,
    ) -> DecodedTaskParameters: ...


class _GraphNativeDecoder(Protocol):
    def __call__(
        self,
        payload: JsonObject,
        *,
        version: str,
        refs: TaskRefIndex,
        graph_context: TaskGraphContext | None,
    ) -> DecodedTaskParameters: ...


@dataclass(frozen=True, slots=True)
class _CanonicalNative:
    """Try the registered typed decoder only after this native availability guard."""

    guard: _AvailabilityGuard


@dataclass(frozen=True, slots=True)
class _LeafProjection:
    encode: _ParameterCodec
    decode: _ParameterCodec
    preserve_guard: _AvailabilityGuard | None = None
    native_decode: _NativeDecoder | _CanonicalNative | None = None


@dataclass(frozen=True, slots=True)
class _ResourceProjection:
    encode: _ResourceCodec
    decode: _ResourceCodec
    preserve_guard: _AvailabilityGuard | None = None
    native_decode: _ResourceNativeDecoder | None = None


@dataclass(frozen=True, slots=True)
class _WorkflowProjection:
    encode: _WorkflowCodec
    decode: _WorkflowCodec
    preserve_guard: _AvailabilityGuard | None = None
    native_decode: _WorkflowNativeDecoder | None = None


@dataclass(frozen=True, slots=True)
class _LocalReferenceProjection:
    encode: _LocalReferenceCodec
    decode: _LocalReferenceCodec
    preserve_guard: _AvailabilityGuard | None = None
    native_decode: _GraphNativeDecoder | None = None


_TaskProjection = (
    _LeafProjection
    | _ResourceProjection
    | _WorkflowProjection
    | _LocalReferenceProjection
)
