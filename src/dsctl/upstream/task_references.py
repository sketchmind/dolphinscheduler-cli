"""Local task-reference locations shared by graph, rename and wire projection."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

ReferenceRole = Literal["predecessor", "successor", "runtime"]


@dataclass(frozen=True)
class TaskReferencePath:
    """One reviewed reference location; None denotes an array index."""

    parts: tuple[str | None, ...]
    role: ReferenceRole
    native_key: str | None = None

    def field(self, *indices: int, native: bool = False) -> str:
        """Render the exact diagnostic path without repeating its structure."""
        remaining = iter(indices)
        parts = tuple(next(remaining) if part is None else part for part in self.parts)
        if native and self.native_key is not None:
            parts = (*parts[:-1], self.native_key)
        return _field(parts)

    def key(self, *, native: bool = False) -> str:
        """Select the canonical or native predicate reference member."""
        return (
            self.native_key
            if native and self.native_key
            else next(part for part in reversed(self.parts) if part is not None)
        )


PREDICATE_TASK = TaskReferencePath(
    ("dependence", "dependTaskList", None, "dependItemList", None, "task"),
    "predecessor",
    native_key="depTaskCode",
)
SWITCH_BRANCH = TaskReferencePath(
    ("switchResult", "dependTaskList", None, "nextNode"), "successor"
)
SWITCH_DEFAULT = TaskReferencePath(("switchResult", "nextNode"), "successor")
CONDITION_RESULTS = (
    TaskReferencePath(("conditionResult", "successNode", None), "successor"),
    TaskReferencePath(("conditionResult", "failedNode", None), "successor"),
)
_SWITCH_RUNTIME = TaskReferencePath(("nextBranch",), "runtime")
_PATHS = {
    "BLOCKING": (PREDICATE_TASK,),
    "CONDITIONS": (PREDICATE_TASK, *CONDITION_RESULTS),
    "SWITCH": (SWITCH_BRANCH, SWITCH_DEFAULT, _SWITCH_RUNTIME),
}


@dataclass(frozen=True)
class TaskReference:
    """One located value; callers retain their own validation and edge policy."""

    path: tuple[str | int, ...]
    value: JsonValue
    role: ReferenceRole

    @property
    def field(self) -> str:
        """Return the diagnostic path used by graph and projection errors."""
        return _field(self.path)


def task_references(
    task_type: str,
    payload: Mapping[str, JsonValue],
    *,
    include_runtime: bool = False,
) -> Iterator[TaskReference]:
    """Locate existing canonical refs without granting typed authoring validity."""
    for definition in _PATHS.get(task_type, ()):
        if definition.role == "runtime" and not include_runtime:
            continue
        for path, value in _locations(payload, definition.parts):
            yield TaskReference(path, value, definition.role)


def rename_task_references(
    task_type: str,
    payload: JsonObject,
    names: Mapping[str, str],
) -> JsonObject:
    """Rewrite supported patch references in a detached parameter document."""
    rewritten = deepcopy(payload)
    if task_type not in {"SWITCH", "CONDITIONS"}:
        return rewritten
    if task_type == "SWITCH":
        # Existing patch behavior drops malformed branch rows while retaining
        # runtime nextBranch. Neither policy changes the typed wire contract.
        switch_result = rewritten.get("switchResult")
        if isinstance(switch_result, dict):
            branches = switch_result.get("dependTaskList")
            if isinstance(branches, list):
                switch_result["dependTaskList"] = [
                    branch for branch in branches if isinstance(branch, Mapping)
                ]
    for reference in task_references(task_type, rewritten, include_runtime=True):
        if isinstance(reference.value, str):
            _replace(
                rewritten, reference.path, names.get(reference.value, reference.value)
            )
    return rewritten


def _locations(
    value: JsonValue,
    remaining: tuple[str | None, ...],
    path: tuple[str | int, ...] = (),
) -> Iterator[tuple[tuple[str | int, ...], JsonValue]]:
    if not remaining:
        yield path, value
        return
    key, *tail = remaining
    if key is None and isinstance(value, list):
        for index, item in enumerate(value):
            yield from _locations(item, tuple(tail), (*path, index))
    elif isinstance(key, str) and isinstance(value, Mapping) and key in value:
        yield from _locations(value[key], tuple(tail), (*path, key))


def _replace(payload: JsonObject, path: tuple[str | int, ...], value: str) -> None:
    # _locations has already verified every container along this path.
    parent: JsonValue = payload
    for part in path[:-1]:
        parent = (
            cast("JsonObject", parent)[part]
            if isinstance(part, str)
            else cast("list[JsonValue]", parent)[part]
        )
    key = path[-1]
    if isinstance(key, str):
        cast("JsonObject", parent)[key] = value
    else:
        cast("list[JsonValue]", parent)[key] = value


def _field(path: tuple[str | int, ...]) -> str:
    return "task_params" + "".join(
        f"[{part}]" if isinstance(part, int) else f".{part}" for part in path
    )
