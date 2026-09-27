from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from typing import Literal, Protocol, cast

_PAGE_SIZE = 100
_OWNED_TASK_NAMES = frozenset({"extract", "load"})
_SAFE_RUN_ID = re.compile(r"[a-z0-9_-]{16,32}").fullmatch


class TaskDefinitionCleanupError(RuntimeError):
    """Reject an unprovable or unsafe release-gate cleanup state."""


class TaskMutationAmbiguousError(RuntimeError):
    """Signal that one cleanup mutation may or may not have reached DS."""


class TaskDeleteAmbiguousError(TaskMutationAmbiguousError):
    """Signal that one delete request may or may not have reached DS."""


class TaskMutationAmbiguityUnresolvedError(TaskDefinitionCleanupError):
    """Forbid automatic retry when one mutation cannot be proven."""


class TaskDeleteAmbiguityUnresolvedError(TaskMutationAmbiguityUnresolvedError):
    """Forbid automatic retry when one ambiguous delete cannot be proven."""


class TaskDeleteRetainedAfterAmbiguityError(TaskDeleteAmbiguityUnresolvedError):
    """Report a freshly observed residue after one ambiguous delete."""


class TaskReleaseAmbiguousError(TaskMutationAmbiguousError):
    """Signal that one offline request may or may not have reached DS."""


class TaskReleaseAmbiguityUnresolvedError(TaskMutationAmbiguityUnresolvedError):
    """Forbid retry when an ambiguous offline request cannot be proven."""


class TaskReleaseRetainedAfterAmbiguityError(TaskReleaseAmbiguityUnresolvedError):
    """Report a freshly observed online task after an ambiguous offline call."""


@dataclass(frozen=True)
class CleanupTaskRef:
    """Minimal identity emitted by the project-wide task inventory."""

    code: int
    name: str
    version: int
    workflow_code: int | None = None
    workflow_version: int | None = None
    workflow_name: str | None = None
    workflow_release_state: str | None = None


@dataclass(frozen=True)
class CleanupTaskDetail:
    """Typed ownership fields projected from one exact generated detail."""

    code: int
    name: str
    version: int
    project_code: int
    task_type: str
    description: str
    raw_script: str
    task_params_fingerprint: str
    flag: Literal["YES", "NO"] = "YES"


@dataclass(frozen=True)
class CleanupTaskPage:
    """One exact page response with all pagination metadata retained."""

    execute_type: str | None
    requested_page: int
    current_page: int
    offset: int
    page_size: int
    total: int
    total_pages: int
    rows: tuple[CleanupTaskRef, ...]


@dataclass(frozen=True)
class CleanupTaskHistoryPage:
    """One exhaustive exact task-definition history page."""

    requested_page: int
    current_page: int
    offset: int
    page_size: int
    total: int
    total_pages: int
    rows: tuple[CleanupTaskDetail, ...]


@dataclass(frozen=True)
class TaskDefinitionCleanupInventory:
    """A fully detailed, exactly owned project task inventory."""

    tasks: tuple[CleanupTaskDetail, ...]


@dataclass(frozen=True)
class TaskDefinitionCleanupReport:
    """Mutation accounting for one fully reconciled cleanup attempt."""

    observed: int
    released: int
    deleted: int
    remaining: int
    remote_mutations: int


class TaskDefinitionCleanupPort(Protocol):
    """External generated-wire boundary consumed by the cleanup module."""

    ds_version: str
    cleanup_strategy: Literal["direct-delete", "workflow-cascade-proof-only"]
    pre_delete_release: Literal["none", "offline"]
    inventory_execute_types: tuple[str, ...]

    def list_page(
        self,
        *,
        project_code: int,
        execute_type: str | None,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskPage:
        """Return one project-wide task inventory page."""
        ...

    def get_detail(
        self,
        *,
        project_code: int,
        code: int,
    ) -> CleanupTaskDetail:
        """Return the exact generated detail for one task code."""
        ...

    def list_history_page(
        self,
        *,
        project_code: int,
        code: int,
        page_no: int,
        page_size: int,
    ) -> CleanupTaskHistoryPage:
        """Return one exhaustive task-definition history page."""
        ...

    def delete(self, *, project_code: int, code: int) -> None:
        """Delete one exact task code without retrying ambiguity."""
        ...

    def release_offline(self, *, project_code: int, code: int) -> None:
        """Release one exact task offline without retrying ambiguity."""
        ...

    def prove_no_workflows(self, *, project_code: int) -> None:
        """Freshly prove the project-wide workflow inventory is empty."""
        ...


def prove_owned_task_inventory(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
    workflow_code: int | None = None,
    run_id: str,
) -> TaskDefinitionCleanupInventory:
    """Prove the project contains exactly the two gate-owned task rows."""
    _validate_inputs(
        project_code=project_code,
        workflow_code=workflow_code,
        run_id=run_id,
    )
    if _port_strategy(port) == "workflow-cascade-proof-only":
        return _prove_workflow_cascade_inventory(
            port,
            project_code=project_code,
            workflow_code=workflow_code,
            run_id=run_id,
        )
    return _owned_inventory(
        port,
        project_code=project_code,
        run_id=run_id,
        require_complete=True,
    )


def cleanup_owned_task_residue(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
    workflow_code: int | None = None,
    run_id: str,
) -> TaskDefinitionCleanupReport:
    """Delete each proven residue once and reconcile freshly after each call."""
    _validate_inputs(
        project_code=project_code,
        workflow_code=workflow_code,
        run_id=run_id,
    )
    if _port_strategy(port) == "workflow-cascade-proof-only":
        return _prove_workflow_cascade_cleanup(
            port,
            project_code=project_code,
        )
    inventory = _owned_inventory(
        port,
        project_code=project_code,
        run_id=run_id,
        require_complete=False,
    )
    observed = len(inventory.tasks)
    remaining = {task.code: task for task in inventory.tasks}
    released = 0
    deleted = 0
    for task in inventory.tasks:
        released_now, prepared = _prepare_owned_task_for_delete(
            port,
            project_code=project_code,
            run_id=run_id,
            task=task,
            expected=remaining,
        )
        released += released_now
        expected = {
            code: detail for code, detail in prepared.items() if code != task.code
        }
        ambiguous = False
        try:
            port.delete(project_code=project_code, code=task.code)
        except TaskDeleteAmbiguousError:
            ambiguous = True
        try:
            reconciled = _owned_inventory(
                port,
                project_code=project_code,
                run_id=run_id,
                require_complete=False,
            )
        except Exception as exc:
            message = (
                "task delete result could not be reconciled and must not be retried"
            )
            raise TaskDeleteAmbiguityUnresolvedError(message) from exc
        actual = {item.code: item for item in reconciled.tasks}
        if actual != expected:
            if ambiguous and actual == prepared:
                message = (
                    "ambiguous task delete retained its target and must not be retried"
                )
                raise TaskDeleteRetainedAfterAmbiguityError(message)
            if ambiguous:
                message = (
                    "ambiguous task delete reconciliation drifted and must not be "
                    "retried"
                )
                raise TaskDeleteAmbiguityUnresolvedError(message)
            message = (
                "task delete reconciliation drifted after mutation and must not be "
                "retried"
            )
            raise TaskDeleteAmbiguityUnresolvedError(message)
        deleted += 1
        remaining = actual
    return TaskDefinitionCleanupReport(
        observed=observed,
        released=released,
        deleted=deleted,
        remaining=len(remaining),
        remote_mutations=released + deleted,
    )


def _prepare_owned_task_for_delete(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
    run_id: str,
    task: CleanupTaskDetail,
    expected: dict[int, CleanupTaskDetail],
) -> tuple[int, dict[int, CleanupTaskDetail]]:
    if _port_pre_delete_release(port) == "none":
        return 0, expected
    fresh = _owned_inventory(
        port,
        project_code=project_code,
        run_id=run_id,
        require_complete=False,
    )
    fresh_by_code = {item.code: item for item in fresh.tasks}
    if fresh_by_code != expected or task.code not in fresh_by_code:
        message = "task cleanup ownership drifted before offline release"
        raise TaskDefinitionCleanupError(message)
    try:
        port.prove_no_workflows(project_code=project_code)
    except Exception as exc:
        message = "project workflow absence could not be freshly proven"
        raise TaskDefinitionCleanupError(message) from exc
    if fresh_by_code[task.code].flag == "NO":
        return 0, fresh_by_code

    ambiguous = False
    try:
        port.release_offline(project_code=project_code, code=task.code)
    except TaskReleaseAmbiguousError:
        ambiguous = True
    try:
        after_release = _owned_inventory(
            port,
            project_code=project_code,
            run_id=run_id,
            require_complete=False,
        )
    except Exception as exc:
        message = (
            "task offline release result could not be reconciled and must not be "
            "retried"
        )
        raise TaskReleaseAmbiguityUnresolvedError(message) from exc
    released_by_code = {item.code: item for item in after_release.tasks}
    expected_after_release = {
        **fresh_by_code,
        task.code: replace(fresh_by_code[task.code], flag="NO"),
    }
    if released_by_code != expected_after_release:
        if ambiguous:
            if released_by_code == fresh_by_code:
                message = (
                    "ambiguous task offline release retained its target and must not "
                    "be retried"
                )
                raise TaskReleaseRetainedAfterAmbiguityError(message)
            message = (
                "ambiguous task offline release reconciliation drifted and must not "
                "be retried"
            )
            raise TaskReleaseAmbiguityUnresolvedError(message)
        message = (
            "task offline release reconciliation drifted after mutation and must not "
            "be retried"
        )
        raise TaskReleaseAmbiguityUnresolvedError(message)
    return 1, released_by_code


def _prove_workflow_cascade_cleanup(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
) -> TaskDefinitionCleanupReport:
    refs = _read_inventory_refs(port, project_code=project_code)
    if refs:
        message = "workflow cascade did not remove every task"
        raise TaskDefinitionCleanupError(message)
    return TaskDefinitionCleanupReport(
        observed=0,
        released=0,
        deleted=0,
        remaining=0,
        remote_mutations=0,
    )


def _prove_workflow_cascade_inventory(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
    workflow_code: int | None,
    run_id: str,
) -> TaskDefinitionCleanupInventory:
    _validate_inputs(
        project_code=project_code,
        workflow_code=workflow_code,
        run_id=run_id,
    )
    refs = _read_inventory_refs(port, project_code=project_code)
    if workflow_code is None:
        _validate_ref_set(refs, require_complete=False)
        if refs:
            message = "project-only task proof requires an empty inventory"
            raise TaskDefinitionCleanupError(message)
        return TaskDefinitionCleanupInventory(tasks=())

    _validate_ref_set(refs, require_complete=True)
    workflow_version = _validate_workflow_bindings(
        refs,
        workflow_code=workflow_code,
        workflow_name=(f"dsctl-full-{port.ds_version.replace('.', '-')}-{run_id}"),
    )
    sorted_refs = tuple(sorted(refs, key=lambda item: (item.name, item.code)))
    try:
        details = tuple(
            port.get_detail(project_code=project_code, code=ref.code)
            for ref in sorted_refs
        )
    except Exception as exc:
        message = "task ownership details could not be freshly proven"
        raise TaskDefinitionCleanupError(message) from exc
    for ref, detail in zip(sorted_refs, details, strict=True):
        _validate_owned_detail(
            ref,
            detail,
            project_code=project_code,
            run_id=run_id,
        )
    try:
        history_pages = tuple(
            port.list_history_page(
                project_code=project_code,
                code=ref.code,
                page_no=1,
                page_size=_PAGE_SIZE,
            )
            for ref in sorted_refs
        )
    except Exception as exc:
        message = "task history could not be freshly proven"
        raise TaskDefinitionCleanupError(message) from exc
    for page in history_pages:
        _validate_history_page(page)
    _validate_narrow_task_lineage(
        sorted_refs,
        details,
        history_pages,
        workflow_version=workflow_version,
        project_code=project_code,
        run_id=run_id,
    )
    return TaskDefinitionCleanupInventory(tasks=details)


def _owned_inventory(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
    run_id: str,
    require_complete: bool,
) -> TaskDefinitionCleanupInventory:
    _validate_inputs(project_code=project_code, workflow_code=None, run_id=run_id)
    refs = _read_inventory_refs(port, project_code=project_code)
    _validate_ref_set(refs, require_complete=require_complete)
    try:
        details = tuple(
            port.get_detail(project_code=project_code, code=ref.code)
            for ref in sorted(refs, key=lambda item: (item.name, item.code))
        )
    except Exception as exc:
        message = "task ownership details could not be freshly proven"
        raise TaskDefinitionCleanupError(message) from exc
    for ref, detail in zip(
        sorted(refs, key=lambda item: (item.name, item.code)),
        details,
        strict=True,
    ):
        _validate_owned_detail(
            ref,
            detail,
            project_code=project_code,
            run_id=run_id,
        )
    return TaskDefinitionCleanupInventory(tasks=details)


def _read_inventory_refs(
    port: TaskDefinitionCleanupPort,
    *,
    project_code: int,
) -> tuple[CleanupTaskRef, ...]:
    scopes = _inventory_scopes(port.inventory_execute_types)
    try:
        pages = tuple(
            port.list_page(
                project_code=project_code,
                execute_type=execute_type,
                page_no=1,
                page_size=_PAGE_SIZE,
            )
            for execute_type in scopes
        )
    except Exception as exc:
        message = "task inventory could not be freshly proven"
        raise TaskDefinitionCleanupError(message) from exc
    for page, execute_type in zip(pages, scopes, strict=True):
        _validate_page(page, execute_type=execute_type)

    return tuple(row for page in pages for row in page.rows)


def _inventory_scopes(execute_types: tuple[str, ...]) -> tuple[str | None, ...]:
    if not execute_types:
        return (None,)
    if execute_types != ("BATCH", "STREAM"):
        message = "task cleanup execute-type inventory contract drifted"
        raise TaskDefinitionCleanupError(message)
    return execute_types


def _validate_page(page: CleanupTaskPage, *, execute_type: str | None) -> None:
    metadata_matches = (
        page.execute_type == execute_type
        and _is_exact_int(page.requested_page, minimum=1)
        and page.requested_page == 1
        and _is_exact_int(page.current_page, minimum=1)
        and page.current_page == 1
        and _is_exact_int(page.offset, minimum=0)
        and page.offset == 0
        and _is_exact_int(page.page_size, minimum=1)
        and page.page_size == _PAGE_SIZE
        and _is_exact_int(page.total, minimum=0)
        and page.total == len(page.rows)
        and _is_exact_int(page.total_pages, minimum=1)
        and page.total_pages == 1
        and type(page.rows) is tuple
    )
    if not metadata_matches:
        message = "task cleanup page metadata is not an exhaustive first page"
        raise TaskDefinitionCleanupError(message)


def _validate_history_page(page: CleanupTaskHistoryPage) -> None:
    metadata_matches = (
        _is_exact_int(page.requested_page, minimum=1)
        and page.requested_page == 1
        and _is_exact_int(page.current_page, minimum=1)
        and page.current_page == 1
        and _is_exact_int(page.offset, minimum=0)
        and page.offset == 0
        and _is_exact_int(page.page_size, minimum=1)
        and page.page_size == _PAGE_SIZE
        and _is_exact_int(page.total, minimum=0)
        and page.total == len(page.rows)
        and _is_exact_int(page.total_pages, minimum=1)
        and page.total_pages == 1
        and type(page.rows) is tuple
    )
    if not metadata_matches:
        message = "task history page metadata is not an exhaustive first page"
        raise TaskDefinitionCleanupError(message)


def _validate_workflow_bindings(
    refs: tuple[CleanupTaskRef, ...],
    *,
    workflow_code: int,
    workflow_name: str,
) -> int:
    versions = {ref.workflow_version for ref in refs}
    if len(versions) != 1 or any(
        not _is_exact_int(ref.workflow_code, minimum=1)
        or ref.workflow_code != workflow_code
        or type(ref.workflow_name) is not str
        or ref.workflow_name != workflow_name
        or type(ref.workflow_release_state) is not str
        or ref.workflow_release_state != "OFFLINE"
        or not _is_exact_int(ref.workflow_version, minimum=1)
        for ref in refs
    ):
        message = "task inventory does not prove one exact offline workflow binding"
        raise TaskDefinitionCleanupError(message)
    return cast("int", next(iter(versions)))


def _validate_narrow_task_lineage(
    refs: tuple[CleanupTaskRef, ...],
    details: tuple[CleanupTaskDetail, ...],
    history_pages: tuple[CleanupTaskHistoryPage, ...],
    *,
    workflow_version: int,
    project_code: int,
    run_id: str,
) -> None:
    refs_by_name = {ref.name: ref for ref in refs}
    details_by_name = {detail.name: detail for detail in details}
    histories_by_name = {
        ref.name: page.rows for ref, page in zip(refs, history_pages, strict=True)
    }
    original_scripts = {
        name: f'printf "%s\\n" "{run_id}-{name}"\n' for name in ("extract", "load")
    }
    updated_load = f'printf "%s\\n" "{run_id}-updated-load"\n'
    phase_a = (
        refs_by_name["extract"].version == 1
        and refs_by_name["load"].version == 1
        and details_by_name["extract"].raw_script == original_scripts["extract"]
        and details_by_name["load"].raw_script == original_scripts["load"]
        and workflow_version == 1
    )
    phase_b = (
        refs_by_name["extract"].version == 1
        and refs_by_name["load"].version == 2
        and details_by_name["extract"].raw_script == original_scripts["extract"]
        and details_by_name["load"].raw_script == updated_load
        and workflow_version in {1, 2, 3}
    )
    if not phase_a and not phase_b:
        message = "task inventory is outside the exact gate-owned lineage"
        raise TaskDefinitionCleanupError(message)
    expected_history = {
        "extract": {(1, original_scripts["extract"])},
        "load": (
            {(1, original_scripts["load"])}
            if phase_a
            else {(1, original_scripts["load"]), (2, updated_load)}
        ),
    }
    for name, rows in histories_by_name.items():
        ref = refs_by_name[name]
        actual: set[tuple[int, str]] = set()
        for row in rows:
            history_ref = CleanupTaskRef(
                code=ref.code,
                name=name,
                version=row.version,
            )
            _validate_owned_detail(
                history_ref,
                row,
                project_code=project_code,
                run_id=run_id,
            )
            actual.add((row.version, row.raw_script))
        if len(actual) != len(rows) or actual != expected_history[name]:
            message = "task history does not prove the exact gate-owned lineage"
            raise TaskDefinitionCleanupError(message)


def _port_strategy(
    port: TaskDefinitionCleanupPort,
) -> Literal["direct-delete", "workflow-cascade-proof-only"]:
    strategy = port.cleanup_strategy
    if strategy not in {"direct-delete", "workflow-cascade-proof-only"}:
        message = "task cleanup strategy drifted"
        raise TaskDefinitionCleanupError(message)
    return strategy


def _port_pre_delete_release(
    port: TaskDefinitionCleanupPort,
) -> Literal["none", "offline"]:
    capability = port.pre_delete_release
    if capability not in {"none", "offline"}:
        message = "task cleanup pre-delete release capability drifted"
        raise TaskDefinitionCleanupError(message)
    if capability == "offline" and _port_strategy(port) != "direct-delete":
        message = "task cleanup release capability requires direct delete"
        raise TaskDefinitionCleanupError(message)
    return capability


def _validate_ref_set(
    refs: tuple[CleanupTaskRef, ...],
    *,
    require_complete: bool,
) -> None:
    identities_valid = all(
        _is_exact_int(ref.code, minimum=1)
        and _is_exact_int(ref.version, minimum=1)
        and type(ref.name) is str
        and bool(ref.name)
        for ref in refs
    )
    codes = {ref.code for ref in refs}
    names = {ref.name for ref in refs}
    expected_names = _OWNED_TASK_NAMES if require_complete else names
    if (
        not identities_valid
        or len(codes) != len(refs)
        or len(names) != len(refs)
        or not names.issubset(_OWNED_TASK_NAMES)
        or names != expected_names
    ):
        message = "project inventory is not the exact owned task set"
        raise TaskDefinitionCleanupError(message)


def _validate_owned_detail(
    ref: CleanupTaskRef,
    detail: CleanupTaskDetail,
    *,
    project_code: int,
    run_id: str,
) -> None:
    expected_description = (
        f"dsctl-conformance-owner:{run_id};resource=task;name={ref.name}"
    )
    expected_scripts = {
        "extract": {f'printf "%s\\n" "{run_id}-extract"\n'},
        "load": {
            f'printf "%s\\n" "{run_id}-load"\n',
            f'printf "%s\\n" "{run_id}-updated-load"\n',
        },
    }
    if (
        not _is_exact_int(detail.code, minimum=1)
        or not _is_exact_int(detail.version, minimum=1)
        or not _is_exact_int(detail.project_code, minimum=1)
        or type(detail.name) is not str
        or type(detail.task_type) is not str
        or detail.flag not in {"YES", "NO"}
        or type(detail.description) is not str
        or type(detail.raw_script) is not str
        or type(detail.task_params_fingerprint) is not str
        or detail.task_params_fingerprint
        != canonical_task_params_fingerprint(detail.raw_script)
        or (detail.code, detail.name, detail.version)
        != (ref.code, ref.name, ref.version)
        or detail.project_code != project_code
        or detail.task_type != "SHELL"
        or detail.description != expected_description
        or detail.raw_script not in expected_scripts[ref.name]
    ):
        message = "task detail does not prove exact gate ownership"
        raise TaskDefinitionCleanupError(message)


def _validate_inputs(
    *,
    project_code: int,
    workflow_code: int | None,
    run_id: str,
) -> None:
    if not _is_exact_int(project_code, minimum=1):
        message = "task cleanup requires a positive project code"
        raise TaskDefinitionCleanupError(message)
    if workflow_code is not None and not _is_exact_int(workflow_code, minimum=1):
        message = "task cleanup workflow code must be a positive integer or null"
        raise TaskDefinitionCleanupError(message)
    if _SAFE_RUN_ID(run_id) is None:
        message = "task cleanup run_id must be 16-32 lowercase safe characters"
        raise TaskDefinitionCleanupError(message)


def canonical_task_params_fingerprint(raw_script: str) -> str:
    """Fingerprint the one exact gate-owned SHELL task-params shape."""
    if type(raw_script) is not str:
        message = "task cleanup raw script must be exact text"
        raise TaskDefinitionCleanupError(message)
    payload = {
        "localParams": [],
        "rawScript": raw_script,
        "resourceList": [],
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _is_exact_int(value: int | None, *, minimum: int) -> bool:
    return type(value) is int and value >= minimum


__all__ = [
    "CleanupTaskDetail",
    "CleanupTaskHistoryPage",
    "CleanupTaskPage",
    "CleanupTaskRef",
    "TaskDefinitionCleanupError",
    "TaskDefinitionCleanupInventory",
    "TaskDefinitionCleanupPort",
    "TaskDefinitionCleanupReport",
    "TaskDeleteAmbiguityUnresolvedError",
    "TaskDeleteAmbiguousError",
    "TaskDeleteRetainedAfterAmbiguityError",
    "TaskMutationAmbiguityUnresolvedError",
    "TaskMutationAmbiguousError",
    "TaskReleaseAmbiguityUnresolvedError",
    "TaskReleaseAmbiguousError",
    "TaskReleaseRetainedAfterAmbiguityError",
    "canonical_task_params_fingerprint",
    "cleanup_owned_task_residue",
    "prove_owned_task_inventory",
]


if __name__ == "__main__":
    from dsctl.release_gate.task_definition_cleanup_runner import main

    raise SystemExit(main())
