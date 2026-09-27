from __future__ import annotations

from collections import Counter, deque
from typing import TYPE_CHECKING

from dsctl.errors import UserInputError
from dsctl.services._workflow.compile import (
    prepare_workflow_create_compilation,
    validate_workflow_create_constraints,
)
from dsctl.services.lint._diagnostics import _diagnostic
from dsctl.upstream.legacy_workflow_graph import (
    LegacyWorkflowGraphError,
    prepare_legacy_workflow_lint_graph,
    validate_legacy_workflow_constraints,
)
from dsctl.upstream.task_references import task_references
from dsctl.upstream.workflow_graph_requests import workflow_graph_counts

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.services.lint._types import (
        LintDiagnosticData,
        WorkflowLintCompilationData,
        WorkflowLintSummaryData,
    )
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.workflow_graph import WorkflowCreatePayload


def _workflow_plan_stage(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
    compile_allowed: bool,
) -> tuple[
    list[tuple[str, str]] | None,
    WorkflowLintCompilationData | None,
    list[LintDiagnosticData],
    UserInputError | None,
]:
    edges, diagnostics = _workflow_graph_diagnostics(spec)
    graph_errors = [item for item in diagnostics if item["severity"] == "error"]
    if graph_errors:
        failure = UserInputError(
            graph_errors[0]["message"],
            suggestion=(
                "Fix task names and references in the workflow input, then retry "
                "the current operation."
            ),
        )
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_compilation_skipped",
                None,
                "Local compilation requires a valid workflow graph.",
            )
        )
        return edges, None, diagnostics, failure
    if not compile_allowed:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_compilation_skipped",
                None,
                "Local compilation requires valid exact task parameter models.",
            )
        )
        return edges, None, diagnostics, None
    unresolved_names = False
    for index, task in enumerate(spec.tasks):
        params = task.task_params
        reference = params.get("datasource") if isinstance(params, dict) else None
        if isinstance(reference, (int, str)) and not isinstance(reference, bool):
            unresolved_names = unresolved_names or isinstance(reference, str)
            diagnostics.append(
                _diagnostic(
                    "info",
                    "workflow_datasource_binding_deferred",
                    f"tasks[{index}].task_params.datasource",
                    "Datasource identity, permission and type checks require "
                    "the workflow create or edit dry run.",
                )
            )
    try:
        if unresolved_names:
            if catalog.profile_version == "1.3.9":
                validate_legacy_workflow_constraints(spec)
            else:
                validate_workflow_create_constraints(spec, catalog=catalog)
            diagnostics.append(
                _diagnostic(
                    "info",
                    "workflow_compilation_deferred",
                    "tasks",
                    "Local models, graph and runtime constraints are valid; "
                    "wire compilation requires resolving datasource names "
                    "to real remote IDs.",
                )
            )
            return edges, None, diagnostics, None
        compiled_edges, compilation = _prepare_workflow_lint_plan(
            spec,
            catalog=catalog,
        )
    except UserInputError as error:
        failure = _lint_compile_error(error)
    except LegacyWorkflowGraphError as error:
        failure = _lint_compile_error(UserInputError(str(error)))
    else:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_compiles_for_create",
                None,
                (
                    "Workflow DAG compiles to the CLI canonical authoring plan; "
                    "remote workflow.create support is version-gated separately."
                ),
            )
        )
        return compiled_edges, compilation, diagnostics, None
    diagnostics.append(
        _diagnostic(
            "error",
            "workflow_local_plan_invalid",
            "tasks",
            failure.message,
        )
    )
    return edges, None, diagnostics, failure


def _workflow_graph_diagnostics(
    spec: WorkflowSpec,
) -> tuple[list[tuple[str, str]], list[LintDiagnosticData]]:
    task_name_counts = Counter(task.name for task in spec.tasks)
    duplicate_names = {name for name, count in task_name_counts.items() if count > 1}
    task_names = set(task_name_counts)
    edges: list[tuple[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    diagnostics = [
        _diagnostic(
            "error",
            "workflow_task_name_duplicate",
            "tasks",
            f"Task '{name}' is duplicated in workflow YAML.",
        )
        for name in sorted(duplicate_names)
    ]
    for task_index, task in enumerate(spec.tasks):
        _append_task_graph_edges(
            task,
            task_index=task_index,
            task_names=task_names,
            edges=edges,
            seen_edges=seen_edges,
            diagnostics=diagnostics,
        )

    if duplicate_names:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_cycle_check_skipped",
                "tasks",
                "Cycle detection requires unique task names.",
            )
        )
    elif _contains_cycle(task_names, edges):
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_dependency_cycle",
                "tasks",
                "Workflow tasks contain a dependency cycle.",
            )
        )
    if not diagnostics:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_graph_valid",
                "tasks",
                "Workflow task references form a valid acyclic graph.",
            )
        )
    return edges, diagnostics


def _append_task_graph_edges(
    task: WorkflowTaskSpec,
    *,
    task_index: int,
    task_names: set[str],
    edges: list[tuple[str, str]],
    seen_edges: set[tuple[str, str]],
    diagnostics: list[LintDiagnosticData],
) -> None:
    for dependency_index, predecessor in enumerate(task.depends_on):
        _append_graph_edge(
            predecessor,
            task.name,
            path=f"tasks[{task_index}].depends_on[{dependency_index}]",
            task_name=task.name,
            task_names=task_names,
            edges=edges,
            seen_edges=seen_edges,
            diagnostics=diagnostics,
        )
    if task.task_params is None:
        return
    for reference in task_references(
        task.type.upper(),
        task.task_params,
    ):
        if not isinstance(reference.value, str):
            continue
        predecessor, successor = (
            (reference.value, task.name)
            if reference.role == "predecessor"
            else (task.name, reference.value)
        )
        _append_graph_edge(
            predecessor,
            successor,
            path=f"tasks[{task_index}].{reference.field}",
            task_name=task.name,
            task_names=task_names,
            edges=edges,
            seen_edges=seen_edges,
            diagnostics=diagnostics,
        )


def _append_graph_edge(
    predecessor: str,
    successor: str,
    *,
    path: str,
    task_name: str,
    task_names: set[str],
    edges: list[tuple[str, str]],
    seen_edges: set[tuple[str, str]],
    diagnostics: list[LintDiagnosticData],
) -> None:
    if predecessor not in task_names:
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_dependency_unknown",
                path,
                f"Task '{task_name}' references unknown task '{predecessor}'.",
            )
        )
        return
    if successor not in task_names:
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_branch_target_unknown",
                path,
                f"Task '{task_name}' references unknown task '{successor}'.",
            )
        )
        return
    if predecessor == successor:
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_task_self_reference",
                path,
                f"Task '{task_name}' cannot reference itself.",
            )
        )
        return
    edge = (predecessor, successor)
    if edge not in seen_edges:
        seen_edges.add(edge)
        edges.append(edge)


def _contains_cycle(task_names: set[str], edges: Sequence[tuple[str, str]]) -> bool:
    indegree = dict.fromkeys(task_names, 0)
    downstream: dict[str, list[str]] = {name: [] for name in task_names}
    for predecessor, successor in edges:
        downstream[predecessor].append(successor)
        indegree[successor] += 1
    ready = deque(name for name, count in indegree.items() if count == 0)
    visited = 0
    while ready:
        current = ready.popleft()
        visited += 1
        for successor in downstream[current]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                ready.append(successor)
    return visited != len(task_names)


def _prepare_workflow_lint_plan(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[list[tuple[str, str]], WorkflowLintCompilationData]:
    """Compile the selected graph dialect without resolving remote identities."""
    if catalog.profile_version == "1.3.9":
        legacy_compilation = prepare_legacy_workflow_lint_graph(spec)
        legacy_compilation.preview()
        edges = list(legacy_compilation.edges)
        return edges, {
            "taskDefinitionCount": len(spec.tasks),
            "taskRelationCount": len(edges),
            "globalParamCount": (
                0
                if spec.workflow.global_params is None
                else len(spec.workflow.global_params)
            ),
        }
    modern_compilation = prepare_workflow_create_compilation(spec, catalog=catalog)
    return list(modern_compilation.edges), _workflow_lint_compilation(
        modern_compilation.preview()
    )


def _lint_compile_error(error: UserInputError) -> UserInputError:
    if error.suggestion is not None:
        return error
    return UserInputError(
        error.message,
        details=error.details,
        suggestion=(
            "Fix the workflow DAG or task references in the YAML file and retry."
        ),
    )


def _workflow_lint_summary(
    spec: WorkflowSpec,
    *,
    edges: list[tuple[str, str]],
) -> WorkflowLintSummaryData:
    upstream_by_task: dict[str, set[str]] = {task.name: set() for task in spec.tasks}
    downstream_by_task: dict[str, set[str]] = {task.name: set() for task in spec.tasks}
    for predecessor, successor in edges:
        upstream_by_task[successor].add(predecessor)
        downstream_by_task[predecessor].add(successor)
    return {
        "name": spec.workflow.name,
        "project": spec.workflow.project,
        "releaseState": spec.workflow.release_state.value,
        "executionType": spec.workflow.execution_type.value,
        "taskCount": len(spec.tasks),
        "edgeCount": len(edges),
        "taskTypeCounts": dict(
            sorted(Counter(task.type for task in spec.tasks).items())
        ),
        "rootTasks": [
            task.name for task in spec.tasks if not upstream_by_task[task.name]
        ],
        "leafTasks": [
            task.name for task in spec.tasks if not downstream_by_task[task.name]
        ],
        "hasSchedule": spec.schedule is not None,
    }


def _workflow_lint_compilation(
    payload: WorkflowCreatePayload,
) -> WorkflowLintCompilationData:
    counts = workflow_graph_counts(payload)
    return {
        "taskDefinitionCount": counts.task_definitions,
        "taskRelationCount": counts.task_relations,
        "globalParamCount": counts.global_parameters,
    }
