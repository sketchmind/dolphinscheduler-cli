from pathlib import Path

import pytest
import yaml
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.services._task_templates import task_template_yaml
from dsctl.services.lint import (
    lint_workflow_instance_patch_result,
    lint_workflow_patch_result,
    lint_workflow_result,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog


def test_lint_workflow_result_returns_local_summary(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  project: analytics
  release_state: ONLINE
tasks:
  - name: extract
    type: shell
    command: echo extract
    depends_on: []
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - extract
schedule:
  cron: "0 0 2 * * ?"
  timezone: Asia/Shanghai
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert result.resolved == {"kind": "workflow", "file": str(spec_path)}
    assert result.failure is None
    assert result.warnings == []
    assert result.warning_details == []
    data = _mapping(result.data)
    assert "checks" not in data
    assert data["kind"] == "workflow"
    assert data["valid"] is True
    assert data["summary"] == {
        "name": "daily-etl",
        "project": "analytics",
        "releaseState": "ONLINE",
        "executionType": "PARALLEL",
        "taskCount": 2,
        "edgeCount": 1,
        "taskTypeCounts": {"SHELL": 2},
        "rootTasks": ["extract"],
        "leafTasks": ["load"],
        "hasSchedule": True,
    }
    assert data["compilation"] == {
        "taskDefinitionCount": 2,
        "taskRelationCount": 2,
        "globalParamCount": 0,
    }
    diagnostics = [dict(_mapping(item)) for item in _sequence(data["diagnostics"])]
    assert all(
        set(item) == {"severity", "code", "path", "message"} for item in diagnostics
    )
    assert [item["code"] for item in diagnostics] == [
        "workflow_yaml_loaded",
        "workflow_spec_model_valid",
        "workflow_task_valid",
        "workflow_task_valid",
        "workflow_schedule_contract_valid",
        "workflow_graph_valid",
        "workflow_compiles_for_create",
    ]


@pytest.mark.parametrize(
    ("value", "code", "message"),
    [
        (
            "2026-09-20",
            "yaml_date_time_not_string",
            "YAML date/time values must be quoted when a string is intended.",
        ),
        (
            "2026-09-20 12:34:56",
            "yaml_date_time_not_string",
            "YAML date/time values must be quoted when a string is intended.",
        ),
        (
            "!!binary U0VDUkVU",
            "yaml_value_type_unsupported",
            "YAML value type 'bytes' is not supported.",
        ),
        (
            "!!set {sensitive: null}",
            "yaml_value_type_unsupported",
            "YAML value type 'set' is not supported.",
        ),
    ],
)
def test_lint_workflow_locates_unsupported_nested_yaml_values(
    tmp_path: Path,
    value: str,
    code: str,
    message: str,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        f"""
workflow:
  name: typed-yaml-value
  global_params:
    business_date: {value}
tasks:
  - name: echo
    type: SHELL
    command: echo value
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    diagnostics = _sequence(_mapping(result.data)["diagnostics"])
    assert dict(_mapping(diagnostics[0])) == {
        "severity": "error",
        "code": code,
        "path": "workflow.global_params.business_date",
        "message": message,
    }
    assert "SECRET" not in message
    assert "sensitive" not in message


def test_lint_workflow_locates_non_string_mapping_key(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: typed-yaml-key
  global_params:
    20260920: hidden-value
tasks:
  - name: echo
    type: SHELL
    command: echo value
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    diagnostic = _mapping(_sequence(_mapping(result.data)["diagnostics"])[0])
    assert dict(diagnostic) == {
        "severity": "error",
        "code": "yaml_mapping_key_not_string",
        "path": "workflow.global_params",
        "message": "YAML mapping keys must be strings; quote this int key.",
    }
    assert "hidden-value" not in str(diagnostic)


@pytest.mark.parametrize("content", ["plain scalar", "- list-root"])
def test_lint_workflow_retains_root_error_for_non_mapping_documents(
    tmp_path: Path,
    content: str,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(content, encoding="utf-8")

    result = lint_workflow_result(file=spec_path)

    diagnostic = _mapping(_sequence(_mapping(result.data)["diagnostics"])[0])
    assert diagnostic["code"] == "yaml_root_not_mapping"
    assert diagnostic["path"] is None


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_lint_uses_every_selected_exact_task_catalog(
    tmp_path: Path,
    ds_version: str,
) -> None:
    spec_path = tmp_path / f"workflow-{ds_version}.yaml"
    spec_path.write_text(
        """
workflow:
  name: exact-profile-lint
tasks:
  - name: echo
    type: SHELL
    command: echo exact
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(
        file=spec_path,
        catalog=get_task_authoring_catalog(ds_version),
    )

    data = _mapping(result.data)
    assert data["valid"] is True
    assert _mapping(data["summary"])["taskTypeCounts"] == {"SHELL": 1}


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_mr_lint_uses_only_local_preview_resource_identity_bindings(
    tmp_path: Path,
    ds_version: str,
) -> None:
    spec_path = tmp_path / f"workflow-{ds_version}-mr.yaml"
    spec_path.write_text(
        """
workflow:
  name: exact-mr-lint
tasks:
  - name: wordcount
    type: MR
    task_params:
      mainJar: /jobs/wordcount.jar
      mainClass: com.example.WordCount
      mainArgs: [hdfs:///input, hdfs:///output]
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(
        file=spec_path,
        catalog=get_task_authoring_catalog(ds_version),
    )

    data = _mapping(result.data)
    assert data["valid"] is True
    assert _mapping(data["summary"])["taskTypeCounts"] == {"MR": 1}


def test_lint_workflow_result_warns_when_project_is_external(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert result.warnings == [
        "workflow.project is not set in the file; workflow create will need "
        "--project or stored project context."
    ]
    assert list(result.warning_details) == [
        {
            "code": "workflow_project_selection_external",
            "message": (
                "workflow.project is not set in the file; workflow create will "
                "need --project or stored project context."
            ),
            "field": "workflow.project",
            "suggestion": (
                "Pass --project when creating the workflow or set project "
                "context before retrying."
            ),
            "accepted_sources": [
                "--project",
                "context.project",
            ],
        }
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert dict(_mapping(diagnostics[-1])) == {
        "severity": "warning",
        "code": "workflow_project_selection_external",
        "message": result.warnings[0],
        "path": "workflow.project",
    }


def test_lint_workflow_result_warns_on_risky_time_parameter_format(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  project: analytics
  global_params:
    week_key: "$[yyyyww]"
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert result.warnings == [
        "workflow.global_params.week_key contains $[yyyyww]: combining "
        "calendar-year tokens such as yyyy with week tokens such as ww can be "
        "wrong near year boundaries."
    ]
    assert list(result.warning_details) == [
        {
            "code": "parameter_time_format_calendar_year_with_week",
            "message": result.warnings[0],
            "field": "workflow.global_params.week_key",
            "expression": "$[yyyyww]",
            "pattern": "yyyyww",
            "suggestion": (
                "Use DS year_week(...) when week-of-year output is intended, or "
                "choose yyyy versus YYYY deliberately before applying the workflow."
            ),
        }
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert dict(_mapping(diagnostics[-1])) == {
        "severity": "warning",
        "code": "parameter_time_format_calendar_year_with_week",
        "message": result.warnings[0],
        "path": "workflow.global_params.week_key",
    }


def test_lint_warns_when_sub_workflow_local_params_are_used_as_child_inputs(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent
  project: analytics
tasks:
  - name: invoke-child
    type: SUB_WORKFLOW
    task_params:
      workflowDefinitionCode: 123456789
      localParams:
        - prop: run_label
          direct: IN
          type: VARCHAR
          value: FROM_PARENT
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert [detail["code"] for detail in result.warning_details] == [
        "sub_workflow_local_params_not_child_inputs"
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert dict(_mapping(diagnostics[-1])) == {
        "severity": "warning",
        "code": "sub_workflow_local_params_not_child_inputs",
        "message": result.warnings[0],
        "path": "tasks[0].task_params.localParams",
    }


def test_lint_warns_on_self_referential_workflow_global(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  project: analytics
  global_params:
    run_label: prefix-${run_label}
tasks:
  - name: report
    type: SHELL
    command: echo ${run_label}
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert [detail["code"] for detail in result.warning_details] == [
        "parameter_global_self_reference"
    ]
    data = _mapping(result.data)
    diagnostics = _sequence(data["diagnostics"])
    assert _mapping(diagnostics[-1])["path"] == "workflow.global_params.run_label"


def test_lint_workflow_result_rejects_schedule_on_offline_workflow(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    depends_on: []
schedule:
  cron: "0 0 2 * * ?"
  timezone: Asia/Shanghai
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert isinstance(result.failure, UserInputError)
    assert "require workflow.release_state=ONLINE" in result.failure.message
    data = _mapping(result.data)
    assert data["valid"] is False
    diagnostics = _sequence(data["diagnostics"])
    assert any(
        _mapping(item)["code"] == "workflow_schedule_contract_invalid"
        for item in diagnostics
    )


def test_lint_workflow_result_rejects_unknown_dependencies(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-etl
tasks:
  - name: load
    type: SHELL
    command: echo load
    depends_on:
      - missing
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert isinstance(result.failure, UserInputError)
    assert result.failure.suggestion == (
        "Fix task names and references in the workflow input, then retry the "
        "current operation."
    )
    data = _mapping(result.data)
    assert data["valid"] is False
    diagnostics = _sequence(data["diagnostics"])
    unknown = next(
        _mapping(item)
        for item in diagnostics
        if _mapping(item)["code"] == "workflow_dependency_unknown"
    )
    assert unknown["path"] == "tasks[0].depends_on[0]"
    assert "unknown task 'missing'" in str(unknown["message"])


@pytest.mark.parametrize("version", ["1.3.9", "3.4.1", "3.4.2"])
@pytest.mark.parametrize("invalid_dependency", [False, True])
def test_named_datasource_lint_defers_binding_but_still_checks_graph(
    tmp_path: Path, version: str, *, invalid_dependency: bool
) -> None:
    path = tmp_path / "named-datasource.yaml"
    dependency = "[missing]" if invalid_dependency else "[]"
    path.write_text(
        "workflow:\n  name: named-source\n  project: analytics\n"
        "tasks:\n  - name: query\n    type: SQL\n"
        f"    depends_on: {dependency}\n"
        "    task_params:\n      type: MYSQL\n      datasource: warehouse\n"
        "      sql: SELECT 1\n      sqlType: 0\n",
        encoding="utf-8",
    )
    result = lint_workflow_result(
        file=path, catalog=get_task_authoring_catalog(version)
    )
    data = _mapping(result.data)
    codes = {_mapping(item)["code"] for item in _sequence(data["diagnostics"])}
    assert "compilation" not in data
    assert _mapping(data["summary"])["taskCount"] == 1
    if invalid_dependency:
        assert isinstance(result.failure, UserInputError)
        assert "workflow_dependency_unknown" in codes
    else:
        assert result.failure is None
        assert data["valid"] is True
        assert "workflow_datasource_binding_deferred" in codes
        assert "workflow_compilation_deferred" in codes
        assert "workflow_graph_valid" in codes


@pytest.mark.parametrize(
    ("version", "task_type"),
    [
        ("3.4.1", "DATAX"),
        ("3.4.1", "SEATUNNEL"),
        ("3.3.2", "PYTORCH"),
        ("3.4.1", "KUBEFLOW"),
        ("3.2.2", "DYNAMIC"),
    ],
)
@pytest.mark.parametrize("datasource", [123, "warehouse"])
@pytest.mark.parametrize("invalid_runtime", [False, True])
def test_datasource_reference_does_not_hide_other_task_runtime_constraints(
    tmp_path: Path,
    version: str,
    task_type: str,
    datasource: int | str,
    *,
    invalid_runtime: bool,
) -> None:
    catalog = get_task_authoring_catalog(version)
    task = dict(
        _mapping(yaml.safe_load(task_template_yaml(task_type, catalog=catalog)))
    )
    globals_: dict[str, str] = {}
    if invalid_runtime:
        if task_type in {"DATAX", "SEATUNNEL"}:
            globals_ = {"bizdate": "20260915"}
        elif task_type == "PYTORCH":
            task["timeout"] = 1
        else:
            task["retry"] = {"times": 2, "interval": 0}
    document = {
        "workflow": {"name": "mixed-task-constraints", "global_params": globals_},
        "tasks": [
            {
                "name": "query",
                "type": "SQL",
                "task_params": {
                    "type": "MYSQL",
                    "datasource": datasource,
                    "sql": "SELECT 1",
                    "sqlType": 0,
                },
            },
            task,
        ],
    }
    path = tmp_path / "mixed-task-constraints.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    result = lint_workflow_result(file=path, catalog=catalog)

    data = _mapping(result.data)
    codes = {_mapping(item)["code"] for item in _sequence(data["diagnostics"])}
    assert data["valid"] is not invalid_runtime
    if invalid_runtime:
        assert isinstance(result.failure, UserInputError)
        assert task_type in result.failure.message
        assert "workflow_local_plan_invalid" in codes
        assert "workflow_compilation_deferred" not in codes
    else:
        assert result.failure is None
        assert ("workflow_compilation_deferred" in codes) == isinstance(datasource, str)


@pytest.mark.parametrize("datasource", [123, "warehouse"])
@pytest.mark.parametrize(
    ("scope", "field", "value"),
    [
        ("workflow", "execution_type", "SERIAL_WAIT"),
        ("task", "delay", 1),
        ("task", "environment_code", 123),
        ("task", "task_group_id", 123),
        ("task", "cpu_quota", 1),
        ("task", "memory_max", 128),
    ],
)
def test_named_datasource_retains_legacy_unrepresentable_setting_errors(
    tmp_path: Path,
    datasource: int | str,
    scope: str,
    field: str,
    value: int | str,
) -> None:
    workflow: dict[str, object] = {"name": "legacy-constraints"}
    task: dict[str, object] = {
        "name": "query",
        "type": "SQL",
        "task_params": {
            "type": "MYSQL",
            "datasource": datasource,
            "sql": "SELECT 1",
            "sqlType": 0,
        },
    }
    target = workflow if scope == "workflow" else task
    target[field] = value
    document = {"workflow": workflow, "tasks": [task]}
    path = tmp_path / "legacy-constraints.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    result = lint_workflow_result(
        file=path, catalog=get_task_authoring_catalog("1.3.9")
    )

    assert _mapping(result.data)["valid"] is False
    assert isinstance(result.failure, UserInputError)
    assert field in result.failure.message
    assert "not representable" in result.failure.message


def test_lint_preserves_all_same_phase_model_errors(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: " "
  timeout: -1
tasks:
  - name: ""
    type: SHELL
    command: echo invalid
    timeout: -2
  - name: second
    type: SHELL
    command: echo invalid
    extra_field: true
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert isinstance(result.failure, UserInputError)
    data = _mapping(result.data)
    diagnostics = [dict(_mapping(item)) for item in _sequence(data["diagnostics"])]
    error_paths = {item["path"] for item in diagnostics if item["severity"] == "error"}
    assert error_paths == {
        "workflow.name",
        "workflow.timeout",
        "tasks[0].name",
        "tasks[0].timeout",
        "tasks[1].extra_field",
    }
    assert "summary" not in data
    assert "compilation" not in data
    assert any(item["code"] == "workflow_compilation_skipped" for item in diagnostics)


def test_lint_aggregates_task_model_name_and_dependency_errors(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "multi.yaml"
    spec_path.write_text(
        """
workflow:
  name: multi-error
tasks:
  - name: duplicate
    type: SQL
    task_params:
      type: MYSQL
      sql: select 1
    depends_on: [missing]
  - name: duplicate
    type: SHELL
    command: echo duplicate
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(file=spec_path)

    assert isinstance(result.failure, UserInputError)
    diagnostics = [
        dict(_mapping(item)) for item in _sequence(_mapping(result.data)["diagnostics"])
    ]
    errors = {
        (item["code"], item["path"])
        for item in diagnostics
        if item["severity"] == "error"
    }
    assert errors == {
        ("workflow_task_model_missing", "tasks[0].task_params.datasource"),
        ("workflow_task_model_missing", "tasks[0].task_params.sqlType"),
        ("workflow_task_name_duplicate", "tasks"),
        ("workflow_dependency_unknown", "tasks[0].depends_on[0]"),
    }
    assert any(item["code"] == "workflow_cycle_check_skipped" for item in diagnostics)
    assert any(item["code"] == "workflow_compilation_skipped" for item in diagnostics)


def test_lint_workflow_patch_validates_offline_and_lists_deferred_checks(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      timeout: 60
  tasks:
    create:
      - name: report
        type: SHELL
        command: |
          echo report
    update:
      - match: {name: existing}
        set: {description: retained}
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_patch_result(file=patch_path)

    assert result.failure is None
    data = _mapping(result.data)
    assert data["kind"] == "workflow-patch"
    assert data["valid"] is True
    assert data["summary"] == {
        "workflowFields": ["timeout"],
        "createCount": 1,
        "updateCount": 1,
        "renameCount": 0,
        "deleteCount": 0,
    }
    baseline = _mapping(data["baseline"])
    assert baseline["requiredForCompleteValidation"] is True
    deferred = _sequence(baseline["unchecked"])
    assert {_mapping(item)["code"] for item in deferred} >= {
        "patch_live_task_matches_unchecked",
        "patch_partial_task_merge_unchecked",
        "patch_final_graph_unchecked",
    }


def test_lint_patch_aggregates_intrinsic_conflicts(tmp_path: Path) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - {from: old, to: new}
      - {from: old, to: other}
    update:
      - match: {name: old}
        set: {description: updated}
    delete: [old]
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_patch_result(file=patch_path)

    assert isinstance(result.failure, UserInputError)
    diagnostics = _sequence(_mapping(result.data)["diagnostics"])
    codes = {
        _mapping(item)["code"]
        for item in diagnostics
        if _mapping(item)["severity"] == "error"
    }
    assert codes == {
        "workflow_patch_rename_source_duplicate",
        "workflow_patch_rename_delete_conflict",
        "workflow_patch_update_delete_conflict",
    }


def test_lint_patch_checks_the_created_subgraph_without_a_baseline(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    create:
      - name: first
        type: SHELL
        command: echo first
        depends_on: [second]
      - name: second
        type: SHELL
        command: echo second
        depends_on: [first]
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_patch_result(file=patch_path)

    assert isinstance(result.failure, UserInputError)
    diagnostics = _sequence(_mapping(result.data)["diagnostics"])
    assert any(
        _mapping(item)["code"] == "workflow_patch_created_task_cycle"
        for item in diagnostics
    )


def test_lint_workflow_instance_patch_rejects_definition_fields(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      name: renamed
      release_state: ONLINE
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_instance_patch_result(file=patch_path)

    assert isinstance(result.failure, UserInputError)
    data = _mapping(result.data)
    assert data["valid"] is False
    diagnostics = _sequence(data["diagnostics"])
    unsupported_paths = {
        _mapping(item)["path"]
        for item in diagnostics
        if _mapping(item)["code"] == "workflow_instance_patch_field_unsupported"
    }
    assert unsupported_paths == {
        "patch.workflow.set.name",
        "patch.workflow.set.release_state",
    }
