from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence
from tests.workflow_domain_fakes import (
    _FakeWorkflowOperations,
    install_workflow_domain_runtime,
)

from dsctl.errors import (
    ApiResultError,
    NotFoundError,
    PermissionDeniedError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.models import WorkflowSpec
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._legacy_workflow_references import (
    audit_legacy_authoring_workflow_graph,
)
from dsctl.services._workflow import authoring as workflow_authoring_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.lint import lint_workflow_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.definition_models import NativeId, ProjectRef
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph
from dsctl.upstream.legacy_workflow_references import (
    LegacyNestedWorkflowLimitError,
    LegacyWorkflowAuthoringResolution,
    LegacyWorkflowReferenceResolver,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.upstream.legacy_workflow_references import (
        LegacyWorkflowReferenceOperations,
    )


_VERSION = "1.3.9"


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    workflows: list[FakeWorkflow] | None = None,
) -> tuple[_FakeWorkflowOperations, FakeWorkflowAdapter]:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod", description="ETL")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=(
            workflows
            if workflows is not None
            else [
                FakeWorkflow(
                    code=202,
                    id=202,
                    name="child-daily",
                    project_code_value=7,
                    project_name_value="etl-prod",
                )
            ]
        ),
        dags={},
    )
    operations = install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=FakeTaskAdapter(workflow_tasks={}),
        profile=make_profile(ds_version=_VERSION),
    )
    monkeypatch.setattr(
        workflow_authoring_service,
        "load_selected_task_authoring_catalog",
        lambda _env_file: get_task_authoring_catalog(_VERSION),
    )
    for workflow in workflow_adapter.workflows:
        operations.legacy_definitions.setdefault(
            workflow.code,
            _legacy_wire(workflow.name or f"workflow-{workflow.code}"),
        )
    return operations, workflow_adapter


def _legacy_wire(
    workflow_name: str,
    *,
    child_id: int | None = None,
) -> tuple[str, str, str]:
    task = (
        {
            "name": "run-child",
            "type": "SUB_PROCESS",
            "task_params": {"processDefinitionId": child_id},
        }
        if child_id is not None
        else {"name": "work", "type": "SHELL", "command": "echo work"}
    )
    prepared = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": workflow_name, "project": "etl-prod"},
                "tasks": [task],
            }
        ),
        task_id_factory=lambda task_name: f"tasks-{workflow_name}-{task_name}",
    ).materialize()
    return (
        prepared["processDefinitionJson"],
        prepared["locations"],
        prepared["connects"],
    )


def test_sub_workflow_139_catalog_owns_name_selector_not_modern_code() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    spec = validate_workflow_document(
        {
            "workflow": {"name": "parent-daily", "project": "etl-prod"},
            "tasks": [
                {
                    "name": "run-child",
                    "type": "SUB_WORKFLOW",
                    "task_params": {"childWorkflowName": "child-daily"},
                }
            ],
        },
        authoring_context=context,
    )

    assert catalog.source_task_type_for_cli("SUB_WORKFLOW") == "SUB_PROCESS"
    assert catalog.authoring_surface.nested_workflow.native_code_field == (
        "processDefinitionId"
    )
    assert spec.tasks[0].task_params == {"childWorkflowName": "child-daily"}

    with pytest.raises(ValueError, match="childWorkflowName"):
        validate_workflow_document(
            {
                "workflow": {"name": "parent-daily", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "run-child",
                        "type": "SUB_WORKFLOW",
                        "task_params": {"workflowDefinitionCode": 202},
                    }
                ],
            },
            authoring_context=context,
        )


@pytest.mark.parametrize("child_name", ["${child}", "$[yyyyMMdd]"])
def test_sub_workflow_139_create_rejects_placeholder_child_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    child_name: str,
) -> None:
    _install_runtime(monkeypatch)
    spec_path = tmp_path / "placeholder-child.yaml"
    spec_path.write_text(
        f"""
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params:
      childWorkflowName: {child_name!r}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="childWorkflowName must be a literal name without DS placeholders",
    ):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_create_rejects_raw_native_sub_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch)
    spec_path = tmp_path / "raw-native-child.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_PROCESS
    task_params:
      processDefinitionId: 202
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="SUB_PROCESS opaque authoring is unsupported",
    ):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_lint_is_local_and_compiles_canonical_name(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "parent.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params:
      childWorkflowName: child-daily
""".strip(),
        encoding="utf-8",
    )

    result = lint_workflow_result(
        file=spec_path,
        catalog=get_task_authoring_catalog(_VERSION),
    )

    data = _mapping(result.data)
    assert data["valid"] is True
    assert _mapping(data["summary"])["taskTypeCounts"] == {"SUB_WORKFLOW": 1}
    assert _mapping(data["compilation"])["taskDefinitionCount"] == 1


def test_sub_workflow_139_create_resolves_name_to_exact_native_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch)
    spec_path = tmp_path / "parent.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params:
      childWorkflowName: child-daily
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    process = _mapping(json.loads(cast("str", form["processDefinitionJson"])))
    task = _mapping(_sequence(process["tasks"])[0])
    assert task["type"] == "SUB_PROCESS"
    assert task["params"] == {"processDefinitionId": 202}


def test_sub_workflow_139_numeric_child_name_is_not_reinterpreted_as_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=202,
                id=202,
                name="123",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
    )
    spec_path = tmp_path / "numeric-name.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: "123"}
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    process = _mapping(json.loads(cast("str", form["processDefinitionJson"])))
    task = _mapping(_sequence(process["tasks"])[0])
    assert task["params"] == {"processDefinitionId": 202}


@pytest.mark.parametrize("version", ["2.0.0", "3.4.2"])
def test_modern_sub_workflow_profiles_keep_code_identity(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    spec = validate_workflow_document(
        {
            "workflow": {"name": "parent-daily", "project": "etl-prod"},
            "tasks": [
                {
                    "name": "run-child",
                    "type": "SUB_WORKFLOW",
                    "task_params": {"workflowDefinitionCode": 202},
                }
            ],
        },
        authoring_context=context,
    )
    assert spec.tasks[0].task_params == {"workflowDefinitionCode": 202}

    with pytest.raises(ValueError, match="workflowDefinitionCode"):
        validate_workflow_document(
            {
                "workflow": {"name": "parent-daily", "project": "etl-prod"},
                "tasks": [
                    {
                        "name": "run-child",
                        "type": "SUB_WORKFLOW",
                        "task_params": {"childWorkflowName": "child-daily"},
                    }
                ],
            },
            authoring_context=context,
        )


def test_sub_workflow_139_create_rejects_missing_child_before_prepare(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(monkeypatch, workflows=[])
    spec_path = tmp_path / "missing-child.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: missing-child}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match=r"missing-child.*could not be resolved"):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert operations.legacy_definitions == {}


def test_sub_workflow_139_create_preserves_child_permission_error_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(monkeypatch)

    def deny_child(_project: object, _workflow_name: str) -> object:
        raise ApiResultError(
            result_code=30001,
            result_message="no workflow permission",
        )

    monkeypatch.setattr(operations, "resolve_workflow_by_name", deny_child)
    spec_path = tmp_path / "forbidden-child.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: child-daily}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(PermissionDeniedError, match="child-daily"):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_create_translates_descendant_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="child-a",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="child-b",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire("child-a", child_id=202)
    original_resolve = operations.resolve_workflow_by_id

    def deny_descendant(project: ProjectRef, workflow_id: int) -> object:
        if workflow_id == 202:
            raise ApiResultError(
                result_code=30001,
                result_message="no descendant permission",
            )
        return original_resolve(project, workflow_id)

    monkeypatch.setattr(operations, "resolve_workflow_by_id", deny_descendant)
    spec_path = tmp_path / "forbidden-descendant.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: child-a}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        PermissionDeniedError,
        match="cannot validate a nested-workflow descendant",
    ):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_create_translates_malformed_descendant_edge(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="child-a",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
    )
    process_json, locations, connects = _legacy_wire("child-a")
    process = cast("dict[str, object]", json.loads(process_json))
    task = cast("dict[str, object]", _sequence(process["tasks"])[0])
    params = cast("dict[str, object]", task["params"])
    params["processDefinitionId"] = "1_0"
    operations.legacy_definitions[101] = (
        json.dumps(process, separators=(",", ":")),
        locations,
        connects,
    )
    spec_path = tmp_path / "malformed-descendant.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: child-a}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="descendants could not be safely validated",
    ):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_service_translates_descendant_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compilation = prepare_legacy_workflow_graph(
        WorkflowSpec.model_validate(
            {
                "workflow": {"name": "parent", "project": "etl-prod"},
                "tasks": [{"name": "work", "type": "SHELL", "command": "true"}],
            }
        )
    )

    def exceed_limit(*_args: object, **_kwargs: object) -> None:
        raise LegacyNestedWorkflowLimitError(1_000)

    monkeypatch.setattr(
        LegacyWorkflowReferenceResolver,
        "audit_compilation",
        exceed_limit,
    )

    with pytest.raises(UserInputError, match="exceeded its safety limit"):
        audit_legacy_authoring_workflow_graph(
            cast("LegacyWorkflowReferenceOperations", object()),
            project=ProjectRef(NativeId(7), "etl-prod", None),
            compilation=compilation,
            containing_workflow_id=None,
            resolution=LegacyWorkflowAuthoringResolution.empty(),
            action="workflow.create",
        )


def test_sub_workflow_139_create_rejects_direct_self_reference(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_runtime(monkeypatch, workflows=[])
    spec_path = tmp_path / "self-child.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: parent-daily}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="recursively reference its parent"):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_create_rejects_reachable_native_cycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="child-a",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="child-b",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire("child-a", child_id=202)
    operations.legacy_definitions[202] = _legacy_wire("child-b", child_id=101)
    spec_path = tmp_path / "cyclic-child.yaml"
    spec_path.write_text(
        """
workflow:
  name: new-parent
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_WORKFLOW
    task_params: {childWorkflowName: child-a}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="nested-workflow cycle"):
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)


def test_sub_workflow_139_export_and_edit_roundtrip_use_child_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="child-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire(
        "parent-daily",
        child_id=202,
    )
    exported = workflow_service.export_workflow_yaml_result(
        "parent-daily",
        project="etl-prod",
    )

    yaml_text = cast("str", _mapping(exported.data)["yaml"])
    document = _mapping(yaml.safe_load(yaml_text))
    task = _mapping(_sequence(document["tasks"])[0])
    assert task["type"] == "SUB_WORKFLOW"
    assert task["task_params"] == {"childWorkflowName": "child-daily"}

    described = workflow_service.describe_workflow_result(
        "parent-daily",
        project="etl-prod",
    )
    described_task = _mapping(_sequence(_mapping(described.data)["tasks"])[0])
    assert described_task["type"] == "SUB_WORKFLOW"
    assert described_task["taskParams"] == {"childWorkflowName": "child-daily"}

    workflow_document = dict(_mapping(document["workflow"]))
    workflow_document["description"] = "Roundtrip metadata edit"
    roundtrip_path = tmp_path / "roundtrip.yaml"
    roundtrip_path.write_text(
        yaml.safe_dump({**document, "workflow": workflow_document}, sort_keys=False),
        encoding="utf-8",
    )
    edited = workflow_service.edit_workflow_result(
        "parent-daily",
        project="etl-prod",
        file=roundtrip_path,
        dry_run=True,
    )
    request = _mapping(first_dry_run_request(_mapping(edited.data)))
    form = _mapping(request["form"])
    process = _mapping(json.loads(cast("str", form["processDefinitionJson"])))
    native_task = _mapping(_sequence(process["tasks"])[0])
    assert native_task["type"] == "SUB_PROCESS"
    assert native_task["params"] == {"processDefinitionId": 202}


def test_sub_workflow_139_read_stays_canonical_when_grandchild_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="child-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire(
        "parent-daily",
        child_id=202,
    )
    operations.legacy_definitions[202] = _legacy_wire(
        "child-daily",
        child_id=999,
    )

    exported = workflow_service.export_workflow_yaml_result(
        "parent-daily",
        project="etl-prod",
    )

    document = _mapping(yaml.safe_load(cast("str", _mapping(exported.data)["yaml"])))
    task = _mapping(_sequence(document["tasks"])[0])
    assert task["type"] == "SUB_WORKFLOW"
    assert task["task_params"] == {"childWorkflowName": "child-daily"}


def test_sub_workflow_139_edit_rejects_new_raw_native_sub_process(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire("parent-daily")
    spec_path = tmp_path / "raw-native-edit.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: run-child
    type: SUB_PROCESS
    task_params:
      processDefinitionId: 202
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UnsupportedFeatureError,
        match="SUB_PROCESS opaque authoring is unsupported",
    ):
        workflow_service.edit_workflow_result(
            "parent-daily",
            project="etl-prod",
            file=spec_path,
            dry_run=True,
        )


def test_sub_workflow_139_edit_can_repair_by_removing_bad_opaque_edge(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire(
        "parent-daily",
        child_id=999,
    )
    spec_path = tmp_path / "remove-bad-edge.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent-daily
  project: etl-prod
tasks:
  - name: replacement
    type: SHELL
    command: echo repaired
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "parent-daily",
        project="etl-prod",
        file=spec_path,
        dry_run=True,
    )

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    form = _mapping(request["form"])
    process = _mapping(json.loads(cast("str", form["processDefinitionJson"])))
    task = _mapping(_sequence(process["tasks"])[0])
    assert task["type"] == "SHELL"
    assert "processDefinitionId" not in _mapping(task["params"])


def test_sub_workflow_139_unresolved_native_reference_exports_opaque(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            )
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire(
        "parent-daily",
        child_id=999,
    )

    exported = workflow_service.export_workflow_yaml_result(
        "parent-daily",
        project="etl-prod",
    )

    document = _mapping(yaml.safe_load(cast("str", _mapping(exported.data)["yaml"])))
    task = _mapping(_sequence(document["tasks"])[0])
    assert task["type"] == "SUB_PROCESS"
    assert task["task_params"] == {"processDefinitionId": 999}


def test_sub_workflow_139_placeholder_named_child_exports_opaque(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="${child}",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    operations.legacy_definitions[101] = _legacy_wire(
        "parent-daily",
        child_id=202,
    )

    exported = workflow_service.export_workflow_yaml_result(
        "parent-daily",
        project="etl-prod",
    )

    document = _mapping(yaml.safe_load(cast("str", _mapping(exported.data)["yaml"])))
    task = _mapping(_sequence(document["tasks"])[0])
    assert task["type"] == "SUB_PROCESS"
    assert task["task_params"] == {"processDefinitionId": 202}


def test_sub_workflow_139_read_translates_exact_missing_definition_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations, _adapter = _install_runtime(monkeypatch)

    def missing_definition(_scope: object, *, action: str) -> object:
        assert action == "workflow.export"
        raise ApiResultError(
            result_code=50001,
            result_message="process definition does not exist",
        )

    monkeypatch.setattr(operations, "legacy_definition", missing_definition)

    with pytest.raises(NotFoundError, match="legacy workflow was not found"):
        workflow_service.export_workflow_yaml_result(
            workflow="child-daily",
            project="etl-prod",
        )


def test_sub_workflow_139_richer_native_reference_stays_lossless_opaque(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operations, _adapter = _install_runtime(
        monkeypatch,
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="parent-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
            FakeWorkflow(
                code=202,
                id=202,
                name="child-daily",
                project_code_value=7,
                project_name_value="etl-prod",
            ),
        ],
    )
    process_json, locations, connects = _legacy_wire(
        "parent-daily",
        child_id=202,
    )
    process = json.loads(process_json)
    assert isinstance(process, dict)
    tasks = process["tasks"]
    assert isinstance(tasks, list)
    native_task = tasks[0]
    assert isinstance(native_task, dict)
    native_task["params"]["futureOption"] = {"kept": True}
    operations.legacy_definitions[101] = (
        json.dumps(process, separators=(",", ":")),
        locations,
        connects,
    )

    exported = workflow_service.export_workflow_yaml_result(
        "parent-daily",
        project="etl-prod",
    )

    yaml_text = cast("str", _mapping(exported.data)["yaml"])
    document = _mapping(yaml.safe_load(yaml_text))
    task = _mapping(_sequence(document["tasks"])[0])
    assert task["type"] == "SUB_PROCESS"
    assert task["task_params"] == {
        "processDefinitionId": 202,
        "futureOption": {"kept": True},
    }

    workflow_document = dict(_mapping(document["workflow"]))
    workflow_document["description"] = "Opaque roundtrip metadata edit"
    roundtrip_path = tmp_path / "opaque-roundtrip.yaml"
    roundtrip_path.write_text(
        yaml.safe_dump({**document, "workflow": workflow_document}, sort_keys=False),
        encoding="utf-8",
    )
    edited = workflow_service.edit_workflow_result(
        "parent-daily",
        project="etl-prod",
        file=roundtrip_path,
        dry_run=True,
    )
    request = _mapping(first_dry_run_request(_mapping(edited.data)))
    form = _mapping(request["form"])
    roundtrip_process = _mapping(json.loads(cast("str", form["processDefinitionJson"])))
    roundtrip_task = _mapping(_sequence(roundtrip_process["tasks"])[0])
    assert roundtrip_task["type"] == "SUB_PROCESS"
    assert roundtrip_task["params"] == native_task["params"]
