from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_explicit_empty_execution_keeps_only_requested_enum_metadata() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    ir = _load_module("ds_codegen.ir")
    release_state = _enum(ir, "example.ReleaseState")
    snapshot = ir.ContractSnapshot(
        ds_version="3.2.2",
        operation_count=1,
        enum_count=1,
        dto_count=0,
        model_count=1,
        operations=[_operation(ir, "ProjectController.queryProjectListPaging")],
        enums=[release_state],
        dtos=[],
        models=[_model(ir, "example.Project")],
    )
    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {},
        additional_type_refs={impact.WireTypeRef("enums", release_state.import_path)},
        execution_operation_ids=set(),
        execution_type_refs=set(),
    )
    assert sliced.operations == sliced.models == sliced.dtos == []
    assert sliced.operation_count == sliced.model_count == sliced.dto_count == 0
    assert sliced.enums == [release_state]
    assert sliced.enum_count == 1
    assert snapshot.operation_count == 1
    with pytest.raises(ValueError, match="at least one semantic binding"):
        runtime_contract.slice_contract_for_bindings(snapshot, {})
    with pytest.raises(ValueError, match="at least one semantic binding"):
        runtime_contract.slice_contract_for_bindings(
            snapshot,
            {},
            execution_operation_ids={"ProjectController.queryProjectListPaging"},
            execution_type_refs=set(),
        )


def test_runtime_contract_slice_keeps_only_the_reviewed_wire_closure() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    ir = _load_module("ds_codegen.ir")
    selected_operation = _operation(ir, "ProjectController.queryProjectListPaging")
    unrelated_operation = _operation(ir, "UsersController.queryUserList")
    project_model = _model(ir, "example.Project")
    unrelated_model = _model(ir, "example.User")
    release_state = _enum(ir, "example.ReleaseState")
    snapshot = ir.ContractSnapshot(
        ds_version="3.2.2",
        operation_count=2,
        enum_count=1,
        dto_count=0,
        model_count=2,
        operations=[unrelated_operation, selected_operation],
        enums=[release_state],
        dtos=[],
        models=[unrelated_model, project_model],
    )
    binding = impact.ReviewedBinding(
        source_operations=(selected_operation.operation_id,),
        type_closure=(
            impact.WireTypeRef("models", project_model.import_path),
            impact.WireTypeRef("enums", release_state.import_path),
        ),
        selector_semantics=(
            impact.SelectorSemantics(
                resource="project",
                consumed_selectors=(),
                exposed_identities=("name", "code"),
                native_identity="code",
                resolution="paged-search-discovery",
            ),
        ),
        evidence_sources=(
            impact.EvidenceSource("controller", "ProjectController.java#query"),
            impact.EvidenceSource("ui", "projects/index.ts"),
        ),
    )

    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"project.page": binding},
    )

    assert sliced.operation_count == 1
    assert [item.operation_id for item in sliced.operations] == [
        "ProjectController.queryProjectListPaging"
    ]
    assert sliced.model_count == 1
    assert [item.import_path for item in sliced.models] == ["example.Project"]
    assert sliced.enum_count == 1
    assert [item.import_path for item in sliced.enums] == ["example.ReleaseState"]

    with pytest.raises(ValueError, match=r"selector_semantics must not be empty"):
        runtime_contract.slice_contract_for_bindings(
            snapshot,
            {
                "project.page": replace(
                    binding,
                    selector_semantics=(),
                )
            },
        )


def test_logical_response_closure_excludes_the_transport_envelope() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    ir = _load_module("ds_codegen.ir")
    operation = replace(
        _operation(ir, "TaskDefinitionController.queryTaskDefinitionDetail"),
        return_type="Result<TaskDefinitionVO>",
        logical_return_type="TaskDefinitionVO",
    )
    result_model = _model(ir, "example.Result")
    task_definition = replace(
        _model(ir, "example.TaskDefinition"),
        fields=[_field(ir, "priority", "Priority")],
    )
    relation = _model(ir, "example.WorkflowTaskRelation")
    task_view = replace(
        _model(ir, "example.TaskDefinitionVO"),
        extends="TaskDefinition",
        fields=[
            _field(
                ir,
                "workflowTaskRelationList",
                "List<WorkflowTaskRelation>",
            )
        ],
    )
    priority = _enum(ir, "example.Priority")
    snapshot = ir.ContractSnapshot(
        ds_version="3.4.2",
        operation_count=1,
        enum_count=1,
        dto_count=0,
        model_count=4,
        operations=[operation],
        enums=[priority],
        dtos=[],
        models=[result_model, task_definition, relation, task_view],
    )
    binding = impact.ReviewedBinding(
        source_operations=(operation.operation_id,),
        type_closure=(impact.WireTypeRef("models", task_view.import_path),),
        selector_semantics=(),
        evidence_sources=(
            impact.EvidenceSource("controller", "TaskDefinitionController.java#query"),
            impact.EvidenceSource("ui", "task-definition/index.ts"),
        ),
    )

    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"identity.current": binding},
    )

    assert {item.import_path for item in sliced.models} == {
        "example.TaskDefinition",
        "example.TaskDefinitionVO",
        "example.WorkflowTaskRelation",
    }
    # Result is the transport envelope; generated operations expose only the
    # exact logical response and its structured dependencies.
    assert all(item.import_path != "example.Result" for item in sliced.models)
    assert [item.import_path for item in sliced.enums] == ["example.Priority"]


def test_runtime_contract_slice_rejects_an_incomplete_source_snapshot() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    ir = _load_module("ds_codegen.ir")
    snapshot = ir.ContractSnapshot(
        ds_version="3.2.2",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )
    binding = impact.ReviewedBinding(
        source_operations=("ProjectController.queryProjectListPaging",),
        type_closure=(impact.WireTypeRef("models", "example.Project"),),
        selector_semantics=(
            impact.SelectorSemantics(
                resource="project",
                consumed_selectors=(),
                exposed_identities=("name", "code"),
                native_identity="code",
                resolution="paged-search-discovery",
            ),
        ),
        evidence_sources=(
            impact.EvidenceSource("controller", "ProjectController.java#query"),
            impact.EvidenceSource("ui", "projects/index.ts"),
        ),
    )

    with pytest.raises(
        ValueError,
        match=(
            r"runtime contract slice is incomplete.*"
            "ProjectController.queryProjectListPaging.*models:example.Project"
        ),
    ):
        runtime_contract.slice_contract_for_bindings(
            snapshot,
            {"project.page": binding},
        )


def test_runtime_semantic_operations_are_sorted_reviewed_binding_keys() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")

    for version in ("1.3.9", "3.4.2"):
        bindings = runtime_contract.runtime_operation_bindings(version)
        auxiliary = runtime_contract.runtime_auxiliary_operation_bindings(version)
        operations = runtime_contract.runtime_semantic_operations(version)

        assert operations == tuple(sorted({**bindings, **auxiliary}))
        assert len(operations) == len(set(operations))
        impact.validate_reviewed_bindings({version: bindings})
        impact.validate_reviewed_bindings({version: auxiliary})
        assert {"identity.current", "project.get", "workflow.page"} <= set(operations)


def test_343_private_identity_dependency_keeps_rich_user_get_independent() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    security = _load_module("ds_codegen.security_contract")
    runtime_bundles = _load_module("ds_codegen.runtime_bundles")

    identity = runtime_contract.runtime_auxiliary_operation_bindings("3.4.3")[
        "user.identity"
    ]
    public_bindings = runtime_contract.runtime_operation_bindings("3.4.3")
    assert "user.identity" not in public_bindings
    assert "user.identity" not in runtime_contract.runtime_auxiliary_operation_bindings(
        "3.4.2"
    )
    assert identity.source_operations == security.user_identity_read_operations("3.4.3")
    assert identity.source_operations == (
        "UsersController.getUserInfo",
        "UsersController.listAll",
        "UsersController.queryUserList",
    )
    assert (
        "UsersController.listAll" not in public_bindings["user.get"].source_operations
    )
    assert "UsersController.listUser" not in identity.source_operations
    assert {item.key for item in identity.type_closure} == {
        "org.apache.dolphinscheduler.dao.entity.User",
        "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO",
    }
    for action in ("create", "update", "generate"):
        consumer = f"access-token.{action}"
        clauses = runtime_bundles._COMPILED_OPERATION_DEPENDENCIES[consumer]
        assert isinstance(clauses, tuple)
        assert len(clauses) == 2
        previous, current = clauses
        assert previous.providers == ("user.get",)
        assert "3.4.3" not in previous.versions
        assert current.providers == ("user.identity",)
        assert current.versions == frozenset({"3.4.3"})
        assert set(identity.source_operations) <= set(
            public_bindings[consumer].source_operations
        )


def test_every_code_identity_runtime_profile_includes_project_lifecycle() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")

    for version in impact.PROJECT_CODE_IDENTITY_VERSIONS:
        operations = runtime_contract.runtime_semantic_operations(version)

        assert {
            "project.create",
            "project.delete",
            "project.get",
            "project.page",
            "project.update",
        }.issubset(operations)


def test_every_code_identity_version_has_an_exact_project_lifecycle_recipe() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    selector_and_get = (
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
    )

    assert tuple(impact.PROJECT_CODE_IDENTITY_VERSIONS) == (
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
    for version in impact.PROJECT_CODE_IDENTITY_VERSIONS:
        bindings = runtime_contract.runtime_operation_bindings(version)
        impact.validate_reviewed_bindings({version: bindings})

        expected_create = (
            (
                "ProjectController.createProject",
                "ProjectController.queryProjectCreatedAndAuthorizedByUser",
                "ProjectController.queryProjectByCode",
            )
            if version in {"2.0.0", "2.0.1"}
            else ("ProjectController.createProject",)
        )
        assert bindings["project.create"].source_operations == expected_create
        assert bindings["project.update"].source_operations == (
            *selector_and_get,
            "ProjectController.updateProject",
        )
        assert bindings["project.delete"].source_operations == (
            *selector_and_get,
            "ProjectController.deleteProject",
        )
        create_selector = bindings["project.create"].selector_semantics[0]
        assert create_selector.native_identity == "code"
        assert create_selector.resolution == (
            "create-locate-by-returned-id-and-exact-name-readback"
            if version in {"2.0.0", "2.0.1"}
            else "create-and-return-native-identity"
        )


def test_every_exact_version_uses_the_reviewed_main_schedule_controller() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")

    for version in impact.REVIEWED_DS_VERSIONS:
        version_key = tuple(map(int, version.split(".")))
        bindings = runtime_contract.runtime_operation_bindings(version)
        schedule_bindings = {
            name: binding
            for name, binding in bindings.items()
            if name.startswith("schedule.")
        }

        assert set(schedule_bindings) == {
            "schedule.create",
            "schedule.delete",
            "schedule.explain",
            "schedule.get",
            "schedule.offline",
            "schedule.online",
            "schedule.page",
            "schedule.preview",
            "schedule.update",
        }
        assert all(
            not operation.startswith("ScheduleV2Controller.")
            for binding in schedule_bindings.values()
            for operation in binding.source_operations
        )
        assert schedule_bindings["schedule.preview"].source_operations == (
            "SchedulerController.previewSchedule",
        )
        expected_offline = (
            "SchedulerController.offline"
            if version_key < (3, 1, 0)
            else "SchedulerController.offlineSchedule"
        )
        expected_online = (
            "SchedulerController.online"
            if version_key < (3, 1, 0)
            else "SchedulerController.publishScheduleOnline"
        )
        assert expected_offline in (
            schedule_bindings["schedule.offline"].source_operations
        )
        assert expected_online in schedule_bindings["schedule.online"].source_operations
        preference_operation = (
            "ProjectPreferenceController.queryProjectPreferenceByProjectCode"
        )
        if version_key >= (3, 2, 0):
            assert preference_operation in (
                schedule_bindings["schedule.create"].source_operations
            )
            assert preference_operation in (
                schedule_bindings["schedule.explain"].source_operations
            )
            assert any(
                item.key == "org.apache.dolphinscheduler.dao.entity.ProjectPreference"
                for item in schedule_bindings["schedule.create"].type_closure
            )
        else:
            assert preference_operation not in {
                operation
                for binding in schedule_bindings.values()
                for operation in binding.source_operations
            }


def test_every_exact_version_has_the_exact_governance_semantic_slice() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    datasource_operations = (
        "datasource.create",
        "datasource.delete",
        "datasource.get",
        "datasource.page",
        "datasource.saved-test",
        "datasource.update",
    )
    namespace_operations = (
        "namespace.available",
        "namespace.create",
        "namespace.delete",
        "namespace.get",
        "namespace.page",
    )

    assert len((*datasource_operations, *namespace_operations)) == 11
    for version in impact.REVIEWED_DS_VERSIONS:
        version_key = tuple(map(int, version.split(".")))
        governance_operations = tuple(
            operation
            for operation in runtime_contract.runtime_semantic_operations(version)
            if operation.startswith(("datasource.", "namespace."))
        )

        assert governance_operations == (
            *datasource_operations,
            *(namespace_operations if version_key >= (3, 0, 0) else ()),
        )


def test_342_runtime_profile_binds_each_extra_operation_to_exact_wire_facts() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")

    bindings = runtime_contract.runtime_operation_bindings("3.4.2")

    impact.validate_reviewed_bindings({"3.4.2": bindings})

    assert bindings["identity.current"].source_operations == (
        "UsersController.getUserInfo",
    )
    assert bindings["project.create"].source_operations == (
        "ProjectController.createProject",
    )
    assert bindings["project.update"].source_operations == (
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
        "ProjectController.updateProject",
    )
    assert bindings["project.delete"].source_operations == (
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
        "ProjectController.deleteProject",
    )
    assert bindings["schedule.page"].source_operations == (
        "SchedulerController.queryScheduleListPaging",
    )
    assert bindings["task.get"].source_operations == (
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
        "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList",
        "WorkflowDefinitionController.queryWorkflowDefinitionByCode",
        "TaskDefinitionController.queryTaskDefinitionDetail",
    )
    assert bindings["task.update"].source_operations == (
        "ProjectController.queryProjectListPaging",
        "ProjectController.queryProjectByCode",
        "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList",
        "WorkflowDefinitionController.queryWorkflowDefinitionByCode",
        "TaskDefinitionController.queryTaskDefinitionDetail",
        "TaskDefinitionController.updateTaskWithUpstream",
    )
    inspection = runtime_contract.runtime_auxiliary_operation_bindings("3.4.2")[
        "workflow.inspect"
    ]
    assert inspection.source_operations == bindings["workflow.get"].source_operations
    assert "workflow.inspect" not in bindings
    assert (
        bindings["project.update"].selector_semantics
        == bindings["project.get"].selector_semantics
    )
    assert (
        bindings["project.delete"].selector_semantics
        == bindings["project.get"].selector_semantics
    )
    assert {
        (item.surface, item.key) for item in bindings["identity.current"].type_closure
    } == {
        ("enums", "org.apache.dolphinscheduler.common.enums.UserType"),
        ("models", "org.apache.dolphinscheduler.api.utils.Result"),
        ("models", "org.apache.dolphinscheduler.dao.entity.User"),
    }


def test_341_and_342_bind_task_definitions_from_each_exact_source() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")

    for version in ("3.4.1", "3.4.2"):
        bindings = runtime_contract.runtime_operation_bindings(version)

        assert bindings["task.get"].source_operations == (
            "ProjectController.queryProjectListPaging",
            "ProjectController.queryProjectByCode",
            "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList",
            "WorkflowDefinitionController.queryWorkflowDefinitionByCode",
            "TaskDefinitionController.queryTaskDefinitionDetail",
        )
        assert bindings["task.update"].source_operations == (
            "ProjectController.queryProjectListPaging",
            "ProjectController.queryProjectByCode",
            "WorkflowDefinitionController.queryWorkflowDefinitionSimpleList",
            "WorkflowDefinitionController.queryWorkflowDefinitionByCode",
            "TaskDefinitionController.queryTaskDefinitionDetail",
            "TaskDefinitionController.updateTaskWithUpstream",
        )
        task_selectors = {
            item.resource: item for item in bindings["task.update"].selector_semantics
        }
        assert set(task_selectors) == {"project", "workflow", "task"}
        assert task_selectors["task"].consumed_selectors == ("name", "code")
        assert task_selectors["task"].exposed_identities == ("name", "code")
        assert task_selectors["task"].native_identity == "code"
        assert task_selectors["task"].parent_identity == "workflow_code"
        assert task_selectors["task"].resolution == (
            "direct-native-or-exact-name-via-workflow-dag"
        )


@pytest.mark.source_rebuild
def test_342_exact_source_derives_the_minimal_task_wire_delta(
    tmp_path: Path,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    version_diff = _load_module("ds_codegen.version_diff")
    package_renderer = _load_module("ds_codegen.render.package.entrypoint")
    snapshot = version_diff.build_snapshot_from_ds_source(
        exact_contract_corpus.source_root("3.4.2")
    )
    bindings = {
        **runtime_contract.runtime_operation_bindings("3.4.2"),
        **runtime_contract.runtime_auxiliary_operation_bindings("3.4.2"),
    }
    semantic_operations = runtime_contract.runtime_semantic_operations("3.4.2")

    complete_slice = runtime_contract.configured_runtime_contract_slice(snapshot)
    baseline_slice = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {
            name: bindings[name]
            for name in semantic_operations
            if name not in {"task.get", "task.update"}
        },
        additional_type_refs={
            impact.WireTypeRef("enums", item.import_path) for item in snapshot.enums
        },
    )

    assert complete_slice.operation_count == baseline_slice.operation_count + 2
    assert complete_slice.model_count == baseline_slice.model_count + 1
    assert complete_slice.enum_count == baseline_slice.enum_count
    assert complete_slice.dto_count == baseline_slice.dto_count
    assert {item.operation_id for item in complete_slice.operations} - {
        item.operation_id for item in baseline_slice.operations
    } == {
        "TaskDefinitionController.queryTaskDefinitionDetail",
        "TaskDefinitionController.updateTaskWithUpstream",
    }
    assert {item.import_path for item in complete_slice.models} - {
        item.import_path for item in baseline_slice.models
    } == {"org.apache.dolphinscheduler.api.vo.TaskDefinitionVO"}
    assert {item.import_path for item in complete_slice.enums} == {
        item.import_path for item in baseline_slice.enums
    }
    assert complete_slice.dtos == baseline_slice.dtos

    task_update = next(
        item
        for item in complete_slice.operations
        if item.operation_id == "TaskDefinitionController.updateTaskWithUpstream"
    )
    assert task_update.http_method == "PUT"
    assert task_update.path == (
        "projects/{projectCode}/task-definition/{code}/with-upstream"
    )
    assert task_update.logical_return_type == "Long"
    assert [
        (item.binding, item.wire_name, item.required, item.hidden)
        for item in task_update.parameters
    ] == [
        ("request_attribute", "Constants.SESSION_USER", None, True),
        ("path_variable", "projectCode", True, False),
        ("path_variable", "code", True, False),
        ("request_param", "taskDefinitionJsonObj", True, False),
        ("request_param", "upstreamCodes", False, False),
    ]

    package_renderer.write_generated_package(complete_slice, tmp_path)
    task_operations_text = (
        tmp_path / "generated/versions/ds_3_4_2/api/operations/task_definition.py"
    ).read_text(encoding="utf-8")
    assert "def update_task_with_upstream(" in task_operations_text
    assert ") -> int:" in task_operations_text
    assert "TypeAdapter(StrictInt)" in task_operations_text
    assert "TypeAdapter(StrictInt | None)" not in task_operations_text


def test_runtime_binding_validation_requires_declared_selector_shape() -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    impact = _load_module("ds_codegen.compatibility_impact")
    bindings = runtime_contract.runtime_operation_bindings("3.4.2")
    identity = bindings["identity.current"]

    with pytest.raises(ValueError, match=r"resources must be exactly none"):
        impact.validate_reviewed_bindings(
            {
                "3.4.2": {
                    "identity.current": replace(
                        identity,
                        selector_semantics=bindings["project.get"].selector_semantics,
                    )
                }
            }
        )

    with pytest.raises(ValueError, match=r"has no semantic binding schema"):
        impact.validate_reviewed_bindings(
            {"3.4.2": {"unknown.action": identity}},
        )


def test_runtime_operation_bindings_reject_invalid_mutation_closure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    update_binding = runtime_contract.RUNTIME_OPERATION_BINDINGS["3.4.2"][
        "project.update"
    ]
    monkeypatch.setitem(
        runtime_contract.RUNTIME_OPERATION_BINDINGS["3.4.2"],
        "project.update",
        replace(
            update_binding,
            source_operations=(
                "ProjectController.queryProjectListPaging",
                "ProjectController.updateProject",
            ),
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"project\.update.*source_operations must contain at least 3",
    ):
        runtime_contract.runtime_operation_bindings("3.4.2")


def test_runtime_operation_bindings_reject_incomplete_200_create_recipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    create_binding = runtime_contract.RUNTIME_OPERATION_BINDINGS["2.0.0"][
        "project.create"
    ]
    monkeypatch.setitem(
        runtime_contract.RUNTIME_OPERATION_BINDINGS["2.0.0"],
        "project.create",
        replace(
            create_binding,
            source_operations=("ProjectController.createProject",),
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"2\.0\.0:project\.create.*exact reviewed project recipe",
    ):
        runtime_contract.runtime_operation_bindings("2.0.0")


def _operation(ir: Any, operation_id: str) -> Any:
    controller, method_name = operation_id.split(".", 1)
    return ir.OperationSpec(
        operation_id=operation_id,
        controller=controller,
        method_name=method_name,
        api_group="TEST",
        http_method="GET",
        path="test",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Result",
        inferred_return_type=None,
        logical_return_type="Project",
        response_projection="direct",
        parameters=[],
    )


def _model(ir: Any, import_path: str) -> Any:
    return ir.ModelSpec(
        name=import_path.rsplit(".", 1)[-1],
        import_path=import_path,
        kind="other_class",
        documentation=None,
        extends=None,
        fields=[],
    )


def _enum(ir: Any, import_path: str) -> Any:
    return ir.EnumSpec(
        name=import_path.rsplit(".", 1)[-1],
        import_path=import_path,
        documentation=None,
        fields=[],
        json_value_field=None,
        values=[],
    )


def _field(ir: Any, name: str, java_type: str) -> Any:
    return ir.DtoFieldSpec(
        name=name,
        java_type=java_type,
        wire_name=name,
        required=None,
        default_value=None,
        nullable=False,
        default_factory=None,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )
