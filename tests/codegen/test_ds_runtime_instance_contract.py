from __future__ import annotations

import importlib
import json
import runpy
import shutil
import sys
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.ir import ContractSnapshot

REPO_ROOT = Path(__file__).resolve().parents[2]
TRACKED_RUNTIME_PROFILE = REPO_ROOT / "src/dsctl/generated/runtime_instance_profiles.py"


def _contract() -> Any:
    tools_dir = REPO_ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.runtime_instance_contract")


def test_runtime_instance_contract_is_exact_and_complete() -> None:
    contract = _contract()

    assert tuple(contract.RUNTIME_INSTANCE_CONTRACTS) == (
        contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    )
    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        assert contract.runtime_instance_contract(version)

    with pytest.raises(ValueError, match="no reviewed runtime-instance contract"):
        contract.runtime_instance_contract("3.4.4")


def test_schedule_environment_runtime_facts_have_exact_membership() -> None:
    contract = _contract()
    forwards = {
        version
        for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS
        if contract.runtime_instance_contract(version).schedule_forwards_environment
    }
    inherits = {
        version
        for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS
        if contract.runtime_instance_contract(
            version
        ).task_inherits_workflow_environment
    }
    assert len(forwards) == 21
    assert len(inherits) == 7
    assert forwards == (
        {"3.0.3", "3.0.4", "3.0.5", "3.0.6"}
        | {f"3.1.{minor}" for minor in range(2, 10)}
        | {"3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.3.2"}
        | {f"3.4.{minor}" for minor in range(4)}
    )
    assert inherits == {"3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
    assert "1.3.9" not in contract.SCHEDULE_ENVIRONMENT_FACTS


@pytest.mark.source_contract
def test_schedule_environment_facts_match_exact_quartz_and_master_source(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()
    contract.validate_schedule_environment_sources(
        {
            version: exact_contract_corpus.source_root(version)
            for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS
        }
    )


@pytest.mark.source_contract
def test_schedule_environment_validation_rejects_changed_empty_code_semantics(
    exact_contract_corpus: ExactContractCorpus,
    tmp_path: Path,
) -> None:
    contract = _contract()
    version = "3.2.2"
    row = contract.SCHEDULE_ENVIRONMENT_FACTS[version]
    source_root = exact_contract_corpus.source_root(version)
    for field in ("scheduler_source", "task_source", "utility_source"):
        relative = Path(row[field])
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_root / relative, target)
    utility = tmp_path / row["utility_source"]
    source = utility.read_text(encoding="utf-8")
    assert "environmentCode <= 0" in source
    utility.write_text(
        source.replace("environmentCode <= 0", "environmentCode < 0"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="empty environment utility source changed"):
        contract.validate_schedule_environment_sources({version: tmp_path})


def test_schedule_environment_review_rejects_non_boolean_facts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = _contract()
    document = json.loads(contract.SCHEDULE_ENVIRONMENT_FACTS_PATH.read_text())
    document["records"][0]["schedule_forwards_environment"] = "false"
    path = tmp_path / "schedule_environment_facts.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setattr(contract, "SCHEDULE_ENVIRONMENT_FACTS_PATH", path)
    with pytest.raises(TypeError, match="must be a reviewed boolean"):
        contract._schedule_environment_facts()


def test_stop_result_uncertainty_is_exact_and_action_specific() -> None:
    contract = _contract()
    expected_versions = {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }

    assert tuple(contract.STOP_RESULT_MAY_BE_UNKNOWN_BY_VERSION) == (
        contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    )
    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        expected = version in expected_versions
        assert contract.STOP_RESULT_MAY_BE_UNKNOWN_BY_VERSION[version] is expected
        assert (
            contract.runtime_instance_contract(version).stop_result_may_be_unknown
            is expected
        )


@pytest.mark.source_contract
def test_stop_result_uncertainty_matches_exact_control_ordering(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    legacy_path = (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "executor/workflow/instance/stop/StopExecuteFunction.java"
    )
    for version in ("3.2.0", "3.2.1", "3.2.2"):
        source = exact_contract_corpus.source_file(version, legacy_path).read_text(
            encoding="utf-8"
        )
        rpc_marker = (
            "apiRpcClient.send("
            if version == "3.2.0"
            else ".onWorkflowInstanceInstanceStateChange("
        )
        assert source.index("processInstanceDao.updateById(workflowInstance)") < (
            source.index(rpc_marker)
        )
        assert "WorkflowExecutionStatus.READY_STOP" in source
        assert "workflow instance status failed" in source.casefold()

    modern_path = (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "executor/workflow/StopWorkflowInstanceExecutorDelegate.java"
    )
    for version in (
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ):
        source = exact_contract_corpus.source_file(version, modern_path).read_text(
            encoding="utf-8"
        )
        assert "directStopInDB(workflowInstance)" in source
        assert "stopInMaster(workflowInstance)" in source
        assert ".stopWorkflowInstance(" in source
        assert "WorkflowExecutionStatus.STOP" in source


def test_task_log_wire_epochs_are_the_single_source_of_wire_bindings() -> None:
    contract = _contract()
    epoch_versions = tuple(
        version
        for epoch in contract.TASK_LOG_WIRE_EPOCH_CONTRACTS.values()
        for version in epoch.members
    )

    assert epoch_versions == contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    profiles = contract.runtime_instance_profile_data()["profiles"]
    assert isinstance(profiles, dict)
    for epoch_name, epoch in contract.TASK_LOG_WIRE_EPOCH_CONTRACTS.items():
        for version in epoch.members:
            recipe = contract.runtime_instance_contract(version)
            profile = profiles[version]
            assert isinstance(profile, dict)
            assert recipe.task_log_wire_epoch == epoch_name
            assert recipe.log_operation == epoch.source_operation
            assert recipe.log_shape == epoch.response_shape
            assert profile["log_epoch"] == epoch_name
            assert "log_shape" not in profile


def test_task_log_first_page_header_is_an_exact_runtime_fact() -> None:
    contract = _contract()
    facts = contract.TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION

    assert tuple(facts) == contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    # DS 1.3.9 LoggerService.java:64-80 returns rollViewLog directly; DS 2.0.0
    # LoggerServiceImpl.java:98-108 prepends LOG_HEAD_FORMAT on the first page.
    assert facts["1.3.9"] is False
    assert all(facts[version] is True for version in tuple(facts)[1:])
    assert (
        contract.runtime_instance_contract("1.3.9").task_log_wire_epoch
        == contract.runtime_instance_contract("2.0.0").task_log_wire_epoch
    )
    assert (
        contract.runtime_instance_contract("1.3.9").log_first_page_header
        is not contract.runtime_instance_contract("2.0.0").log_first_page_header
    )


@pytest.mark.source_contract
def test_task_log_runtime_facts_match_exact_sources(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()
    renderer_epochs = (
        (
            "dolphinscheduler-server/src/main/java/org/apache/dolphinscheduler/"
            "server/log/LoggerRequestProcessor.java",
            False,
            (
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
            ),
        ),
        (
            "dolphinscheduler-log-server/src/main/java/org/apache/dolphinscheduler/"
            "server/log/LoggerRequestProcessor.java",
            True,
            (
                "3.0.0",
                "3.0.1",
                "3.0.2",
                "3.0.3",
                "3.0.4",
                "3.0.5",
                "3.0.6",
                "3.1.0",
            ),
        ),
        (
            "dolphinscheduler-service/src/main/java/org/apache/dolphinscheduler/"
            "service/log/LoggerRequestProcessor.java",
            True,
            (
                "3.1.1",
                "3.1.2",
                "3.1.3",
                "3.1.4",
                "3.1.5",
                "3.1.6",
                "3.1.7",
                "3.1.8",
                "3.1.9",
            ),
        ),
        (
            "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
            "common/utils/LogUtils.java",
            True,
            (
                "3.2.0",
                "3.2.1",
                "3.2.2",
                "3.3.1",
                "3.3.2",
                "3.4.0",
                "3.4.1",
                "3.4.2",
                "3.4.3",
            ),
        ),
    )
    assert (
        tuple(version for _, _, members in renderer_epochs for version in members)
        == contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    )
    renderer_by_version = {
        version: (path, is_size_capped)
        for path, is_size_capped, members in renderer_epochs
        for version in members
    }
    assert len(renderer_by_version) == len(contract.TARGET_RUNTIME_INSTANCE_VERSIONS)

    for version, has_header in contract.TASK_LOG_FIRST_PAGE_HEADER_BY_VERSION.items():
        if version == "1.3.9":
            service_path = (
                "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
                "api/service/LoggerService.java"
            )
        else:
            service_path = (
                "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
                "api/service/impl/LoggerServiceImpl.java"
            )
        service = exact_contract_corpus.source_file(
            version,
            service_path,
        ).read_text(encoding="utf-8")
        assert ("LOG_HEAD_FORMAT" in service) is has_header
        assert ("if (skipLineNum == 0)" in service) is has_header

        renderer_path, is_size_capped = renderer_by_version[version]
        renderer = exact_contract_corpus.source_file(
            version,
            renderer_path,
        ).read_text(encoding="utf-8")
        assert '"\\r\\n"' in renderer
        assert ("MaxResponseLogSize = 65535" in renderer) is is_size_capped
        if is_size_capped:
            assert "totalLogByteSize += lineByteSize" in renderer
            assert "if (totalLogByteSize >= MaxResponseLogSize)" in renderer


def test_generated_runtime_profile_is_the_exact_contract_projection() -> None:
    contract = _contract()
    projected = contract.runtime_instance_profile_data()

    assert TRACKED_RUNTIME_PROFILE.read_text(encoding="utf-8") == (
        contract.render_runtime_instance_profiles(projected)
    )
    namespace = runpy.run_path(str(TRACKED_RUNTIME_PROFILE))
    profiles = namespace["RUNTIME_INSTANCE_PROFILES"]
    assert namespace["RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION"] == (
        contract.RUNTIME_INSTANCE_PROFILE_SCHEMA_VERSION
    )
    assert tuple(profiles) == contract.TARGET_RUNTIME_INSTANCE_VERSIONS
    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        assert asdict(profiles[version]) == projected["profiles"][version]


@pytest.mark.source_contract
def test_every_runtime_instance_source_type_and_enum_exists(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        snapshot: ContractSnapshot = exact_contract_corpus.snapshot(version)
        operation_ids = {operation.operation_id for operation in snapshot.operations}
        type_keys = (
            {model.import_path for model in snapshot.models}
            | {dto.import_path for dto in snapshot.dtos}
            | {enum.import_path for enum in snapshot.enums}
        )
        sources = contract.semantic_operation_sources(version)
        type_roots = contract.semantic_operation_type_roots(version)
        enum_roots = contract.semantic_operation_enum_roots(version)

        assert type_roots.keys() == sources.keys()
        assert enum_roots.keys() == sources.keys()
        for action, required_sources in sources.items():
            assert set(required_sources) <= operation_ids, (version, action)
            assert set(type_roots[action]) <= type_keys, (version, action)
            assert set(enum_roots[action]) <= type_keys, (version, action)
            assert all("V2Controller" not in item for item in required_sources)


def test_runtime_instance_terminal_boundaries_are_explicit() -> None:
    contract = _contract()

    one_three = contract.terminal_decisions("1.3.9")
    assert set(one_three) == {
        "workflow-instance.execute-task",
        "task-instance.force-success",
        "task-instance.savepoint",
        "task-instance.stop",
    }
    assert one_three["task-instance.stop"].reason == ("upstream_capability_absent")

    assert set(contract.terminal_decisions("2.0.0")) == {
        "workflow-instance.execute-task",
        "task-instance.savepoint",
        "task-instance.stop",
    }
    assert set(contract.terminal_decisions("3.1.9")) == {
        "workflow-instance.execute-task"
    }
    assert contract.terminal_decisions("3.2.0") == {}
    assert contract.terminal_decisions("3.4.2") == {}


def test_runtime_instance_wire_transitions_match_reviewed_boundaries() -> None:
    contract = _contract()

    one_three = contract.runtime_instance_contract("1.3.9")
    two_zero = contract.runtime_instance_contract("2.0.0")
    three_zero = contract.runtime_instance_contract("3.0.0")
    three_one = contract.runtime_instance_contract("3.1.0")
    three_two = contract.runtime_instance_contract("3.2.0")
    three_three = contract.runtime_instance_contract("3.3.1")
    three_four_two = contract.runtime_instance_contract("3.4.2")

    assert (one_three.project_identity, one_three.definition_identity) == (
        "id",
        "id",
    )
    assert one_three.project_route == "name"
    assert one_three.update_shape == "legacy-process-data"
    assert contract.action_support("1.3.9")["workflow-instance.export"] == ("supported")
    assert contract.action_support("1.3.9")["workflow-instance.edit"] == "supported"
    assert two_zero.update_shape == "modern-with-tenant"
    assert three_zero.task_log_wire_epoch == "log-detail-record"
    assert three_one.savepoint is True
    assert three_two.execute_task is True
    assert three_three.workflow_controller == "WorkflowInstanceController"
    assert all(
        contract.runtime_instance_contract(version).workflow_sub_result_key
        == "subProcessInstanceId"
        for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS[
            : contract.TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.3.1")
        ]
    )
    assert all(
        contract.runtime_instance_contract(version).workflow_sub_result_key
        == "subWorkflowInstanceId"
        for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS[
            contract.TARGET_RUNTIME_INSTANCE_VERSIONS.index("3.3.1") :
        ]
    )
    assert three_three.control_operation.endswith("controlWorkflowInstance")
    assert three_four_two.log_operation == "LoggerController.queryLog"


def test_runtime_instance_local_compositions_declare_full_remote_closure() -> None:
    contract = _contract()

    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        assert sources["workflow-instance.watch"] == sources["workflow-instance.get"]
        assert (
            "TaskInstanceController.queryTaskListPaging"
            in sources["workflow-instance.digest"]
        )
        assert sources["task-instance.watch"] == sources["task-instance.get"]
        assert (
            contract.runtime_instance_contract(version).workflow_get_operation
            in (sources["task-instance.list"])
        )
        assert all(
            operation.startswith("LoggerController.")
            for operation in sources["task-instance.log"]
        )
        log_roots = contract.semantic_operation_type_roots(version)["task-instance.log"]
        assert contract._PROJECT_MODEL not in log_roots
        assert contract._PAGE_INFO_MODEL not in log_roots
        assert contract.runtime_instance_contract(version).workflow_model not in (
            log_roots
        )
        assert contract._TASK_INSTANCE_MODEL not in log_roots
        assert (
            contract.semantic_operation_enum_roots(version)["task-instance.log"] == ()
        )

    legacy_sources = contract.semantic_operation_sources("1.3.9")
    assert (
        legacy_sources["workflow-instance.export"]
        == legacy_sources["workflow-instance.get"]
    )
    assert (
        "ProcessInstanceController.updateProcessInstance"
        in legacy_sources["workflow-instance.edit"]
    )
    assert (
        "TaskDefinitionController.genTaskCodeList"
        not in legacy_sources["workflow-instance.edit"]
    )


@pytest.mark.source_contract
def test_runtime_instance_evidence_and_facets_close_over_actions(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()

    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        sources = contract.semantic_operation_sources(version)
        evidence = contract.semantic_operation_evidence(version)
        facets = contract.semantic_operation_facets(version)
        assert evidence.keys() == sources.keys()
        assert facets.keys() == sources.keys()
        for action_evidence in evidence.values():
            assert {item.kind for item in action_evidence} == {
                "controller",
                "ui",
            }
            for item in action_evidence:
                assert item.version == version
                assert exact_contract_corpus.source_file(
                    version,
                    item.source,
                ).is_file()
        assert all(item["v2_route"] is False for item in facets.values())


def test_summary_and_detail_instances_keep_independent_exact_native_roots() -> None:
    contract = _contract()
    current = contract.runtime_instance_contract("3.4.3")
    assert (
        current.workflow_summary_model
        == "org.apache.dolphinscheduler.api.vo.WorkflowInstanceSummaryVO"
    )
    assert (
        current.workflow_model
        == "org.apache.dolphinscheduler.dao.entity.WorkflowInstance"
    )
    assert (
        current.workflow_summary_model
        in contract.semantic_operation_type_roots("3.4.3")["workflow-instance.list"]
    )
    assert (
        current.workflow_summary_model
        not in contract.semantic_operation_type_roots("3.4.3")[
            "workflow-instance.export"
        ]
    )
    for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS:
        recipe = contract.runtime_instance_contract(version)
        assert (recipe.workflow_summary_model is not None) == (version == "3.4.3")


def test_instance_dag_sync_requirement_has_exact_membership() -> None:
    contract = _contract()
    assert {
        version
        for version in contract.TARGET_RUNTIME_INSTANCE_VERSIONS
        if contract.runtime_instance_contract(version).instance_dag_edit_requires_sync
    } == {"2.0.0", "2.0.1", "2.0.2"}


@pytest.mark.source_contract
@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2", "2.0.3"])
def test_instance_dag_sync_requirement_matches_exact_source(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    source = exact_contract_corpus.source_file(
        version,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/ProcessInstanceServiceImpl.java",
    ).read_text(encoding="utf-8")
    method = source.split("public Map<String, Object> updateProcessInstance(", 1)[1]
    method = method.split("private void setProcessInstance(", 1)[0]
    if version == "2.0.3":
        assert "if (Boolean.TRUE.equals(syncDefine))" not in method
        assert (
            "saveTaskDefine(loginUser, projectCode, taskDefinitionLogs, syncDefine)"
            in method
        )
    else:
        conditional = method.split("if (Boolean.TRUE.equals(syncDefine)) {", 1)[1]
        conditional = conditional.split(
            "int update = processService.updateProcessInstance", 1
        )[0]
        assert (
            "saveTaskDefine(loginUser, projectCode, taskDefinitionLogs)" in conditional
        )
        assert "result.put(Constants.DATA_LIST, processDefinition)" in conditional
        assert (
            "processInstance.setProcessDefinitionVersion(insertVersion)" in conditional
        )
        assert "saveProcessDefine(loginUser, processDefinition, false)" in conditional
        process = exact_contract_corpus.source_file(
            version,
            "dolphinscheduler-service/src/main/java/org/apache/dolphinscheduler/service/process/ProcessService.java",
        ).read_text(encoding="utf-8")
        assert (
            "setReleaseState(isFromProcessDefine ? ReleaseState.OFFLINE "
            ": ReleaseState.ONLINE)" in process
        )


@pytest.mark.source_contract
def test_instance_update_nullable_return_has_only_three_exact_members(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    nullable = set()
    for version in exact_contract_corpus.versions:
        for operation in exact_contract_corpus.snapshot(version).operations:
            if operation.operation_id in {
                "ProcessInstanceController.updateProcessInstance",
                "WorkflowInstanceController.updateWorkflowInstance",
            } and operation.logical_return_type.startswith("Optional<"):
                nullable.add(version)
                assert operation.logical_return_type == "Optional<ProcessDefinition>"
    assert nullable == {"2.0.0", "2.0.1", "2.0.2"}


@pytest.mark.source_contract
def test_modern_instance_scalars_are_separate_from_definition_dag_in_exact_source(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    contract = _contract()
    for version in exact_contract_corpus.versions:
        if version == "1.3.9":
            continue
        recipe = contract.runtime_instance_contract(version)
        family = (
            "Workflow"
            if recipe.workflow_controller == "WorkflowInstanceController"
            else "Process"
        )
        service = exact_contract_corpus.source_file(
            version,
            "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/"
            f"{family}InstanceServiceImpl.java",
        ).read_text(encoding="utf-8")
        entity = exact_contract_corpus.source_file(
            version,
            "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/dao/entity/"
            f"{family}Instance.java",
        ).read_text(encoding="utf-8")
        assert ".setDagData(processService.genDagData(" in service
        assert "private String globalParams" in entity
        assert "int timeout" in entity
