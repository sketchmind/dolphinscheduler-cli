from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    empty_task_adapter,
)
from tests.support import make_profile
from tests.workflow_domain_fakes import install_workflow_domain_runtime

from dsctl.errors import ConflictError
from dsctl.output import require_json_object
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.mutation import load_workflow_edit_spec_or_error
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog


@dataclass(frozen=True)
class _PolicySchedule(FakeSchedule):
    policy: str | None = None
    has_missed_fire_policy: bool = True

    @property
    def missedFirePolicy(self) -> FakeEnumValue | None:  # noqa: N802
        return None if self.policy is None else FakeEnumValue(self.policy)


def _install(
    monkeypatch: pytest.MonkeyPatch, *, version: str, policy: str | None
) -> tuple[FakeWorkflowAdapter, FakeScheduleAdapter]:
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        project_name_value="analytics",
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    workflows = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[
                    FakeTaskDefinition(
                        code=201,
                        name="extract",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo extract"}',
                        worker_group_value="default",
                    )
                ],
                workflow_task_relation_list_value=[],
            )
        },
    )
    schedules = FakeScheduleAdapter(
        schedules=[
            _PolicySchedule(
                id=23,
                workflow_definition_code_value=101,
                project_code_value=7,
                start_time_value="2026-01-01 00:00:00",
                end_time_value="2027-12-31 23:59:59",
                timezone_id_value="UTC",
                crontab_value="0 0 2 * * ?",
                failure_strategy_value=FakeEnumValue("CONTINUE"),
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                release_state_value=FakeEnumValue("OFFLINE"),
                policy=policy,
                has_missed_fire_policy=version == "3.4.3",
            )
        ]
    )
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="analytics")]
        ),
        workflow_adapter=workflows,
        task_adapter=empty_task_adapter(),
        schedule_adapter=schedules,
        profile=make_profile(ds_version=version),
    )
    return workflows, schedules


@pytest.mark.parametrize(
    ("version", "policy"),
    [
        ("3.4.3", "SKIP_MISSED"),
        ("3.4.3", "FIRE_ONCE_NOW"),
        ("3.4.3", None),
        ("3.4.2", None),
    ],
)
def test_service_schedule_policy_survives_export_parse_and_unchanged_file_edit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, version: str, policy: str | None
) -> None:
    workflows, _ = _install(monkeypatch, version=version, policy=policy)
    described = workflow_service.describe_workflow_result(
        "daily-sync", project="analytics"
    )
    description = require_json_object(described.data, label="description")
    workflow = require_json_object(description["workflow"], label="workflow")
    schedule = require_json_object(workflow["schedule"], label="schedule")
    if version == "3.4.3":
        assert schedule["missedFirePolicy"] == policy
    else:
        assert "missedFirePolicy" not in schedule
    exported = workflow_service.export_workflow_yaml_result(
        "daily-sync", project="analytics"
    )
    yaml_text = require_json_object(exported.data, label="export")["yaml"]
    assert isinstance(yaml_text, str)
    file = tmp_path / "exported.yaml"
    file.write_text(yaml_text, encoding="utf-8")
    parsed = load_workflow_edit_spec_or_error(
        file, catalog=get_task_authoring_catalog(version)
    )
    assert parsed.schedule is not None
    assert parsed.schedule.missed_fire_policy == policy
    if policy is None:
        assert "missed_fire_policy:" not in yaml_text
    else:
        assert f"missed_fire_policy: {policy}" in yaml_text
    edited = workflow_service.edit_workflow_result(
        "daily-sync", project="analytics", file=file, dry_run=True
    )
    assert require_json_object(edited.data, label="edit")["no_change"] is True
    assert workflows.update_calls == []


@pytest.mark.parametrize("policy", ["SKIP_MISSED", "FIRE_ONCE_NOW"])
def test_service_exported_nondefault_schedule_policy_detects_concurrent_change(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, policy: str
) -> None:
    workflows, schedules = _install(monkeypatch, version="3.4.3", policy=policy)
    exported = workflow_service.export_workflow_yaml_result(
        "daily-sync", project="analytics"
    )
    yaml_text = require_json_object(exported.data, label="export")["yaml"]
    assert isinstance(yaml_text, str)
    file = tmp_path / "exported.yaml"
    file.write_text(yaml_text, encoding="utf-8")
    schedule = schedules.schedules[0]
    assert isinstance(schedule, _PolicySchedule)
    schedules.schedules[0] = replace(schedule, policy="FIRE_ALL_MISSED")
    with pytest.raises(ConflictError) as exc:
        workflow_service.edit_workflow_result(
            "daily-sync", project="analytics", file=file, dry_run=True
        )
    assert exc.value.details["mismatched_fields"] == ["missed_fire_policy"]
    assert exc.value.details["mismatches"] == {
        "missed_fire_policy": {"file": policy, "current": "FIRE_ALL_MISSED"}
    }
    assert exc.value.details["mutation_applied"] is False
    assert workflows.update_calls == []
