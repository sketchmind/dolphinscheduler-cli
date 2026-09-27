from pathlib import Path

import pytest
import yaml
from tests.value_shape_assertions import assert_mapping as _mapping

from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.services.lint import lint_workflow_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import task_template_result, workflow_template_result


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
@pytest.mark.parametrize("with_schedule", [False, True])
def test_workflow_template_lints_for_every_exact_version(
    tmp_path: Path, ds_version: str, *, with_schedule: bool
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    result = workflow_template_result(with_schedule=with_schedule, catalog=catalog)
    data = _mapping(result.data)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    assert yaml_text.startswith(
        "# Workflow YAML template for `dsctl workflow create --file FILE`\n"
    )
    assert result.resolved["ds_version"] == ds_version
    assert result.resolved["with_schedule"] is with_schedule
    document = yaml.safe_load(yaml_text)
    assert [task["name"] for task in document["tasks"]] == ["extract", "load"]
    assert document["tasks"][0]["depends_on"] == []
    assert document["tasks"][1]["depends_on"] == ["extract"]
    assert document["workflow"]["global_params"] == {"bizdate": "${system.biz.date}"}
    for task in document["tasks"]:
        assert set(task) == {"name", "type", "command", "depends_on"}
        assert "${bizdate}" in task["command"]
    assert ("schedule" in document) is with_schedule
    path = tmp_path / "workflow.yaml"
    path.write_text(yaml_text, encoding="utf-8")

    lint = _mapping(lint_workflow_result(file=path, catalog=catalog).data)

    assert lint["valid"] is True
    summary = _mapping(lint["summary"])
    assert summary["taskCount"] == 2
    assert summary["edgeCount"] == 1
    assert summary["rootTasks"] == ["extract"]
    assert summary["leafTasks"] == ["load"]
    assert summary["taskTypeCounts"] == {"SHELL": 2}
    assert summary["hasSchedule"] is with_schedule
    assert summary["releaseState"] == ("ONLINE" if with_schedule else "OFFLINE")


@pytest.mark.parametrize(
    ("ds_version", "absent_fields", "present_fields"),
    [
        (
            "1.3.9",
            (
                "execution_type",
                "delay",
                "environment_code",
                "task_group_id",
                "task_group_priority",
                "cpu_quota",
                "memory_max",
                "timezone",
            ),
            (),
        ),
        *[
            (
                f"2.0.{patch}",
                (
                    "execution_type",
                    "task_group_id",
                    "task_group_priority",
                    "cpu_quota",
                    "memory_max",
                ),
                ("delay", "environment_code", "timezone"),
            )
            for patch in range(10)
        ],
        *[
            (
                f"3.0.{patch}",
                ("cpu_quota", "memory_max"),
                (
                    "execution_type",
                    "delay",
                    "environment_code",
                    "task_group_id",
                    "task_group_priority",
                    "timezone",
                ),
            )
            for patch in range(7)
        ],
        *[
            (
                version,
                (),
                (
                    "execution_type",
                    "delay",
                    "environment_code",
                    "task_group_id",
                    "task_group_priority",
                    "cpu_quota",
                    "memory_max",
                    "timezone",
                ),
            )
            for version in ("3.1.0", "3.1.9", "3.2.0", "3.3.1", "3.4.1", "3.4.2")
        ],
    ],
)
def test_workflow_and_task_templates_follow_historical_field_boundaries(
    ds_version: str, absent_fields: tuple[str, ...], present_fields: tuple[str, ...]
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    workflow_yaml = _mapping(
        workflow_template_result(with_schedule=True, catalog=catalog).data
    )["yaml"]
    task_yaml = _mapping(task_template_result("SHELL", catalog=catalog).data)["yaml"]
    assert isinstance(workflow_yaml, str)
    assert isinstance(task_yaml, str)
    yaml_text = f"{workflow_yaml}\n{task_yaml}"

    for field in absent_fields:
        assert f"{field}:" not in yaml_text
    for field in present_fields:
        assert f"{field}:" in yaml_text
    assert "# flag: NO" in yaml_text
    assert "# timeout_notify_strategy: WARN" in yaml_text
    if ds_version == "1.3.9":
        assert "server-local" in yaml_text.lower()
        assert "timezone" in yaml_text.lower()
    document = yaml.safe_load(workflow_yaml)
    assert document["schedule"]["enabled"] is False
    task = yaml.safe_load(task_yaml)
    assert task["worker_group"] == "default"
    assert task["priority"] == "MEDIUM"
    assert task["retry"] == {"times": 0, "interval": 0}
    assert task["timeout"] == 0
    for field in (
        "flag",
        "environment_code",
        "task_group_id",
        "task_group_priority",
        "timeout_notify_strategy",
        "cpu_quota",
        "memory_max",
    ):
        assert f"# {field}:" not in workflow_yaml
