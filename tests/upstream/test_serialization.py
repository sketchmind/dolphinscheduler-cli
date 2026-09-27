from __future__ import annotations

import pytest

from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.serialization import serialize_task
from dsctl.upstream.task_definition_wire import project_exact_task_payload


@pytest.mark.parametrize(
    (
        "task",
        "ds_version",
        "project_code",
        "task_code",
        "expected_group",
        "expected_resource",
    ),
    [
        (
            {"code": 200, "projectCode": 20, "version": 1},
            "2.0.0",
            20,
            200,
            (0, 0),
            (None, None, None),
        ),
        (
            {"code": 209, "projectCode": 20, "version": 1},
            "2.0.9",
            20,
            209,
            (0, 0),
            (None, None, None),
        ),
        (
            {
                "code": 300,
                "projectCode": 30,
                "version": 1,
                "taskGroupId": 12,
                "taskGroupPriority": 3,
            },
            "3.0.0",
            30,
            300,
            (12, 3),
            (None, None, None),
        ),
        (
            {
                "code": 306,
                "projectCode": 30,
                "version": 1,
                "taskGroupId": 14,
                "taskGroupPriority": 5,
            },
            "3.0.6",
            30,
            306,
            (14, 5),
            (None, None, None),
        ),
        (
            {
                "code": 310,
                "projectCode": 31,
                "version": 1,
                "taskGroupId": 16,
                "taskGroupPriority": 7,
                "taskExecuteType": "STREAM",
                "cpuQuota": 8,
                "memoryMax": 1024,
            },
            "3.1.0",
            31,
            310,
            (16, 7),
            ("STREAM", 8, 1024),
        ),
    ],
)
def test_serialize_task_projects_fields_absent_from_older_exact_models(
    task: object,
    ds_version: str,
    project_code: int,
    task_code: int,
    expected_group: tuple[int, int],
    expected_resource: tuple[str | None, int | None, int | None],
) -> None:
    adapter = (
        WORKFLOW_PROGRAMS.profile(ds_version).program("task_get").codec.response_adapter
    )
    assert adapter is not None
    canonical = project_exact_task_payload(
        adapter.validate_python(task),
        ds_version=ds_version,
        expected_project_code=project_code,
        expected_task_code=task_code,
    )
    payload = serialize_task(canonical)

    assert (payload["taskGroupId"], payload["taskGroupPriority"]) == expected_group
    assert (
        payload["taskExecuteType"],
        payload["cpuQuota"],
        payload["memoryMax"],
    ) == expected_resource
