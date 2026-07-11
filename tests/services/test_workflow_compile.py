import json

import pytest

from dsctl.errors import ApiTransportError
from dsctl.models import WorkflowSpec
from dsctl.services._workflow_compile import compile_workflow_create_payload
from dsctl.services._workflow_identity import WorkflowTaskIdentity


def _two_task_spec() -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": "daily-sync"},
            "tasks": [
                {"name": "extract", "type": "SHELL", "command": "echo extract"},
                {
                    "name": "load",
                    "type": "SHELL",
                    "command": "echo load",
                    "depends_on": ["extract"],
                },
            ],
        }
    )


def test_workflow_compiler_allocates_all_missing_task_codes_once() -> None:
    spec = _two_task_spec()
    requested_counts: list[int] = []

    def allocate_task_codes(count: int) -> list[int]:
        requested_counts.append(count)
        return [9_001, 9_002]

    payload = compile_workflow_create_payload(
        spec,
        allocate_task_codes=allocate_task_codes,
    )
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert requested_counts == [2]
    assert [task["code"] for task in task_definitions] == [9_001, 9_002]


@pytest.mark.parametrize(
    ("allocated_codes", "task_identities", "message"),
    [
        ([], None, "returned 0 task codes when 2 were required"),
        ([9_001, 0], None, "must be positive integers"),
        ([9_001, True], None, "must be positive integers"),
        ([9_001, 9_001], None, "contained duplicate task codes"),
        (
            [9_001],
            {"extract": WorkflowTaskIdentity(code=9_001, version=1)},
            "collided with an existing task code",
        ),
    ],
)
def test_workflow_compiler_rejects_invalid_allocated_task_codes(
    allocated_codes: list[int],
    task_identities: dict[str, WorkflowTaskIdentity] | None,
    message: str,
) -> None:
    with pytest.raises(ApiTransportError, match=message):
        compile_workflow_create_payload(
            _two_task_spec(),
            allocate_task_codes=lambda _count: allocated_codes,
            task_identities=task_identities,
        )


def test_workflow_compiler_does_not_allocate_when_all_task_codes_exist() -> None:
    def unexpected_allocation(_count: int) -> list[int]:
        message = "task code allocator must not be called"
        raise AssertionError(message)

    payload = compile_workflow_create_payload(
        _two_task_spec(),
        allocate_task_codes=unexpected_allocation,
        task_identities={
            "extract": WorkflowTaskIdentity(code=201, version=3),
            "load": WorkflowTaskIdentity(code=202, version=4),
        },
    )
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert [(task["code"], task["version"]) for task in task_definitions] == [
        (201, 3),
        (202, 4),
    ]
