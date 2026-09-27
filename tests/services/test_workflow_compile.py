import json

import pytest

from dsctl.errors import ApiTransportError, UserInputError
from dsctl.models import WorkflowSpec
from dsctl.services._workflow.authoring import workflow_authoring_catalog_for_version
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.identity import WorkflowTaskIdentity


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


def test_prepared_workflow_compilation_materializes_preview_and_server_identities() -> (
    None
):
    prepared = prepare_preserved_workflow_update_compilation(
        _two_task_spec(),
        release_state="OFFLINE",
        active_task_identities={
            "extract": WorkflowTaskIdentity(code=201, version=3),
        },
        unavailable_task_identities=(WorkflowTaskIdentity(code=202, version=4),),
    )

    preview_payload = prepared.preview()
    server_payload = prepared.materialize([9_101])
    preview_definitions = json.loads(preview_payload["taskDefinitionJson"])
    server_definitions = json.loads(server_payload["taskDefinitionJson"])
    server_relations = json.loads(server_payload["taskRelationJson"])
    preview_identities = [
        (task["name"], task["code"], task["version"]) for task in preview_definitions
    ]
    server_identities = [
        (task["name"], task["code"], task["version"]) for task in server_definitions
    ]

    assert prepared.required_task_code_count == 1
    assert preview_identities[0] == ("extract", 201, 3)
    assert preview_identities[1][0] == "load"
    assert isinstance(preview_identities[1][1], int)
    assert preview_identities[1][1] > 0
    assert preview_identities[1][2] == 1
    assert prepared.preview() == preview_payload
    assert server_identities == [
        ("extract", 201, 3),
        ("load", 9_101, 1),
    ]
    assert [
        (relation["preTaskCode"], relation["postTaskCode"])
        for relation in server_relations
    ] == [(0, 201), (201, 9_101)]

    with pytest.raises(
        ApiTransportError,
        match="collided with an existing task code",
    ):
        prepared.materialize([202])


def test_prepared_workflow_compilation_rejects_codes_when_none_are_required() -> None:
    prepared = prepare_preserved_workflow_update_compilation(
        _two_task_spec(),
        release_state="OFFLINE",
        active_task_identities={
            "extract": WorkflowTaskIdentity(code=201, version=3),
            "load": WorkflowTaskIdentity(code=202, version=4),
        },
        unavailable_task_identities=(),
    )

    with pytest.raises(
        ApiTransportError,
        match="returned 1 task codes when 0 were required",
    ):
        prepared.materialize([9_101])


def test_prepare_workflow_compilation_rejects_invalid_graph() -> None:
    spec = _two_task_spec()
    spec.tasks[1].depends_on = ["missing"]

    with pytest.raises(UserInputError, match="unknown task 'missing'"):
        prepare_workflow_create_compilation(spec)


def test_typed_compilation_rejects_unknown_decimal_task_name_reference() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "switch-workflow"},
            "tasks": [
                {
                    "name": "route",
                    "type": "SWITCH",
                    "task_params": {"switchResult": {"nextNode": "999"}},
                },
                {"name": "load", "type": "SHELL", "command": "echo load"},
            ],
        }
    )

    with pytest.raises(UserInputError, match="unknown task '999'"):
        prepare_workflow_create_compilation(spec)


def test_conditions_predicates_contribute_predecessor_edges() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "conditions-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                },
                {
                    "name": "route",
                    "type": "CONDITIONS",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "AND",
                                    "dependItemList": [
                                        {"task": "extract", "status": "SUCCESS"}
                                    ],
                                }
                            ],
                        },
                        "conditionResult": {
                            "successNode": ["on-success"],
                            "failedNode": ["on-failed"],
                        },
                    },
                },
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
            ],
        }
    )

    prepared = prepare_workflow_create_compilation(spec)

    assert prepared.edges == (
        ("extract", "route"),
        ("route", "on-success"),
        ("route", "on-failed"),
    )


@pytest.mark.parametrize(
    ("ds_version", "legacy_wire", "string_refs"),
    [
        ("2.0.0", True, True),
        ("3.2.2", True, False),
        ("3.4.1", False, False),
    ],
)
def test_prepared_compilation_projects_canonical_logic_tasks_to_exact_wire(
    ds_version: str,
    *,
    legacy_wire: bool,
    string_refs: bool,
) -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "exact-task-wire"},
            "tasks": [
                {"name": "upstream", "type": "SHELL", "command": "echo up"},
                {
                    "name": "switch",
                    "type": "SWITCH",
                    "task_params": {
                        "switchResult": {
                            "dependTaskList": [
                                {"condition": "true", "nextNode": "success"}
                            ],
                            "nextNode": "failed",
                        }
                    },
                },
                {
                    "name": "conditions",
                    "type": "CONDITIONS",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "AND",
                                    "dependItemList": [
                                        {"task": "upstream", "status": "SUCCESS"}
                                    ],
                                }
                            ],
                        },
                        "conditionResult": {
                            "successNode": ["success"],
                            "failedNode": ["failed"],
                        },
                    },
                },
                {
                    "name": "dependent",
                    "type": "DEPENDENT",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "AND",
                                    "dependItemList": [
                                        {
                                            "dependentType": "DEPENDENT_ON_WORKFLOW",
                                            "projectCode": 7001,
                                            "definitionCode": 8001,
                                            "depTaskCode": 0,
                                            "cycle": "day",
                                            "dateValue": "today",
                                        }
                                    ],
                                }
                            ],
                        }
                    },
                },
                {
                    "name": "http",
                    "type": "HTTP",
                    "task_params": {
                        "url": "https://example.test/health",
                        "httpMethod": "GET",
                        "httpParams": [],
                        "httpCheckCondition": "STATUS_CODE_DEFAULT",
                        "connectTimeout": 60000,
                    },
                },
                {
                    "name": "child",
                    "type": "SUB_WORKFLOW",
                    "task_params": {"workflowDefinitionCode": 9001},
                },
                {"name": "success", "type": "SHELL", "command": "echo ok"},
                {"name": "failed", "type": "SHELL", "command": "echo no"},
            ],
        }
    )
    allocated_codes = list(range(9_001, 9_009))

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=workflow_authoring_catalog_for_version(ds_version),
    ).materialize(allocated_codes)
    definitions = {
        definition["name"]: definition
        for definition in json.loads(payload["taskDefinitionJson"])
    }
    codes = {name: definition["code"] for name, definition in definitions.items()}

    switch_params = json.loads(definitions["switch"]["taskParams"])
    expected_success_ref: int | str = codes["success"]
    expected_failed_ref: int | str = codes["failed"]
    if string_refs:
        expected_success_ref = str(expected_success_ref)
        expected_failed_ref = str(expected_failed_ref)
    assert switch_params["switchResult"] == {
        "dependTaskList": [{"condition": "true", "nextNode": expected_success_ref}],
        "nextNode": expected_failed_ref,
    }

    conditions_params = json.loads(definitions["conditions"]["taskParams"])
    predicate = conditions_params["dependence"]["dependTaskList"][0]["dependItemList"][
        0
    ]
    assert predicate == {"depTaskCode": codes["upstream"], "status": "SUCCESS"}
    assert conditions_params["conditionResult"] == {
        "successNode": [expected_success_ref],
        "failedNode": [expected_failed_ref],
    }

    dependent_item = json.loads(definitions["dependent"]["taskParams"])["dependence"][
        "dependTaskList"
    ][0]["dependItemList"][0]
    assert ("dependentType" in dependent_item) is not legacy_wire

    http_params = json.loads(definitions["http"]["taskParams"])
    assert ("socketTimeout" in http_params) is legacy_wire
    if legacy_wire:
        assert http_params["socketTimeout"] == 60000

    child = definitions["child"]
    child_params = json.loads(child["taskParams"])
    assert child["taskType"] == ("SUB_PROCESS" if legacy_wire else "SUB_WORKFLOW")
    assert (
        child_params[
            "processDefinitionCode" if legacy_wire else "workflowDefinitionCode"
        ]
        == 9001
    )


def test_prepare_workflow_compilation_eagerly_validates_all_task_code_references() -> (
    None
):
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "switch-workflow"},
            "tasks": [
                {
                    "name": "route",
                    "type": "SWITCH",
                    "task_params": {
                        "switchResult": {"nextNode": "missing"},
                    },
                },
                {"name": "load", "type": "SHELL", "command": "echo load"},
            ],
        }
    )

    with pytest.raises(UserInputError, match="unknown task 'missing'"):
        prepare_workflow_create_compilation(spec)


def test_prepared_workflow_compilation_captures_specs_and_task_identities() -> None:
    spec = _two_task_spec()
    identities = {"extract": WorkflowTaskIdentity(code=201, version=3)}
    prepared = prepare_preserved_workflow_update_compilation(
        spec,
        release_state="OFFLINE",
        active_task_identities=identities,
        unavailable_task_identities=(),
    )

    spec.tasks[0].name = "changed-after-prepare"
    spec.tasks[0].command = "echo changed-after-prepare"
    identities.clear()
    payload = prepared.materialize([9_101])
    definitions = json.loads(payload["taskDefinitionJson"])

    assert [(task["name"], task["code"]) for task in definitions] == [
        ("extract", 201),
        ("load", 9_101),
    ]
    assert json.loads(definitions[0]["taskParams"])["rawScript"] == "echo extract"


def test_prepare_workflow_compilation_rejects_invalid_active_identities() -> None:
    with pytest.raises(ApiTransportError, match="unknown active task identity"):
        prepare_preserved_workflow_update_compilation(
            _two_task_spec(),
            release_state="OFFLINE",
            active_task_identities={
                "missing": WorkflowTaskIdentity(code=201, version=3),
            },
            unavailable_task_identities=(),
        )

    with pytest.raises(ApiTransportError, match="duplicate active task code"):
        prepare_preserved_workflow_update_compilation(
            _two_task_spec(),
            release_state="OFFLINE",
            active_task_identities={
                "extract": WorkflowTaskIdentity(code=201, version=3),
                "load": WorkflowTaskIdentity(code=201, version=4),
            },
            unavailable_task_identities=(),
        )

    with pytest.raises(ApiTransportError, match="both active and unavailable"):
        prepare_preserved_workflow_update_compilation(
            _two_task_spec(),
            release_state="OFFLINE",
            active_task_identities={
                "extract": WorkflowTaskIdentity(code=201, version=3),
            },
            unavailable_task_identities=(WorkflowTaskIdentity(code=201, version=3),),
        )


def test_prepared_workflow_compilation_binds_all_missing_task_codes() -> None:
    prepared = prepare_workflow_create_compilation(_two_task_spec())
    payload = prepared.materialize([9_001, 9_002])
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert prepared.required_task_code_count == 2
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
def test_prepared_workflow_compilation_rejects_invalid_allocated_task_codes(
    allocated_codes: list[int],
    task_identities: dict[str, WorkflowTaskIdentity] | None,
    message: str,
) -> None:
    prepared = (
        prepare_workflow_create_compilation(_two_task_spec())
        if task_identities is None
        else prepare_preserved_workflow_update_compilation(
            _two_task_spec(),
            release_state="OFFLINE",
            active_task_identities=task_identities,
            unavailable_task_identities=(),
        )
    )
    with pytest.raises(ApiTransportError, match=message):
        prepared.materialize(allocated_codes)


def test_prepared_workflow_compilation_requires_no_codes_when_all_exist() -> None:
    prepared = prepare_preserved_workflow_update_compilation(
        _two_task_spec(),
        release_state="OFFLINE",
        active_task_identities={
            "extract": WorkflowTaskIdentity(code=201, version=3),
            "load": WorkflowTaskIdentity(code=202, version=4),
        },
        unavailable_task_identities=(),
    )
    payload = prepared.materialize([])
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert prepared.required_task_code_count == 0
    assert [(task["code"], task["version"]) for task in task_definitions] == [
        (201, 3),
        (202, 4),
    ]


@pytest.mark.parametrize(
    ("ds_version", "expected_cache"),
    [
        ("1.3.9", None),
        ("3.1.9", None),
        ("3.2.0", "NO"),
        ("3.2.1", "NO"),
        ("3.2.2", "NO"),
        ("3.3.1", None),
    ],
)
def test_prepared_workflow_compilation_projects_exact_task_cache_create_epoch(
    ds_version: str,
    expected_cache: str | None,
) -> None:
    prepared = prepare_workflow_create_compilation(
        _two_task_spec(),
        catalog=workflow_authoring_catalog_for_version(ds_version),
    )
    payload = prepared.materialize([9_001, 9_002])
    task_definitions = json.loads(payload["taskDefinitionJson"])

    if expected_cache is None:
        assert all("isCache" not in task for task in task_definitions)
    else:
        assert [task["isCache"] for task in task_definitions] == [
            expected_cache,
            expected_cache,
        ]


def test_prepared_workflow_compilation_preserves_live_task_cache() -> None:
    prepared = prepare_preserved_workflow_update_compilation(
        _two_task_spec(),
        release_state="OFFLINE",
        active_task_identities={
            "extract": WorkflowTaskIdentity(code=201, version=3, is_cache="YES"),
            "load": WorkflowTaskIdentity(code=202, version=4, is_cache="NO"),
        },
        unavailable_task_identities=(),
        catalog=workflow_authoring_catalog_for_version("3.2.0"),
    )
    payload = prepared.materialize([])
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert [(task["name"], task["isCache"]) for task in task_definitions] == [
        ("extract", "YES"),
        ("load", "NO"),
    ]


def test_prepare_workflow_compilation_rejects_missing_live_task_cache() -> None:
    with pytest.raises(ApiTransportError, match="cache state"):
        prepare_preserved_workflow_update_compilation(
            _two_task_spec(),
            release_state="OFFLINE",
            active_task_identities={
                "extract": WorkflowTaskIdentity(code=201, version=3),
                "load": WorkflowTaskIdentity(code=202, version=4),
            },
            unavailable_task_identities=(),
            catalog=workflow_authoring_catalog_for_version("3.2.0"),
        )
