import json
from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeProjectAdapter,
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiTransportError, UserInputError
from dsctl.output import error_payload, success_payload
from dsctl.services import runtime as runtime_service
from dsctl.services import task_type as task_type_service
from dsctl.upstream import upstream_default_task_types


def _install_task_type_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeTaskTypeAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            task_type_adapter=adapter,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def _local_json_refs(value: object) -> list[str]:
    refs: list[str] = []
    if isinstance(value, Mapping):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/"):
            refs.append(ref)
        for nested in value.values():
            refs.extend(_local_json_refs(nested))
    elif isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for nested in value:
            refs.extend(_local_json_refs(nested))
    return refs


def _resolve_local_json_ref(schema: Mapping[str, object], ref: str) -> object:
    current: object = schema
    for raw_token in ref.removeprefix("#/").split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        assert isinstance(current, Mapping), ref
        assert token in current, ref
        current = current[token]
    return current


def test_list_task_types_result_returns_remote_payload_and_cli_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value="SHELL",
                is_collection_value=True,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="REMOTE_SHELL",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="CUSTOM_PLUGIN",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="SUB_WORKFLOW",
                is_collection_value=True,
                task_category_value="Logic",
            ),
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    result = task_type_service.list_task_types_result()
    data = _mapping(result.data)
    task_types = _sequence(data["taskTypes"])
    coverage = _mapping(data["cliCoverage"])

    assert result.resolved == {"source": "favourite/taskTypes"}
    assert data["count"] == 4
    assert list(task_types) == [
        {
            "taskType": "SHELL",
            "isCollection": True,
            "taskCategory": "Universal",
        },
        {
            "taskType": "REMOTE_SHELL",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "CUSTOM_PLUGIN",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "SUB_WORKFLOW",
            "isCollection": True,
            "taskCategory": "Logic",
        },
    ]
    assert data["taskTypesByCategory"] == {
        "Universal": ["SHELL", "REMOTE_SHELL", "CUSTOM_PLUGIN"],
        "Logic": ["SUB_WORKFLOW"],
    }
    assert "REMOTESHELL" in _sequence(coverage["taskTemplateTypes"])
    assert "SPARK" in _sequence(coverage["genericTaskTemplateTypes"])
    assert "CUSTOM_PLUGIN" in _sequence(coverage["untemplatedTaskTypes"])
    assert "REMOTE_SHELL" not in _sequence(coverage["untemplatedTaskTypes"])


def test_list_task_types_result_rejects_missing_required_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value=None,
                task_category_value="Universal",
            )
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    with pytest.raises(ApiTransportError, match="missing required field 'taskType'"):
        task_type_service.list_task_types_result()


def test_task_type_summary_result_describes_local_authoring_contract() -> None:
    result = task_type_service.task_type_summary_result("sql")
    data = _mapping(result.data)

    assert result.resolved == {"task_type": "SQL"}
    assert data["task_type"] == "SQL"
    assert data["template_command"] == "dsctl template task SQL"
    assert data["raw_template_command"] == "dsctl template task SQL --raw"
    assert "task_params.sql" in _sequence(data["required_paths"])
    rows = [_mapping(item) for item in _sequence(data["rows"])]
    commands = {row["name"]: row["command"] for row in rows}
    assert commands["schema"] == "dsctl task-type schema SQL"
    assert commands["json-schema"] == "dsctl task-type schema SQL --json-schema"
    assert commands["compile-mappings"] == (
        "dsctl task-type schema SQL --compile-mappings"
    )
    assert commands["full-schema"] == "dsctl task-type schema SQL --full"


def test_sub_workflow_summary_explains_child_parameter_inheritance() -> None:
    result = task_type_service.task_type_summary_result("SUB_WORKFLOW")
    data = _mapping(result.data)
    workflow_usage = _mapping(data["workflow_usage"])

    assert workflow_usage["child_parameters"] == (
        "Set values supplied by this parent in workflow.global_params or pass them "
        "when starting the parent. DS 3.4.1 forwards parent globals, startup "
        "parameters, and workflow-instance varPool. Define standalone fallbacks "
        "in the child workflow.global_params; SUB_WORKFLOW task localParams are "
        "not child inputs."
    )


def test_sub_workflow_local_params_schema_rejects_child_input_interpretation() -> None:
    result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.localParams[]",
    )
    fields = _sequence(_mapping(result.data)["fields"])

    assert len(fields) == 1
    field = _mapping(fields[0])
    assert field["description"] == (
        "DS-native task-local properties. In DS 3.4.1 they do not become child "
        "workflow inputs. Supply parent-specific values via the parent "
        "workflow.global_params or startup parameters."
    )
    assert "dsctl template params --topic context" in _sequence(
        field["related_commands"]
    )


def test_sub_workflow_schema_marks_unused_ds_fields_as_round_trip_only() -> None:
    local_value_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.localParams[].value",
    )
    var_pool_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.varPool[]",
    )
    resource_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.resourceList[].resourceName",
    )

    local_value = _mapping(_sequence(_mapping(local_value_result.data)["fields"])[0])
    var_pool = _mapping(_sequence(_mapping(var_pool_result.data)["fields"])[0])
    resource = _mapping(_sequence(_mapping(resource_result.data)["fields"])[0])
    assert "do not become child workflow inputs" in str(local_value["description"])
    assert "not the parent workflow-instance varPool" in str(var_pool["description"])
    assert "round-trip-only" in str(resource["description"])
    assert "choice_source" not in resource
    assert "related_commands" not in resource

    summary = _mapping(task_type_service.task_type_summary_result("SUB_WORKFLOW").data)
    choice_sources = _sequence(summary["choice_sources"])
    choice_paths = [_mapping(item)["path"] for item in choice_sources]
    assert "task_params.workflowDefinitionCode" in choice_paths
    assert not any("resourceList" in str(path) for path in choice_paths)


@pytest.mark.parametrize(
    (
        "task_type",
        "expected_payload_modes",
        "expected_required_paths",
        "expected_required_paths_by_payload_mode",
    ),
    [
        (
            "SHELL",
            ["command", "task_params"],
            ["name", "type"],
            {
                "command": ["command"],
                "task_params": ["task_params.rawScript"],
            },
        ),
        (
            "PYTHON",
            ["command", "task_params"],
            ["name", "type"],
            {
                "command": ["command"],
                "task_params": ["task_params.rawScript"],
            },
        ),
        (
            "REMOTESHELL",
            ["task_params"],
            ["name", "type"],
            {
                "task_params": [
                    "task_params.rawScript",
                    "task_params.datasource",
                ]
            },
        ),
        (
            "SQL",
            ["task_params"],
            [
                "name",
                "type",
                "task_params",
                "task_params.type",
                "task_params.datasource",
                "task_params.sql",
                "task_params.sqlType",
            ],
            {},
        ),
    ],
)
def test_task_type_summary_separates_payload_mode_requirements(
    task_type: str,
    expected_payload_modes: list[str],
    expected_required_paths: list[str],
    expected_required_paths_by_payload_mode: dict[str, list[str]],
) -> None:
    data = _mapping(task_type_service.task_type_summary_result(task_type).data)

    assert data["payload_modes"] == expected_payload_modes
    assert data["required_paths"] == expected_required_paths
    assert (
        data["required_paths_by_payload_mode"]
        == expected_required_paths_by_payload_mode
    )


def test_remote_shell_schema_exposes_ssh_as_the_only_connection_type() -> None:
    data = _mapping(task_type_service.task_type_schema_result("REMOTESHELL").data)
    fields = [_mapping(item) for item in _sequence(data["fields"])]
    remote_type = next(field for field in fields if field["path"] == "task_params.type")

    assert remote_type["type"] == "enum"
    assert remote_type["choices"] == ["SSH"]

    json_schema_data = _mapping(
        task_type_service.task_type_schema_result(
            "REMOTESHELL",
            json_schema=True,
        ).data
    )
    schema = _mapping(json_schema_data["schema"])
    definitions = _mapping(schema["$defs"])
    task_params = _mapping(definitions["task_params"])
    properties = _mapping(task_params["properties"])
    assert _mapping(properties["type"])["const"] == "SSH"


def test_task_type_schema_result_describes_fields_and_state_rules() -> None:
    result = task_type_service.task_type_schema_result("SQL")
    data = _mapping(result.data)
    fields = _sequence(data["fields"])
    state_rules = _sequence(data["state_rules"])

    assert result.resolved == {"task_type": "SQL", "view": "fields"}
    assert data["schema_version"] == 2
    assert "rows" not in data
    assert "schema" not in data
    assert "choice_sources" not in data
    assert "compile_mappings" not in data
    assert any(_mapping(field)["path"] == "task_params.sqlType" for field in fields)
    assert _mapping(state_rules[1])["when"] == "task_params.sqlType == 1"
    links = _mapping(data["links"])
    assert links["field"] == "dsctl task-type schema SQL --field 'FIELD_PATH'"
    assert links["json_schema"] == "dsctl task-type schema SQL --json-schema"
    assert links["full"] == "dsctl task-type schema SQL --full"


def test_task_type_json_schema_preserves_nested_and_array_authoring_fields() -> None:
    result = task_type_service.task_type_schema_result("SHELL", json_schema=True)
    data = _mapping(result.data)
    schema = _mapping(data["schema"])
    properties = _mapping(schema["properties"])

    assert result.resolved == {"task_type": "SHELL", "view": "json_schema"}
    assert "fields" not in data
    assert "state_rules" not in data
    assert "choice_sources" not in data
    assert "compile_mappings" not in data
    schema_metadata = _mapping(schema["x-dsctl"])
    assert "state_rules" not in schema_metadata
    assert "choice_sources" not in schema_metadata
    assert "compile_mappings" not in schema_metadata

    retry = _mapping(properties["retry"])
    assert retry["type"] == "object"
    retry_properties = _mapping(retry["properties"])
    assert _mapping(retry_properties["times"])["type"] == "integer"
    assert _mapping(retry_properties["times"])["default"] == 0
    assert _mapping(retry_properties["interval"])["type"] == "integer"
    assert _mapping(retry_properties["interval"])["default"] == 0

    depends_on = _mapping(properties["depends_on"])
    assert depends_on["type"] == "array"
    assert depends_on["default"] == []
    assert _mapping(depends_on["items"])["type"] == "string"


@pytest.mark.parametrize("task_type", upstream_default_task_types())
def test_task_type_json_schema_local_refs_resolve(task_type: str) -> None:
    result = task_type_service.task_type_schema_result(task_type, json_schema=True)
    schema = _mapping(_mapping(result.data)["schema"])

    for ref in _local_json_refs(schema):
        _resolve_local_json_ref(schema, ref)


def test_task_type_schema_result_exposes_field_discovery_commands() -> None:
    sql_result = task_type_service.task_type_schema_result("SQL")
    shell_result = task_type_service.task_type_schema_result("SHELL")
    conditions_result = task_type_service.task_type_schema_result("CONDITIONS")

    sql_data = _mapping(sql_result.data)
    shell_data = _mapping(shell_result.data)
    conditions_data = _mapping(conditions_result.data)

    sql_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(sql_data["fields"])
    }
    shell_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(shell_data["fields"])
    }
    conditions_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(conditions_data["fields"])
    }

    assert sql_fields["task_params.groupId"]["choice_source"] == (
        "dsctl alert-group list"
    )
    assert "dsctl alert-group create --name NAME --instance-id ID" in _sequence(
        sql_fields["task_params.groupId"]["related_commands"]
    )

    resource_field = shell_fields["task_params.resourceList[].resourceName"]
    assert resource_field["choice_source"] == "dsctl resource list --dir DIR"
    assert resource_field["choice_value"] == "fullName"
    assert "dsctl resource upload --file FILE" in _sequence(
        resource_field["related_commands"]
    )

    project_field = conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].projectCode"
    ]
    assert project_field["choice_source"] == "dsctl project list"
    assert conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].cycle"
    ]["choices"] == ["hour", "day", "week", "month"]
    assert "today" in _sequence(
        conditions_fields[
            "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
        ]["choices"]
    )

    shell_full_result = task_type_service.task_type_schema_result("SHELL", full=True)
    shell_full_data = _mapping(shell_full_result.data)
    shell_choice_sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(shell_full_data["choice_sources"])
    }
    assert shell_choice_sources["task_params.resourceList[].resourceName"] == {
        "path": "task_params.resourceList[].resourceName",
        "command": "dsctl resource list --dir DIR",
        "value": "fullName",
        "description": (
            "Run `dsctl resource list --dir DIR` and use `fullName` as "
            "task_params.resourceList[].resourceName; upload the file first "
            "when it is missing."
        ),
        "related_commands": [
            "dsctl resource list",
            "dsctl resource upload --file FILE",
            "dsctl resource view RESOURCE",
        ],
    }


def test_task_type_schema_result_supports_compile_and_full_views() -> None:
    compile_result = task_type_service.task_type_schema_result(
        "SQL",
        compile_mappings=True,
    )
    compile_data = _mapping(compile_result.data)

    assert compile_result.resolved == {
        "task_type": "SQL",
        "view": "compile_mappings",
    }
    assert "fields" not in compile_data
    mappings = _sequence(compile_data["compile_mappings"])
    assert compile_data["compile_mapping_policy"] == (
        "Compiled by workflow create/edit before sending DS REST form fields."
    )
    assert all(
        set(_mapping(mapping)) == {"authoring_path", "ds_payload_path"}
        for mapping in mappings
    )
    assert any(
        _mapping(mapping)["authoring_path"] == "task_params.sql" for mapping in mappings
    )

    full_result = task_type_service.task_type_schema_result("SQL", full=True)
    full_data = _mapping(full_result.data)
    assert full_result.resolved == {"task_type": "SQL", "view": "full"}
    assert "schema_version" not in full_data
    assert set(full_data) == {
        "task_type",
        "category",
        "kind",
        "schema",
        "fields",
        "state_rules",
        "choice_sources",
        "compile_mappings",
        "template_command",
        "raw_template_command",
    }
    full_fields = _sequence(full_data["fields"])
    assert all("choice_value" not in _mapping(field) for field in full_fields)
    full_schema_metadata = _mapping(_mapping(full_data["schema"])["x-dsctl"])
    assert full_schema_metadata["state_rules"] == full_data["state_rules"]
    assert full_schema_metadata["choice_sources"] == full_data["choice_sources"]
    assert full_schema_metadata["compile_mappings"] == full_data["compile_mappings"]


def test_task_type_schema_result_filters_one_field_and_related_rules() -> None:
    result = task_type_service.task_type_schema_result(
        "SQL",
        field="task_params.sqlType",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "task_type": "SQL",
        "view": "field",
        "field": "task_params.sqlType",
    }
    fields = _sequence(data["fields"])
    assert [_mapping(item)["path"] for item in fields] == ["task_params.sqlType"]
    state_rules = _sequence(data["state_rules"])
    assert [_mapping(item)["when"] for item in state_rules] == [
        "task_params.sqlType == 0",
        "task_params.sqlType == 1",
    ]


@pytest.mark.parametrize(
    ("task_type", "field", "expected_when"),
    [
        ("SQL", "task_params.sql", []),
        ("SQL", "task_params.preStatements[]", ["task_params.sqlType == 1"]),
        (
            "SQL",
            "task_params.sendEmail",
            ["task_params.sqlType == 0", "task_params.sqlType == 1"],
        ),
        (
            "DEPENDENT",
            "task_params.dependence.dependTaskList[].dependItemList[].cycle",
            [
                "dependItem.cycle == hour",
                "dependItem.cycle == day",
                "dependItem.cycle == week",
                "dependItem.cycle == month",
            ],
        ),
        (
            "CONDITIONS",
            "task_params.dependence.dependTaskList[].dependItemList[].dateValue",
            [
                "dependItem.cycle == hour",
                "dependItem.cycle == day",
                "dependItem.cycle == week",
                "dependItem.cycle == month",
            ],
        ),
        (
            "SHELL",
            "task_params.localParams[]",
            ["command is set", "task_params is set"],
        ),
        ("SQL", "name", []),
    ],
)
def test_task_type_field_view_uses_explicit_state_rule_paths(
    task_type: str,
    field: str,
    expected_when: list[str],
) -> None:
    result = task_type_service.task_type_schema_result(task_type, field=field)
    rules = _sequence(_mapping(result.data)["state_rules"])

    assert [_mapping(rule)["when"] for rule in rules] == expected_when


def test_task_type_choice_value_is_only_emitted_for_selectable_values() -> None:
    result = task_type_service.task_type_schema_result("SHELL")
    fields = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(_mapping(result.data)["fields"])
    }

    assert fields["task_params.resourceList[].resourceName"]["choice_value"] == (
        "fullName"
    )
    assert "choice_value" not in fields["task_params.localParams[]"]

    full_result = task_type_service.task_type_schema_result("SHELL", full=True)
    sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(_mapping(full_result.data)["choice_sources"])
    }
    assert "task_params.localParams[]" not in sources


@pytest.mark.parametrize(
    ("first_selector", "second_selector"),
    [
        ("--field", "--json-schema"),
        ("--field", "--compile-mappings"),
        ("--field", "--full"),
        ("--json-schema", "--compile-mappings"),
        ("--json-schema", "--full"),
        ("--compile-mappings", "--full"),
    ],
)
def test_task_type_schema_result_rejects_ambiguous_selectors(
    first_selector: str,
    second_selector: str,
) -> None:
    selected = {first_selector, second_selector}
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="command" if "--field" in selected else None,
            json_schema="--json-schema" in selected,
            compile_mappings="--compile-mappings" in selected,
            full="--full" in selected,
        )

    assert exc_info.value.details == {
        "constraint": "at_most_one_of",
        "selected": [first_selector, second_selector],
    }
    assert exc_info.value.suggestion is not None
    assert "only one" in exc_info.value.suggestion


def test_task_type_schema_result_returns_bounded_field_candidates() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="task_params.resource.resourceName",
        )

    details = exc_info.value.details
    assert details["task_type"] == "SHELL"
    assert details["field"] == "task_params.resource.resourceName"
    assert isinstance(details["available_count"], int)
    candidates = _sequence(details["candidates"])
    assert len(candidates) <= 3
    candidate = _mapping(candidates[0])
    assert set(candidate) == {"path", "command"}
    assert candidate["path"] == "task_params.resourceList[].resourceName"
    assert candidate["command"] == (
        "dsctl task-type schema SHELL --field 'task_params.resourceList[].resourceName'"
    )
    assert "available_fields" not in details
    candidate_command = candidate["command"]
    assert isinstance(candidate_command, str)
    assert candidate_command in (exc_info.value.suggestion or "")


def test_generic_task_type_typos_recover_known_common_fields() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result("SPARK", field="naem")

    candidates = _sequence(exc_info.value.details["candidates"])
    assert _mapping(candidates[0])["path"] == "name"
    assert "open_task_params" not in exc_info.value.details


def test_generic_plugin_field_points_to_the_open_task_params_contract() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SPARK",
            field="task_params.pluginSpecificField",
        )

    assert exc_info.value.details["candidates"] == []
    assert exc_info.value.details["open_task_params"] is True


@pytest.mark.parametrize("task_type", upstream_default_task_types())
def test_task_type_default_schema_has_bounded_compact_envelope(task_type: str) -> None:
    payload = success_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type),
    )
    compact = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()

    assert len(compact) < 12 * 1024


@pytest.mark.parametrize("task_type", upstream_default_task_types())
def test_task_type_detailed_views_have_bounded_compact_envelopes(
    task_type: str,
) -> None:
    json_schema_payload = success_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type, json_schema=True),
    )
    compile_payload = success_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type, compile_mappings=True),
    )
    default_data = _mapping(task_type_service.task_type_schema_result(task_type).data)
    fields = _sequence(default_data["fields"])

    def compact_size(payload: object) -> int:
        return len(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )

    assert compact_size(json_schema_payload) < 10 * 1024
    assert compact_size(compile_payload) < 5 * 1024
    for field in fields:
        field_path = _mapping(field)["path"]
        assert isinstance(field_path, str)
        field_payload = success_payload(
            "task-type.schema",
            task_type_service.task_type_schema_result(task_type, field=field_path),
        )
        # Dependency date fields intentionally retain four cycle-specific rules;
        # correctness is worth the increase over a metadata-only field row.
        assert compact_size(field_payload) < 3 * 1024


def test_unknown_task_type_field_error_has_bounded_compact_envelope() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="task_params.resource.resourceName",
        )

    payload = error_payload("task-type.schema", exc_info.value)
    compact = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert len(compact) < 2 * 1024
