from dsctl.cli_surface import (
    COMMAND_GROUPS,
    RESOURCE_COMMAND_TREE,
    RESOURCE_COMMANDS,
    SURFACE_PLANES,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)
from dsctl.services.capabilities import get_capabilities_result
from dsctl.services.schema import get_schema_result


def test_surface_discovery_is_consistent_between_schema_and_capabilities() -> None:
    capabilities = get_capabilities_result().data
    schema = get_schema_result().data

    assert isinstance(capabilities, dict)
    assert isinstance(schema, dict)

    resource_groups = capabilities["resources"]["groups"]
    assert isinstance(resource_groups, dict)
    assert capabilities["resources"]["top_level"] == list(TOP_LEVEL_COMMANDS)
    assert capabilities["planes"] == {
        name: list(resources) for name, resources in SURFACE_PLANES.items()
    }

    commands = schema["commands"]
    assert isinstance(commands, list)

    top_level_command_names: list[str] = []
    schema_groups: dict[str, tuple[SurfaceCommand, ...]] = {}
    ordered_group_names: list[str] = []
    for item in commands:
        assert isinstance(item, dict)
        if item.get("kind") != "group":
            command_name = item["name"]
            assert isinstance(command_name, str)
            top_level_command_names.append(command_name)
            continue
        name = item["name"]
        assert isinstance(name, str)
        ordered_group_names.append(name)
        schema_groups[name] = _schema_surface(item["commands"])

    assert top_level_command_names == list(TOP_LEVEL_COMMANDS)
    assert ordered_group_names == list(COMMAND_GROUPS)
    assert list(resource_groups) == list(COMMAND_GROUPS)
    for resource in COMMAND_GROUPS:
        commands_payload = resource_groups[resource]
        assert isinstance(commands_payload, dict)
        expected_commands = commands_payload["commands"]
        assert isinstance(expected_commands, list)
        assert expected_commands == list(RESOURCE_COMMANDS[resource])
        assert schema_groups[resource] == RESOURCE_COMMAND_TREE[resource]


def _schema_surface(commands: object) -> tuple[SurfaceCommand, ...]:
    assert isinstance(commands, list)
    nodes: list[SurfaceCommand] = []
    for item in commands:
        assert isinstance(item, dict)
        kind = item.get("kind")
        name = item.get("name")
        assert isinstance(kind, str)
        assert isinstance(name, str)
        child_commands = item.get("commands")
        if kind == "command":
            nodes.append(SurfaceCommand(name=name))
            continue
        assert kind == "group"
        nodes.append(
            SurfaceCommand(
                name=name,
                commands=_schema_surface(child_commands),
            )
        )
    return tuple(nodes)
