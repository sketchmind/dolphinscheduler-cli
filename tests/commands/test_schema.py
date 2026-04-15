import json

from typer.testing import CliRunner

from dsctl.app import app
from dsctl.models import supported_typed_task_types
from dsctl.services.template import parameter_syntax_index_data, task_template_metadata
from dsctl.upstream import upstream_default_task_types

runner = CliRunner()


def test_schema_command_returns_machine_readable_cli_surface() -> None:
    result = runner.invoke(app, ["schema"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "schema"
    assert payload["data"]["schema_version"] == 1
    assert payload["data"]["cli"] == {"name": "dsctl", "version": "0.1.0"}
    command_names = [item["name"] for item in payload["data"]["commands"]]
    assert command_names[:18] == [
        "version",
        "context",
        "doctor",
        "schema",
        "capabilities",
        "use",
        "enum",
        "lint",
        "env",
        "cluster",
        "datasource",
        "namespace",
        "resource",
        "queue",
        "worker-group",
        "task-group",
        "alert-plugin",
        "alert-group",
    ]
    assert "task-type" in command_names
    expected_supported_types = list(upstream_default_task_types())
    expected_typed_types = list(supported_typed_task_types())
    expected_generic_types = [
        task_type
        for task_type in expected_supported_types
        if task_type not in expected_typed_types
    ]
    assert payload["data"]["capabilities"]["templates"]["task"] == {
        "supported_types": expected_supported_types,
        "typed_types": expected_typed_types,
        "generic_types": expected_generic_types,
        "templates_by_type": task_template_metadata(),
    }
    assert payload["data"]["capabilities"]["templates"]["parameters"] == (
        parameter_syntax_index_data()
    )
    assert payload["data"]["capabilities"]["self_description"] == {
        "schema": True,
        "template": True,
        "capabilities": True,
    }
    assert payload["data"]["errors"] == {
        "fields": ["type", "message", "details", "source", "suggestion"],
        "source": {
            "field": "error.source",
            "kind": "remote",
            "system": "dolphinscheduler",
            "layers": {
                "result": {
                    "fields": [
                        "kind",
                        "system",
                        "layer",
                        "result_code",
                        "result_message",
                    ]
                },
                "http": {
                    "fields": [
                        "kind",
                        "system",
                        "layer",
                        "status_code",
                    ]
                },
            },
        },
    }
    assert payload["data"]["output"] == {
        "success_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
        ],
        "error_fields": [
            "ok",
            "action",
            "resolved",
            "data",
            "warnings",
            "warning_details",
            "error",
        ],
        "ok_values": {
            "success": True,
            "error": False,
        },
        "warning_details_aligned": True,
    }
    assert payload["data"]["capabilities"]["monitor"] == {
        "health": True,
        "database": True,
        "server_types": ["master", "worker", "alert-server"],
    }
