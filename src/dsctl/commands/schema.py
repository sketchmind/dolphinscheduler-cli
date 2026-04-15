import typer

from dsctl.cli_runtime import emit_result
from dsctl.services.schema import get_schema_result


def register_schema_commands(app: typer.Typer) -> None:
    """Register the top-level `schema` command."""
    app.command("schema")(schema_command)


def schema_command() -> None:
    """Print the stable machine-readable CLI schema."""
    emit_result("schema", get_schema_result)
