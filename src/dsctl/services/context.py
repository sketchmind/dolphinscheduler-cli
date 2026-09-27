from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl import context as context_store
from dsctl.config import validate_context_file
from dsctl.context import (
    NamedContext,
    create_context,
    delete_context,
    registry_path,
    update_context,
)
from dsctl.errors import UserInputError
from dsctl.output import CommandResult, require_json_value

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.support.yaml_io import JsonObject


def _context_data(context: NamedContext) -> JsonObject:
    return {
        "name": context.name,
        "env_file": str(context.env_file),
        "api_url": context.api_url,
        "project": context.project,
    }


def _local_result(data: JsonObject | list[JsonObject]) -> CommandResult:
    return CommandResult(
        data=require_json_value(data, label="saved context data"),
        resolved={
            "registry": str(registry_path()),
            "remote_validation": "not_performed",
        },
    )


def list_contexts_result() -> CommandResult:
    """Inspect all saved entries without reading their connection files."""
    registry = context_store.load_registry()
    return _local_result(
        [
            {
                **_context_data(context),
                "default": context.name == registry.default_context,
            }
            for context in sorted(
                registry.contexts.values(), key=lambda item: item.name
            )
        ]
    )


def get_saved_context_result(name: str) -> CommandResult:
    """Inspect one saved entry without checking its remote connection."""
    registry = context_store.load_registry()
    context = registry.context(name)
    return _local_result(
        {**_context_data(context), "default": context.name == registry.default_context}
    )


def create_context_result(
    name: str, *, file: Path, project: str | None = None
) -> CommandResult:
    """Validate a complete local connection and save its reference."""
    connection = validate_context_file(file)
    return _local_result(
        _context_data(
            create_context(
                name, env_file=file, api_url=connection.api_url, project=project
            )
        )
    )


def update_context_result(
    name: str,
    *,
    file: Path | None = None,
    project: str | None = None,
    clear_project: bool = False,
) -> CommandResult:
    """Update an entry without probing the referenced server."""
    if project is not None and clear_project:
        message = "--project cannot be combined with --clear-project"
        raise UserInputError(
            message,
            suggestion="Pass either --project PROJECT or --clear-project.",
        )
    if file is None and project is None and not clear_project:
        message = "At least one context update is required"
        raise UserInputError(
            message,
            suggestion="Pass --file FILE, --project PROJECT, or --clear-project.",
        )
    connection = None if file is None else validate_context_file(file)
    return _local_result(
        _context_data(
            update_context(
                name,
                env_file=file,
                api_url=None if connection is None else connection.api_url,
                project=project,
                clear_project=clear_project,
            )
        )
    )


def delete_context_result(name: str) -> CommandResult:
    """Remove one local entry and return the deleted reference."""
    context = delete_context(name)
    return _local_result({**_context_data(context), "deleted": True})
