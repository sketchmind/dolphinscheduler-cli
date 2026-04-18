from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, cast

from dsctl.errors import UserInputError
from dsctl.services._data_shapes import DataShape, data_shape_for_action

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

OutputFormat: TypeAlias = Literal["json", "table", "tsv"]
OUTPUT_FORMAT_CHOICES: tuple[OutputFormat, ...] = ("json", "table", "tsv")


@dataclass(frozen=True)
class RenderOptions:
    """Resolved display settings for one command invocation."""

    output_format: OutputFormat = "json"
    columns: tuple[str, ...] = ()


def parse_columns(value: str | None) -> tuple[str, ...]:
    """Parse a comma-separated display-column option."""
    if value is None:
        return ()
    columns = tuple(item.strip() for item in value.split(",") if item.strip())
    if not columns:
        message = "--columns must include at least one column name"
        raise UserInputError(
            message,
            suggestion="Pass a comma-separated list such as `--columns id,name,state`.",
        )
    return columns


def validate_render_options(options: RenderOptions) -> RenderOptions:
    """Reject ambiguous global display settings before running a command."""
    if options.columns and options.output_format == "json":
        message = "--columns requires --output-format table or --output-format tsv"
        raise UserInputError(
            message,
            details={
                "format": options.output_format,
                "columns": list(options.columns),
            },
            suggestion="Retry with `--output-format table` or `--output-format tsv`.",
        )
    return options


def render_payload(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> str:
    """Render one standard output envelope using the requested format."""
    if options.output_format == "json":
        return _render_json(payload)
    if not payload.get("ok"):
        return _render_error_payload(payload, output_format=options.output_format)
    return _render_success_payload(payload, action=action, options=options)


def _render_json(payload: JsonValue) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)


def _render_success_payload(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> str:
    data = payload.get("data")
    shape = data_shape_for_action(action)
    rows = _extract_rows(data, shape=shape)
    if rows is not None:
        columns = _resolve_columns(
            rows,
            requested=options.columns,
            defaults=() if shape is None else shape.default_columns,
            action=action,
        )
        return _render_rows(rows, columns=columns, output_format=options.output_format)

    if isinstance(data, Mapping):
        if options.columns:
            row = _mapping_to_json_object(data)
            rows = (row,)
            columns = _resolve_columns(
                rows,
                requested=options.columns,
                defaults=(),
                action=action,
            )
            return _render_rows(
                rows,
                columns=columns,
                output_format=options.output_format,
            )
        key_value_rows = _object_rows(data)
        return _render_rows(
            key_value_rows,
            columns=("field", "value"),
            output_format=options.output_format,
        )

    scalar_rows: tuple[JsonObject, ...] = (
        {"field": "data", "value": _format_cell(data)},
    )
    return _render_rows(
        scalar_rows,
        columns=("field", "value"),
        output_format=options.output_format,
    )


def _render_error_payload(payload: JsonObject, *, output_format: OutputFormat) -> str:
    rows: list[JsonObject] = [
        {"field": "ok", "value": "false"},
        {"field": "action", "value": _format_cell(payload.get("action"))},
    ]
    error = payload.get("error")
    if isinstance(error, Mapping):
        rows.extend(
            {"field": f"error.{key}", "value": _format_cell(error[key])}
            for key in ("type", "message", "suggestion")
            if key in error
        )
        source = error.get("source")
        if isinstance(source, Mapping):
            for key, value in source.items():
                rows.append(
                    {
                        "field": f"error.source.{key}",
                        "value": _format_cell(value),
                    }
                )
    return _render_rows(rows, columns=("field", "value"), output_format=output_format)


def _extract_rows(
    data: JsonValue | None,
    *,
    shape: DataShape | None,
) -> tuple[JsonObject, ...] | None:
    if shape is not None and shape.row_path is not None:
        value = _value_at_path({"data": data}, shape.row_path)
        rows = _coerce_rows(value)
        if rows is not None:
            return rows

    if isinstance(data, Mapping):
        total_list = data.get("totalList")
        rows = _coerce_rows(total_list)
        if rows is not None:
            return rows
        return None
    return _coerce_rows(data)


def _value_at_path(root: Mapping[str, JsonValue | None], path: str) -> JsonValue | None:
    current: JsonValue | None = cast("JsonValue | None", root)
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _coerce_rows(value: JsonValue | None) -> tuple[JsonObject, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    rows: list[JsonObject] = []
    for item in value:
        if isinstance(item, Mapping):
            rows.append(_mapping_to_json_object(item))
        else:
            rows.append({"value": _format_cell(item)})
    return tuple(rows)


def _mapping_to_json_object(value: Mapping[str, JsonValue]) -> JsonObject:
    return {str(key): item for key, item in value.items()}


def _object_rows(data: Mapping[str, JsonValue]) -> tuple[JsonObject, ...]:
    return tuple(
        {"field": key, "value": _format_cell(value)} for key, value in data.items()
    )


def _resolve_columns(
    rows: Sequence[JsonObject],
    *,
    requested: tuple[str, ...],
    defaults: tuple[str, ...],
    action: str,
) -> tuple[str, ...]:
    if requested:
        if "*" in requested:
            _validate_wildcard_columns(requested)
            return _infer_columns(rows)
        _validate_requested_columns(rows, requested, action=action)
        return requested
    if defaults and _any_column_present(rows, defaults):
        return defaults
    return _infer_columns(rows)


def _validate_wildcard_columns(columns: tuple[str, ...]) -> None:
    if columns == ("*",):
        return
    message = "--columns '*' cannot be combined with explicit columns"
    raise UserInputError(
        message,
        details={"columns": list(columns), "wildcard": "*"},
        suggestion=(
            "Use `--columns '*'` for all row fields, or pass explicit columns "
            "such as `--columns id,name,state`."
        ),
    )


def _validate_requested_columns(
    rows: Sequence[JsonObject],
    columns: tuple[str, ...],
    *,
    action: str,
) -> None:
    if not rows:
        return
    missing = [column for column in columns if not any(column in row for row in rows)]
    if not missing:
        return
    message = f"Unknown display column for {action}: {', '.join(missing)}"
    raise UserInputError(
        message,
        details={
            "action": action,
            "columns": list(columns),
            "unknown_columns": missing,
            "available_columns": _infer_columns(rows),
        },
        suggestion=(
            f"Run `dsctl schema --command {action}` and inspect data_shape, "
            "or retry with columns present in the JSON row payload."
        ),
    )


def _any_column_present(rows: Sequence[JsonObject], columns: tuple[str, ...]) -> bool:
    if not rows:
        return True
    return any(column in row for row in rows for column in columns)


def _infer_columns(rows: Sequence[JsonObject]) -> tuple[str, ...]:
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    return tuple(columns)


def _render_rows(
    rows: Sequence[JsonObject],
    *,
    columns: tuple[str, ...],
    output_format: OutputFormat,
) -> str:
    if output_format == "tsv":
        return _render_tsv(rows, columns=columns)
    return _render_table(rows, columns=columns)


def _render_tsv(rows: Sequence[JsonObject], *, columns: tuple[str, ...]) -> str:
    lines = ["\t".join(columns)]
    lines.extend(
        "\t".join(_format_tsv_cell(row.get(column)) for column in columns)
        for row in rows
    )
    return "\n".join(lines)


def _render_table(rows: Sequence[JsonObject], *, columns: tuple[str, ...]) -> str:
    if not columns:
        return "(no rows)"
    rendered_rows = [
        [_format_cell(row.get(column)) for column in columns] for row in rows
    ]
    widths = [
        max(len(column), *(len(row[index]) for row in rendered_rows))
        for index, column in enumerate(columns)
    ]
    header = _render_table_line(columns, widths=widths)
    separator = "-+-".join("-" * width for width in widths)
    lines = [header, separator]
    lines.extend(_render_table_line(tuple(row), widths=widths) for row in rendered_rows)
    return "\n".join(lines)


def _render_table_line(values: tuple[str, ...], *, widths: Sequence[int]) -> str:
    padded: list[str] = []
    for index, value in enumerate(values):
        if index == len(values) - 1:
            padded.append(value)
        else:
            padded.append(value.ljust(widths[index]))
    return " | ".join(padded)


def _format_tsv_cell(value: JsonValue | None) -> str:
    return _format_cell(value).replace("\t", " ").replace("\n", " ")


def _format_cell(value: JsonValue | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


__all__ = [
    "OUTPUT_FORMAT_CHOICES",
    "OutputFormat",
    "RenderOptions",
    "parse_columns",
    "render_payload",
    "validate_render_options",
]
