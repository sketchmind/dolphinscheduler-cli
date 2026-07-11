from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeGuard, cast

from dsctl.data_shapes import DataShape, data_shape_for_action
from dsctl.errors import UserInputError
from dsctl.schema_contract_rows import command_contract_rows

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

OutputFormat: TypeAlias = Literal["json", "table", "tsv"]
OUTPUT_FORMAT_CHOICES: tuple[OutputFormat, ...] = ("json", "table", "tsv")


@dataclass(frozen=True)
class RenderOptions:
    """Resolved display settings for one command invocation."""

    output_format: OutputFormat = "json"
    columns: tuple[str, ...] = ()
    compact: bool = False


@dataclass(frozen=True)
class RenderedCommand:
    """Exact process output and exit status for one rendered command."""

    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0


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
    if options.compact and options.output_format != "json":
        message = "--compact can only be used with --output-format json"
        raise UserInputError(
            message,
            details={
                "compact": True,
                "output_format": options.output_format,
            },
            suggestion="Remove --compact or use --output-format json.",
        )
    if options.columns and "*" in options.columns:
        _validate_wildcard_columns(options.columns)
    return options


def render_command(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> RenderedCommand:
    """Render one standard result into exact process channels and status."""
    body = render_payload(payload, action=action, options=options)
    if not payload.get("ok"):
        return RenderedCommand(
            stderr=_with_final_newline(body),
            exit_code=1,
        )

    diagnostics = ""
    if options.output_format != "json":
        diagnostics = _render_success_diagnostics(payload, include_page=True)
    return RenderedCommand(
        stdout=_with_final_newline(body),
        stderr=_with_final_newline(diagnostics) if diagnostics else "",
    )


def render_raw_command(body: str, *, payload: JsonObject) -> RenderedCommand:
    """Render one successful raw artifact without changing its body bytes."""
    diagnostics = _render_success_diagnostics(payload, include_page=False)
    return RenderedCommand(
        stdout=body,
        stderr=_with_final_newline(diagnostics) if diagnostics else "",
    )


def render_payload(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> str:
    """Render one standard output envelope using the requested format."""
    if options.output_format == "json":
        if options.columns and payload.get("ok"):
            payload = _project_json_payload(
                payload,
                action=action,
                columns=options.columns,
            )
        return _render_json(payload, compact=options.compact)
    if not payload.get("ok"):
        return _render_error_payload(payload, output_format=options.output_format)
    return _render_success_payload(payload, action=action, options=options)


def _render_json(payload: JsonValue, *, compact: bool) -> str:
    return json.dumps(
        payload,
        indent=None if compact else 2,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":") if compact else None,
    )


def _render_success_diagnostics(
    payload: JsonObject,
    *,
    include_page: bool,
) -> str:
    lines: list[str] = []
    if include_page:
        page = _pagination_diagnostic(payload)
        if page is not None:
            lines.append(page)
    lines.extend(_warning_diagnostics(payload))
    return "\n".join(lines)


def _pagination_diagnostic(payload: JsonObject) -> str | None:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    rows = data.get("totalList")
    if not _is_sequence_like(rows):
        return None

    total = _int_metadata(data.get("total"))
    page_no = _int_metadata(data.get("pageNo"))
    total_page = _int_metadata(data.get("totalPage"))
    if total is None or page_no is None or total_page is None:
        return None

    row_count = len(rows)
    if page_no <= 1 and total_page <= 1 and row_count >= total:
        return None
    return f"page: {page_no}/{total_page}; showing {row_count} of {total} rows"


def _warning_diagnostics(payload: JsonObject) -> list[str]:
    warnings = payload.get("warnings")
    if not _is_sequence_like(warnings):
        return []
    details = payload.get("warning_details")
    detail_items = details if _is_sequence_like(details) else ()

    lines: list[str] = []
    for index, warning in enumerate(warnings):
        if not isinstance(warning, str):
            continue
        detail = detail_items[index] if index < len(detail_items) else None
        code = detail.get("code") if isinstance(detail, Mapping) else None
        label = f"warning[{code}]" if isinstance(code, str) and code else "warning"
        lines.append(f"{label}: {warning}")
        suggestion = detail.get("suggestion") if isinstance(detail, Mapping) else None
        if isinstance(suggestion, str) and suggestion:
            lines.append(f"  suggestion: {suggestion}")
    return lines


def _int_metadata(value: JsonValue | None) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _with_final_newline(value: str) -> str:
    return value if value.endswith("\n") else f"{value}\n"


def _render_success_payload(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> str:
    data = payload.get("data")
    view = _result_view(payload, action=action)
    shape = data_shape_for_action(action, view=view)
    derived_rows = _derived_rows(data, action=action, view=view)
    rows = derived_rows
    if rows is None:
        rows = _extract_rows(data, shape=shape)
    if rows is not None:
        columns = _resolve_columns(
            rows,
            requested=options.columns,
            defaults=(
                ()
                if shape is None or (derived_rows is not None and view != "command")
                else shape.default_columns
            ),
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


def _project_json_payload(
    payload: JsonObject,
    *,
    action: str,
    columns: tuple[str, ...],
) -> JsonObject:
    """Return a copy of a success envelope with row/object data projected."""
    projected = deepcopy(payload)
    view = _result_view(projected, action=action)
    shape = data_shape_for_action(action, view=view)
    derived_rows = _derived_rows(projected.get("data"), action=action, view=view)
    if (
        action == "schema"
        and view in {"command", "full_group", "full_command"}
        and derived_rows is not None
    ):
        data = projected.get("data")
        if not isinstance(data, dict):
            raise _projection_not_supported_error(action=action, columns=columns)
        row_field = "command" if view == "command" else "rows"
        data[row_field] = _project_json_value(
            list(derived_rows),
            columns=columns,
            action=action,
        )
        return projected
    if shape is not None and shape.row_path is not None:
        value = _value_at_path(projected, shape.row_path)
        replacement = _project_json_value(value, columns=columns, action=action)
        if _replace_value_at_path(projected, shape.row_path, replacement):
            return projected
        raise _projection_not_supported_error(action=action, columns=columns)

    data = projected.get("data")
    if isinstance(data, Mapping):
        total_list = data.get("totalList")
        if _is_sequence_like(total_list):
            replacement = _project_json_value(
                total_list,
                columns=columns,
                action=action,
            )
            data_copy = dict(data)
            data_copy["totalList"] = replacement
            projected["data"] = data_copy
            return projected
        projected["data"] = _project_json_value(
            data,
            columns=columns,
            action=action,
        )
        return projected

    projected["data"] = _project_json_value(data, columns=columns, action=action)
    return projected


def _result_view(payload: JsonObject, *, action: str) -> str | None:
    if action != "schema":
        return None
    resolved = payload.get("resolved")
    schema = resolved.get("schema") if isinstance(resolved, Mapping) else None
    scope = schema.get("scope") if isinstance(schema, Mapping) else None
    data = payload.get("data")
    if isinstance(data, Mapping):
        data_view = data.get("view")
        if isinstance(data_view, str):
            if data_view == "full" and scope in {"group", "command"}:
                return f"full_{scope}"
            return data_view
    if not isinstance(schema, Mapping):
        return None
    resolved_view = schema.get("view")
    return resolved_view if isinstance(resolved_view, str) else None


def _derived_rows(
    data: JsonValue | None,
    *,
    action: str,
    view: str | None,
) -> tuple[JsonObject, ...] | None:
    if action != "schema" or not isinstance(data, Mapping):
        return None
    if view == "command":
        command = data.get("command")
        action_name = command.get("action") if isinstance(command, Mapping) else None
        if isinstance(command, Mapping) and isinstance(action_name, str):
            return tuple(command_contract_rows(command, action=action_name))
    if view in {"full_group", "full_command"}:
        legacy_rows = _coerce_rows(data.get("rows"))
        if legacy_rows is not None:
            return legacy_rows
    return None


def _project_json_value(
    value: JsonValue | None,
    *,
    columns: tuple[str, ...],
    action: str,
) -> JsonValue:
    if isinstance(value, Mapping):
        row = _mapping_to_json_object(value)
        resolved = _resolve_columns(
            (row,),
            requested=columns,
            defaults=(),
            action=action,
        )
        return _project_row(row, resolved)

    if _is_sequence_like(value):
        rows = _json_rows_for_projection(value, action=action, columns=columns)
        resolved = _resolve_columns(rows, requested=columns, defaults=(), action=action)
        return [_project_row(row, resolved) for row in rows]

    raise _projection_not_supported_error(action=action, columns=columns)


def _json_rows_for_projection(
    value: JsonValue | None,
    *,
    action: str,
    columns: tuple[str, ...],
) -> tuple[JsonObject, ...]:
    if not _is_sequence_like(value):
        raise _projection_not_supported_error(action=action, columns=columns)
    rows: list[JsonObject] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise _projection_not_supported_error(action=action, columns=columns)
        rows.append(_mapping_to_json_object(item))
    return tuple(rows)


def _project_row(row: JsonObject, columns: tuple[str, ...]) -> JsonObject:
    return {column: row[column] for column in columns if column in row}


def _replace_value_at_path(root: JsonObject, path: str, value: JsonValue) -> bool:
    current: JsonValue = root
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(current, dict):
            return False
        current = current.get(part)
    if not isinstance(current, dict):
        return False
    current[parts[-1]] = value
    return True


def _is_sequence_like(value: JsonValue | None) -> TypeGuard[Sequence[JsonValue]]:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _projection_not_supported_error(
    *,
    action: str,
    columns: tuple[str, ...],
) -> UserInputError:
    message = f"--columns can only project object or row-oriented output for {action}"
    return UserInputError(
        message,
        details={"action": action, "columns": list(columns)},
        suggestion=(
            f"Run `dsctl schema --command {action}` and inspect "
            "data.command.data_shape, "
            "or omit --columns for this command."
        ),
    )


def _extract_rows(
    data: JsonValue | None,
    *,
    shape: DataShape | None,
) -> tuple[JsonObject, ...] | None:
    if shape is not None and shape.row_path is not None:
        value = _value_at_path({"data": data}, shape.row_path)
        if shape.kind == "object" and isinstance(value, Mapping):
            return (_mapping_to_json_object(value),)
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
            f"Run `dsctl schema --command {action}` and inspect "
            "data.command.data_shape, "
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
        max((len(column), *(len(row[index]) for row in rendered_rows)))
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
    "RenderedCommand",
    "parse_columns",
    "render_command",
    "render_payload",
    "render_raw_command",
    "validate_render_options",
]
