from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeGuard, cast

from rich.cells import cell_len

from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.compact_json import render_compact_table
from dsctl.data_shapes import (
    DataShape,
    data_shape_for_action,
    data_shapes_by_view_schema_for_action,
)
from dsctl.errors import UserInputError
from dsctl.schema_contract_rows import command_contract_rows

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

OutputFormat: TypeAlias = Literal["json", "json-compact", "table", "tsv"]
OUTPUT_FORMAT_CHOICES = cast(
    "tuple[OutputFormat, ...]",
    COMMAND_CATALOG.global_option("format").input.choices,
)


@dataclass(frozen=True)
class RenderOptions:
    """Resolved display settings for one command invocation."""

    output_format: OutputFormat = "json"
    columns: tuple[str, ...] = ()


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
    try:
        COMMAND_CATALOG.validate_global_values(
            {
                "format": options.output_format,
            }
        )
    except CommandBindingError as exc:
        message = f"Unsupported output format: {options.output_format}"
        raise UserInputError(
            message,
            details={
                "output_format": options.output_format,
            },
            suggestion="Use --format json, json-compact, table or tsv.",
        ) from exc
    if options.columns and "*" in options.columns:
        _validate_wildcard_columns(options.columns)
    return options


def validate_action_render_options(action: str, options: RenderOptions) -> None:
    """Validate fixed list projection rules before making a service request."""
    shape = data_shape_for_action(action)
    if (
        options.output_format == "json-compact"
        and shape is not None
        and shape.compact_rows
        and any("." in column for column in options.columns)
    ):
        message = "Compact list columns must be top-level field names."
        raise UserInputError(
            message,
            details={"action": action, "columns": list(options.columns)},
            suggestion=(
                "Select the parent field (for example --columns id,taskParams), "
                "or use --format json for dotted field paths."
            ),
        )


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
    if options.output_format not in {"json", "json-compact"}:
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
    payload = _dry_run_view(payload, options=options)
    if payload.get("ok"):
        _validate_data_shape_render_options(
            payload,
            action=action,
            options=options,
        )
    if options.output_format in {"json", "json-compact"}:
        if payload.get("ok"):
            validate_action_render_options(action, options)
        if options.columns and payload.get("ok"):
            payload = _project_json_payload(
                payload,
                action=action,
                columns=options.columns,
            )
        shape = data_shape_for_action(action, view=_result_view(payload, action=action))
        if (
            payload.get("ok")
            and options.output_format == "json-compact"
            and shape is not None
            and shape.compact_rows
            and shape.row_path is not None
        ):
            return render_compact_table(
                payload, row_path=shape.row_path, columns=options.columns, action=action
            )
        return _render_json(payload, compact=options.output_format == "json-compact")
    if not payload.get("ok"):
        return _render_error_payload(payload, output_format=options.output_format)
    return _render_success_payload(payload, action=action, options=options)


def _dry_run_view(payload: JsonObject, *, options: RenderOptions) -> JsonObject:
    """Show prepared effects first; expand wire details through existing columns."""
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("dry_run") is not True:
        return payload
    requests = data.get("requests")
    if not isinstance(requests, list):
        return payload
    projected = dict(payload)
    preview = dict(data)
    projected["data"] = preview
    preview["execution_order"] = (
        []
        if data.get("no_change") is True
        else [
            {key: request[key] for key in ("method", "path") if key in request}
            for request in requests
            if isinstance(request, dict)
        ]
    )
    preview["observation"] = (
        "Prepared mutation stages only; lookup and verification reads are omitted. "
        "Apply prepares again and may observe changed state. "
        "No server state is reserved."
    )
    if not ({"requests", "*"} & set(options.columns)):
        preview.pop("requests")
        preview["request_details"] = (
            "Repeat this dry-run with --columns requests (or '*')."
        )
    return projected


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
        coverage = _coverage_diagnostic(payload)
        if coverage is not None:
            lines.append(coverage)
    lines.extend(_warning_diagnostics(payload))
    return "\n".join(lines)


def _coverage_diagnostic(payload: JsonObject) -> str | None:
    data = payload.get("data")
    coverage = data.get("coverage") if isinstance(data, dict) else None
    if not isinstance(coverage, dict):
        return None
    parts = [
        f"scope={_format_table_cell(coverage.get('scope'))}",
        f"{_format_cell(coverage.get('rows_read'))} rows / "
        f"{_format_cell(coverage.get('pages_read'))} pages",
        "complete" if coverage.get("scope_complete") is True else "partial",
    ]
    if coverage.get("totals_changed"):
        parts.append("totals changed during reading")
    if coverage.get("atomic_snapshot") is False:
        parts.append("non-atomic observation")
    return "coverage: " + "; ".join(parts)


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
    lines: list[str] = []
    for warning in warnings:
        if not isinstance(warning, Mapping):
            continue
        message = warning.get("message")
        if not isinstance(message, str):
            continue
        code = warning.get("code")
        label = f"warning[{code}]" if isinstance(code, str) and code else "warning"
        lines.append(f"{label}: {message}")
        suggestion = warning.get("suggestion")
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
    if derived_rows is None:
        derived_rows = _document_line_rows(payload, shape=shape)
    rows = derived_rows
    if rows is None:
        rows = _extract_rows(data, shape=shape)
    if rows is not None:
        columns = _resolve_columns(
            rows,
            requested=options.columns,
            defaults=(
                ()
                if shape is None
                or (
                    derived_rows is not None
                    and view != "command"
                    and shape.line_source_path is None
                )
                else shape.default_columns
            ),
            action=action,
            view=view,
        )
        if (
            options.output_format == "table"
            and shape is not None
            and shape.kind == "object"
            and len(rows) == 1
            and any(isinstance(rows[0].get(column), (dict, list)) for column in columns)
        ):
            return _render_object_sections(_project_row(rows[0], columns))
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
                view=view,
            )
            return _render_rows(
                rows,
                columns=columns,
                output_format=options.output_format,
            )
        if data.get("dry_run") is True:
            data = {"resolved": payload.get("resolved"), **data}
        if options.output_format == "table":
            return _render_object_sections(data)
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
    del output_format
    lines: list[str] = []
    error = payload.get("error")
    if isinstance(error, Mapping):
        lines.append(
            f"Error [{_format_table_cell(error.get('type'))}]: "
            f"{_format_table_cell(error.get('message'))}"
        )
        if error.get("suggestion"):
            lines.append(f"Hint: {_format_table_cell(error['suggestion'])}")
        details = error.get("details")
        if isinstance(details, Mapping) and details:
            lines.append(_render_object_sections(details))
        source = error.get("source")
        if isinstance(source, Mapping) and source:
            lines.append(f"Source\n{_render_object_sections(source)}")
    data = payload.get("data")
    if isinstance(data, Mapping) and data:
        lines.append(_render_object_sections(data))
    render_error_details = error.get("details") if isinstance(error, Mapping) else None
    if (
        isinstance(render_error_details, Mapping)
        and render_error_details.get("result_available") is True
    ):
        resolved = payload.get("resolved")
        if isinstance(resolved, Mapping) and resolved:
            lines.append(f"Resolved\n{_render_object_sections(resolved)}")
    return "\n".join(lines)


def _render_object_sections(data: Mapping[str, JsonValue]) -> str:
    """Expand nested details into labeled field views instead of JSON cells."""
    scalar_rows: list[JsonObject] = []
    sections: list[str] = []
    for key, value in data.items():
        if isinstance(value, dict) and value:
            sections.append(
                f"{_format_table_cell(key)}\n{_render_object_sections(value)}"
            )
        elif (
            isinstance(value, list)
            and value
            and any(isinstance(item, (dict, list)) for item in value)
        ):
            rows = _coerce_rows(value) or ()
            if all(
                not isinstance(item, (dict, list))
                for row in rows
                for item in row.values()
            ):
                body = _render_table(rows, columns=_infer_columns(rows))
            else:
                body = "\n\n".join(
                    f"[{index}]\n{_render_object_sections(row)}"
                    for index, row in enumerate(rows)
                )
            sections.append(f"{_format_table_cell(key)}\n{body}")
        else:
            scalar_rows.append({"field": key, "value": value})
    if scalar_rows:
        sections.insert(0, _render_table(scalar_rows, columns=("field", "value")))
    return "\n\n".join(sections) or "(no fields)"


def _project_json_payload(
    payload: JsonObject,
    *,
    action: str,
    columns: tuple[str, ...],
) -> JsonObject:
    """Copy selected data and changed containers; retain read-only metadata."""
    projected = dict(payload)
    view = _result_view(projected, action=action)
    shape = data_shape_for_action(action, view=view)
    document_rows = _document_line_rows(projected, shape=shape)
    if document_rows is not None and shape is not None and shape.row_path is not None:
        replacement = _project_json_value(
            list(document_rows),
            columns=columns,
            action=action,
            view=view,
        )
        if _replace_value_at_path(projected, shape.row_path, replacement):
            _remove_value_at_path(projected, shape.line_source_path)
            return projected
    derived_rows = _derived_rows(projected.get("data"), action=action, view=view)
    if (
        action == "schema"
        and view in {"command", "full_group", "full_command"}
        and derived_rows is not None
    ):
        data = projected.get("data")
        if not isinstance(data, dict):
            raise _projection_not_supported_error(
                action=action,
                columns=columns,
                view=view,
            )
        row_field = "command" if view == "command" else "rows"
        projected["data"] = data_copy = dict(data)
        data_copy[row_field] = _project_json_value(
            list(derived_rows),
            columns=columns,
            action=action,
            view=view,
        )
        return projected
    if shape is not None and shape.row_path is not None:
        value = _value_at_path(projected, shape.row_path)
        replacement = _project_json_value(
            value,
            columns=columns,
            action=action,
            view=view,
        )
        if _replace_value_at_path(projected, shape.row_path, replacement):
            return projected
        raise _projection_not_supported_error(
            action=action,
            columns=columns,
            view=view,
        )

    data = projected.get("data")
    if isinstance(data, Mapping):
        total_list = data.get("totalList")
        if _is_sequence_like(total_list):
            replacement = _project_json_value(
                total_list,
                columns=columns,
                action=action,
                view=view,
            )
            data_copy = dict(data)
            data_copy["totalList"] = replacement
            projected["data"] = data_copy
            return projected
        projected["data"] = _project_json_value(
            data,
            columns=columns,
            action=action,
            view=view,
        )
        return projected

    projected["data"] = _project_json_value(
        data,
        columns=columns,
        action=action,
        view=view,
    )
    return projected


def _validate_data_shape_render_options(
    payload: JsonObject,
    *,
    action: str,
    options: RenderOptions,
) -> None:
    view = _result_view(payload, action=action)
    shape = data_shape_for_action(action, view=view)
    if shape is None:
        return
    if options.output_format not in shape.supported_output_formats:
        message = (
            f"The {view or 'default'} view for {action} is JSON-only and cannot be "
            f"rendered as {options.output_format}."
        )
        raise UserInputError(
            message,
            details={
                "action": action,
                "view": view,
                "output_format": options.output_format,
                "supported_output_formats": list(shape.supported_output_formats),
            },
            suggestion=(
                "Omit --columns and use --format json or json-compact "
                "to preserve the complete document."
            ),
        )
    if options.columns and not shape.column_projection:
        message = (
            f"The {view or 'default'} view for {action} is JSON-only and does not "
            "support --columns."
        )
        raise UserInputError(
            message,
            details={
                "action": action,
                "view": view,
                "columns": list(options.columns),
                "column_projection": False,
            },
            suggestion=(
                "Omit --columns to preserve the complete document; "
                "use --format json-compact for compact JSON."
            ),
        )


def _result_view(payload: JsonObject, *, action: str) -> str | None:
    resolved = payload.get("resolved")
    data = payload.get("data")
    if action == "template.task" and isinstance(data, Mapping):
        return "template" if isinstance(data.get("yaml"), str) else "index"
    if action != "schema":
        resolved_view = resolved.get("view") if isinstance(resolved, Mapping) else None
        if isinstance(resolved_view, str):
            return resolved_view
        data_view = data.get("view") if isinstance(data, Mapping) else None
        return data_view if isinstance(data_view, str) else None

    schema = resolved.get("schema") if isinstance(resolved, Mapping) else None
    scope = schema.get("scope") if isinstance(schema, Mapping) else None
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


def _document_line_rows(
    payload: JsonObject,
    *,
    shape: DataShape | None,
) -> tuple[JsonObject, ...] | None:
    """Derive display rows without storing a second copy of an artifact."""
    if shape is None or shape.line_source_path is None:
        return None
    text = _value_at_path(payload, shape.line_source_path)
    if not isinstance(text, str):
        return None
    return tuple(
        {"line_no": number, "line": line}
        for number, line in enumerate(text.splitlines(), start=1)
    )


def _remove_value_at_path(root: JsonObject, path: str | None) -> None:
    if path is None:
        return
    parent = _copy_path_parent(root, path)
    if parent is not None:
        parent.pop(path.rsplit(".", 1)[-1], None)


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
            return (
                *_schema_capability_rows(data),
                *command_contract_rows(command, action=action_name),
            )
    if view in {"full_group", "full_command"}:
        legacy_rows = _coerce_rows(data.get("rows"))
        if legacy_rows is not None:
            if view == "full_command":
                return (*_schema_capability_rows(data), *legacy_rows)
            return legacy_rows
    return None


def _schema_capability_rows(data: Mapping[str, JsonValue]) -> tuple[JsonObject, ...]:
    """Project selected-version capability facts into schema tabular views."""
    capability = data.get("capability")
    if not isinstance(capability, Mapping):
        return ()
    rows: list[JsonObject] = []
    for field in ("availability", "verification", "constraint"):
        value = capability.get(field)
        if isinstance(value, str):
            rows.append({"kind": "capability", "name": field, "value": value})
    return tuple(rows)


def _project_json_value(
    value: JsonValue | None,
    *,
    columns: tuple[str, ...],
    action: str,
    view: str | None,
) -> JsonValue:
    if isinstance(value, Mapping):
        row = _mapping_to_json_object(value)
        resolved = _resolve_columns(
            (row,),
            requested=columns,
            defaults=(),
            action=action,
            view=view,
        )
        return _project_row(row, resolved)

    if _is_sequence_like(value):
        rows = _json_rows_for_projection(
            value,
            action=action,
            columns=columns,
            view=view,
        )
        resolved = _resolve_columns(
            rows,
            requested=columns,
            defaults=(),
            action=action,
            view=view,
        )
        return [_project_row(row, resolved) for row in rows]

    raise _projection_not_supported_error(action=action, columns=columns, view=view)


def _json_rows_for_projection(
    value: JsonValue | None,
    *,
    action: str,
    columns: tuple[str, ...],
    view: str | None,
) -> tuple[JsonObject, ...]:
    if not _is_sequence_like(value):
        raise _projection_not_supported_error(
            action=action,
            columns=columns,
            view=view,
        )
    rows: list[JsonObject] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise _projection_not_supported_error(
                action=action,
                columns=columns,
                view=view,
            )
        rows.append(_mapping_to_json_object(item))
    return tuple(rows)


def _project_row(row: JsonObject, columns: tuple[str, ...]) -> JsonObject:
    projected: JsonObject = {}
    for column in columns:
        found, value = _column_value(row, column)
        if not found:
            continue
        if column in row:
            projected[column] = deepcopy(value)
            continue
        current = projected
        parts = column.split(".")
        for part in parts[:-1]:
            child = current.get(part)
            if not isinstance(child, dict):
                child = {}
                current[part] = child
            current = child
        current[parts[-1]] = deepcopy(value)
    return projected


def _column_value(row: JsonObject, column: str) -> tuple[bool, JsonValue]:
    """Prefer literal keys, then traverse an explicit dotted object path."""
    if column in row:
        return True, row[column]
    value: JsonValue = row
    for part in column.split("."):
        if not isinstance(value, dict) or part not in value:
            return False, None
        value = value[part]
    return True, value


def _replace_value_at_path(root: JsonObject, path: str, value: JsonValue) -> bool:
    parent = _copy_path_parent(root, path)
    if parent is None:
        return False
    parent[path.rsplit(".", 1)[-1]] = value
    return True


def _copy_path_parent(root: JsonObject, path: str) -> JsonObject | None:
    """Detach containers on an edited path without copying unrelated subtrees."""
    current = root
    for part in path.split(".")[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            return None
        current[part] = child_copy = dict(child)
        current = child_copy
    return current


def _is_sequence_like(value: JsonValue | None) -> TypeGuard[Sequence[JsonValue]]:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _projection_not_supported_error(
    *,
    action: str,
    columns: tuple[str, ...],
    view: str | None,
) -> UserInputError:
    message = f"--columns can only project object or row-oriented output for {action}"
    return UserInputError(
        message,
        details={"action": action, "view": view, "columns": list(columns)},
        suggestion=(
            f"Run `dsctl schema --command {action}` and inspect "
            f"{_data_shape_contract_path(action, view)}, "
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
    view: str | None,
) -> tuple[str, ...]:
    if requested:
        if "*" in requested:
            _validate_wildcard_columns(requested)
            return _infer_columns(rows)
        _validate_requested_columns(rows, requested, action=action, view=view)
        return requested
    identity_columns = _identity_native_default_columns(rows, action=action)
    if identity_columns is not None:
        return identity_columns
    if defaults and _any_column_present(rows, defaults):
        return defaults
    return _infer_columns(rows)


def _identity_native_default_columns(
    rows: Sequence[JsonObject],
    *,
    action: str,
) -> tuple[str, ...] | None:
    if action not in {"task.list", "task.get"} or not rows:
        return None
    if any("code" in row or "id" not in row for row in rows):
        return None
    return tuple(
        column for column in ("id", "name") if any(column in row for row in rows)
    )


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
    view: str | None,
) -> None:
    if not rows:
        return
    missing = [
        column
        for column in columns
        if not any(_column_value(row, column)[0] for row in rows)
    ]
    if not missing:
        return
    message = f"Unknown display column for {action}: {', '.join(missing)}"
    raise UserInputError(
        message,
        details={
            "action": action,
            "view": view,
            "columns": list(columns),
            "unknown_columns": missing,
            "available_columns": _infer_columns(rows),
        },
        suggestion=(
            f"Run `dsctl schema --command {action}` and inspect "
            f"{_data_shape_contract_path(action, view)}, "
            "or retry with columns present in the JSON row payload."
        ),
    )


def _data_shape_contract_path(action: str, view: str | None) -> str:
    if view is None or view not in data_shapes_by_view_schema_for_action(action):
        return "data.command.data_shape"
    return f"data.command.data_shapes_by_view.{view}"


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
    lines = ["\t".join(_format_tsv_cell(column) for column in columns)]
    lines.extend(
        "\t".join(_format_tsv_cell(_column_value(row, column)[1]) for column in columns)
        for row in rows
    )
    return "\n".join(lines)


def _render_table(rows: Sequence[JsonObject], *, columns: tuple[str, ...]) -> str:
    if not columns:
        return "(no rows)"
    rendered_rows = [
        [_format_table_cell(_column_value(row, column)[1]) for column in columns]
        for row in rows
    ]
    headings = tuple(_format_table_cell(column) for column in columns)
    widths = [
        max((cell_len(column), *(cell_len(row[index]) for row in rendered_rows)))
        for index, column in enumerate(headings)
    ]
    header = _render_table_line(headings, widths=widths)
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
            padded.append(value + " " * (widths[index] - cell_len(value)))
    return " | ".join(padded)


_CONTROL_ESCAPES = {code: f"\\x{code:02x}" for code in (*range(32), 127)}
_TABLE_ESCAPES = {**_CONTROL_ESCAPES, 9: "\\t", 10: "\\n", 13: "\\r", 92: "\\\\"}
_TSV_ESCAPES = {**_CONTROL_ESCAPES, 9: " ", 10: " ", 13: " "}


def _format_tsv_cell(value: JsonValue | None) -> str:
    return _format_cell(value).translate(_TSV_ESCAPES)


def _format_table_cell(value: JsonValue | None) -> str:
    """Keep control characters visible without breaking a displayed row."""
    return _format_cell(value).translate(_TABLE_ESCAPES)


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
    "validate_action_render_options",
    "validate_render_options",
]
