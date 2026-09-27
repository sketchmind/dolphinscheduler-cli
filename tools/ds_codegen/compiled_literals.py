"""Read manifested compiled data without executing candidate Python modules."""

from __future__ import annotations

import ast
from copy import deepcopy
from typing import TYPE_CHECKING

from ds_codegen.compiled_codec_records import CODECS_EXPRESSION

if TYPE_CHECKING:
    from pathlib import Path


def read_compiled_literals(path: Path, names: set[str]) -> dict[str, object]:
    statements = ast.parse(path.read_text(encoding="utf-8"), filename=str(path)).body
    requested = (
        names | {"CODEC_RECORDS", "CODEC_BINDINGS"} if "CODECS" in names else names
    )
    expressions: dict[str, ast.expr] = {}
    for statement in statements:
        assignment = _literal_assignment(statement)
        if assignment is None:
            continue
        target, value = assignment
        if target not in requested:
            continue
        if target in expressions:
            msg = f"{path}: duplicate literal {target}"
            raise ValueError(msg)
        expressions[target] = value
    if set(expressions) & names != names:
        msg = f"{path} literal inventory does not match"
        raise ValueError(msg)
    values: dict[str, object] = {}
    for name in names:
        if name == "CODECS":
            values[name] = _read_codec_records(path, statements, expressions)
            continue
        try:
            values[name] = ast.literal_eval(expressions[name])
        except (ValueError, TypeError) as exc:
            msg = f"{path}: {name} must be literal candidate data"
            raise ValueError(msg) from exc
    return values


def _literal_assignment(statement: ast.stmt) -> tuple[str, ast.expr] | None:
    """Read only assignment values; annotations are never evaluated."""
    if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
        target = statement.targets[0]
        value = statement.value
    elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
        target = statement.target
        value = statement.value
    else:
        return None
    return (target.id, value) if isinstance(target, ast.Name) else None


def _read_codec_records(
    path: Path,
    statements: list[ast.stmt],
    expressions: dict[str, ast.expr],
) -> dict[str, object]:
    expression = expressions["CODECS"]
    pooled = {"CODEC_RECORDS", "CODEC_BINDINGS"} & expressions.keys()
    if not pooled:
        return _codec_literal_mapping(expression, path=path, name="CODECS")
    if pooled != {"CODEC_RECORDS", "CODEC_BINDINGS"}:
        msg = f"{path}: incomplete codec record pool"
        raise ValueError(msg)
    expected = ast.parse(CODECS_EXPRESSION, mode="eval").body
    if ast.dump(expression) != ast.dump(expected):
        msg = f"{path}: CODECS must use the fixed codec record expansion"
        raise ValueError(msg)
    _require_codec_pool_bindings(statements, path=path)
    records = _codec_literal_mapping(
        expressions["CODEC_RECORDS"], path=path, name="CODEC_RECORDS"
    )
    bindings = _codec_literal_mapping(
        expressions["CODEC_BINDINGS"], path=path, name="CODEC_BINDINGS"
    )
    if not all(isinstance(record, dict) for record in records.values()):
        msg = f"{path}: codec records must be mappings"
        raise ValueError(msg)
    if not all(isinstance(key, str) and key in records for key in bindings.values()):
        msg = f"{path}: codec binding references an unknown record"
        raise ValueError(msg)
    if set(bindings.values()) != records.keys():
        msg = f"{path}: codec record pool contains unused records"
        raise ValueError(msg)
    return {name: deepcopy(records[str(key)]) for name, key in bindings.items()}


def _codec_literal_mapping(
    expression: ast.expr, *, path: Path, name: str
) -> dict[str, object]:
    # literal_eval alone silently accepts repeated mapping keys. Reject them
    # before evaluating the data, including fields nested inside codec records.
    for node in ast.walk(expression):
        if not isinstance(node, ast.Dict):
            continue
        keys: set[str] = set()
        for key in node.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                msg = f"{path}: {name} must contain literal string keys"
                raise TypeError(msg)
            if key.value in keys:
                msg = f"{path}: duplicate {name} key {key.value!r}"
                raise ValueError(msg)
            keys.add(key.value)
    try:
        value = ast.literal_eval(expression)
    except (ValueError, TypeError) as exc:
        msg = f"{path}: {name} must be literal candidate data"
        raise ValueError(msg) from exc
    if not isinstance(value, dict):
        msg = f"{path}: {name} must be a literal mapping"
        raise TypeError(msg)
    return value


def _require_codec_pool_bindings(statements: list[ast.stmt], *, path: Path) -> None:
    """Reject mutation or shadowing of the closed pool expansion's symbols."""
    symbols = {"CODEC_RECORDS", "CODEC_BINDINGS", "CODECS", "deepcopy"}
    copy_imports = 0
    for statement in statements:
        if (
            isinstance(statement, ast.ImportFrom)
            and statement.module == "copy"
            and statement.level == 0
            and len(statement.names) == 1
            and statement.names[0].name == "deepcopy"
            and statement.names[0].asname is None
        ):
            copy_imports += 1
            continue
        assignment = _literal_assignment(statement)
        if assignment is not None and assignment[0] in symbols - {"deepcopy"}:
            if isinstance(statement, ast.AnnAssign):
                annotation = (
                    "dict[str, str]"
                    if assignment[0] == "CODEC_BINDINGS"
                    else "dict[str, JsonObject]"
                )
                if ast.dump(statement.annotation) != ast.dump(
                    ast.parse(annotation, mode="eval").body
                ):
                    msg = f"{path}: codec pool annotations must use canonical types"
                    raise ValueError(msg)
            continue
        for node in ast.walk(statement):
            if (
                (isinstance(node, ast.Name) and node.id in symbols)
                or (
                    isinstance(
                        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                    )
                    and node.name in symbols
                )
                or (
                    isinstance(node, ast.alias)
                    and (node.asname or node.name) in symbols
                )
            ):
                msg = f"{path}: codec pool symbols cannot be rebound or mutated"
                raise ValueError(msg)
    if copy_imports != 1:
        msg = f"{path}: codec expansion requires the canonical deepcopy import"
        raise ValueError(msg)
