"""Closed codec pooling preserves exact values without evaluating candidates."""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_codec_records import CODECS_EXPRESSION, render_codec_records
from ds_codegen.compiled_literals import read_compiled_literals

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.support.json_types import JsonObject, JsonValue


def _rendered() -> str:
    first: JsonObject = {
        "method": "POST",
        "path": "projects/{projectCode}/tasks",
        "fields": [{"name": "projectCode", "binding": "path_variable"}],
        "capture": None,
    }
    return (
        "from __future__ import annotations\nfrom copy import deepcopy\n"
        + render_codec_records(
            (("create_2_0_0", first), ("create_2_0_9", dict(reversed(first.items()))))
        )
    )


def test_rendered_codec_pool_retains_keys_values_and_independent_nested_records(
    tmp_path: Path,
) -> None:
    source = _rendered()
    path = tmp_path / "domain.py"
    path.write_text(source)
    literals = read_compiled_literals(
        path, {"CODEC_RECORDS", "CODEC_BINDINGS", "CODECS"}
    )
    assert literals["CODEC_BINDINGS"] == {
        "create_2_0_0": "create_2_0_0",
        "create_2_0_9": "create_2_0_0",
    }
    pool = literals["CODEC_RECORDS"]
    assert isinstance(pool, dict)
    assert len(pool) == 1
    namespace: dict[str, object] = {}
    # Only this test's trusted renderer output is executed; candidate evidence
    # always crosses the separate, non-executing static reader above.
    exec(compile(source, str(path), "exec"), namespace)  # noqa: S102
    codecs = namespace["CODECS"]
    assert codecs == literals["CODECS"]
    assert isinstance(codecs, dict)
    assert codecs["create_2_0_0"] == codecs["create_2_0_9"]
    assert codecs["create_2_0_0"] is not codecs["create_2_0_9"]
    codecs["create_2_0_0"]["fields"][0]["binding"] = "changed"
    assert codecs["create_2_0_9"]["fields"][0]["binding"] == "path_variable"
    assert namespace["CODEC_RECORDS"] == literals["CODEC_RECORDS"]
    assert _rendered() == source


@pytest.mark.parametrize(
    ("first", "second"), [(True, 1), (False, 0), (1, 1.0), (["a", "b"], ["b", "a"])]
)
def test_codec_pool_does_not_merge_distinct_json_values(
    tmp_path: Path, first: JsonValue, second: JsonValue
) -> None:
    records = (("first", {"value": first}), ("second", {"value": second}))
    # The parameterized values above are all JSON-compatible boundary values.
    source = "from copy import deepcopy\n" + render_codec_records(records)
    path = tmp_path / "domain.py"
    path.write_text(source)
    assert read_compiled_literals(path, {"CODEC_BINDINGS"})["CODEC_BINDINGS"] == {
        "first": "first",
        "second": "second",
    }


def test_codec_renderer_rejects_duplicate_exact_keys() -> None:
    with pytest.raises(ValueError, match="identity 'first' is duplicated"):
        render_codec_records((("first", {}), ("first", {})))


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("CODEC_RECORDS = {'a': {}, 'a': {}}", "duplicate CODEC_RECORDS key"),
        (
            "CODEC_RECORDS = {'a': {'method': 'GET', 'method': 'POST'}}",
            "duplicate CODEC_RECORDS key",
        ),
        (
            "CODEC_RECORDS = {'a': {'fields': [{'name': 'id', 'name': 'code'}]}}",
            "duplicate CODEC_RECORDS key",
        ),
        ("CODEC_RECORDS = {'a': {}, 'unused': {}}", "unused records"),
        ("CODEC_RECORDS = {'a': []}", "records must be mappings"),
        ("CODEC_RECORDS = {'a': dict()}", "literal candidate data"),
        ("CODEC_RECORDS = {**{'a': {}}}", "literal string keys"),
        (
            "CODEC_BINDINGS = {'exact': 'a', 'exact': 'a'}",
            "duplicate CODEC_BINDINGS key",
        ),
        ("CODEC_BINDINGS = {'exact': 'missing'}", "unknown record"),
        ("CODEC_BINDINGS = {'exact': ['a']}", "unknown record"),
        (
            "CODEC_BINDINGS = {name: 'a' for name in ['exact']}",
            "literal candidate data",
        ),
        ("CODECS = {'exact': CODEC_RECORDS['a']}", "fixed codec record expansion"),
        ("CODECS = eval('CODEC_RECORDS')", "fixed codec record expansion"),
        (
            "CODECS = {name: dict(CODEC_RECORDS[record]) "
            "for name, record in CODEC_BINDINGS.items()}",
            "fixed codec record expansion",
        ),
        (
            "CODECS = {name: deepcopy(CODEC_RECORDS[record]) "
            "for name, record in CODEC_BINDINGS.items() if name}",
            "fixed codec record expansion",
        ),
    ],
)
def test_static_codec_reader_rejects_nonliteral_or_noncanonical_records(
    tmp_path: Path, source: str, message: str
) -> None:
    assignment = source.split(" = ", 1)[0]
    statements = {
        "CODEC_RECORDS": "CODEC_RECORDS = {'a': {}}",
        "CODEC_BINDINGS": "CODEC_BINDINGS = {'exact': 'a'}",
        "CODECS": "CODECS = " + CODECS_EXPRESSION,
    }
    statements[assignment] = source
    path = tmp_path / "candidate.py"
    path.write_text("from copy import deepcopy\n" + "\n".join(statements.values()))
    with pytest.raises((TypeError, ValueError), match=message):
        read_compiled_literals(path, {"CODECS"})


@pytest.mark.parametrize(
    "extra",
    [
        "CODEC_RECORDS['create_2_0_0'] = {}",
        "CODEC_BINDINGS.update({'other': 'create_2_0_0'})",
        "del CODECS",
        "deepcopy = dict",
        "def deepcopy(value):\n    return value",
        "from other import deepcopy",
        "from copy import deepcopy",
        "alias = CODEC_RECORDS",
        "def mutate():\n    CODECS.clear()",
    ],
)
def test_static_codec_reader_rejects_pool_mutation_and_shadowed_copy(
    tmp_path: Path, extra: str
) -> None:
    path = tmp_path / "candidate.py"
    path.write_text(_rendered() + "\n" + extra + "\n")
    with pytest.raises(
        ValueError, match=r"codec pool symbols|canonical deepcopy import"
    ):
        read_compiled_literals(path, {"CODECS"})


def test_static_codec_reader_requires_both_pool_literals_and_copy_import(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidate.py"
    source = _rendered()
    statements = ast.parse(source).body
    binding = next(
        statement
        for statement in statements
        if isinstance(statement, ast.AnnAssign)
        and isinstance(statement.target, ast.Name)
        and statement.target.id == "CODEC_BINDINGS"
    )
    lines = source.splitlines(keepends=True)
    del lines[binding.lineno - 1 : binding.end_lineno]
    path.write_text("".join(lines))
    with pytest.raises(ValueError, match="incomplete codec record pool"):
        read_compiled_literals(path, {"CODECS"})
    path.write_text(source.replace("from copy import deepcopy\n", ""))
    with pytest.raises(ValueError, match="canonical deepcopy import"):
        read_compiled_literals(path, {"CODECS"})


def test_static_codec_reader_retains_literal_fixture_support(tmp_path: Path) -> None:
    path = tmp_path / "candidate.py"
    path.write_text("CODECS = {'legacy': {'method': 'GET'}}")
    assert read_compiled_literals(path, {"CODECS"}) == {
        "CODECS": {"legacy": {"method": "GET"}}
    }


@pytest.mark.parametrize("name", ["CODEC_RECORDS", "CODEC_BINDINGS", "CODECS"])
def test_static_codec_reader_rejects_duplicate_top_level_definitions(
    tmp_path: Path, name: str
) -> None:
    path = tmp_path / "candidate.py"
    path.write_text(_rendered() + f"\n{name} = {{}}\n")
    with pytest.raises(ValueError, match=f"duplicate literal {name}"):
        read_compiled_literals(path, {"CODECS"})


def test_static_codec_reader_never_executes_candidate_code(tmp_path: Path) -> None:
    sentinel = tmp_path / "executed"
    path = tmp_path / "candidate.py"
    path.write_text(
        "from copy import deepcopy\n"
        "CODEC_RECORDS = {'a': {}}\n"
        "CODEC_BINDINGS = {'exact': 'a'}\n"
        f"CODECS = __import__('pathlib').Path({str(sentinel)!r}).touch()\n"
    )
    with pytest.raises(ValueError, match="fixed codec record expansion"):
        read_compiled_literals(path, {"CODECS"})
    assert not sentinel.exists()


def test_static_codec_reader_rejects_executable_pool_annotations(
    tmp_path: Path,
) -> None:
    sentinel = tmp_path / "executed"
    path = tmp_path / "candidate.py"
    source = _rendered().replace(
        "CODECS: dict[str, JsonObject]",
        f"CODECS: __import__('pathlib').Path({str(sentinel)!r}).touch()",
    )
    path.write_text(source)
    with pytest.raises(ValueError, match="annotations must use canonical types"):
        read_compiled_literals(path, {"CODECS"})
    assert not sentinel.exists()
