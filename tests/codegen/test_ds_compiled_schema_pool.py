"""Enum implementation equality never implies equal exact schema ownership."""

from __future__ import annotations

import ast
import sys
from dataclasses import replace
from types import ModuleType

import pytest
from pydantic import ValidationError
from tests.codegen.compiled_support import load_schema_pool

from ds_codegen.compiled_schema_pool import (
    PooledSchemaSource,
    pool_schema_enums,
    unique_schema_modules,
)
from ds_codegen.ir import ContractSnapshot, EnumSpec, EnumValueSpec, OperationSpec
from ds_codegen.render.package import executable_schema

_ENUM = """class State(StrEnum):
    code: int

    def __new__(cls, value: str, code: int) -> State:
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.code = code
        return obj

    FIRST = ('first', 1)
    SECOND = ('second', 2)

    @classmethod
    def from_code(cls, code: int) -> "State":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown State code: {code}")
"""
_SOURCE = (
    "from __future__ import annotations\nfrom enum import StrEnum\n"
    "from pydantic import BaseModel\n\n"
    + _ENUM
    + "\nclass Payload(BaseModel):\n    state: State\n\n"
    "__all__ = ['State', 'Payload']\n"
)
_PARTS = ("wire_programs", "_schemas", "probe", "response")


def _load(source: str, name: str, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    module = ModuleType(f"dsctl.generated.wire_programs._schemas.probe.{name}")
    module.__package__ = "dsctl.generated.wire_programs._schemas.probe"
    monkeypatch.setitem(sys.modules, module.__name__, module)
    exec(compile(source, f"<{name}>", "exec"), module.__dict__)  # noqa: S102
    return module


def test_pool_keeps_public_schema_errors_members_and_helpers_identical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pooled = pool_schema_enums(_SOURCE, module_parts=_PARTS)
    assert len(pooled.modules) == 1
    load_schema_pool(pooled.modules, monkeypatch)
    old = _load(_SOURCE, "old", monkeypatch)
    new = _load(pooled.source, "new", monkeypatch)
    sibling = _load(pooled.source, "sibling", monkeypatch)
    assert new.State is sibling.State
    assert new.Payload is not sibling.Payload
    assert new.State.__name__ == old.State.__name__ == "State"
    assert new.State.__module__ != old.State.__module__
    assert new.__all__ == old.__all__ == ["State", "Payload"]
    for mode in ("validation", "serialization"):
        assert new.Payload.model_json_schema(
            mode=mode
        ) == old.Payload.model_json_schema(mode=mode)
    assert [(member.name, member.value, member.code) for member in new.State] == [
        ("FIRST", "first", 1),
        ("SECOND", "second", 2),
    ]
    assert new.State.from_code(2) is new.State.SECOND
    assert new.Payload.model_validate({"state": "first"}).model_dump(mode="json") == {
        "state": "first"
    }
    for payload in ({"state": "missing"}, {"state": 1}, {}):
        with pytest.raises(ValidationError) as actual:
            new.Payload.model_validate(payload)
        with pytest.raises(ValidationError) as expected:
            old.Payload.model_validate(payload)
        assert actual.value.errors(include_url=False) == expected.value.errors(
            include_url=False
        )


@pytest.mark.parametrize(
    "changed",
    [
        _SOURCE.replace("State", "DifferentState"),
        _SOURCE.replace("StrEnum", "Enum"),
        _SOURCE.replace("('first', 1)", "('first', 10)"),
        _SOURCE.replace(
            "    FIRST = ('first', 1)\n    SECOND = ('second', 2)",
            "    SECOND = ('second', 2)\n    FIRST = ('first', 1)",
        ),
        _SOURCE.replace("obj.code = code", "obj.code = code + 1"),
        _SOURCE.replace("member.code == code", "member.code != code"),
    ],
)
def test_name_base_values_order_constructor_and_helpers_are_all_identity(
    changed: str,
) -> None:
    original = pool_schema_enums(_SOURCE, module_parts=_PARTS)
    different = pool_schema_enums(changed, module_parts=_PARTS)
    assert len(original.modules) == len(different.modules) == 1
    assert original.modules[0].name != different.modules[0].name


@pytest.mark.parametrize(
    "source",
    [
        _SOURCE.replace("from __future__ import annotations\n", ""),
        _SOURCE.replace("StrEnum\n", "StrEnum\nfrom other import StrEnum\n"),
        _SOURCE.replace("class State", "str = custom_string\nclass State"),
        _SOURCE.replace("('first', 1)", "('first', DEFAULT_CODE)"),
        _SOURCE.replace("code: int", "code: ExternalCode"),
        _SOURCE.replace('-> "State"', '-> "ExternalState"'),
        _SOURCE.replace('-> "State"', '-> "invalid annotation !"'),
        _SOURCE.replace("return obj", "obj.owner = cls.__module__\n        return obj"),
        _SOURCE.replace("class State", "@external_decorator\nclass State"),
        _SOURCE.replace("return obj", "import external\n        return obj"),
        _SOURCE.replace("class State(StrEnum):", "class State(StrEnum, ExternalBase):"),
    ],
)
def test_external_dependencies_and_module_sensitive_enums_stay_inline(
    source: str,
) -> None:
    result = pool_schema_enums(source, module_parts=_PARTS)
    assert result.source == source
    assert result.modules == ()


def test_formatting_only_changes_share_modules_and_hash_conflicts_fail_closed() -> None:
    original = pool_schema_enums(_SOURCE, module_parts=_PARTS)
    formatted = pool_schema_enums(ast.unparse(ast.parse(_SOURCE)), module_parts=_PARTS)
    assert original.modules == formatted.modules
    assert (
        unique_schema_modules((*original.modules, *formatted.modules))
        == original.modules
    )
    conflict = replace(original.modules[0], content="CHANGED = True\n")
    with pytest.raises(ValueError, match="content collision"):
        unique_schema_modules((*original.modules, conflict))


def _response() -> tuple[ContractSnapshot, OperationSpec]:
    operation = OperationSpec(
        operation_id="ProbeController.get",
        controller="ProbeController",
        method_name="get",
        api_group="probe",
        http_method="GET",
        path="probe",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="State",
        inferred_return_type=None,
        logical_return_type="State",
        response_projection="direct",
        parameters=[],
    )
    enum = EnumSpec(
        name="State",
        import_path="org.apache.dolphinscheduler.common.enums.State",
        documentation=None,
        fields=[],
        json_value_field=None,
        values=[EnumValueSpec(name="FIRST", arguments=[], documentation=None)],
    )
    return ContractSnapshot(
        ds_version="3.4.1",
        operation_count=1,
        enum_count=1,
        dto_count=0,
        model_count=0,
        operations=[operation],
        enums=[enum],
        dtos=[],
        models=[],
    ), operation


def test_root_executable_digest_tracks_pool_and_retains_original_closure_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot, operation = _response()
    rendered = executable_schema.render_operation_response_module(
        snapshot,
        operation,
        module_parts=_PARTS,
        root_model_module_parts=("wire_runtime", "_models"),
    )
    assert rendered.pool_modules
    monkeypatch.setattr(
        executable_schema,
        "pool_schema_enums",
        lambda source, **_kwargs: PooledSchemaSource(source, ()),
    )
    original = executable_schema.render_operation_response_module(
        snapshot,
        operation,
        module_parts=_PARTS,
        root_model_module_parts=("wire_runtime", "_models"),
    )
    assert rendered.import_paths == original.import_paths
    assert rendered.executable_digest != original.executable_digest
    assert f"SOURCE_CLOSURE_DIGEST = {original.executable_digest!r}" in rendered.source
    assert rendered.executable_digest == executable_schema.executable_schema_digest(
        kind="response",
        source=rendered.source,
        root_annotation=rendered.adapter_annotation,
    )
    assert rendered.pool_modules[0].name in rendered.source
