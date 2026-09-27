"""Executable request defaults retain their native omission/input distinction."""

from __future__ import annotations

import sys
import warnings
from enum import StrEnum
from types import ModuleType
from typing import TYPE_CHECKING, Literal, get_args, get_origin

import pytest
from pydantic import BaseModel, Field, ValidationError
from tests.codegen.compiled_support import load_schema_pool

from ds_codegen.ir import (
    ContractSnapshot,
    EnumSpec,
    EnumValueSpec,
    OperationSpec,
    ParameterSpec,
)
from ds_codegen.render.package.executable_schema import render_operation_request_params
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def _request(
    java_type: str, default: str, monkeypatch: pytest.MonkeyPatch
) -> type[BaseModel]:
    parameter = ParameterSpec(
        name="value",
        java_type=java_type,
        binding="request_param",
        wire_name="value",
        required=False,
        default_value=default,
        hidden=False,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=None,
    )
    operation = OperationSpec(
        operation_id="ProbeController.request",
        controller="ProbeController",
        method_name="request",
        api_group="probe",
        http_method="POST",
        path="probe",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="void",
        inferred_return_type=None,
        logical_return_type="void",
        response_projection="direct",
        parameters=[parameter],
    )
    enums = (
        [
            EnumSpec(
                name="WarningType",
                import_path="org.apache.dolphinscheduler.common.enums.WarningType",
                documentation=None,
                fields=[],
                json_value_field=None,
                values=[
                    EnumValueSpec(name=name, arguments=[], documentation=None)
                    for name in ("NONE", "ALL")
                ],
            )
        ]
        if java_type == "WarningType"
        else []
    )
    snapshot = ContractSnapshot(
        ds_version="3.4.1",
        operation_count=1,
        enum_count=len(enums),
        dto_count=0,
        model_count=0,
        operations=[operation],
        enums=enums,
        dtos=[],
        models=[],
    )
    rendered = render_operation_request_params(snapshot, operation, class_name="Params")
    load_schema_pool(rendered.pool_modules, monkeypatch)
    module_name = "dsctl.generated.wire_programs._default_types_test"
    module = ModuleType(module_name)
    module.__package__ = "dsctl.generated.wire_programs"
    module.__dict__.update(BaseParamsModel=BaseParamsModel, Field=Field)
    monkeypatch.setitem(sys.modules, module_name, module)
    # Execute only trusted renderer output; the expected model below is independent.
    exec(  # noqa: S102
        compile(rendered.support_source or rendered.source, module_name, "exec"),
        module.__dict__,
    )
    candidate = module.__dict__["Params"]
    assert isinstance(candidate, type)
    assert issubclass(candidate, BaseModel)
    return candidate


class WarningType(StrEnum):
    NONE = "NONE"
    ALL = "ALL"


def _expected(default: str, *, enum: bool) -> type[BaseModel]:
    # This is the pre-refactor native input schema with its unvalidated default.
    # Metadata preserves that original baseline without a type-incorrect assignment.
    class Params(BaseParamsModel):
        value: int | None

    class EnumParams(BaseParamsModel):
        value: WarningType | None

    expected: type[BaseModel] = EnumParams if enum else Params
    expected.model_config["title"] = "Params"
    expected.model_fields["value"].default = default
    expected.model_rebuild(force=True)
    return expected


@pytest.mark.parametrize(
    ("java_type", "default", "valid", "invalid"),
    [
        ("int", "DEFAULT_NOTIFY_GROUP_ID", {"value": "7"}, "DEFAULT_NOTIFY_GROUP_ID"),
        (
            "WarningType",
            "DEFAULT_WARNING_TYPE",
            {"value": "ALL"},
            "DEFAULT_WARNING_TYPE",
        ),
        ("WarningType", "NONE", {"value": "NONE"}, "NOT_A_MEMBER"),
    ],
)
def test_default_types_preserve_omission_input_schema_serialization_and_errors(
    java_type: str,
    default: str,
    valid: JsonObject,
    invalid: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _request(java_type, default, monkeypatch)
    expected = _expected(default, enum=java_type == "WarningType")
    for mode in ("validation", "serialization"):
        assert candidate.model_json_schema(mode=mode) == expected.model_json_schema(
            mode=mode
        )
    annotation = candidate.model_fields["value"].annotation
    assert any(
        get_origin(member) is Literal and get_args(member) == (default,)
        for member in get_args(annotation)
    )
    for payload in ({}, {"value": None}, valid):
        actual = candidate.model_validate(payload)
        previous = expected.model_validate(payload)
        if not payload:
            assert type(actual.__dict__["value"]) is str
            assert actual.__dict__["value"] == default
        expected_wire: JsonObject = (
            {}
            if not payload or payload["value"] is None
            else {"value": 7 if java_type == "int" else payload["value"]}
        )
        assert (
            actual.model_dump(
                by_alias=True, exclude_none=True, exclude_unset=True, mode="json"
            )
            == expected_wire
        )
        for mode in ("python", "json"):
            with warnings.catch_warnings(record=True) as actual_warnings:
                warnings.simplefilter("always")
                actual_dump = actual.model_dump(mode=mode)
            with warnings.catch_warnings(record=True) as expected_warnings:
                warnings.simplefilter("always")
                expected_dump = previous.model_dump(mode=mode)
            assert actual_dump == expected_dump
            assert [str(w.message) for w in actual_warnings] == [
                str(w.message) for w in expected_warnings
            ]
    invalid_payloads: tuple[JsonObject, ...] = (
        {"value": invalid},
        {"value": []},
        {"unsupported": True},
    )
    for invalid_payload in invalid_payloads:
        with pytest.raises(ValidationError) as actual_error:
            candidate.model_validate(invalid_payload)
        with pytest.raises(ValidationError) as previous_error:
            expected.model_validate(invalid_payload)
        assert actual_error.value.errors(
            include_url=False
        ) == previous_error.value.errors(include_url=False)


@pytest.mark.parametrize(
    ("java_type", "default", "expected"), [("int", "7", 7), ("String", "NONE", "NONE")]
)
def test_native_scalar_defaults_need_no_stored_literal_union(
    java_type: str,
    default: str,
    expected: int | str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _request(java_type, default, monkeypatch)
    assert candidate().model_dump() == {"value": expected}
    assert get_args(candidate.model_fields["value"].annotation) == (
        int if java_type == "int" else str,
        type(None),
    )
