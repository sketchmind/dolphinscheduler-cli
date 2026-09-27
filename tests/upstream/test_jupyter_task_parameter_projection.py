from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from dsctl.models.task_spec import (
    JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
    JUPYTER_SECRET_PARAMETER_NAMES,
    JupyterNotebookTaskParamsSpec,
    normalize_typed_task_params,
    task_params_model_for_type,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject


_JUPYTER_VERSIONS = (
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)


@pytest.fixture
def refs() -> TaskRefIndex:
    """JUPYTER has no task-reference fields but shares the projector seam."""
    return TaskRefIndex.from_code_by_name({})


def test_jupyter_preinstalled_notebook_model_normalizes_safe_intent() -> None:
    """The public typed-model seam owns one conservative notebook subset."""
    authored: YamlObject = {
        "condaEnvName": "analytics_py311",
        "inputNotePath": "/opt/notebooks/daily_input.ipynb",
        "outputNotePath": "/var/lib/notebooks/daily_output.ipynb",
        "parameters": {"business_date": "2026-08-20", "region": "east"},
        "kernel": "python3",
        "engine": "nbclient",
        "executionTimeout": 600,
        "startTimeout": 90,
    }

    model = task_params_model_for_type("jupyter")

    assert model is JupyterNotebookTaskParamsSpec
    assert normalize_typed_task_params(model, authored) == authored


@pytest.mark.parametrize(
    "override",
    [
        {"condaEnvName": "requirements.txt"},
        {"condaEnvName": "packed.tar.gz"},
        {"condaEnvName": "analytics env"},
        {"inputNotePath": "relative/input.ipynb"},
        {"inputNotePath": "/opt/../input.ipynb"},
        {"inputNotePath": "/opt/input.ipynb;id"},
        {"outputNotePath": "/opt/input.ipynb"},
        {"parameters": {"password": "visible"}},
        {"parameters": {"endpoint": "https://user@example.test/api"}},
        {"parameters": {"region": "east west"}},
        {"parameters": {"region": 7}},
        {"kernel": "python3;id"},
        {"engine": "nb client"},
        {"executionTimeout": "600"},
        {"executionTimeout": True},
        {"startTimeout": 0},
        {"others": "--prepare-only"},
    ],
)
def test_jupyter_model_rejects_unsafe_or_unowned_values(
    override: YamlObject,
) -> None:
    """Typed authoring never feeds unquoted shell slots from unsafe YAML values."""
    authored: YamlObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        **override,
    }

    with pytest.raises(ValueError):
        normalize_typed_task_params(JupyterNotebookTaskParamsSpec, authored)


def test_jupyter_secret_name_check_is_an_exact_bounded_denylist() -> None:
    """Ordinary names containing a denylisted substring are not over-rejected."""
    authored: YamlObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        "parameters": {"tokenizer": "wordpiece"},
    }

    assert (
        normalize_typed_task_params(
            JupyterNotebookTaskParamsSpec,
            authored,
        )
        == authored
    )


def test_jupyter_exported_parameter_key_pattern_matches_secret_policy() -> None:
    """Catalog JSON Schema can reuse the same case-insensitive exact denylist."""
    for name in JUPYTER_SECRET_PARAMETER_NAMES:
        assert re.fullmatch(JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN, name) is None
        assert (
            re.fullmatch(JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN, name.upper())
            is None
        )
    assert re.fullmatch(JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN, "tokenizer")


def test_jupyter_surface_is_exactly_available_from_3_1() -> None:
    """Discovery exposes the shared runtime hazards only where the plugin exists."""
    for version in ("1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6"):
        surface = get_task_authoring_surface(version).jupyter
        assert not surface.available
        assert surface.environment_mode is None
        assert not surface.literal_parameters
        assert not surface.timeouts_as_decimal_strings
        assert not surface.supports_preinstalled_notebook
        assert not surface.authored_values_logged
        assert not surface.failover_supported

    for version in _JUPYTER_VERSIONS:
        surface = get_task_authoring_surface(version).jupyter
        assert surface.available
        assert surface.environment_mode == "preinstalled-conda"
        assert surface.literal_parameters
        assert surface.timeouts_as_decimal_strings
        assert surface.supports_preinstalled_notebook
        assert surface.authored_values_logged
        assert not surface.failover_supported


@pytest.mark.parametrize("version", _JUPYTER_VERSIONS)
def test_jupyter_typed_params_roundtrip_exact_native_wire(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """Typed intent serializes JSON parameters and decimal timeout strings."""
    canonical: JsonObject = {
        "condaEnvName": "analytics_py311",
        "inputNotePath": "/opt/notebooks/daily_input.ipynb",
        "outputNotePath": "/var/lib/notebooks/daily_output.ipynb",
        "parameters": {"region": "east", "business_date": "2026-08-20"},
        "kernel": "python3",
        "engine": "nbclient",
        "executionTimeout": 600,
        "startTimeout": 90,
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="JUPYTER",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "JUPYTER"
    assert encoded.task_params == {
        "condaEnvName": "analytics_py311",
        "inputNotePath": "/opt/notebooks/daily_input.ipynb",
        "outputNotePath": "/var/lib/notebooks/daily_output.ipynb",
        "parameters": '{"business_date":"2026-08-20","region":"east"}',
        "kernel": "python3",
        "engine": "nbclient",
        "executionTimeout": "600",
        "startTimeout": "90",
    }
    assert (
        decode_task_parameters(
            version=version,
            task_type="JUPYTER",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


def test_jupyter_empty_parameters_are_omitted_from_native_wire(
    refs: TaskRefIndex,
) -> None:
    """The canonical empty object maps to the plugin's absent string field."""
    canonical: JsonObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/notebooks/input.ipynb",
        "outputNotePath": "/opt/notebooks/output.ipynb",
        "parameters": {},
    }

    encoded = encode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/notebooks/input.ipynb",
        "outputNotePath": "/opt/notebooks/output.ipynb",
    }
    assert (
        decode_task_parameters(
            version="3.4.1",
            task_type="JUPYTER",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


def test_jupyter_encode_omits_explicit_none_optional_fields(
    refs: TaskRefIndex,
) -> None:
    """Defensive projection never emits null into string-valued plugin fields."""
    encoded = encode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params={
            "condaEnvName": "analytics",
            "inputNotePath": "/opt/notebooks/input.ipynb",
            "outputNotePath": "/opt/notebooks/output.ipynb",
            "parameters": {},
            "kernel": None,
            "engine": None,
            "executionTimeout": None,
            "startTimeout": None,
        },
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/notebooks/input.ipynb",
        "outputNotePath": "/opt/notebooks/output.ipynb",
    }


@pytest.mark.parametrize(
    "version",
    ["1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6"],
)
def test_jupyter_typed_projection_rejects_absent_epochs(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """A selected pre-3.1 profile fails before any JUPYTER wire is emitted."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="JUPYTER",
            task_params={},
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "encode",
        "task_type": "JUPYTER",
        "field": "task.type",
        "reason": "task-type-absent-in-version",
    }


@pytest.mark.parametrize(
    ("direction", "unowned_field"),
    [
        ("encode", "others"),
        ("encode", "resourceList"),
        ("decode", "localParams"),
        ("decode", "varPool"),
    ],
)
def test_jupyter_typed_projection_rejects_unowned_native_fields(
    direction: str,
    unowned_field: str,
    refs: TaskRefIndex,
) -> None:
    """Resource-backed setup, raw options, and shared runtime state stay opaque."""
    payload: JsonObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/notebooks/input.ipynb",
        "outputNotePath": "/opt/notebooks/output.ipynb",
        unowned_field: [],
    }
    project = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(TaskParameterProjectionError) as captured:
        project(
            version="3.4.1",
            task_type="JUPYTER",
            task_params=payload,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == f"task_params.{unowned_field}"
    assert captured.value.details["reason"] == "outside-reviewed-notebook-subset"


def test_jupyter_opaque_preserve_is_identity_for_complex_native_state(
    refs: TaskRefIndex,
) -> None:
    """Untyped exports retain resource and arbitrary papermill option evidence."""
    native: JsonObject = {
        "condaEnvName": "packed.tar.gz",
        "inputNotePath": "relative/input.ipynb",
        "outputNotePath": "relative/output.ipynb",
        "parameters": '{"password":"visible"}',
        "others": "--prepare-only",
        "resourceList": [{"id": 7, "res": "packed.tar.gz"}],
        "localParams": [{"prop": "name", "value": "value"}],
    }

    encoded = encode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    decoded = decode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert encoded.task_params == native
    assert decoded.task_params == native


def test_jupyter_opaque_preserve_keeps_future_object_state_lossless(
    refs: TaskRefIndex,
) -> None:
    """A future object-valued parameter field plus unknown state is not canonical."""
    native: JsonObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        "parameters": {"future": {"nested": True}},
        "bootstrapConfig": {"image": "future/runtime"},
    }

    encoded = encode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    decoded = decode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert encoded.task_params == native
    assert decoded.task_params == native


def test_jupyter_opaque_safe_native_roundtrip_uses_canonical_edit_shape(
    refs: TaskRefIndex,
) -> None:
    """Representable exports decode for editing and re-encode to the exact wire."""
    native: JsonObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        "parameters": '{"business_date":"2026-08-20"}',
        "kernel": "python3",
        "executionTimeout": "600",
        "startTimeout": "90",
    }

    decoded = decode_task_parameters(
        version="3.4.1",
        task_type="JUPYTER",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        "parameters": {"business_date": "2026-08-20"},
        "kernel": "python3",
        "executionTimeout": 600,
        "startTimeout": 90,
    }
    assert (
        encode_task_parameters(
            version="3.4.1",
            task_type="JUPYTER",
            task_params=decoded.task_params,
            refs=refs,
            source=ProjectionSource.OPAQUE_PRESERVE,
        ).task_params
        == native
    )


@pytest.mark.parametrize(
    ("payload_override", "drop_field", "expected_field"),
    [
        ({}, "condaEnvName", "task_params.condaEnvName"),
        ({"condaEnvName": "packed.tar.gz"}, None, "task_params.condaEnvName"),
        (
            {"inputNotePath": "relative/input.ipynb"},
            None,
            "task_params.inputNotePath",
        ),
        (
            {"outputNotePath": "/opt/input.ipynb"},
            None,
            "task_params.outputNotePath",
        ),
        (
            {"parameters": '{"region":"east","region":"west"}'},
            None,
            "task_params.parameters",
        ),
        (
            {"parameters": '{"password":"visible"}'},
            None,
            "task_params.parameters.password",
        ),
        (
            {"parameters": '{"endpoint":"https://user@example.test/api"}'},
            None,
            "task_params.parameters.endpoint",
        ),
        (
            {"parameters": '{"region":"east west"}'},
            None,
            "task_params.parameters.region",
        ),
        ({"kernel": "python3;id"}, None, "task_params.kernel"),
        ({"executionTimeout": "060"}, None, "task_params.executionTimeout"),
        ({"startTimeout": 90}, None, "task_params.startTimeout"),
    ],
)
def test_jupyter_typed_decode_rejects_unrepresentable_native_values(
    payload_override: JsonObject,
    drop_field: str | None,
    expected_field: str,
    refs: TaskRefIndex,
) -> None:
    """Typed decode accepts only exact wire values inside the reviewed subset."""
    native: JsonObject = {
        "condaEnvName": "analytics",
        "inputNotePath": "/opt/input.ipynb",
        "outputNotePath": "/opt/output.ipynb",
        **payload_override,
    }
    if drop_field is not None:
        del native[drop_field]

    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="3.4.1",
            task_type="JUPYTER",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["direction"] == "decode"
    assert captured.value.details["field"] == expected_field


def test_jupyter_typed_encode_defensively_rejects_unsafe_canonical_values(
    refs: TaskRefIndex,
) -> None:
    """The projector remains fail-closed even when callers bypass the model seam."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.1",
            task_type="JUPYTER",
            task_params={
                "condaEnvName": "analytics",
                "inputNotePath": "/opt/input.ipynb",
                "outputNotePath": "/opt/output.ipynb",
                "parameters": {"region": "east west"},
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.parameters.region"
