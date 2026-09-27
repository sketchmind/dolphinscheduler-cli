from __future__ import annotations

import re
from copy import deepcopy
from typing import TYPE_CHECKING

import pytest

from dsctl.models.task_spec import (
    DINKY_ADDRESS_JSON_SCHEMA_PATTERN,
    DINKY_TASK_ID_JSON_SCHEMA_PATTERN,
    DinkyJobTriggerTaskParamsSpec,
    normalize_typed_task_params,
    task_params_model_for_type,
)
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


_DINKY_VERSIONS = (
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
_DINKY_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)


@pytest.fixture
def refs() -> TaskRefIndex:
    """DINKY has no task-reference fields but shares the projector seam."""
    return TaskRefIndex.from_code_by_name({})


def test_dinky_model_registers_closed_identity_wire_with_explicit_default() -> None:
    authored: YamlObject = {
        "address": "https://dinky.example.com/platform",
        "taskId": "daily-job_7",
    }

    model = task_params_model_for_type("dinky")

    assert model is DinkyJobTriggerTaskParamsSpec
    assert normalize_typed_task_params(model, authored) == {
        **authored,
        "online": False,
    }


def test_dinky_exported_patterns_match_the_literal_subset() -> None:
    for address in (
        "https://dinky.example.com",
        "http://dinky.internal:8888/base",
        "http://[2001:db8::7]:8888",
    ):
        assert re.fullmatch(DINKY_ADDRESS_JSON_SCHEMA_PATTERN, address)
    for address in (
        "HTTPS://dinky.example.com",
        "https://alice:secret@dinky.example.com",
        "https://dinky.example.com?token=value",
        "http://999.999.999.999",
        "http://[:::]",
        "http://[1.2.3.4]",
        "http://" + ".".join(["a" * 63] * 4),
        "${DINKY_ADDRESS}",
    ):
        assert re.fullmatch(DINKY_ADDRESS_JSON_SCHEMA_PATTERN, address) is None
    assert re.fullmatch(DINKY_TASK_ID_JSON_SCHEMA_PATTERN, "daily-job_7")
    for task_id in ("daily job", "${job_id}", "job;id"):
        assert re.fullmatch(DINKY_TASK_ID_JSON_SCHEMA_PATTERN, task_id) is None


@pytest.mark.parametrize(
    "override",
    [
        {"address": "https://alice:secret@dinky.example.com"},
        {"address": "https://dinky.example.com?token=value"},
        {"address": "http://999.999.999.999"},
        {"address": "${DINKY_ADDRESS}"},
        {"taskId": "job;id"},
        {"taskId": "${job_id}"},
        {"online": "false"},
        {"online": 0},
        {"localParams": []},
        {"varPool": []},
        {"futureField": True},
    ],
)
def test_dinky_model_rejects_unsafe_or_unowned_values(
    override: YamlObject,
) -> None:
    authored: YamlObject = {
        "address": "https://dinky.example.com",
        "taskId": "1842",
        **override,
    }

    with pytest.raises(ValueError):
        normalize_typed_task_params(DinkyJobTriggerTaskParamsSpec, authored)


@pytest.mark.parametrize("version", _DINKY_VERSIONS)
def test_dinky_typed_projection_is_exact_identity_in_every_present_version(
    version: str,
    refs: TaskRefIndex,
) -> None:
    canonical: JsonObject = {
        "address": "https://dinky.example.com/platform",
        "taskId": "daily-job_7",
        "online": True,
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="DINKY",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "DINKY"
    assert encoded.task_params == canonical
    assert (
        decode_task_parameters(
            version=version,
            task_type="DINKY",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", _DINKY_VERSIONS)
def test_dinky_projection_restores_missing_online_default(
    version: str,
    refs: TaskRefIndex,
) -> None:
    params: JsonObject = {
        "address": "http://dinky.internal:8888",
        "taskId": "1842",
    }

    assert decode_task_parameters(
        version=version,
        task_type="DINKY",
        task_params=params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params == {**params, "online": False}


@pytest.mark.parametrize("version", _DINKY_ABSENT_VERSIONS)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_dinky_projection_fails_when_task_type_is_absent(
    version: str,
    direction: str,
    refs: TaskRefIndex,
) -> None:
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(
        TaskParameterProjectionError,
        match=f"DINKY does not exist in DolphinScheduler {version}",
    ):
        projector(
            version=version,
            task_type="DINKY",
            task_params={
                "address": "https://dinky.example.com",
                "taskId": "1842",
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "override",
    [
        {"address": "${DINKY_ADDRESS}"},
        {"taskId": "job;id"},
        {"online": "false"},
        {"localParams": []},
        {"futureField": {"native": True}},
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_dinky_typed_projection_never_downgrades_invalid_canonical_values(
    override: JsonObject,
    direction: str,
    refs: TaskRefIndex,
) -> None:
    params: JsonObject = {
        "address": "https://dinky.example.com",
        "taskId": "1842",
        "online": False,
        **override,
    }
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(TaskParameterProjectionError):
        projector(
            version="3.4.2",
            task_type="DINKY",
            task_params=params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "native",
    [
        {
            "address": "${DINKY_ADDRESS}",
            "taskId": "1842",
            "online": False,
        },
        {
            "address": "https://dinky.example.com",
            "taskId": "1842",
            "online": False,
            "localParams": [{"prop": "secret", "value": "${token}"}],
            "varPool": [{"prop": "state", "value": {"attempt": 1}}],
            "futureField": {"nested": ["preserve"]},
        },
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_dinky_opaque_projection_deep_preserves_unsafe_native_state(
    native: JsonObject,
    direction: str,
    refs: TaskRefIndex,
) -> None:
    expected = deepcopy(native)
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    projected = projector(
        version="3.4.2",
        task_type="DINKY",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    native["mutated"] = True

    assert projected.task_params == expected


@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_dinky_opaque_projection_canonicalizes_safe_native_state(
    direction: str,
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "address": "https://dinky.example.com",
        "taskId": "1842",
    }
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    projected = projector(
        version="3.4.2",
        task_type="DINKY",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert projected.task_params == {**native, "online": False}
