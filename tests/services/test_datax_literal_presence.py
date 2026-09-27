from __future__ import annotations

import pytest

from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    encode_task_parameters,
)


@pytest.mark.parametrize(
    "job_value",
    [
        "1" + "0" * 5_000,
        "-1" + "0" * 5_000,
        "1e99999999999999999999",
        "null",
        "{}",
        '" { } "',
    ],
    ids=[
        "large-integer",
        "negative-large-integer",
        "large-exponent",
        "null",
        "object",
        "string",
    ],
)
def test_datax_343_presence_check_preserves_nonempty_literal_jobs(
    job_value: str,
) -> None:
    json_text = '{\n  "job": ' + job_value + "\n}\n"

    normalized = get_task_authoring_catalog("3.4.3").normalize_task_params(
        "DATAX",
        {"json": json_text},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    encoded = encode_task_parameters(
        version="3.4.3",
        task_type="DATAX",
        task_params={"json": json_text},
        refs=TaskRefIndex.from_code_by_name({}),
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert normalized == {"json": json_text}
    assert encoded.task_params == {
        "customConfig": 1,
        "json": json_text,
        "xms": 1,
        "xmx": 1,
    }


@pytest.mark.parametrize(
    "json_text", ["[]", '{"job":}', "\u00a0{}\u00a0", '{"job":NaN}']
)
def test_datax_343_presence_check_retains_literal_json_validation(
    json_text: str,
) -> None:
    with pytest.raises(ValueError, match="json"):
        get_task_authoring_catalog("3.4.3").normalize_task_params(
            "DATAX",
            {"json": json_text},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    with pytest.raises(TaskParameterProjectionError) as error:
        encode_task_parameters(
            version="3.4.3",
            task_type="DATAX",
            task_params={"json": json_text},
            refs=TaskRefIndex.from_code_by_name({}),
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert error.value.details["reason"] == "invalid-canonical-value"
