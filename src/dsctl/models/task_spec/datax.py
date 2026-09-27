from __future__ import annotations

import json
from typing import ClassVar

from pydantic import (
    Field,
)

from dsctl.models.task_spec.literal_json import (
    _LiteralJsonObjectTaskParamsSpec,
    _validate_literal_json_object,
)

_DATAX_JSON_ROOT_TYPE_ERROR = "datax_json_object_type"
_DATAX_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete DATAX task parameters and launched command; "
    "this field is not secret storage, and dsctl neither detects nor redacts "
    "credentials embedded in the job document."
)


def validate_datax_literal_custom_json(value: str) -> str:
    """Keep one literal DataX JSON-object job without rewriting its spelling."""
    return _validate_literal_json_object(
        value,
        root_type_error=_DATAX_JSON_ROOT_TYPE_ERROR,
    )


def validate_datax_inline_job_presence(
    value: str, *, empty_json_object_is_absent: bool
) -> None:
    """Reject the exact native file-fallback discriminator in the literal facet."""
    if empty_json_object_is_absent and json.loads(value) == {}:
        message = (
            "DATAX task_params.json must contain a nonempty JSON object on this "
            "profile: upstream treats an empty object as absent and requires a "
            "JSON resource file, which is outside the typed literal-job facet."
        )
        raise ValueError(message)


class DataxLiteralCustomJsonJobTaskParamsSpec(_LiteralJsonObjectTaskParamsSpec):
    """One closed literal custom-JSON DataX job document."""

    json_root_type_error: ClassVar[str] = _DATAX_JSON_ROOT_TYPE_ERROR

    json_text: str = Field(
        alias="json",
        min_length=1,
        description=_DATAX_LOGGED_VALUE_DESCRIPTION,
    )
