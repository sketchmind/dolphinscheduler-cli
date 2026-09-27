from __future__ import annotations

from typing import ClassVar

from pydantic import (
    Field,
)

from dsctl.models.task_spec.literal_json import (
    _LiteralJsonObjectTaskParamsSpec,
)

_CHUNJUN_JSON_ROOT_TYPE_ERROR = "chunjun_json_object_type"


_CHUNJUN_LOGGED_VALUE_DESCRIPTION = (
    "Upstream logs the complete ChunJun task parameters at INFO and the expanded "
    "job JSON at DEBUG; this field is not secret storage, and dsctl neither "
    "detects nor redacts secrets."
)


class ChunJunLiteralLocalJsonJobTaskParamsSpec(_LiteralJsonObjectTaskParamsSpec):
    """One closed literal worker-local ChunJun JSON job document."""

    json_root_type_error: ClassVar[str] = _CHUNJUN_JSON_ROOT_TYPE_ERROR

    json_text: str = Field(
        alias="json",
        min_length=1,
        description=_CHUNJUN_LOGGED_VALUE_DESCRIPTION,
    )
