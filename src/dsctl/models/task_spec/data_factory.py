from __future__ import annotations

import re
from typing import Annotated

from pydantic import (
    AfterValidator,
    ConfigDict,
    Field,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)

_DATA_FACTORY_NAME_EDGE_WHITESPACE = (
    r"\x09\x0a\x0b\x0c\x0d\x20\x85\xa0\u1680\u2000-\u200a"
    r"\u2028\u2029\u202f\u205f\u3000\ufeff"
)
DATA_FACTORY_NAME_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S])(?![{_DATA_FACTORY_NAME_EDGE_WHITESPACE}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?![\s\S]*[{_DATA_FACTORY_NAME_EDGE_WHITESPACE}](?![\s\S]))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*"
    r"(?![\s\S])"
)
_DATA_FACTORY_NAME_PATTERN = re.compile(DATA_FACTORY_NAME_JSON_SCHEMA_PATTERN)


def validate_data_factory_name(value: str) -> str:
    """Validate one literal Azure Data Factory identity without rewriting it."""
    if not _DATA_FACTORY_NAME_PATTERN.fullmatch(value):
        message = (
            "Azure Data Factory names must be nonblank literal strings without "
            "edge whitespace, controls, surrogates, or DS placeholders"
        )
        raise ValueError(message)
    return value


_DataFactoryLiteralName = Annotated[
    str,
    Field(
        min_length=1,
        json_schema_extra={"pattern": DATA_FACTORY_NAME_JSON_SCHEMA_PATTERN},
    ),
    AfterValidator(validate_data_factory_name),
]


class DataFactoryPipelineTriggerTaskParamsSpec(TaskParamsSpec):
    """Closed identity of one existing Azure Data Factory pipeline."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    factory_name: _DataFactoryLiteralName = Field(alias="factoryName")
    resource_group_name: _DataFactoryLiteralName = Field(alias="resourceGroupName")
    pipeline_name: _DataFactoryLiteralName = Field(alias="pipelineName")
