from __future__ import annotations

from pydantic import Field
from ....wire_runtime._models import BaseViewModel

class UserSimpleInfoVO(BaseViewModel):
    id: int | None = Field(default=None)
    userName: str | None = Field(default=None)

__all__ = ["UserSimpleInfoVO"]

UserSimpleInfoVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = list[UserSimpleInfoVO]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:53e9c9b24b75374adf5cbf8117818c0b7e9ce1215d333af942ead3fa5044b151'
