from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class WorkflowUpdateRequest(BaseContractModel):
    """Workflow update request"""
    name: str | None = Field(default=None, examples=["workflow's name"])
    description: str | None = Field(default=None, examples=["workflow's description"])
    releaseState: str | None = Field(default=None, json_schema_extra={'allowable_values': ['ONLINE', 'OFFLINE']})
    globalParams: str | None = Field(default=None, examples=['[{\\"prop\\":\\"key\\",\\"value\\":\\"value\\",\\"direct\\":\\"IN\\",\\"type\\":\\"VARCHAR\\"}]'])
    warningGroupId: int = Field(default=0, examples=[2])
    timeout: int = Field(default=0, examples=[60])
    executionType: str | None = Field(default=None, description='default PARALLEL if not provide.', json_schema_extra={'allowable_values': ['PARALLEL', 'SERIAL_WAIT', 'SERIAL_DISCARD', 'SERIAL_PRIORITY']})
    location: str | None = Field(default=None, examples=['[{\\\\\\"taskCode\\\\\\":7009653961024,\\\\\\"x\\\\\\":312,\\\\\\"y\\\\\\":196}]'])

__all__ = ["WorkflowUpdateRequest"]
