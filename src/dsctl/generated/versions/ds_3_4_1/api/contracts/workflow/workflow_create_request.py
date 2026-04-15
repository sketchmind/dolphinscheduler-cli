from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class WorkflowCreateRequest(BaseContractModel):
    """Workflow create request"""
    name: str = Field(examples=['workflow name'])
    description: str | None = Field(default=None, examples=["workflow's description"])
    projectCode: int = Field(examples=[12345])
    releaseState: str | None = Field(default=None, description='default OFFLINE if not provide.', json_schema_extra={'allowable_values': ['ONLINE', 'OFFLINE']})
    globalParams: str | None = Field(default=None, examples=['[{\\"prop\\":\\"key\\",\\"value\\":\\"value\\",\\"direct\\":\\"IN\\",\\"type\\":\\"VARCHAR\\"}]'])
    warningGroupId: int = Field(default=0, examples=[2])
    timeout: int = Field(default=0, examples=[60])
    executionType: str | None = Field(default=None, description='default PARALLEL if not provide.', json_schema_extra={'allowable_values': ['PARALLEL', 'SERIAL_WAIT', 'SERIAL_DISCARD', 'SERIAL_PRIORITY']})

__all__ = ["WorkflowCreateRequest"]
