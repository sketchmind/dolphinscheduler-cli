from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

class K8sNamespace(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None, description='cluster code')
    namespace: str | None = Field(default=None, description='namespace name')
    userId: int = Field(default=0, description='owner')
    userName: str | None = Field(default=None, description='user name')
    createTime: str | None = Field(default=None, description='create_time')
    updateTime: str | None = Field(default=None, description='update_time')
    clusterCode: int | None = Field(default=None, description='cluster code')
    clusterName: str | None = Field(default=None, description='k8s name')

__all__ = ["K8sNamespace"]
