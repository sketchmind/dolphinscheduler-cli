from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ..._models import BaseContractModel

from ...dao.entities.access_token import AccessToken
from ...dao.entities.alert_group import AlertGroup
from ...dao.entities.command import Command
from ...dao.entities.data_source import DataSource
from ...dao.entities.error_command import ErrorCommand
from ...dao.entities.k8s_namespace import K8sNamespace
from ...dao.entities.project import Project
from ...dao.entities.project_parameter import ProjectParameter
from ...dao.entities.queue import Queue
from ...dao.entities.schedule import Schedule
from ...dao.entities.task_definition import TaskDefinition
from ...dao.entities.task_definition_log import TaskDefinitionLog
from ...dao.entities.task_group import TaskGroup
from ...dao.entities.task_group_queue import TaskGroupQueue
from ...dao.entities.task_instance import TaskInstance
from ...dao.entities.tenant import Tenant
from ...dao.entities.user import User
from ...dao.entities.worker_group_page_detail import WorkerGroupPageDetail
from ...dao.entities.workflow_definition import WorkflowDefinition
from ...dao.entities.workflow_definition_log import WorkflowDefinitionLog
from ...dao.entities.workflow_instance import WorkflowInstance
from ..views.alert_plugin_instance import AlertPluginInstanceVO
from ..views.resource_item import ResourceItemVO
from ..views.schedule import ScheduleVO
from .audit_dto import AuditDto
from .cluster_dto import ClusterDto
from .environment_dto import EnvironmentDto

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list, description='totalList')
    total: int = Field(default=0, description='total')
    totalPage: int | None = Field(default=None, description='total Page')
    pageSize: int = Field(default=20, description='page size')
    currentPage: int | None = Field(default=0, description='current page')
    pageNo: int | None = Field(default=None, description='pageNo')

class PageInfoAccessToken(PageInfo[AccessToken]):
    """Specialized view for PageInfo<AccessToken>."""

class PageInfoAlertGroup(PageInfo[AlertGroup]):
    """Specialized view for PageInfo<AlertGroup>."""

class PageInfoAlertPluginInstanceVO(PageInfo[AlertPluginInstanceVO]):
    """Specialized view for PageInfo<AlertPluginInstanceVO>."""

class PageInfoAuditDto(PageInfo[AuditDto]):
    """Specialized view for PageInfo<AuditDto>."""

class PageInfoClusterDto(PageInfo[ClusterDto]):
    """Specialized view for PageInfo<ClusterDto>."""

class PageInfoCommand(PageInfo[Command]):
    """Specialized view for PageInfo<Command>."""

class PageInfoDataSource(PageInfo[DataSource]):
    """Specialized view for PageInfo<DataSource>."""

class PageInfoEnvironmentDto(PageInfo[EnvironmentDto]):
    """Specialized view for PageInfo<EnvironmentDto>."""

class PageInfoErrorCommand(PageInfo[ErrorCommand]):
    """Specialized view for PageInfo<ErrorCommand>."""

class PageInfoK8sNamespace(PageInfo[K8sNamespace]):
    """Specialized view for PageInfo<K8sNamespace>."""

class PageInfoProject(PageInfo[Project]):
    """Specialized view for PageInfo<Project>."""

class PageInfoProjectParameter(PageInfo[ProjectParameter]):
    """Specialized view for PageInfo<ProjectParameter>."""

class PageInfoQueue(PageInfo[Queue]):
    """Specialized view for PageInfo<Queue>."""

class PageInfoResourceItemVO(PageInfo[ResourceItemVO]):
    """Specialized view for PageInfo<ResourceItemVO>."""

class PageInfoSchedule(PageInfo[Schedule]):
    """Specialized view for PageInfo<Schedule>."""

class PageInfoScheduleVO(PageInfo[ScheduleVO]):
    """Specialized view for PageInfo<ScheduleVO>."""

class PageInfoTaskDefinition(PageInfo[TaskDefinition]):
    """Specialized view for PageInfo<TaskDefinition>."""

class PageInfoTaskDefinitionLog(PageInfo[TaskDefinitionLog]):
    """Specialized view for PageInfo<TaskDefinitionLog>."""

class PageInfoTaskGroup(PageInfo[TaskGroup]):
    """Specialized view for PageInfo<TaskGroup>."""

class PageInfoTaskGroupQueue(PageInfo[TaskGroupQueue]):
    """Specialized view for PageInfo<TaskGroupQueue>."""

class PageInfoTaskInstance(PageInfo[TaskInstance]):
    """Specialized view for PageInfo<TaskInstance>."""

class PageInfoTenant(PageInfo[Tenant]):
    """Specialized view for PageInfo<Tenant>."""

class PageInfoUser(PageInfo[User]):
    """Specialized view for PageInfo<User>."""

class PageInfoWorkerGroupPageDetail(PageInfo[WorkerGroupPageDetail]):
    """Specialized view for PageInfo<WorkerGroupPageDetail>."""

class PageInfoWorkflowDefinition(PageInfo[WorkflowDefinition]):
    """Specialized view for PageInfo<WorkflowDefinition>."""

class PageInfoWorkflowDefinitionLog(PageInfo[WorkflowDefinitionLog]):
    """Specialized view for PageInfo<WorkflowDefinitionLog>."""

class PageInfoWorkflowInstance(PageInfo[WorkflowInstance]):
    """Specialized view for PageInfo<WorkflowInstance>."""

__all__ = ["PageInfo", "PageInfoAccessToken", "PageInfoAlertGroup", "PageInfoAlertPluginInstanceVO", "PageInfoAuditDto", "PageInfoClusterDto", "PageInfoCommand", "PageInfoDataSource", "PageInfoEnvironmentDto", "PageInfoErrorCommand", "PageInfoK8sNamespace", "PageInfoProject", "PageInfoProjectParameter", "PageInfoQueue", "PageInfoResourceItemVO", "PageInfoSchedule", "PageInfoScheduleVO", "PageInfoTaskDefinition", "PageInfoTaskDefinitionLog", "PageInfoTaskGroup", "PageInfoTaskGroupQueue", "PageInfoTaskInstance", "PageInfoTenant", "PageInfoUser", "PageInfoWorkerGroupPageDetail", "PageInfoWorkflowDefinition", "PageInfoWorkflowDefinitionLog", "PageInfoWorkflowInstance"]
