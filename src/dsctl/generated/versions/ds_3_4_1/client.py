from __future__ import annotations

from .api.operations._base import SessionLike

from .api.operations.access_token import AccessTokenOperations
from .api.operations.access_token_v2 import AccessTokenV2Operations
from .api.operations.alert_group import AlertGroupOperations
from .api.operations.alert_plugin_instance import AlertPluginInstanceOperations
from .api.operations.audit_log import AuditLogOperations
from .api.operations.cloud import CloudOperations
from .api.operations.cluster import ClusterOperations
from .api.operations.data_analysis import DataAnalysisOperations
from .api.operations.data_source import DataSourceOperations
from .api.operations.dynamic_task_type import DynamicTaskTypeOperations
from .api.operations.environment import EnvironmentOperations
from .api.operations.executor import ExecutorOperations
from .api.operations.fav_task import FavTaskOperations
from .api.operations.k8s_namespace import K8sNamespaceOperations
from .api.operations.logger import LoggerOperations
from .api.operations.login import LoginOperations
from .api.operations.monitor import MonitorOperations
from .api.operations.project import ProjectOperations
from .api.operations.project_parameter import ProjectParameterOperations
from .api.operations.project_preference import ProjectPreferenceOperations
from .api.operations.project_v2 import ProjectV2Operations
from .api.operations.project_worker_group import ProjectWorkerGroupOperations
from .api.operations.queue import QueueOperations
from .api.operations.queue_v2 import QueueV2Operations
from .api.operations.resources import ResourcesOperations
from .api.operations.schedule_v2 import ScheduleV2Operations
from .api.operations.scheduler import SchedulerOperations
from .api.operations.statistics_v2 import StatisticsV2Operations
from .api.operations.task_definition import TaskDefinitionOperations
from .api.operations.task_definition_v2 import TaskDefinitionV2Operations
from .api.operations.task_group import TaskGroupOperations
from .api.operations.task_instance import TaskInstanceOperations
from .api.operations.task_instance_v2 import TaskInstanceV2Operations
from .api.operations.tenant import TenantOperations
from .api.operations.ui_plugin import UiPluginOperations
from .api.operations.users import UsersOperations
from .api.operations.worker_group import WorkerGroupOperations
from .api.operations.workflow_definition import WorkflowDefinitionOperations
from .api.operations.workflow_instance import WorkflowInstanceOperations
from .api.operations.workflow_instance_v2 import WorkflowInstanceV2Operations
from .api.operations.workflow_lineage import WorkflowLineageOperations
from .api.operations.workflow_task_relation import WorkflowTaskRelationOperations
from .api.operations.workflow_task_relation_v2 import WorkflowTaskRelationV2Operations
from .api.operations.workflow_v2 import WorkflowV2Operations

class DS341Client:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        session: SessionLike | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._session = session
        self.access_token = AccessTokenOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.access_token_v2 = AccessTokenV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.alert_group = AlertGroupOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.alert_plugin_instance = AlertPluginInstanceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.audit_log = AuditLogOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.cloud = CloudOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.cluster = ClusterOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.data_analysis = DataAnalysisOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.data_source = DataSourceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.dynamic_task_type = DynamicTaskTypeOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.environment = EnvironmentOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.executor = ExecutorOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.fav_task = FavTaskOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.k8s_namespace = K8sNamespaceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.logger = LoggerOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.login = LoginOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.monitor = MonitorOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.project = ProjectOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.project_parameter = ProjectParameterOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.project_preference = ProjectPreferenceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.project_v2 = ProjectV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.project_worker_group = ProjectWorkerGroupOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.queue = QueueOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.queue_v2 = QueueV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.resources = ResourcesOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.schedule_v2 = ScheduleV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.scheduler = SchedulerOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.statistics_v2 = StatisticsV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.task_definition = TaskDefinitionOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.task_definition_v2 = TaskDefinitionV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.task_group = TaskGroupOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.task_instance = TaskInstanceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.task_instance_v2 = TaskInstanceV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.tenant = TenantOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.ui_plugin = UiPluginOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.users = UsersOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.worker_group = WorkerGroupOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_definition = WorkflowDefinitionOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_instance = WorkflowInstanceOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_instance_v2 = WorkflowInstanceV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_lineage = WorkflowLineageOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_task_relation = WorkflowTaskRelationOperations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_task_relation_v2 = WorkflowTaskRelationV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )
        self.workflow_v2 = WorkflowV2Operations(
            self.base_url,
            self.token,
            session=self._session,
        )

__all__ = ["DS341Client"]
