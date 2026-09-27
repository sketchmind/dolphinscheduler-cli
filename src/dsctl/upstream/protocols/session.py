from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocols.governance import CurrentUserOperations
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire


class ReadUpstreamSession(Protocol):
    """Minimal bound operations for exact-version project/workflow reads."""

    @property
    def definitions(self) -> DefinitionReads:
        """Return the deep project/workflow definition read module."""


class TaskDefinitionUpstreamSession(Protocol):
    """Exact operations required by the deep task-definition module."""

    @property
    def definitions(self) -> DefinitionReads:
        """Return the deep code-native project/workflow resolver."""

    @property
    def task_definitions(self) -> TaskDefinitionWire:
        """Return exact generated task detail and update programs."""


class ReadUpstreamAdapter(Protocol):
    """Protocol for an exact-version generated read dialect."""

    ds_version: str

    def bind_read(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ReadUpstreamSession:
        """Bind only the operations required by supported read actions."""


class IdentityUpstreamAdapter(Protocol):
    """Binding port for an exact-version authenticated-user dialect."""

    def bind_identity(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> CurrentUserOperations:
        """Bind only the current-user operation used by identity checks."""


class TaskDefinitionUpstreamAdapter(Protocol):
    """Binding port for an exact task-definition recipe closure."""

    def bind_task_definitions(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskDefinitionUpstreamSession:
        """Bind selectors and exact generated task programs."""
