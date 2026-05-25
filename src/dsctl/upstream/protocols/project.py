from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence


class ProjectRecord(Protocol):
    """Structural project identity shared across services and adapters."""

    @property
    def code(self) -> int | None:
        """Project code used for stable API addressing."""

    @property
    def name(self) -> str | None:
        """Human-facing project name."""

    @property
    def description(self) -> str | None:
        """Optional project description."""


class ProjectPayloadRecord(ProjectRecord, Protocol):
    """Structural project payload returned by upstream project operations."""

    @property
    def code(self) -> int:
        """Project code used for stable API addressing."""

    @property
    def id(self) -> int | None:
        """Project id."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Project owner user id."""

    @property
    def userName(self) -> str | None:  # noqa: N802
        """Project owner user name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Project creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Project update time."""

    @property
    def perm(self) -> int:
        """Permission bitset."""

    @property
    def defCount(self) -> int:  # noqa: N802
        """Workflow definition count."""


class ProjectPageRecord(Protocol):
    """Structural DS paging payload for project list operations."""

    @property
    def totalList(self) -> Sequence[ProjectPayloadRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ProjectOperations(Protocol):
    """Bound project operations exposed to the service layer."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ProjectPageRecord:
        """Return one page of projects visible to the configured user."""

    def get(self, *, code: int) -> ProjectPayloadRecord:
        """Fetch a single project by code."""

    def create(
        self,
        *,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        """Create a project and return the created entity."""

    def update(
        self,
        *,
        code: int,
        name: str,
        description: str | None = None,
    ) -> ProjectPayloadRecord:
        """Update a project and return the updated entity."""

    def delete(self, *, code: int) -> bool:
        """Delete a project by code and return the remote deletion flag."""


class ProjectParameterRecord(Protocol):
    """Structural project-parameter payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project-parameter row id."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Owning user id."""

    @property
    def operator(self) -> int | None:
        """Last operator user id."""

    @property
    def code(self) -> int | None:
        """Stable project-parameter code."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def paramName(self) -> str | None:  # noqa: N802
        """Parameter name."""

    @property
    def paramValue(self) -> str | None:  # noqa: N802
        """Parameter value."""

    @property
    def paramDataType(self) -> str | None:  # noqa: N802
        """Parameter data type label."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""

    @property
    def createUser(self) -> str | None:  # noqa: N802
        """Creation user display name."""

    @property
    def modifyUser(self) -> str | None:  # noqa: N802
        """Last modifier display name."""


class ProjectParameterPageRecord(Protocol):
    """Structural DS paging payload for project-parameter list operations."""

    @property
    def totalList(self) -> Sequence[ProjectParameterRecord] | None:  # noqa: N802
        """Page items."""

    @property
    def total(self) -> int | None:
        """Total remote item count."""

    @property
    def totalPage(self) -> int | None:  # noqa: N802
        """Remote total page count."""

    @property
    def pageSize(self) -> int | None:  # noqa: N802
        """Remote page size."""

    @property
    def currentPage(self) -> int | None:  # noqa: N802
        """Remote current page number."""

    @property
    def pageNo(self) -> int | None:  # noqa: N802
        """Alternate remote page number field."""


class ProjectParameterOperations(Protocol):
    """Bound project-parameter operations exposed to the service layer."""

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        data_type: str | None = None,
    ) -> ProjectParameterPageRecord:
        """Return one page of project parameters for the selected project."""

    def get(self, *, project_code: int, code: int) -> ProjectParameterRecord:
        """Fetch a single project parameter by code."""

    def create(
        self,
        *,
        project_code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        """Create one project parameter and return the created entity."""

    def update(
        self,
        *,
        project_code: int,
        code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        """Update one project parameter and return the updated entity."""

    def delete(self, *, project_code: int, code: int) -> bool:
        """Delete one project parameter by code."""


class ProjectPreferenceRecord(Protocol):
    """Structural project-preference payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project-preference row id."""

    @property
    def code(self) -> int:
        """Project-preference code."""

    @property
    def projectCode(self) -> int:  # noqa: N802
        """Owning project code."""

    @property
    def preferences(self) -> str | None:
        """Stored DS project preference JSON string."""

    @property
    def userId(self) -> int | None:  # noqa: N802
        """Updating user id when available."""

    @property
    def state(self) -> int:
        """Project-preference enabled state."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ProjectPreferenceOperations(Protocol):
    """Bound project-preference operations exposed to the service layer."""

    def get(self, *, project_code: int) -> ProjectPreferenceRecord | None:
        """Fetch the singleton project preference for one selected project."""

    def update(
        self,
        *,
        project_code: int,
        preferences: str,
    ) -> ProjectPreferenceRecord:
        """Create or update the singleton project preference."""

    def set_state(self, *, project_code: int, state: int) -> None:
        """Set the singleton project-preference enabled state."""


class ProjectWorkerGroupRecord(Protocol):
    """Structural project worker-group payload used by project-scoped services."""

    @property
    def id(self) -> int | None:
        """Project worker-group row id."""

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Owning project code."""

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        """Assigned or implied worker-group name."""

    @property
    def createTime(self) -> str | None:  # noqa: N802
        """Creation time."""

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        """Update time."""


class ProjectWorkerGroupOperations(Protocol):
    """Bound project worker-group operations exposed to the service layer."""

    def list(self, *, project_code: int) -> Sequence[ProjectWorkerGroupRecord]:
        """Return the current worker groups reported for one selected project."""

    def set(
        self,
        *,
        project_code: int,
        worker_groups: Sequence[str],
    ) -> None:
        """Replace the explicit worker-group assignment set for one project."""
