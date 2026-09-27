from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.response_projection import (
    non_empty_text,
    optional_int_field,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    require_none,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        ProjectParameterOperations,
        ProjectParameterPageRecord,
        ProjectParameterRecord,
    )
    from dsctl.upstream.wire import CompiledWireProfile


_RESOURCE = "project-parameter"
_INTRODUCED_IN = "3.2.0"
_Primitive = Literal["page", "get", "create", "update", "delete"]
_PROJECT_PARAMETER_PROGRAMS = CompiledDomainPrograms[_Primitive](
    name="project_parameter",
    schema_constant="COMPILED_PROJECT_PARAMETER_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "get": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "update": MUTATION_ONCE_REQUIRED,
        "delete": MUTATION_ONCE_REQUIRED,
    },
)
_LEGACY_DATA_TYPE = "VARCHAR"


@dataclass(frozen=True)
class ProjectParameterDomain:
    """Project resolution plus one exact project-parameter lifecycle."""

    definitions: DefinitionReads
    parameters: ProjectParameterOperations


@dataclass(frozen=True)
class ProjectParameterSnapshot:
    """Version-neutral parameter projection consumed by stable services."""

    id: int | None
    userId: int | None  # noqa: N815
    operator: int | None
    code: int
    projectCode: int  # noqa: N815
    paramName: str  # noqa: N815
    paramValue: str | None  # noqa: N815
    paramDataType: str  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    createUser: str | None  # noqa: N815
    modifyUser: str | None  # noqa: N815


class ProjectParameterAdapter:
    """Compiled project-parameter adapter for every supporting profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed recipe."""
        self._profile = _PROJECT_PARAMETER_PROGRAMS.profile(ds_version)
        self._supports_data_type = _supports_data_type(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ProjectParameterAdapter:
        """Return the exact adapter for one reviewed source version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectParameterDomain:
        """Bind project selection and project-parameter operations."""
        read = CodeNativeReadAdapter.for_version(self.ds_version).bind_read(
            profile,
            http_client=http_client,
        )
        return ProjectParameterDomain(
            definitions=read.definitions,
            parameters=cast(
                "ProjectParameterOperations",
                _Operations(
                    _PROJECT_PARAMETER_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                    self._supports_data_type,
                ),
            ),
        )


@dataclass(frozen=True)
class _AbsentAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ProjectParameterDomain:
        del profile, http_client
        message = (
            "Project parameter management does not exist in DolphinScheduler "
            f"{self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _INTRODUCED_IN,
            },
            suggestion=(
                "Use DolphinScheduler 3.2.0 or newer for project parameter management."
            ),
        )


def _adapter_for_version(
    ds_version: str,
) -> BoundDomainAdapter[ProjectParameterDomain]:
    profile = _PROJECT_PARAMETER_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentAdapter(ds_version)
    if profile.status == "supported":
        return ProjectParameterAdapter.for_version(ds_version)
    message = f"DS {ds_version} has no reviewed project-parameter capability decision"
    raise WireContractError(message)


PROJECT_PARAMETER_DOMAIN = BoundDomain[ProjectParameterDomain](
    name=_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _Operations:
    programs: BoundCompiledPrograms[_Primitive]
    supports_data_type: bool

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
        data_type: str | None = None,
    ) -> ProjectParameterPageRecord:
        values: JsonObject = {
            "projectCode": project_code,
            "searchVal": search,
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if self.supports_data_type:
            values["projectParameterDataType"] = data_type
        else:
            _require_legacy_data_type(data_type, ds_version=self.ds_version)
        page = self.programs.call("page", values)
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        snapshots = [
            _snapshot(
                item,
                ds_version=self.ds_version,
                supports_data_type=self.supports_data_type,
            )
            for item in items
        ]
        return cast(
            "ProjectParameterPageRecord",
            project_page(
                page,
                snapshots,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
        )

    def get(self, *, project_code: int, code: int) -> ProjectParameterRecord:
        return cast(
            "ProjectParameterRecord",
            _snapshot(
                self.programs.call("get", {"projectCode": project_code, "code": code}),
                ds_version=self.ds_version,
                supports_data_type=self.supports_data_type,
                expected_code=code,
                expected_project_code=project_code,
            ),
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        values: JsonObject = {
            "projectCode": project_code,
            "projectParameterName": name,
            "projectParameterValue": value,
        }
        if self.supports_data_type:
            values["projectParameterDataType"] = data_type
        else:
            _require_legacy_data_type(data_type, ds_version=self.ds_version)
        payload = mutation_call(
            lambda: self.programs.call("create", values),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="create",
        )
        return cast(
            "ProjectParameterRecord",
            verify_mutation(
                lambda: _snapshot(
                    payload,
                    ds_version=self.ds_version,
                    supports_data_type=self.supports_data_type,
                    expected_project_code=project_code,
                    expected_name=name,
                    expected_data_type=data_type.upper(),
                ),
                ds_version=self.ds_version,
                resource=_RESOURCE,
                operation="create",
                phase="mutation_response",
            ),
        )

    def update(
        self,
        *,
        project_code: int,
        code: int,
        name: str,
        value: str,
        data_type: str,
    ) -> ProjectParameterRecord:
        values: JsonObject = {
            "projectCode": project_code,
            "projectParameterName": name,
            "projectParameterValue": value,
        }
        if self.supports_data_type:
            values["projectParameterDataType"] = data_type
        else:
            _require_legacy_data_type(data_type, ds_version=self.ds_version)
        payload = mutation_call(
            lambda: self.programs.call("update", {**values, "code": code}),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="update",
        )
        return cast(
            "ProjectParameterRecord",
            verify_mutation(
                lambda: _snapshot(
                    payload,
                    ds_version=self.ds_version,
                    supports_data_type=self.supports_data_type,
                    expected_code=code,
                    expected_project_code=project_code,
                    expected_name=name,
                    expected_data_type=data_type.upper(),
                ),
                ds_version=self.ds_version,
                resource=_RESOURCE,
                operation="update",
                phase="mutation_response",
            ),
        )

    def delete(self, *, project_code: int, code: int) -> bool:
        payload = mutation_call(
            lambda: self.programs.call(
                "delete", {"projectCode": project_code, "code": code}
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="delete",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="deleteResult",
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="delete",
            phase="mutation_response",
        )
        return True

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


def _supports_data_type(profile: CompiledWireProfile) -> bool:
    if profile.recipe_id == "legacy_varchar":
        return False
    if profile.recipe_id == "data_type":
        return True
    message = f"Compiled project-parameter recipe is unsupported: {profile.recipe_id!r}"
    raise WireContractError(message)


def project_parameter_data_type_choices(ds_version: str) -> tuple[str, ...] | None:
    """Expose the exact VARCHAR-only restriction without inventing typed enums."""
    profile = _PROJECT_PARAMETER_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent" or _supports_data_type(profile):
        return None
    return (_LEGACY_DATA_TYPE,)


def _require_legacy_data_type(value: str | None, *, ds_version: str) -> None:
    if value is None or value.upper() == _LEGACY_DATA_TYPE:
        return
    message = f"DolphinScheduler {ds_version} project parameters are VARCHAR-only"
    raise UnsupportedFeatureError(
        message,
        details={
            "resource": _RESOURCE,
            "ds_version": ds_version,
            "reason": "upstream_option_absent",
            "option": "data_type",
            "requested": value,
            "supported": [_LEGACY_DATA_TYPE],
        },
        suggestion=(
            "Use --data-type VARCHAR, or upgrade DolphinScheduler to 3.3.1 or "
            "newer for typed project parameters."
        ),
    )


def _snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    supports_data_type: bool,
    expected_code: int | None = None,
    expected_project_code: int | None = None,
    expected_name: str | None = None,
    expected_data_type: str | None = None,
) -> ProjectParameterSnapshot:
    resource = _RESOURCE
    code = positive_int(
        response_field(item, "code", ds_version=ds_version, resource=resource),
        ds_version=ds_version,
        resource=resource,
        field="code",
    )
    project_code = positive_int(
        response_field(
            item,
            "projectCode",
            ds_version=ds_version,
            resource=resource,
        ),
        ds_version=ds_version,
        resource=resource,
        field="projectCode",
    )
    name = non_empty_text(
        response_field(item, "paramName", ds_version=ds_version, resource=resource),
        ds_version=ds_version,
        resource=resource,
        field="paramName",
    )
    if not supports_data_type:
        data_type_value = getattr(item, "paramDataType", None) or _LEGACY_DATA_TYPE
    else:
        data_type_value = response_field(
            item,
            "paramDataType",
            ds_version=ds_version,
            resource=resource,
        )
    data_type = non_empty_text(
        data_type_value,
        ds_version=ds_version,
        resource=resource,
        field="paramDataType",
    ).upper()
    if expected_code is not None and code != expected_code:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="code",
            reason="response code does not match the requested parameter",
        )
    if expected_project_code is not None and project_code != expected_project_code:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="projectCode",
            reason="response escaped the requested project scope",
        )
    if expected_name is not None and name != expected_name:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="paramName",
            reason="response name does not match the requested mutation",
        )
    if expected_data_type is not None and data_type != expected_data_type:
        raise projection_error(
            ds_version=ds_version,
            resource=resource,
            field="paramDataType",
            reason="response data type does not match the requested mutation",
        )
    return ProjectParameterSnapshot(
        id=optional_int_field(item, "id", ds_version=ds_version, resource=resource),
        userId=optional_int_field(
            item,
            "userId",
            ds_version=ds_version,
            resource=resource,
        ),
        operator=(
            optional_int_field(
                item,
                "operator",
                ds_version=ds_version,
                resource=resource,
            )
            if hasattr(item, "operator")
            else None
        ),
        code=code,
        projectCode=project_code,
        paramName=name,
        paramValue=optional_text_field(
            item,
            "paramValue",
            ds_version=ds_version,
            resource=resource,
        ),
        paramDataType=data_type,
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=resource,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=resource,
        ),
        createUser=(
            optional_text_field(
                item,
                "createUser",
                ds_version=ds_version,
                resource=resource,
            )
            if hasattr(item, "createUser")
            else None
        ),
        modifyUser=(
            optional_text_field(
                item,
                "modifyUser",
                ds_version=ds_version,
                resource=resource,
            )
            if hasattr(item, "modifyUser")
            else None
        ),
    )


__all__ = [
    "PROJECT_PARAMETER_DOMAIN",
    "ProjectParameterAdapter",
    "ProjectParameterDomain",
    "ProjectParameterSnapshot",
    "project_parameter_data_type_choices",
]
