from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiResultError
from dsctl.upstream._compiled_resource import RESOURCE_PROGRAMS
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.response_projection import (
    optional_bool_field,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from dsctl.client import BinaryResponse, DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._compiled_resource import ResourcePrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.protocol import (
        ResourceContentRecord,
        ResourceOperations,
        ResourcePageRecord,
        StringEnumValue,
        TaskResourceResolver,
    )


_RESOURCE = "resource"
_RESOURCE_NOT_EXIST = 20004

_Epoch = Literal["legacy-1.3", "id-rest", "storage-path", "absolute-path"]


@dataclass(frozen=True)
class ResourceDomain:
    """Caller-oriented exact resource surface."""

    resources: ResourceOperations


@dataclass(frozen=True)
class ResourceSnapshot:
    """Version-neutral resource row consumed by stable serialization."""

    alias: str | None
    userName: str | None  # noqa: N815
    fileName: str | None  # noqa: N815
    fullName: str | None  # noqa: N815
    isDirectory: bool  # noqa: N815
    type: StringEnumValue | None
    size: int
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class ResourceContentSnapshot:
    """Stable text-window payload independent of generated view names."""

    content: str | None


@dataclass(frozen=True)
class TaskResourceFileSnapshot:
    """Canonical task FILE resolved into one exact persisted identity."""

    resource_id: int | None
    wire_full_name: str


@dataclass(frozen=True)
class _ResourceRecipe:
    epoch: _Epoch
    view_json: bool


_RESOURCE_RECIPES = {
    "legacy_139": _ResourceRecipe(epoch="legacy-1.3", view_json=True),
    "id_rest": _ResourceRecipe(epoch="id-rest", view_json=True),
    "storage_path": _ResourceRecipe(epoch="storage-path", view_json=True),
    "storage_path_322": _ResourceRecipe(epoch="storage-path", view_json=True),
    "absolute_path": _ResourceRecipe(epoch="absolute-path", view_json=False),
    "absolute_path_native_view": _ResourceRecipe(epoch="absolute-path", view_json=True),
}


class ResourceAdapter:
    """Resource behavior across exact compiled wire epochs."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact profile and its reviewed resource recipe."""
        self._profile = RESOURCE_PROGRAMS.profile(ds_version)
        self._recipe = _resource_recipe(self._profile.recipe_id)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ResourceAdapter:
        """Return the resource adapter for one exact reviewed version."""
        return cls(ds_version)

    @property
    def task_file_uses_id(self) -> bool:
        """Return the task FILE identity selected by the exact compiled recipe."""
        return self._recipe.epoch in {"legacy-1.3", "id-rest"}

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ResourceDomain:
        """Bind generated JSON operations and narrow raw byte transports."""
        return ResourceDomain(
            resources=cast(
                "ResourceOperations",
                _ResourceOperations(
                    RESOURCE_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                    self._recipe,
                ),
            )
        )


RESOURCE_DOMAIN = BoundDomain[ResourceDomain](
    name=_RESOURCE,
    adapter_for_version=ResourceAdapter.for_version,
)


def bind_task_resource_resolver(
    ds_version: str,
    profile: ClusterProfile,
    *,
    http_client: DolphinSchedulerClient,
) -> TaskResourceResolver:
    """Bind exact task FILE verification without exposing resource CRUD."""
    adapter = ResourceAdapter.for_version(ds_version)
    return cast(
        "TaskResourceResolver",
        _ResourceOperations(
            RESOURCE_PROGRAMS.bind(adapter._profile, profile, http_client=http_client),
            adapter._recipe,
        ),
    )


@dataclass(frozen=True)
class _ResourceOperations:
    programs: BoundCompiledPrograms[ResourcePrimitive]
    recipe: _ResourceRecipe

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    @property
    def epoch(self) -> _Epoch:
        return self.recipe.epoch

    def resolve_id(self, full_name: str) -> int:
        """Resolve one exact visible FILE fullName on an id-backed profile."""
        return self._lookup_id(full_name)

    def resolve_task_file(self, full_name: str) -> TaskResourceFileSnapshot:
        """Resolve one canonical FILE name into the selected exact task wire."""
        if self.epoch in {"legacy-1.3", "id-rest"}:
            return TaskResourceFileSnapshot(
                resource_id=self._lookup_id(full_name),
                wire_full_name=full_name,
            )
        return TaskResourceFileSnapshot(
            resource_id=None,
            wire_full_name=self._verify_name_backed_file(full_name),
        )

    def resolve_full_name(self, resource_id: int) -> str:
        """Resolve one exact visible FILE id on an id-backed profile."""
        if (
            not isinstance(resource_id, int)
            or isinstance(resource_id, bool)
            or resource_id <= 0
        ):
            raise projection_error(
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="id",
                reason="resource id is not a positive integer",
            )
        if self.epoch not in {"legacy-1.3", "id-rest"}:
            message = "Resource id lookup is unavailable for this profile"
            raise WireContractError(message)
        values: JsonObject = {
            "fullName": None,
            "type": "FILE",
            "id": resource_id,
        }
        payload = self.programs.call("lookup", values)
        actual_id = positive_int(
            response_field(
                payload,
                "id",
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            field="id",
        )
        if actual_id != resource_id:
            raise ApiResultError(
                result_code=_RESOURCE_NOT_EXIST,
                result_message=f"resource id {resource_id} does not exist",
            )
        full_name = optional_text_field(
            payload,
            "fullName",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        self._require_file_resource(payload, identity=f"id {resource_id}")
        if not full_name:
            raise projection_error(
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="fullName",
                reason="resource fullName is empty",
            )
        return full_name

    def base_dir(self) -> str:
        if self.epoch in {"legacy-1.3", "id-rest"}:
            return "/"
        return self._base_dir_for_type("FILE")

    def _base_dir_for_type(self, resource_type: str) -> str:
        """Read one exact resource-type base directory from the generated wire."""
        if self.epoch in {"legacy-1.3", "id-rest"}:
            return "/"
        payload = self.programs.call("base_dir", {"type": resource_type})
        if isinstance(payload, str) and payload:
            return payload.rstrip("/") or "/"
        raise projection_error(
            ds_version=self.ds_version,
            resource=_RESOURCE,
            field="baseDir",
            reason="base directory is not non-empty text",
        )

    def list(
        self,
        *,
        directory: str,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ResourcePageRecord:
        values: JsonObject = {
            "type": "FILE",
            "pageNo": page_no,
            "searchVal": search,
            "pageSize": page_size,
        }
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values["id"] = self._directory_id(directory)
        else:
            values["fullName"] = directory
            if self.epoch == "storage-path":
                values["tenantCode"] = ""
            elif search is None:
                values["searchVal"] = ""
        page = self.programs.call("page", values)
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        return cast(
            "ResourcePageRecord",
            project_page(
                page,
                [
                    _resource_snapshot(item, ds_version=self.ds_version)
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
        )

    def view(
        self,
        *,
        full_name: str,
        skip_line_num: int,
        limit: int,
    ) -> ResourceContentRecord:
        if not self.recipe.view_json:
            response = self.download(full_name=full_name)
            return cast(
                "ResourceContentRecord",
                ResourceContentSnapshot(
                    _resource_view_content(
                        response,
                        skip_line_num=skip_line_num,
                        limit=limit,
                    )
                ),
            )
        values: JsonObject = {
            "skipLineNum": skip_line_num,
            "limit": limit,
        }
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values["id"] = self._lookup_id(full_name)
        else:
            values["fullName"] = full_name
            if self.epoch == "storage-path":
                values["tenantCode"] = ""
        payload = self.programs.call("view_native", values)
        content = _content_from_generated(payload, ds_version=self.ds_version)

        return cast(
            "ResourceContentRecord",
            ResourceContentSnapshot(content),
        )

    def upload(self, *, current_dir: str, name: str, file: IO[bytes]) -> None:
        values: JsonObject = {
            "type": "FILE",
            "name": name,
            "currentDir": self._mutation_directory(current_dir),
        }
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values["pid"] = self._directory_id(current_dir)
        mutation_call(
            lambda: self.programs.upload(
                "upload",
                values,
                files={"file": (name, file)},
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="upload",
        )

    def create_from_content(
        self,
        *,
        current_dir: str,
        file_name: str,
        suffix: str,
        content: str,
    ) -> None:
        values: JsonObject = {
            "type": "FILE",
            "fileName": file_name,
            "suffix": suffix,
            "content": content,
            "currentDir": self._mutation_directory(current_dir),
        }
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values.update(
                {
                    "description": None,
                    "pid": self._directory_id(current_dir),
                }
            )
        prepared = self.programs.prepare("create", values)
        mutation_call(
            lambda: self.programs.execute("create", prepared).payload,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="create",
        )
        if self.epoch in {"legacy-1.3", "id-rest"}:
            name = f"{file_name.strip()}.{suffix.strip()}"
            full_name = (
                f"/{name}"
                if current_dir == "/"
                else f"{current_dir.rstrip('/')}/{name}"
            )
            verify_mutation(
                lambda: self._lookup_id(full_name),
                ds_version=self.ds_version,
                resource=_RESOURCE,
                operation="create",
            )

    def create_directory(self, *, current_dir: str, name: str) -> None:
        values: JsonObject = {
            "type": "FILE",
            "name": name,
            "currentDir": self._mutation_directory(current_dir),
        }
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values.update(
                {
                    "description": None,
                    "pid": self._directory_id(current_dir),
                }
            )
        elif self.epoch == "storage-path":
            values["pid"] = -1
        prepared = self.programs.prepare("mkdir", values)
        mutation_call(
            lambda: self.programs.execute("mkdir", prepared).payload,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="mkdir",
        )

    def delete(self, *, full_name: str) -> bool:
        if self.epoch in {"legacy-1.3", "id-rest"}:
            values: JsonObject = {"id": self._lookup_id(full_name, require_file=False)}
        else:
            values = {"fullName": full_name}
            if self.epoch == "storage-path":
                values["tenantCode"] = None
        prepared = self.programs.prepare("delete", values)
        mutation_call(
            lambda: self.programs.execute("delete", prepared).payload,
            ds_version=self.ds_version,
            resource=_RESOURCE,
            operation="delete",
        )
        return True

    def download(self, *, full_name: str) -> BinaryResponse:
        values: JsonObject = (
            {"id": self._lookup_id(full_name)}
            if self.epoch in {"legacy-1.3", "id-rest"}
            else {"fullName": full_name}
        )
        return self.programs.download("download", values)

    def _directory_id(self, full_name: str) -> int:
        return (
            -1 if full_name == "/" else self._lookup_id(full_name, require_file=False)
        )

    def _lookup_id(self, full_name: str, *, require_file: bool = True) -> int:
        if self.epoch not in {"legacy-1.3", "id-rest"}:
            message = "Resource id lookup is unavailable for this profile"
            raise WireContractError(message)
        values: JsonObject = {
            "fullName": full_name,
            "type": "FILE",
            "id": None if self.epoch == "legacy-1.3" else -1,
        }
        payload = self.programs.call("lookup", values)
        resource_id = positive_int(
            response_field(
                payload,
                "id",
                ds_version=self.ds_version,
                resource=_RESOURCE,
            ),
            ds_version=self.ds_version,
            resource=_RESOURCE,
            field="id",
        )
        actual_full_name = optional_text_field(
            payload,
            "fullName",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        )
        if actual_full_name != full_name:
            raise ApiResultError(
                result_code=_RESOURCE_NOT_EXIST,
                result_message=f"resource {full_name!r} does not exist",
            )
        if require_file:
            self._require_file_resource(payload, identity=repr(full_name))
        return resource_id

    def _verify_name_backed_file(self, full_name: str) -> str:
        """Resolve one canonical base-relative name to a modern storage path."""
        base_dir = self.base_dir()
        if self.epoch == "storage-path":
            udf_base_dir = self._base_dir_for_type("UDF")
            file_parent, file_separator, file_leaf = base_dir.rpartition("/")
            udf_parent, udf_separator, udf_leaf = udf_base_dir.rpartition("/")
            if (
                udf_base_dir == base_dir
                or file_separator != "/"
                or udf_separator != "/"
                or not file_parent
                or file_leaf != "resources"
                or udf_leaf != "udfs"
                or udf_parent != file_parent
            ):
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=_RESOURCE,
                    field="baseDir",
                    reason=(
                        "task FILE resolution cannot distinguish exact sibling "
                        "resources/udfs tenant bases from the DolphinScheduler "
                        "3.2 administrator ALL-resource root"
                    ),
                )
        if base_dir.rstrip("/").rsplit("/", maxsplit=1)[-1] != "resources":
            raise projection_error(
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="baseDir",
                reason=(
                    "task FILE resolution requires a tenant FILE base directory "
                    "ending in the resources path segment"
                ),
            )
        wire_full_name = (
            full_name if base_dir == "/" else f"{base_dir.rstrip('/')}{full_name}"
        )
        parent, separator, leaf = wire_full_name.rpartition("/")
        if not separator or not leaf:
            raise ApiResultError(
                result_code=_RESOURCE_NOT_EXIST,
                result_message=f"resource {full_name!r} does not exist",
            )
        directory = parent or "/"
        items = collect_pages(
            lambda *, page_no, page_size: self.list(
                directory=directory,
                page_no=page_no,
                page_size=page_size,
                search=leaf,
            ),
            resource=_RESOURCE,
        )
        matches = [item for item in items if item.fullName == wire_full_name]
        if not matches:
            raise ApiResultError(
                result_code=_RESOURCE_NOT_EXIST,
                result_message=f"resource {full_name!r} does not exist",
            )
        if len(matches) != 1:
            raise projection_error(
                ds_version=self.ds_version,
                resource=_RESOURCE,
                field="fullName",
                reason="resource list returned duplicate exact fullNames",
            )
        self._require_file_resource(matches[0], identity=repr(full_name))
        return wire_full_name

    def _require_file_resource(
        self,
        payload: OpaqueGeneratedValue,
        *,
        identity: str,
    ) -> None:
        """Reject directory identities from task FILE resource binding."""
        if not optional_bool_field(
            payload,
            "isDirectory",
            ds_version=self.ds_version,
            resource=_RESOURCE,
        ):
            return
        raise ApiResultError(
            result_code=_RESOURCE_NOT_EXIST,
            result_message=f"resource {identity} is not a file",
        )

    def _mutation_directory(self, current_dir: str) -> str:
        if self.epoch != "storage-path" or current_dir.endswith("/"):
            return current_dir
        return f"{current_dir}/"


def _resource_recipe(recipe_id: str | None) -> _ResourceRecipe:
    if recipe_id is None or recipe_id not in _RESOURCE_RECIPES:
        message = f"Compiled resource recipe is unsupported: {recipe_id!r}"
        raise WireContractError(message)
    return _RESOURCE_RECIPES[recipe_id]


def _resource_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> ResourceSnapshot:
    raw_size = response_field(
        item,
        "size",
        ds_version=ds_version,
        resource=_RESOURCE,
    )
    if not isinstance(raw_size, int) or isinstance(raw_size, bool) or raw_size < 0:
        raise projection_error(
            ds_version=ds_version,
            resource=_RESOURCE,
            field="size",
            reason="resource size is not a non-negative integer",
        )
    raw_type = response_field(
        item,
        "type",
        ds_version=ds_version,
        resource=_RESOURCE,
    )
    return ResourceSnapshot(
        alias=optional_text_field(
            item,
            "alias",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        userName=optional_text_field(
            item,
            "userName",
            ds_version=ds_version,
            resource=_RESOURCE,
            missing_is_none=True,
        ),
        fileName=optional_text_field(
            item,
            "fileName",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        fullName=optional_text_field(
            item,
            "fullName",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        isDirectory=optional_bool_field(
            item,
            "isDirectory",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        type=cast("StringEnumValue | None", raw_type),
        size=raw_size,
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=_RESOURCE,
        ),
    )


def _content_from_generated(
    payload: OpaqueGeneratedValue, *, ds_version: str
) -> str | None:
    """Project an exact wire-validated Map or structured content response."""
    if not isinstance(payload, Mapping):
        return optional_text_field(
            payload,
            "content",
            ds_version=ds_version,
            resource=_RESOURCE,
        )
    content = payload.get("content")
    if content is None or isinstance(content, str):
        return content
    raise projection_error(
        ds_version=ds_version,
        resource=_RESOURCE,
        field="content",
        reason="view content is not text or null",
    )


def _resource_view_content(
    response: BinaryResponse,
    *,
    skip_line_num: int,
    limit: int,
) -> str:
    encoding = _content_type_charset(response.content_type) or "utf-8"
    text = response.content.decode(encoding, errors="replace")
    lines = text.splitlines()
    return "\n".join(lines[skip_line_num : skip_line_num + limit])


def _content_type_charset(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    for part in content_type.split(";"):
        name, separator, value = part.strip().partition("=")
        if separator and name.lower() == "charset":
            normalized = value.strip().strip('"')
            return normalized or None
    return None


__all__ = [
    "RESOURCE_DOMAIN",
    "ResourceAdapter",
    "ResourceContentSnapshot",
    "ResourceDomain",
    "ResourceSnapshot",
    "TaskResourceFileSnapshot",
    "bind_task_resource_resolver",
]
