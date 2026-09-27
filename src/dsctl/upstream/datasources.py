from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
    CompiledProgramExpectation,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.response_projection import (
    non_empty_text,
    project_page,
    projection_error,
    require_boolean,
    require_none,
    sequence_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        DataSourceOperations,
        DataSourcePageRecord,
        DataSourceRecord,
        StringEnumValue,
    )


_DATASOURCE_RESOURCE = "datasource"
_DataSourcePrimitive = Literal[
    "page",
    "get_legacy",
    "get",
    "create",
    "update",
    "delete_legacy",
    "delete",
    "connection_test",
]
_MODERN_VERSIONS = frozenset(TARGET_DS_VERSIONS) - {"1.3.9"}
_DATASOURCE_EXPECTATIONS: Mapping[_DataSourcePrimitive, CompiledProgramExpectation] = {
    "page": READ_RETRY_OPTIONAL,
    "get_legacy": replace(MUTATION_ONCE_REQUIRED, absent_versions=_MODERN_VERSIONS),
    "get": replace(READ_RETRY_OPTIONAL, absent_versions=frozenset({"1.3.9"})),
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    # 1.3.9 DataSourceController uses GET for a database deletion.
    "delete_legacy": replace(
        MUTATION_ONCE_REQUIRED,
        envelope=READ_RETRY_OPTIONAL.envelope,
        absent_versions=_MODERN_VERSIONS,
    ),
    "delete": replace(MUTATION_ONCE_REQUIRED, absent_versions=frozenset({"1.3.9"})),
    "connection_test": READ_RETRY_OPTIONAL,
}
_DATASOURCE_PROGRAMS = CompiledDomainPrograms[_DataSourcePrimitive](
    name="datasource",
    schema_constant="COMPILED_DATASOURCE_SCHEMA_VERSION",
    schema_version=1,
    expectations=_DATASOURCE_EXPECTATIONS,
)
_PayloadWire = Literal["legacy-form", "typed-body", "json-string"]
_MutationResult = Literal["none", "datasource"]
_BooleanResult = Literal["none", "boolean"]
_CredentialUpdateBehavior = Literal["explicit-value", "blank-preserves"]

_REQUIRES_EXPLICIT_UPDATE_VALUE: _CredentialUpdateBehavior = "explicit-value"
_BLANK_UPDATE_PRESERVES_EXISTING: _CredentialUpdateBehavior = "blank-preserves"


@dataclass(frozen=True)
class DataSourceDomain:
    """Caller-oriented datasource resource surface."""

    datasources: DataSourceOperations


@dataclass(frozen=True)
class DataSourceSnapshot:
    """Version-neutral datasource summary used by services and resolvers."""

    id: int | None
    name: str | None
    note: str | None
    type: StringEnumValue | str | None
    userId: int  # noqa: N815
    userName: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _DataSourceRecipe:
    payload_wire: _PayloadWire
    mutation_result: _MutationResult
    boolean_result: _BooleanResult
    password_update: _CredentialUpdateBehavior
    get_primitive: Literal["get", "get_legacy"] = "get"
    delete_primitive: Literal["delete", "delete_legacy"] = "delete"


_LEGACY_FORM = _DataSourceRecipe(
    payload_wire="legacy-form",
    mutation_result="none",
    boolean_result="none",
    password_update=_REQUIRES_EXPLICIT_UPDATE_VALUE,
    get_primitive="get_legacy",
    delete_primitive="delete_legacy",
)
_TYPED_BODY = _DataSourceRecipe(
    payload_wire="typed-body",
    mutation_result="none",
    boolean_result="none",
    password_update=_BLANK_UPDATE_PRESERVES_EXISTING,
)
_JSON_VOID = _DataSourceRecipe(
    payload_wire="json-string",
    mutation_result="none",
    boolean_result="none",
    password_update=_BLANK_UPDATE_PRESERVES_EXISTING,
)
_JSON_ENTITY = _DataSourceRecipe(
    payload_wire="json-string",
    mutation_result="datasource",
    boolean_result="boolean",
    password_update=_BLANK_UPDATE_PRESERVES_EXISTING,
)

_DATASOURCE_RECIPES = {
    "legacy_form": _LEGACY_FORM,
    "typed_body": _TYPED_BODY,
    "json_void": _JSON_VOID,
    "json_entity": _JSON_ENTITY,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _DataSourceRecipe:
    if profile.recipe_id is None or profile.recipe_id not in _DATASOURCE_RECIPES:
        message = f"DS {profile.ds_version} compiled datasource recipe is unsupported"
        raise WireContractError(message)
    return _DATASOURCE_RECIPES[profile.recipe_id]


class DataSourceAdapter:
    """Compiled datasource adapter for every reviewed exact DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Select one exact compiled profile and its ordinary lifecycle recipe."""
        self._profile = _DATASOURCE_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> DataSourceAdapter:
        """Return one adapter for an explicitly reviewed exact version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> DataSourceDomain:
        """Bind the datasource operations for this exact profile."""
        return DataSourceDomain(
            datasources=cast(
                "DataSourceOperations",
                _CompiledDataSourceOperations(
                    _DATASOURCE_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                    self._recipe,
                ),
            )
        )


DATASOURCE_DOMAIN = BoundDomain[DataSourceDomain](
    name=_DATASOURCE_RESOURCE,
    adapter_for_version=DataSourceAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledDataSourceOperations:
    programs: BoundCompiledPrograms[_DataSourcePrimitive]
    recipe: _DataSourceRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> DataSourcePageRecord:
        page = self.programs.call(
            "page", {"searchVal": search, "pageNo": page_no, "pageSize": page_size}
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
        )
        snapshots = [
            _datasource_snapshot(item, ds_version=self.ds_version) for item in items
        ]
        return cast(
            "DataSourcePageRecord",
            project_page(
                page,
                snapshots,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
            ),
        )

    def get(self, *, datasource_id: int) -> JsonObject:
        payload = self.programs.call(self.recipe.get_primitive, {"id": datasource_id})
        detail = _json_object(payload, ds_version=self.ds_version, field="detail")
        detail_id = detail.get("id")
        if detail_id is not None and detail_id != datasource_id:
            raise projection_error(
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                field="id",
                reason="response identity does not match the requested datasource",
            )
        return detail

    def create(self, *, payload_json: str) -> DataSourceRecord:
        payload = _decode_payload(payload_json, ds_version=self.ds_version)
        result = mutation_call(
            lambda: self._call_create(payload, payload_json),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="create",
        )
        datasource_id = verify_mutation(
            lambda: _validate_create_or_update_result(
                result,
                recipe=self.recipe,
                ds_version=self.ds_version,
                field="createResult",
            ),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="create",
            phase="mutation_response",
        )
        name = _required_payload_text(payload, "name", ds_version=self.ds_version)
        summary = verify_mutation(
            lambda: self._find_created(name=name, datasource_id=datasource_id),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="create",
        )
        if datasource_id is None:
            verify_mutation(
                lambda: self.get(
                    datasource_id=_required_snapshot_id(
                        summary,
                        ds_version=self.ds_version,
                    )
                ),
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                operation="create",
            )
        return cast("DataSourceRecord", summary)

    def update(
        self,
        *,
        datasource_id: int,
        payload_json: str,
    ) -> DataSourceRecord:
        payload = _decode_payload(payload_json, ds_version=self.ds_version)
        result = mutation_call(
            lambda: self._call_update(datasource_id, payload, payload_json),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="update",
        )
        verify_mutation(
            lambda: _validate_create_or_update_result(
                result,
                recipe=self.recipe,
                ds_version=self.ds_version,
                field="updateResult",
                expected_id=datasource_id,
            ),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="update",
            phase="mutation_response",
        )
        detail = verify_mutation(
            lambda: self.get(datasource_id=datasource_id),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="update",
        )
        expected_name = _required_payload_text(
            payload,
            "name",
            ds_version=self.ds_version,
        )
        if detail.get("name") != expected_name:
            verify_mutation(
                lambda: _raise_readback_mismatch(self.ds_version, "name"),
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                operation="update",
            )
        return cast(
            "DataSourceRecord",
            _datasource_snapshot_from_detail(
                detail,
                ds_version=self.ds_version,
                datasource_id=datasource_id,
            ),
        )

    def delete(self, *, datasource_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call(
                self.recipe.delete_primitive, {"id": datasource_id}
            ),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="delete",
        )
        return verify_mutation(
            lambda: _validate_boolean_result(
                result,
                recipe=self.recipe,
                ds_version=self.ds_version,
                field="deleteResult",
            ),
            ds_version=self.ds_version,
            resource=_DATASOURCE_RESOURCE,
            operation="delete",
            phase="mutation_response",
        )

    def connection_test(self, *, datasource_id: int) -> bool:
        result = self.programs.call("connection_test", {"id": datasource_id})
        return _validate_boolean_result(
            result,
            recipe=self.recipe,
            ds_version=self.ds_version,
            field="connectionTestResult",
        )

    @property
    def blank_password_preserves_existing(self) -> bool:
        """Whether an empty update password preserves the stored secret."""
        return self.recipe.password_update == _BLANK_UPDATE_PRESERVES_EXISTING

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def _call_create(
        self, payload: JsonObject, payload_json: str
    ) -> OpaqueGeneratedValue:
        if self.recipe.payload_wire == "json-string":
            args: JsonObject = {"jsonStr": payload_json}
        elif self.recipe.payload_wire == "legacy-form":
            args = _legacy_form_payload(payload, ds_version=self.ds_version)
        else:
            args = {"dataSourceParam": payload}
        return self.programs.call("create", args)

    def _call_update(
        self,
        datasource_id: int,
        payload: JsonObject,
        payload_json: str,
    ) -> OpaqueGeneratedValue:
        if self.recipe.payload_wire == "json-string":
            args: JsonObject = {"id": datasource_id, "jsonStr": payload_json}
        elif self.recipe.payload_wire == "legacy-form":
            args = _legacy_form_payload(payload, ds_version=self.ds_version)
            args["id"] = datasource_id
        else:
            typed_payload = {**payload, "id": datasource_id}
            args = {"id": datasource_id, "dataSourceParam": typed_payload}
        return self.programs.call("update", args)

    def _find_created(
        self,
        *,
        name: str,
        datasource_id: int | None,
    ) -> DataSourceSnapshot:
        if datasource_id is not None:
            detail = self.get(datasource_id=datasource_id)
            return _datasource_snapshot_from_detail(
                detail,
                ds_version=self.ds_version,
                datasource_id=datasource_id,
            )
        matches = [
            cast("DataSourceSnapshot", item)
            for item in collect_pages(
                lambda *, page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=name,
                ),
                resource=_DATASOURCE_RESOURCE,
            )
            if item.name == name
        ]
        if len(matches) != 1:
            raise projection_error(
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                field="mutationReadback",
                reason="created datasource could not be resolved uniquely by name",
            )
        return matches[0]


def _datasource_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> DataSourceSnapshot:
    datasource_id = _optional_mapping_or_model_int(
        item,
        "id",
        ds_version=ds_version,
    )
    user_id = _optional_mapping_or_model_int(
        item,
        "userId",
        ds_version=ds_version,
    )
    return DataSourceSnapshot(
        id=datasource_id,
        name=_optional_mapping_or_model_text(item, "name", ds_version=ds_version),
        note=_optional_mapping_or_model_text(item, "note", ds_version=ds_version),
        type=cast(
            "StringEnumValue | str | None",
            _mapping_or_model_field(item, "type", ds_version=ds_version),
        ),
        userId=user_id or 0,
        userName=_optional_mapping_or_model_text(
            item,
            "userName",
            ds_version=ds_version,
        ),
        createTime=_optional_mapping_or_model_text(
            item,
            "createTime",
            ds_version=ds_version,
        ),
        updateTime=_optional_mapping_or_model_text(
            item,
            "updateTime",
            ds_version=ds_version,
        ),
    )


def _datasource_snapshot_from_detail(
    detail: Mapping[str, JsonValue],
    *,
    ds_version: str,
    datasource_id: int,
) -> DataSourceSnapshot:
    return DataSourceSnapshot(
        id=datasource_id,
        name=_optional_mapping_text(detail, "name", ds_version=ds_version),
        note=_optional_mapping_text(detail, "note", ds_version=ds_version),
        type=cast("StringEnumValue | str | None", detail.get("type")),
        userId=_optional_mapping_int(detail, "userId", ds_version=ds_version) or 0,
        userName=_optional_mapping_text(detail, "userName", ds_version=ds_version),
        createTime=_optional_mapping_text(
            detail,
            "createTime",
            ds_version=ds_version,
        ),
        updateTime=_optional_mapping_text(
            detail,
            "updateTime",
            ds_version=ds_version,
        ),
    )


def _validate_create_or_update_result(
    result: OpaqueGeneratedValue,
    *,
    recipe: _DataSourceRecipe,
    ds_version: str,
    field: str,
    expected_id: int | None = None,
) -> int | None:
    if recipe.mutation_result == "none":
        require_none(
            result,
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
        )
        return None
    datasource_id = _optional_mapping_or_model_int(result, "id", ds_version=ds_version)
    if datasource_id is None:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
            reason="datasource mutation result is missing an id",
        )
    if expected_id is not None and datasource_id != expected_id:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
            reason="datasource mutation result identity does not match the request",
        )
    return datasource_id


def _validate_boolean_result(
    result: OpaqueGeneratedValue,
    *,
    recipe: _DataSourceRecipe,
    ds_version: str,
    field: str,
) -> bool:
    if recipe.boolean_result == "none":
        require_none(
            result,
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
        )
        return True
    return require_boolean(
        result,
        ds_version=ds_version,
        resource=_DATASOURCE_RESOURCE,
        field=field,
    )


def _decode_payload(payload_json: str, *, ds_version: str) -> JsonObject:
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError as exc:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="payload",
            reason="service supplied invalid JSON",
        ) from exc
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) for key in payload
    ):
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="payload",
            reason="service payload is not a JSON object",
        )
    return cast("JsonObject", payload)


def _legacy_form_payload(payload: JsonObject, *, ds_version: str) -> JsonObject:
    other = payload.get("other", {})
    if isinstance(other, dict):
        other_value: JsonValue = json.dumps(
            other,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    elif isinstance(other, str):
        other_value = other
    else:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="other",
            reason="legacy datasource other must be an object or JSON string",
        )
    port = payload.get("port")
    if isinstance(port, bool) or not isinstance(port, (int, str)):
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="port",
            reason="legacy datasource port must be an integer or string",
        )
    return cast(
        "JsonObject",
        {
            "name": payload.get("name"),
            "note": payload.get("note"),
            "type": payload.get("type"),
            "host": payload.get("host"),
            "port": str(port),
            "database": payload.get("database"),
            "principal": payload.get("principal", ""),
            "userName": payload.get("userName", ""),
            "password": payload.get("password", ""),
            "connectType": payload.get("connectType", "ORACLE_SERVICE_NAME"),
            "other": other_value,
        },
    )


def _required_payload_text(
    payload: Mapping[str, JsonValue],
    field: str,
    *,
    ds_version: str,
) -> str:
    return non_empty_text(
        payload.get(field),
        ds_version=ds_version,
        resource=_DATASOURCE_RESOURCE,
        field=field,
    )


def _json_object(
    payload: OpaqueGeneratedValue, *, ds_version: str, field: str
) -> JsonObject:
    if isinstance(payload, BaseModel):
        dumped = payload.model_dump(mode="json", by_alias=True)
    elif isinstance(payload, Mapping):
        dumped = dict(payload)
    else:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
            reason="generated detail is not an object",
        )
    if not all(isinstance(key, str) for key in dumped):
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
            reason="generated detail has a non-string key",
        )
    return cast("JsonObject", dumped)


def _mapping_or_model_field(
    item: OpaqueGeneratedValue, field: str, *, ds_version: str
) -> OpaqueGeneratedValue:
    if isinstance(item, Mapping):
        return item.get(field)
    try:
        return getattr(item, field)
    except AttributeError as exc:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field=field,
            reason="generated payload is missing a canonical field",
        ) from exc


def _optional_mapping_or_model_int(
    item: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
) -> int | None:
    value = _mapping_or_model_field(item, field, ds_version=ds_version)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=_DATASOURCE_RESOURCE,
        field=field,
        reason="payload field is not an integer or null",
    )


def _optional_mapping_or_model_text(
    item: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
) -> str | None:
    value = _mapping_or_model_field(item, field, ds_version=ds_version)
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=_DATASOURCE_RESOURCE,
        field=field,
        reason="payload field is not text or null",
    )


def _optional_mapping_int(
    item: Mapping[str, JsonValue],
    field: str,
    *,
    ds_version: str,
) -> int | None:
    return _optional_mapping_or_model_int(item, field, ds_version=ds_version)


def _optional_mapping_text(
    item: Mapping[str, JsonValue],
    field: str,
    *,
    ds_version: str,
) -> str | None:
    return _optional_mapping_or_model_text(item, field, ds_version=ds_version)


def _required_snapshot_id(
    snapshot: DataSourceSnapshot,
    *,
    ds_version: str,
) -> int:
    if snapshot.id is None:
        raise projection_error(
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="id",
            reason="created datasource summary is missing an id",
        )
    return snapshot.id


def _raise_readback_mismatch(ds_version: str, field: str) -> None:
    raise projection_error(
        ds_version=ds_version,
        resource=_DATASOURCE_RESOURCE,
        field=field,
        reason="readback field does not match the requested mutation",
    )


__all__ = [
    "DATASOURCE_DOMAIN",
    "DataSourceAdapter",
    "DataSourceDomain",
    "DataSourceSnapshot",
]
