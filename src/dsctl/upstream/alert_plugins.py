from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Literal, Protocol, cast

from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.support.json_types import JsonValue, is_json_value
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
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.response_projection import (
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        AlertPluginListItemRecord,
        AlertPluginPageRecord,
        PluginDefineRecord,
        UiPluginOperations,
    )


_MutationResult = Literal["none", "entity"]
_DeleteResult = Literal["none", "boolean"]
_AlertPluginPrimitive = Literal[
    "page",
    "list",
    "update_baseline",
    "create",
    "update",
    "delete",
    "test_send",
    "definition_list",
    "definition_get",
]
# Only transient updates consume a baseline read; this is recipe-stage membership.
_UPDATE_BASELINE_UNUSED_VERSIONS = frozenset(
    {
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_TEST_SEND_ABSENT_VERSIONS = frozenset(
    {
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }
)
_ALERT_PLUGIN_EXPECTATIONS: Mapping[
    _AlertPluginPrimitive, CompiledProgramExpectation
] = {
    "page": READ_RETRY_OPTIONAL,
    "list": READ_RETRY_OPTIONAL,
    "update_baseline": CompiledProgramExpectation(
        mode=READ_RETRY_OPTIONAL.mode,
        envelope=READ_RETRY_OPTIONAL.envelope,
        absent_versions=_UPDATE_BASELINE_UNUSED_VERSIONS,
    ),
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
    "test_send": CompiledProgramExpectation(
        mode=MUTATION_ONCE_REQUIRED.mode,
        envelope=MUTATION_ONCE_REQUIRED.envelope,
        absent_versions=_TEST_SEND_ABSENT_VERSIONS,
    ),
    "definition_list": READ_RETRY_OPTIONAL,
    "definition_get": READ_RETRY_OPTIONAL,
}
_ALERT_PLUGIN_PROGRAMS = CompiledDomainPrograms[_AlertPluginPrimitive](
    name="alert_plugin",
    schema_constant="COMPILED_ALERT_PLUGIN_SCHEMA_VERSION",
    schema_version=2,
    expectations=_ALERT_PLUGIN_EXPECTATIONS,
)


@dataclass(frozen=True)
class AlertPluginDomain:
    """Caller-oriented exact alert-plugin and definition surface."""

    alert_plugins: ExactAlertPluginOperations
    ui_plugins: UiPluginOperations


class ExactAlertPluginOperations(Protocol):
    """Deep alert-plugin port whose mutations include verified list readback."""

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertPluginPageRecord:
        """Return one exact-version instance page."""

    def list_all(self) -> Sequence[AlertPluginListItemRecord]:
        """Return every visible instance using the stable list projection."""

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        """Create and return the verified list projection."""

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        """Update and return the verified list projection."""

    def delete(self, *, alert_plugin_id: int) -> bool:
        """Delete one instance and verify its absence."""

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        """Send one exact-version test alert."""


@dataclass(frozen=True)
class AlertPluginSnapshot:
    """Version-neutral alert-plugin instance projection."""

    id: int
    pluginDefineId: int  # noqa: N815
    instanceName: str | None  # noqa: N815
    pluginInstanceParams: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    instanceType: str | None  # noqa: N815
    warningType: str | None  # noqa: N815
    alertPluginName: str | None  # noqa: N815


@dataclass(frozen=True)
class PluginDefineSnapshot:
    """Version-neutral alert UI-plugin definition projection."""

    id: int
    pluginName: str | None  # noqa: N815
    pluginType: str | None  # noqa: N815
    pluginParams: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _AlertPluginRecipe:
    searchable_page: bool
    create_result: _MutationResult
    update_result: _MutationResult
    delete_result: _DeleteResult
    test_send: bool
    transient_delivery_fields: bool


_VOID_LOCAL_SEARCH_RECIPE = _AlertPluginRecipe(
    searchable_page=False,
    create_result="none",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_VOID_RECIPE = _AlertPluginRecipe(
    searchable_page=True,
    create_result="none",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_CREATE_ENTITY_RECIPE = _AlertPluginRecipe(
    searchable_page=True,
    create_result="entity",
    update_result="none",
    delete_result="none",
    test_send=False,
    transient_delivery_fields=False,
)
_TRANSIENT_ENTITY_RECIPE = _AlertPluginRecipe(
    searchable_page=True,
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    test_send=True,
    transient_delivery_fields=True,
)
_ENTITY_RECIPE = _AlertPluginRecipe(
    searchable_page=True,
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    test_send=True,
    transient_delivery_fields=False,
)


class AlertPluginAdapter:
    """Compiled alert-plugin adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _ALERT_PLUGIN_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> AlertPluginAdapter:
        """Return the adapter for one explicitly reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> AlertPluginDomain:
        """Bind only alert-plugin instance and definition operations."""
        if self._recipe is None:
            if {
                self.ds_version,
                profile.ds_version,
                http_client.profile.ds_version,
            } != {self.ds_version}:
                message = (
                    "Generated alert-plugin adapter does not match the exact "
                    "client profile"
                )
                raise WireContractError(message)
            unavailable_plugins = _UnavailableAlertPluginOperations(
                ds_version=self.ds_version
            )
            unavailable_definitions = _UnavailableUiPluginOperations(
                ds_version=self.ds_version
            )
            return AlertPluginDomain(
                alert_plugins=cast(
                    "ExactAlertPluginOperations",
                    unavailable_plugins,
                ),
                ui_plugins=cast("UiPluginOperations", unavailable_definitions),
            )
        programs = _ALERT_PLUGIN_PROGRAMS.bind(
            self._profile, profile, http_client=http_client
        )
        return AlertPluginDomain(
            alert_plugins=cast(
                "ExactAlertPluginOperations",
                _CompiledAlertPluginOperations(programs, self._recipe),
            ),
            ui_plugins=cast(
                "UiPluginOperations",
                _CompiledUiPluginOperations(programs),
            ),
        )


ALERT_PLUGIN_DOMAIN = BoundDomain[AlertPluginDomain](
    name="alert-plugin",
    adapter_for_version=AlertPluginAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledAlertPluginOperations:
    programs: BoundCompiledPrograms[_AlertPluginPrimitive]
    recipe: _AlertPluginRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertPluginPageRecord:
        if search is None or self.recipe.searchable_page:
            return self._server_page(
                page_no=page_no,
                page_size=page_size,
                search=search,
            )
        collected = collect_pages(
            lambda page_no, page_size: self._server_page(
                page_no=page_no,
                page_size=page_size,
                search=None,
            ),
            resource="alert-plugin",
        )
        folded_search = search.casefold()
        filtered = [
            item
            for item in collected
            if item.instanceName is not None
            and folded_search in item.instanceName.casefold()
        ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        total_pages = _page_count(total, page_size)
        return cast(
            "AlertPluginPageRecord",
            ReadPage(
                totalList=filtered[start:stop],
                total=total,
                totalPage=total_pages,
                pageSize=page_size,
                currentPage=page_no,
                pageNo=page_no,
            ),
        )

    def _server_page(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> AlertPluginPageRecord:
        values: JsonObject = {
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if self.recipe.searchable_page:
            values["searchVal"] = search
        page = self.programs.call("page", values)
        native_items = response_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="alert-plugin",
        )
        native_total = response_field(
            page,
            "total",
            ds_version=self.ds_version,
            resource="alert-plugin",
        )
        page_start = (page_no - 1) * page_size
        if (
            native_items is None
            and isinstance(native_total, int)
            and not isinstance(native_total, bool)
            and native_total > page_start
        ):
            raise projection_error(
                ds_version=self.ds_version,
                resource="alert-plugin",
                field="totalList",
                reason="null page items contradict the reported total",
            )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="alert-plugin",
        )
        return cast(
            "AlertPluginPageRecord",
            project_page(
                page,
                [
                    _alert_plugin_snapshot(
                        item,
                        ds_version=self.ds_version,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource="alert-plugin",
            ),
        )

    def list_all(self) -> Sequence[AlertPluginListItemRecord]:
        return collect_pages(self.list, resource="alert-plugin")

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        plugin_instance_params = _plugin_param_request_text(
            plugin_instance_params,
            ds_version=self.ds_version,
        )
        expected_param_values = _requested_plugin_param_values(
            plugin_instance_params,
            ds_version=self.ds_version,
        )
        values: JsonObject = {
            "pluginDefineId": plugin_define_id,
            "instanceName": instance_name,
            "pluginInstanceParams": plugin_instance_params,
        }
        if self.recipe.transient_delivery_fields:
            # These are the exact retained UI defaults for DS 3.2.1/3.2.2.
            values.update({"instanceType": "NORMAL", "warningType": "ALL"})
        result = mutation_call(
            lambda: self.programs.call("create", values),
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation="create",
        )
        return self._verified_readback(
            operation="create",
            result=result,
            expected_result=self.recipe.create_result,
            alert_plugin_id=None,
            plugin_define_id=plugin_define_id,
            instance_name=instance_name,
            expected_param_values=expected_param_values,
        )

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        plugin_instance_params = _plugin_param_request_text(
            plugin_instance_params,
            ds_version=self.ds_version,
        )
        expected_param_values = _requested_plugin_param_values(
            plugin_instance_params,
            ds_version=self.ds_version,
        )
        values: JsonObject = {
            "id": alert_plugin_id,
            "instanceName": instance_name,
            "pluginInstanceParams": plugin_instance_params,
        }
        if self.recipe.transient_delivery_fields:
            current = self.programs.call(
                "update_baseline",
                {"id": alert_plugin_id},
            )
            warning_type = optional_text_field(
                current,
                "warningType",
                ds_version=self.ds_version,
                resource="alert-plugin",
            )
            if warning_type is None:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource="alert-plugin",
                    field="warningType",
                    reason="3.2.x update preservation requires warningType",
                )
            values["warningType"] = warning_type
        result = mutation_call(
            lambda: self.programs.call("update", values),
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation="update",
        )
        return self._verified_readback(
            operation="update",
            result=result,
            expected_result=self.recipe.update_result,
            alert_plugin_id=alert_plugin_id,
            plugin_define_id=None,
            instance_name=instance_name,
            expected_param_values=expected_param_values,
        )

    def delete(self, *, alert_plugin_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call("delete", {"id": alert_plugin_id}),
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation="delete",
        )

        def verify() -> bool:
            _require_delete_result(
                result,
                expected=self.recipe.delete_result,
                ds_version=self.ds_version,
            )
            if any(item.id == alert_plugin_id for item in self.list_all()):
                message = "Alert-plugin deletion readback still returned the deleted id"
                raise ApiTransportError(
                    message,
                    details={"alert_plugin_id": alert_plugin_id},
                )
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation="delete",
        )

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        if not self.recipe.test_send:
            raise _test_send_absent_error(self.ds_version)
        result = mutation_call(
            lambda: self.programs.call(
                "test_send",
                {
                    "pluginDefineId": plugin_define_id,
                    "pluginInstanceParams": plugin_instance_params,
                },
            ),
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation="test",
        )
        if isinstance(result, bool):
            return result
        raise projection_error(
            ds_version=self.ds_version,
            resource="alert-plugin",
            field="testResult",
            reason="test-send result is not boolean",
        )

    def _verified_readback(
        self,
        *,
        operation: str,
        result: OpaqueGeneratedValue,
        expected_result: _MutationResult,
        alert_plugin_id: int | None,
        plugin_define_id: int | None,
        instance_name: str,
        expected_param_values: dict[str, JsonValue],
    ) -> AlertPluginListItemRecord:
        def verify() -> AlertPluginListItemRecord:
            _require_mutation_result(
                result,
                expected=expected_result,
                operation=operation,
                ds_version=self.ds_version,
            )
            candidates = self.list_all()
            matches = [
                item
                for item in candidates
                if (
                    item.id == alert_plugin_id
                    if alert_plugin_id is not None
                    else item.instanceName == instance_name
                    and item.pluginDefineId == plugin_define_id
                )
            ]
            if len(matches) != 1:
                message = (
                    "Alert-plugin mutation readback did not return one exact match"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "alert_plugin_id": alert_plugin_id,
                        "instance_name": instance_name,
                        "match_count": len(matches),
                    },
                )
            refreshed = matches[0]
            refreshed_params = _readback_plugin_param_values(
                refreshed.pluginInstanceParams,
                ds_version=self.ds_version,
            )
            if (
                refreshed.instanceName != instance_name
                or not _plugin_param_values_match(
                    expected=expected_param_values,
                    readback=refreshed_params,
                )
            ):
                message = (
                    "Alert-plugin mutation readback did not match requested fields"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "alert_plugin_id": refreshed.id,
                        "instance_name": instance_name,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="alert-plugin",
            operation=operation,
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


def _requested_plugin_param_values(
    value: str,
    *,
    ds_version: str,
) -> dict[str, JsonValue]:
    """Project UI params to the map persisted by ``PluginParamsTransfer``."""
    parsed = _parse_plugin_param_json(
        value,
        ds_version=ds_version,
        readback=False,
    )
    if not isinstance(parsed, list):
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason="requested params are not a JSON array",
        )
    values: dict[str, JsonValue] = {}
    for item in parsed:
        field, item_value = _plugin_param_item(
            item,
            ds_version=ds_version,
            readback=False,
        )
        # PluginParamsTransfer uses HashMap.put, so the last duplicate wins.
        _validate_requested_plugin_param_value(
            item_value,
            ds_version=ds_version,
        )
        values[field] = item_value
    return values


def _plugin_param_request_text(value: str, *, ds_version: str) -> str:
    """Turn the native empty-list readback marker into valid mutation input."""
    parsed = _parse_plugin_param_json(
        value,
        ds_version=ds_version,
        readback=False,
    )
    return "[]" if parsed is None else value


def _plugin_param_values_match(
    *,
    expected: dict[str, JsonValue],
    readback: dict[str, str | None],
) -> bool:
    """Compare persisted values while allowing template-only null fields."""
    for field, value in expected.items():
        if field not in readback:
            return False
        if not _plugin_param_value_matches(value, readback.get(field)):
            return False
    return all(
        value is None for field, value in readback.items() if field not in expected
    )


def _readback_plugin_param_values(
    value: str | None,
    *,
    ds_version: str,
) -> dict[str, str | None]:
    """Project the upstream UI-list representation back to persisted values."""
    if value is None:
        return {}
    parsed = _parse_plugin_param_json(
        value,
        ds_version=ds_version,
        readback=True,
    )
    # Empty persisted maps are deliberately rendered as JSON null upstream.
    if parsed is None:
        return {}
    if not isinstance(parsed, list):
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason="readback params are neither a JSON array nor null",
        )
    values: dict[str, str | None] = {}
    for item in parsed:
        field, item_value = _plugin_param_item(
            item,
            ds_version=ds_version,
            readback=True,
        )
        if field in values:
            raise _plugin_param_projection_error(
                ds_version=ds_version,
                reason="readback params contain a duplicate field",
            )
        if item_value is not None and not isinstance(item_value, str):
            raise _plugin_param_projection_error(
                ds_version=ds_version,
                reason="readback param value is not text or null",
            )
        values[field] = item_value
    return values


def _parse_plugin_param_json(
    value: str,
    *,
    ds_version: str,
    readback: bool,
) -> JsonValue:
    try:
        parsed: JsonValue = json.loads(value)
    except (json.JSONDecodeError, TypeError) as error:
        phase = "readback" if readback else "requested"
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params are not valid JSON",
        ) from error
    if not is_json_value(parsed):
        phase = "readback" if readback else "requested"
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params contain non-JSON values",
        )
    return parsed


def _plugin_param_item(
    item: JsonValue,
    *,
    ds_version: str,
    readback: bool,
) -> tuple[str, JsonValue]:
    phase = "readback" if readback else "requested"
    if not isinstance(item, dict):
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params contain a non-object item",
        )
    field = item.get("field")
    if not isinstance(field, str) or not field:
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params contain an item without a text field",
        )
    param_type = item.get("type")
    if not isinstance(param_type, str) or not param_type:
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params contain an item without text type",
        )
    if not isinstance(item.get("title"), str):
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason=f"{phase} params contain an item without text title",
        )
    return field, item.get("value")


def _validate_requested_plugin_param_value(
    value: JsonValue,
    *,
    ds_version: str,
) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise _plugin_param_projection_error(
            ds_version=ds_version,
            reason="requested param value cannot be verified safely",
        )
    if isinstance(value, list):
        for item in value:
            _validate_requested_plugin_param_value(
                item,
                ds_version=ds_version,
            )
    elif isinstance(value, dict):
        for item in value.values():
            _validate_requested_plugin_param_value(
                item,
                ds_version=ds_version,
            )


def _plugin_param_value_matches(expected: JsonValue, actual: str | None) -> bool:
    if expected is None:
        return actual is None
    if actual is None:
        return False
    if isinstance(expected, str):
        return actual == expected
    if isinstance(expected, bool):
        return actual == ("true" if expected else "false")
    if isinstance(expected, int):
        return actual == str(expected)
    if isinstance(expected, float):
        try:
            return Decimal(actual) == Decimal(str(expected))
        except InvalidOperation:
            return False
    return _java_collection_value_matches(expected, actual)


_JAVA_NUMBER_PATTERN = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"


def _java_collection_value_matches(expected: JsonValue, actual: str) -> bool:
    """Match Java collection text while comparing nested floats numerically."""
    expected_floats: list[float] = []
    pattern = _java_collection_pattern(expected, expected_floats=expected_floats)
    matched = re.fullmatch(pattern, actual)
    if matched is None:
        return False
    return all(
        Decimal(captured) == Decimal(str(expected_float))
        for captured, expected_float in zip(
            matched.groups(), expected_floats, strict=True
        )
    )


def _java_collection_pattern(
    value: JsonValue,
    *,
    expected_floats: list[float],
) -> str:
    if isinstance(value, float):
        expected_floats.append(value)
        return f"({_JAVA_NUMBER_PATTERN})"
    if isinstance(value, list):
        items = (
            _java_collection_pattern(item, expected_floats=expected_floats)
            for item in value
        )
        return re.escape("[") + re.escape(", ").join(items) + re.escape("]")
    if isinstance(value, dict):
        items = (
            re.escape(f"{key}=")
            + _java_collection_pattern(item, expected_floats=expected_floats)
            for key, item in value.items()
        )
        return re.escape("{") + re.escape(", ").join(items) + re.escape("}")
    return re.escape(_java_collection_text(value))


def _java_collection_text(value: JsonValue) -> str:
    """Mirror Java collection ``toString`` used by PluginParamsTransfer."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_java_collection_text(item) for item in value) + "]"
    if isinstance(value, dict):
        return (
            "{"
            + ", ".join(
                f"{key}={_java_collection_text(item)}" for key, item in value.items()
            )
            + "}"
        )
    message = "Validated JSON value has an unsupported runtime type"
    raise AssertionError(message)


def _plugin_param_projection_error(
    *,
    ds_version: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "Alert-plugin UI params could not be projected safely",
        details={
            "ds_version": ds_version,
            "resource": "alert-plugin",
            "field": "pluginInstanceParams",
            "reason": reason,
        },
    )


@dataclass(frozen=True)
class _CompiledUiPluginOperations:
    programs: BoundCompiledPrograms[_AlertPluginPrimitive]

    def list(self, *, plugin_type: str) -> Sequence[PluginDefineRecord]:
        result = self.programs.call(
            "definition_list",
            {"pluginType": plugin_type},
        )
        if not isinstance(result, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource="alert-plugin-definition",
                field="definitions",
                reason="definition result is not a list",
            )
        return [
            _plugin_define_snapshot(
                item,
                ds_version=self.ds_version,
            )
            for item in result
        ]

    def get(self, *, plugin_id: int) -> PluginDefineRecord:
        result = self.programs.call("definition_get", {"id": plugin_id})
        return _plugin_define_snapshot(
            result,
            ds_version=self.ds_version,
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


@dataclass(frozen=True)
class _UnavailableAlertPluginOperations:
    ds_version: str

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertPluginPageRecord:
        del page_no, page_size, search
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.page")

    def list_all(self) -> Sequence[AlertPluginListItemRecord]:
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.get")

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        del plugin_define_id, instance_name, plugin_instance_params
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.create")

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> AlertPluginListItemRecord:
        del alert_plugin_id, instance_name, plugin_instance_params
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.update")

    def delete(self, *, alert_plugin_id: int) -> bool:
        del alert_plugin_id
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.delete")

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        del plugin_define_id, plugin_instance_params
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.test")


@dataclass(frozen=True)
class _UnavailableUiPluginOperations:
    ds_version: str

    def list(self, *, plugin_type: str) -> Sequence[PluginDefineRecord]:
        del plugin_type
        raise _plugin_absent_error(
            self.ds_version,
            action="alert-plugin.definition.list",
        )

    def get(self, *, plugin_id: int) -> PluginDefineRecord:
        del plugin_id
        raise _plugin_absent_error(self.ds_version, action="alert-plugin.schema")


_RECIPES = {
    "void_local_search": _VOID_LOCAL_SEARCH_RECIPE,
    "void": _VOID_RECIPE,
    "create_entity": _CREATE_ENTITY_RECIPE,
    "transient_entity": _TRANSIENT_ENTITY_RECIPE,
    "entity": _ENTITY_RECIPE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _AlertPluginRecipe | None:
    if profile.status == "upstream_absent":
        return None
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled alert-plugin recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled alert-plugin recipe {profile.recipe_id!r} is unsupported"
        raise WireContractError(message) from exc


def _alert_plugin_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> AlertPluginSnapshot:
    return AlertPluginSnapshot(
        id=positive_int(
            response_field(
                item,
                "id",
                ds_version=ds_version,
                resource="alert-plugin",
            ),
            ds_version=ds_version,
            resource="alert-plugin",
            field="id",
        ),
        pluginDefineId=positive_int(
            response_field(
                item,
                "pluginDefineId",
                ds_version=ds_version,
                resource="alert-plugin",
            ),
            ds_version=ds_version,
            resource="alert-plugin",
            field="pluginDefineId",
        ),
        instanceName=optional_text_field(
            item,
            "instanceName",
            ds_version=ds_version,
            resource="alert-plugin",
        ),
        pluginInstanceParams=optional_text_field(
            item,
            "pluginInstanceParams",
            ds_version=ds_version,
            resource="alert-plugin",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="alert-plugin",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="alert-plugin",
        ),
        instanceType=optional_text_field(
            item,
            "instanceType",
            ds_version=ds_version,
            resource="alert-plugin",
            missing_is_none=True,
        ),
        warningType=optional_text_field(
            item,
            "warningType",
            ds_version=ds_version,
            resource="alert-plugin",
            missing_is_none=True,
        ),
        alertPluginName=optional_text_field(
            item,
            "alertPluginName",
            ds_version=ds_version,
            resource="alert-plugin",
            missing_is_none=True,
        ),
    )


def _plugin_define_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> PluginDefineSnapshot:
    return PluginDefineSnapshot(
        id=positive_int(
            response_field(
                item,
                "id",
                ds_version=ds_version,
                resource="alert-plugin-definition",
            ),
            ds_version=ds_version,
            resource="alert-plugin-definition",
            field="id",
        ),
        pluginName=optional_text_field(
            item,
            "pluginName",
            ds_version=ds_version,
            resource="alert-plugin-definition",
        ),
        pluginType=optional_text_field(
            item,
            "pluginType",
            ds_version=ds_version,
            resource="alert-plugin-definition",
        ),
        pluginParams=optional_text_field(
            item,
            "pluginParams",
            ds_version=ds_version,
            resource="alert-plugin-definition",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="alert-plugin-definition",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="alert-plugin-definition",
        ),
    )


def _require_mutation_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _MutationResult,
    operation: str,
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "entity" and result is not None:
        return
    raise projection_error(
        ds_version=ds_version,
        resource="alert-plugin",
        field=f"{operation}Result",
        reason="mutation result did not match the exact-version contract",
    )


def _require_delete_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _DeleteResult,
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "boolean" and result is True:
        return
    raise projection_error(
        ds_version=ds_version,
        resource="alert-plugin",
        field="deleteResult",
        reason="delete result did not confirm success",
    )


def _plugin_absent_error(
    ds_version: str,
    *,
    action: str,
) -> UnsupportedFeatureError:
    return UnsupportedFeatureError(
        f"{action} is unavailable on DolphinScheduler {ds_version}.",
        details={
            "action": action,
            "selected_version": ds_version,
            "reason": "upstream_capability_absent",
        },
        suggestion=(
            "Alert-plugin definitions and instances require DolphinScheduler "
            "2.0.0 or newer."
        ),
    )


def _test_send_absent_error(ds_version: str) -> UnsupportedFeatureError:
    return UnsupportedFeatureError(
        f"alert-plugin.test is unavailable on DolphinScheduler {ds_version}.",
        details={
            "action": "alert-plugin.test",
            "selected_version": ds_version,
            "reason": "upstream_capability_absent",
        },
        suggestion="Alert-plugin test-send requires DolphinScheduler 3.2.1 or newer.",
    )


def _page_count(total: int, page_size: int) -> int:
    quotient, remainder = divmod(total, page_size)
    return quotient + (1 if remainder else 0)


__all__ = [
    "ALERT_PLUGIN_DOMAIN",
    "AlertPluginAdapter",
    "AlertPluginDomain",
    "AlertPluginSnapshot",
    "ExactAlertPluginOperations",
    "PluginDefineSnapshot",
]
