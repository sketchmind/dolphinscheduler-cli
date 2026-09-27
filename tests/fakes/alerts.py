"""In-memory alerts collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakePluginDefine:
    id: int | None
    plugin_name_value: str | None
    plugin_type_value: str | None = None
    plugin_params_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def pluginName(self) -> str | None:  # noqa: N802
        return self.plugin_name_value

    @property
    def pluginType(self) -> str | None:  # noqa: N802
        return self.plugin_type_value

    @property
    def pluginParams(self) -> str | None:  # noqa: N802
        return self.plugin_params_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass
class FakeUiPluginAdapter:
    plugin_defines: list[FakePluginDefine]

    def list(self, *, plugin_type: str) -> Sequence[FakePluginDefine]:
        normalized_plugin_type = plugin_type.casefold()
        return [
            plugin_define
            for plugin_define in self.plugin_defines
            if plugin_define.pluginType is not None
            and plugin_define.pluginType.casefold() == normalized_plugin_type
        ]

    def get(self, *, plugin_id: int) -> FakePluginDefine:
        for plugin_define in self.plugin_defines:
            if plugin_define.id == plugin_id:
                return plugin_define
        raise ApiResultError(
            result_code=110004,
            result_message=f"alert plugin define id {plugin_id} not found",
        )


@dataclass(frozen=True)
class FakeAlertPlugin:
    id: int
    plugin_define_id_value: int
    instance_name_value: str | None
    plugin_instance_params_value: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    instance_type_value: str | None = None
    warning_type_value: str | None = None
    alert_plugin_name_value: str | None = None

    @property
    def pluginDefineId(self) -> int:  # noqa: N802
        return self.plugin_define_id_value

    @property
    def instanceName(self) -> str | None:  # noqa: N802
        return self.instance_name_value

    @property
    def pluginInstanceParams(self) -> str | None:  # noqa: N802
        return self.plugin_instance_params_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def instanceType(self) -> str | None:  # noqa: N802
        return self.instance_type_value

    @property
    def warningType(self) -> str | None:  # noqa: N802
        return self.warning_type_value

    @property
    def alertPluginName(self) -> str | None:  # noqa: N802
        return self.alert_plugin_name_value


@dataclass(frozen=True)
class FakeAlertPluginPage(_FakePage[FakeAlertPlugin]):
    pass


@dataclass
class FakeAlertPluginAdapter:
    alert_plugins: list[FakeAlertPlugin]
    plugin_names_by_id: dict[int, str] = field(default_factory=dict)
    test_send_results_by_plugin_define_id: dict[int, bool] = field(default_factory=dict)
    delete_errors_by_id: dict[int, ApiResultError] = field(default_factory=dict)
    test_send_errors_by_plugin_define_id: dict[int, ApiResultError] = field(
        default_factory=dict
    )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeAlertPluginPage:
        filtered = list(self.alert_plugins)
        if search is not None:
            filtered = [
                alert_plugin
                for alert_plugin in filtered
                if alert_plugin.instanceName is not None
                and search.lower() in alert_plugin.instanceName.lower()
            ]
        return FakeAlertPluginPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def list_all(self) -> Sequence[FakeAlertPlugin]:
        return list(self.alert_plugins)

    def create(
        self,
        *,
        plugin_define_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> FakeAlertPlugin:
        for alert_plugin in self.alert_plugins:
            if alert_plugin.instanceName == instance_name:
                raise ApiResultError(
                    result_code=110010,
                    result_message=f"alert plugin {instance_name} already exists",
                )
        next_id = max((item.id for item in self.alert_plugins), default=0) + 1
        created = FakeAlertPlugin(
            id=next_id,
            plugin_define_id_value=plugin_define_id,
            instance_name_value=instance_name,
            plugin_instance_params_value=plugin_instance_params,
            instance_type_value="ALERT",
            warning_type_value="ALL",
            alert_plugin_name_value=self.plugin_names_by_id.get(plugin_define_id),
        )
        self.alert_plugins.append(created)
        return created

    def update(
        self,
        *,
        alert_plugin_id: int,
        instance_name: str,
        plugin_instance_params: str,
    ) -> FakeAlertPlugin:
        for alert_plugin in self.alert_plugins:
            if (
                alert_plugin.id != alert_plugin_id
                and alert_plugin.instanceName == instance_name
            ):
                raise ApiResultError(
                    result_code=110010,
                    result_message=f"alert plugin {instance_name} already exists",
                )
        for index, alert_plugin in enumerate(self.alert_plugins):
            if alert_plugin.id == alert_plugin_id:
                updated = replace(
                    alert_plugin,
                    instance_name_value=instance_name,
                    plugin_instance_params_value=plugin_instance_params,
                )
                self.alert_plugins[index] = updated
                return updated
        raise ApiResultError(
            result_code=110007,
            result_message=f"alert plugin id {alert_plugin_id} not found",
        )

    def delete(self, *, alert_plugin_id: int) -> bool:
        if alert_plugin_id in self.delete_errors_by_id:
            raise self.delete_errors_by_id[alert_plugin_id]
        for index, alert_plugin in enumerate(self.alert_plugins):
            if alert_plugin.id == alert_plugin_id:
                self.alert_plugins.pop(index)
                return True
        raise ApiResultError(
            result_code=110006,
            result_message=f"alert plugin id {alert_plugin_id} not found",
        )

    def test_send(
        self,
        *,
        plugin_define_id: int,
        plugin_instance_params: str,
    ) -> bool:
        del plugin_instance_params
        if plugin_define_id in self.test_send_errors_by_plugin_define_id:
            raise self.test_send_errors_by_plugin_define_id[plugin_define_id]
        return self.test_send_results_by_plugin_define_id.get(plugin_define_id, True)


@dataclass(frozen=True)
class FakeAlertGroup:
    id: int
    group_name_value: str | None
    alert_instance_ids_value: str | None = None
    group_type_value: str | None = None
    description: str | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    create_user_id_value: int = 0

    @property
    def groupName(self) -> str | None:  # noqa: N802
        return self.group_name_value

    @property
    def alertInstanceIds(self) -> str | None:  # noqa: N802
        return self.alert_instance_ids_value

    @property
    def groupType(self) -> str | None:  # noqa: N802
        return self.group_type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def createUserId(self) -> int:  # noqa: N802
        return self.create_user_id_value


@dataclass(frozen=True)
class FakeAlertGroupPage(_FakePage[FakeAlertGroup]):
    pass


@dataclass
class FakeAlertGroupAdapter:
    alert_groups: list[FakeAlertGroup]
    association: Literal["legacy-alert-type", "plugin-instance-ids"] = (
        "plugin-instance-ids"
    )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeAlertGroupPage:
        filtered = list(self.alert_groups)
        if search is not None:
            filtered = [
                alert_group
                for alert_group in filtered
                if alert_group.groupName is not None
                and search.lower() in alert_group.groupName.lower()
            ]
        return FakeAlertGroupPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, alert_group_id: int) -> FakeAlertGroup:
        for alert_group in self.alert_groups:
            if alert_group.id == alert_group_id:
                return alert_group
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )

    def create(
        self,
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> FakeAlertGroup:
        for alert_group in self.alert_groups:
            if alert_group.groupName == group_name:
                raise ApiResultError(
                    result_code=10012,
                    result_message="alarm group already exists",
                )
        next_id = max((item.id for item in self.alert_groups), default=0) + 1
        created = FakeAlertGroup(
            id=next_id,
            group_name_value=group_name,
            alert_instance_ids_value=alert_instance_ids,
            group_type_value=group_type,
            description=description,
            create_user_id_value=1,
        )
        self.alert_groups.append(created)
        return created

    def update(
        self,
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> FakeAlertGroup:
        for other in self.alert_groups:
            if other.id != alert_group_id and other.groupName == group_name:
                raise ApiResultError(
                    result_code=10012,
                    result_message="alarm group already exists",
                )
        for index, alert_group in enumerate(self.alert_groups):
            if alert_group.id == alert_group_id:
                updated = replace(
                    alert_group,
                    group_name_value=group_name,
                    alert_instance_ids_value=alert_instance_ids,
                    group_type_value=group_type,
                    description=description,
                    create_user_id_value=1,
                )
                self.alert_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )

    def delete(self, *, alert_group_id: int) -> bool:
        if alert_group_id == 1:
            raise ApiResultError(
                result_code=130030,
                result_message="Not allow to delete the default alarm group",
            )
        for index, alert_group in enumerate(self.alert_groups):
            if alert_group.id == alert_group_id:
                self.alert_groups.pop(index)
                return True
        raise ApiResultError(
            result_code=10011,
            result_message=f"alert group id {alert_group_id} not found",
        )


def empty_ui_plugin_adapter() -> FakeUiPluginAdapter:
    return FakeUiPluginAdapter(plugin_defines=[])


def empty_alert_plugin_adapter() -> FakeAlertPluginAdapter:
    return FakeAlertPluginAdapter(alert_plugins=[])


def empty_alert_group_adapter() -> FakeAlertGroupAdapter:
    return FakeAlertGroupAdapter(alert_groups=[])
