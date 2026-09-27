"""In-memory audit collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeAudit:
    user_name_value: str | None
    model_type_value: str | None
    model_name_value: str | None
    operation_value: str | None
    create_time_value: str | None = None
    description_value: str | None = None
    detail_value: str | None = None
    latency_value: str | None = None

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def modelType(self) -> str | None:  # noqa: N802
        return self.model_type_value

    @property
    def modelName(self) -> str | None:  # noqa: N802
        return self.model_name_value

    @property
    def operation(self) -> str | None:
        return self.operation_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def description(self) -> str | None:
        return self.description_value

    @property
    def detail(self) -> str | None:
        return self.detail_value

    @property
    def latency(self) -> str | None:
        return self.latency_value


@dataclass(frozen=True)
class FakeAuditPage(_FakePage[FakeAudit]):
    pass


@dataclass(frozen=True)
class FakeAuditModelType:
    name: str | None
    child: list[FakeAuditModelType] | None = None


@dataclass(frozen=True)
class FakeAuditOperationType:
    name: str | None


@dataclass
class FakeAuditAdapter:
    audit_logs: list[FakeAudit]
    model_types: list[FakeAuditModelType] = field(default_factory=list)
    operation_types: list[FakeAuditOperationType] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        model_types: Sequence[str] | None = None,
        operation_types: Sequence[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        user_name: str | None = None,
        model_name: str | None = None,
    ) -> FakeAuditPage:
        filtered = list(self.audit_logs)
        if model_types is not None:
            allowed_model_types = set(model_types)
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.modelType in allowed_model_types
            ]
        if operation_types is not None:
            allowed_operation_types = set(operation_types)
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.operation in allowed_operation_types
            ]
        if start_date is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.createTime is not None
                and audit_log.createTime >= start_date
            ]
        if end_date is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.createTime is not None and audit_log.createTime <= end_date
            ]
        if user_name is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.userName is not None
                and user_name.lower() in audit_log.userName.lower()
            ]
        if model_name is not None:
            filtered = [
                audit_log
                for audit_log in filtered
                if audit_log.modelName is not None
                and model_name.lower() in audit_log.modelName.lower()
            ]
        return FakeAuditPage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def list_model_types(self) -> Sequence[FakeAuditModelType]:
        return list(self.model_types)

    def list_operation_types(self) -> Sequence[FakeAuditOperationType]:
        return list(self.operation_types)


def empty_audit_adapter() -> FakeAuditAdapter:
    return FakeAuditAdapter(audit_logs=[])
