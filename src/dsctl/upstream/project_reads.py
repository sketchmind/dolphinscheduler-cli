"""Project read execution shared by definition and lifecycle adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from dsctl.errors import ApiTransportError
from dsctl.upstream.response_projection import (
    nullable_sequence_field,
    positive_int,
    preserved_page,
    response_field,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.upstream._compiled_project import ProjectPrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.protocol import ProjectPageRecord, ProjectPayloadRecord

    ProjectRecordProjector = Callable[
        [OpaqueGeneratedValue, str, int | None], ProjectPayloadRecord
    ]
    ProjectReadErrorFactory = Callable[..., ApiTransportError]


def _canonical_projection_error(
    *,
    ds_version: str,
    resource: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "DolphinScheduler response cannot be projected to the canonical read contract.",
        details={
            "ds_version": ds_version,
            "resource": resource,
            "field": field,
            "reason": reason,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=(
            "Verify DS_VERSION matches the server and retry after checking API health."
        ),
    )


def canonical_project_record(
    item: OpaqueGeneratedValue,
    ds_version: str,
    expected_code: int | None,
) -> ProjectPayloadRecord:
    """Preserve the canonical definition-read project projection contract."""
    code = positive_int(
        response_field(
            item,
            "code",
            ds_version=ds_version,
            resource="project",
            error_factory=_canonical_projection_error,
        ),
        ds_version=ds_version,
        resource="project",
        field="code",
        error_factory=_canonical_projection_error,
    )
    if expected_code is not None and code != expected_code:
        raise _canonical_projection_error(
            ds_version=ds_version,
            resource="project",
            field="code",
            reason="response identity does not match the requested scope",
        )
    response_field(
        item,
        "name",
        ds_version=ds_version,
        resource="project",
        error_factory=_canonical_projection_error,
    )
    return cast("ProjectPayloadRecord", item)


@dataclass(frozen=True)
class CompiledProjectReads:
    """Execute the one compiled project page/get contract."""

    programs: BoundCompiledPrograms[ProjectPrimitive]
    record_projector: ProjectRecordProjector = field(
        default=canonical_project_record,
        kw_only=True,
    )
    error_factory: ProjectReadErrorFactory = field(
        default=_canonical_projection_error,
        kw_only=True,
    )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ProjectPageRecord:
        """Return one page through the compiled project page program."""
        page = self.programs.call(
            "page", {"searchVal": search, "pageSize": page_size, "pageNo": page_no}
        )
        items = nullable_sequence_field(
            page,
            "totalList",
            ds_version=self.programs.ds_version,
            resource="project.page",
            error_factory=self.error_factory,
        )
        projected = (
            None
            if items is None
            else [
                self.record_projector(item, self.programs.ds_version, None)
                for item in items
            ]
        )
        return cast(
            "ProjectPageRecord",
            preserved_page(
                page,
                projected,
                ds_version=self.programs.ds_version,
                resource="project.page",
                error_factory=self.error_factory,
            ),
        )

    def get(self, *, code: int) -> ProjectPayloadRecord:
        """Return one project through the compiled project get program."""
        return self.record_projector(
            self.programs.call("get", {"code": code}),
            self.programs.ds_version,
            code,
        )


__all__ = ["CompiledProjectReads", "canonical_project_record"]
