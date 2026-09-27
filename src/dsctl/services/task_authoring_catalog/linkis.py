from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        LinkisAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringStateRule,
    TaskTypeAuthoringProfile,
)

_LINKIS_RUNTIME_EXCLUSION_FACET = "LINKIS/runtime_exclusion"


def _linkis_runtime_exclusion_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    """Materialize all source-present LINKIS releases as preserve-only holes."""
    surface: LinkisAuthoringSurface = get_task_authoring_surface(profile_version).linkis
    reason = surface.exclusion_reason
    if (
        not surface.registered
        or surface.typed_available
        or reason != "linkis-command-result-never-populated-and-status-not-polled"
        or surface.command_result_populated
        or surface.status_check_count != 1
        or surface.status_polled_to_terminal
        or surface.task_id_persisted_after_submit
        or surface.cancel_after_submit_failure
        or surface.durable_application_id
        or surface.failover_supported
        or not surface.retry_can_duplicate
    ):
        message = f"LINKIS {profile_version} is not the reviewed runtime hole"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=_LINKIS_RUNTIME_EXCLUSION_FACET,
        family="linkis-runtime-exclusion-v1",
        review="linkis-command-result-and-status-tracking-runtime-exclusion",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when=f"existing exact {profile_version} LINKIS server state",
                condition_paths=(),
                active_paths=("task_params",),
                compile_policy=(("task_params", "preserve unchanged only"),),
                description=(
                    "The worker never populates the command result string that "
                    "LINKIS parses for its remote task id and status, then its "
                    "remote-task base performs only one status check. Typed and "
                    "raw opaque create/edit are closed because no task payload "
                    "can repair those executor defects."
                ),
            ),
        ),
        templates=(),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=False,
        typed_edit=False,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            f"Exact {profile_version} LINKIS cannot reliably capture or track "
            "the submitted remote job; existing state is unchanged/export "
            "preservation only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="LINKIS",
        category="Other",
        kind="generic",
        default_facet=_LINKIS_RUNTIME_EXCLUSION_FACET,
        facets={_LINKIS_RUNTIME_EXCLUSION_FACET: membership},
    )
