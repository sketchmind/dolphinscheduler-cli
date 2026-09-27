from __future__ import annotations

from dsctl.models.task_spec import (
    PROCEDURE_PARAMETER_DATA_TYPES,
)
from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

PROCEDURE_CALL_FACET = "PROCEDURE/call"


_PROCEDURE_FIELDS = (
    model_field(
        "task_params.type",
        "enum",
        default="MYSQL",
        choice_source="dsctl enum list db-type",
        related_commands=(
            "dsctl enum list db-type",
            "dsctl template datasource",
            "dsctl template datasource --type TYPE",
        ),
        compile_path="taskDefinitionJson[].taskParams.type",
        description="Datasource type used by the PROCEDURE plugin.",
    ),
    model_field(
        "task_params.datasource",
        "integer|string",
        required=True,
        choices=(),
        choice_source="dsctl datasource list",
        related_commands=(
            "dsctl datasource list",
            "dsctl datasource get DATASOURCE",
            "dsctl datasource test DATASOURCE",
        ),
        compile_path="taskDefinitionJson[].taskParams.datasource",
        description=(
            "Positive datasource id or exact datasource name used for the "
            "procedure call."
        ),
    ),
    model_field(
        "task_params.method",
        compile_path="taskDefinitionJson[].taskParams.method",
        description=(
            "Canonical JDBC call such as {call schema.refresh_daily(?,?)}; "
            "the positional placeholder count must equal localParams length."
        ),
    ),
)

_PROCEDURE_PARAMETER_FIELDS = (
    "task_params.localParams[]",
    "task_params.varPool[]",
)

_PROCEDURE_TEMPLATES = (
    TaskAuthoringTemplate(
        name="minimal",
        summary="Procedure call with one selected datasource.",
        payload_modes=("task_params",),
        parameter_fields=_PROCEDURE_PARAMETER_FIELDS,
        yaml=task_template_with_runtime_controls(
            """# Task template for PROCEDURE
name: procedure-task
type: PROCEDURE
description: Call one stored procedure
task_params:
  type: MYSQL
  datasource: 1
  method: "{call refresh_daily()}"
  localParams: []
  varPool: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
    TaskAuthoringTemplate(
        name="params",
        purpose="option",
        summary="Procedure call with one typed input parameter.",
        payload_modes=("task_params",),
        parameter_fields=_PROCEDURE_PARAMETER_FIELDS,
        yaml="""task_params:
  localParams:
  - prop: bizdate
    direct: IN
    type: VARCHAR
    value: ${system.biz.date}
  method: '{call refresh_daily(?)}'
""",
    ),
)

_PROCEDURE_CALL_CONTRACT = TaskAuthoringFacetContract(
    facet_id=PROCEDURE_CALL_FACET,
    family="procedure-call-v1",
    review="procedure-seven-epoch-exact-projection",
    params_model=_family_model("PROCEDURE"),
    fields=_PROCEDURE_FIELDS,
    state_rules=(),
    templates=_PROCEDURE_TEMPLATES,
    parameter_data_types=PROCEDURE_PARAMETER_DATA_TYPES,
    allow_local_out_without_var_pool=True,
    runtime_only_fields=("outProperty",),
)


def _procedure_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    procedure_surface = get_task_authoring_surface(profile_version).procedure
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=_PROCEDURE_CALL_CONTRACT,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
        runtime_only_fields=procedure_surface.runtime_only_fields,
    )
    return TaskTypeAuthoringProfile(
        task_type="PROCEDURE",
        category="Universal",
        kind="typed",
        default_facet=PROCEDURE_CALL_FACET,
        facets={PROCEDURE_CALL_FACET: membership},
    )
