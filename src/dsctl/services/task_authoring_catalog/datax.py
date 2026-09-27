from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec.datax import validate_datax_inline_job_presence
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        DataxAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringStateRule,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

DATAX_LITERAL_CUSTOM_JSON_JOB_FACET = "DATAX/literal_custom_json_job"
_DATAX_RUNTIME_EXCLUSION_OPAQUE_FACET = "DATAX/runtime_exclusion_opaque"


def validate_semantics(
    task_params: Mapping[str, YamlValue], *, surface: DataxAuthoringSurface
) -> None:
    """Preserve the exact distinction between literal jobs and resource fallback."""
    value = task_params.get("json")
    if isinstance(value, str):
        validate_datax_inline_job_presence(
            value, empty_json_object_is_absent=surface.empty_json_object_is_absent
        )


def _datax_runtime_guidance(surface: DataxAuthoringSurface) -> str:
    """Describe the exact launcher, disclosure, and replay boundaries."""
    if not surface.registered:
        return "DATAX is absent from this exact DolphinScheduler profile."
    if not surface.typed_custom_json_available:
        reason = surface.exclusion_reason or "exact custom-JSON runtime unavailable"
        return f"DATAX typed custom-JSON authoring is unavailable: {reason}."
    if surface.wire_epoch not in {
        "legacy-custom-json",
        "custom-json-jvm-memory",
    }:
        message = "Available DATAX surface lacks an exact custom-JSON wire epoch"
        raise ValueError(message)
    if surface.line_separator not in {"lf", "system"}:
        message = "Available DATAX surface lacks an exact line-separator contract"
        raise ValueError(message)
    if (
        not surface.parameter_substitution
        or not surface.task_params_logged
        or not surface.command_logged
        or surface.secret_storage_supported
        or surface.result_output_supported
        or surface.cancel_mode
        not in {
            "wrapper-kill",
            "direct-process-destroy",
            "process-tree-and-application",
        }
        or surface.durable_application_id
        or surface.failover_supported
        or not surface.retry_reexecutes
    ):
        message = "Available DATAX surface contradicts the reviewed runtime contract"
        raise ValueError(message)
    line_separator = (
        "LF"
        if surface.line_separator == "lf"
        else "the eligible worker's operating-system line separator"
    )
    resource_scope = (
        "Upstream can stage resource files, but this facet deliberately authors none. "
        if surface.resource_files_supported
        else "This exact executor does not expose resource-file staging. "
    )
    if surface.empty_json_object_is_absent:
        resource_scope += (
            "An empty JSON object selects native resource-file fallback and is "
            "therefore rejected by this literal-job facet. "
        )
    prepared_params = (
        "The executor also forwards every prepared workflow, startup, local, and "
        "varPool value to the DataX CLI as shell-built -p -D options. Upstream "
        "does not safely quote shell metacharacters there, so a parameter-free "
        "workflow is a prerequisite for this facet's safe typed claim. "
        "dsctl rejects visible workflow globals. Startup and worker-prepared "
        "values must be absent; dsctl cannot verify their absence. "
        if surface.prepared_params_forwarded
        else "The executor does not forward prepared values as DataX CLI -p options. "
    )
    cancellation = {
        "wrapper-kill": "the legacy worker-local command-wrapper kill",
        "direct-process-destroy": (
            "direct destruction of the worker-local launched process"
        ),
        "process-tree-and-application": (
            "worker-local process-tree kill plus generic application cancellation"
        ),
    }[surface.cancel_mode]
    return (
        f"Route the task to a worker configured for {surface.python_launcher} and "
        f"{surface.datax_launcher}, with compatible DataX plugins, drivers, "
        "network access, and source/target data permissions. The worker replaces "
        f"source-text CRLF pairs with {line_separator}, then substitutes prepared "
        "values and writes the job document as UTF-8. Typed input rejects CR and "
        "every DolphinScheduler placeholder so accepted JSON keeps its exact "
        f"spelling. {prepared_params}{resource_scope}Upstream logs complete task "
        "parameters and the final launched command at INFO; child-process output "
        "also enters task logs. Task fields are not secret storage and dsctl does "
        "not redact them. The task publishes no structured output or durable "
        f"application id. Cancellation uses {cancellation}; worker failover cannot "
        "resume it, and retry reexecutes the whole "
        "transfer with possible duplicate writes."
    )


def _datax_fields(
    surface: DataxAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    compile_path = (
        "processDefinitionJson.tasks[].params.json"
        if surface.wire_epoch == "legacy-custom-json"
        else "taskDefinitionJson[].taskParams.json"
    )
    return (
        model_field(
            "task_params.json",
            compile_path=compile_path,
            description=(
                "One nonblank syntactically valid JSON object containing the "
                "complete literal DataX job. Arrays and scalar roots, DS "
                "placeholders, CR characters, parameters, datasource "
                "generation, and resource references are outside this typed "
                f"facet. {_datax_runtime_guidance(surface)}"
            ),
        ),
    )


def _datax_state_rules(
    surface: DataxAuthoringSurface,
) -> tuple[TaskAuthoringStateRule, ...]:
    compile_policy: list[tuple[str, str]] = [
        ("task_params.customConfig", "send compiler-owned 1"),
    ]
    if surface.wire_epoch != "legacy-custom-json":
        compile_policy.extend(
            (
                ("task_params.xms", "send compiler-owned 1"),
                ("task_params.xmx", "send compiler-owned 1"),
            )
        )
    return (
        TaskAuthoringStateRule(
            when="typed DATAX literal custom-JSON job authoring",
            condition_paths=(),
            active_paths=("task_params.json",),
            compile_policy=tuple(compile_policy),
            description=(
                "The compiler selects native customConfig=1 and owns every exact "
                "wrapper default. It omits UI-only empty localParams and "
                "resourceList state. DataSource-generated mode, resources, "
                "authored parameters, JVM memory controls, inherited runtime "
                "state, and future fields remain outside typed create/edit and "
                "are available only through unchanged/export opaque preservation."
            ),
        ),
    )


def _datax_templates(
    surface: DataxAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    comments = (
        f"# Runtime prerequisite: {_datax_runtime_guidance(surface)}\n"
        "# Typed scope: one literal JSON-object job. The compiler fixes "
        "customConfig=1, fixes xms/xmx=1 where native, and omits UI-only empty "
        "localParams/resourceList fields.\n"
        "# Preservation: datasource-generated mode, resource files, parameters, "
        "xms/xmx, inherited runtime state, and future fields remain "
        "unchanged/export opaque preservation only.\n"
        "# Logging warning: the complete task parameters and launched command are "
        "logged; do not place credentials or other secrets in this JSON.\n"
    )
    body = task_template_with_runtime_controls(
        """# Task template for one literal DataX stream-to-stream job
name: run-literal-datax-job
type: DATAX
description: Run one literal DataX custom JSON job
task_params:
  json: |-
    {
      "job": {
        "setting": {"speed": {"channel": 1}},
        "content": [
          {
            "reader": {
              "name": "streamreader",
              "parameter": {
                "column": [{"type": "string", "value": "hello-datax"}],
                "sliceRecordCount": 1
              }
            },
            "writer": {
              "name": "streamwriter",
              "parameter": {"print": true}
            }
          }
        ]
      }
    }
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal DataX custom JSON stream-to-stream job.",
            payload_modes=("task_params",),
            yaml=f"{comments}{body}",
        ),
    )


def _is_explicit_datax_native_opaque_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Require the native mode discriminator before selecting raw authoring."""
    custom_config = task_params.get("customConfig")
    return (
        isinstance(custom_config, int)
        and not isinstance(custom_config, bool)
        and custom_config in {0, 1}
    )


def _datax_runtime_exclusion_opaque_profile(
    profile_version: str,
    *,
    surface: DataxAuthoringSurface,
) -> TaskTypeAuthoringProfile:
    """Materialize the 3.1.0 hole without accepting canonical input as native."""
    reason = surface.exclusion_reason
    if (
        profile_version != "3.1.0"
        or not surface.registered
        or surface.typed_custom_json_available
        or reason != "null-empty-prepare-params-map-breaks-custom-command"
    ):
        message = f"DATAX {profile_version} is not the reviewed runtime hole"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=_DATAX_RUNTIME_EXCLUSION_OPAQUE_FACET,
        family="datax-runtime-exclusion-opaque-v1",
        review="datax-custom-json-runtime-exclusion-with-explicit-native-opaque-mode",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when="exact 3.1.0 DATAX native opaque authoring",
                condition_paths=("task_params.customConfig",),
                active_paths=("task_params",),
                compile_policy=(("task_params", "preserve native object unchanged"),),
                description=(
                    "Only a native object with strict customConfig=0 or 1 may be "
                    "created or edited opaquely. Canonical json-only input remains "
                    "fail-closed because the 3.1.0 custom-command executor "
                    "dereferences an empty prepared-parameter map."
                ),
            ),
        ),
        templates=(
            TaskAuthoringTemplate(
                name="minimal",
                summary=(
                    "Start an explicit native DATAX payload for the exact 3.1.0 "
                    "opaque escape hatch."
                ),
                payload_modes=("task_params",),
                yaml=task_template_with_runtime_controls(
                    """# Exact 3.1.0 native opaque DATAX scaffold
# Typed custom-JSON authoring is disabled because the worker dereferences an
# empty prepared-parameter map. This scaffold is not executable until you add
# the exact native fields required by your chosen built-in DataX mode.
name: run-datax-native-opaque
type: DATAX
description: Author an explicit native DataX payload at your own risk
task_params:
  customConfig: 0
  xms: 1
  xmx: 1
  localParams: []
  resourceList: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
                ),
            ),
        ),
        opaque_authoring_selector=_is_explicit_datax_native_opaque_mode,
        restrict_opaque_authoring_to_selector=True,
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=False,
        typed_edit=False,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
        constraint=(
            "DATAX literal custom-JSON typed create/edit is disabled because exact "
            f"{profile_version} has {reason}; only explicitly discriminated native "
            "opaque create/edit and unchanged opaque preservation are allowed."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="DATAX",
        category="DataIntegration",
        kind="generic",
        default_facet=_DATAX_RUNTIME_EXCLUSION_OPAQUE_FACET,
        facets={_DATAX_RUNTIME_EXCLUSION_OPAQUE_FACET: membership},
    )


def _datax_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).datax
    if not surface.registered:
        message = f"DATAX is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    if not surface.typed_custom_json_available:
        if surface.exclusion_reason is not None:
            return _datax_runtime_exclusion_opaque_profile(
                profile_version,
                surface=surface,
            )
        reason = surface.exclusion_reason or "exact custom-JSON runtime unavailable"
        message = (
            "DATAX literal custom JSON typed authoring is unavailable on "
            f"DolphinScheduler {profile_version}: {reason}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DATAX_LITERAL_CUSTOM_JSON_JOB_FACET,
        family="datax-literal-custom-json-job-v1",
        review="datax-literal-custom-json-job-exact-subset",
        params_model=_family_model("DATAX"),
        fields=_datax_fields(surface),
        state_rules=_datax_state_rules(surface),
        templates=_datax_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="DATAX",
        category="DataIntegration",
        kind="typed",
        default_facet=DATAX_LITERAL_CUSTOM_JSON_JOB_FACET,
        facets={DATAX_LITERAL_CUSTOM_JSON_JOB_FACET: membership},
    )
