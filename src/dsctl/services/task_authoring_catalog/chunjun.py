from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        ChunJunAuthoringSurface,
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

CHUNJUN_LITERAL_LOCAL_JSON_JOB_FACET = "CHUNJUN/literal_local_json_job"


def _chunjun_runtime_guidance(surface: ChunJunAuthoringSurface) -> str:
    """Describe the exact local custom-JSON execution and lifecycle boundary."""
    if not surface.available:
        return "CHUNJUN is absent from this exact DolphinScheduler profile."
    if (
        surface.json_encoding != "utf-8"
        or surface.line_separator != "lf"
        or not surface.parameter_substitution
        or not surface.local_params_supported
        or surface.cancel_mode is None
        or surface.ui_custom_config_default is None
    ):
        message = "Available CHUNJUN surface lacks the reviewed UTF-8/LF contract"
        raise ValueError(message)
    application_ids = (
        "Exact 3.1.0 may discover application ids from completed process logs, "
        "but does not persist a reattachable id. "
        if surface.application_id_observation == "post-exit-log-discovery-nondurable"
        else "The task records no ChunJun application id. "
    )
    cancellation = {
        "wrapper-kill": (
            "Cancellation sends tenant-scoped soft and hard kill signals to the "
            "legacy wrapper process. "
        ),
        "direct-process-destroy": (
            "Cancellation destroys the direct worker process and forces it after "
            "five seconds when needed. "
        ),
        "process-tree-and-application": (
            "Cancellation kills the process tree and then attempts generic "
            "YARN/Kubernetes application cancellation. "
        ),
    }[surface.cancel_mode]
    ui_defect = (
        "Exact 3.1.0-3.1.6 UI defaults customConfig=false even though that built-in "
        "branch cannot construct its JSON; dsctl fixes customConfig=1. "
        if not surface.ui_custom_config_default
        else ""
    )
    return (
        "Route the task to a worker with CHUNJUN_HOME, a compatible Java and "
        "ChunJun distribution, the required connectors and drivers, endpoint "
        "connectivity, and source/target data permissions. The documented "
        "start-chunjun launcher must be patched to run in the foreground by "
        "removing its trailing background '&'; otherwise task status and "
        "cancellation are not trustworthy. Upstream accepts localParams and "
        "normalizes CRLF to LF, substitutes prepared values without JSON-aware "
        "escaping, and writes the job file as UTF-8. This typed facet therefore "
        "excludes localParams and rejects CR and DolphinScheduler placeholders. "
        "Complete task params are INFO-logged, "
        "the expanded JSON is DEBUG-logged, and the launched command and child "
        "output enter task logs, so task fields are not secret storage. "
        f"{ui_defect}{application_ids}{cancellation}There is no structured output "
        "or failover resume; cancellation remains best effort and retry reexecutes "
        "the whole transfer with possible duplicate writes."
    )


def _chunjun_fields() -> tuple[TaskAuthoringField, ...]:
    return (
        model_field(
            "task_params.json",
            compile_path="taskDefinitionJson[].taskParams.json",
            description=(
                "One nonblank syntactically valid JSON object containing the "
                "complete literal worker-local ChunJun job. DS placeholders, "
                "CR characters, parameters, resources, arbitrary launcher "
                "options, and built-in datasource generation are outside this "
                "typed facet."
            ),
        ),
    )


def _chunjun_state_rules(
    surface: ChunJunAuthoringSurface,
) -> tuple[TaskAuthoringStateRule, ...]:
    return (
        TaskAuthoringStateRule(
            when="typed CHUNJUN literal worker-local JSON job authoring",
            condition_paths=(),
            active_paths=("task_params.json",),
            compile_policy=(
                ("task_params.customConfig", "send compiler-owned 1"),
                ("task_params.deployMode", "send compiler-owned local"),
            ),
            description=(
                "The compiler selects the only runnable custom-JSON branch and "
                "worker-local launch. It omits empty UI residue. Correctly spelled "
                "reviewed non-local custom modes remain selector-restricted native "
                "opaque authoring; built-in mode is preserve-only because the "
                "worker never builds its JSON. Local options, parameters, "
                "resources, inherited runtime state, and future fields remain "
                "unchanged/export opaque preservation only. "
                f"{_chunjun_runtime_guidance(surface)}"
            ),
        ),
    )


def _chunjun_templates(
    surface: ChunJunAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    comments = (
        f"# Runtime prerequisite: {_chunjun_runtime_guidance(surface)}\n"
        "# Typed scope: one literal worker-local JSON-object job. Replace the "
        "content array with a complete connector configuration before running.\n"
        "# Compiler-owned wire: customConfig=1 and deployMode=local; empty UI "
        "residue is omitted.\n"
        "# Preservation: built-in mode, local launcher options, parameters, "
        "resources, inherited runtime state, and future fields are "
        "unchanged/export opaque preservation only.\n"
        "# Logging warning: do not place credentials or other secrets in this JSON.\n"
    )
    body = task_template_with_runtime_controls(
        """# Task template for one literal worker-local ChunJun JSON job
name: run-literal-chunjun-job
type: CHUNJUN
description: Run one literal ChunJun custom JSON job locally
task_params:
  json: |-
    {
      "job": {
        "content": [],
        "setting": {"speed": {"channel": 1}}
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
            summary="Author one literal worker-local ChunJun JSON job.",
            payload_modes=("task_params",),
            yaml=f"{comments}{body}",
        ),
    )


def _is_reviewed_chunjun_nonlocal_opaque_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize only documented native non-local custom-JSON modes."""
    custom_config = task_params.get("customConfig")
    return (
        isinstance(custom_config, int)
        and not isinstance(custom_config, bool)
        and custom_config == 1
        and task_params.get("deployMode")
        in {"standalone", "yarn-session", "yarn-per-job"}
    )


def _chunjun_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).chunjun
    if not surface.available:
        message = f"CHUNJUN is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=CHUNJUN_LITERAL_LOCAL_JSON_JOB_FACET,
        family="chunjun-literal-local-json-job-v1",
        review="chunjun-literal-local-json-job-exact-subset",
        params_model=_family_model("CHUNJUN"),
        fields=_chunjun_fields(),
        state_rules=_chunjun_state_rules(surface),
        templates=_chunjun_templates(surface),
        opaque_authoring_selector=_is_reviewed_chunjun_nonlocal_opaque_mode,
        restrict_opaque_authoring_to_selector=True,
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="CHUNJUN",
        category="Other",
        kind="typed",
        default_facet=CHUNJUN_LITERAL_LOCAL_JSON_JOB_FACET,
        facets={CHUNJUN_LITERAL_LOCAL_JSON_JOB_FACET: membership},
    )
