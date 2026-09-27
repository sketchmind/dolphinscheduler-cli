from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        JavaAuthoringSurface,
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

JAVA_LITERAL_FAT_JAR_FACET = "JAVA/literal_fat_jar"
_JAVA_LEGACY_SOURCE_OPAQUE_FACET = "JAVA/legacy_source_opaque"


def _java_runtime_guidance(surface: JavaAuthoringSurface) -> str:
    """Describe the exact safe fat-JAR runtime and its operational hazards."""
    if (
        not surface.available
        or not surface.typed_fat_jar_supported
        or surface.wire_epoch is None
        or surface.runtime_epoch is None
        or surface.fat_jar_run_type is None
        or surface.cancel_mode is None
    ):
        reason = surface.exclusion_reason or "typed fat-JAR runtime unavailable"
        return f"JAVA literal fat-JAR authoring is unavailable: {reason}."
    if (
        not surface.task_params_logged
        or not surface.command_logged
        or surface.result_output_supported
        or surface.durable_application_id
        or surface.failover_supported
        or not surface.retry_reexecutes
    ):
        message = "Available JAVA surface lacks its reviewed runtime contract"
        raise ValueError(message)
    common = (
        "Every eligible worker must provide JAVA_HOME, a compatible JDK, tenant "
        "execution permissions, and access to DolphinScheduler resource storage. "
        "The compiler writes the same JAR fullName to mainJar and resourceList "
        "because the worker downloads only resourceList. mainArgs are joined with "
        "one space; each item is restricted to one unquoted shell-safe token. "
        "jvmArgs stays empty, isModulePath stays false, localParams stays empty, "
        "and no additional dependency resource is authored. Upstream INFO-logs "
        "the complete task params, final shell command, and child-process output. "
        "Typed fields are not secret storage, and the CLI does not detect or "
        "redact secrets. This subset publishes no DS output, has no durable "
        "application id or failover reattachment, and retries rerun the whole JAR "
        "with possible duplicate side effects. No live evidence is refreshed and "
        "no profile is promoted. "
    )
    if surface.cancel_mode == "direct-process":
        cancel = (
            "Cancellation destroys only the direct Java process and can leave its "
            "child processes behind. "
        )
    else:
        cancel = (
            "Cancellation kills the local process tree and attempts generic "
            "application cancellation, but JAVA still has no durable remote id. "
        )
    if surface.runtime_epoch == "legacy-resource-map":
        exact = (
            "Exact 3.2.0 uses runType=JAR and the legacy resource map. Its native "
            "jvmArgs slot follows the JAR and application arguments, so the typed "
            "projector fixes it empty."
        )
    elif surface.runtime_epoch == "fixed-resource-context":
        exact = (
            "Exact 3.2.2 uses runType=JAR after repairing the 3.2.1 absolute-path "
            "double-prefix defect. Its native jvmArgs slot still follows the JAR "
            "and application arguments, so the typed projector fixes it empty."
        )
    elif surface.runtime_epoch == "fat-normal-legacy-argument-order":
        exact = (
            "Exact 3.3.1 through 3.4.0 use runType=FAT_JAR. Native jvmArgs still "
            "follows the target and application arguments, so the typed projector "
            "fixes it empty; NORMAL_JAR remains explicit opaque authoring."
        )
    else:
        exact = (
            "This exact profile uses runType=FAT_JAR and places jvmArgs before "
            "the target. The executor also substitutes the final command, but this "
            "shared literal subset keeps jvmArgs empty and rejects placeholders; "
            "NORMAL_JAR remains explicit opaque authoring."
        )
    return common + cancel + exact


def _java_fields(
    surface: JavaAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    guidance = _java_runtime_guidance(surface)
    return (
        model_field(
            "task_params.mainJar",
            choice_source="dsctl resource list",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.mainJar.resourceName",
            description=(
                "Absolute shell-safe DS resource fullName ending in .jar. The "
                "compiler also duplicates it into resourceList. " + guidance
            ),
        ),
        model_field(
            "task_params.mainArgs",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.mainArgs",
            description=(
                "Ordered application-argument tokens joined with one native space. "
                "Values are INFO-logged and are not secret storage."
            ),
        ),
        model_field(
            "task_params.mainArgs[]",
            compile_path="taskDefinitionJson[].taskParams.mainArgs",
            description=(
                "One nonblank unquoted shell-safe token without whitespace, shell "
                "expansion, or DolphinScheduler placeholders."
            ),
        ),
    )


def _java_templates(
    surface: JavaAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _java_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one downloaded literal executable fat JAR.",
            payload_modes=("task_params",),
            resource_fields=("task_params.mainJar",),
            yaml=(
                f"# Runtime prerequisite: {guidance}\n"
                "# Typed scope: mainJar plus shell-safe mainArgs only. The exact "
                "runType, mainJar/resourceList duplication, empty jvmArgs, false "
                "module-path flag, and empty parameter state are compiler-owned.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one literal executable Java fat JAR
name: run-java-fat-jar
type: JAVA
description: Run a downloaded executable fat JAR
task_params:
  mainJar: /jobs/daily-orders.jar
  mainArgs:
    - --date
    - "2026-08-30"
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
                )
            ),
        ),
    )


def _is_reviewed_java_opaque_mode(
    task_params: Mapping[str, YamlValue],
    *,
    surface: JavaAuthoringSurface,
) -> bool:
    """Recognize only the exact alternate native JAVA execution mode."""
    if surface.wire_epoch == "source-and-jar":
        raw_script = task_params.get("rawScript")
        return (
            task_params.get("runType") == "JAVA"
            and isinstance(raw_script, str)
            and bool(raw_script.strip())
        )
    if surface.wire_epoch == "fat-and-normal-jar":
        return task_params.get("runType") == "NORMAL_JAR"
    return False


def _java_legacy_source_opaque_profile(
    profile_version: str,
    *,
    surface: JavaAuthoringSurface,
) -> TaskTypeAuthoringProfile:
    """Materialize the exact 3.2.1 exclusion without opening broken JAR writes."""
    reason = surface.exclusion_reason
    if (
        not surface.available
        or surface.typed_fat_jar_supported
        or not surface.source_mode_supported
        or reason != "main-jar-absolute-path-is-double-prefixed"
    ):
        message = f"JAVA {profile_version} is not the reviewed legacy source hole"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=_JAVA_LEGACY_SOURCE_OPAQUE_FACET,
        family="java-legacy-source-opaque-v1",
        review="java-fat-jar-runtime-exclusion-with-legacy-source-opaque-mode",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when="exact legacy JAVA source-mode opaque authoring",
                condition_paths=("task_params.runType",),
                active_paths=("task_params.rawScript",),
                compile_policy=(("task_params", "preserve native object unchanged"),),
                description=(
                    "Only a nonblank native runType=JAVA source payload may be "
                    "created or edited opaquely. The broken JAR mode is "
                    "preserve-only."
                ),
            ),
        ),
        templates=(
            TaskAuthoringTemplate(
                name="minimal",
                summary="Author exact legacy JAVA source mode as native opaque state.",
                payload_modes=("task_params",),
                yaml=task_template_with_runtime_controls(
                    """# Exact 3.2.1 opaque JAVA source-mode template
# JAR create/edit is disabled because the worker double-prefixes staged paths.
name: run-java-source-opaque
type: JAVA
description: Run native legacy Java source mode
task_params:
  runType: JAVA
  rawScript: |
    public class Main {
      public static void main(String[] args) {
        System.out.println("hello from DolphinScheduler");
      }
    }
  mainArgs: ""
  jvmArgs: ""
  isModulePath: false
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
        opaque_authoring_selector=partial(
            _is_reviewed_java_opaque_mode,
            surface=surface,
        ),
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
            "JAVA literal fat-JAR typed create/edit is disabled because exact "
            f"{profile_version} has {reason}; only native runType=JAVA source "
            "create/edit and unchanged opaque preservation are allowed."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="JAVA",
        category="Universal",
        kind="generic",
        default_facet=_JAVA_LEGACY_SOURCE_OPAQUE_FACET,
        facets={_JAVA_LEGACY_SOURCE_OPAQUE_FACET: membership},
    )


def _java_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).java
    if not surface.typed_fat_jar_supported:
        if surface.available and surface.exclusion_reason is not None:
            return _java_legacy_source_opaque_profile(
                profile_version,
                surface=surface,
            )
        reason = surface.exclusion_reason or "typed fat-JAR runtime unavailable"
        message = f"JAVA typed authoring is unavailable on {profile_version}: {reason}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=JAVA_LITERAL_FAT_JAR_FACET,
        family="java-literal-fat-jar-v1",
        review="java-literal-fat-jar-shell-safe-subset",
        params_model=_family_model("JAVA"),
        fields=_java_fields(surface),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed JAVA literal fat-JAR authoring",
                condition_paths=(),
                active_paths=("task_params.mainJar", "task_params.mainArgs"),
                compile_policy=(
                    (
                        "task_params.mainJar",
                        "send the same ResourceInfo to mainJar and resourceList",
                    ),
                    ("task_params.mainArgs", "join safe tokens with one space"),
                    ("task_params.jvmArgs", "send compiler-owned empty string"),
                    ("task_params.isModulePath", "send compiler-owned false"),
                    ("task_params.localParams", "send compiler-owned empty list"),
                ),
                description=(
                    "The portable facet owns one executable fat JAR and literal "
                    "application arguments only."
                ),
            ),
        ),
        templates=_java_templates(surface),
        opaque_authoring_selector=partial(
            _is_reviewed_java_opaque_mode,
            surface=surface,
        ),
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
        task_type="JAVA",
        category="Universal",
        kind="typed",
        default_facet=JAVA_LITERAL_FAT_JAR_FACET,
        facets={JAVA_LITERAL_FAT_JAR_FACET: membership},
    )
