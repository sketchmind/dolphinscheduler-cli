from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        PytorchAuthoringSurface,
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

PYTORCH_LITERAL_RESOURCE_SCRIPT_FACET = "PYTORCH/literal_resource_script"


def _pytorch_runtime_guidance(
    surface: PytorchAuthoringSurface,
    *,
    profile_version: str,
) -> str:
    """Describe the exact PYTORCH launcher, disclosure, and lifecycle limits."""
    if not surface.available or surface.wire_epoch is None:
        return "PYTORCH is absent from this exact DolphinScheduler profile."
    launcher = (
        "pythonCommand"
        if surface.wire_epoch == "positive-resource-id-python-home"
        else "pythonLauncher"
    )
    encoding = (
        "The exact worker writes the command file as UTF-8. "
        if surface.script_encoding == "utf-8"
        else "The exact worker writes the command file with its platform charset. "
    )
    stop = (
        "Outer worker machinery attempts best-effort process cancellation. "
        if surface.stop_mode == "outer-process-best-effort"
        else (
            "The 3.3.x stop path reaches only the plugin no-op and can leave the "
            "process running. "
        )
    )
    timeout = (
        "This 3.3.x executor can block before its timeout branch; typed authoring "
        "therefore requires timeout=0. "
        if surface.stop_mode == "plugin-cancel-noop"
        else "Timeout cancellation remains best-effort. "
    )
    resource_resolution = (
        "The canonical scriptResource is relative to the FILE root; the compiler "
        "resolves its positive id. "
        if surface.wire_epoch == "positive-resource-id-python-home"
        else (
            "The canonical scriptResource is relative to the FILE root; the "
            "compiler resolves the exact storage base and persists its absolute "
            "resourceName. "
        )
    )
    if profile_version in {"3.2.0", "3.2.1", "3.2.2"}:
        resource_resolution += (
            "The 3.2.x admin base-dir endpoint returns an ALL-resource root, so "
            "typed resolution fails closed for admin sessions; use a non-admin "
            "tenant user. "
        )
    return (
        "pythonExecutable must exist on every eligible Unix-like worker and include "
        "the required PyTorch dependencies. The compiler projects it into native "
        f"{launcher}, fixes environment creation off, and runs one staged .py "
        "resource with literal shell-safe argument tokens. Upstream substitutes "
        "prepared values into the unquoted command, so every typed command-bearing "
        f"value is literal ASCII. {encoding}Upstream INFO-logs full "
        "task parameters and the command, so values are not secret storage. "
        "Git projects, environment creation, launcher shell fragments, parameters, "
        "outputs, and extra resources are outside typed authoring. "
        f"{resource_resolution}{stop}{timeout}"
        "There is no durable application id or failover reattachment, and retry "
        "reruns the complete script with possible duplicate effects."
    )


def _pytorch_fields(
    surface: PytorchAuthoringSurface,
    *,
    profile_version: str,
) -> tuple[TaskAuthoringField, ...]:
    guidance = _pytorch_runtime_guidance(surface, profile_version=profile_version)
    return (
        model_field(
            "task_params.pythonExecutable",
            compile_path="taskDefinitionJson[].taskParams.pythonCommand|pythonLauncher",
            description=(
                "Absolute ASCII shell-safe Python executable path present on every "
                "eligible worker. It is INFO-logged and is not secret storage."
            ),
        ),
        model_field(
            "task_params.scriptResource",
            choice_source="dsctl resource list",
            related_commands=(
                "dsctl resource list",
                "dsctl resource upload --file FILE",
                "dsctl resource view RESOURCE",
            ),
            compile_path="taskDefinitionJson[].taskParams.resourceList[0]",
            description=(
                "Leading-slash ASCII shell-safe path relative to the selected "
                "user's DS FILE resource root and ending in .py; do not pass the "
                "storage absolute fullName. The compiler resolves the exact wire "
                f"identity and stages the relative script path. {guidance}"
            ),
        ),
        model_field(
            "task_params.scriptArgs",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.scriptParams",
            description=(
                "Ordered script-argument tokens joined with one native space. "
                "Values are INFO-logged and are not secret storage."
            ),
        ),
        model_field(
            "task_params.scriptArgs[]",
            compile_path="taskDefinitionJson[].taskParams.scriptParams",
            description=(
                "One nonblank ASCII shell-safe token without whitespace, shell "
                "expansion, or DolphinScheduler placeholders."
            ),
        ),
    )


def _pytorch_templates(
    surface: PytorchAuthoringSurface,
    *,
    profile_version: str,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _pytorch_runtime_guidance(surface, profile_version=profile_version)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one downloaded literal Python script with PyTorch.",
            payload_modes=("task_params",),
            resource_fields=("task_params.scriptResource",),
            yaml=(
                f"# Runtime prerequisites and limits: {guidance}\n"
                "# Typed scope: an explicit Python executable, one .py resource, "
                "and shell-safe scriptArgs only.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one downloaded PyTorch script
name: train-pytorch-model
type: PYTORCH
description: Run a downloaded PyTorch training script
task_params:
  pythonExecutable: /usr/bin/python3
  scriptResource: /ml/train.py
  scriptArgs:
    - --epochs
    - "10"
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


def _pytorch_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    """Materialize the seven exact reviewed PYTORCH memberships."""
    surface = get_task_authoring_surface(profile_version).pytorch
    if not surface.available:
        message = f"PYTORCH is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=PYTORCH_LITERAL_RESOURCE_SCRIPT_FACET,
        family="pytorch-literal-resource-script-v1",
        review="pytorch-literal-resource-script-exact-subset",
        params_model=_family_model("PYTORCH"),
        fields=_pytorch_fields(surface, profile_version=profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed PYTORCH literal resource-script authoring",
                condition_paths=("timeout",),
                active_paths=(
                    "task_params.pythonExecutable",
                    "task_params.scriptResource",
                    "task_params.scriptArgs",
                ),
                compile_policy=(
                    (
                        "task_params.pythonExecutable",
                        "send as pythonCommand or pythonLauncher by exact version",
                    ),
                    (
                        "task_params.scriptResource",
                        "stage one exact-version ResourceInfo and derive script",
                    ),
                    (
                        "task_params.scriptArgs",
                        "join literal safe tokens with one space",
                    ),
                    ("task_params.isCreateEnvironment", "send compiler-owned false"),
                    ("task_params.pythonPath", "send compiler-owned dot path"),
                    ("task_params.localParams", "send compiler-owned empty list"),
                    ("timeout", "require zero on exact 3.3.x"),
                ),
                description=(
                    "The portable facet owns one worker Python executable, one "
                    "downloaded .py script, and literal arguments only. Richer "
                    "native state is preserve-only."
                ),
            ),
        ),
        templates=_pytorch_templates(surface, profile_version=profile_version),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "PYTORCH/literal_resource_script closes raw opaque create/edit; "
            "richer native state is unchanged/export preserve-only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="PYTORCH",
        category="MachineLearning",
        kind="typed",
        default_facet=PYTORCH_LITERAL_RESOURCE_SCRIPT_FACET,
        facets={PYTORCH_LITERAL_RESOURCE_SCRIPT_FACET: membership},
    )
