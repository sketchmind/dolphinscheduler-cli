from __future__ import annotations

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

DVC_OPERATION_FACET = "DVC/operation"


def _dvc_runtime_guidance() -> str:
    return (
        "Route to a POSIX worker with git and dvc on PATH, configured Git "
        "identity and repository/DVC-remote credentials. Put credentials in "
        "the worker SSH agent or credential helper, never in task fields. "
        "Upstream assembles a shell script, logs its parameters and command, "
        "does not guard every intermediate command, and cannot resume a remote "
        "DVC job after worker failover."
    )


def _dvc_fields() -> tuple[TaskAuthoringField, ...]:
    shell_word_guidance = (
        "One shell-safe token only: no whitespace, glob, option prefix, shell "
        "expansion, control text, or inline URI credentials."
    )
    return (
        model_field(
            "task_params.dvcTaskType",
            compile_path="taskDefinitionJson[].taskParams.dvcTaskType",
            description=(f"Exact native DVC operation. {_dvc_runtime_guidance()}"),
        ),
        model_field(
            "task_params.dvcRepository",
            compile_path="taskDefinitionJson[].taskParams.dvcRepository",
            description=(
                f"Git repository read by every DVC mode. {shell_word_guidance}"
            ),
        ),
        model_field(
            "task_params.dvcDataLocation",
            required=True,
            active_when=("task_params.dvcTaskType is Upload or Download"),
            compile_path="taskDefinitionJson[].taskParams.dvcDataLocation",
            description=f"DVC-tracked data location. {shell_word_guidance}",
        ),
        model_field(
            "task_params.dvcLoadSaveDataPath",
            required=True,
            active_when=("task_params.dvcTaskType is Upload or Download"),
            compile_path="taskDefinitionJson[].taskParams.dvcLoadSaveDataPath",
            description=f"Worker-side load/save path. {shell_word_guidance}",
        ),
        model_field(
            "task_params.dvcVersion",
            required=True,
            active_when=("task_params.dvcTaskType is Upload or Download"),
            compile_path="taskDefinitionJson[].taskParams.dvcVersion",
            description=(
                "Portable Git tag, branch, or SHA used by DVC. Advanced Git "
                "revspec syntax remains opaque-only."
            ),
        ),
        model_field(
            "task_params.dvcMessage",
            required=True,
            active_when="task_params.dvcTaskType == Upload",
            compile_path="taskDefinitionJson[].taskParams.dvcMessage",
            description=(
                "Git commit/tag message. Spaces, single quotes, and Unicode are "
                "allowed; shell expansion, double quotes, backslashes, and "
                "control text are rejected."
            ),
        ),
        model_field(
            "task_params.dvcStoreUrl",
            required=True,
            active_when="task_params.dvcTaskType == Init DVC",
            compile_path="taskDefinitionJson[].taskParams.dvcStoreUrl",
            description=(f"DVC remote configured by Init DVC. {shell_word_guidance}"),
        ),
    )


_DVC_STATE_RULES = (
    TaskAuthoringStateRule(
        when="task_params.dvcTaskType == Upload",
        condition_paths=("task_params.dvcTaskType",),
        active_paths=(
            "task_params.dvcDataLocation",
            "task_params.dvcLoadSaveDataPath",
            "task_params.dvcVersion",
            "task_params.dvcMessage",
        ),
        inactive_paths=("task_params.dvcStoreUrl",),
        description="Upload and version one worker-local data path.",
    ),
    TaskAuthoringStateRule(
        when="task_params.dvcTaskType == Download",
        condition_paths=("task_params.dvcTaskType",),
        active_paths=(
            "task_params.dvcDataLocation",
            "task_params.dvcLoadSaveDataPath",
            "task_params.dvcVersion",
        ),
        inactive_paths=(
            "task_params.dvcMessage",
            "task_params.dvcStoreUrl",
        ),
        description="Download one DVC location at a portable Git revision.",
    ),
    TaskAuthoringStateRule(
        when="task_params.dvcTaskType == Init DVC",
        condition_paths=("task_params.dvcTaskType",),
        active_paths=("task_params.dvcStoreUrl",),
        inactive_paths=(
            "task_params.dvcDataLocation",
            "task_params.dvcLoadSaveDataPath",
            "task_params.dvcVersion",
            "task_params.dvcMessage",
        ),
        description="Initialize DVC metadata and one configured remote.",
    ),
)


def _dvc_template_body(body: str) -> str:
    comments = (
        f"# Runtime prerequisite: {_dvc_runtime_guidance()}\n"
        "# Security boundary: typed values form a shell-safe subset; use worker "
        "Git/DVC credential configuration.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


_DVC_TEMPLATES = (
    TaskAuthoringTemplate(
        name="upload",
        summary="Upload and version one data path with DVC and Git.",
        payload_modes=("task_params",),
        yaml=_dvc_template_body(
            """# Task template for DVC Upload
name: dvc-upload
type: DVC
description: Upload one versioned dataset
task_params:
  dvcTaskType: Upload
  dvcRepository: git@github.com:example-org/dvc-data.git
  dvcDataLocation: datasets/orders-v1
  dvcLoadSaveDataPath: ~/datasets/orders-v1
  dvcVersion: orders_v1.0.0
  dvcMessage: Publish orders dataset v1.0.0
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
        name="download",
        summary="Download one DVC data revision to a worker path.",
        payload_modes=("task_params",),
        yaml=_dvc_template_body(
            """# Task template for DVC Download
name: dvc-download
type: DVC
description: Download one versioned dataset
task_params:
  dvcTaskType: Download
  dvcRepository: git@github.com:example-org/dvc-data.git
  dvcDataLocation: datasets/orders-v1
  dvcLoadSaveDataPath: /var/lib/dvc/orders-v1
  dvcVersion: orders_v1.0.0
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
        name="init",
        summary="Initialize DVC metadata and configure one remote store.",
        payload_modes=("task_params",),
        yaml=_dvc_template_body(
            """# Task template for DVC Init
name: dvc-init
type: DVC
description: Initialize one DVC repository
task_params:
  dvcTaskType: Init DVC
  dvcRepository: git@github.com:example-org/dvc-data.git
  dvcStoreUrl: s3://example-bucket/dvc-data
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        ),
    ),
)


_DVC_OPERATION_CONTRACT = TaskAuthoringFacetContract(
    facet_id=DVC_OPERATION_FACET,
    family="dvc-operation-v1",
    review="dvc-exact-shell-safe-subset",
    params_model=_family_model("DVC"),
    fields=_dvc_fields(),
    state_rules=_DVC_STATE_RULES,
    templates=_DVC_TEMPLATES,
)


def _dvc_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=_DVC_OPERATION_CONTRACT,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="DVC",
        category="MachineLearning",
        kind="typed",
        default_facet=DVC_OPERATION_FACET,
        facets={DVC_OPERATION_FACET: membership},
    )
