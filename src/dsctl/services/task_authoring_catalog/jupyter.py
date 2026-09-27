from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlValue
    from dsctl.upstream.task_authoring_surface import (
        JupyterAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

JUPYTER_PREINSTALLED_NOTEBOOK_FACET = "JUPYTER/preinstalled_notebook"


def _jupyter_runtime_guidance(surface: JupyterAuthoringSurface) -> str:
    """Describe exact worker prerequisites without widening typed ownership."""
    if not surface.available:
        return "JUPYTER is absent from this exact DolphinScheduler profile."
    logging = (
        "Authored task parameters and the assembled command are logged upstream. "
        if surface.authored_values_logged
        else ""
    )
    failover = (
        "Upstream exposes no remote application id or failover resume; retry "
        "re-executes the notebook. "
        if not surface.failover_supported
        else ""
    )
    return (
        "Use a POSIX worker with conda.path configured and the selected Conda "
        "environment, Papermill, Jupyter, kernel, and engine already installed. "
        "The input notebook must be readable and the output path writable on that "
        f"worker. {logging}{failover}The output notebook remains a worker file, "
        "not a DolphinScheduler output parameter."
    )


def _jupyter_fields(
    surface: JupyterAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    """Return the portable preinstalled-notebook authoring surface."""
    runtime = _jupyter_runtime_guidance(surface)
    return (
        model_field(
            "task_params.condaEnvName",
            compile_path="taskDefinitionJson[].taskParams.condaEnvName",
            description=(
                "Shell-safe preinstalled Conda environment name. Resource-backed "
                f".txt/.tar.gz bootstrap remains opaque-only. {runtime}"
            ),
        ),
        model_field(
            "task_params.inputNotePath",
            compile_path="taskDefinitionJson[].taskParams.inputNotePath",
            description=(
                "Absolute shell-safe POSIX path to one readable .ipynb notebook."
            ),
        ),
        model_field(
            "task_params.outputNotePath",
            compile_path="taskDefinitionJson[].taskParams.outputNotePath",
            description=(
                "Distinct absolute shell-safe POSIX .ipynb output path on the "
                "worker; this is not a DS resource upload or output parameter."
            ),
        ),
        model_field(
            "task_params.parameters",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.parameters",
            description=(
                "Literal shell-safe string map serialized to the native JSON "
                "string. Exact secret-like key names and URI userinfo are rejected, "
                "but dsctl does not redact arbitrary secrets and detects only this "
                "bounded subset; never use this map as secret storage. DS "
                "placeholders are outside typed ownership."
            ),
        ),
        model_field(
            "task_params.kernel",
            compile_path="taskDefinitionJson[].taskParams.kernel",
            description="Optional shell-safe preinstalled Papermill kernel name.",
        ),
        model_field(
            "task_params.engine",
            compile_path="taskDefinitionJson[].taskParams.engine",
            description="Optional shell-safe preinstalled Papermill engine name.",
        ),
        model_field(
            "task_params.executionTimeout",
            compile_path="taskDefinitionJson[].taskParams.executionTimeout",
            description=(
                "Optional strict positive cell timeout in seconds; exact projection "
                "encodes the canonical integer as a native decimal string."
            ),
        ),
        model_field(
            "task_params.startTimeout",
            compile_path="taskDefinitionJson[].taskParams.startTimeout",
            description=(
                "Optional strict positive kernel-start timeout in seconds; exact "
                "projection encodes the canonical integer as a decimal string."
            ),
        ),
    )


def _jupyter_template_body(
    surface: JupyterAuthoringSurface,
    body: str,
) -> str:
    comments = (
        f"# Runtime prerequisite: {_jupyter_runtime_guidance(surface)}\n"
        "# Typed scope: a preinstalled environment and shell-safe literal values. "
        "The bounded denylist is not comprehensive secret detection; keep secrets "
        "in worker configuration. Resource bootstrap and raw options remain "
        "opaque-only.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _jupyter_templates(
    surface: JupyterAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    minimal = TaskAuthoringTemplate(
        name="minimal",
        summary="Execute one notebook in a preinstalled Conda environment.",
        payload_modes=("task_params",),
        yaml=_jupyter_template_body(
            surface,
            """# Task template for one preinstalled Jupyter notebook
name: execute-preinstalled-notebook
type: JUPYTER
description: Execute one notebook with Papermill
task_params:
  condaEnvName: analytics-py310
  inputNotePath: /opt/notebooks/input.ipynb
  outputNotePath: /opt/notebooks/output.ipynb
  parameters: {}
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
        ),
    )
    params = TaskAuthoringTemplate(
        name="params",
        purpose="option",
        summary="Execute one notebook with literal parameters and safe options.",
        payload_modes=("task_params",),
        yaml="""task_params:
  parameters:
    business_date: '2026-08-20'
    region: east
""",
    )
    return (minimal, params)


def _is_reviewed_jupyter_opaque_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize only the exact upstream resource-bootstrap discriminator."""
    conda_env_name = task_params.get("condaEnvName")
    return isinstance(conda_env_name, str) and conda_env_name.endswith(
        (".txt", ".tar.gz")
    )


def _jupyter_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).jupyter
    if not surface.supports_preinstalled_notebook:
        message = (
            "JUPYTER does not expose the reviewed preinstalled-notebook contract "
            f"in DolphinScheduler {profile_version}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=JUPYTER_PREINSTALLED_NOTEBOOK_FACET,
        family="jupyter-preinstalled-notebook-v1",
        review="jupyter-preinstalled-notebook-shell-safe-subset",
        params_model=_family_model("JUPYTER"),
        fields=_jupyter_fields(surface),
        state_rules=(),
        templates=_jupyter_templates(surface),
        opaque_authoring_selector=_is_reviewed_jupyter_opaque_mode,
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
        task_type="JUPYTER",
        category="MachineLearning",
        kind="typed",
        default_facet=JUPYTER_PREINSTALLED_NOTEBOOK_FACET,
        facets={JUPYTER_PREINSTALLED_NOTEBOOK_FACET: membership},
    )
