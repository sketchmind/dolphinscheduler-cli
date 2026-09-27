from __future__ import annotations

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
)

from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.k8s import (
    _K8S_NAMESPACE_PATTERN,
    K8S_LITERAL_JSON_SCHEMA_PATTERN,
    K8S_NAMESPACE_JSON_SCHEMA_PATTERN,
    validate_k8s_literal,
)

KUBEFLOW_WORKFLOW_INSTANCE_PARAMETER_NAME = "system.workflow.instance.id"
KUBEFLOW_WORKFLOW_INSTANCE_PLACEHOLDER = (
    f"${{{KUBEFLOW_WORKFLOW_INSTANCE_PARAMETER_NAME}}}"
)
KUBEFLOW_TFJOB_NAME_PREFIX_MAX_LENGTH = 52
_KUBEFLOW_LOGGED_VALUE_DESCRIPTION = (
    "Upstream INFO-logs complete KUBEFLOW task parameters, the expanded manifest, "
    "status conditions, and terminal resource JSON. This field is not secret "
    "storage, and dsctl neither detects nor redacts secrets."
)


class KubeflowTfjobManifestTaskParamsSpec(TaskParamsSpec):
    """One closed TFJob manifest with workflow-instance-scoped identity."""

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=False,
        validate_by_alias=True,
        validate_by_name=False,
        strict=True,
        json_schema_extra={
            "x-dsctl-runtime-validations": [
                "yamlContent is one ASCII LF single-document YAML mapping",
                "apiVersion is kubeflow.org/v1 and kind is TFJob",
                "metadata.namespace exactly matches namespace",
                "spec.tfReplicaSpecs is a nonempty mapping and root status is absent",
                "generateName and server-owned metadata fields are rejected",
                (
                    "metadata.name is a DNS-safe prefix followed by the sole "
                    "allowed ${system.workflow.instance.id} placeholder"
                ),
                (
                    "duplicates, aliases, merge keys, and YAML tags outside "
                    "JSON-core string/null/bool/int/float are rejected"
                ),
            ]
        },
    )

    namespace: str = Field(
        min_length=1,
        description=_KUBEFLOW_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_NAMESPACE_JSON_SCHEMA_PATTERN},
    )
    cluster: str = Field(
        min_length=1,
        description=_KUBEFLOW_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_LITERAL_JSON_SCHEMA_PATTERN},
    )
    yaml_content: str = Field(
        alias="yamlContent",
        min_length=1,
        max_length=1_000_000,
        description=_KUBEFLOW_LOGGED_VALUE_DESCRIPTION,
    )

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        """Require the same DNS-1123 namespace shape as upstream K8S selection."""
        if not _K8S_NAMESPACE_PATTERN.fullmatch(value):
            message = "namespace must be one Kubernetes DNS-1123 label"
            raise ValueError(message)
        return value

    @field_validator("cluster")
    @classmethod
    def validate_cluster(cls, value: str) -> str:
        """Keep the server-side cluster lookup name literal and unambiguous."""
        return validate_k8s_literal(value, field="cluster")
