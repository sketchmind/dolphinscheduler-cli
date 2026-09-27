from __future__ import annotations

import math
import re
from enum import StrEnum
from typing import Literal

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from dsctl.models.common import (
    YamlSpecModel,
    YamlValue,
)
from dsctl.models.task_spec.base import (
    TaskParamsSpec,
)
from dsctl.models.task_spec.datasource_ref import DatasourceReference
from dsctl.models.task_spec.values import UNICODE_EDGE_WHITESPACE_PATTERN


class K8sConnectionMode(StrEnum):
    """Stable Kubernetes connection intent across native wire epochs."""

    NAMESPACE = "NAMESPACE"
    DATASOURCE = "DATASOURCE"


K8S_TASK_NAME_MAX_LENGTH = 52
K8S_TASK_NAME_JSON_SCHEMA_PATTERN = (
    rf"^[A-Za-z0-9][A-Za-z0-9-]{{0,{K8S_TASK_NAME_MAX_LENGTH - 1}}}$"
)
K8S_LITERAL_JSON_SCHEMA_PATTERN = (
    rf"^(?=[\s\S])(?![{UNICODE_EDGE_WHITESPACE_PATTERN}])"
    r"(?![\s\S]*(?:\$\{|\$\[))"
    rf"(?![\s\S]*[{UNICODE_EDGE_WHITESPACE_PATTERN}](?![\s\S]))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*(?![\s\S])"
)
K8S_NAMESPACE_JSON_SCHEMA_PATTERN = r"^(?=.{1,63}$)[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
K8S_ENVIRONMENT_NAME_JSON_SCHEMA_PATTERN = (
    r"^(?!taskInstanceId$)[-._A-Za-z][-._A-Za-z0-9]*$"
)
K8S_VALUE_JSON_SCHEMA_PATTERN = (
    r"^(?![\s\S]*(?:\$\{|\$\[))"
    r"[^\x00-\x1f\x7f-\x9f\ud800-\udfff]*(?![\s\S])"
)
_K8S_DNS_LABEL_PATTERN = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
K8S_DNS_SUBDOMAIN_JSON_SCHEMA_PATTERN = (
    rf"^(?=[a-z0-9.-]{{1,253}}$){_K8S_DNS_LABEL_PATTERN}"
    rf"(?:\.{_K8S_DNS_LABEL_PATTERN})*$"
)
_K8S_QUALIFIED_NAME_PART_PATTERN = r"[A-Za-z0-9](?:[-._A-Za-z0-9]{0,61}[A-Za-z0-9])?"
K8S_QUALIFIED_NAME_JSON_SCHEMA_PATTERN = (
    rf"^(?:(?=[a-z0-9.-]{{1,253}}/){_K8S_DNS_LABEL_PATTERN}"
    rf"(?:\.{_K8S_DNS_LABEL_PATTERN})*/)?"
    rf"{_K8S_QUALIFIED_NAME_PART_PATTERN}$"
)
K8S_CUSTOM_LABEL_KEY_JSON_SCHEMA_PATTERN = (
    r"^(?!(?:k8s\.cn/layer|k8s\.cn/name|dolphinscheduler-label)$)"
    rf"(?:(?=[a-z0-9.-]{{1,253}}/){_K8S_DNS_LABEL_PATTERN}"
    rf"(?:\.{_K8S_DNS_LABEL_PATTERN})*/)?"
    rf"{_K8S_QUALIFIED_NAME_PART_PATTERN}$"
)
K8S_LABEL_VALUE_JSON_SCHEMA_PATTERN = rf"^(?:{_K8S_QUALIFIED_NAME_PART_PATTERN})?$"
K8S_NODE_SELECTOR_LABEL_VALUE_JSON_SCHEMA_PATTERN = (
    rf"^{_K8S_QUALIFIED_NAME_PART_PATTERN}$"
)
K8S_NODE_SELECTOR_INTEGER_MAX = 9_223_372_036_854_775_807


def _unsigned_decimal_max_json_schema_pattern(maximum: int) -> str:
    """Build an ECMA-compatible decimal-string range through one maximum."""
    digits = str(maximum)
    alternatives = [rf"[0-9]{{1,{len(digits) - 1}}}"]
    for index, digit in enumerate(digits):
        upper = int(digit) - 1
        if upper < 0:
            continue
        prefix = digits[:index]
        suffix_length = len(digits) - index - 1
        suffix = rf"[0-9]{{{suffix_length}}}" if suffix_length else ""
        alternatives.append(rf"{prefix}[0-{upper}]{suffix}")
    alternatives.append(digits)
    return rf"^0*(?:{'|'.join(alternatives)})$"


K8S_NODE_SELECTOR_INTEGER_JSON_SCHEMA_PATTERN = (
    _unsigned_decimal_max_json_schema_pattern(K8S_NODE_SELECTOR_INTEGER_MAX)
)
_K8S_TASK_NAME_PATTERN = re.compile(K8S_TASK_NAME_JSON_SCHEMA_PATTERN)
_K8S_LITERAL_PATTERN = re.compile(K8S_LITERAL_JSON_SCHEMA_PATTERN)
_K8S_VALUE_PATTERN = re.compile(K8S_VALUE_JSON_SCHEMA_PATTERN)
_K8S_NAMESPACE_PATTERN = re.compile(K8S_NAMESPACE_JSON_SCHEMA_PATTERN)
_K8S_ENVIRONMENT_NAME_PATTERN = re.compile(K8S_ENVIRONMENT_NAME_JSON_SCHEMA_PATTERN)
_K8S_DNS_SUBDOMAIN_PATTERN = re.compile(K8S_DNS_SUBDOMAIN_JSON_SCHEMA_PATTERN)
_K8S_QUALIFIED_NAME_PATTERN = re.compile(K8S_QUALIFIED_NAME_JSON_SCHEMA_PATTERN)
_K8S_LABEL_VALUE_PATTERN = re.compile(K8S_LABEL_VALUE_JSON_SCHEMA_PATTERN)
_K8S_NODE_SELECTOR_LABEL_VALUE_PATTERN = re.compile(
    K8S_NODE_SELECTOR_LABEL_VALUE_JSON_SCHEMA_PATTERN
)
_K8S_NODE_SELECTOR_INTEGER_PATTERN = re.compile(
    K8S_NODE_SELECTOR_INTEGER_JSON_SCHEMA_PATTERN
)
_K8S_RESERVED_LABEL_KEYS = frozenset(
    {"k8s.cn/layer", "k8s.cn/name", "dolphinscheduler-label"}
)
_K8S_LOGGED_VALUE_DESCRIPTION = (
    "Kubernetes task parameters and prepared values may be logged or injected "
    "into the Pod environment. This field is not secret storage, and the CLI "
    "does not detect or redact secrets."
)


def validate_k8s_task_name(value: str) -> str:
    """Keep the authored task name safe for the derived Kubernetes identity."""
    if not _K8S_TASK_NAME_PATTERN.fullmatch(value):
        message = (
            f"K8S task name must match {K8S_TASK_NAME_JSON_SCHEMA_PATTERN} so "
            "the lowercase name plus '-' and a Java int taskInstanceId forms a "
            "valid Kubernetes runtime name"
        )
        raise ValueError(message)
    return value


def validate_k8s_literal(value: str, *, field: str) -> str:
    """Keep one Kubernetes authored value literal without rewriting it."""
    if not _K8S_LITERAL_PATTERN.fullmatch(value):
        message = (
            f"{field} must be one nonblank literal string without edge whitespace, "
            "controls, surrogates, or DS placeholders"
        )
        raise ValueError(message)
    return value


class K8sEnvironmentVariableSpec(YamlSpecModel):
    """One literal input value projected into native K8S localParams."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str = Field(
        min_length=1,
        description=_K8S_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_ENVIRONMENT_NAME_JSON_SCHEMA_PATTERN},
    )
    value: str = Field(
        description=_K8S_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_VALUE_JSON_SCHEMA_PATTERN},
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require one Kubernetes environment name and reserve taskInstanceId."""
        if not _K8S_ENVIRONMENT_NAME_PATTERN.fullmatch(value):
            message = (
                "environment names must be Kubernetes environment identifiers; "
                "taskInstanceId is reserved by the upstream executor"
            )
            raise ValueError(message)
        return value

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        """Keep one prepared-value input literal and safe for INFO logging."""
        if not _K8S_VALUE_PATTERN.fullmatch(value):
            message = (
                "environment values must not contain controls, surrogates, "
                "or DS placeholders"
            )
            raise ValueError(message)
        return value


class K8sLabelSpec(YamlSpecModel):
    """One deterministic Kubernetes label entry."""

    model_config = ConfigDict(extra="forbid", strict=True)

    label: str = Field(
        min_length=1,
        json_schema_extra={"pattern": K8S_CUSTOM_LABEL_KEY_JSON_SCHEMA_PATTERN},
    )
    value: str = Field(
        json_schema_extra={"pattern": K8S_LABEL_VALUE_JSON_SCHEMA_PATTERN}
    )

    @field_validator("label")
    @classmethod
    def validate_label(cls, value: str) -> str:
        """Keep user labels valid and away from executor-owned tracking keys."""
        if (
            not _K8S_QUALIFIED_NAME_PATTERN.fullmatch(value)
            or value in _K8S_RESERVED_LABEL_KEYS
        ):
            message = "customizedLabels contains an invalid or reserved label key"
            raise ValueError(message)
        return value

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        """Validate one Kubernetes label value without changing its spelling."""
        if not _K8S_LABEL_VALUE_PATTERN.fullmatch(value):
            message = "customizedLabels values must be Kubernetes label values"
            raise ValueError(message)
        return value


class K8sNodeSelectorSpec(YamlSpecModel):
    """One Kubernetes node-selector expression with exact operator cardinality."""

    model_config = ConfigDict(extra="forbid", strict=True)

    key: str = Field(
        min_length=1,
        json_schema_extra={"pattern": K8S_QUALIFIED_NAME_JSON_SCHEMA_PATTERN},
    )
    operator: Literal["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"]
    values: list[str]

    @field_validator("key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        """Require one Kubernetes qualified node label key."""
        if not _K8S_QUALIFIED_NAME_PATTERN.fullmatch(value):
            message = "nodeSelectors keys must be Kubernetes qualified names"
            raise ValueError(message)
        return value

    @field_validator("values", mode="before")
    @classmethod
    def validate_value_collection(cls, value: YamlValue) -> YamlValue:
        """Reject coercion and keep comma joining unambiguous."""
        if not isinstance(value, list):
            message = "nodeSelectors values must be a strict list"
            error_type = "k8s_node_selector_values_type"
            raise PydanticCustomError(error_type, message)
        for item in value:
            if (
                not isinstance(item, str)
                or item != item.strip()
                or "," in item
                or not _K8S_VALUE_PATTERN.fullmatch(item)
            ):
                message = (
                    "nodeSelectors values must be literal strings without "
                    "edge whitespace, commas, controls, or DS placeholders"
                )
                raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_operator_cardinality(self) -> K8sNodeSelectorSpec:
        """Match Kubernetes selector operator/value requirements exactly."""
        if self.operator in {"In", "NotIn"} and not self.values:
            message = f"{self.operator} requires at least one selector value"
            raise ValueError(message)
        if self.operator in {"In", "NotIn"} and (
            len(self.values) != len(set(self.values))
            or any(
                not _K8S_NODE_SELECTOR_LABEL_VALUE_PATTERN.fullmatch(value)
                for value in self.values
            )
        ):
            message = (
                f"{self.operator} values must be unique, nonempty Kubernetes "
                "label values"
            )
            raise ValueError(message)
        if self.operator in {"Exists", "DoesNotExist"} and self.values:
            message = f"{self.operator} requires an empty selector value list"
            raise ValueError(message)
        if self.operator in {"Gt", "Lt"} and (
            len(self.values) != 1
            or not _K8S_NODE_SELECTOR_INTEGER_PATTERN.fullmatch(self.values[0])
        ):
            message = f"{self.operator} requires exactly one decimal integer value"
            raise ValueError(message)
        return self


class K8sOutputSpec(YamlSpecModel):
    """One stdout-derived task output projected to native OUT localParams."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str = Field(
        min_length=1,
        description=(
            "Output declaration only; the container must print the exact marker "
            "documented for the selected version. Not secret storage."
        ),
        json_schema_extra={"pattern": K8S_ENVIRONMENT_NAME_JSON_SCHEMA_PATTERN},
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require one unambiguous downstream parameter name."""
        if not _K8S_ENVIRONMENT_NAME_PATTERN.fullmatch(value):
            message = "outputs names must be Kubernetes-style parameter names"
            raise ValueError(message)
        return value


class K8sLiteralContainerJobTaskParamsSpec(TaskParamsSpec):
    """Stable literal Kubernetes container-job intent across exact wire epochs."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    connection_mode: K8sConnectionMode = Field(alias="connectionMode")
    namespace: str | None = Field(
        default=None,
        min_length=1,
        description=_K8S_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_NAMESPACE_JSON_SCHEMA_PATTERN},
    )
    cluster: str | None = Field(
        default=None,
        min_length=1,
        description=_K8S_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_LITERAL_JSON_SCHEMA_PATTERN},
    )
    datasource: DatasourceReference | None = Field(
        default=None,
        description=(
            "Positive DolphinScheduler K8S datasource id or exact name; the "
            "datasource may contain kubeconfig credentials."
        ),
    )
    image: str = Field(
        min_length=1,
        description=_K8S_LOGGED_VALUE_DESCRIPTION,
        json_schema_extra={"pattern": K8S_LITERAL_JSON_SCHEMA_PATTERN},
    )
    min_cpu_cores: float = Field(default=0.0, alias="minCpuCores", ge=0)
    min_memory_space: float = Field(default=0.0, alias="minMemorySpace", ge=0)
    environment: list[K8sEnvironmentVariableSpec] = Field(default_factory=list)
    outputs: list[K8sOutputSpec] = Field(default_factory=list)
    command: list[str] = Field(default_factory=list)
    args: list[str] = Field(default_factory=list)
    pull_secret: str | None = Field(
        default=None,
        alias="pullSecret",
        json_schema_extra={"pattern": K8S_DNS_SUBDOMAIN_JSON_SCHEMA_PATTERN},
    )
    image_pull_policy: Literal["IfNotPresent", "Always", "Never"] = Field(
        default="IfNotPresent",
        alias="imagePullPolicy",
    )
    customized_labels: list[K8sLabelSpec] = Field(
        default_factory=list,
        alias="customizedLabels",
    )
    node_selectors: list[K8sNodeSelectorSpec] = Field(
        default_factory=list,
        alias="nodeSelectors",
    )

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep primitive Java defaults and the canonical environment explicit."""
        return ("min_cpu_cores", "min_memory_space", "environment")

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str | None) -> str | None:
        """Require one DNS-1123 namespace label when the field is active."""
        if value is not None and not _K8S_NAMESPACE_PATTERN.fullmatch(value):
            message = "namespace must be one Kubernetes DNS-1123 label"
            raise ValueError(message)
        return value

    @field_validator("cluster", "image")
    @classmethod
    def validate_literal_field(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        """Keep cluster and image values literal without normalizing spelling."""
        if value is None:
            return None
        field = "cluster" if info.field_name == "cluster" else "image"
        return validate_k8s_literal(value, field=field)

    @field_validator("command", "args", mode="before")
    @classmethod
    def validate_argv(cls, value: YamlValue, info: ValidationInfo) -> YamlValue:
        """Keep list-to-JSON-array projection exact, including empty arguments."""
        if not isinstance(value, list):
            message = f"{info.field_name} must be a strict list of strings"
            error_type = "k8s_argv_type"
            raise PydanticCustomError(error_type, message)
        for item in value:
            if not isinstance(item, str) or not _K8S_VALUE_PATTERN.fullmatch(item):
                message = (
                    f"{info.field_name} items must be literal strings without "
                    "controls, surrogates, or DS placeholders"
                )
                raise ValueError(message)
        return value

    @field_validator("pull_secret")
    @classmethod
    def validate_pull_secret(cls, value: str | None) -> str | None:
        """Accept one Kubernetes Secret object name, never secret content."""
        if value is not None and not _K8S_DNS_SUBDOMAIN_PATTERN.fullmatch(value):
            message = "pullSecret must be one Kubernetes DNS-subdomain object name"
            raise ValueError(message)
        return value

    @field_validator("min_cpu_cores", "min_memory_space", mode="before")
    @classmethod
    def validate_resource_quantity(cls, value: YamlValue) -> YamlValue:
        """Reject coercion, booleans, negatives, and non-finite quantities."""
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value < 0
        ):
            message = "K8S resource quantities must be finite non-negative numbers"
            raise ValueError(message)
        return float(value)

    @field_validator("environment")
    @classmethod
    def validate_unique_environment(
        cls,
        value: list[K8sEnvironmentVariableSpec],
    ) -> list[K8sEnvironmentVariableSpec]:
        """Prevent native prepared-map last-write-wins ambiguity."""
        names = [item.name for item in value]
        if len(names) != len(set(names)):
            message = "environment names must be unique"
            raise ValueError(message)
        return value

    @field_validator("outputs")
    @classmethod
    def validate_unique_outputs(cls, value: list[K8sOutputSpec]) -> list[K8sOutputSpec]:
        """Prevent var-pool last-write-wins ambiguity."""
        names = [item.name for item in value]
        if len(names) != len(set(names)):
            message = "outputs names must be unique"
            raise ValueError(message)
        return value

    @field_validator("customized_labels")
    @classmethod
    def validate_unique_labels(cls, value: list[K8sLabelSpec]) -> list[K8sLabelSpec]:
        """Prevent upstream label-map last-write-wins ambiguity."""
        labels = [item.label for item in value]
        if len(labels) != len(set(labels)):
            message = "customizedLabels label keys must be unique"
            raise ValueError(message)
        return value

    @field_validator("node_selectors")
    @classmethod
    def validate_unique_selectors(
        cls,
        value: list[K8sNodeSelectorSpec],
    ) -> list[K8sNodeSelectorSpec]:
        """Reject only exact duplicate expressions; repeated keys form AND."""
        expressions = [(item.key, item.operator, tuple(item.values)) for item in value]
        if len(expressions) != len(set(expressions)):
            message = "nodeSelectors expressions must not be exact duplicates"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def validate_connection_fields(self) -> K8sLiteralContainerJobTaskParamsSpec:
        """Require only the connection fields active for the canonical mode."""
        active_by_mode = {
            K8sConnectionMode.NAMESPACE: frozenset({"namespace", "cluster"}),
            K8sConnectionMode.DATASOURCE: frozenset({"datasource"}),
        }
        active_fields = active_by_mode[self.connection_mode]
        for field_name in ("namespace", "cluster", "datasource"):
            value = getattr(self, field_name)
            if field_name in active_fields and value is None:
                message = f"{field_name} is required for {self.connection_mode.value}"
                raise ValueError(message)
            if field_name not in active_fields and field_name in self.model_fields_set:
                message = f"{field_name} is inactive for {self.connection_mode.value}"
                raise ValueError(message)
        input_names = {item.name for item in self.environment}
        output_names = {item.name for item in self.outputs}
        duplicates = sorted(input_names.intersection(output_names))
        if duplicates:
            message = (
                "environment and outputs names must be disjoint: "
                f"{', '.join(duplicates)}"
            )
            raise ValueError(message)
        return self


class K8sAdvancedLiteralContainerJobTaskParamsSpec(
    K8sLiteralContainerJobTaskParamsSpec
):
    """K8S canonical intent with exact 3.2+ container option defaults."""

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Publish exact stable defaults present in the advanced wire epochs."""
        return (
            *super().default_payload_field_names(),
            "command",
            "args",
            "image_pull_policy",
            "customized_labels",
            "node_selectors",
        )


class K8sLegacyLiteralContainerJobTaskParamsSpec(K8sLiteralContainerJobTaskParamsSpec):
    """Exact 3.1.9 intent without later container or output fields."""

    @model_validator(mode="before")
    @classmethod
    def reject_later_epoch_fields(cls, value: YamlValue) -> YamlValue:
        """Require later fields to be absent, including explicit null values."""
        if not isinstance(value, dict):
            return value
        unsupported = sorted(
            {
                "outputs",
                "command",
                "args",
                "pullSecret",
                "pull_secret",
                "imagePullPolicy",
                "image_pull_policy",
                "customizedLabels",
                "customized_labels",
                "nodeSelectors",
                "node_selectors",
            }.intersection(value)
        )
        if unsupported:
            message = (
                "K8S 3.1.9 container/output fields must be absent: "
                f"{', '.join(unsupported)}"
            )
            raise ValueError(message)
        return value


class K8sOutputLiteralContainerJobTaskParamsSpec(
    K8sAdvancedLiteralContainerJobTaskParamsSpec
):
    """Advanced K8S intent on exact versions with working output transport."""

    def default_payload_field_names(self) -> tuple[str, ...]:
        """Keep the version-supported output declaration list explicit."""
        return (*super().default_payload_field_names(), "outputs")


class K8sLegacyOutputLiteralContainerJobTaskParamsSpec(
    K8sOutputLiteralContainerJobTaskParamsSpec
):
    """Exact 3.2.0 intent whose executor cannot mutate an empty label map."""

    customized_labels: list[K8sLabelSpec] = Field(
        alias="customizedLabels",
        min_length=1,
    )


def k8s_literal_container_job_params_model(
    *,
    advanced_container_fields: bool,
    empty_customized_labels_supported: bool,
    output_transport_supported: bool,
) -> type[K8sLiteralContainerJobTaskParamsSpec]:
    """Select one exact K8S canonical model from reviewed runtime facts."""
    if not advanced_container_fields:
        return K8sLegacyLiteralContainerJobTaskParamsSpec
    if not empty_customized_labels_supported:
        return K8sLegacyOutputLiteralContainerJobTaskParamsSpec
    if output_transport_supported:
        return K8sOutputLiteralContainerJobTaskParamsSpec
    return K8sAdvancedLiteralContainerJobTaskParamsSpec
