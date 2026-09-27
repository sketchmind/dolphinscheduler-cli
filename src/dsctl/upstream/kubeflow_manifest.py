from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode
from yaml.tokens import AliasToken, AnchorToken

from dsctl.models.task_spec import (
    KUBEFLOW_TFJOB_NAME_PREFIX_MAX_LENGTH,
    KUBEFLOW_WORKFLOW_INSTANCE_PLACEHOLDER,
    contains_ds_parameter_placeholder,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_MAPPING_TAG = "tag:yaml.org,2002:map"
_SEQUENCE_TAG = "tag:yaml.org,2002:seq"
_JSON_SCALAR_TAGS = frozenset(
    {
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:float",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:null",
        "tag:yaml.org,2002:str",
    }
)
_STRING_TAG = "tag:yaml.org,2002:str"
_TFJOB_NAME_PATTERN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,50}[a-z0-9])?-"
    + re.escape(KUBEFLOW_WORKFLOW_INSTANCE_PLACEHOLDER)
)
_SERVER_OWNED_METADATA_FIELDS = frozenset(
    {
        "creationTimestamp",
        "deletionGracePeriodSeconds",
        "deletionTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "selfLink",
        "uid",
    }
)


@dataclass(frozen=True)
class KubeflowTfjobIdentityTemplate:
    """One exact TFJob resource identity before workflow-instance expansion."""

    cluster: str
    namespace: str
    name_template: str


def _mapping(node: Node, *, field: str) -> dict[str, Node]:
    """Return one string-keyed YAML mapping already checked for duplicates."""
    if not isinstance(node, MappingNode) or node.tag != _MAPPING_TAG:
        message = f"KUBEFLOW {field} must be one YAML mapping"
        raise ValueError(message)
    projected: dict[str, Node] = {}
    for key_node, value_node in node.value:
        if not isinstance(key_node, ScalarNode) or key_node.tag != _STRING_TAG:
            message = f"KUBEFLOW {field} keys must be literal strings"
            raise ValueError(message)
        key = key_node.value
        if key in projected:
            message = f"KUBEFLOW YAML contains duplicate key {key!r}"
            raise ValueError(message)
        projected[key] = value_node
    return projected


def _validate_nodes(root: Node) -> None:
    """Reject aliases, non-JSON tags, complex keys, and duplicate mapping keys."""
    pending = [root]
    visited: set[int] = set()
    while pending:
        node = pending.pop()
        node_identity = id(node)
        if node_identity in visited:
            message = "KUBEFLOW YAML aliases and recursive references are unsupported"
            raise ValueError(message)
        visited.add(node_identity)
        if isinstance(node, MappingNode):
            if node.tag != _MAPPING_TAG:
                message = "KUBEFLOW YAML non-JSON mapping tags are unsupported"
                raise ValueError(message)
            mapping = _mapping(node, field="mapping")
            pending.extend(mapping.values())
            pending.extend(key for key, _ in node.value)
            continue
        if isinstance(node, SequenceNode):
            if node.tag != _SEQUENCE_TAG:
                message = "KUBEFLOW YAML non-JSON sequence tags are unsupported"
                raise ValueError(message)
            pending.extend(node.value)
            continue
        if not isinstance(node, ScalarNode) or node.tag not in _JSON_SCALAR_TAGS:
            message = "KUBEFLOW YAML permits only JSON-core scalar tags"
            raise ValueError(message)


def _scalar(mapping: Mapping[str, Node], key: str, *, field: str) -> str:
    """Read one required literal string from a reviewed manifest mapping."""
    node = mapping.get(key)
    if not isinstance(node, ScalarNode) or node.tag != _STRING_TAG:
        message = f"KUBEFLOW {field}.{key} must be one literal string"
        raise ValueError(message)
    return cast("str", node.value)


def _validate_yaml_text(value: str) -> None:
    """Reject nonportable bytes and every placeholder outside task identity."""
    if (
        not value.strip()
        or not value.isascii()
        or "\r" in value
        or any(ord(character) < 32 and character != "\n" for character in value)
        or "\x7f" in value
    ):
        message = "yamlContent must be nonblank ASCII LF text without controls or CR"
        raise ValueError(message)
    if value.count(KUBEFLOW_WORKFLOW_INSTANCE_PLACEHOLDER) != 1:
        message = (
            "yamlContent must contain exactly one ${system.workflow.instance.id} "
            "placeholder in metadata.name"
        )
        raise ValueError(message)
    remaining = value.replace(KUBEFLOW_WORKFLOW_INSTANCE_PLACEHOLDER, "", 1)
    if contains_ds_parameter_placeholder(remaining):
        message = "yamlContent contains an unsupported DolphinScheduler placeholder"
        raise ValueError(message)


def _compose_yaml(value: str) -> Node:
    """Compose exactly one safe YAML document while preserving source spelling."""
    try:
        if any(
            isinstance(token, (AliasToken, AnchorToken))
            for token in yaml.scan(value, Loader=yaml.SafeLoader)
        ):
            message = "KUBEFLOW YAML anchors and aliases are unsupported"
            raise ValueError(message)
        documents = list(yaml.compose_all(value, Loader=yaml.SafeLoader))
    except (RecursionError, yaml.YAMLError) as exc:
        message = "yamlContent must be valid, safely bounded YAML"
        raise ValueError(message) from exc
    if len(documents) != 1 or documents[0] is None:
        message = "yamlContent must contain exactly one nonempty YAML document"
        raise ValueError(message)
    root = cast("Node", documents[0])
    _validate_nodes(root)
    return root


def _validate_metadata(
    manifest: Mapping[str, Node],
    *,
    namespace: str,
) -> str:
    """Validate and return the workflow-instance-scoped TFJob name template."""
    metadata_node = manifest.get("metadata")
    if metadata_node is None:
        message = "KUBEFLOW yamlContent requires metadata"
        raise ValueError(message)
    metadata = _mapping(metadata_node, field="metadata")
    if "generateName" in metadata:
        message = "KUBEFLOW metadata.generateName is outside the reviewed identity"
        raise ValueError(message)
    server_owned_metadata = sorted(_SERVER_OWNED_METADATA_FIELDS.intersection(metadata))
    if server_owned_metadata:
        message = (
            "KUBEFLOW typed authoring excludes server-owned metadata fields: "
            + ", ".join(server_owned_metadata)
        )
        raise ValueError(message)
    name = _scalar(metadata, "name", field="metadata")
    if not _TFJOB_NAME_PATTERN.fullmatch(name):
        message = (
            "KUBEFLOW metadata.name must be a DNS-safe prefix of at most "
            f"{KUBEFLOW_TFJOB_NAME_PREFIX_MAX_LENGTH} characters followed by "
            "-${system.workflow.instance.id}"
        )
        raise ValueError(message)
    manifest_namespace = _scalar(metadata, "namespace", field="metadata")
    if manifest_namespace != namespace:
        message = "KUBEFLOW metadata.namespace must exactly match task_params.namespace"
        raise ValueError(message)
    return name


def _validate_spec(manifest: Mapping[str, Node]) -> None:
    """Require the minimal runnable TFJob payload and exclude server state."""
    if "status" in manifest:
        message = "KUBEFLOW typed authoring excludes server-owned root status"
        raise ValueError(message)
    spec_node = manifest.get("spec")
    if spec_node is None:
        message = "KUBEFLOW yamlContent requires a spec mapping"
        raise ValueError(message)
    spec = _mapping(spec_node, field="spec")
    replica_specs_node = spec.get("tfReplicaSpecs")
    if replica_specs_node is None:
        message = "KUBEFLOW spec requires a nonempty tfReplicaSpecs mapping"
        raise ValueError(message)
    replica_specs = _mapping(
        replica_specs_node,
        field="spec.tfReplicaSpecs",
    )
    if not replica_specs:
        message = "KUBEFLOW spec.tfReplicaSpecs must not be empty"
        raise ValueError(message)


def kubeflow_tfjob_identity_template(
    value: str,
    *,
    namespace: str,
    cluster: str,
) -> KubeflowTfjobIdentityTemplate:
    """Validate one reviewed manifest and return its target identity template."""
    _validate_yaml_text(value)
    root = _compose_yaml(value)
    manifest = _mapping(root, field="document root")
    if _scalar(manifest, "apiVersion", field="root") != "kubeflow.org/v1":
        message = "KUBEFLOW typed authoring requires apiVersion kubeflow.org/v1"
        raise ValueError(message)
    if _scalar(manifest, "kind", field="root") != "TFJob":
        message = "KUBEFLOW typed authoring requires kind TFJob"
        raise ValueError(message)
    name = _validate_metadata(manifest, namespace=namespace)
    _validate_spec(manifest)
    return KubeflowTfjobIdentityTemplate(
        cluster=cluster,
        namespace=namespace,
        name_template=name,
    )


__all__ = ["KubeflowTfjobIdentityTemplate", "kubeflow_tfjob_identity_template"]
