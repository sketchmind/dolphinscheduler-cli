from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from live_gate.conformance_image_provenance import validate_image_provenance
from live_gate.exact_profile_policy import (
    EXACT_PROFILE_GATE_POLICIES,
    ExactProfileGatePolicy,
    exact_profile_gate_policy,
)
from private_manifest_io import absolute_path as _absolute_path
from private_manifest_io import load_private_json_object as _load_private_json_object
from private_manifest_io import publish_private_json_pair as _publish_private_json_pair

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import NoReturn


_PROJECTION_PROVISIONER = "dsmatrix-exact-read-state-projection/v2"
_HMAC_SHA256 = re.compile(r"^hmac-sha256:[0-9a-f]{64}$")
_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_CAMPAIGN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class _FixtureValues:
    project_name: str
    project_code: int
    workflow_name: str
    workflow_code: int
    schedule_id: int


@dataclass(frozen=True)
class _StateValues:
    project_name: str
    project_code: int
    workflow_name: str
    workflow_code: int
    schedule_id: int
    task_name: str
    task_code: int


def project_exact_profile_matrix_fixture(
    *,
    ds_version: str,
    cluster_manifest: Path,
    fixture_manifest: Path,
    state_file: Path,
    image_inspection: Path,
    cluster_output: Path,
    fixture_output: Path,
) -> tuple[Path, Path]:
    """Project one validated matrix read fixture into private gate manifests."""
    policy = exact_profile_gate_policy(ds_version)
    cluster = _load_private_json_object(
        cluster_manifest,
        label="Generic cluster manifest",
    )
    fixture = _load_private_json_object(
        fixture_manifest,
        label="Generic fixture manifest",
    )
    state = _load_private_json_object(state_file, label="Matrix state file")
    inspection = _load_private_json_object(
        image_inspection,
        label="Image inspection",
    )
    _validate_cluster_shape(cluster, policy=policy)
    fixture_values = _validate_fixture_shape(fixture, policy=policy)
    state_values = _validate_ready_owned_state(state, policy=policy)
    image_digest = _validate_image_binding(
        cluster=cluster,
        fixture=fixture,
        inspection=inspection,
        policy=policy,
    )
    _validate_fixture_state_binding(
        fixture=fixture_values,
        state=state_values,
    )

    cluster_v2 = {
        "schema_version": 2,
        "ds_version": policy.ds_version,
        "image_tag": cluster["image_ref"],
        "image_digest": image_digest,
        "image_source": cluster["image_source"],
        "image_observed_at": cluster["image_observed_at"],
        "api_target_hmac_sha256": cluster["api_target_hmac_sha256"],
        "principal_hmac_sha256": cluster["principal_hmac_sha256"],
        "persona": cluster["persona"],
    }
    fixture_v2 = {
        "schema_version": 2,
        "ds_version": policy.ds_version,
        "image_tag": cluster["image_ref"],
        "image_digest": image_digest,
        "exclusive": True,
        "provisioner": _PROJECTION_PROVISIONER,
        "project": {
            "name": fixture_values.project_name,
            "code": fixture_values.project_code,
        },
        "workflow": {
            "name": fixture_values.workflow_name,
            "code": fixture_values.workflow_code,
            "scheduled": True,
            "schedule_id": fixture_values.schedule_id,
            "release_state": "OFFLINE",
            "editable_task": {
                "name": state_values.task_name,
                "code": state_values.task_code,
                "type": "SHELL",
            },
        },
    }
    return _publish_private_json_pair(
        cluster_v2,
        cluster_output,
        fixture_v2,
        fixture_output,
    )


def _validate_ready_owned_state(
    state: dict[str, object], *, policy: ExactProfileGatePolicy
) -> _StateValues:
    _require_exact_keys(
        state,
        {
            "campaign_id",
            "email",
            "names",
            "ownership_marker",
            "pending",
            "phase",
            "resources",
            "schema_version",
            "user_password",
            "version",
        },
        label="Matrix state",
    )
    if (
        not _is_schema_one(state.get("schema_version"))
        or state.get("version") != policy.ds_version
    ):
        message = f"Matrix state must declare schema 1 and DS {policy.ds_version}"
        raise ValueError(message)
    if state.get("phase") != "ready":
        message = "Matrix state phase must be ready"
        raise ValueError(message)
    if state.get("pending") is not None:
        message = "Matrix state pending must be null"
        raise ValueError(message)
    campaign_id = state.get("campaign_id")
    if not isinstance(campaign_id, str) or _CAMPAIGN_ID.fullmatch(campaign_id) is None:
        message = "Matrix state campaign identifier has an invalid format"
        raise ValueError(message)
    expected_marker = re.compile(
        rf"dsmatrix-exact-read/v1/{re.escape(policy.ds_version)}/{re.escape(campaign_id)}/[0-9a-f]{{12}}"
    )
    marker = state.get("ownership_marker")
    if not isinstance(marker, str) or expected_marker.fullmatch(marker) is None:
        message = "Matrix state ownership marker does not bind its campaign"
        raise ValueError(message)
    _require_text(state.get("email"), label="Matrix state email")
    _require_text(state.get("user_password"), label="Matrix state user password")
    names = _load_state_names(state.get("names"))
    return _load_state_resources(state.get("resources"), names=names)


def _validate_cluster_shape(
    cluster: dict[str, object], *, policy: ExactProfileGatePolicy
) -> None:
    _require_exact_keys(
        cluster,
        {
            "api_target_hmac_sha256",
            "ds_version",
            "image_id",
            "image_observed_at",
            "image_ref",
            "image_source",
            "persona",
            "principal_hmac_sha256",
            "schema_version",
        },
        label="Generic cluster manifest",
    )
    if (
        not _is_schema_one(cluster.get("schema_version"))
        or cluster.get("ds_version") != policy.ds_version
    ):
        message = (
            f"Generic cluster manifest must declare schema 1 and DS {policy.ds_version}"
        )
        raise ValueError(message)
    if cluster.get("persona") != "etl-developer":
        message = "Generic cluster persona must be etl-developer"
        raise ValueError(message)
    _require_pattern(
        cluster.get("api_target_hmac_sha256"),
        _HMAC_SHA256,
        label="Generic cluster API target HMAC",
    )
    _require_pattern(
        cluster.get("principal_hmac_sha256"),
        _HMAC_SHA256,
        label="Generic cluster principal HMAC",
    )
    _require_pattern(
        cluster.get("image_ref"),
        re.compile(rf"^(?P<repository>\S+):{re.escape(policy.ds_version)}$"),
        label=f"Generic cluster image reference for DS {policy.ds_version}",
    )
    _require_pattern(
        cluster.get("image_id"),
        _IMAGE_ID,
        label="Generic cluster image ID with sha256 identity",
    )
    _require_text(cluster.get("image_source"), label="Generic cluster image source")
    observed_at = _require_text(
        cluster.get("image_observed_at"),
        label="Generic cluster observation timestamp",
    )
    try:
        timestamp = datetime.fromisoformat(observed_at)
    except ValueError as error:
        message = "Generic cluster observation timestamp must be ISO-8601"
        raise ValueError(message) from error
    if timestamp.tzinfo is None:
        message = "Generic cluster observation timestamp must include a timezone"
        raise ValueError(message)


def _validate_fixture_shape(
    fixture: dict[str, object], *, policy: ExactProfileGatePolicy
) -> _FixtureValues:
    _require_exact_keys(
        fixture,
        {
            "ds_version",
            "image_id",
            "image_ref",
            "project",
            "provisioner",
            "schema_version",
            "workflow",
        },
        label="Generic fixture manifest",
    )
    if (
        not _is_schema_one(fixture.get("schema_version"))
        or fixture.get("ds_version") != policy.ds_version
    ):
        message = (
            f"Generic fixture manifest must declare schema 1 and DS {policy.ds_version}"
        )
        raise ValueError(message)
    if fixture.get("provisioner") != "dsmatrix-exact-read/v1":
        message = "Generic fixture provisioner must be dsmatrix-exact-read/v1"
        raise ValueError(message)
    project = _required_mapping(fixture.get("project"), label="Fixture project")
    workflow = _required_mapping(fixture.get("workflow"), label="Fixture workflow")
    _require_exact_keys(
        project,
        {"identity", "name"},
        label="Fixture project",
    )
    _require_exact_keys(
        workflow,
        {"identity", "name", "release_state", "schedule_id", "scheduled"},
        label="Fixture workflow",
    )
    if workflow.get("scheduled") is not True:
        message = "Generic fixture workflow must have an attached schedule"
        raise ValueError(message)
    if workflow.get("release_state") != "OFFLINE":
        message = "Generic fixture workflow must be OFFLINE"
        raise ValueError(message)
    return _FixtureValues(
        project_name=_require_text(project.get("name"), label="Fixture project name"),
        project_code=_load_code_identity(
            project.get("identity"),
            label="Fixture project identity",
        ),
        workflow_name=_require_text(
            workflow.get("name"),
            label="Fixture workflow name",
        ),
        workflow_code=_load_code_identity(
            workflow.get("identity"),
            label="Fixture workflow identity",
        ),
        schedule_id=_require_positive_int(
            workflow.get("schedule_id"),
            label="Fixture schedule ID",
        ),
    )


def _load_code_identity(value: object, *, label: str) -> int:
    identity = _required_mapping(value, label=label)
    _require_exact_keys(identity, {"kind", "value"}, label=label)
    if identity.get("kind") != "code":
        message = f"{label} kind must be code"
        raise ValueError(message)
    return _require_positive_int(identity.get("value"), label=f"{label} value")


def _load_state_names(value: object) -> dict[str, str]:
    names = _required_mapping(value, label="Matrix state names")
    _require_exact_keys(
        names,
        {"project", "task", "tenant", "user", "workflow"},
        label="Matrix state names",
    )
    return {
        field: _require_text(names.get(field), label=f"Matrix state {field} name")
        for field in ("project", "task", "tenant", "user", "workflow")
    }


def _load_state_resources(
    value: object,
    *,
    names: Mapping[str, str],
) -> _StateValues:
    resources = _required_mapping(value, label="Matrix state resources")
    _require_exact_keys(
        resources,
        {"project", "schedule", "tenant", "token", "user", "workflow"},
        label="Matrix state resources",
    )
    project = _state_resource(
        resources.get("project"),
        fields={"code", "id", "name", "user_id"},
        label="Matrix project resource",
    )
    workflow = _state_resource(
        resources.get("workflow"),
        fields={"code", "id", "name", "release_state", "task_code"},
        label="Matrix workflow resource",
    )
    schedule = _state_resource(
        resources.get("schedule"),
        fields={"id", "release_state", "workflow_code"},
        label="Matrix schedule resource",
    )
    tenant = _state_resource(
        resources.get("tenant"),
        fields={"id", "name"},
        label="Matrix tenant resource",
    )
    token = _state_resource(
        resources.get("token"),
        fields={"id", "user_id"},
        label="Matrix token resource",
    )
    user = _state_resource(
        resources.get("user"),
        fields={"id", "name", "tenant_id"},
        label="Matrix user resource",
    )
    return _validated_state_values(
        names=names,
        project=project,
        workflow=workflow,
        schedule=schedule,
        tenant=tenant,
        token=token,
        user=user,
    )


def _state_resource(
    value: object,
    *,
    fields: set[str],
    label: str,
) -> dict[str, object]:
    resource = _required_mapping(value, label=label)
    _require_exact_keys(resource, fields, label=label)
    return resource


def _validated_state_values(
    *,
    names: Mapping[str, str],
    project: Mapping[str, object],
    workflow: Mapping[str, object],
    schedule: Mapping[str, object],
    tenant: Mapping[str, object],
    token: Mapping[str, object],
    user: Mapping[str, object],
) -> _StateValues:
    project_code = _require_positive_int(
        project.get("code"),
        label="Matrix project code",
    )
    workflow_code = _require_positive_int(
        workflow.get("code"),
        label="Matrix workflow code",
    )
    schedule_id = _require_positive_int(
        schedule.get("id"),
        label="Matrix schedule ID",
    )
    _validate_state_relationships(
        names=names,
        project=project,
        workflow=workflow,
        schedule=schedule,
        tenant=tenant,
        token=token,
        user=user,
        workflow_code=workflow_code,
    )
    return _StateValues(
        project_name=names["project"],
        project_code=project_code,
        workflow_name=names["workflow"],
        workflow_code=workflow_code,
        schedule_id=schedule_id,
        task_name=names["task"],
        task_code=_require_positive_int(
            workflow.get("task_code"),
            label="Matrix task code",
        ),
    )


def _validate_state_relationships(
    *,
    names: Mapping[str, str],
    project: Mapping[str, object],
    workflow: Mapping[str, object],
    schedule: Mapping[str, object],
    tenant: Mapping[str, object],
    token: Mapping[str, object],
    user: Mapping[str, object],
    workflow_code: int,
) -> None:
    if project.get("name") != names["project"]:
        _fail("Matrix project name is inconsistent")
    if workflow.get("name") != names["workflow"]:
        _fail("Matrix workflow name is inconsistent")
    if tenant.get("name") != names["tenant"]:
        _fail("Matrix tenant name is inconsistent")
    if user.get("name") != names["user"]:
        _fail("Matrix user name is inconsistent")
    if workflow.get("release_state") != "OFFLINE":
        _fail("Matrix workflow must be OFFLINE")
    if schedule.get("release_state") != "OFFLINE":
        _fail("Matrix schedule must be OFFLINE")
    if schedule.get("workflow_code") != workflow_code:
        _fail("Matrix schedule workflow code is inconsistent")
    user_id = _require_positive_int(user.get("id"), label="Matrix user ID")
    tenant_id = _require_positive_int(tenant.get("id"), label="Matrix tenant ID")
    if project.get("user_id") != user_id or token.get("user_id") != user_id:
        _fail("Matrix user resource relationships are inconsistent")
    if user.get("tenant_id") != tenant_id:
        _fail("Matrix tenant resource relationship is inconsistent")
    _require_positive_int(project.get("id"), label="Matrix project ID")
    _require_positive_int(workflow.get("id"), label="Matrix workflow ID")
    _require_positive_int(token.get("id"), label="Matrix token ID")


def _validate_fixture_state_binding(
    *,
    fixture: _FixtureValues,
    state: _StateValues,
) -> None:
    if fixture.project_name != state.project_name:
        _fail("Fixture and matrix state project name differ")
    if fixture.project_code != state.project_code:
        _fail("Fixture and matrix state project code differ")
    if fixture.workflow_name != state.workflow_name:
        _fail("Fixture and matrix state workflow name differ")
    if fixture.workflow_code != state.workflow_code:
        _fail("Fixture and matrix state workflow code differ")
    if fixture.schedule_id != state.schedule_id:
        _fail("Fixture and matrix state schedule ID differ")


def _validate_image_binding(
    *,
    policy: ExactProfileGatePolicy,
    cluster: dict[str, object],
    fixture: dict[str, object],
    inspection: dict[str, object],
) -> str:
    if fixture.get("image_ref") != cluster.get("image_ref") or fixture.get(
        "image_id"
    ) != cluster.get("image_id"):
        message = (
            "Generic cluster and fixture manifests must identify the same image "
            "reference and ID"
        )
        raise ValueError(message)
    schema_version = inspection.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version not in {1, 2}
    ):
        message = "Image inspection must declare schema 1 or 2"
        raise ValueError(message)
    inspection_fields = {
        "image_id",
        "image_ref",
        "repo_digests",
        "schema_version",
        "selected_repo_digest",
    }
    if schema_version == 2:
        inspection_fields.add("kind")
    _require_exact_keys(inspection, inspection_fields, label="Image inspection")
    image_ref = _require_pattern(
        inspection.get("image_ref"),
        re.compile(rf"^(?P<repository>\S+):{re.escape(policy.ds_version)}$"),
        label=f"Image inspection reference for DS {policy.ds_version}",
    )
    image_id = _require_pattern(
        inspection.get("image_id"),
        _IMAGE_ID,
        label="Image inspection ID with sha256 identity",
    )
    if image_ref != cluster.get("image_ref") or image_id != cluster.get("image_id"):
        message = "Image inspection does not match the cluster image reference and ID"
        raise ValueError(message)
    provenance = {
        key: value
        for key, value in inspection.items()
        if key not in {"image_id", "image_ref", "schema_version"}
    }
    provenance.setdefault("kind", "registry-digest/v1")
    validated = validate_image_provenance(
        provenance,
        image_ref=image_ref,
        image_id=image_id,
        ds_version=policy.ds_version,
        label="Image inspection provenance",
    )
    return cast("str", validated["selected_repo_digest"])


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    label: str,
) -> None:
    if set(value) == expected:
        return
    message = f"{label} has invalid fields"
    raise ValueError(message)


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _required_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _require_positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} must be a positive integer"
        raise TypeError(message)
    return value


def _is_schema_one(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == 1


def _require_pattern(value: object, pattern: re.Pattern[str], *, label: str) -> str:
    text = _require_text(value, label=label)
    if pattern.fullmatch(text) is None:
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text


def _fail(message: str) -> NoReturn:
    raise ValueError(message)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Project an existing exact DolphinScheduler matrix read fixture "
            "into private mutating-gate manifests."
        )
    )
    parser.add_argument(
        "--version", required=True, choices=tuple(EXACT_PROFILE_GATE_POLICIES)
    )
    parser.add_argument("--cluster-manifest", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--image-inspection", type=Path, required=True)
    parser.add_argument("--cluster-output", type=Path, required=True)
    parser.add_argument("--fixture-output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the private matrix-fixture projection."""
    args = _parse_args(argv)
    try:
        project_exact_profile_matrix_fixture(
            ds_version=args.version,
            cluster_manifest=_absolute_path(args.cluster_manifest),
            fixture_manifest=_absolute_path(args.fixture_manifest),
            state_file=_absolute_path(args.state_file),
            image_inspection=_absolute_path(args.image_inspection),
            cluster_output=_absolute_path(args.cluster_output),
            fixture_output=_absolute_path(args.fixture_output),
        )
    except (RuntimeError, TypeError, ValueError) as error:
        print(f"Projection failed: {error}", file=sys.stderr)
        return 2
    except OSError:
        print(
            "Projection failed during private file access or publication.",
            file=sys.stderr,
        )
        return 2
    print(f"Projected private exact {args.version} gate manifests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
