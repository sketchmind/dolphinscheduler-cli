from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from live_gate.conformance_image_provenance import (
    validate_image_provenance,
    validate_image_reference,
)
from private_manifest_io import (
    absolute_path,
    load_private_json_object,
    publish_private_json_pair,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


IdentityKind = Literal["id", "code"]
_MATRIX_IMAGE_SOURCE = "swarm task plus node-local Docker inspection"
_OUTPUT_IMAGE_SOURCE = "node-local-inspection"
_INPUT_PROVISIONER = "dsmatrix-exact-read/v1"
_OUTPUT_PROVISIONER = "dsmatrix-conformance-fixture/v1"
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HMAC_SHA256 = re.compile(r"hmac-sha256:[0-9a-f]{64}\Z")
_CAMPAIGN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_MAX_OBSERVATION_AGE = timedelta(minutes=15)
_MAX_CLOCK_SKEW = timedelta(minutes=2)


@dataclass(frozen=True)
class _NativeIdentity:
    kind: IdentityKind
    value: int


@dataclass(frozen=True)
class _FixtureValues:
    project_name: str
    project_identity: _NativeIdentity
    workflow_name: str
    workflow_identity: _NativeIdentity
    workflow_release_state: str
    schedule_id: int


@dataclass(frozen=True)
class _StateValues:
    project_name: str
    project_identity_value: int
    workflow_name: str
    workflow_identity_value: int
    workflow_release_state: str
    schedule_id: int


def project_conformance_matrix_fixture(
    *,
    ds_version: str,
    cluster_manifest: Path,
    fixture_manifest: Path,
    state_file: Path,
    image_inspection: Path,
    cluster_output: Path,
    fixture_output: Path,
) -> tuple[Path, Path]:
    """Project one validated matrix fixture into conformance-loader manifests."""
    if ds_version not in TARGET_DS_VERSIONS:
        message = "Selected DS version is not a reviewed exact profile"
        raise ValueError(message)
    cluster = load_private_json_object(
        cluster_manifest,
        label="Generic cluster manifest",
    )
    fixture = load_private_json_object(
        fixture_manifest,
        label="Generic fixture manifest",
    )
    state = load_private_json_object(state_file, label="Matrix state file")
    inspection = load_private_json_object(
        image_inspection,
        label="Image inspection",
    )
    _validate_cluster(cluster, ds_version=ds_version)
    fixture_values = _validate_fixture(fixture, ds_version=ds_version)
    _validate_state(state, fixture=fixture_values, ds_version=ds_version)
    image_provenance = _validate_image_binding(
        cluster=cluster,
        fixture=fixture,
        inspection=inspection,
        ds_version=ds_version,
    )
    cluster_projection = {
        "schema_version": 2,
        "ds_version": ds_version,
        "image_ref": cluster["image_ref"],
        "image_id": cluster["image_id"],
        "image_source": _OUTPUT_IMAGE_SOURCE,
        "image_provenance": image_provenance,
        "image_observed_at": cluster["image_observed_at"],
        "api_target_hmac_sha256": cluster["api_target_hmac_sha256"],
        "principal_hmac_sha256": cluster["principal_hmac_sha256"],
        "persona": cluster["persona"],
    }
    fixture_projection = {
        "schema_version": 1,
        "ds_version": ds_version,
        "image_ref": cluster["image_ref"],
        "image_id": cluster["image_id"],
        "provisioner": _OUTPUT_PROVISIONER,
        "project": {
            "name": fixture_values.project_name,
            "identity": {
                "kind": fixture_values.project_identity.kind,
                "value": fixture_values.project_identity.value,
            },
        },
        "workflow": {
            "name": fixture_values.workflow_name,
            "identity": {
                "kind": fixture_values.workflow_identity.kind,
                "value": fixture_values.workflow_identity.value,
            },
            "scheduled": True,
            "schedule_id": fixture_values.schedule_id,
            "release_state": fixture_values.workflow_release_state,
        },
    }
    return publish_private_json_pair(
        cluster_projection,
        cluster_output,
        fixture_projection,
        fixture_output,
    )


def _validate_cluster(cluster: Mapping[str, object], *, ds_version: str) -> None:
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
    _require_schema_and_version(cluster, ds_version=ds_version, label="cluster")
    if cluster.get("image_source") != _MATRIX_IMAGE_SOURCE:
        message = "Generic cluster image_source is not the canonical matrix value"
        raise ValueError(message)
    if cluster.get("persona") != "etl-developer":
        message = "Generic cluster persona must be etl-developer"
        raise ValueError(message)
    _require_digest(
        cluster.get("api_target_hmac_sha256"),
        _HMAC_SHA256,
        label="Generic cluster API target HMAC",
    )
    _require_digest(
        cluster.get("principal_hmac_sha256"),
        _HMAC_SHA256,
        label="Generic cluster principal HMAC",
    )
    _require_digest(
        cluster.get("image_id"),
        _SHA256,
        label="Generic cluster image ID",
    )
    _validate_image_ref(cluster.get("image_ref"), ds_version=ds_version)
    observed_at = _require_text(
        cluster.get("image_observed_at"),
        label="Generic cluster image observation",
    )
    try:
        observed = datetime.fromisoformat(observed_at)
    except ValueError as error:
        message = "Generic cluster image observation must be ISO-8601"
        raise ValueError(message) from error
    if observed.tzinfo is None:
        message = "Generic cluster image observation must include a timezone"
        raise ValueError(message)
    now = datetime.now(tz=UTC)
    observed = observed.astimezone(UTC)
    if observed > now + _MAX_CLOCK_SKEW or now - observed > _MAX_OBSERVATION_AGE:
        message = "Generic cluster image observation is outside the live gate window"
        raise ValueError(message)


def _validate_fixture(
    fixture: Mapping[str, object],
    *,
    ds_version: str,
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
    _require_schema_and_version(fixture, ds_version=ds_version, label="fixture")
    if fixture.get("provisioner") != _INPUT_PROVISIONER:
        message = f"Generic fixture provisioner must be {_INPUT_PROVISIONER}"
        raise ValueError(message)
    project = _require_mapping(fixture.get("project"), label="Fixture project")
    workflow = _require_mapping(fixture.get("workflow"), label="Fixture workflow")
    _require_exact_keys(project, {"identity", "name"}, label="Fixture project")
    _require_exact_keys(
        workflow,
        {"identity", "name", "release_state", "schedule_id", "scheduled"},
        label="Fixture workflow",
    )
    if workflow.get("scheduled") is not True:
        message = "Generic fixture workflow must have an attached schedule"
        raise ValueError(message)
    release_state = _require_text(
        workflow.get("release_state"),
        label="Fixture workflow release state",
    )
    if release_state not in {"ONLINE", "OFFLINE"}:
        message = "Fixture workflow release state must be ONLINE or OFFLINE"
        raise ValueError(message)
    identity_kind: IdentityKind = "id" if ds_version == "1.3.9" else "code"
    return _FixtureValues(
        project_name=_require_non_numeric_name(
            project.get("name"),
            label="Fixture project name",
        ),
        project_identity=_load_identity(
            project.get("identity"),
            expected_kind=identity_kind,
            label="Fixture project identity",
        ),
        workflow_name=_require_non_numeric_name(
            workflow.get("name"),
            label="Fixture workflow name",
        ),
        workflow_identity=_load_identity(
            workflow.get("identity"),
            expected_kind=identity_kind,
            label="Fixture workflow identity",
        ),
        workflow_release_state=release_state,
        schedule_id=_require_positive_int(
            workflow.get("schedule_id"),
            label="Fixture schedule ID",
        ),
    )


def _validate_state(
    state: Mapping[str, object],
    *,
    fixture: _FixtureValues,
    ds_version: str,
) -> None:
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
        or state.get("version") != ds_version
    ):
        message = f"Matrix state must declare schema 1 and DS {ds_version}"
        raise ValueError(message)
    if state.get("phase") != "ready" or state.get("pending") is not None:
        message = "Matrix state must be ready with pending null"
        raise ValueError(message)
    campaign = _require_text(state.get("campaign_id"), label="Matrix campaign")
    if _CAMPAIGN_ID.fullmatch(campaign) is None:
        message = "Matrix campaign identifier has an invalid format"
        raise ValueError(message)
    _require_text(state.get("email"), label="Matrix state email")
    _require_text(state.get("user_password"), label="Matrix state user password")
    marker = _require_text(
        state.get("ownership_marker"),
        label="Matrix ownership marker",
    )
    expected_marker = re.compile(
        rf"dsmatrix-exact-read/v1/{re.escape(ds_version)}/"
        rf"{re.escape(campaign)}/[0-9a-f]{{12}}\Z"
    )
    if expected_marker.fullmatch(marker) is None:
        message = "Matrix state ownership marker does not bind its campaign"
        raise ValueError(message)
    names = _load_state_names(state.get("names"))
    state_values = _load_state_resources(
        state.get("resources"),
        names=names,
        ds_version=ds_version,
    )
    _validate_fixture_state_binding(fixture=fixture, state=state_values)


def _load_state_names(value: object) -> dict[str, str]:
    names = _require_mapping(value, label="Matrix state names")
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
    ds_version: str,
) -> _StateValues:
    resources = _require_mapping(value, label="Matrix state resources")
    _require_exact_keys(
        resources,
        {"project", "schedule", "tenant", "token", "user", "workflow"},
        label="Matrix state resources",
    )
    if ds_version == "1.3.9":
        project_fields = {"id", "name", "user_id"}
        workflow_fields = {
            "id",
            "name",
            "project_id",
            "release_state",
            "task_id",
        }
        schedule_fields = {"id", "release_state", "workflow_id"}
    else:
        project_fields = {"code", "id", "name", "user_id"}
        workflow_fields = {"code", "id", "name", "release_state", "task_code"}
        schedule_fields = {"id", "release_state", "workflow_code"}
    project = _state_resource(
        resources.get("project"),
        fields=project_fields,
        label="Matrix project",
    )
    workflow = _state_resource(
        resources.get("workflow"),
        fields=workflow_fields,
        label="Matrix workflow",
    )
    schedule = _state_resource(
        resources.get("schedule"),
        fields=schedule_fields,
        label="Matrix schedule",
    )
    tenant = _state_resource(
        resources.get("tenant"),
        fields={"id", "name"},
        label="Matrix tenant",
    )
    token = _state_resource(
        resources.get("token"),
        fields={"id", "user_id"},
        label="Matrix token",
    )
    user = _state_resource(
        resources.get("user"),
        fields={"id", "name", "tenant_id"},
        label="Matrix user",
    )
    return _validated_state_values(
        names=names,
        project=project,
        workflow=workflow,
        schedule=schedule,
        tenant=tenant,
        token=token,
        user=user,
        ds_version=ds_version,
    )


def _state_resource(
    value: object,
    *,
    fields: set[str],
    label: str,
) -> dict[str, object]:
    resource = _require_mapping(value, label=label)
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
    ds_version: str,
) -> _StateValues:
    project_id = _require_positive_int(project.get("id"), label="Matrix project ID")
    workflow_id = _require_positive_int(
        workflow.get("id"),
        label="Matrix workflow ID",
    )
    if ds_version == "1.3.9":
        project_identity_value = project_id
        workflow_identity_value = workflow_id
        _require_text(workflow.get("task_id"), label="Matrix task identifier")
    else:
        project_identity_value = _require_positive_int(
            project.get("code"),
            label="Matrix project code",
        )
        workflow_identity_value = _require_positive_int(
            workflow.get("code"),
            label="Matrix workflow code",
        )
        _require_positive_int(workflow.get("task_code"), label="Matrix task code")
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
        ds_version=ds_version,
        project_identity_value=project_identity_value,
        workflow_identity_value=workflow_identity_value,
    )
    workflow_release_state = _require_release_state(
        workflow.get("release_state"),
        label="Matrix workflow release state",
    )
    _require_release_state(
        schedule.get("release_state"),
        label="Matrix schedule release state",
    )
    return _StateValues(
        project_name=names["project"],
        project_identity_value=project_identity_value,
        workflow_name=names["workflow"],
        workflow_identity_value=workflow_identity_value,
        workflow_release_state=workflow_release_state,
        schedule_id=schedule_id,
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
    ds_version: str,
    project_identity_value: int,
    workflow_identity_value: int,
) -> None:
    if project.get("name") != names.get("project"):
        message = "Matrix project resource name is inconsistent"
        raise ValueError(message)
    if workflow.get("name") != names.get("workflow"):
        message = "Matrix workflow resource name is inconsistent"
        raise ValueError(message)
    user_id = _require_positive_int(user.get("id"), label="Matrix user ID")
    tenant_id = _require_positive_int(tenant.get("id"), label="Matrix tenant ID")
    _require_positive_int(token.get("id"), label="Matrix token ID")
    project_user_id = _require_positive_int(
        project.get("user_id"),
        label="Matrix project user ID",
    )
    token_user_id = _require_positive_int(
        token.get("user_id"),
        label="Matrix token user ID",
    )
    if project_user_id != user_id or token_user_id != user_id:
        message = "Matrix user resource relationships are inconsistent"
        raise ValueError(message)
    user_tenant_id = _require_positive_int(
        user.get("tenant_id"),
        label="Matrix user tenant ID",
    )
    if user_tenant_id != tenant_id:
        message = "Matrix tenant resource relationship is inconsistent"
        raise ValueError(message)
    if tenant.get("name") != names.get("tenant"):
        message = "Matrix tenant resource name is inconsistent"
        raise ValueError(message)
    if user.get("name") != names.get("user"):
        message = "Matrix user resource name is inconsistent"
        raise ValueError(message)
    if ds_version == "1.3.9":
        workflow_project_id = _require_positive_int(
            workflow.get("project_id"),
            label="Matrix workflow project ID",
        )
        if workflow_project_id != project_identity_value:
            message = "Matrix workflow project relationship is inconsistent"
            raise ValueError(message)
        workflow_reference = _require_positive_int(
            schedule.get("workflow_id"),
            label="Matrix schedule workflow ID",
        )
    else:
        workflow_reference = _require_positive_int(
            schedule.get("workflow_code"),
            label="Matrix schedule workflow code",
        )
    if workflow_reference != workflow_identity_value:
        message = "Matrix schedule workflow relationship is inconsistent"
        raise ValueError(message)


def _validate_fixture_state_binding(
    *,
    fixture: _FixtureValues,
    state: _StateValues,
) -> None:
    if state.project_name != fixture.project_name:
        message = "Matrix and fixture project names differ"
        raise ValueError(message)
    if state.workflow_name != fixture.workflow_name:
        message = "Matrix and fixture workflow names differ"
        raise ValueError(message)
    if state.project_identity_value != fixture.project_identity.value:
        message = "Matrix and fixture project identities differ"
        raise ValueError(message)
    if state.workflow_identity_value != fixture.workflow_identity.value:
        message = "Matrix and fixture workflow identities differ"
        raise ValueError(message)
    if state.workflow_release_state != fixture.workflow_release_state:
        message = "Matrix workflow resource differs from its fixture"
        raise ValueError(message)
    if state.schedule_id != fixture.schedule_id:
        message = "Matrix schedule resource differs from its fixture"
        raise ValueError(message)


def _require_release_state(value: object, *, label: str) -> str:
    release_state = _require_text(value, label=label)
    if release_state not in {"ONLINE", "OFFLINE"}:
        message = f"{label} must be ONLINE or OFFLINE"
        raise ValueError(message)
    return release_state


def _validate_image_binding(
    *,
    cluster: Mapping[str, object],
    fixture: Mapping[str, object],
    inspection: Mapping[str, object],
    ds_version: str,
) -> dict[str, object]:
    if fixture.get("image_ref") != cluster.get("image_ref") or fixture.get(
        "image_id"
    ) != cluster.get("image_id"):
        message = "Generic cluster and fixture image identities differ"
        raise ValueError(message)
    if inspection.get("schema_version") != 2:
        message = "Image inspection must declare schema 2"
        raise ValueError(message)
    _validate_image_ref(inspection.get("image_ref"), ds_version=ds_version)
    _require_digest(
        inspection.get("image_id"),
        _SHA256,
        label="Image inspection image ID",
    )
    if inspection.get("image_ref") != cluster.get("image_ref") or inspection.get(
        "image_id"
    ) != cluster.get("image_id"):
        message = "Image inspection differs from the cluster image identity"
        raise ValueError(message)
    kind = _require_text(inspection.get("kind"), label="Image inspection kind")
    if kind == "registry-digest/v1":
        _require_exact_keys(
            inspection,
            {
                "image_id",
                "image_ref",
                "kind",
                "repo_digests",
                "schema_version",
                "selected_repo_digest",
            },
            label="Registry image inspection",
        )
        provenance: dict[str, object] = {
            "kind": kind,
            "repo_digests": inspection["repo_digests"],
            "selected_repo_digest": inspection["selected_repo_digest"],
        }
    elif kind == "managed-image-lock/v1":
        _require_exact_keys(
            inspection,
            {
                "actual",
                "image_id",
                "image_ref",
                "kind",
                "lock",
                "management",
                "schema_version",
            },
            label="Managed image inspection",
        )
        provenance = {
            "kind": kind,
            "management": inspection["management"],
            "lock": inspection["lock"],
            "actual": inspection["actual"],
        }
    else:
        message = "Image inspection kind is not recognized"
        raise ValueError(message)
    return validate_image_provenance(
        provenance,
        image_ref=cast("str", cluster["image_ref"]),
        image_id=cast("str", cluster["image_id"]),
        ds_version=ds_version,
        label="Image inspection provenance",
    )


def _load_identity(
    value: object,
    *,
    expected_kind: IdentityKind,
    label: str,
) -> _NativeIdentity:
    identity = _require_mapping(value, label=label)
    _require_exact_keys(identity, {"kind", "value"}, label=label)
    if identity.get("kind") != expected_kind:
        message = f"{label} must use native kind {expected_kind}"
        raise ValueError(message)
    return _NativeIdentity(
        kind=expected_kind,
        value=_require_positive_int(identity.get("value"), label=f"{label} value"),
    )


def _require_schema_and_version(
    value: Mapping[str, object],
    *,
    ds_version: str,
    label: str,
) -> None:
    if (
        not _is_schema_one(value.get("schema_version"))
        or value.get("ds_version") != ds_version
    ):
        message = f"Generic {label} manifest must declare schema 1 and DS {ds_version}"
        raise ValueError(message)


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    label: str,
) -> None:
    if set(value) != expected:
        message = f"{label} has invalid fields"
        raise ValueError(message)


def _require_mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _require_text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        message = f"{label} must be non-empty text"
        raise TypeError(message)
    return value


def _require_non_numeric_name(value: object, *, label: str) -> str:
    name = _require_text(value, label=label)
    try:
        int(name)
    except ValueError:
        return name
    message = f"{label} must not be a numeric selector"
    raise ValueError(message)


def _require_positive_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} must be a positive integer"
        raise TypeError(message)
    return value


def _is_schema_one(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == 1


def _require_digest(
    value: object,
    pattern: re.Pattern[str],
    *,
    label: str,
) -> str:
    text = _require_text(value, label=label)
    if pattern.fullmatch(text) is None:
        message = f"{label} has an invalid format"
        raise ValueError(message)
    return text


def _validate_image_ref(value: object, *, ds_version: str) -> str:
    return validate_image_reference(value, ds_version=ds_version)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Project one exact-version matrix read fixture into private "
            "conformance-gate manifests."
        )
    )
    parser.add_argument("--ds-version", choices=TARGET_DS_VERSIONS, required=True)
    parser.add_argument("--cluster-manifest", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--image-inspection", type=Path, required=True)
    parser.add_argument("--cluster-output", type=Path, required=True)
    parser.add_argument("--fixture-output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the private matrix-to-conformance projection."""
    args = _parse_args(argv)
    try:
        project_conformance_matrix_fixture(
            ds_version=args.ds_version,
            cluster_manifest=absolute_path(args.cluster_manifest),
            fixture_manifest=absolute_path(args.fixture_manifest),
            state_file=absolute_path(args.state_file),
            image_inspection=absolute_path(args.image_inspection),
            cluster_output=absolute_path(args.cluster_output),
            fixture_output=absolute_path(args.fixture_output),
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
    print("Projected private conformance gate manifests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
