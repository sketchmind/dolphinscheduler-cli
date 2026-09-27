from __future__ import annotations

import importlib
import json
import os
import stat
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING

import pytest
from tests.live.conformance_bundle_gate import (
    identity_hmac,
    load_conformance_bundle_gate_config,
)
from tests.tools.test_conformance_bundle_scenario import _config as _conformance_config
from tests.tools.test_conformance_bundle_scenario import _write_runtime_inputs

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType
    from typing import Any


ROOT = Path(__file__).resolve().parents[2]
# Independently captured HEADs of the managed read-only exact source checkouts.
_MANAGED_SOURCE_COMMITS = {
    "1.3.9": "174c78c4a90a53fdfe7131e9b065edaa38b7936f",
    "2.0.0": "9bd693b7efd8a20075685968ebe6a5941c22fd28",
    "2.0.4": "0ff01e3c68da6a174836216f87c05f707233f7f9",
    "2.0.7": "bf5a0f228b7858c8a3e682a0caa225e3569db15e",
    "2.0.8": "2a22b960580c907f67128a294df2b10b4380312a",
    "2.0.9": "23302054559410093573fb169336bafeb02db97f",
    "3.0.2": "d2ba3d4cfb3ab9f252340aa603ea036baa52f88c",
    "3.1.2": "f1aefae5e25daa5beef08accd8fbd26c32fb6b47",
}
EXACT_VERSIONS = (
    "1.3.9",
    *(f"2.0.{patch}" for patch in range(10)),
    *(f"3.0.{patch}" for patch in range(7)),
    *(f"3.1.{patch}" for patch in range(10)),
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)
MATRIX_IMAGE_SOURCE = "swarm task plus node-local Docker inspection"


def _load_module() -> ModuleType:
    tools_dir = ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("project_conformance_matrix_fixture")


def _load_private_io() -> ModuleType:
    _load_module()
    return importlib.import_module("private_manifest_io")


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.2"])
def test_projection_emits_cluster_schema_two_and_fixture_schema_one(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"

    projection.project_conformance_matrix_fixture(
        ds_version=ds_version,
        **inputs,
        cluster_output=cluster_output,
        fixture_output=fixture_output,
    )

    expected_kind = "id" if ds_version == "1.3.9" else "code"
    expected_project = 71 if expected_kind == "id" else 7001
    expected_workflow = 72 if expected_kind == "id" else 8001
    cluster_source = json.loads(inputs["cluster_manifest"].read_text())
    image_ref = _api_image_ref(ds_version)
    image_repository = image_ref.rpartition(":")[0]
    assert json.loads(cluster_output.read_text(encoding="utf-8")) == {
        "api_target_hmac_sha256": "hmac-sha256:" + "a" * 64,
        "ds_version": ds_version,
        "image_id": "sha256:" + "c" * 64,
        "image_observed_at": cluster_source["image_observed_at"],
        "image_ref": image_ref,
        "image_source": "node-local-inspection",
        "image_provenance": {
            "kind": "registry-digest/v1",
            "repo_digests": [image_repository + "@sha256:" + "d" * 64],
            "selected_repo_digest": (image_repository + "@sha256:" + "d" * 64),
        },
        "persona": "etl-developer",
        "principal_hmac_sha256": "hmac-sha256:" + "b" * 64,
        "schema_version": 2,
    }
    assert json.loads(fixture_output.read_text(encoding="utf-8")) == {
        "ds_version": ds_version,
        "image_id": "sha256:" + "c" * 64,
        "image_ref": image_ref,
        "project": {
            "identity": {"kind": expected_kind, "value": expected_project},
            "name": "fixture-project",
        },
        "provisioner": "dsmatrix-conformance-fixture/v1",
        "schema_version": 1,
        "workflow": {
            "identity": {"kind": expected_kind, "value": expected_workflow},
            "name": "fixture-workflow",
            "release_state": "OFFLINE",
            "schedule_id": 1001,
            "scheduled": True,
        },
    }
    assert stat.S_IMODE(cluster_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(fixture_output.stat().st_mode) == 0o600
    rendered = cluster_output.read_text() + fixture_output.read_text()
    for forbidden in (
        "do-not-publish-this-password",
        "fixture@example.invalid",
        "fixture-tenant",
        "fixture-user",
        "user_password",
        '"token"',
    ):
        assert forbidden not in rendered


@pytest.mark.parametrize("ds_version", ["1.3.9", "3.4.2"])
def test_projection_outputs_bind_through_public_conformance_loader(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    config = _conformance_config(tmp_path, ds_version=ds_version)
    environment = _write_runtime_inputs(tmp_path, config)
    inputs = _valid_inputs(tmp_path / "matrix", ds_version=ds_version)
    api_url = "http://127.0.0.1:12345/dolphinscheduler"
    _replace_json_value(
        inputs["cluster_manifest"],
        ("api_target_hmac_sha256",),
        identity_hmac(api_url, key=config.attestation_key),
    )
    cluster_output = tmp_path / "projected" / "cluster.json"
    fixture_output = tmp_path / "projected" / "fixture.json"

    projection.project_conformance_matrix_fixture(
        ds_version=ds_version,
        **inputs,
        cluster_output=cluster_output,
        fixture_output=fixture_output,
    )
    environment["DS_LIVE_CONFORMANCE_CLUSTER_MANIFEST"] = str(cluster_output)
    environment["DS_LIVE_CONFORMANCE_FIXTURE_MANIFEST"] = str(fixture_output)

    loaded = load_conformance_bundle_gate_config(environment)

    assert loaded.ds_version == ds_version
    assert loaded.cluster.image_source == "node-local-inspection"
    assert loaded.fixture.provisioner == "dsmatrix-conformance-fixture/v1"
    assert loaded.fixture.project_identity.kind == (
        "id" if ds_version == "1.3.9" else "code"
    )


@pytest.mark.parametrize("ds_version", EXACT_VERSIONS)
def test_projection_accepts_every_exact_matrix_version(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)

    projection.project_conformance_matrix_fixture(
        ds_version=ds_version,
        **inputs,
        cluster_output=tmp_path / "output" / "cluster.json",
        fixture_output=tmp_path / "output" / "fixture.json",
    )

    fixture = json.loads((tmp_path / "output" / "fixture.json").read_text())
    expected_kind = "id" if ds_version == "1.3.9" else "code"
    assert fixture["project"]["identity"]["kind"] == expected_kind
    assert fixture["workflow"]["identity"]["kind"] == expected_kind


@pytest.mark.parametrize(
    ("input_name", "field_path", "value", "message"),
    [
        ("cluster_manifest", ("schema_version",), True, "cluster.*schema 1"),
        ("fixture_manifest", ("schema_version",), 2, "fixture.*schema 1"),
        ("state_file", ("schema_version",), True, "state.*schema 1"),
        ("image_inspection", ("schema_version",), True, "inspection.*schema 2"),
        ("cluster_manifest", ("ds_version",), "3.4.1", "cluster.*DS 3.4.2"),
        ("fixture_manifest", ("ds_version",), "3.4.1", "fixture.*DS 3.4.2"),
        ("state_file", ("version",), "3.4.1", "state.*DS 3.4.2"),
    ],
)
def test_projection_requires_exact_input_schemas_and_selected_version(
    tmp_path: Path,
    input_name: str,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises(ValueError, match=message):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_rejects_stale_image_observation(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    stale = (datetime.now(tz=UTC) - timedelta(hours=1)).isoformat()
    _replace_json_value(
        inputs["cluster_manifest"],
        ("image_observed_at",),
        stale,
    )

    with pytest.raises(ValueError, match=r"observation.*window"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    ("input_name", "shape"),
    [
        ("cluster_manifest", "top"),
        ("fixture_manifest", "top"),
        ("state_file", "top"),
        ("image_inspection", "top"),
        ("fixture_manifest", "nested"),
        ("state_file", "nested"),
    ],
)
def test_projection_rejects_extra_fields(
    tmp_path: Path,
    input_name: str,
    shape: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    field_path: tuple[str, ...] = (
        ("project", "unexpected") if shape == "nested" else ("unexpected",)
    )
    if input_name == "state_file" and shape == "nested":
        field_path = ("resources", "workflow", "unexpected")
    _replace_json_value(inputs[input_name], field_path, "do-not-echo-extra-key")

    with pytest.raises(ValueError, match="invalid fields"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "input_name",
    ["cluster_manifest", "fixture_manifest", "state_file", "image_inspection"],
)
def test_projection_rejects_duplicate_json_keys(
    tmp_path: Path,
    input_name: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    duplicate = inputs[input_name]
    duplicate.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    duplicate.chmod(0o600)

    with pytest.raises(ValueError, match="duplicate"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    ("input_name", "field_path", "value", "message"),
    [
        (
            "cluster_manifest",
            ("image_source",),
            "swarm service inspection",
            "image_source.*canonical matrix",
        ),
        ("cluster_manifest", ("persona",), "admin", "persona.*etl-developer"),
        (
            "cluster_manifest",
            ("api_target_hmac_sha256",),
            "sha256:" + "a" * 64,
            "API target HMAC",
        ),
        (
            "cluster_manifest",
            ("principal_hmac_sha256",),
            "hmac-sha256:ABC",
            "principal HMAC",
        ),
        (
            "cluster_manifest",
            ("image_ref",),
            "dsmatrix-local/dolphinscheduler:3.4.1",
            "exact DS 3.4.2",
        ),
        ("cluster_manifest", ("image_id",), "local-id", "cluster image ID"),
        (
            "fixture_manifest",
            ("provisioner",),
            "manual-bootstrap",
            "dsmatrix-exact-read/v1",
        ),
        (
            "fixture_manifest",
            ("project", "identity", "kind"),
            "id",
            "project identity.*code",
        ),
        (
            "fixture_manifest",
            ("workflow", "identity", "kind"),
            "id",
            "workflow identity.*code",
        ),
        (
            "fixture_manifest",
            ("project", "name"),
            "1234",
            "project name.*numeric",
        ),
        (
            "fixture_manifest",
            ("workflow", "scheduled"),
            False,
            "attached schedule",
        ),
        (
            "fixture_manifest",
            ("workflow", "release_state"),
            "PAUSED",
            "release state.*ONLINE or OFFLINE",
        ),
    ],
)
def test_projection_rejects_untrusted_identity_and_provenance(
    tmp_path: Path,
    input_name: str,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("phase",), "provisioning", "ready with pending null"),
        (("pending",), {"operation": "workflow"}, "ready with pending null"),
        (("campaign_id",), "campaign/nested", "campaign identifier"),
        (("ownership_marker",), "foreign", "ownership marker"),
        (
            ("names", "project"),
            "other-project",
            "project (resource name is inconsistent|names differ)",
        ),
        (
            ("names", "workflow"),
            "other-workflow",
            "workflow (resource name is inconsistent|names differ)",
        ),
        (("resources", "project", "code"), 7002, "project identities differ"),
        (
            ("resources", "workflow", "code"),
            8002,
            "workflow (relationship|identities)",
        ),
        (("resources", "schedule", "id"), 1002, "schedule resource differs"),
        (
            ("resources", "workflow", "release_state"),
            "ONLINE",
            "workflow resource differs",
        ),
        (
            ("resources", "schedule", "release_state"),
            "PAUSED",
            "schedule release state",
        ),
        (
            ("resources", "schedule", "workflow_code"),
            8002,
            "schedule workflow relationship",
        ),
        (
            ("resources", "schedule", "workflow_code"),
            8001.0,
            "schedule workflow code",
        ),
        (("resources", "project", "user_id"), 99, "user resource"),
        (("resources", "user", "tenant_id"), 99, "tenant resource"),
        (("resources", "workflow", "task_code"), 0, "task code"),
    ],
)
def test_projection_rejects_state_or_resource_drift(
    tmp_path: Path,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    _replace_json_value(inputs["state_file"], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("resources", "project", "code"), 7001, "project.*invalid fields"),
        (
            ("resources", "workflow", "project_id"),
            99,
            "workflow project relationship",
        ),
        (
            ("resources", "schedule", "workflow_id"),
            99,
            "schedule workflow relationship",
        ),
        (("resources", "workflow", "task_id"), "", "task identifier"),
        (("resources", "project", "user_id"), 74.0, "project user ID"),
        (("resources", "token", "user_id"), 74.0, "token user ID"),
        (("resources", "user", "tenant_id"), 73.0, "user tenant ID"),
        (
            ("resources", "workflow", "project_id"),
            71.0,
            "workflow project ID",
        ),
        (
            ("resources", "schedule", "workflow_id"),
            72.0,
            "schedule workflow ID",
        ),
    ],
)
def test_projection_rejects_legacy_id_resource_drift(
    tmp_path: Path,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="1.3.9")
    _replace_json_value(inputs["state_file"], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_conformance_matrix_fixture(
            ds_version="1.3.9",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            (("fixture_manifest", ("image_id",), "sha256:" + "e" * 64),),
            "cluster and fixture image identities",
        ),
        (
            (
                (
                    "fixture_manifest",
                    ("image_ref",),
                    "mirror/dolphinscheduler:3.4.2",
                ),
            ),
            "cluster and fixture image identities",
        ),
        (
            (("image_inspection", ("image_id",), "sha256:" + "e" * 64),),
            "inspection differs",
        ),
        (
            (
                (
                    "image_inspection",
                    ("image_ref",),
                    "mirror/dolphinscheduler:3.4.2",
                ),
            ),
            "exact DS 3.4.2",
        ),
        (
            (
                (
                    "image_inspection",
                    ("selected_repo_digest",),
                    "apache/dolphinscheduler-api@sha256:" + "e" * 64,
                ),
            ),
            "occur exactly once",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    [
                        "apache/dolphinscheduler-api@sha256:" + "d" * 64,
                        "apache/dolphinscheduler-api@sha256:" + "d" * 64,
                    ],
                ),
            ),
            "must be unique",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    [
                        "apache/dolphinscheduler-api@sha256:" + "d" * 64,
                        "apache/dolphinscheduler-api@sha256:" + "e" * 64,
                    ],
                ),
            ),
            "one selected service RepoDigest",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    ["not-a-repository-digest"],
                ),
                (
                    "image_inspection",
                    ("selected_repo_digest",),
                    "not-a-repository-digest",
                ),
            ),
            "RepoDigest.*invalid format",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    ["bad repository@sha256:" + "d" * 64],
                ),
                (
                    "image_inspection",
                    ("selected_repo_digest",),
                    "bad repository@sha256:" + "d" * 64,
                ),
            ),
            "RepoDigest.*invalid format",
        ),
    ],
)
def test_projection_rejects_image_binding_drift(
    tmp_path: Path,
    changes: Sequence[tuple[str, Sequence[str], object]],
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    for input_name, field_path, value in changes:
        _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", "2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"],
)
def test_projection_accepts_exact_unified_managed_image_lock_provenance(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    _write_private(
        inputs["image_inspection"],
        _managed_image_inspection(ds_version=ds_version),
    )

    projection.project_conformance_matrix_fixture(
        ds_version=ds_version,
        **inputs,
        cluster_output=tmp_path / "output" / "cluster.json",
        fixture_output=tmp_path / "output" / "fixture.json",
    )

    cluster = json.loads((tmp_path / "output" / "cluster.json").read_text())
    provenance = cluster["image_provenance"]
    assert provenance["kind"] == "managed-image-lock/v1"
    assert provenance["management"] == {
        "commit": "1" * 40,
        "lock_file_sha256": "sha256:" + "2" * 64,
        "worktree_clean": True,
    }
    assert provenance["lock"]["component"] == "unified"
    assert provenance["lock"]["version"] == ds_version
    assert provenance["actual"]["archive_verified"] is True
    rendered = json.dumps(provenance, sort_keys=True)
    assert "runtime/images" not in rendered
    assert provenance["lock"]["archive_basename"] == f"unified-{ds_version}.tar"


@pytest.mark.parametrize("ds_version", tuple(_MANAGED_SOURCE_COMMITS))
def test_projection_rejects_matching_forged_managed_source_before_publishing(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    inspection = _managed_image_inspection(ds_version=ds_version)
    inspection["lock"]["source_commit"] = "f" * 40
    if ds_version != "1.3.9":
        inspection["actual"]["labels"][
            "org.apache.dolphinscheduler.matrix.source-commit"
        ] = "f" * 40
    _write_private(inputs["image_inspection"], inspection)
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"

    with pytest.raises(ValueError, match="source commit differs"):
        projection.project_conformance_matrix_fixture(
            ds_version=ds_version,
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
        )

    assert not cluster_output.exists()
    assert not fixture_output.exists()


@pytest.mark.parametrize(
    "ds_version", ["2.0.1", "2.0.6", "3.0.0", "3.0.6", "3.4.2", "3.4.3"]
)
def test_projection_rejects_managed_lock_as_api_fallback_outside_unified_epoch(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    _write_private(
        inputs["image_inspection"],
        _managed_image_inspection(ds_version=ds_version),
    )

    with pytest.raises(ValueError, match=r"managed.*unified.*not trusted"):
        projection.project_conformance_matrix_fixture(
            ds_version=ds_version,
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "ds_version", ["2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"]
)
def test_projection_rejects_managed_archive_claim_without_hash_verification(
    tmp_path: Path,
    ds_version: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    inspection = _managed_image_inspection(ds_version=ds_version)
    inspection["actual"]["archive_verified"] = False
    _write_private(inputs["image_inspection"], inspection)

    with pytest.raises(ValueError, match="archive verification claim"):
        projection.project_conformance_matrix_fixture(
            ds_version=ds_version,
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize("observed", [None, "sha256:" + "8" * 64])
@pytest.mark.parametrize(
    "ds_version", ["2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"]
)
def test_projection_rejects_managed_true_archive_claim_without_matching_digest(
    tmp_path: Path,
    ds_version: str,
    observed: str | None,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    inspection = _managed_image_inspection(ds_version=ds_version)
    inspection["actual"]["archive_sha256"] = observed
    _write_private(inputs["image_inspection"], inspection)

    with pytest.raises(ValueError, match=r"archive (verification|digest)"):
        projection.project_conformance_matrix_fixture(
            ds_version=ds_version,
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "ds_version", ["2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"]
)
def test_projection_rejects_managed_provenance_label_drift(
    tmp_path: Path, ds_version: str
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version=ds_version)
    inspection = _managed_image_inspection(ds_version=ds_version)
    inspection["actual"]["labels"][
        "org.apache.dolphinscheduler.matrix.source-commit"
    ] = "9" * 40
    _write_private(inputs["image_inspection"], inspection)

    with pytest.raises(ValueError, match="provenance labels"):
        projection.project_conformance_matrix_fixture(
            ds_version=ds_version,
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_rejects_managed_139_injected_provenance_label(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="1.3.9")
    inspection = _managed_image_inspection(ds_version="1.3.9")
    inspection["actual"]["labels"] = {
        "org.apache.dolphinscheduler.matrix.source-tag": "1.3.9"
    }
    _write_private(inputs["image_inspection"], inspection)

    with pytest.raises(ValueError, match=r"1.3.9.*must not claim"):
        projection.project_conformance_matrix_fixture(
            ds_version="1.3.9",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_rejects_managed_published_manifest_from_wrong_repository(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="2.0.9")
    inspection = _managed_image_inspection(ds_version="2.0.9")
    inspection["lock"]["published_manifest"] = "evil/repo@sha256:" + "d" * 64
    inspection["actual"]["repo_digests"] = ["evil/repo@sha256:" + "d" * 64]
    _write_private(inputs["image_inspection"], inspection)

    with pytest.raises(
        ValueError,
        match=r"published manifest.*wrong API repository",
    ):
        projection.project_conformance_matrix_fixture(
            ds_version="2.0.9",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "image_ref",
    [
        "private.internal/team/dolphinscheduler-api:3.4.2",
        "user@host/dolphinscheduler-api:3.4.2",
        "https://apache/dolphinscheduler-api:3.4.2",
    ],
)
def test_projection_rejects_non_allowlisted_api_image_repository(
    tmp_path: Path,
    image_ref: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    for input_name in ("cluster_manifest", "fixture_manifest", "image_inspection"):
        _replace_json_value(inputs[input_name], ("image_ref",), image_ref)

    with pytest.raises(ValueError, match=r"exact DS 3.4.2"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_accepts_tag_and_published_digest_image_reference(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    published = "sha256:" + "d" * 64
    image_ref = f"apache/dolphinscheduler-api:3.4.2@{published}"
    for input_name in ("cluster_manifest", "fixture_manifest", "image_inspection"):
        _replace_json_value(inputs[input_name], ("image_ref",), image_ref)

    projection.project_conformance_matrix_fixture(
        ds_version="3.4.2",
        **inputs,
        cluster_output=tmp_path / "cluster.json",
        fixture_output=tmp_path / "fixture.json",
    )

    cluster = json.loads((tmp_path / "cluster.json").read_text())
    assert cluster["image_ref"] == image_ref


def test_projection_accepts_pinned_digest_with_multiple_repository_digests(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="2.0.5")
    repository = "apache/dolphinscheduler"
    selected_digest = (
        "sha256:e1303513da517f594132a03d772dc29465a83d0e5fbc0297f97c1752c2e16a09"
    )
    other_digest = (
        "sha256:6191c39acc35dc0afb8cfffdbea375e6ab2f4ee94244e40c3bef39973a1a51eb"
    )
    selected = f"{repository}@{selected_digest}"
    image_ref = f"{repository}:2.0.5@{selected_digest}"
    for input_name in ("cluster_manifest", "fixture_manifest", "image_inspection"):
        _replace_json_value(inputs[input_name], ("image_ref",), image_ref)
    _replace_json_value(
        inputs["image_inspection"],
        ("repo_digests",),
        [
            f"{repository}@{other_digest}",
            selected,
        ],
    )
    _replace_json_value(
        inputs["image_inspection"],
        ("selected_repo_digest",),
        selected,
    )

    projection.project_conformance_matrix_fixture(
        ds_version="2.0.5",
        **inputs,
        cluster_output=tmp_path / "cluster.json",
        fixture_output=tmp_path / "fixture.json",
    )

    cluster = json.loads((tmp_path / "cluster.json").read_text())
    assert cluster["image_provenance"]["selected_repo_digest"] == selected
    assert cluster["image_provenance"]["repo_digests"] == [
        f"{repository}@{other_digest}",
        selected,
    ]


def test_projection_rejects_tag_digest_not_selected_by_registry_proof(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    image_ref = "apache/dolphinscheduler-api:3.4.2@sha256:" + "e" * 64
    for input_name in ("cluster_manifest", "fixture_manifest", "image_inspection"):
        _replace_json_value(inputs[input_name], ("image_ref",), image_ref)

    with pytest.raises(ValueError, match=r"published digest.*selected"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_rejects_pinned_digest_with_selected_wrong_repository(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="2.0.5")
    repository = "apache/dolphinscheduler"
    selected_digest = (
        "sha256:e1303513da517f594132a03d772dc29465a83d0e5fbc0297f97c1752c2e16a09"
    )
    image_ref = f"{repository}:2.0.5@{selected_digest}"
    for input_name in ("cluster_manifest", "fixture_manifest", "image_inspection"):
        _replace_json_value(inputs[input_name], ("image_ref",), image_ref)
    _replace_json_value(
        inputs["image_inspection"],
        ("repo_digests",),
        [
            f"{repository}@{selected_digest}",
            "mirror.example/dolphinscheduler@sha256:" + "e" * 64,
        ],
    )
    _replace_json_value(
        inputs["image_inspection"],
        ("selected_repo_digest",),
        "mirror.example/dolphinscheduler@sha256:" + "e" * 64,
    )

    with pytest.raises(ValueError, match=r"published digest.*selected"):
        projection.project_conformance_matrix_fixture(
            ds_version="2.0.5",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


@pytest.mark.parametrize(
    "input_name",
    ["cluster_manifest", "fixture_manifest", "state_file", "image_inspection"],
)
@pytest.mark.parametrize("invalid_kind", ["public", "symlink", "directory"])
def test_projection_accepts_only_owner_private_regular_inputs(
    tmp_path: Path,
    input_name: str,
    invalid_kind: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    input_path = inputs[input_name]
    if invalid_kind == "public":
        input_path.chmod(0o640)
    elif invalid_kind == "symlink":
        target = input_path.with_suffix(".target.json")
        input_path.rename(target)
        input_path.symlink_to(target)
    else:
        input_path.unlink()
        input_path.mkdir(mode=0o700)

    with pytest.raises((PermissionError, ValueError), match="owner-private regular"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_projection_rejects_owner_private_fifo_without_blocking(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    fifo = inputs["cluster_manifest"]
    fifo.unlink()
    os.mkfifo(fifo, mode=0o600)
    errors: list[BaseException] = []

    def invoke_projection() -> None:
        try:
            projection.project_conformance_matrix_fixture(
                ds_version="3.4.2",
                **inputs,
                cluster_output=tmp_path / "cluster.json",
                fixture_output=tmp_path / "fixture.json",
            )
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=invoke_projection, daemon=True)
    worker.start()
    worker.join(timeout=0.25)
    completed_without_writer = not worker.is_alive()
    if worker.is_alive():
        unblock_descriptor = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
        worker.join(timeout=1)
        os.close(unblock_descriptor)

    assert completed_without_writer
    assert len(errors) == 1
    assert isinstance(errors[0], PermissionError)


def test_private_json_rejects_non_finite_constants(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    cluster = inputs["cluster_manifest"]
    raw = cluster.read_text(encoding="utf-8").replace(
        '"schema_version": 1',
        '"schema_version": NaN',
    )
    cluster.write_text(raw, encoding="utf-8")
    cluster.chmod(0o600)

    with pytest.raises(ValueError, match="non-finite"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=tmp_path / "cluster.json",
            fixture_output=tmp_path / "fixture.json",
        )


def test_private_json_publication_rejects_non_finite_values(tmp_path: Path) -> None:
    private_io = _load_private_io()
    first = tmp_path / "output" / "first.json"
    second = tmp_path / "output" / "second.json"

    with pytest.raises(ValueError, match="JSON compliant"):
        private_io.publish_private_json_pair(
            {"value": float("nan")},
            first,
            {"value": "finite"},
            second,
        )

    assert not first.exists()
    assert not second.exists()


def test_projection_forces_output_mode_0600_under_restrictive_umask(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    cluster_output = output_dir / "cluster.json"
    fixture_output = output_dir / "fixture.json"

    previous_umask = os.umask(0o777)
    try:
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
        )
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE(cluster_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(fixture_output.stat().st_mode) == 0o600


@pytest.mark.parametrize("existing_output", ["cluster", "fixture"])
def test_projection_preflights_both_outputs_before_publication(
    tmp_path: Path,
    existing_output: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"
    conflict = cluster_output if existing_output == "cluster" else fixture_output
    conflict.parent.mkdir(mode=0o700)
    conflict.write_text("racer-owned", encoding="utf-8")

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
        )

    assert conflict.read_text() == "racer-owned"
    untouched = fixture_output if existing_output == "cluster" else cluster_output
    assert not untouched.exists()


def test_projection_rolls_back_only_its_first_publish_on_second_link_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _load_module()
    private_io = _load_private_io()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"
    real_link = private_io.os.link

    def race_second_publish(
        source: str,
        destination: str,
        **kwargs: Any,
    ) -> None:
        if destination == fixture_output.name:
            fixture_output.write_text("racer-owned", encoding="utf-8")
            raise FileExistsError
        real_link(source, destination, **kwargs)

    monkeypatch.setattr(private_io.os, "link", race_second_publish)

    with pytest.raises(FileExistsError):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
        )

    assert not cluster_output.exists()
    assert fixture_output.read_text() == "racer-owned"


def test_projection_rolls_back_first_publish_on_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _load_module()
    private_io = _load_private_io()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"
    real_link = private_io.os.link
    link_count = 0

    def interrupt_second_publish(
        source: str,
        destination: str,
        **kwargs: Any,
    ) -> None:
        nonlocal link_count
        link_count += 1
        if link_count == 2:
            raise KeyboardInterrupt
        real_link(source, destination, **kwargs)

    monkeypatch.setattr(private_io.os, "link", interrupt_second_publish)

    with pytest.raises(KeyboardInterrupt):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
        )

    assert not cluster_output.exists()
    assert not fixture_output.exists()


def test_projection_refuses_output_symlink_and_public_parent(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    cluster_output = output_dir / "cluster.json"
    target = output_dir / "target.json"
    cluster_output.symlink_to(target)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=output_dir / "fixture.json",
        )
    assert cluster_output.is_symlink()
    assert not target.exists()

    cluster_output.unlink()
    output_dir.chmod(0o755)
    with pytest.raises(PermissionError, match="owner-private directory"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=cluster_output,
            fixture_output=output_dir / "fixture.json",
        )


def test_projection_requires_distinct_explicit_output_paths(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    output = tmp_path / "output" / "manifest.json"

    with pytest.raises(ValueError, match="different output paths"):
        projection.project_conformance_matrix_fixture(
            ds_version="3.4.2",
            **inputs,
            cluster_output=output,
            fixture_output=output,
        )
    assert not output.exists()


def test_projection_cli_prints_no_fixture_or_secret_values(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")

    exit_code = projection.main(
        [
            "--ds-version",
            "3.4.2",
            "--cluster-manifest",
            str(inputs["cluster_manifest"]),
            "--fixture-manifest",
            str(inputs["fixture_manifest"]),
            "--state-file",
            str(inputs["state_file"]),
            "--image-inspection",
            str(inputs["image_inspection"]),
            "--cluster-output",
            str(tmp_path / "output" / "cluster.json"),
            "--fixture-output",
            str(tmp_path / "output" / "fixture.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Projected private conformance gate manifests." in captured.out
    for protected in (
        "do-not-publish-this-password",
        "fixture@example.invalid",
        "fixture-project",
        "fixture-workflow",
        "fixture-shell-task",
        "7001",
        "8001",
        "9001",
        "1001",
    ):
        assert protected not in captured.out
        assert protected not in captured.err


def test_projection_cli_rejects_bare_digest_parameter(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")

    with pytest.raises(SystemExit):
        projection._parse_args(
            [
                "--ds-version",
                "3.4.2",
                "--cluster-manifest",
                str(inputs["cluster_manifest"]),
                "--fixture-manifest",
                str(inputs["fixture_manifest"]),
                "--state-file",
                str(inputs["state_file"]),
                "--image-digest",
                "dsmatrix-local/dolphinscheduler@sha256:" + "d" * 64,
                "--cluster-output",
                str(tmp_path / "cluster.json"),
                "--fixture-output",
                str(tmp_path / "fixture.json"),
            ]
        )


def test_projection_cli_sanitizes_untrusted_validation_values(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path, ds_version="3.4.2")
    protected = "DS_API_TOKEN=do-not-echo-this"
    _replace_json_value(inputs["state_file"], (protected,), True)

    exit_code = projection.main(
        [
            "--ds-version",
            "3.4.2",
            "--cluster-manifest",
            str(inputs["cluster_manifest"]),
            "--fixture-manifest",
            str(inputs["fixture_manifest"]),
            "--state-file",
            str(inputs["state_file"]),
            "--image-inspection",
            str(inputs["image_inspection"]),
            "--cluster-output",
            str(tmp_path / "cluster.json"),
            "--fixture-output",
            str(tmp_path / "fixture.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "invalid fields" in captured.err
    assert "Traceback" not in captured.err
    assert protected not in captured.out
    assert protected not in captured.err


def _valid_inputs(tmp_path: Path, *, ds_version: str) -> dict[str, Path]:
    cluster_manifest = tmp_path / "cluster-v1.json"
    fixture_manifest = tmp_path / "fixture-v1.json"
    state_file = tmp_path / "state.json"
    image_inspection = tmp_path / "image-inspection.json"
    observed_at = datetime.now(tz=UTC).isoformat()
    image_ref = _api_image_ref(ds_version)
    image_id = "sha256:" + "c" * 64
    image_digest = image_ref.rpartition(":")[0] + "@sha256:" + "d" * 64
    identity_kind = "id" if ds_version == "1.3.9" else "code"
    project_identity = 71 if identity_kind == "id" else 7001
    workflow_identity = 72 if identity_kind == "id" else 8001
    _write_private(
        cluster_manifest,
        {
            "api_target_hmac_sha256": "hmac-sha256:" + "a" * 64,
            "ds_version": ds_version,
            "image_id": image_id,
            "image_observed_at": observed_at,
            "image_ref": image_ref,
            "image_source": MATRIX_IMAGE_SOURCE,
            "persona": "etl-developer",
            "principal_hmac_sha256": "hmac-sha256:" + "b" * 64,
            "schema_version": 1,
        },
    )
    _write_private(
        fixture_manifest,
        {
            "ds_version": ds_version,
            "image_id": image_id,
            "image_ref": image_ref,
            "project": {
                "identity": {"kind": identity_kind, "value": project_identity},
                "name": "fixture-project",
            },
            "provisioner": "dsmatrix-exact-read/v1",
            "schema_version": 1,
            "workflow": {
                "identity": {"kind": identity_kind, "value": workflow_identity},
                "name": "fixture-workflow",
                "release_state": "OFFLINE",
                "schedule_id": 1001,
                "scheduled": True,
            },
        },
    )
    campaign_id = "conformance-20260810"
    if ds_version == "1.3.9":
        resources = {
            "project": {
                "id": 71,
                "name": "fixture-project",
                "user_id": 74,
            },
            "schedule": {
                "id": 1001,
                "release_state": "OFFLINE",
                "workflow_id": 72,
            },
            "tenant": {"id": 73, "name": "fixture-tenant"},
            "token": {"id": 75, "user_id": 74},
            "user": {"id": 74, "name": "fixture-user", "tenant_id": 73},
            "workflow": {
                "id": 72,
                "name": "fixture-workflow",
                "project_id": 71,
                "release_state": "OFFLINE",
                "task_id": "fixture-native-task-id",
            },
        }
    else:
        resources = {
            "project": {
                "code": 7001,
                "id": 71,
                "name": "fixture-project",
                "user_id": 74,
            },
            "schedule": {
                "id": 1001,
                "release_state": "OFFLINE",
                "workflow_code": 8001,
            },
            "tenant": {"id": 73, "name": "fixture-tenant"},
            "token": {"id": 75, "user_id": 74},
            "user": {"id": 74, "name": "fixture-user", "tenant_id": 73},
            "workflow": {
                "code": 8001,
                "id": 72,
                "name": "fixture-workflow",
                "release_state": "OFFLINE",
                "task_code": 9001,
            },
        }
    _write_private(
        state_file,
        {
            "campaign_id": campaign_id,
            "email": "fixture@example.invalid",
            "names": {
                "project": "fixture-project",
                "task": "fixture-shell-task",
                "tenant": "fixture-tenant",
                "user": "fixture-user",
                "workflow": "fixture-workflow",
            },
            "ownership_marker": (
                f"dsmatrix-exact-read/v1/{ds_version}/{campaign_id}/0123456789ab"
            ),
            "pending": None,
            "phase": "ready",
            "resources": resources,
            "schema_version": 1,
            "user_password": "do-not-publish-this-password",
            "version": ds_version,
        },
    )
    _write_private(
        image_inspection,
        {
            "kind": "registry-digest/v1",
            "image_id": image_id,
            "image_ref": image_ref,
            "repo_digests": [image_digest],
            "schema_version": 2,
            "selected_repo_digest": image_digest,
        },
    )
    return {
        "cluster_manifest": cluster_manifest,
        "fixture_manifest": fixture_manifest,
        "state_file": state_file,
        "image_inspection": image_inspection,
    }


def _managed_image_inspection(*, ds_version: str) -> dict[str, Any]:
    source_commit = _MANAGED_SOURCE_COMMITS.get(ds_version, "3" * 40)
    source_sha512 = "4" * 128 if ds_version != "1.3.9" else None
    binary_sha512 = (
        "5" * 128
        if ds_version in {"2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"}
        else None
    )
    base_manifest = (
        "mirror.example/base@sha256:" + "6" * 64 if ds_version != "1.3.9" else None
    )
    labels = {}
    if ds_version in {
        "2.0.0",
        "2.0.4",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.2",
        "3.1.2",
    }:
        labels = {
            "org.apache.dolphinscheduler.matrix.base-digest": base_manifest,
            "org.apache.dolphinscheduler.matrix.source-commit": source_commit,
            "org.apache.dolphinscheduler.matrix.source-tag": ds_version,
        }
        if source_sha512 is not None:
            labels["org.apache.dolphinscheduler.matrix.source-sha512"] = source_sha512
        if binary_sha512 is not None:
            labels["org.apache.dolphinscheduler.matrix.binary-sha512"] = binary_sha512
    return {
        "actual": {
            "archive_sha256": "sha256:" + "7" * 64,
            "archive_verified": True,
            "labels": labels,
            "repo_digests": [],
        },
        "image_id": "sha256:" + "c" * 64,
        "image_ref": _api_image_ref(ds_version),
        "kind": "managed-image-lock/v1",
        "lock": {
            "archive_basename": f"unified-{ds_version}.tar",
            "archive_sha256": "sha256:" + "7" * 64,
            "base_manifest": base_manifest,
            "binary_sha512": (
                "sha512:" + binary_sha512 if binary_sha512 is not None else None
            ),
            "component": "unified",
            "image_id": "sha256:" + "c" * 64,
            "image_ref": _api_image_ref(ds_version),
            "published_manifest": (
                _api_image_ref(ds_version).rpartition(":")[0] + "@sha256:" + "d" * 64
                if ds_version == "1.3.9"
                else None
            ),
            "schema_sha256": ("sha256:" + "8" * 64 if ds_version == "2.0.0" else None),
            "source_commit": source_commit,
            "source_sha512": (
                "sha512:" + source_sha512 if source_sha512 is not None else None
            ),
            "version": ds_version,
        },
        "management": {
            "commit": "1" * 40,
            "lock_file_sha256": "sha256:" + "2" * 64,
            "worktree_clean": True,
        },
        "schema_version": 2,
    }


def _api_image_ref(ds_version: str) -> str:
    if ds_version in {"2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9"}:
        repository = "dsmatrix-local/dolphinscheduler"
    elif ds_version in {"3.0.2", "3.1.2"}:
        repository = "dsmatrix-local/dolphinscheduler-api"
    elif ds_version in {"1.3.9", *(f"2.0.{patch}" for patch in range(1, 7))}:
        repository = "apache/dolphinscheduler"
    else:
        repository = "apache/dolphinscheduler-api"
    return f"{repository}:{ds_version}"


def _write_private(path: Path, payload: object) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)


def _replace_json_value(
    path: Path,
    field_path: Sequence[str],
    value: object,
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    target = payload
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = value
    _write_private(path, payload)
