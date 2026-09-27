from __future__ import annotations

import importlib
import json
import stat
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType
    from typing import Any


ROOT = Path(__file__).resolve().parents[2]
IMAGE_DIGEST = "apache/dolphinscheduler-api@sha256:" + "d" * 64


def _load_module() -> ModuleType:
    tools_dir = ROOT / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.exact_profile_fixture")


def _load_private_io() -> ModuleType:
    _load_module()
    return importlib.import_module("private_manifest_io")


def test_projection_emits_private_schema_2_gate_manifests(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"

    projection.project_exact_profile_matrix_fixture(
        **inputs,
        cluster_output=cluster_output,
        fixture_output=fixture_output,
        ds_version="3.4.2",
    )

    assert json.loads(cluster_output.read_text(encoding="utf-8")) == {
        "api_target_hmac_sha256": "hmac-sha256:" + "a" * 64,
        "ds_version": "3.4.2",
        "image_digest": IMAGE_DIGEST,
        "image_observed_at": "2026-08-10T01:02:03Z",
        "image_source": "swarm task plus node-local Docker inspection",
        "image_tag": "apache/dolphinscheduler-api:3.4.2",
        "persona": "etl-developer",
        "principal_hmac_sha256": "hmac-sha256:" + "b" * 64,
        "schema_version": 2,
    }
    assert json.loads(fixture_output.read_text(encoding="utf-8")) == {
        "ds_version": "3.4.2",
        "exclusive": True,
        "image_digest": IMAGE_DIGEST,
        "image_tag": "apache/dolphinscheduler-api:3.4.2",
        "project": {"code": 7001, "name": "fixture-project"},
        "provisioner": "dsmatrix-exact-read-state-projection/v2",
        "schema_version": 2,
        "workflow": {
            "code": 8001,
            "editable_task": {
                "code": 9001,
                "name": "fixture-shell-task",
                "type": "SHELL",
            },
            "name": "fixture-workflow",
            "release_state": "OFFLINE",
            "schedule_id": 1001,
            "scheduled": True,
        },
    }
    assert stat.S_IMODE(cluster_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(fixture_output.stat().st_mode) == 0o600
    rendered = cluster_output.read_text(encoding="utf-8") + fixture_output.read_text(
        encoding="utf-8"
    )
    for forbidden in (
        "do-not-publish-this-password",
        "fixture@example.invalid",
        "fixture-tenant",
        "fixture-user",
        "user_password",
        '"token"',
    ):
        assert forbidden not in rendered


def test_projection_accepts_current_collector_schema_2_image_inspection(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _write_current_schema_2_image_inspection(inputs["image_inspection"])
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"

    projection.project_exact_profile_matrix_fixture(
        **inputs,
        cluster_output=cluster_output,
        fixture_output=fixture_output,
        ds_version="3.4.2",
    )

    assert json.loads(cluster_output.read_text(encoding="utf-8"))["image_digest"] == (
        IMAGE_DIGEST
    )
    assert json.loads(fixture_output.read_text(encoding="utf-8"))["image_digest"] == (
        IMAGE_DIGEST
    )


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("registry-digest/v2", "kind is not recognized"),
        ("managed-image-lock/v1", "provenance has invalid fields"),
    ],
)
def test_projection_rejects_current_collector_schema_2_with_wrong_kind(
    tmp_path: Path,
    kind: str,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _write_current_schema_2_image_inspection(
        inputs["image_inspection"],
        kind=kind,
    )

    with pytest.raises(ValueError, match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )
    assert not (tmp_path / "cluster-v2.json").exists()
    assert not (tmp_path / "fixture-v2.json").exists()


def test_projection_rejects_current_collector_schema_2_with_extra_field(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _write_current_schema_2_image_inspection(
        inputs["image_inspection"],
        unexpected=True,
    )

    with pytest.raises(ValueError, match="Image inspection has invalid fields"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


def test_projection_rejects_ambiguous_current_collector_schema_2_repo_digests(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _write_current_schema_2_image_inspection(
        inputs["image_inspection"],
        repo_digests=[
            IMAGE_DIGEST,
            "apache/dolphinscheduler-api@sha256:" + "e" * 64,
        ],
    )

    with pytest.raises(ValueError, match="one selected service RepoDigest"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


def test_projection_requires_the_private_matrix_state_file(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    inputs["state_file"].chmod(0o640)

    with pytest.raises(PermissionError, match=r"state file.*group or others"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    "input_name",
    ["cluster_manifest", "fixture_manifest", "image_inspection", "state_file"],
)
@pytest.mark.parametrize("invalid_kind", ["public", "symlink", "directory"])
def test_projection_accepts_only_owner_private_regular_inputs(
    tmp_path: Path,
    input_name: str,
    invalid_kind: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
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
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("schema_version",), 2, "schema 1 and DS 3.4.2"),
        (("version",), "3.4.1", "schema 1 and DS 3.4.2"),
        (("phase",), "provisioning", "phase must be ready"),
        (("pending",), {"operation": "workflow"}, "pending must be null"),
        (("ownership_marker",), "not-campaign-owned", "ownership marker"),
    ],
)
def test_projection_requires_ready_campaign_owned_state(
    tmp_path: Path,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs["state_file"], field_path, value)

    with pytest.raises(ValueError, match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("input_name", "field_path", "value", "message"),
    [
        ("cluster_manifest", ("schema_version",), 2, "cluster.*schema 1"),
        ("fixture_manifest", ("schema_version",), 2, "fixture.*schema 1"),
        ("image_inspection", ("schema_version",), 3, "inspection.*schema 1"),
        ("state_file", ("unexpected",), True, "state.*invalid fields"),
        ("cluster_manifest", ("unexpected",), True, "cluster.*invalid fields"),
        ("fixture_manifest", ("unexpected",), True, "fixture.*invalid fields"),
        ("image_inspection", ("unexpected",), True, "inspection.*invalid fields"),
    ],
)
def test_projection_requires_exact_schema_1_input_shapes(
    tmp_path: Path,
    input_name: str,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises(ValueError, match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("input_name", "version_field"),
    [
        ("cluster_manifest", "schema_version"),
        ("fixture_manifest", "schema_version"),
        ("image_inspection", "schema_version"),
        ("state_file", "schema_version"),
    ],
)
def test_projection_rejects_boolean_schema_versions(
    tmp_path: Path,
    input_name: str,
    version_field: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs[input_name], (version_field,), True)

    with pytest.raises(ValueError, match="schema 1"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


def test_projection_rejects_noncanonical_campaign_identifiers(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs["state_file"], ("campaign_id",), "campaign/nested")
    _replace_json_value(
        inputs["state_file"],
        ("ownership_marker",),
        "dsmatrix-exact-read/v1/3.4.2/campaign/nested/0123456789ab",
    )

    with pytest.raises(ValueError, match="campaign identifier"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("persona",), "admin-bootstrap", "persona.*etl-developer"),
        (("api_target_hmac_sha256",), "sha256:plain", "API target HMAC"),
        (("principal_hmac_sha256",), "hmac-sha256:ABC", "principal HMAC"),
        (
            ("image_ref",),
            "apache/dolphinscheduler-api:3.4.1",
            "image reference.*3.4.2",
        ),
        (("image_id",), "local-image-id", "image ID.*sha256"),
        (("image_source",), "", "image source"),
        (("image_observed_at",), "yesterday", "observation timestamp"),
    ],
)
def test_projection_rejects_untrusted_cluster_identity(
    tmp_path: Path,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs["cluster_manifest"], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            (
                (
                    "fixture_manifest",
                    ("image_ref",),
                    "mirror/dolphinscheduler-api:3.4.2",
                ),
            ),
            "same image reference and ID",
        ),
        (
            (
                (
                    "fixture_manifest",
                    ("image_id",),
                    "sha256:" + "e" * 64,
                ),
            ),
            "same image reference and ID",
        ),
        (
            (("fixture_manifest", ("provisioner",), "manual-bootstrap"),),
            "dsmatrix-exact-read/v1",
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
                    ("image_id",),
                    "sha256:" + "e" * 64,
                ),
            ),
            "cluster image reference and ID",
        ),
        (
            (
                (
                    "image_inspection",
                    ("image_ref",),
                    "mirror/dolphinscheduler-api:3.4.2",
                ),
            ),
            "cluster image reference and ID",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    [IMAGE_DIGEST, "apache/dolphinscheduler-api@sha256:" + "e" * 64],
                ),
            ),
            "one selected service RepoDigest",
        ),
        (
            (("image_inspection", ("repo_digests",), [IMAGE_DIGEST, IMAGE_DIGEST]),),
            "RepoDigests must be unique",
        ),
        (
            (
                (
                    "image_inspection",
                    ("repo_digests",),
                    ["mirror/dolphinscheduler-api@sha256:" + "d" * 64],
                ),
                (
                    "image_inspection",
                    ("selected_repo_digest",),
                    "mirror/dolphinscheduler-api@sha256:" + "d" * 64,
                ),
            ),
            "one selected service RepoDigest",
        ),
    ],
)
def test_projection_binds_inspected_image_identity(
    tmp_path: Path,
    changes: Sequence[tuple[str, Sequence[str], object]],
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    for input_name, field_path, value in changes:
        _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises(ValueError, match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("input_name", "field_path", "value", "message"),
    [
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
            ("workflow", "scheduled"),
            False,
            "attached schedule",
        ),
        (
            "fixture_manifest",
            ("workflow", "release_state"),
            "ONLINE",
            "workflow.*OFFLINE",
        ),
        (
            "state_file",
            ("resources", "project", "code"),
            7002,
            "project code",
        ),
        (
            "state_file",
            ("resources", "workflow", "code"),
            8002,
            "workflow code",
        ),
        (
            "state_file",
            ("resources", "schedule", "id"),
            1002,
            "schedule ID",
        ),
        (
            "state_file",
            ("resources", "schedule", "workflow_code"),
            8002,
            "schedule workflow code",
        ),
        (
            "state_file",
            ("resources", "workflow", "release_state"),
            "ONLINE",
            "workflow.*OFFLINE",
        ),
        (
            "state_file",
            ("resources", "schedule", "release_state"),
            "ONLINE",
            "schedule.*OFFLINE",
        ),
        (
            "state_file",
            ("resources", "workflow", "task_code"),
            0,
            "task code",
        ),
        (
            "state_file",
            ("names", "task"),
            "",
            "task name",
        ),
        (
            "state_file",
            ("names", "project"),
            "different-project",
            "project name",
        ),
        (
            "state_file",
            ("names", "workflow"),
            "different-workflow",
            "workflow name",
        ),
    ],
)
def test_projection_requires_exact_fixture_resources(
    tmp_path: Path,
    input_name: str,
    field_path: Sequence[str],
    value: object,
    message: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(inputs[input_name], field_path, value)

    with pytest.raises((TypeError, ValueError), match=message):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize(
    ("input_name", "field_path", "drift_kind"),
    [
        ("fixture_manifest", ("project", "unexpected"), "extra"),
        ("fixture_manifest", ("workflow", "identity", "unexpected"), "extra"),
        ("state_file", ("names", "unexpected"), "extra"),
        ("state_file", ("resources", "workflow", "unexpected"), "extra"),
        ("state_file", ("resources", "workflow", "task_code"), "remove"),
    ],
)
def test_projection_rejects_nested_shape_drift_without_key_errors(
    tmp_path: Path,
    input_name: str,
    field_path: Sequence[str],
    drift_kind: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    if drift_kind == "remove":
        _remove_json_value(inputs[input_name], field_path)
    else:
        _replace_json_value(inputs[input_name], field_path, "drift")

    with pytest.raises(ValueError, match="invalid fields"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=tmp_path / "cluster-v2.json",
            fixture_output=tmp_path / "fixture-v2.json",
            ds_version="3.4.2",
        )


@pytest.mark.parametrize("existing_output", ["cluster", "fixture"])
def test_projection_preflights_both_outputs_before_publishing(
    tmp_path: Path,
    existing_output: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"
    conflict = cluster_output if existing_output == "cluster" else fixture_output
    conflict.parent.mkdir(mode=0o700)
    conflict.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
            ds_version="3.4.2",
        )

    assert conflict.read_text(encoding="utf-8") == "existing"
    untouched = fixture_output if existing_output == "cluster" else cluster_output
    assert not untouched.exists()


def test_projection_rolls_back_only_its_first_publish_on_a_second_link_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _load_module()
    private_io = _load_private_io()
    inputs = _valid_inputs(tmp_path)
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
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
            ds_version="3.4.2",
        )

    assert not cluster_output.exists()
    assert fixture_output.read_text(encoding="utf-8") == "racer-owned"


def test_projection_rolls_back_the_first_publish_when_interrupted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _load_module()
    private_io = _load_private_io()
    inputs = _valid_inputs(tmp_path)
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
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=cluster_output,
            fixture_output=fixture_output,
            ds_version="3.4.2",
        )

    assert not cluster_output.exists()
    assert not fixture_output.exists()


def test_projection_refuses_an_output_symlink_without_creating_its_target(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    output_dir = tmp_path / "output"
    output_dir.mkdir(mode=0o700)
    cluster_output = output_dir / "cluster.json"
    target = output_dir / "target.json"
    cluster_output.symlink_to(target)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=cluster_output,
            fixture_output=output_dir / "fixture.json",
            ds_version="3.4.2",
        )

    assert cluster_output.is_symlink()
    assert not target.exists()
    assert not (output_dir / "fixture.json").exists()


def test_projection_pins_the_private_parent_during_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _load_module()
    private_io = _load_private_io()
    inputs = _valid_inputs(tmp_path)
    output_dir = tmp_path / "output"
    detached_dir = tmp_path / "detached-output"
    real_token_hex = private_io.secrets.token_hex
    swapped = False

    def swap_parent_before_staging(size: int) -> str:
        nonlocal swapped
        if not swapped:
            swapped = True
            output_dir.rename(detached_dir)
            output_dir.mkdir(mode=0o700)
        return str(real_token_hex(size))

    monkeypatch.setattr(private_io.secrets, "token_hex", swap_parent_before_staging)

    with pytest.raises(RuntimeError, match="parent changed"):
        projection.project_exact_profile_matrix_fixture(
            **inputs,
            cluster_output=output_dir / "cluster.json",
            fixture_output=output_dir / "fixture.json",
            ds_version="3.4.2",
        )

    assert not (output_dir / "cluster.json").exists()
    assert not (output_dir / "fixture.json").exists()
    assert not (detached_dir / "cluster.json").exists()
    assert not (detached_dir / "fixture.json").exists()


def test_projection_creates_private_output_parent_and_rejects_one_destination(
    tmp_path: Path,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    output = tmp_path / "private" / "manifest.json"

    with pytest.raises(ValueError, match="different output paths"):
        projection.project_exact_profile_matrix_fixture(
            **inputs, cluster_output=output, fixture_output=output, ds_version="3.4.2"
        )

    assert not output.exists()
    cluster_output = output.with_name("cluster.json")
    fixture_output = output.with_name("fixture.json")
    projection.project_exact_profile_matrix_fixture(
        **inputs,
        cluster_output=cluster_output,
        fixture_output=fixture_output,
        ds_version="3.4.2",
    )
    assert stat.S_IMODE(output.parent.stat().st_mode) == 0o700


def test_projection_cli_does_not_print_fixture_or_secret_values(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    cluster_output = tmp_path / "output" / "cluster.json"
    fixture_output = tmp_path / "output" / "fixture.json"

    exit_code = projection.main(
        [
            "--version",
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
            str(cluster_output),
            "--fixture-output",
            str(fixture_output),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Projected private exact 3.4.2 gate manifests." in captured.out
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


def test_projection_cli_rejects_a_bare_image_digest(tmp_path: Path) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)

    with pytest.raises(SystemExit):
        projection._parse_args(
            [
                "--version",
                "3.4.2",
                "--cluster-manifest",
                str(inputs["cluster_manifest"]),
                "--fixture-manifest",
                str(inputs["fixture_manifest"]),
                "--state-file",
                str(inputs["state_file"]),
                "--image-digest",
                IMAGE_DIGEST,
                "--cluster-output",
                str(tmp_path / "cluster-v2.json"),
                "--fixture-output",
                str(tmp_path / "fixture-v2.json"),
            ]
        )


def test_projection_cli_reports_sanitized_validation_errors_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    _replace_json_value(
        inputs["state_file"],
        ("phase",),
        "do-not-publish-this-password",
    )

    exit_code = projection.main(
        [
            "--version",
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
    assert "Matrix state phase must be ready" in captured.err
    assert "Traceback" not in captured.err
    assert "do-not-publish-this-password" not in captured.err


def test_projection_cli_does_not_resolve_an_input_symlink(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    cluster_link = inputs["cluster_manifest"]
    target = cluster_link.with_suffix(".target.json")
    cluster_link.rename(target)
    cluster_link.symlink_to(target)

    exit_code = projection.main(
        [
            "--version",
            "3.4.2",
            "--cluster-manifest",
            str(cluster_link),
            "--fixture-manifest",
            str(inputs["fixture_manifest"]),
            "--state-file",
            str(inputs["state_file"]),
            "--image-inspection",
            str(inputs["image_inspection"]),
            "--cluster-output",
            str(tmp_path / "cluster-v2.json"),
            "--fixture-output",
            str(tmp_path / "fixture-v2.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "owner-private regular file" in captured.err
    assert not (tmp_path / "cluster-v2.json").exists()
    assert not (tmp_path / "fixture-v2.json").exists()


@pytest.mark.parametrize("input_name", ["image_inspection", "state_file"])
def test_projection_cli_does_not_echo_untrusted_json_key_names(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    input_name: str,
) -> None:
    projection = _load_module()
    inputs = _valid_inputs(tmp_path)
    protected_key = "DS_API_TOKEN=do-not-echo-this"
    _replace_json_value(inputs[input_name], (protected_key,), True)

    exit_code = projection.main(
        [
            "--version",
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
            str(tmp_path / "cluster-v2.json"),
            "--fixture-output",
            str(tmp_path / "fixture-v2.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "invalid fields" in captured.err
    assert protected_key not in captured.err


def _valid_inputs(tmp_path: Path) -> dict[str, Path]:
    cluster_manifest = tmp_path / "cluster-v1.json"
    fixture_manifest = tmp_path / "fixture-v1.json"
    state_file = tmp_path / "state.json"
    image_inspection = tmp_path / "image-inspection.json"
    _write_private(
        cluster_manifest,
        {
            "api_target_hmac_sha256": "hmac-sha256:" + "a" * 64,
            "ds_version": "3.4.2",
            "image_id": "sha256:" + "c" * 64,
            "image_observed_at": "2026-08-10T01:02:03Z",
            "image_ref": "apache/dolphinscheduler-api:3.4.2",
            "image_source": "swarm task plus node-local Docker inspection",
            "persona": "etl-developer",
            "principal_hmac_sha256": "hmac-sha256:" + "b" * 64,
            "schema_version": 1,
        },
    )
    _write_private(
        fixture_manifest,
        {
            "ds_version": "3.4.2",
            "image_id": "sha256:" + "c" * 64,
            "image_ref": "apache/dolphinscheduler-api:3.4.2",
            "project": {
                "identity": {"kind": "code", "value": 7001},
                "name": "fixture-project",
            },
            "provisioner": "dsmatrix-exact-read/v1",
            "schema_version": 1,
            "workflow": {
                "identity": {"kind": "code", "value": 8001},
                "name": "fixture-workflow",
                "release_state": "OFFLINE",
                "schedule_id": 1001,
                "scheduled": True,
            },
        },
    )
    _write_private(
        state_file,
        {
            "campaign_id": "promotion-schema6-20260810",
            "email": "fixture@example.invalid",
            "names": {
                "project": "fixture-project",
                "task": "fixture-shell-task",
                "tenant": "fixture-tenant",
                "user": "fixture-user",
                "workflow": "fixture-workflow",
            },
            "ownership_marker": (
                "dsmatrix-exact-read/v1/3.4.2/promotion-schema6-20260810/0123456789ab"
            ),
            "pending": None,
            "phase": "ready",
            "resources": {
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
            },
            "schema_version": 1,
            "user_password": "do-not-publish-this-password",
            "version": "3.4.2",
        },
    )
    _write_private(
        image_inspection,
        {
            "image_id": "sha256:" + "c" * 64,
            "image_ref": "apache/dolphinscheduler-api:3.4.2",
            "repo_digests": [IMAGE_DIGEST],
            "schema_version": 1,
            "selected_repo_digest": IMAGE_DIGEST,
        },
    )
    return {
        "cluster_manifest": cluster_manifest,
        "fixture_manifest": fixture_manifest,
        "image_inspection": image_inspection,
        "state_file": state_file,
    }


def _write_private(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)


def _write_current_schema_2_image_inspection(
    path: Path,
    **changes: object,
) -> None:
    payload: dict[str, object] = {
        "schema_version": 2,
        "kind": "registry-digest/v1",
        "image_ref": "apache/dolphinscheduler-api:3.4.2",
        "image_id": "sha256:" + "c" * 64,
        "repo_digests": [IMAGE_DIGEST],
        "selected_repo_digest": IMAGE_DIGEST,
    }
    payload.update(changes)
    _write_private(path, payload)


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


def _remove_json_value(path: Path, field_path: Sequence[str]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    target = payload
    for field in field_path[:-1]:
        target = target[field]
    del target[field_path[-1]]
    _write_private(path, payload)
