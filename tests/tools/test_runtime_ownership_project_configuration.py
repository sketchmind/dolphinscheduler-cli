from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from ds_codegen.compiled_wire_artifacts import _ARTIFACT_DIGEST_DOMAIN
from live_gate.exact_profile_read_corpus import load_tracked_artifacts
from live_gate.runtime_ownership import load_current_runtime_ownership

_ROOT = Path(__file__).resolve().parents[2]


def test_static_ownership_accepts_project_configuration_and_exact_dependencies() -> (
    None
):
    ownership = load_current_runtime_ownership(
        _ROOT, contracts=load_tracked_artifacts(_ROOT).contracts
    )
    assert len(ownership) == 37
    assert {"schedule.create", "schedule.explain"} <= ownership["1.3.9"]
    assert "project-preference.read" not in ownership["1.3.9"]
    assert "project-worker-group.page" not in ownership["3.2.0"]
    for version in ("3.4.2", "3.4.3"):
        assert {
            "project-parameter.get",
            "project-preference.get",
            "project-preference.read",
            "project-worker-group.page",
            "schedule.create",
            "schedule.explain",
        } <= ownership[version]
    assert "user.identity" not in ownership["3.4.2"]
    assert "user.identity" in ownership["3.4.3"]


@pytest.mark.parametrize(
    ("projection", "message"),
    [
        ("direct", "program digest"),
        ("single_data", "program digest"),
        ("single_data_list", "response projection is unsupported"),
        (None, "response projection is unsupported"),
        (["status_data"], "response projection is unsupported"),
    ],
)
def test_static_ownership_rejects_projection_drift_after_outer_artifact_rebinding(
    tmp_path: Path, projection: object, message: str
) -> None:
    generated = tmp_path / "src" / "dsctl" / "generated"
    for name in ("wire_runtime", "wire_programs"):
        shutil.copytree(
            _ROOT / "src" / "dsctl" / "generated" / name,
            generated / name,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    compiled = generated / "wire_programs"
    path = compiled / "project_worker_group.py"
    source = path.read_text(encoding="utf-8")
    original = "'response_projection': 'status_data'"
    assert source.count(original) == 1
    path.write_text(
        source.replace(original, f"'response_projection': {projection!r}", 1),
        encoding="utf-8",
    )
    # Rebind the outer bytes so the independent codec/program guard is exercised.
    digest = hashlib.sha256(_ARTIFACT_DIGEST_DOMAIN)
    for module in sorted(compiled.rglob("*.py")):
        if module.name == "_manifest.py":
            continue
        digest.update(module.relative_to(compiled).as_posix().encode())
        digest.update(b"\0")
        digest.update(module.read_bytes())
        digest.update(b"\0")
    manifest = compiled / "_manifest.py"
    lines = manifest.read_text(encoding="utf-8").splitlines(keepends=True)
    manifest.write_text(
        "".join(
            f'CONTENT_DIGEST = "sha256:{digest.hexdigest()}"\n'
            if line.startswith("CONTENT_DIGEST = ")
            else line
            for line in lines
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_current_runtime_ownership(
            tmp_path, contracts=load_tracked_artifacts(_ROOT).contracts
        )
