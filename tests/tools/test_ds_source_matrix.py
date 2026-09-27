from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

_EXPECTED_SOURCE_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
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


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_default_source_matrix_covers_every_exact_target_once() -> None:
    matrix = _load_module("ds_codegen.source_matrix")
    repo_root = Path(__file__).resolve().parents[2]

    specs = matrix.load_exact_source_specs(repo_root)

    assert tuple(spec.version for spec in specs) == _EXPECTED_SOURCE_VERSIONS
    assert len({spec.version for spec in specs}) == len(_EXPECTED_SOURCE_VERSIONS)
    assert tuple(spec.source_root for spec in specs) == tuple(
        repo_root / "build" / "upstream" / f"ds-{version}"
        for version in _EXPECTED_SOURCE_VERSIONS
    )


def test_source_matrix_accepts_an_independent_candidate_plan(tmp_path: Path) -> None:
    matrix = _load_module("ds_codegen.source_matrix")
    manifest = tmp_path / "exact_sources.json"
    _write_manifest(
        manifest,
        ("9.9.9",),
    )

    specs = matrix.load_exact_source_specs(tmp_path, manifest)
    assert tuple(spec.version for spec in specs) == ("9.9.9",)


def test_task_plugin_inventory_accepts_the_exact_source_matrix(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    tool = _load_module("generate_ds_task_plugin_inventory")
    manifest = tmp_path / "exact_sources.json"
    _write_manifest(manifest, _EXPECTED_SOURCE_VERSIONS)
    output = tmp_path / "inventory.json"
    observed: list[Any] = []

    def generate(inputs: list[Any], **_: object) -> dict[str, object]:
        observed.extend(inputs)
        return {
            "complete": True,
            "source_complete": True,
            "targets": [],
            "diagnostics": [],
        }

    monkeypatch.setattr(tool, "generate_task_plugin_inventory", generate)

    exit_code = tool.main(
        [
            "--repo-root",
            str(tmp_path),
            "--source-manifest",
            str(manifest),
            "--output",
            str(output),
        ]
    )

    assert exit_code == 0
    assert tuple(item.label for item in observed) == _EXPECTED_SOURCE_VERSIONS
    assert {item.kind for item in observed} == {"ds-source"}
    assert tuple(item.path for item in observed) == tuple(
        tmp_path / "build" / "upstream" / f"ds-{version}"
        for version in _EXPECTED_SOURCE_VERSIONS
    )


def test_prepare_source_matrix_creates_clean_exact_checkouts(tmp_path: Path) -> None:
    matrix = _load_module("ds_codegen.source_matrix")
    contract_inputs = _load_module("ds_codegen.contract_inputs")
    prepare = _load_module("prepare_ds_source_matrix")
    remote = _tagged_remote(tmp_path, _EXPECTED_SOURCE_VERSIONS)
    repo_root = tmp_path / "consumer"
    repo_root.mkdir()
    manifest = repo_root / "exact_sources.json"
    _write_manifest(manifest, _EXPECTED_SOURCE_VERSIONS)

    result = prepare.main(
        [
            "--repo-root",
            str(repo_root),
            "--manifest",
            str(manifest),
            "--remote",
            remote.as_uri(),
        ]
    )

    assert result == 0
    specs = matrix.load_exact_source_specs(repo_root, manifest)
    identities = [
        contract_inputs.require_exact_git_source_identity(
            spec.source_root,
            label=spec.version,
        )
        for spec in specs
    ]
    assert tuple(identity.tag for identity in identities) == _EXPECTED_SOURCE_VERSIONS

    assert (
        prepare.main(
            [
                "--repo-root",
                str(repo_root),
                "--manifest",
                str(manifest),
                "--remote",
                remote.as_uri(),
            ]
        )
        == 0
    )


def _write_manifest(path: Path, versions: tuple[str, ...]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "dolphinscheduler-exact-source-matrix",
                "targets": [
                    {
                        "version": version,
                        "source_root": f"build/upstream/ds-{version}",
                    }
                    for version in versions
                ],
            }
        ),
        encoding="utf-8",
    )


def _tagged_remote(tmp_path: Path, versions: tuple[str, ...]) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.email", "test@example.com")
    _git(source, "config", "user.name", "Test")
    tracked = source / "pom.xml"
    for version in versions:
        tracked.write_text(f"{version}\n", encoding="utf-8")
        _git(source, "add", "pom.xml")
        _git(source, "commit", "-m", version)
        _git(source, "tag", version)
    remote = tmp_path / "remote.git"
    _git(tmp_path, "clone", "--bare", str(source), str(remote))
    return remote


def _git(cwd: Path, *args: str) -> str:
    executable = shutil.which("git")
    assert executable is not None
    completed = subprocess.run(  # noqa: S603
        [executable, *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
