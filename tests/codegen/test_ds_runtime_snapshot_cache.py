from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

# These tests intentionally provide only source identity plus synthetic IR.
# Public API source validation is independently exercised by discovery tests.
pytestmark = pytest.mark.usefixtures("stub_discovery_profiles")


def _runtime_bundles() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.runtime_bundles")


def _contract_inputs() -> Any:
    _runtime_bundles()
    return importlib.import_module("ds_codegen.contract_inputs")


def test_exact_snapshot_cache_hit_skips_source_extraction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _snapshot("3.4.1")
    snapshot_dir = tmp_path / "snapshots"
    snapshot_path = snapshot_dir / "ds-3.4.1-contract.json"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_path,
        provenance=contract_inputs.build_source_provenance(
            source_root,
            label="3.4.1",
            snapshot=snapshot,
        ),
    )
    original_loader = runtime_bundles.load_contract_input

    def load_without_source(item: Any, *, require_exact: bool) -> Any:
        if item.kind == "ds-source":
            message = "exact cache hit must not extract Java source"
            raise AssertionError(message)
        return original_loader(item, require_exact=require_exact)

    monkeypatch.setattr(
        runtime_bundles,
        "load_contract_input",
        load_without_source,
    )

    bundles = runtime_bundles.load_runtime_bundles(
        tmp_path,
        manifest,
        snapshot_dir=snapshot_dir,
        snapshot_mode="prefer",
    )

    assert len(bundles) == 1
    assert bundles[0].input_kind == "snapshot"
    assert bundles[0].snapshot_fallback_reason is None
    assert bundles[0].metadata.source_tree == _git(
        source_root,
        "rev-parse",
        "HEAD^{tree}",
    )


@pytest.mark.parametrize(
    ("drift", "reason"),
    [
        pytest.param("extractor", "extractor fingerprint", id="extractor-drift"),
        pytest.param("source", "source identity", id="source-drift"),
    ],
)
def test_preferred_snapshot_drift_falls_back_to_source_and_refreshes_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
    reason: str,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _snapshot("3.4.1")
    source_provenance = contract_inputs.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    cached_provenance = json.loads(json.dumps(source_provenance))
    if drift == "extractor":
        cached_provenance["extractor_fingerprint"] = "sha256:" + "0" * 64
    else:
        cached_provenance["origin"]["git"]["commit"] = "a" * 40
    snapshot_dir = tmp_path / "snapshots"
    snapshot_path = snapshot_dir / "ds-3.4.1-contract.json"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_path,
        provenance=cached_provenance,
    )
    original_loader = runtime_bundles.load_contract_input
    source_loads: list[str] = []

    def load_with_stubbed_source(item: Any, *, require_exact: bool) -> Any:
        if item.kind == "snapshot":
            return original_loader(item, require_exact=require_exact)
        source_loads.append(item.label)
        return contract_inputs.LoadedContract(
            label=item.label,
            snapshot=snapshot,
            provenance=source_provenance,
        )

    monkeypatch.setattr(
        runtime_bundles,
        "load_contract_input",
        load_with_stubbed_source,
    )

    bundles = runtime_bundles.load_runtime_bundles(
        tmp_path,
        manifest,
        snapshot_dir=snapshot_dir,
        snapshot_mode="prefer",
    )

    assert source_loads == ["3.4.1"]
    assert bundles[0].input_kind == "source"
    assert reason in bundles[0].snapshot_fallback_reason
    refreshed = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert refreshed["provenance"]["extractor_fingerprint"] == (
        contract_inputs.contract_extractor_fingerprint()
    )
    assert refreshed["provenance"]["origin"]["git"]["commit"] == _git(
        source_root,
        "rev-parse",
        "HEAD^{commit}",
    )


def test_required_snapshot_drift_fails_without_source_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _snapshot("3.4.1")
    provenance = contract_inputs.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    provenance["extractor_fingerprint"] = "sha256:" + "0" * 64
    snapshot_dir = tmp_path / "snapshots"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_dir / "ds-3.4.1-contract.json",
        provenance=provenance,
    )
    original_loader = runtime_bundles.load_contract_input

    def load_without_source(item: Any, *, require_exact: bool) -> Any:
        if item.kind == "ds-source":
            message = "required snapshot mode must fail closed"
            raise AssertionError(message)
        return original_loader(item, require_exact=require_exact)

    monkeypatch.setattr(
        runtime_bundles,
        "load_contract_input",
        load_without_source,
    )

    with pytest.raises(
        runtime_bundles.RuntimeSnapshotCacheError,
        match="extractor fingerprint",
    ):
        runtime_bundles.load_runtime_bundles(
            tmp_path,
            manifest,
            snapshot_dir=snapshot_dir,
            snapshot_mode="require",
        )


def test_snapshot_compile_failure_is_a_preferred_cache_miss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _snapshot("3.4.1")
    provenance = contract_inputs.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    snapshot_dir = tmp_path / "snapshots"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_dir / "ds-3.4.1-contract.json",
        provenance=provenance,
    )
    original_loader = runtime_bundles.load_contract_input
    original_compiler = runtime_bundles.compile_runtime_bundle

    def load_with_stubbed_source(item: Any, *, require_exact: bool) -> Any:
        if item.kind == "snapshot":
            return original_loader(item, require_exact=require_exact)
        return contract_inputs.LoadedContract(
            label=item.label,
            snapshot=snapshot,
            provenance=provenance,
        )

    def compile_with_stale_snapshot(spec: Any, loaded: Any) -> Any:
        if loaded.provenance["input_kind"] == "snapshot":
            message = "runtime contract slice is incomplete"
            raise ValueError(message)
        return original_compiler(spec, loaded)

    monkeypatch.setattr(
        runtime_bundles,
        "load_contract_input",
        load_with_stubbed_source,
    )
    monkeypatch.setattr(
        runtime_bundles,
        "compile_runtime_bundle",
        compile_with_stale_snapshot,
    )

    bundles = runtime_bundles.load_runtime_bundles(
        tmp_path,
        manifest,
        snapshot_dir=snapshot_dir,
        snapshot_mode="prefer",
    )

    assert bundles[0].input_kind == "source"
    assert "runtime contract slice is incomplete" in (
        bundles[0].snapshot_fallback_reason
    )


def test_retired_resolution_table_requires_or_refreshes_exact_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _qualified_snapshot("3.4.1")
    provenance = contract_inputs.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    snapshot_dir = tmp_path / "snapshots"
    snapshot_path = snapshot_dir / "ds-3.4.1-contract.json"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_path,
        provenance=provenance,
    )
    legacy_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    legacy_payload["reference_resolutions"] = [
        {
            "import_path": "example.first.Thing",
            "owner_kind": "operation_response",
            "owner_ref": "ThingController.queryThing",
            "reference_name": "Thing",
        }
    ]
    snapshot_path.write_text(json.dumps(legacy_payload), encoding="utf-8")
    original_loader = runtime_bundles.load_contract_input
    source_loads: list[str] = []

    def load_with_exact_source(item: Any, *, require_exact: bool) -> Any:
        if item.kind == "snapshot":
            return original_loader(item, require_exact=require_exact)
        source_loads.append(item.label)
        return contract_inputs.LoadedContract(
            label=item.label,
            snapshot=snapshot,
            provenance=provenance,
        )

    monkeypatch.setattr(
        runtime_bundles,
        "load_contract_input",
        load_with_exact_source,
    )

    with pytest.raises(
        runtime_bundles.RuntimeSnapshotCacheError,
        match=r"retired reference_resolutions side table.*refresh it from exact source",
    ):
        runtime_bundles.load_runtime_bundles(
            tmp_path,
            manifest,
            snapshot_dir=snapshot_dir,
            snapshot_mode="require",
        )
    assert source_loads == []

    bundles = runtime_bundles.load_runtime_bundles(
        tmp_path,
        manifest,
        snapshot_dir=snapshot_dir,
        snapshot_mode="prefer",
    )

    assert source_loads == ["3.4.1"]
    assert bundles[0].input_kind == "source"
    assert "retired reference_resolutions side table" in (
        bundles[0].snapshot_fallback_reason
    )
    refreshed = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert "reference_resolutions" not in refreshed


def test_source_mode_ignores_an_exact_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _runtime_bundles()
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_manifest(tmp_path, source_root=source_root)
    snapshot = _snapshot("3.4.1")
    provenance = contract_inputs.build_source_provenance(
        source_root,
        label="3.4.1",
        snapshot=snapshot,
    )
    snapshot_dir = tmp_path / "snapshots"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        snapshot_dir / "ds-3.4.1-contract.json",
        provenance=provenance,
    )
    input_kinds: list[str] = []

    def load_source_only(item: Any, *, require_exact: bool) -> Any:
        input_kinds.append(item.kind)
        assert require_exact is True
        return contract_inputs.LoadedContract(
            label=item.label,
            snapshot=snapshot,
            provenance=provenance,
        )

    monkeypatch.setattr(runtime_bundles, "load_contract_input", load_source_only)

    bundles = runtime_bundles.load_runtime_bundles(
        tmp_path,
        manifest,
        snapshot_dir=snapshot_dir,
        snapshot_mode="source",
    )

    assert input_kinds == ["ds-source"]
    assert bundles[0].input_kind == "source"
    assert bundles[0].snapshot_fallback_reason is None


def test_extractor_fingerprint_survives_snapshot_round_trip(tmp_path: Path) -> None:
    contract_inputs = _contract_inputs()
    source_root = _tagged_source_tree(tmp_path, version="9.9.9")
    snapshot = _snapshot("9.9.9")
    provenance = contract_inputs.build_source_provenance(
        source_root,
        label="9.9.9",
        snapshot=snapshot,
    )
    path = tmp_path / "snapshot.json"
    contract_inputs.write_contract_snapshot_document(
        snapshot,
        path,
        provenance=provenance,
    )

    loaded = contract_inputs.load_contract_input(
        contract_inputs.ContractInput("9.9.9", path, "snapshot"),
        require_exact=True,
    )

    assert loaded.provenance["extractor_fingerprint"] == (
        contract_inputs.contract_extractor_fingerprint()
    )


def _snapshot(version: str) -> Any:
    ir = importlib.import_module("ds_codegen.ir")
    return ir.ContractSnapshot(
        ds_version=version,
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )


def _qualified_snapshot(version: str) -> Any:
    ir = importlib.import_module("ds_codegen.ir")
    operation = ir.OperationSpec(
        operation_id="ThingController.queryThing",
        controller="ThingController",
        method_name="queryThing",
        api_group="v1",
        http_method="GET",
        path="things",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Thing",
        inferred_return_type="example.first.Thing",
        logical_return_type="example.first.Thing",
        response_projection="direct",
        parameters=[],
    )
    models = [
        ir.ModelSpec(
            name="Thing",
            import_path=import_path,
            kind="other_class",
            documentation=None,
            extends=None,
            fields=[],
        )
        for import_path in ("example.first.Thing", "example.second.Thing")
    ]
    return ir.ContractSnapshot(
        ds_version=version,
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=2,
        operations=[operation],
        enums=[],
        dtos=[],
        models=models,
    )


def _write_manifest(repo_root: Path, *, source_root: Path) -> Path:
    # Exercise caching after real admission preflight using a reviewed version.
    manifest = repo_root / "runtime_bundles.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": source_root.relative_to(repo_root).as_posix(),
                        "selection": "full",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def _tagged_source_tree(tmp_path: Path, *, version: str) -> Path:
    source_root = tmp_path / f"source-{version}"
    source_root.mkdir()
    (source_root / "pom.xml").write_text(
        "<project><modelVersion>4.0.0</modelVersion>"
        f"<groupId>test</groupId><artifactId>ds</artifactId><version>{version}</version>"
        "</project>",
        encoding="utf-8",
    )
    _git(source_root, "init")
    _git(source_root, "config", "user.email", "test@example.com")
    _git(source_root, "config", "user.name", "Test")
    _git(source_root, "add", "pom.xml")
    _git(source_root, "commit", "-m", "source")
    _git(source_root, "tag", version)
    return source_root


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
