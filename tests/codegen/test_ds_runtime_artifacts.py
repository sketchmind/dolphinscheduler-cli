from __future__ import annotations

import hashlib
import importlib
import json
import runpy
import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from ds_codegen.runtime_bundles import RuntimeBundle

_FINGERPRINTS = {
    "source": "sha256:" + "1" * 64,
    "effective_wire": "sha256:" + "2" * 64,
    "consumed_projection": "sha256:" + "3" * 64,
    "preservation": "sha256:" + "4" * 64,
}


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_recipe_digest_aggregates_runtime_and_auxiliary_consumers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    compatibility = _module("ds_codegen.compatibility_impact")
    shared = "WidgetController.get"
    runtime_bindings = {
        "widget.get": _binding(compatibility, (shared,), type_key="Widget"),
        "dashboard.get": _binding(
            compatibility,
            ("DashboardController.get", shared),
            type_key="Dashboard",
        ),
    }
    auxiliary_bindings = {
        "widget.cleanup": _binding(
            compatibility,
            (shared, "WidgetController.delete"),
            type_key="Cleanup",
        )
    }
    monkeypatch.setattr(
        artifacts,
        "runtime_operation_bindings",
        lambda _version: runtime_bindings,
    )
    monkeypatch.setattr(
        artifacts,
        "runtime_auxiliary_operation_bindings",
        lambda _version: auxiliary_bindings,
    )
    profile_data = _profile_data(
        ("3.4.1", "3.4.2"),
        runtime_bindings,
    )

    records = artifacts._consumer_records(
        "3.4.1",
        profile_data=profile_data,
    )
    first = artifacts.recipe_semantic_digest(
        "3.4.1",
        profile_data=profile_data,
    )
    second = artifacts.recipe_semantic_digest(
        "3.4.2",
        profile_data=profile_data,
    )

    assert [record["semantic_operation"] for record in records] == [
        "widget.cleanup",
        "dashboard.get",
        "widget.get",
    ]
    assert records[0]["kind"] == "auxiliary"
    assert records[0]["profile_fingerprints"] is None
    assert all(
        set(record["profile_fingerprints"]) == set(_FINGERPRINTS)
        for record in records[1:]
    )
    assert first != second

    changed = json.loads(json.dumps(profile_data))
    changed["profiles"]["3.4.2"]["build_decisions"]["dashboard.get"]["fingerprints"][
        "preservation"
    ] = "sha256:" + "9" * 64
    assert (
        artifacts.recipe_semantic_digest(
            "3.4.2",
            profile_data=changed,
        )
        != second
    )


def test_recipe_digest_includes_every_recipe_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    compatibility = _module("ds_codegen.compatibility_impact")
    bindings = {
        "widget.get": _binding(
            compatibility,
            ("WidgetController.get",),
            type_key="Widget",
        ),
        "other.get": _binding(
            compatibility,
            ("OtherController.get",),
            type_key="Other",
        ),
    }
    monkeypatch.setattr(artifacts, "runtime_operation_bindings", lambda _v: bindings)
    monkeypatch.setattr(
        artifacts,
        "runtime_auxiliary_operation_bindings",
        lambda _v: {},
    )
    profile_data = _profile_data(("3.4.1",), bindings)
    original = artifacts.recipe_semantic_digest(
        "3.4.1",
        profile_data=profile_data,
    )
    profile_data["profiles"]["3.4.1"]["build_decisions"]["other.get"]["fingerprints"][
        "source"
    ] = "sha256:" + "9" * 64

    assert (
        artifacts.recipe_semantic_digest(
            "3.4.1",
            profile_data=profile_data,
        )
        != original
    )


def test_runtime_consumer_without_four_axis_profile_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    compatibility = _module("ds_codegen.compatibility_impact")
    bindings = {
        "widget.get": _binding(
            compatibility,
            ("WidgetController.get",),
            type_key="Widget",
        )
    }
    monkeypatch.setattr(artifacts, "runtime_operation_bindings", lambda _v: bindings)
    monkeypatch.setattr(
        artifacts,
        "runtime_auxiliary_operation_bindings",
        lambda _v: {},
    )
    profile_data: dict[str, object] = {"profiles": {"3.4.1": {"build_decisions": {}}}}

    with pytest.raises(TypeError, match=r"widget\.get must be an object"):
        artifacts.recipe_semantic_digest(
            "3.4.1",
            profile_data=profile_data,
        )


def test_profile_and_recipe_digests_have_separate_domains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    compatibility = _module("ds_codegen.compatibility_impact")
    bindings = {
        "widget.get": _binding(
            compatibility,
            ("WidgetController.get",),
            type_key="Widget",
        )
    }
    monkeypatch.setattr(artifacts, "runtime_operation_bindings", lambda _v: bindings)
    monkeypatch.setattr(
        artifacts,
        "runtime_auxiliary_operation_bindings",
        lambda _v: {},
    )
    profile_data = _profile_data(("3.4.1",), bindings)
    profile = profile_data["profiles"]["3.4.1"]

    profile_digest = artifacts.profile_semantic_digest(profile)
    recipe_digest = artifacts.recipe_semantic_digest(
        "3.4.1",
        profile_data=profile_data,
    )
    profile["documentation"] = "non-recipe profile fact"

    assert artifacts.profile_semantic_digest(profile) != profile_digest
    assert (
        artifacts.recipe_semantic_digest("3.4.1", profile_data=profile_data)
        == recipe_digest
    )


def test_binding_digest_matches_loader_v4_canonical_record() -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    wire_runtime_digest = "sha256:" + "1" * 64
    profile_digest = "sha256:" + "2" * 64
    recipe_digest = "sha256:" + "3" * 64

    observed = artifacts.binding_digest(
        "3.4.1",
        wire_runtime_content_digest=wire_runtime_digest,
        profile_digest=profile_digest,
        recipe_digest=recipe_digest,
    )
    encoded = json.dumps(
        {
            "schema_version": 4,
            "renderer_abi": 3,
            "ds_version": "3.4.1",
            "wire_runtime_content_digest": wire_runtime_digest,
            "profile_digest": profile_digest,
            "recipe_digest": recipe_digest,
            "family_bindings": {},
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()

    assert observed == f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _binding(
    compatibility: Any,
    source_operations: tuple[str, ...],
    *,
    type_key: str,
) -> Any:
    return compatibility.ReviewedBinding(
        source_operations=source_operations,
        type_closure=(compatibility.WireTypeRef("models", type_key),),
        selector_semantics=(
            compatibility.SelectorSemantics(
                resource="widget",
                consumed_selectors=("name",),
                exposed_identities=("code",),
                native_identity="code",
                resolution="lookup-by-name",
            ),
        ),
        evidence_sources=(),
    )


def _profile_data(
    versions: tuple[str, ...],
    bindings: dict[str, Any],
) -> dict[str, Any]:
    return {
        "profiles": {
            version: {
                "server_version": version,
                "build_decisions": {
                    semantic_operation: {
                        "semantic_operation": semantic_operation,
                        "source_operations": list(binding.source_operations),
                        "type_closure": [
                            {"surface": item.surface, "key": item.key}
                            for item in binding.type_closure
                        ],
                        "selector_semantics": [
                            {
                                "resource": item.resource,
                                "consumed_selectors": list(item.consumed_selectors),
                                "exposed_identities": list(item.exposed_identities),
                                "native_identity": item.native_identity,
                                "resolution": item.resolution,
                                "parent_identity": item.parent_identity,
                            }
                            for item in binding.selector_semantics
                        ],
                        "fingerprints": dict(_FINGERPRINTS),
                    }
                    for semantic_operation, binding in bindings.items()
                },
            }
            for version in versions
        }
    }


def _bundle(
    version: str,
    tmp_path: Path,
) -> Any:
    contract_inputs = _module("ds_codegen.contract_inputs")
    ir = _module("ds_codegen.ir")
    runtime_bundles = _module("ds_codegen.runtime_bundles")
    snapshot = ir.ContractSnapshot(
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
    digest = contract_inputs.contract_snapshot_digest(snapshot)
    source_root = tmp_path / f"source-{version}"
    source_root.mkdir(exist_ok=True)
    return runtime_bundles.RuntimeBundle(
        spec=runtime_bundles.RuntimeBundleSpec(
            version=version,
            source_root=source_root,
            selection="runtime-slice",
        ),
        snapshot=snapshot,
        metadata=runtime_bundles.RuntimeBundleMetadata(
            version=version,
            selection="runtime-slice",
            semantic_operations=("widget.get", "dashboard.get"),
            source_tag=version,
            source_commit="a" * 40,
            source_tree="b" * 40,
            source_contract_digest=digest,
            rendered_contract_digest=digest,
            operation_count=0,
            enum_count=0,
            dto_count=0,
            model_count=0,
        ),
    )


@pytest.mark.parametrize("failure", ["duplicate", "uncompiled"])
def test_render_rejects_invalid_ownership_before_writing(
    tmp_path: Path, failure: str
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    bundle = _bundle("3.4.1", tmp_path)
    bundles: tuple[RuntimeBundle, ...]
    if failure == "duplicate":
        bundles = (bundle, bundle)
    else:
        bundles = (
            replace(bundle, snapshot=replace(bundle.snapshot, operations=[None])),
        )
    output = tmp_path / "output"
    with pytest.raises(ValueError):
        artifacts.render_runtime_artifacts(
            bundles, output, profile_data=_profile_data(("3.4.1",), {})
        )
    assert not output.exists()


def test_render_binds_exact_identities_and_every_runtime_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    monkeypatch.setattr(artifacts, "runtime_operation_bindings", lambda _v: {})
    monkeypatch.setattr(
        artifacts, "runtime_auxiliary_operation_bindings", lambda _v: {}
    )
    bundles = tuple(_bundle(version, tmp_path) for version in ("3.4.2", "3.4.1"))
    output = tmp_path / "output"
    result = artifacts.render_runtime_artifacts(
        bundles, output, profile_data=_profile_data(("3.4.1", "3.4.2"), {})
    )
    assert tuple(result) == ("3.4.1", "3.4.2")
    assert not (output / "generated" / "wire_families").exists()
    runtime = output / "generated" / "wire_runtime"
    assert not (runtime / "api" / "operations" / "_kernels.py").exists()
    manifest = runpy.run_path(str(runtime / "_manifest.py"))
    modules = tuple(
        sorted(
            path.relative_to(runtime).as_posix()
            for path in runtime.rglob("*.py")
            if path.name != "_manifest.py"
        )
    )
    assert manifest["MODULES"] == modules
    digest = hashlib.sha256(b"dsctl-wire-runtime-v1\0")
    for module in modules:
        digest.update(module.encode() + b"\0" + (runtime / module).read_bytes() + b"\0")
    assert manifest["CONTENT_DIGEST"] == "sha256:" + digest.hexdigest()
    for version, rendered in result.items():
        exact = runpy.run_path(
            str(
                output
                / "generated"
                / "versions"
                / ("ds_" + version.replace(".", "_"))
                / "_artifact.py"
            )
        )
        assert exact["FAMILY_IDS"] == ()
        assert exact["BINDING_DIGEST"] == rendered.binding_digest
        assert exact["PROFILE_DIGEST"] == rendered.profile_digest
        assert exact["RECIPE_DIGEST"] == rendered.recipe_digest
        assert exact["WIRE_RUNTIME_CONTENT_DIGEST"] == manifest["CONTENT_DIGEST"]
    assert (
        artifacts.render_runtime_artifacts(
            tuple(reversed(bundles)),
            output,
            profile_data=_profile_data(("3.4.1", "3.4.2"), {}),
        )
        == result
    )


@pytest.mark.parametrize(
    "field",
    [
        "semantic_operation",
        "source_operations",
        "type_closure",
        "selector_semantics",
        "fingerprints",
    ],
)
def test_render_rejects_stale_exact_recipe_evidence_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    artifacts = _module("ds_codegen.runtime_artifacts")
    compatibility = _module("ds_codegen.compatibility_impact")
    bindings = {
        "widget.get": _binding(
            compatibility, ("WidgetController.get",), type_key="Widget"
        )
    }
    monkeypatch.setattr(artifacts, "runtime_operation_bindings", lambda _v: bindings)
    monkeypatch.setattr(
        artifacts, "runtime_auxiliary_operation_bindings", lambda _v: {}
    )
    profile_data = _profile_data(("3.4.1",), bindings)
    profile_data["profiles"]["3.4.1"]["build_decisions"]["widget.get"][field] = {}
    bundle = _bundle("3.4.1", tmp_path)
    output = tmp_path / "output"
    with pytest.raises(ValueError):
        artifacts.render_runtime_artifacts((bundle,), output, profile_data=profile_data)
    assert not output.exists()
