from __future__ import annotations

import importlib
import json
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest


def _load_module() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.runtime_bundles")


def test_tracked_manifest_declares_the_complete_runtime_bundle_plan() -> None:
    runtime_bundles = _load_module()
    repo_root = Path(__file__).resolve().parents[2]

    specs = runtime_bundles.load_runtime_bundle_specs(repo_root)

    expected_versions = [
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
    ]
    assert [spec.version for spec in specs] == expected_versions
    assert [spec.selection for spec in specs] == [
        "runtime-slice" for _ in expected_versions
    ]
    assert [spec.source_root.relative_to(repo_root).as_posix() for spec in specs] == [
        f"build/upstream/ds-{version}" for version in expected_versions
    ]


def test_tracked_runtime_packages_are_reviewed_semantic_slices() -> None:
    runtime_bundles = _load_module()
    runtime_contract = importlib.import_module("ds_codegen.runtime_contract")
    repo_root = Path(__file__).resolve().parents[2]
    compiled_semantics = set().union(
        *(
            definition.semantic_operations
            for definition in runtime_bundles._COMPILED_DOMAINS
        )
    )

    for spec in runtime_bundles.load_runtime_bundle_specs(repo_root):
        manifest = runpy.run_path(
            str(
                repo_root
                / "src"
                / "dsctl"
                / "generated"
                / "versions"
                / spec.package_slug
                / "_manifest.py"
            )
        )

        assert manifest["SELECTION"] == "runtime-slice", spec.version
        assert manifest["SEMANTIC_OPERATIONS"] == tuple(
            operation
            for operation in runtime_contract.runtime_semantic_operations(spec.version)
            if operation not in compiled_semantics
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        pytest.param(
            {"schema_version": 2, "bundles": []},
            "schema_version must be 1",
            id="unknown-schema",
        ),
        pytest.param(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": "one",
                        "selection": "full",
                    },
                    {
                        "version": "3.4.1",
                        "source_root": "two",
                        "selection": "full",
                    },
                ],
            },
            "duplicate runtime bundle versions: 3.4.1",
            id="duplicate-version",
        ),
        pytest.param(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": "source",
                        "selection": "partial",
                    }
                ],
            },
            "selection must be 'full' or 'runtime-slice'",
            id="unknown-selection",
        ),
        pytest.param(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": str(Path("absolute-source").resolve()),
                        "selection": "full",
                    }
                ],
            },
            "source_root must be repository-relative",
            id="absolute-source",
        ),
    ],
)
def test_runtime_bundle_manifest_rejects_ambiguous_plans(
    tmp_path: Path,
    payload: dict[str, object],
    message: str,
) -> None:
    runtime_bundles = _load_module()
    manifest = tmp_path / "runtime_bundles.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        runtime_bundles.load_runtime_bundle_specs(tmp_path, manifest)


def test_runtime_bundle_source_identity_uses_exact_git_without_extraction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _load_module()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_runtime_bundle_manifest(
        tmp_path,
        source_root=source_root,
        version="3.4.1",
    )

    def reject_extraction(*_args: object, **_kwargs: object) -> object:
        message = "contract extraction is not part of source identity inspection"
        raise AssertionError(message)

    monkeypatch.setattr(runtime_bundles, "load_contract_input", reject_extraction)

    identities = runtime_bundles.load_runtime_bundle_source_identities(
        tmp_path,
        manifest,
    )

    assert identities == (
        runtime_bundles.RuntimeBundleSourceIdentity(
            version="3.4.1",
            selection="full",
            source_tag="3.4.1",
            source_commit=_git(source_root, "rev-parse", "HEAD^{commit}"),
            source_tree=_git(source_root, "rev-parse", "HEAD^{tree}"),
        ),
    )


def test_runtime_bundle_source_identity_rejects_a_dirty_exact_tag(
    tmp_path: Path,
) -> None:
    runtime_bundles = _load_module()
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    manifest = _write_runtime_bundle_manifest(
        tmp_path,
        source_root=source_root,
        version="3.4.1",
    )
    (source_root / "pom.xml").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(ValueError, match="not a clean exact Git tag"):
        runtime_bundles.load_runtime_bundle_source_identities(tmp_path, manifest)


def test_full_runtime_bundle_preserves_exact_source_identity(tmp_path: Path) -> None:
    runtime_bundles = _load_module()
    contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
    ir = importlib.import_module("ds_codegen.ir")
    snapshot = ir.ContractSnapshot(
        ds_version="3.4.1",
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
    loaded = contract_inputs.LoadedContract(
        label="3.4.1",
        snapshot=snapshot,
        provenance=_exact_snapshot_provenance("3.4.1", digest),
    )
    spec = runtime_bundles.RuntimeBundleSpec(
        version="3.4.1",
        source_root=tmp_path / "source",
        selection="full",
    )

    bundle = runtime_bundles.compile_runtime_bundle(spec, loaded)

    assert bundle.snapshot is snapshot
    assert bundle.metadata.version == "3.4.1"
    assert bundle.metadata.source_tag == "3.4.1"
    assert bundle.metadata.source_commit == "a" * 40
    assert bundle.metadata.source_tree == "b" * 40
    assert bundle.metadata.source_contract_digest == digest
    assert bundle.metadata.rendered_contract_digest == digest
    assert bundle.metadata.semantic_operations == ()


def test_runtime_bundle_rejects_a_nonmatching_exact_tag(tmp_path: Path) -> None:
    runtime_bundles = _load_module()
    contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
    ir = importlib.import_module("ds_codegen.ir")
    snapshot = ir.ContractSnapshot(
        ds_version="3.4.1",
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
    loaded = contract_inputs.LoadedContract(
        label="3.4.1",
        snapshot=snapshot,
        provenance=_exact_snapshot_provenance("3.4.0", digest),
    )
    spec = runtime_bundles.RuntimeBundleSpec(
        version="3.4.1",
        source_root=tmp_path / "source",
        selection="full",
    )

    with pytest.raises(
        contract_inputs.ExactProvenanceError,
        match="not a clean exact Git tag",
    ):
        runtime_bundles.compile_runtime_bundle(spec, loaded)


def test_render_runtime_bundles_writes_one_complete_auditable_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_bundles = _load_module()
    version_profiles = importlib.import_module("ds_codegen.version_profiles")
    real_load_ledger = version_profiles.load_version_profile_ledger
    real_compile_profiles = version_profiles.compile_version_profile_data
    real_write_profiles = runtime_bundles.write_version_profiles
    real_write_conformance = runtime_bundles.write_conformance_bundle_data
    loaded_ledgers: list[object] = []
    compile_ledgers: list[object] = []
    compiled_profiles: list[object] = []
    version_writer_ledgers: list[object] = []
    version_writer_profiles: list[object] = []
    conformance_writer_profiles: list[object] = []

    def load_ledger_once() -> dict[str, Any]:
        ledger = real_load_ledger()
        loaded_ledgers.append(ledger)
        return cast("dict[str, Any]", ledger)

    def compile_profiles_once(
        *,
        stable_actions: Any,
        ledger: Any = None,
        versions: tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        compile_ledgers.append(ledger)
        profile_data = real_compile_profiles(
            stable_actions=stable_actions,
            ledger=ledger,
            versions=versions,
        )
        compiled_profiles.append(profile_data)
        return cast("dict[str, Any]", profile_data)

    def capture_version_profile_writer(
        output_root: Path,
        **kwargs: Any,
    ) -> Path:
        version_writer_ledgers.append(kwargs.get("ledger"))
        version_writer_profiles.append(kwargs.get("profile_data"))
        return cast("Path", real_write_profiles(output_root, **kwargs))

    def capture_conformance_writer(
        output_root: Path,
        profile_data: Any,
        **kwargs: Any,
    ) -> Path:
        conformance_writer_profiles.append(profile_data)
        return cast(
            "Path",
            real_write_conformance(output_root, profile_data, **kwargs),
        )

    monkeypatch.setattr(
        version_profiles,
        "load_version_profile_ledger",
        load_ledger_once,
    )
    monkeypatch.setattr(
        version_profiles,
        "compile_version_profile_data",
        compile_profiles_once,
    )
    monkeypatch.setattr(
        runtime_bundles,
        "load_version_profile_ledger",
        load_ledger_once,
        raising=False,
    )
    monkeypatch.setattr(
        runtime_bundles,
        "compile_version_profile_data",
        compile_profiles_once,
    )
    monkeypatch.setattr(
        runtime_bundles,
        "write_version_profiles",
        capture_version_profile_writer,
    )
    monkeypatch.setattr(
        runtime_bundles,
        "write_conformance_bundle_data",
        capture_conformance_writer,
    )
    compiled_modules = (object(), object(), object())

    def compile_domains(
        original_bundles: tuple[object, ...],
        definitions: tuple[object, ...],
        *,
        operation_dependencies: dict[str, tuple[str, ...]],
    ) -> object:
        assert definitions == runtime_bundles._COMPILED_DOMAINS
        assert operation_dependencies == (
            runtime_bundles._COMPILED_OPERATION_DEPENDENCIES
        )

        class Result:
            legacy_bundles = original_bundles

            @staticmethod
            def modules() -> tuple[object, ...]:
                return compiled_modules

        return Result()

    monkeypatch.setattr(runtime_bundles, "compile_domains", compile_domains)

    def render_compiled_wire_artifact(
        modules: tuple[object, ...],
        output_root: Path,
    ) -> None:
        assert modules == compiled_modules
        package = output_root / "generated" / "wire_programs"
        package.mkdir(parents=True)
        (package / "cluster.py").write_text("# compiled cluster\n", encoding="utf-8")
        (package / "environment.py").write_text(
            "# compiled environment\n",
            encoding="utf-8",
        )
        (package / "worker_group.py").write_text(
            "# compiled worker group\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        runtime_bundles,
        "render_compiled_wire_artifact",
        render_compiled_wire_artifact,
    )
    output_root = tmp_path / "output"
    stale_package = output_root / "generated" / "versions" / "ds_9_9_9"
    stale_package.mkdir(parents=True)
    (stale_package / "__init__.py").write_text("stale\n", encoding="utf-8")
    bundles = tuple(
        _empty_bundle(runtime_bundles, tmp_path, version, selection)
        for version, selection in (
            ("3.2.2", "runtime-slice"),
            ("3.4.1", "full"),
        )
    )
    loaded_ledgers.clear()

    schedule_source_reviews: list[dict[str, Path]] = []
    monkeypatch.setattr(
        runtime_bundles,
        "validate_schedule_environment_sources",
        schedule_source_reviews.append,
    )

    runtime_bundles.render_runtime_bundles(
        bundles,
        output_root,
    )

    assert len(loaded_ledgers) == 1
    assert schedule_source_reviews == [
        {bundle.spec.version: bundle.spec.source_root for bundle in bundles}
    ]
    assert len(compiled_profiles) == 1
    assert compile_ledgers[0] is loaded_ledgers[0]
    assert version_writer_ledgers[0] is loaded_ledgers[0]
    assert version_writer_profiles[0] is compiled_profiles[0]
    assert conformance_writer_profiles[0] is compiled_profiles[0]

    versions_root = output_root / "generated" / "versions"
    assert not stale_package.exists()
    assert (output_root / "generated" / "version_profiles.py").is_file()
    conformance_path = output_root / "generated" / "conformance_bundles.py"
    assert conformance_path.is_file()
    conformance = runpy.run_path(str(conformance_path))["CONFORMANCE_BUNDLE_DATA"]
    assert conformance["claim"] == "static-action-closure-only"
    assert conformance["catalog_digest"].startswith("sha256:")
    assert conformance["assessment_digest"].startswith("sha256:")
    assert conformance["summary"] == {
        "bundle_count": 2,
        "coordinate_count": 4,
        "ready_coordinate_count": 4,
        "blocked_coordinate_count": 0,
    }
    assert (output_root / "generated" / "datasource_profiles.py").is_file()
    assert (output_root / "generated" / "version_discovery.py").is_file()
    task_profiles_path = output_root / "generated" / "task_profiles.py"
    task_profiles = runpy.run_path(str(task_profiles_path))
    assert task_profiles["TARGET_DS_VERSIONS"] == ("3.2.2", "3.4.1")
    assert len(task_profiles["TASK_PROFILES"]) == 2
    assert (output_root / "generated" / "task_definition_profiles.py").is_file()
    assert (output_root / "generated" / "task_definition_cleanup_profiles.py").is_file()
    assert (output_root / "generated" / "workflow_profiles.py").is_file()
    assert (output_root / "generated" / "runtime_instance_profiles.py").is_file()
    assert (output_root / "generated" / "wire_programs" / "cluster.py").is_file()
    assert (output_root / "generated" / "wire_programs" / "environment.py").is_file()
    assert (output_root / "generated" / "wire_programs" / "worker_group.py").is_file()
    assert (versions_root / "ds_3_2_2" / "__init__.py").is_file()
    assert (versions_root / "ds_3_4_1" / "__init__.py").is_file()
    rendered_manifest = runpy.run_path(str(versions_root / "ds_3_2_2" / "_manifest.py"))
    assert rendered_manifest["DS_VERSION"] == "3.2.2"
    assert rendered_manifest["BUNDLE_MANIFEST_SCHEMA_VERSION"] == 2
    assert rendered_manifest["SELECTION"] == "runtime-slice"
    expected_operations = importlib.import_module(
        "ds_codegen.runtime_contract"
    ).runtime_semantic_operations("3.2.2")
    assert rendered_manifest["SEMANTIC_OPERATIONS"] == expected_operations
    assert rendered_manifest["SOURCE_TAG"] == "3.2.2"
    profile_ledger = importlib.import_module(
        "ds_codegen.version_profiles"
    ).load_version_profile_ledger()
    assert (
        rendered_manifest["SOURCE_COMMIT"]
        == (profile_ledger["sources"]["3.2.2"]["commit"])
    )
    assert rendered_manifest["SOURCE_CONTRACT_DIGEST"].startswith("sha256:")
    assert rendered_manifest["RENDERED_CONTRACT_DIGEST"].startswith("sha256:")
    artifact_manifest = runpy.run_path(str(versions_root / "ds_3_2_2" / "_artifact.py"))
    assert artifact_manifest["RUNTIME_ARTIFACT_SCHEMA_VERSION"] == 4
    assert artifact_manifest["RENDERER_ABI"] == 3
    assert artifact_manifest["FAMILY_IDS"] == ()
    assert rendered_manifest["PROFILE_DIGEST"] == artifact_manifest["PROFILE_DIGEST"]
    assert rendered_manifest["RECIPE_DIGEST"] == artifact_manifest["RECIPE_DIGEST"]
    assert rendered_manifest["BINDING_DIGEST"] == artifact_manifest["BINDING_DIGEST"]
    runtime_package = versions_root / "ds_3_2_2"
    assert runtime_package.joinpath("__init__.py").read_text(encoding="utf-8") == ""
    assert not runtime_package.joinpath("client.py").exists()
    assert not runtime_package.joinpath("_models.py").exists()
    assert not runtime_package.joinpath("api", "operations").exists()
    manifest_text = (versions_root / "ds_3_2_2" / "_manifest.py").read_text(
        encoding="utf-8"
    )
    assert 'DS_VERSION = "3.2.2"' in manifest_text
    assert f'SEMANTIC_OPERATIONS = (\n    "{expected_operations[0]}",' in manifest_text
    assert 'SOURCE_CONTRACT_DIGEST = (\n    "sha256:' in manifest_text


def test_load_prepared_runtime_bundles_retains_each_full_contract_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub_discovery_profiles: list[tuple[Any, Path]],
) -> None:
    runtime_bundles = _load_module()
    contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
    ir = importlib.import_module("ds_codegen.ir")
    manifest = tmp_path / "runtime_bundles.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": "new",
                        "selection": "full",
                    },
                    {
                        "version": "3.2.2",
                        "source_root": "old",
                        "selection": "full",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    calls: list[tuple[str, Path, bool]] = []

    def load_contract_input(item: Any, *, require_exact: bool) -> Any:
        calls.append((item.label, item.path, require_exact))
        snapshot = ir.ContractSnapshot(
            ds_version=item.label,
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
        return contract_inputs.LoadedContract(
            label=item.label,
            snapshot=snapshot,
            provenance=_exact_snapshot_provenance(item.label, digest),
        )

    monkeypatch.setattr(runtime_bundles, "load_contract_input", load_contract_input)
    monkeypatch.setattr(
        runtime_bundles,
        "require_exact_git_source_identity",
        lambda _source_root, *, label: runtime_bundles.ExactGitSourceIdentity(
            tag=label,
            commit="a" * 40,
            tree="b" * 40,
        ),
    )

    prepared = runtime_bundles.load_prepared_runtime_bundles(tmp_path, manifest)

    assert [bundle.spec.version for bundle in prepared.bundles] == ["3.2.2", "3.4.1"]
    assert [loaded.label for loaded in prepared.loaded_contracts] == [
        "3.2.2",
        "3.4.1",
    ]
    assert all(
        snapshot is loaded.snapshot
        for (snapshot, _), loaded in zip(
            stub_discovery_profiles, prepared.loaded_contracts, strict=True
        )
    )
    assert [path for _, path in stub_discovery_profiles] == [
        tmp_path / "old",
        tmp_path / "new",
    ]
    assert all(bundle.discovery_profile is not None for bundle in prepared.bundles)
    assert all(
        bundle.snapshot is loaded.snapshot
        for bundle, loaded in zip(
            prepared.bundles,
            prepared.loaded_contracts,
            strict=True,
        )
    )
    assert calls == [
        ("3.2.2", tmp_path / "old", True),
        ("3.4.1", tmp_path / "new", True),
    ]


def test_runtime_bundle_source_must_remain_exact_until_render_completes(
    tmp_path: Path,
) -> None:
    runtime_bundles = _load_module()
    contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
    ir = importlib.import_module("ds_codegen.ir")
    source_root = _tagged_source_tree(tmp_path, version="3.4.1")
    snapshot = ir.ContractSnapshot(
        ds_version="3.4.1",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )
    loaded = contract_inputs.LoadedContract(
        label="3.4.1",
        snapshot=snapshot,
        provenance=contract_inputs.build_source_provenance(
            source_root,
            label="3.4.1",
            snapshot=snapshot,
        ),
    )
    bundle = runtime_bundles.compile_runtime_bundle(
        runtime_bundles.RuntimeBundleSpec(
            version="3.4.1",
            source_root=source_root,
            selection="full",
        ),
        loaded,
    )
    (source_root / "pom.xml").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(
        contract_inputs.ExactProvenanceError,
        match="not a clean exact Git tag",
    ):
        runtime_bundles.require_runtime_bundle_sources_unchanged((bundle,))


def _exact_snapshot_provenance(version: str, digest: str) -> dict[str, object]:
    tree = "b" * 40
    return {
        "schema_version": 1,
        "exact": True,
        "input_kind": "snapshot",
        "content_digest": digest,
        "contract_digest": digest,
        "origin": {
            "kind": "ds-source",
            "content_digest": f"git-tree:{tree}",
            "git": {
                "commit": "a" * 40,
                "tree": tree,
                "tag": version,
                "ref": f"refs/tags/{version}",
                "dirty": False,
            },
        },
    }


def _write_runtime_bundle_manifest(
    repo_root: Path,
    *,
    source_root: Path,
    version: str,
) -> Path:
    manifest = repo_root / "runtime_bundles.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": version,
                        "source_root": source_root.relative_to(repo_root).as_posix(),
                        "selection": "full",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return manifest


def _empty_bundle(
    runtime_bundles: Any,
    tmp_path: Path,
    version: str,
    selection: str,
) -> Any:
    ir = importlib.import_module("ds_codegen.ir")
    contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
    version_profiles = importlib.import_module("ds_codegen.version_profiles")
    source_identity = version_profiles.load_version_profile_ledger()["sources"][version]
    source_root = tmp_path / f"source-{version}"
    source_root.mkdir()
    datasource_reviews = importlib.import_module(
        "ds_codegen.datasource_profiles"
    ).load_datasource_profile_reviews()
    datasource_types = tuple(dict.fromkeys(("MYSQL", *datasource_reviews[version])))
    snapshot = ir.ContractSnapshot(
        ds_version=version,
        operation_count=0,
        enum_count=1,
        dto_count=0,
        model_count=1,
        operations=[],
        enums=[
            ir.EnumSpec(
                name="DbType",
                import_path="org.apache.dolphinscheduler.spi.enums.DbType",
                documentation=None,
                fields=[],
                json_value_field=None,
                values=[
                    ir.EnumValueSpec(
                        name=name,
                        arguments=[],
                        documentation=None,
                    )
                    for name in datasource_types
                ],
            )
        ],
        dtos=[],
        models=[
            ir.ModelSpec(
                name="BaseDataSourceParamDTO",
                import_path=(
                    "org.apache.dolphinscheduler.plugin.datasource.api.datasource."
                    "BaseDataSourceParamDTO"
                ),
                kind="other_class",
                documentation=None,
                extends=None,
                fields=[
                    ir.DtoFieldSpec(
                        name="password",
                        java_type="String",
                        wire_name="password",
                        required=None,
                        default_value=None,
                        nullable=True,
                        default_factory=None,
                        description=None,
                        example=None,
                        allowable_values=None,
                        documentation=None,
                    )
                ],
            )
        ],
    )
    digest = contract_inputs.contract_snapshot_digest(snapshot)
    semantic_operations = (
        importlib.import_module(
            "ds_codegen.runtime_contract"
        ).runtime_semantic_operations(version)
        if selection == "runtime-slice"
        else ()
    )
    spec = runtime_bundles.RuntimeBundleSpec(
        version=version,
        source_root=source_root,
        selection=selection,
    )
    metadata = runtime_bundles.RuntimeBundleMetadata(
        version=version,
        selection=selection,
        semantic_operations=semantic_operations,
        source_tag=version,
        source_commit=source_identity["commit"],
        source_tree=source_identity["tree"],
        source_contract_digest=digest,
        rendered_contract_digest=digest,
        operation_count=0,
        enum_count=1,
        dto_count=0,
        model_count=1,
    )
    return runtime_bundles.RuntimeBundle(
        spec=spec,
        snapshot=snapshot,
        metadata=metadata,
    )


def _tagged_source_tree(tmp_path: Path, *, version: str) -> Path:
    source_root = tmp_path / f"tagged-{version}"
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
