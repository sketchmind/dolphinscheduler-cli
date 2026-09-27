from __future__ import annotations

import importlib
import io
import json
import sys
import tarfile
import warnings
import zipfile
from functools import cache
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Protocol, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType


class _CheckResult(Protocol):
    ok: bool
    errors: tuple[str, ...]


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("check_package_contents")


@cache
def _default_package_expectation() -> object:
    checker = _load_module()
    specs = checker.load_runtime_bundle_specs(
        checker.ROOT,
        checker.DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    )
    return checker._build_package_expectation(specs)


def _check_wheel_paths(names: Sequence[str]) -> _CheckResult:
    checker = _load_module()
    return cast(
        "_CheckResult",
        checker._check_wheel_inventory(
            Path("fixture.whl"),
            inventory=checker._ArchiveInventory.from_names(names),
            expectation=_default_package_expectation(),
        ),
    )


def _check_sdist_paths(names: Sequence[str]) -> _CheckResult:
    checker = _load_module()
    root = "dolphinscheduler_cli-0.2.0"
    inventory = checker._ArchiveInventory.from_names(
        [f"{root}/", *(f"{root}/{name}" for name in names)]
    )
    return cast(
        "_CheckResult",
        checker._check_sdist_inventory(
            Path("fixture.tar.gz"),
            inventory=inventory,
            expectation=_default_package_expectation(),
        ),
    )


def test_wheel_package_content_check_accepts_runtime_only_wheel(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_path = tmp_path / "dolphinscheduler_cli-0.2.0-py3-none-any.whl"
    _write_zip(wheel_path, _complete_wheel_fixture_paths())

    result = checker.check_distribution(wheel_path)

    assert result.ok


def test_wheel_package_content_check_requires_complete_tracked_runtime_bundle() -> None:
    missing_path = "dsctl/generated/wire_runtime/api/operations/_base.py"
    names = [name for name in _complete_wheel_fixture_paths() if name != missing_path]

    result = _check_wheel_paths(names)

    assert not result.ok
    assert f"missing required package path: {missing_path}" in result.errors


@pytest.mark.parametrize("archive_kind", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "filename",
    [
        "conformance_bundles.py",
        "runtime_instance_profiles.py",
        "task_definition_profiles.py",
        "task_profiles.py",
        "version_discovery.py",
        "_discovery_evidence.py",
        "_discovery_operations.py",
        "_discovery_parameters.py",
        "_discovery_profiles.py",
        "_discovery_types.py",
        "workflow_profiles.py",
    ],
)
def test_package_content_check_requires_generated_runtime_profiles(
    archive_kind: str, filename: str
) -> None:
    if archive_kind == "wheel":
        missing_path = f"dsctl/generated/{filename}"
        names = [
            name for name in _complete_wheel_fixture_paths() if name != missing_path
        ]
        result = _check_wheel_paths(names)
    else:
        missing_path = f"src/dsctl/generated/{filename}"
        names = [
            name for name in _complete_sdist_fixture_paths() if name != missing_path
        ]
        result = _check_sdist_paths(names)

    assert result.errors == (f"missing required package path: {missing_path}",)


def test_package_content_check_rejects_undeclared_generated_root_module() -> None:
    wheel_orphan = "dsctl/generated/orphan_profiles.py"
    sdist_orphan = f"src/{wheel_orphan}"

    wheel_result = _check_wheel_paths([*_complete_wheel_fixture_paths(), wheel_orphan])
    sdist_result = _check_sdist_paths([*_complete_sdist_fixture_paths(), sdist_orphan])

    assert wheel_result.errors == (f"undeclared generated module: {wheel_orphan}",)
    assert sdist_result.errors == (f"undeclared generated module: {sdist_orphan}",)


@pytest.mark.parametrize(
    "path",
    ["__init__.py", "_catalog.py", "_manifest.py", "orphan/", "wf_old/nested/extra.py"],
)
def test_wheel_rejects_retired_family_namespace(path: str) -> None:
    orphan_path = f"dsctl/generated/wire_families/{path}"
    result = _check_wheel_paths([*_complete_wheel_fixture_paths(), orphan_path])
    assert result.errors == (f"undeclared generated module: {orphan_path}",)


def test_wheel_package_content_check_rejects_undeclared_wire_runtime_module() -> None:
    orphan_path = "dsctl/generated/wire_runtime/extra_runtime.py"
    result = _check_wheel_paths([*_complete_wheel_fixture_paths(), orphan_path])

    assert not result.ok
    assert result.errors == (
        "undeclared wire runtime module: dsctl/generated/wire_runtime/extra_runtime.py",
    )


def test_package_content_check_requires_manifest_owned_wire_program_modules() -> None:
    for relative_path in ("wire_programs/_manifest.py", "wire_programs/cluster.py"):
        wheel_path = f"dsctl/generated/{relative_path}"
        wheel_result = _check_wheel_paths(
            [name for name in _complete_wheel_fixture_paths() if name != wheel_path]
        )
        sdist_path = f"src/dsctl/generated/{relative_path}"
        sdist_result = _check_sdist_paths(
            [name for name in _complete_sdist_fixture_paths() if name != sdist_path]
        )

        assert f"missing required package path: {wheel_path}" in wheel_result.errors
        assert f"missing required package path: {sdist_path}" in sdist_result.errors


def test_package_content_check_rejects_undeclared_wire_program_modules() -> None:
    wheel_orphan = "dsctl/generated/wire_programs/orphan.py"
    wheel_result = _check_wheel_paths([*_complete_wheel_fixture_paths(), wheel_orphan])
    sdist_orphan = "src/dsctl/generated/wire_programs/orphan.py"
    sdist_result = _check_sdist_paths([*_complete_sdist_fixture_paths(), sdist_orphan])

    assert wheel_result.errors == (f"undeclared wire program module: {wheel_orphan}",)
    assert sdist_result.errors == (f"undeclared wire program module: {sdist_orphan}",)


def test_wire_program_archive_paths_obey_portable_path_rules(tmp_path: Path) -> None:
    checker = _load_module()
    traversal = "dsctl/generated/wire_programs/../escape.py"
    traversal_result = _check_wheel_paths([*_complete_wheel_fixture_paths(), traversal])
    assert f"path traversal archive path is not allowed: {traversal}" in (
        traversal_result.errors
    )

    alias = "src/dsctl/generated/wire_programs//cluster.py"
    alias_archive_path = f"dolphinscheduler_cli-0.2.0/{alias}"
    alias_archive = tmp_path / "wire-program-alias.tar.gz"
    _write_sdist(alias_archive, [*_complete_sdist_fixture_paths(), alias])
    alias_result = checker.check_distribution(alias_archive)
    assert f"non-canonical archive path is not allowed: {alias_archive_path}" in (
        alias_result.errors
    )

    canonical = "dsctl/generated/wire_programs/cluster.py"
    case_alias = "dsctl/generated/wire_programs/Cluster.py"
    collision_result = _check_wheel_paths(
        [*_complete_wheel_fixture_paths(), case_alias]
    )
    assert (
        "portable archive path collision is not allowed: "
        f"{case_alias} <-> {canonical}" in collision_result.errors
    )


def test_wire_program_inventory_is_read_from_its_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checker = _load_module()
    manifest = (
        tmp_path / "src" / "dsctl" / "generated" / "wire_programs" / "_manifest.py"
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        "MODULES = ('__init__.py', '_schemas/__init__.py', "
        "'_schemas/example/__init__.py', "
        "'_schemas/example/response_get_7192d1014419.py', 'example.py')\n"
        "DOMAIN_MODULES = ('example.py',)\n"
        "SUPPORT_MODULES = ('_schemas/__init__.py', "
        "'_schemas/example/__init__.py', "
        "'_schemas/example/response_get_7192d1014419.py')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(checker, "ROOT", tmp_path)

    assert checker._tracked_wire_program_modules() == (
        "__init__.py",
        "_manifest.py",
        "_schemas/__init__.py",
        "_schemas/example/__init__.py",
        "_schemas/example/response_get_7192d1014419.py",
        "example.py",
    )


def test_wire_program_manifest_rejects_unsafe_module_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checker = _load_module()
    manifest = (
        tmp_path / "src" / "dsctl" / "generated" / "wire_programs" / "_manifest.py"
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        "MODULES = ('../escape.py', '__init__.py')\n"
        "DOMAIN_MODULES = ('../escape.py',)\n"
        "SUPPORT_MODULES = ()\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(checker, "ROOT", tmp_path)

    with pytest.raises(ValueError, match="contains an unsafe module"):
        checker._tracked_wire_program_modules()


def test_wheel_package_content_check_rejects_extension_module_shadows(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    cases = (
        (
            "dsctl/generated/wire_runtime/_models.abi3.so",
            "undeclared wire runtime module: "
            "dsctl/generated/wire_runtime/_models.abi3.so",
        ),
        (
            "dsctl/generated/wire_families/_catalog.abi3.so",
            "undeclared generated module: "
            "dsctl/generated/wire_families/_catalog.abi3.so",
        ),
        (
            "dsctl/generated/wire_programs/cluster.abi3.so",
            "undeclared wire program module: "
            "dsctl/generated/wire_programs/cluster.abi3.so",
        ),
        (
            "dsctl/generated/versions/ds_3_4_2/client.abi3.so",
            "undeclared runtime bundle module: "
            "dsctl/generated/versions/ds_3_4_2/client.abi3.so",
        ),
        (
            "dsctl/generated/versions/ds_3_4_2.abi3.so",
            "undeclared runtime bundle module: "
            "dsctl/generated/versions/ds_3_4_2.abi3.so",
        ),
        (
            "dsctl/generated/version_profiles.abi3.so",
            "undeclared generated module: dsctl/generated/version_profiles.abi3.so",
        ),
    )
    for index, (orphan_path, expected_error) in enumerate(cases):
        wheel_path = tmp_path / f"extension-shadow-{index}.whl"
        _write_zip(
            wheel_path,
            [*_complete_wheel_fixture_paths(), orphan_path],
        )

        result = checker.check_distribution(wheel_path)

        assert not result.ok
        assert result.errors == (expected_error,)


def test_sdist_package_content_check_enforces_fixed_wire_family_layout() -> None:
    cases = (
        (
            f"src/dsctl/generated/wire_families/wf_{'0' * 64}/exchange.py",
            "undeclared generated module: "
            f"src/dsctl/generated/wire_families/wf_{'0' * 64}/exchange.py",
        ),
        (
            "src/dsctl/generated/wire_families/_catalog.abi3.so",
            "undeclared generated module: "
            "src/dsctl/generated/wire_families/_catalog.abi3.so",
        ),
    )
    for orphan_path, expected_error in cases:
        result = _check_sdist_paths([*_complete_sdist_fixture_paths(), orphan_path])

        assert not result.ok
        assert result.errors == (expected_error,)


def test_wheel_package_content_check_rejects_versions_root_module() -> None:
    orphan_path = "dsctl/generated/versions/extra.py"
    result = _check_wheel_paths([*_complete_wheel_fixture_paths(), orphan_path])

    assert not result.ok
    assert result.errors == (
        "undeclared runtime bundle module: dsctl/generated/versions/extra.py",
    )


def test_wheel_package_content_check_rejects_undeclared_generated_packages() -> None:
    cases = (
        (
            "dsctl/generated/versions/orphan/__init__.py",
            "undeclared runtime bundle package: dsctl/generated/versions/orphan",
        ),
        (
            "dsctl/generated/wire_runtime/orphan/__init__.py",
            "undeclared wire runtime module: "
            "dsctl/generated/wire_runtime/orphan/__init__.py",
        ),
        (
            "dsctl/generated/orphan/__init__.py",
            "undeclared generated module: dsctl/generated/orphan/__init__.py",
        ),
    )
    for orphan_path, expected_error in cases:
        result = _check_wheel_paths([*_complete_wheel_fixture_paths(), orphan_path])

        assert not result.ok
        assert result.errors == (expected_error,)


def test_package_content_check_rejects_duplicate_archive_members(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_path = tmp_path / "duplicate.whl"
    _write_zip(
        wheel_path,
        [*_complete_wheel_fixture_paths(), "dsctl/py.typed"],
    )
    sdist_path = tmp_path / "duplicate.tar.gz"
    _write_sdist(
        sdist_path,
        [*_complete_sdist_fixture_paths(), "README.md"],
    )

    wheel_result = checker.check_distribution(wheel_path)
    sdist_result = checker.check_distribution(sdist_path)

    assert wheel_result.errors == (
        "duplicate archive path is not allowed: dsctl/py.typed",
    )
    assert sdist_result.errors == (
        "duplicate archive path is not allowed: dolphinscheduler_cli-0.2.0/README.md",
        "duplicate archive path is not allowed: README.md",
    )


def test_package_content_check_rejects_noncanonical_archive_aliases(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_alias = "dsctl/generated/wire_runtime//_models.py"
    wheel_path = tmp_path / "aliased.whl"
    _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), wheel_alias])
    sdist_alias = "src/dsctl/generated/wire_runtime/./_models.py"
    sdist_path = tmp_path / "aliased.tar.gz"
    _write_sdist(sdist_path, [*_complete_sdist_fixture_paths(), sdist_alias])

    wheel_result = checker.check_distribution(wheel_path)
    sdist_result = checker.check_distribution(sdist_path)

    assert (
        f"non-canonical archive path is not allowed: {wheel_alias}"
        in wheel_result.errors
    )
    assert (
        "non-canonical archive path is not allowed: "
        f"dolphinscheduler_cli-0.2.0/{sdist_alias}" in sdist_result.errors
    )


def test_package_content_check_rejects_backslash_archive_aliases(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_alias = "dsctl\\generated/wire_runtime/_models.py"
    wheel_path = tmp_path / "backslash-aliased.whl"
    _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), wheel_alias])
    sdist_alias = "src\\dsctl/generated/wire_runtime/_models.py"
    sdist_path = tmp_path / "backslash-aliased.tar.gz"
    _write_sdist(sdist_path, [*_complete_sdist_fixture_paths(), sdist_alias])

    wheel_result = checker.check_distribution(wheel_path)
    sdist_result = checker.check_distribution(sdist_path)

    assert f"backslash archive path is not allowed: {wheel_alias}" in (
        wheel_result.errors
    )
    assert (
        "backslash archive path is not allowed: "
        f"dolphinscheduler_cli-0.2.0/{sdist_alias}" in sdist_result.errors
    )


def test_package_content_check_rejects_windows_rooted_archive_paths(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_alias = "C:/dsctl/generated/wire_runtime/_models.py"
    wheel_path = tmp_path / "windows-rooted.whl"
    _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), wheel_alias])

    result = checker.check_distribution(wheel_path)

    assert f"Windows-rooted archive path is not allowed: {wheel_alias}" in (
        result.errors
    )


def test_package_content_check_rejects_portable_case_collisions(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    canonical_path = "dsctl/generated/wire_runtime/_models.py"
    wheel_alias = "DSCTL/generated/wire_runtime/_models.py"
    wheel_path = tmp_path / "case-collision.whl"
    _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), wheel_alias])
    sdist_alias = "src/dsctl/Generated/wire_runtime/_models.py"
    sdist_path = tmp_path / "case-collision.tar.gz"
    _write_sdist(sdist_path, [*_complete_sdist_fixture_paths(), sdist_alias])

    wheel_result = checker.check_distribution(wheel_path)
    sdist_result = checker.check_distribution(sdist_path)

    assert (
        "portable archive path collision is not allowed: "
        f"{wheel_alias} <-> {canonical_path}" in wheel_result.errors
    )
    assert (
        "portable archive path collision is not allowed: "
        "dolphinscheduler_cli-0.2.0/src/dsctl/Generated/wire_runtime/_models.py"
        " <-> dolphinscheduler_cli-0.2.0/src/dsctl/generated/wire_runtime/"
        "_models.py" in sdist_result.errors
    )


def test_package_content_check_rejects_portable_protected_path_spelling(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_alias = "DSCTL/generated/wire_runtime/_models.abi3.so"
    wheel_path = tmp_path / "case-shadow.whl"
    _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), wheel_alias])
    sdist_alias = "src/dsctl./generated/wire_runtime/_models.abi3.so"
    sdist_path = tmp_path / "trailing-dot-shadow.tar.gz"
    _write_sdist(sdist_path, [*_complete_sdist_fixture_paths(), sdist_alias])

    wheel_result = checker.check_distribution(wheel_path)
    sdist_result = checker.check_distribution(sdist_path)

    assert f"protected archive path spelling is not allowed: {wheel_alias}" in (
        wheel_result.errors
    )
    assert (
        "protected archive path spelling is not allowed: "
        "src/dsctl./generated/wire_runtime/_models.abi3.so" in sdist_result.errors
    )


def test_wheel_package_content_check_rejects_data_relocation_payloads(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    for scheme in ("purelib", "platlib"):
        relocated_path = (
            f"dolphinscheduler_cli-0.2.0.data/{scheme}/"
            "dsctl/generated/wire_runtime/_models.py"
        )
        wheel_path = tmp_path / f"relocated-{scheme}-shadow.whl"
        _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), relocated_path])

        result = checker.check_distribution(wheel_path)

        assert result.errors == (
            f"wheel data relocation path is not allowed: {relocated_path}",
        )


def test_wheel_package_content_check_rejects_subtree_paths_as_files(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    for index, subtree in enumerate(
        ("versions", "wire_families", "wire_programs", "wire_runtime")
    ):
        shadow_path = f"dsctl/generated/{subtree}"
        wheel_path = tmp_path / f"subtree-file-{index}.whl"
        _write_zip(wheel_path, [*_complete_wheel_fixture_paths(), shadow_path])

        result = checker.check_distribution(wheel_path)

        assert f"undeclared generated module: {shadow_path}" in result.errors


def test_wheel_package_content_check_requires_exact_bundle_manifests() -> None:
    missing_manifest = "dsctl/generated/versions/ds_3_2_2/_manifest.py"
    result = _check_wheel_paths(
        [name for name in _complete_wheel_fixture_paths() if name != missing_manifest]
    )

    assert not result.ok
    assert any(
        "dsctl/generated/versions/ds_3_2_2/_manifest.py" in error
        for error in result.errors
    )


def test_wheel_package_content_check_requires_the_322_runtime_bundle() -> None:
    result = _check_wheel_paths(
        [name for name in _complete_wheel_fixture_paths() if "/ds_3_2_2/" not in name],
    )

    assert not result.ok
    assert any(
        "dsctl/generated/versions/ds_3_2_2/__init__.py" in error
        for error in result.errors
    )


def test_wheel_package_requirements_follow_the_runtime_bundle_manifest(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    manifest_path = tmp_path / "runtime_bundles.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "9.8.7",
                        "source_root": "upstream/ds-9.8.7",
                        "selection": "runtime-slice",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    wheel_path = tmp_path / "dolphinscheduler_cli-0.2.0-py3-none-any.whl"
    fixture_paths = [
        "dsctl/__init__.py",
        "dsctl/app.py",
        "dsctl/upstream/registry.py",
        *_tracked_generated_root_fixture_paths("dsctl"),
        *_tracked_generated_artifact_fixture_paths("dsctl"),
        *_fallback_runtime_fixture_paths(
            "dsctl/generated/versions/ds_9_8_7",
        ),
        "dsctl/py.typed",
        "dolphinscheduler_cli-0.2.0.dist-info/METADATA",
        "dolphinscheduler_cli-0.2.0.dist-info/entry_points.txt",
        "dolphinscheduler_cli-0.2.0.dist-info/licenses/LICENSE",
    ]
    _write_zip(wheel_path, fixture_paths)

    result = checker.check_distribution(
        wheel_path,
        runtime_bundle_manifest=manifest_path,
    )

    assert result.ok

    missing_artifact = "dsctl/generated/versions/ds_9_8_7/_artifact.py"
    incomplete_wheel_path = tmp_path / "missing-artifact.whl"
    _write_zip(
        incomplete_wheel_path,
        [name for name in fixture_paths if name != missing_artifact],
    )

    incomplete_result = checker.check_distribution(
        incomplete_wheel_path,
        runtime_bundle_manifest=manifest_path,
    )

    assert f"missing required package path: {missing_artifact}" in (
        incomplete_result.errors
    )


def test_wheel_package_rejects_a_bundle_not_declared_by_the_manifest() -> None:
    result = _check_wheel_paths(
        [
            *_complete_wheel_fixture_paths(),
            "dsctl/generated/versions/ds_9_9_9/__init__.py",
            "dsctl/generated/versions/ds_9_9_9/_manifest.py",
        ],
    )

    assert not result.ok
    assert result.errors == (
        "undeclared runtime bundle package: dsctl/generated/versions/ds_9_9_9",
    )


def test_wheel_package_content_check_rejects_development_files() -> None:
    result = _check_wheel_paths(
        [
            *_complete_wheel_fixture_paths(),
            "tools/generate_ds_contract.py",
        ],
    )

    assert not result.ok
    assert any("tools/generate_ds_contract.py" in error for error in result.errors)


def test_sdist_package_content_check_accepts_reviewable_source_archive(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    sdist_path = tmp_path / "dolphinscheduler_cli-0.2.0.tar.gz"
    _write_sdist(sdist_path, _complete_sdist_fixture_paths())
    with tarfile.open(sdist_path, mode="r:gz") as archive:
        assert any(
            member.isdir() and not member.name.endswith("/")
            for member in archive.getmembers()
        )

    result = checker.check_distribution(sdist_path)

    assert result.ok


def test_sdist_package_content_check_rejects_non_regular_member(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    sdist_path = tmp_path / "symlink.tar.gz"
    _write_sdist(
        sdist_path,
        _complete_sdist_fixture_paths(),
        symlinks={"docs/unsafe-link": "../README.md"},
    )

    result = checker.check_distribution(sdist_path)

    assert result.errors == (
        "non-regular sdist member is not allowed: "
        "dolphinscheduler_cli-0.2.0/docs/unsafe-link",
    )


def test_sdist_package_content_check_rejects_multiple_roots_and_relative_aliases(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    sdist_path = tmp_path / "multiple-roots.tar.gz"
    with tarfile.open(sdist_path, mode="w:gz") as archive:
        for name in ("first/README.md", "second/README.md"):
            item = tarfile.TarInfo(name)
            item.size = 0
            archive.addfile(item, io.BytesIO())

    result = checker.check_distribution(sdist_path)

    assert (
        "sdist archive must use exactly one top-level root: first, second"
        in result.errors
    )
    assert "duplicate archive path is not allowed: README.md" in result.errors


def test_sdist_package_content_check_requires_every_manifest_bundle() -> None:
    result = _check_sdist_paths(
        [name for name in _complete_sdist_fixture_paths() if "/ds_3_2_2/" not in name],
    )

    assert not result.ok
    assert any(
        "src/dsctl/generated/versions/ds_3_2_2/__init__.py" in error
        for error in result.errors
    )


def test_sdist_package_content_check_requires_the_runtime_bundle_plan() -> None:
    result = _check_sdist_paths(
        [
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "src/dsctl/__init__.py",
            "docs/development/release.md",
            "docs/development/tooling.md",
            "tools/check_package_contents.py",
            "tools/generate_ds_runtime_bundles.py",
            "tools/runtime_bundle_manifest.py",
            "tests/packaging/test_pyproject_metadata.py",
        ],
    )

    assert not result.ok
    assert any(
        "tools/ds_codegen/runtime_bundles.json" in error for error in result.errors
    )


def test_sdist_package_content_check_requires_the_conformance_catalog() -> None:
    catalog = "tools/ds_codegen/conformance_bundles.json"
    result = _check_sdist_paths(
        [name for name in _complete_sdist_fixture_paths() if name != catalog],
    )

    assert not result.ok
    assert f"missing required package path: {catalog}" in result.errors


@pytest.mark.parametrize(
    "fixture_path",
    [
        "tests/fixtures/error_translation/pre_package_workflow_inventory.json",
        "tests/fixtures/task_authoring/current_contract_fingerprints.json",
        "tests/fixtures/task_authoring/parameter_examples.json",
        "tests/fixtures/command_contract/pre_catalog_fingerprints.json",
        "tests/fixtures/version_discovery/legacy-swagger.json",
        "tests/fixtures/version_discovery/README.md",
        "tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json",
    ],
)
def test_sdist_package_content_check_requires_reviewed_test_inputs(
    fixture_path: str,
) -> None:
    result = _check_sdist_paths(
        [name for name in _complete_sdist_fixture_paths() if name != fixture_path]
    )

    assert result.errors == (f"missing required package path: {fixture_path}",)


@pytest.mark.parametrize("legacy_location", [False, True])
def test_sdist_package_content_check_requires_exact_live_evidence(
    *, legacy_location: bool
) -> None:
    evidence = "docs/development/live-evidence/external-shell/3.4.2/receipt.json"
    paths = [name for name in _complete_sdist_fixture_paths() if name != evidence]
    if legacy_location:
        paths.append("docs/development/live-evidence/3.4.2/receipt.json")
    result = _check_sdist_paths(
        paths,
    )

    assert not result.ok
    assert (
        "missing required package path: "
        "docs/development/live-evidence/external-shell/3.4.2/*.json"
    ) in result.errors


def test_sdist_package_content_check_rejects_local_env_files() -> None:
    result = _check_sdist_paths(
        [
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "src/dsctl/__init__.py",
            "docs/development/release.md",
            "docs/development/tooling.md",
            "tools/check_package_contents.py",
            "tools/generate_ds_runtime_bundles.py",
            "tools/ds_codegen/runtime_bundles.json",
            "tools/runtime_bundle_manifest.py",
            "tests/packaging/test_pyproject_metadata.py",
            "config/live-admin.env",
        ],
    )

    assert not result.ok
    assert any("config/live-admin.env" in error for error in result.errors)


@pytest.mark.parametrize("archive_kind", ["wheel", "sdist"])
@pytest.mark.parametrize(
    "private_path",
    [
        ".codex/config.toml",
        ".claude/settings.json",
        ".agents/skills/private/SKILL.md",
        "localdevdocs/progress.md",
        "nested/.codex/config.toml",
        "nested/.claude/settings.json",
        "nested/.agents/skills/private/SKILL.md",
        "nested/localdevdocs/progress.md",
        "nested/.CODEX/config.toml",
        "nested/LocalDevDocs/progress.md",
        "tests/fixtures/__pycache__/cached.json",
        "tests/fixtures/.pytest_cache/cached.json",
        "tests/fixtures/.MYPY_CACHE/cached.json",
        "tests/fixtures/.ruff_cache/cached.json",
        "tests/fixtures/.git/config",
        "tests/fixtures/.venv/config.json",
        "AGENTS.md",
        "nested/CLAUDE.md",
        "nested/agents.md",
        ".env",
        ".env.production",
        "nested/.env.production",
        "nested/cluster.env",
        "nested/.envrc",
        "nested/.envrc.local",
        "nested/.env.example.bak",
    ],
)
def test_package_content_check_rejects_private_workspace_material(
    archive_kind: str, private_path: str
) -> None:
    if archive_kind == "wheel":
        result = _check_wheel_paths([*_complete_wheel_fixture_paths(), private_path])
    else:
        result = _check_sdist_paths([*_complete_sdist_fixture_paths(), private_path])

    assert not result.ok
    assert len(result.errors) == 1
    assert private_path in result.errors[0]
    assert result.errors[0].startswith(("forbidden path", "forbidden file"))


@pytest.mark.parametrize(
    "example_path",
    [".env.example", ".env.production.example", "docs/examples/cluster.env.example"],
)
def test_sdist_package_content_check_accepts_explicit_env_templates(
    example_path: str,
) -> None:
    result = _check_sdist_paths([*_complete_sdist_fixture_paths(), example_path])

    assert result.ok


def test_sdist_package_content_check_rejects_separate_agent_skill() -> None:
    result = _check_sdist_paths(
        [
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "src/dsctl/__init__.py",
            "docs/development/release.md",
            "docs/development/tooling.md",
            "tools/check_package_contents.py",
            "tools/generate_ds_runtime_bundles.py",
            "tools/ds_codegen/runtime_bundles.json",
            "tools/runtime_bundle_manifest.py",
            "tests/packaging/test_pyproject_metadata.py",
            "skills/dsctl/SKILL.md",
        ],
    )

    assert not result.ok
    assert any("skills/dsctl/SKILL.md" in error for error in result.errors)


def _write_zip(path: Path, names: Sequence[str]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(path, mode="w") as archive:
            for name in names:
                archive.writestr(name, "")


def _write_sdist(
    path: Path,
    names: Sequence[str],
    *,
    symlinks: dict[str, str] | None = None,
) -> None:
    symlinks = symlinks or {}
    directories = {
        parent.as_posix()
        for name in [*names, *symlinks]
        for parent in PurePosixPath(name).parents
        if parent.as_posix() != "."
    }
    with tarfile.open(path, mode="w:gz") as archive:
        root = tarfile.TarInfo("dolphinscheduler_cli-0.2.0")
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        for directory in sorted(
            directories,
            key=lambda value: (len(PurePosixPath(value).parts), value),
        ):
            item = tarfile.TarInfo(f"dolphinscheduler_cli-0.2.0/{directory}")
            item.type = tarfile.DIRTYPE
            archive.addfile(item)
        for name in names:
            payload = b""
            item = tarfile.TarInfo(f"dolphinscheduler_cli-0.2.0/{name}")
            item.size = len(payload)
            archive.addfile(item, io.BytesIO(payload))
        for name, target in symlinks.items():
            item = tarfile.TarInfo(f"dolphinscheduler_cli-0.2.0/{name}")
            item.type = tarfile.SYMTYPE
            item.linkname = target
            archive.addfile(item)


@cache
def _complete_wheel_fixture_paths() -> tuple[str, ...]:
    return (
        "dsctl/__init__.py",
        "dsctl/app.py",
        "dsctl/upstream/registry.py",
        *_tracked_runtime_fixture_paths("dsctl"),
        "dsctl/py.typed",
        "dolphinscheduler_cli-0.2.0.dist-info/METADATA",
        "dolphinscheduler_cli-0.2.0.dist-info/entry_points.txt",
        "dolphinscheduler_cli-0.2.0.dist-info/licenses/LICENSE",
    )


@cache
def _complete_sdist_fixture_paths() -> tuple[str, ...]:
    return (
        "pyproject.toml",
        "MANIFEST.in",
        "README.md",
        "LICENSE",
        "src/dsctl/__init__.py",
        "src/dsctl/upstream/registry.py",
        *_tracked_runtime_fixture_paths("src/dsctl"),
        "docs/development/release.md",
        "docs/development/tooling.md",
        "docs/development/live-evidence/external-shell/3.4.2/receipt.json",
        "tools/check_package_contents.py",
        "tools/analyze_ds_conformance_bundles.py",
        "tools/generate_ds_runtime_bundles.py",
        "tools/ds_codegen/conformance_bundles.json",
        "tools/ds_codegen/conformance_bundles.py",
        "tools/ds_codegen/runtime_bundles.json",
        "tools/ds_codegen/runtime_artifacts.py",
        "tools/prepare_ds_runtime_sources.py",
        "tools/runtime_bundle_manifest.py",
        "tests/packaging/test_pyproject_metadata.py",
        "tests/fixtures/error_translation/pre_package_workflow_inventory.json",
        "tests/fixtures/task_authoring/current_contract_fingerprints.json",
        "tests/fixtures/task_authoring/parameter_examples.json",
        "tests/fixtures/command_contract/pre_catalog_fingerprints.json",
        "tests/fixtures/version_discovery/legacy-swagger.json",
        "tests/fixtures/version_discovery/README.md",
        "tests/compatibility/corpus/v0.3.0-ds3.4.1-sql-inline.json",
    )


@cache
def _tracked_runtime_fixture_paths(package_root: str) -> tuple[str, ...]:
    generated_root = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "generated"
    source_versions = generated_root / "versions"
    generated_paths = [
        (
            Path(package_root)
            / "generated"
            / "versions"
            / path.relative_to(source_versions)
        ).as_posix()
        for path in sorted(source_versions.rglob("*.py"))
        if not any(part == "__pycache__" for part in path.parts)
    ]
    return (
        *_tracked_generated_root_fixture_paths(package_root),
        *_tracked_generated_artifact_fixture_paths(package_root),
        *generated_paths,
    )


@cache
def _tracked_generated_artifact_fixture_paths(
    package_root: str,
) -> tuple[str, ...]:
    checker = _load_module()
    return tuple(
        (Path(package_root) / "generated" / relative_path).as_posix()
        for relative_path in checker._tracked_generated_artifact_modules()
    )


@cache
def _tracked_generated_root_fixture_paths(package_root: str) -> tuple[str, ...]:
    generated_root = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "generated"
    module_names = {
        "__init__.py",
        "conformance_bundles.py",
        "runtime_instance_profiles.py",
        "task_definition_profiles.py",
        "task_profiles.py",
        "version_profiles.py",
        "version_discovery.py",
        "workflow_profiles.py",
        *(path.name for path in generated_root.glob("*.py")),
    }
    return tuple(
        (Path(package_root) / "generated" / module_name).as_posix()
        for module_name in sorted(module_names)
    )


def _fallback_runtime_fixture_paths(package_root: str) -> tuple[str, ...]:
    return tuple(
        f"{package_root}/{relative_path}"
        for relative_path in (
            "__init__.py",
            "_artifact.py",
            "_manifest.py",
        )
    )
