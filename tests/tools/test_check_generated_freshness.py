from __future__ import annotations

import importlib
import os
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("check_generated_freshness")


def _configure_cached_trees(
    freshness: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    src_generated = tmp_path / "src" / "dsctl" / "generated"
    fresh_output = tmp_path / "cache" / "fresh"
    fresh_generated = fresh_output / "generated"
    for generated_root in (src_generated, fresh_generated):
        generated_root.mkdir(parents=True)
        (generated_root / "__init__.py").write_text("", encoding="utf-8")
        (generated_root / "versions").mkdir()
        (generated_root / "versions" / "__init__.py").write_text("", encoding="utf-8")
        version_root = generated_root / "versions" / "ds_3_4_1"
        version_root.mkdir()
        (version_root / "__init__.py").write_text("", encoding="utf-8")

    cache_stamp = tmp_path / "cache" / ".stamp"
    snapshot_dir = tmp_path / "snapshots"
    monkeypatch.setattr(freshness, "SRC_GENERATED", src_generated / "versions")
    monkeypatch.setattr(freshness, "CACHE_OUTPUT", fresh_output)
    monkeypatch.setattr(freshness, "CACHE_STAMP", cache_stamp)
    monkeypatch.setattr(freshness, "SNAPSHOT_DIR", snapshot_dir)
    monkeypatch.setattr(freshness, "INPUT_ROOTS", ())
    monkeypatch.setattr(
        freshness,
        "load_runtime_bundle_source_identities",
        lambda _repo_root, _manifest_path: (),
    )
    monkeypatch.setattr(
        freshness,
        "load_runtime_bundles",
        lambda _repo_root, _manifest_path, **_kwargs: (),
    )
    monkeypatch.setattr(
        freshness,
        "require_runtime_bundle_sources_unchanged",
        lambda _bundles: None,
    )
    cache_stamp.write_text(
        freshness._input_fingerprint(
            source_identities=freshness._runtime_input_fingerprint_values(
                (),
                snapshot_mode="prefer",
                snapshot_dir=snapshot_dir,
            )
        ),
        encoding="utf-8",
    )
    return src_generated, fresh_generated


def test_main_fails_when_committed_generated_versions_are_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    empty_versions = tmp_path / "src" / "dsctl" / "generated" / "versions"
    empty_versions.mkdir(parents=True)
    monkeypatch.setattr(freshness, "SRC_GENERATED", empty_versions)

    exit_code = freshness.main()

    assert exit_code == 1
    assert "no version packages in src" in capsys.readouterr().out


def test_main_fails_when_fresh_generation_adds_a_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _, fresh_generated = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    added_version = fresh_generated / "versions" / "ds_3_5_0"
    added_version.mkdir()
    (added_version / "__init__.py").write_text("", encoding="utf-8")

    exit_code = freshness.main()

    assert exit_code == 1
    assert "ds_3_5_0" in capsys.readouterr().out


def test_main_reports_a_version_removed_from_the_bundle_plan_as_hand_added(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    stale_version = src_generated / "versions" / "ds_3_2_2"
    stale_version.mkdir()
    (stale_version / "__init__.py").write_text("", encoding="utf-8")

    exit_code = freshness.main()

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "hand-added:" in output
    assert "generated/versions/ds_3_2_2" in output


def test_main_fails_when_generated_root_init_has_drifted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _, fresh_generated = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    (fresh_generated / "__init__.py").write_text("fresh\n", encoding="utf-8")

    exit_code = freshness.main()

    assert exit_code == 1
    assert "generated/__init__.py" in capsys.readouterr().out


@pytest.mark.parametrize(
    "filename",
    [
        "runtime_instance_profiles.py",
        "task_definition_profiles.py",
        "version_discovery.py",
        "workflow_profiles.py",
    ],
)
def test_main_fails_when_generated_runtime_profile_has_drifted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    filename: str,
) -> None:
    freshness = _load_module()
    _, fresh_generated = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    (fresh_generated / filename).write_text(
        "fresh\n",
        encoding="utf-8",
    )

    exit_code = freshness.main()

    assert exit_code == 1
    assert f"generated/{filename}" in capsys.readouterr().out


def test_main_fails_when_generated_versions_init_has_drifted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _, fresh_generated = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    (fresh_generated / "versions" / "__init__.py").write_text(
        "fresh\n", encoding="utf-8"
    )

    exit_code = freshness.main()

    assert exit_code == 1
    assert "generated/versions/__init__.py" in capsys.readouterr().out


def test_main_reports_missing_generated_root_init(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    (src_generated / "__init__.py").unlink()

    exit_code = freshness.main()

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "missing:" in output
    assert "generated/__init__.py" in output


def test_main_reports_missing_generated_versions_init(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    (src_generated / "versions" / "__init__.py").unlink()

    exit_code = freshness.main()

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "missing:" in output
    assert "generated/versions/__init__.py" in output


def test_main_ignores_python_bytecode_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    bytecode_cache = src_generated / "__pycache__"
    bytecode_cache.mkdir()
    (bytecode_cache / "__init__.cpython-312.pyc").write_bytes(b"cache")

    exit_code = freshness.main()

    assert exit_code == 0
    assert "generated code is fresh" in capsys.readouterr().out


def test_main_revalidates_exact_sources_before_accepting_a_cache_hit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _configure_cached_trees(freshness, tmp_path, monkeypatch)

    def reject_non_exact_sources(_repo_root: Path, _manifest_path: Path) -> object:
        message = "3.2.2 source is not a clean exact Git tag"
        raise ValueError(message)

    monkeypatch.setattr(
        freshness,
        "load_runtime_bundle_source_identities",
        reject_non_exact_sources,
    )

    exit_code = freshness.main()

    assert exit_code == 2
    assert "not a clean exact Git tag" in capsys.readouterr().out


def test_main_cache_hit_skips_contract_extraction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    _configure_cached_trees(freshness, tmp_path, monkeypatch)

    def reject_extraction(
        _repo_root: Path,
        _manifest_path: Path,
        **_kwargs: object,
    ) -> object:
        message = "full contract extraction must not run on a cache hit"
        raise AssertionError(message)

    monkeypatch.setattr(freshness, "load_runtime_bundles", reject_extraction)

    assert freshness.main() == 0


def test_main_rejects_stale_generated_task_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _configure_cached_trees(freshness, tmp_path, monkeypatch)
    stale_output = tmp_path / "src" / "dsctl" / "generated" / "task_profiles.py"
    stale_output.parent.mkdir(parents=True, exist_ok=True)
    stale_output.write_text("# stale\n", encoding="utf-8")
    monkeypatch.setattr(freshness, "TASK_PROFILE_OUTPUT", stale_output)

    assert freshness.main() == 1
    output = capsys.readouterr().out
    assert "generate_ds_task_profiles.py" in output
    assert "task_profiles.py" in output


def test_source_mode_bypasses_the_freshness_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    calls: list[str] = []

    def load_from_source(
        _repo_root: Path,
        _manifest_path: Path,
        **kwargs: object,
    ) -> tuple[object, ...]:
        calls.append(str(kwargs["snapshot_mode"]))
        return ()

    monkeypatch.setattr(freshness, "load_runtime_bundles", load_from_source)
    _install_regenerator(
        freshness,
        monkeypatch,
        src_generated=src_generated,
    )

    assert freshness.main(["--snapshot-mode", "source"]) == 0
    assert calls == ["source"]


def test_snapshot_content_change_invalidates_the_freshness_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    snapshot_dir = freshness.SNAPSHOT_DIR
    snapshot_dir.mkdir(parents=True)
    snapshot_path = snapshot_dir / "ds-3.4.1-contract.json"
    snapshot_path.write_text("alpha\n", encoding="utf-8")
    original_mtime = snapshot_path.stat().st_mtime_ns
    identity = SimpleNamespace(
        version="3.4.1",
        selection="full",
        source_tag="3.4.1",
        source_commit="a" * 40,
        source_tree="b" * 40,
    )
    identities = (identity,)
    monkeypatch.setattr(
        freshness,
        "load_runtime_bundle_source_identities",
        lambda _repo_root, _manifest_path: identities,
    )
    freshness.CACHE_STAMP.write_text(
        freshness._input_fingerprint(
            source_identities=freshness._runtime_input_fingerprint_values(
                identities,
                snapshot_mode="prefer",
                snapshot_dir=snapshot_dir,
            )
        ),
        encoding="utf-8",
    )
    calls = _install_regenerator(
        freshness,
        monkeypatch,
        src_generated=src_generated,
    )

    snapshot_path.write_text("bravo\n", encoding="utf-8")
    os.utime(snapshot_path, ns=(original_mtime, original_mtime))

    assert freshness.main() == 0
    assert calls == ["generated"]


def test_main_compares_contents_when_file_metadata_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    src_generated, fresh_generated = _configure_cached_trees(
        freshness, tmp_path, monkeypatch
    )
    relative_path = Path("versions/ds_3_4_1/model.py")
    src_file = src_generated / relative_path
    fresh_file = fresh_generated / relative_path
    src_file.write_text("alpha\n", encoding="utf-8")
    fresh_file.write_text("bravo\n", encoding="utf-8")
    same_timestamp_ns = 1_700_000_000_000_000_000
    os.utime(src_file, ns=(same_timestamp_ns, same_timestamp_ns))
    os.utime(fresh_file, ns=(same_timestamp_ns, same_timestamp_ns))

    exit_code = freshness.main()

    assert exit_code == 1
    assert "generated/versions/ds_3_4_1/model.py" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("source_kind", "fresh_kind"),
    [
        pytest.param("file", "directory", id="file-to-directory"),
        pytest.param("directory", "file", id="directory-to-file"),
        pytest.param("symlink", "file", id="symlink-to-file"),
    ],
)
def test_main_reports_generated_entry_type_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    source_kind: str,
    fresh_kind: str,
) -> None:
    freshness = _load_module()
    src_generated, fresh_generated = _configure_cached_trees(
        freshness, tmp_path, monkeypatch
    )
    relative_path = Path("versions/ds_3_4_1/entry.py")
    for generated_root in (src_generated, fresh_generated):
        (generated_root / relative_path.parent / "target.py").write_text(
            "same\n", encoding="utf-8"
        )
    _write_generated_entry(src_generated / relative_path, kind=source_kind)
    _write_generated_entry(fresh_generated / relative_path, kind=fresh_kind)

    exit_code = freshness.main()

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "type-changed:" in output
    assert "generated/versions/ds_3_4_1/entry.py" in output


def test_main_invalidates_cache_when_input_content_changes_without_newer_mtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    input_file = input_root / "contract.java"
    input_file.write_text("alpha\n", encoding="utf-8")
    input_mtime_ns = input_file.stat().st_mtime_ns
    monkeypatch.setattr(freshness, "INPUT_ROOTS", (input_root,))
    freshness.CACHE_STAMP.write_text(
        freshness._input_fingerprint(),
        encoding="utf-8",
    )
    regeneration_calls = _install_regenerator(
        freshness,
        monkeypatch,
        src_generated=src_generated,
    )

    input_file.write_text("bravo\n", encoding="utf-8")
    os.utime(input_file, ns=(input_mtime_ns, input_mtime_ns))

    assert freshness.main() == 0
    assert regeneration_calls == ["generated"]


def test_main_invalidates_cache_when_an_input_is_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    retained_input = input_root / "retained.java"
    deleted_input = input_root / "deleted.java"
    retained_input.write_text("retained\n", encoding="utf-8")
    deleted_input.write_text("deleted\n", encoding="utf-8")
    monkeypatch.setattr(freshness, "INPUT_ROOTS", (input_root,))
    freshness.CACHE_STAMP.write_text(
        freshness._input_fingerprint(),
        encoding="utf-8",
    )
    regeneration_calls = _install_regenerator(
        freshness,
        monkeypatch,
        src_generated=src_generated,
    )

    deleted_input.unlink()

    assert freshness.main() == 0
    assert regeneration_calls == ["generated"]


def test_main_invalidates_cache_when_symlinked_input_content_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    input_target = tmp_path / "input-target"
    input_target.mkdir()
    input_file = input_target / "contract.java"
    input_file.write_text("alpha\n", encoding="utf-8")
    input_link = tmp_path / "input-link"
    input_link.symlink_to(input_target, target_is_directory=True)
    monkeypatch.setattr(freshness, "INPUT_ROOTS", (input_link,))
    freshness.CACHE_STAMP.write_text(
        freshness._input_fingerprint(),
        encoding="utf-8",
    )
    regeneration_calls = _install_regenerator(
        freshness,
        monkeypatch,
        src_generated=src_generated,
    )

    input_file.write_text("bravo\n", encoding="utf-8")

    assert freshness.main() == 0
    assert regeneration_calls == ["generated"]


def test_main_keeps_the_previous_cache_when_bundle_rendering_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    freshness = _load_module()
    _, fresh_generated = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    input_file = input_root / "manifest.json"
    input_file.write_text("before\n", encoding="utf-8")
    monkeypatch.setattr(freshness, "INPUT_ROOTS", (input_root,))
    freshness.CACHE_STAMP.write_text(
        freshness._input_fingerprint(),
        encoding="utf-8",
    )
    original_init = fresh_generated / "versions" / "ds_3_4_1" / "__init__.py"
    original_init.write_text("previous cache\n", encoding="utf-8")
    input_file.write_text("after\n", encoding="utf-8")

    def fail_after_partial_render(
        _bundles: tuple[object, ...],
        output: Path,
    ) -> None:
        partial = output / "generated" / "versions" / "ds_3_2_2" / "partial.py"
        partial.parent.mkdir(parents=True)
        partial.write_text("partial\n", encoding="utf-8")
        message = "second bundle failed"
        raise RuntimeError(message)

    monkeypatch.setattr(
        freshness,
        "render_runtime_bundles",
        fail_after_partial_render,
    )

    exit_code = freshness.main()

    assert exit_code == 2
    assert "second bundle failed" in capsys.readouterr().out
    assert original_init.read_text(encoding="utf-8") == "previous cache\n"
    assert not (
        freshness.CACHE_OUTPUT / "generated" / "versions" / "ds_3_2_2" / "partial.py"
    ).exists()


def test_main_invalidates_cache_when_exact_source_identity_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    freshness = _load_module()
    src_generated, _ = _configure_cached_trees(freshness, tmp_path, monkeypatch)
    monkeypatch.setattr(freshness, "INPUT_ROOTS", ())
    current_commit = ["a" * 40]

    def load_runtime_bundle_source_identities(
        _repo_root: Path,
        _manifest_path: Path,
    ) -> tuple[object]:
        return (
            SimpleNamespace(
                version="3.4.1",
                selection="full",
                source_tag="3.4.1",
                source_commit=current_commit[0],
                source_tree="b" * 40,
            ),
        )

    def load_runtime_bundles(
        _repo_root: Path,
        _manifest_path: Path,
        **_kwargs: object,
    ) -> tuple[object]:
        return (
            SimpleNamespace(
                spec=SimpleNamespace(version="3.4.1"),
                metadata=SimpleNamespace(
                    version="3.4.1",
                    source_tag="3.4.1",
                    source_commit=current_commit[0],
                    source_tree="b" * 40,
                    source_contract_digest="sha256:" + "c" * 64,
                    rendered_contract_digest="sha256:" + "d" * 64,
                ),
            ),
        )

    render_calls: list[str] = []

    def render_runtime_bundles(_bundles: tuple[object], output: Path) -> None:
        render_calls.append(current_commit[0])
        shutil.copytree(src_generated, output / "generated")

    monkeypatch.setattr(freshness, "load_runtime_bundles", load_runtime_bundles)
    monkeypatch.setattr(
        freshness,
        "load_runtime_bundle_source_identities",
        load_runtime_bundle_source_identities,
    )
    monkeypatch.setattr(
        freshness,
        "render_runtime_bundles",
        render_runtime_bundles,
    )

    assert freshness.main() == 0
    current_commit[0] = "e" * 40
    assert freshness.main() == 0

    assert render_calls == ["a" * 40, "e" * 40]


def _write_generated_entry(path: Path, *, kind: str) -> None:
    if kind == "file":
        path.write_text("same\n", encoding="utf-8")
        return
    if kind == "directory":
        path.mkdir()
        return
    if kind == "symlink":
        path.symlink_to("target.py")
        return
    message = f"unsupported test entry kind: {kind}"
    raise AssertionError(message)


def _install_regenerator(
    freshness: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    *,
    src_generated: Path,
) -> list[str]:
    calls: list[str] = []

    def render_runtime_bundles(
        _bundles: tuple[object, ...],
        output: Path,
    ) -> None:
        calls.append("generated")
        shutil.copytree(src_generated, output / "generated")

    monkeypatch.setattr(
        freshness,
        "render_runtime_bundles",
        render_runtime_bundles,
    )
    return calls
