from __future__ import annotations

import importlib
import os
import shutil
import sys
from pathlib import Path
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
    monkeypatch.setattr(freshness, "SRC_GENERATED", src_generated / "versions")
    monkeypatch.setattr(freshness, "CACHE_OUTPUT", fresh_output)
    monkeypatch.setattr(freshness, "CACHE_STAMP", cache_stamp)
    monkeypatch.setattr(freshness, "INPUT_ROOTS", ())
    cache_stamp.write_text(freshness._input_fingerprint(), encoding="utf-8")
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

    def build_contract_snapshot(_root: Path) -> object:
        calls.append("generated")
        return object()

    def write_generated_package(
        _root: Path,
        _snapshot: object,
        output: Path,
    ) -> None:
        shutil.copytree(src_generated, output / "generated")

    monkeypatch.setattr(freshness, "build_contract_snapshot", build_contract_snapshot)
    monkeypatch.setattr(freshness, "write_generated_package", write_generated_package)
    return calls
