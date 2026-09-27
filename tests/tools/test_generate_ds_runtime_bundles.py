from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from types import ModuleType


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("generate_ds_runtime_bundles")


def test_generator_defaults_to_verified_snapshot_preference() -> None:
    generator = _load_module()

    args = generator._build_parser().parse_args([])

    assert args.snapshot_mode == "prefer"
    assert args.snapshot_dir == generator.DEFAULT_RUNTIME_SNAPSHOT_DIR


def test_generator_replaces_the_complete_namespace_only_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator = _load_module()
    output_root = tmp_path / "src" / "dsctl"
    old_file = output_root / "generated" / "versions" / "old.py"
    old_file.parent.mkdir(parents=True)
    old_file.write_text("old\n", encoding="utf-8")
    bundles = (object(), object())
    monkeypatch.setattr(
        generator,
        "load_runtime_bundles",
        lambda _repo_root, _manifest, **_kwargs: bundles,
    )

    def render_runtime_bundles(items: tuple[Any, ...], staging_root: Path) -> None:
        assert items == bundles
        new_file = staging_root / "generated" / "versions" / "new.py"
        new_file.parent.mkdir(parents=True)
        new_file.write_text("new\n", encoding="utf-8")

    monkeypatch.setattr(generator, "render_runtime_bundles", render_runtime_bundles)
    monkeypatch.setattr(
        generator,
        "require_runtime_bundle_sources_unchanged",
        lambda _bundles: None,
    )

    exit_code = generator.main(
        [
            "--repo-root",
            str(tmp_path),
            "--manifest",
            str(tmp_path / "manifest.json"),
            "--output-root",
            str(output_root),
        ]
    )

    assert exit_code == 0
    assert not old_file.exists()
    assert (output_root / "generated" / "versions" / "new.py").read_text() == ("new\n")


def test_generator_keeps_the_existing_namespace_when_staging_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generator = _load_module()
    output_root = tmp_path / "src" / "dsctl"
    old_file = output_root / "generated" / "versions" / "old.py"
    old_file.parent.mkdir(parents=True)
    old_file.write_text("old\n", encoding="utf-8")
    monkeypatch.setattr(
        generator,
        "load_runtime_bundles",
        lambda _repo_root, _manifest, **_kwargs: (object(), object()),
    )

    def fail_after_partial_render(
        _items: tuple[Any, ...],
        staging_root: Path,
    ) -> None:
        partial = staging_root / "generated" / "versions" / "partial.py"
        partial.parent.mkdir(parents=True)
        partial.write_text("partial\n", encoding="utf-8")
        message = "second bundle failed"
        raise RuntimeError(message)

    monkeypatch.setattr(generator, "render_runtime_bundles", fail_after_partial_render)
    monkeypatch.setattr(
        generator,
        "require_runtime_bundle_sources_unchanged",
        lambda _bundles: None,
    )

    with pytest.raises(RuntimeError, match="second bundle failed"):
        generator.main(
            [
                "--repo-root",
                str(tmp_path),
                "--manifest",
                str(tmp_path / "manifest.json"),
                "--output-root",
                str(output_root),
            ]
        )

    assert old_file.read_text(encoding="utf-8") == "old\n"
    assert not (output_root / "generated" / "versions" / "partial.py").exists()
