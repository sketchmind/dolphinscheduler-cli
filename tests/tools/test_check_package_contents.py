from __future__ import annotations

import importlib
import io
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("check_package_contents")


def test_wheel_package_content_check_accepts_runtime_only_wheel(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_path = tmp_path / "dolphinscheduler_cli-0.1.0-py3-none-any.whl"
    _write_zip(
        wheel_path,
        [
            "dsctl/__init__.py",
            "dsctl/app.py",
            "dsctl/generated/versions/ds_3_4_1/__init__.py",
            "dsctl/py.typed",
            "dolphinscheduler_cli-0.1.0.dist-info/METADATA",
            "dolphinscheduler_cli-0.1.0.dist-info/entry_points.txt",
            "dolphinscheduler_cli-0.1.0.dist-info/licenses/LICENSE",
        ],
    )

    result = checker.check_distribution(wheel_path)

    assert result.ok


def test_wheel_package_content_check_rejects_development_files(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    wheel_path = tmp_path / "dolphinscheduler_cli-0.1.0-py3-none-any.whl"
    _write_zip(
        wheel_path,
        [
            "dsctl/__init__.py",
            "dsctl/app.py",
            "dsctl/generated/versions/ds_3_4_1/__init__.py",
            "dsctl/py.typed",
            "dolphinscheduler_cli-0.1.0.dist-info/METADATA",
            "dolphinscheduler_cli-0.1.0.dist-info/entry_points.txt",
            "dolphinscheduler_cli-0.1.0.dist-info/licenses/LICENSE",
            "tools/generate_ds_contract.py",
        ],
    )

    result = checker.check_distribution(wheel_path)

    assert not result.ok
    assert any("tools/generate_ds_contract.py" in error for error in result.errors)


def test_sdist_package_content_check_accepts_reviewable_source_archive(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    sdist_path = tmp_path / "dolphinscheduler_cli-0.1.0.tar.gz"
    _write_sdist(
        sdist_path,
        [
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "src/dsctl/__init__.py",
            "docs/development/release.md",
            "docs/development/tooling.md",
            "tools/check_package_contents.py",
            "tests/packaging/test_pyproject_metadata.py",
        ],
    )

    result = checker.check_distribution(sdist_path)

    assert result.ok


def test_sdist_package_content_check_rejects_local_env_files(
    tmp_path: Path,
) -> None:
    checker = _load_module()
    sdist_path = tmp_path / "dolphinscheduler_cli-0.1.0.tar.gz"
    _write_sdist(
        sdist_path,
        [
            "pyproject.toml",
            "MANIFEST.in",
            "README.md",
            "LICENSE",
            "src/dsctl/__init__.py",
            "docs/development/release.md",
            "docs/development/tooling.md",
            "tools/check_package_contents.py",
            "tests/packaging/test_pyproject_metadata.py",
            "config/live-admin.env",
        ],
    )

    result = checker.check_distribution(sdist_path)

    assert not result.ok
    assert any("config/live-admin.env" in error for error in result.errors)


def _write_zip(path: Path, names: list[str]) -> None:
    with zipfile.ZipFile(path, mode="w") as archive:
        for name in names:
            archive.writestr(name, "")


def _write_sdist(path: Path, names: list[str]) -> None:
    with tarfile.open(path, mode="w:gz") as archive:
        for name in names:
            payload = b""
            item = tarfile.TarInfo(f"dolphinscheduler_cli-0.1.0/{name}")
            item.size = len(payload)
            archive.addfile(item, io.BytesIO(payload))
