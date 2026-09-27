from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
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
    return importlib.import_module("prepare_ds_runtime_sources")


def test_prepare_sources_selects_bundle_groups_from_the_manifest(
    tmp_path: Path,
) -> None:
    prepare = _load_module()
    remote = _tagged_remote(tmp_path)
    repo_root = tmp_path / "consumer"
    repo_root.mkdir()
    manifest_path = repo_root / "runtime_bundles.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "bundles": [
                    {
                        "version": "3.4.1",
                        "source_root": "references/dolphinscheduler",
                        "selection": "full",
                    },
                    {
                        "version": "3.2.2",
                        "source_root": "build/upstream/ds-3.2.2",
                        "selection": "runtime-slice",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    result = prepare.main(
        [
            "--repo-root",
            str(repo_root),
            "--manifest",
            str(manifest_path),
            "--remote",
            remote.as_uri(),
            "--selection",
            "full",
        ]
    )

    assert result == 0
    assert _exact_tag(repo_root / "references/dolphinscheduler") == "3.4.1"
    assert not (repo_root / "build/upstream/ds-3.2.2").exists()

    result = prepare.main(
        [
            "--repo-root",
            str(repo_root),
            "--manifest",
            str(manifest_path),
            "--remote",
            remote.as_uri(),
            "--selection",
            "runtime-slice",
        ]
    )

    assert result == 0
    assert _exact_tag(repo_root / "build/upstream/ds-3.2.2") == "3.2.2"


def _tagged_remote(tmp_path: Path) -> Path:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.email", "test@example.com")
    _git(source, "config", "user.name", "Test")
    tracked = source / "pom.xml"
    tracked.write_text("3.2.2\n", encoding="utf-8")
    _git(source, "add", "pom.xml")
    _git(source, "commit", "-m", "3.2.2")
    _git(source, "tag", "3.2.2")
    tracked.write_text("3.4.1\n", encoding="utf-8")
    _git(source, "add", "pom.xml")
    _git(source, "commit", "-m", "3.4.1")
    _git(source, "tag", "3.4.1")
    remote = tmp_path / "remote.git"
    _git(tmp_path, "clone", "--bare", str(source), str(remote))
    return remote


def _exact_tag(path: Path) -> str:
    return _git(path, "describe", "--tags", "--exact-match", "HEAD")


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
