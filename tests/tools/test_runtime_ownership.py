from __future__ import annotations

import importlib
import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType


@pytest.mark.parametrize("drift", ["duplicate", "traversal", "symlink"])
def test_wheel_ownership_rejects_unsafe_selected_members(
    tmp_path: Path, drift: str
) -> None:
    wheel = tmp_path / "candidate.whl"
    name = "dsctl/generated/wire_programs/user.py"
    if drift == "traversal":
        name = "dsctl/generated/wire_programs/../../outside.py"
    member = zipfile.ZipInfo(name)
    if drift == "symlink":
        member.create_system = 3
        member.external_attr = 0o120777 << 16
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(member, b"candidate data")
        if drift == "duplicate":
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr(name, b"second candidate")

    with pytest.raises(ValueError, match="duplicate or unsafe"):
        _load_module().load_wheel_runtime_ownership(wheel)

    assert not (tmp_path / "outside.py").exists()


def test_wheel_ownership_requires_its_own_complete_exact_manifest_matrix(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "candidate.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("unrelated.py", b"candidate data")

    with pytest.raises(ValueError, match="exact manifests are incomplete"):
        _load_module().load_wheel_runtime_ownership(wheel)


def _load_module() -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("live_gate.runtime_ownership")
