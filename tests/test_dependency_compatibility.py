from __future__ import annotations

import tomllib
from importlib.metadata import requires
from pathlib import Path

from typer.core import TyperArgument


def _project_dependencies() -> tuple[str, ...]:
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    project = tomllib.loads(pyproject.read_text())["project"]
    return tuple(project["dependencies"])


def test_external_click_is_not_part_of_the_runtime_contract() -> None:
    project_requirements = _project_dependencies()
    typer_requirements = requires("typer") or ()

    assert not any(item.lower().startswith("click") for item in project_requirements)
    assert not any(item.lower().startswith("click") for item in typer_requirements)
    assert not any(
        cls.__module__ == "click" or cls.__module__.startswith("click.")
        for cls in TyperArgument.__mro__
    )
