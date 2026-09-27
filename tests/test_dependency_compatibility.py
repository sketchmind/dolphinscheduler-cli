from __future__ import annotations

import tomllib
from importlib.metadata import requires
from pathlib import Path
from unittest.mock import Mock

import pytest
import typer
from typer import _click
from typer.core import TyperArgument, TyperGroup
from typer.main import get_command

from dsctl.app import _usage_exit, app


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


def test_embedded_usage_exit_uses_public_typer_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(_click.exceptions, "Exit", raising=False)

    with pytest.raises(typer.Exit) as result:
        _usage_exit(2, standalone_mode=False)

    assert result.value.exit_code == 2


@pytest.mark.parametrize("standalone_mode", [False, True])
def test_cli_exit_handles_public_exception_without_private_alias(
    monkeypatch: pytest.MonkeyPatch,
    *,
    standalone_mode: bool,
) -> None:
    command = get_command(app)
    monkeypatch.delattr(_click.exceptions, "Exit", raising=False)
    monkeypatch.setattr(TyperGroup, "main", Mock(side_effect=typer.Exit(7)))

    if standalone_mode:
        with pytest.raises(SystemExit) as process_exit:
            command.main([], standalone_mode=True)
        assert process_exit.value.code == 7
    else:
        with pytest.raises(typer.Exit) as embedded_exit:
            command.main([], standalone_mode=False)
        assert embedded_exit.value.exit_code == 7


@pytest.mark.parametrize("standalone_mode", [False, True])
def test_cli_abort_handles_public_exception_without_private_aliases(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    standalone_mode: bool,
) -> None:
    command = get_command(app)
    monkeypatch.delattr(_click.exceptions, "Exit", raising=False)
    monkeypatch.delattr(_click.exceptions, "Abort", raising=False)
    monkeypatch.setattr(TyperGroup, "main", Mock(side_effect=typer.Abort()))

    if standalone_mode:
        with pytest.raises(SystemExit) as process_exit:
            command.main([], standalone_mode=True)
        assert process_exit.value.code == 1
    else:
        with pytest.raises(typer.Abort):
            command.main([], standalone_mode=False)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ("Aborted!\n" if standalone_mode else "")
