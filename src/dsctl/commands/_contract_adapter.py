from __future__ import annotations

from typing import TYPE_CHECKING, cast

import typer

from dsctl.command_contract import CommandContractError, InputContract

if TYPE_CHECKING:
    from typer.models import OptionInfo


def typer_option(contract: InputContract) -> OptionInfo:
    """Project one canonical option into Typer without changing callback types."""
    flag = contract.flag
    if flag is None:
        message = f"{contract.name!r} is an argument, not a Typer option"
        raise CommandContractError(message)
    path_rules = contract.path_rules
    if path_rules is None:
        return cast(
            "OptionInfo",
            typer.Option(
                flag,
                help=contract.description,
                metavar=contract.value_name,
            ),
        )
    return cast(
        "OptionInfo",
        typer.Option(
            flag,
            dir_okay=path_rules.dir_okay,
            exists=path_rules.exists,
            file_okay=path_rules.file_okay,
            help=contract.description,
            metavar=contract.value_name,
            readable=path_rules.readable,
            resolve_path=path_rules.resolve_path,
        ),
    )


__all__ = ["typer_option"]
