"""Read explicit secret inputs without putting their contents in diagnostics."""

from pathlib import Path
from typing import Literal, overload

import typer

from dsctl.errors import UserInputError


@overload
def read_password(
    password: str | None, source_file: Path | None, *, required: Literal[True]
) -> str: ...


@overload
def read_password(
    password: str | None, source_file: Path | None, *, required: Literal[False]
) -> str | None: ...


def read_password(
    password: str | None, source_file: Path | None, *, required: bool
) -> str | None:
    """Select a literal password or a UTF-8 file; '-' explicitly consumes stdin."""
    if password is not None and source_file is not None:
        msg = "--password and --password-file are mutually exclusive."
        raise UserInputError(
            msg,
            suggestion="Use --password-file FILE, or --password-file - to read stdin.",
        )
    if source_file is None:
        if required and password is None:
            msg = "A password is required."
            raise UserInputError(
                msg,
                suggestion=(
                    "Pass --password-file FILE, or --password-file - to read stdin."
                ),
            )
        return password
    try:
        value = (
            typer.get_text_stream("stdin").read()
            if source_file == Path("-")
            else source_file.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError) as exc:
        msg = "Cannot read the password input as UTF-8 text."
        raise UserInputError(
            msg,
            suggestion=(
                "Provide a readable UTF-8 password file or use --password-file -."
            ),
        ) from exc
    # Accept the line ending normally emitted by secret stores and shell pipes.
    value = value.removesuffix("\n").removesuffix("\r")
    if not value or "\n" in value or "\r" in value:
        msg = "Password input must contain one nonempty line."
        raise UserInputError(
            msg,
            suggestion="Provide one password with at most one final line ending.",
        )
    return value
