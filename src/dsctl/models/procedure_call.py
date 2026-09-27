from __future__ import annotations

import re
from dataclasses import dataclass

_PROCEDURE_NAME_PATTERN = r"[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*)*"
_PROCEDURE_NAME = re.compile(_PROCEDURE_NAME_PATTERN)
_PROCEDURE_CALL = re.compile(
    rf"\{{\s*call\s+(?P<name>{_PROCEDURE_NAME_PATTERN})\s*"
    r"\(\s*(?P<arguments>.*?)\s*\)\s*\}",
    re.IGNORECASE,
)
PROCEDURE_POSITIONAL_CALL_PATTERN = (
    rf"^\{{\s*call\s+{_PROCEDURE_NAME_PATTERN}\s*"
    r"\(\s*(?:\?\s*(?:,\s*\?\s*)*)?\)\s*\}$"
)


@dataclass(frozen=True, slots=True)
class ProcedureCall:
    """One parsed JDBC procedure-call expression."""

    name: str
    arguments: tuple[str, ...]

    @classmethod
    def parse(cls, method: str) -> ProcedureCall | None:
        """Parse the strict call envelope while retaining native arguments."""
        matched = _PROCEDURE_CALL.fullmatch(method.strip())
        if matched is None:
            return None
        raw_arguments = matched.group("arguments").strip()
        arguments = (
            ()
            if not raw_arguments
            else tuple(argument.strip() for argument in raw_arguments.split(","))
        )
        if any(not argument for argument in arguments):
            return None
        return cls(name=matched.group("name"), arguments=arguments)

    @classmethod
    def from_parts(
        cls,
        name: str,
        arguments: tuple[str, ...],
    ) -> ProcedureCall:
        """Build a call from already separated values and validate its name."""
        normalized_name = name.strip()
        if _PROCEDURE_NAME.fullmatch(normalized_name) is None:
            message = f"Invalid qualified procedure name: {name!r}"
            raise ValueError(message)
        if any(not argument.strip() for argument in arguments):
            message = "Procedure call arguments must not be empty"
            raise ValueError(message)
        return cls(
            name=normalized_name,
            arguments=tuple(argument.strip() for argument in arguments),
        )

    @classmethod
    def has_valid_name(cls, name: str) -> bool:
        """Return whether text is a representable qualified procedure name."""
        return _PROCEDURE_NAME.fullmatch(name.strip()) is not None

    @property
    def uses_positional_placeholders(self) -> bool:
        """Return whether every argument is the canonical JDBC placeholder."""
        return all(argument == "?" for argument in self.arguments)

    def render(self) -> str:
        """Render the compact canonical/native JDBC call envelope."""
        return f"{{call {self.name}({','.join(self.arguments)})}}"


__all__ = ["PROCEDURE_POSITIONAL_CALL_PATTERN", "ProcedureCall"]
