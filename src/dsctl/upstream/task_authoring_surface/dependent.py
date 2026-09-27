from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dsctl.models.task_spec import (
    DEPENDENT_BASE_MONTH_DATE_VALUES,
    DEPENDENT_DAY_DATE_VALUES,
    DEPENDENT_EXTENDED_MONTH_DATE_VALUES,
    DEPENDENT_HOUR_DATE_VALUES,
    DEPENDENT_WEEK_DATE_VALUES,
)

DependentIdentityWire = Literal["legacy-name-to-id", "definition-code"]


@dataclass(frozen=True, slots=True)
class DependentAuthoringSurface:
    """DEPENDENT fields safe to expose for typed authoring."""

    failure_control: bool
    parameter_passing: bool
    identity_wire: DependentIdentityWire
    date_values_by_cycle: tuple[tuple[str, tuple[str, ...]], ...]

    def date_values(self, cycle: str) -> tuple[str, ...]:
        """Return the exact runtime-recognized values for one cycle."""
        return next(
            (
                values
                for candidate, values in self.date_values_by_cycle
                if candidate == cycle
            ),
            (),
        )


_DEPENDENT_BASE_DATE_VALUES = (
    ("hour", DEPENDENT_HOUR_DATE_VALUES),
    ("day", DEPENDENT_DAY_DATE_VALUES),
    ("week", DEPENDENT_WEEK_DATE_VALUES),
    ("month", DEPENDENT_BASE_MONTH_DATE_VALUES),
)
_DEPENDENT_EXTENDED_DATE_VALUES = (
    ("hour", DEPENDENT_HOUR_DATE_VALUES),
    ("day", DEPENDENT_DAY_DATE_VALUES),
    ("week", DEPENDENT_WEEK_DATE_VALUES),
    ("month", DEPENDENT_EXTENDED_MONTH_DATE_VALUES),
)


def _dependent_surface(
    *,
    failure_control: bool,
    parameter_passing: bool,
    extended_month_boundaries: bool,
    identity_wire: DependentIdentityWire = "definition-code",
) -> DependentAuthoringSurface:
    """Build one exact DEPENDENT wire/date epoch."""
    return DependentAuthoringSurface(
        failure_control=failure_control,
        parameter_passing=parameter_passing,
        identity_wire=identity_wire,
        date_values_by_cycle=(
            _DEPENDENT_EXTENDED_DATE_VALUES
            if extended_month_boundaries
            else _DEPENDENT_BASE_DATE_VALUES
        ),
    )


_DEPENDENT_139 = _dependent_surface(
    failure_control=False,
    parameter_passing=False,
    identity_wire="legacy-name-to-id",
    extended_month_boundaries=False,
)
_DEPENDENT_BASIC_BASE = _dependent_surface(
    failure_control=False,
    parameter_passing=False,
    extended_month_boundaries=False,
)
_DEPENDENT_BASIC_EXTENDED = _dependent_surface(
    failure_control=False,
    parameter_passing=False,
    extended_month_boundaries=True,
)
_DEPENDENT_FAILURE_CONTROL = _dependent_surface(
    failure_control=True,
    parameter_passing=False,
    extended_month_boundaries=True,
)
_DEPENDENT_PARAMETER_PASSING = _dependent_surface(
    failure_control=True,
    parameter_passing=True,
    extended_month_boundaries=True,
)
