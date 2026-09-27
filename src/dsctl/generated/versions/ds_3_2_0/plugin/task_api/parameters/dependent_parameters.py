from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DependentParametersDependentFailurePolicyEnum(StrEnum):
    """The dependent task failure policy."""
    DEPENDENT_FAILURE_FAILURE = 'DEPENDENT_FAILURE_FAILURE'
    DEPENDENT_FAILURE_WAITING = 'DEPENDENT_FAILURE_WAITING'

__all__ = ["DependentParametersDependentFailurePolicyEnum"]
