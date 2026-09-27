from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ReleaseState(StrEnum):
    """Process define release state"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> ReleaseState:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 offline 1 online
    OFFLINE = ('OFFLINE', 0, 'offline')
    ONLINE = ('ONLINE', 1, 'online')

    @classmethod
    def from_code(cls, code: int) -> "ReleaseState":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ReleaseState code: {code}")

__all__ = ["ReleaseState"]
