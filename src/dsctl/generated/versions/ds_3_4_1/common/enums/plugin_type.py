from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class PluginType(StrEnum):
    """PluginType"""
    code: int
    desc: str
    hasUi: bool

    def __new__(cls, wire_value: str, code: int, desc: str, hasUi: bool) -> PluginType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        obj.hasUi = hasUi
        return obj
    ALERT = ('ALERT', 1, 'alert', True)
    REGISTER = ('REGISTER', 2, 'register', False)
    TASK = ('TASK', 3, 'task', True)

    @classmethod
    def from_code(cls, code: int) -> "PluginType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown PluginType code: {code}")

__all__ = ["PluginType"]
