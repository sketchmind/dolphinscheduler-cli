from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DataType(StrEnum):
    """Data types in user define parameter"""
    VARCHAR = 'VARCHAR'
    INTEGER = 'INTEGER'
    LONG = 'LONG'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    DATE = 'DATE'
    TIME = 'TIME'
    TIMESTAMP = 'TIMESTAMP'
    BOOLEAN = 'BOOLEAN'
    LIST = 'LIST'
    FILE = 'FILE'

__all__ = ["DataType"]
