from __future__ import annotations

import os
import socket
import time
from dataclasses import dataclass, field
from threading import Lock

START_TIMESTAMP_MS = 1609430400000
LOW_DIGIT_BIT = 5
MACHINE_BIT = 5
MAX_LOW_DIGIT = ~(-1 << LOW_DIGIT_BIT)
HIGH_DIGIT_LEFT = LOW_DIGIT_BIT + MACHINE_BIT
MAX_MACHINE_VALUE = 1 << MACHINE_BIT


@dataclass
class _CodeGenerator:
    """Local translation of DS `CodeGenerateUtils`."""

    machine_hash: int
    low_digit: int = 0
    record_millisecond: int = -1
    lock: Lock = field(default_factory=Lock)

    def gen_code(self) -> int:
        """Generate one monotonically increasing DS-style long code."""
        with self.lock:
            now_millisecond = self._system_millisecond()
            if now_millisecond < self.record_millisecond:
                message = "system clock moved backwards while generating a DS code"
                raise RuntimeError(message)

            if now_millisecond == self.record_millisecond:
                self.low_digit = (self.low_digit + 1) & MAX_LOW_DIGIT
                if self.low_digit == 0:
                    while now_millisecond <= self.record_millisecond:
                        now_millisecond = self._system_millisecond()
            else:
                self.low_digit = 0

            self.record_millisecond = now_millisecond
            return (
                ((now_millisecond - START_TIMESTAMP_MS) << HIGH_DIGIT_LEFT)
                | (self.machine_hash << LOW_DIGIT_BIT)
                | self.low_digit
            )

    @staticmethod
    def _system_millisecond() -> int:
        return int(time.time() * 1000)


def _machine_hash() -> int:
    app_name = f"{socket.gethostname()}-{os.getpid()}"
    return abs(hash(app_name)) % MAX_MACHINE_VALUE


_GENERATOR = _CodeGenerator(machine_hash=_machine_hash())


def gen_code() -> int:
    """Generate one DS-style stable long code."""
    return _GENERATOR.gen_code()
