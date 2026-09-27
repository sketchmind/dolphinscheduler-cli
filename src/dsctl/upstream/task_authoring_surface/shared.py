from __future__ import annotations

from typing import Literal

CommandTaskLineSeparator = Literal["lf", "system"]
CommandTaskCancelMode = Literal[
    "wrapper-kill",
    "direct-process-destroy",
    "process-tree-and-application",
]
