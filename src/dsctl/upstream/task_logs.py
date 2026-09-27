from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Protocol, TypedDict

from dsctl.errors import UserInputError
from dsctl.upstream.pagination import observation_time

_LOG_CHUNK_SIZE = 1000
_MAX_LOG_CHUNKS = 200


@dataclass(frozen=True, slots=True)
class TaskLogChunk:
    """One internal, version-normalized logger response."""

    message: str | None
    cursor_advance: int
    eof: bool
    header_lines: int = 0


class LogWindow(TypedDict):
    """Source-line coordinates and bounded observation facts."""

    mode: str
    start_line: int
    end_line: int | None
    requested_limit: int
    lines_scanned: int
    returned_lines: int
    clipped: bool
    header_lines: int
    has_more: bool | None
    next_start_line: int | None
    scope_complete: bool
    atomic_snapshot: bool
    observation_started_at: str
    observation_finished_at: str


@dataclass(frozen=True, slots=True)
class TaskLogTail:
    """Caller-oriented tail of a complete task log."""

    text: str
    line_count: int
    window: LogWindow | None = None


class TaskLogChunkReader(Protocol):
    """Internal wire seam used while exhausting the logger endpoint."""

    def __call__(
        self,
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> TaskLogChunk: ...


def tail_task_log(
    *,
    task_instance_id: int,
    max_lines: int,
    read_chunk: TaskLogChunkReader,
) -> TaskLogTail:
    """Read all available chunks and retain only the requested tail."""
    started = observation_time()
    header_lines = 0
    lines: deque[str] = deque(maxlen=max_lines)
    skip_line_num = 0
    for _ in range(_MAX_LOG_CHUNKS):
        chunk = read_chunk(
            task_instance_id=task_instance_id,
            skip_line_num=skip_line_num,
            limit=_LOG_CHUNK_SIZE,
        )
        header_lines += chunk.header_lines
        message = chunk.message or ""
        if chunk.header_lines:
            preamble = message.split("\n", maxsplit=chunk.header_lines)
            lines.extend(preamble[:-1])
            message = preamble[-1]
        # The exact logger terminates each source line with CRLF. str.splitlines
        # would additionally split control characters inside a source line.
        lines.extend(message.split("\r\n")[:-1] if message else [])
        if chunk.eof:
            break
        if chunk.cursor_advance <= 0:
            message = "Refusing to continue task log pagination without progress"
            raise UserInputError(
                message,
                details={"task_instance_id": task_instance_id},
            )
        skip_line_num += chunk.cursor_advance
    else:
        message = "Refusing to fetch more task log chunks than the safety limit"
        raise UserInputError(
            message,
            details={
                "task_instance_id": task_instance_id,
                "max_chunks": _MAX_LOG_CHUNKS,
            },
            suggestion=(
                "Inspect the task log in the DS UI or worker log storage if you "
                "need more output than the CLI safety limit allows."
            ),
        )

    retained_header = header_lines if skip_line_num + header_lines <= max_lines else 0
    retained_source_lines = max(0, len(lines) - retained_header)
    return TaskLogTail(
        text="\n".join(lines),
        line_count=len(lines),
        window={
            "mode": "tail",
            "start_line": max(1, skip_line_num - retained_source_lines + 1),
            "end_line": skip_line_num or None,
            "requested_limit": max_lines,
            "lines_scanned": skip_line_num,
            "returned_lines": retained_source_lines,
            "clipped": skip_line_num > retained_source_lines,
            "header_lines": retained_header,
            "has_more": False,
            "next_start_line": None,
            "scope_complete": True,
            "atomic_snapshot": False,
            "observation_started_at": started,
            "observation_finished_at": observation_time(),
        },
    )


def window_task_log(
    *,
    task_instance_id: int,
    start_line: int,
    limit: int,
    read_chunk: TaskLogChunkReader,
) -> TaskLogTail:
    """Read a source-line window directly; an extra line establishes continuation."""
    if start_line < 1 or limit < 1:
        message = "Log --start-line and --limit must be positive integers"
        raise UserInputError(message)
    if limit > _LOG_CHUNK_SIZE * _MAX_LOG_CHUNKS:
        message = "Requested log window exceeds the existing log read safety budget"
        raise UserInputError(
            message,
            details={"max_lines": _LOG_CHUNK_SIZE * _MAX_LOG_CHUNKS},
            suggestion=(
                "Use a smaller --limit and continue with the returned next_start_line."
            ),
        )
    started = observation_time()
    lines: list[str] = []
    scanned = 0
    has_more: bool | None = None
    for _ in range(_MAX_LOG_CHUNKS):
        requested = min(_LOG_CHUNK_SIZE, limit + 1 - len(lines))
        chunk = read_chunk(
            task_instance_id=task_instance_id,
            skip_line_num=start_line - 1 + scanned,
            limit=requested,
        )
        message = chunk.message or ""
        if chunk.header_lines:
            message = message.split("\n", maxsplit=chunk.header_lines)[-1]
        body = message.split("\r\n")[:-1] if message else []
        if len(body) != chunk.cursor_advance or (
            message and not message.endswith("\r\n")
        ):
            error_message = "Logger response does not preserve source-line boundaries"
            raise UserInputError(
                error_message, details={"task_instance_id": task_instance_id}
            )
        lines.extend(body)
        scanned += chunk.cursor_advance
        if len(lines) > limit:
            has_more = True
            break
        if chunk.eof or chunk.cursor_advance < requested:
            has_more = False
            break
        if chunk.cursor_advance <= 0:
            message = "Refusing to continue task log pagination without progress"
            raise UserInputError(message)
    kept = lines[:limit]
    next_line = start_line + len(kept) if has_more is not False else None
    return TaskLogTail(
        text="\n".join(kept),
        line_count=len(kept),
        window={
            "mode": "window",
            "start_line": start_line,
            "end_line": start_line + len(kept) - 1 if kept else None,
            "requested_limit": limit,
            "lines_scanned": scanned,
            "returned_lines": len(kept),
            "clipped": has_more is not False or start_line > 1,
            "header_lines": 0,
            "has_more": has_more,
            "next_start_line": next_line,
            "scope_complete": len(kept) == limit or has_more is False,
            "atomic_snapshot": False,
            "observation_started_at": started,
            "observation_finished_at": observation_time(),
        },
    )


__all__ = ["TaskLogChunk", "TaskLogTail", "tail_task_log", "window_task_log"]
