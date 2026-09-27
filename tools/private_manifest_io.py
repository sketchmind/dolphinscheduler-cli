"""Owner-private strict JSON I/O with atomic no-overwrite pair publication."""

from __future__ import annotations

import json
import os
import secrets
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class _OutputDirectory:
    path: Path
    descriptor: int
    identity: tuple[int, int]


@dataclass(frozen=True)
class _OutputTarget:
    path: Path
    directory: _OutputDirectory
    name: str


@dataclass(frozen=True)
class _StagedFile:
    directory: _OutputDirectory
    name: str
    identity: tuple[int, int]


class _DuplicateObjectKeyError(ValueError):
    """A JSON object repeated a member name."""


class _NonFiniteJsonConstantError(ValueError):
    """A JSON document used a non-finite JavaScript number constant."""


def absolute_path(path: Path) -> Path:
    """Return an absolute path without following a final-component symlink."""
    return Path(os.path.abspath(os.fspath(path.expanduser())))  # noqa: PTH100


def load_private_json_object(path: Path, *, label: str) -> dict[str, object]:
    """Read one owner-private, non-symlink regular file as strict JSON."""
    descriptor = _open_private_regular_file(path, label=label)
    with os.fdopen(descriptor, encoding="utf-8") as stream:
        try:
            payload: object = json.load(
                stream,
                object_pairs_hook=_strict_object,
                parse_constant=_reject_non_finite_json_constant,
            )
        except _DuplicateObjectKeyError as error:
            message = f"{label} must not contain duplicate object keys"
            raise ValueError(message) from error
        except _NonFiniteJsonConstantError as error:
            message = f"{label} must not contain non-finite JSON constants"
            raise ValueError(message) from error
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            message = f"{label} must contain valid UTF-8 JSON"
            raise ValueError(message) from error
    if not isinstance(payload, dict):
        message = f"{label} must contain a JSON object"
        raise TypeError(message)
    return payload


def publish_private_json_pair(
    cluster_payload: object,
    cluster_destination: Path,
    fixture_payload: object,
    fixture_destination: Path,
) -> tuple[Path, Path]:
    """Publish two new 0600 JSON files atomically or roll back both."""
    cluster_destination = absolute_path(cluster_destination)
    fixture_destination = absolute_path(fixture_destination)
    if cluster_destination == fixture_destination:
        message = "Cluster and fixture manifests require different output paths"
        raise ValueError(message)
    cluster_directory, fixture_directory = _open_output_directories(
        cluster_destination.parent,
        fixture_destination.parent,
    )
    cluster_target = _OutputTarget(
        path=cluster_destination,
        directory=cluster_directory,
        name=cluster_destination.name,
    )
    fixture_target = _OutputTarget(
        path=fixture_destination,
        directory=fixture_directory,
        name=fixture_destination.name,
    )
    staged: list[_StagedFile] = []
    try:
        _require_new_output(cluster_target)
        _require_new_output(fixture_target)
        cluster_staged = _stage_private_json(cluster_payload, cluster_target)
        staged.append(cluster_staged)
        fixture_staged = _stage_private_json(fixture_payload, fixture_target)
        staged.append(fixture_staged)
        try:
            _link_staged_file(cluster_staged, cluster_target)
            _link_staged_file(fixture_staged, fixture_target)
            _require_directory_binding(cluster_directory)
            _require_directory_binding(fixture_directory)
        except BaseException as error:
            _rollback_published_targets(
                (
                    (cluster_target, cluster_staged.identity),
                    (fixture_target, fixture_staged.identity),
                ),
                cause=error,
            )
            if isinstance(error, FileExistsError):
                message = "Refusing to overwrite an existing projection output"
                raise FileExistsError(message) from error
            if isinstance(error, OSError):
                message = "Private projection publication failed"
                raise OSError(message) from error
            raise
        return cluster_destination, fixture_destination
    finally:
        for temporary in staged:
            _unlink_staged_file(temporary)
        _close_output_directories(cluster_directory, fixture_directory)


def _strict_object(pairs: Sequence[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateObjectKeyError
        result[key] = value
    return result


def _reject_non_finite_json_constant(_constant: str) -> None:
    raise _NonFiniteJsonConstantError


def _require_private_regular_status(status: os.stat_result, *, label: str) -> None:
    if (
        not stat.S_ISREG(status.st_mode)
        or status.st_uid != os.geteuid()
        or (status.st_mode & 0o400) == 0
        or status.st_mode & 0o077
    ):
        message = (
            f"{label} must be an owner-private regular file and must not be "
            "accessible by group or others"
        )
        raise PermissionError(message)


def _open_private_regular_file(path: Path, *, label: str) -> int:
    descriptor: int | None = None
    try:
        link_status = path.lstat()
    except OSError as error:
        message = f"{label} must be an owner-private regular file"
        raise ValueError(message) from error
    if stat.S_ISLNK(link_status.st_mode):
        message = f"{label} must be an owner-private regular file"
        raise ValueError(message)
    _require_private_regular_status(link_status, label=label)
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        file_status = os.fstat(descriptor)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        message = f"{label} must be an owner-private regular file"
        raise ValueError(message) from error
    try:
        _require_private_regular_status(file_status, label=label)
    except PermissionError:
        os.close(descriptor)
        raise
    if (file_status.st_dev, file_status.st_ino) != (
        link_status.st_dev,
        link_status.st_ino,
    ):
        os.close(descriptor)
        message = f"{label} changed while opening its private regular file"
        raise PermissionError(message)
    return descriptor


def _open_output_directories(
    cluster_parent: Path,
    fixture_parent: Path,
) -> tuple[_OutputDirectory, _OutputDirectory]:
    cluster_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fixture_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cluster_directory = _open_private_output_directory(cluster_parent)
    if fixture_parent == cluster_parent:
        return cluster_directory, cluster_directory
    try:
        fixture_directory = _open_private_output_directory(fixture_parent)
    except BaseException:
        os.close(cluster_directory.descriptor)
        raise
    return cluster_directory, fixture_directory


def _open_private_output_directory(path: Path) -> _OutputDirectory:
    try:
        link_status = path.lstat()
    except OSError as error:
        message = "Projection output parent must be an owner-private directory"
        raise PermissionError(message) from error
    if not stat.S_ISDIR(link_status.st_mode):
        message = "Projection output parent must be an owner-private directory"
        raise PermissionError(message)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        opened_status = os.fstat(descriptor)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        message = "Projection output parent must be an owner-private directory"
        raise PermissionError(message) from error
    if (
        not stat.S_ISDIR(opened_status.st_mode)
        or opened_status.st_uid != os.geteuid()
        or opened_status.st_mode & 0o077
        or (opened_status.st_dev, opened_status.st_ino)
        != (link_status.st_dev, link_status.st_ino)
    ):
        os.close(descriptor)
        message = "Projection output parent must be an owner-private directory"
        raise PermissionError(message)
    return _OutputDirectory(
        path=path,
        descriptor=descriptor,
        identity=(opened_status.st_dev, opened_status.st_ino),
    )


def _require_new_output(target: _OutputTarget) -> None:
    try:
        os.stat(
            target.name,
            dir_fd=target.directory.descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    message = "Refusing to overwrite an existing projection output"
    raise FileExistsError(message)


def _stage_private_json(payload: object, target: _OutputTarget) -> _StagedFile:
    try:
        raw = (
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    except (TypeError, ValueError) as error:
        message = "Projection payload must contain only JSON compliant values"
        raise ValueError(message) from error
    temporary_name, descriptor = _create_private_temporary(target)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        status = os.stat(
            temporary_name,
            dir_fd=target.directory.descriptor,
            follow_symlinks=False,
        )
    except BaseException:
        _unlink_directory_entry(target.directory, temporary_name)
        raise
    return _StagedFile(
        directory=target.directory,
        name=temporary_name,
        identity=(status.st_dev, status.st_ino),
    )


def _create_private_temporary(target: _OutputTarget) -> tuple[str, int]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _attempt in range(16):
        name = f".{target.name}.{secrets.token_hex(8)}.tmp"
        try:
            descriptor = os.open(
                name,
                flags,
                0o600,
                dir_fd=target.directory.descriptor,
            )
        except FileExistsError:
            continue
        try:
            os.fchmod(descriptor, 0o600)
        except BaseException:
            os.close(descriptor)
            _unlink_directory_entry(target.directory, name)
            raise
        return name, descriptor
    message = "Could not allocate a private projection temporary file"
    raise RuntimeError(message)


def _link_staged_file(staged: _StagedFile, target: _OutputTarget) -> None:
    os.link(
        staged.name,
        target.name,
        src_dir_fd=staged.directory.descriptor,
        dst_dir_fd=target.directory.descriptor,
        follow_symlinks=False,
    )


def _require_directory_binding(directory: _OutputDirectory) -> None:
    try:
        status = directory.path.lstat()
    except OSError as error:
        message = "Projection output parent changed during publication"
        raise RuntimeError(message) from error
    if (
        not stat.S_ISDIR(status.st_mode)
        or (status.st_dev, status.st_ino) != directory.identity
    ):
        message = "Projection output parent changed during publication"
        raise RuntimeError(message)


def _rollback_published_targets(
    targets: tuple[tuple[_OutputTarget, tuple[int, int]], ...],
    *,
    cause: BaseException,
) -> None:
    rollback_error: OSError | None = None
    for target, identity in targets:
        try:
            _unlink_matching_target(target, identity=identity)
        except OSError as error:
            rollback_error = error
    if rollback_error is not None:
        message = "Could not roll back partial projection publication"
        raise RuntimeError(message) from cause


def _unlink_matching_target(
    target: _OutputTarget,
    *,
    identity: tuple[int, int],
) -> None:
    try:
        status = os.stat(
            target.name,
            dir_fd=target.directory.descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    if (status.st_dev, status.st_ino) == identity:
        os.unlink(target.name, dir_fd=target.directory.descriptor)


def _unlink_staged_file(staged: _StagedFile) -> None:
    _unlink_directory_entry(staged.directory, staged.name)


def _unlink_directory_entry(directory: _OutputDirectory, name: str) -> None:
    with suppress(FileNotFoundError):
        os.unlink(name, dir_fd=directory.descriptor)


def _close_output_directories(*directories: _OutputDirectory) -> None:
    closed: set[int] = set()
    for directory in directories:
        if directory.descriptor in closed:
            continue
        os.close(directory.descriptor)
        closed.add(directory.descriptor)


__all__ = [
    "absolute_path",
    "load_private_json_object",
    "publish_private_json_pair",
]
