from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from tests.live.support import (
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from tests.live.support import DsctlCommandResult


IdentityKind = Literal["id", "code"]


@dataclass(frozen=True)
class ProjectNativeIdentity:
    """One exact-version project identity returned by ``resolved.project``."""

    kind: IdentityKind
    value: int


class ProjectLifecycleCleanup:
    """Freshly reconcile and remove one uniquely named live-test project."""

    def __init__(
        self,
        repo_root: Path,
        admin_env_file: Path,
        *,
        initial_name: str,
        updated_name: str | None,
        owned_descriptions: tuple[str, ...],
        execute: Callable[..., DsctlCommandResult],
        before_delete: Callable[[ProjectNativeIdentity, Mapping[str, object]], None]
        | None = None,
    ) -> None:
        if updated_name is not None and initial_name == updated_name:
            message = "Project lifecycle names must be distinct"
            raise ValueError(message)
        self._repo_root = repo_root
        self._admin_env_file = admin_env_file
        self._names = (
            (initial_name, updated_name)
            if updated_name is not None
            else (initial_name,)
        )
        self._owned_descriptions = frozenset(owned_descriptions)
        self._execute = execute
        self._before_delete = before_delete
        self._mutation_attempted = False
        self._expected_identity: ProjectNativeIdentity | None = None

    def prepare(self) -> None:
        """Prove every configured name absent before the first mutation attempt."""
        self._prove_absent(identity=None, label="project lifecycle preflight")

    def mark_create_attempted(self) -> None:
        """Fence cleanup before dispatching the possibly uncertain create."""
        self._mutation_attempted = True

    def remember_identity(
        self,
        payload: Mapping[str, object],
        *,
        label: str,
    ) -> ProjectNativeIdentity:
        """Remember the profile-native identity without aliasing database ids."""
        identity = _native_identity(payload, label=label)
        if self._expected_identity not in {None, identity}:
            message = "Project lifecycle native identity changed"
            raise AssertionError(message)
        self._expected_identity = identity
        return identity

    def cleanup(self, *, expected_absent: bool = False) -> None:
        """Delete only freshly proven owned residue, then prove full absence."""
        if not self._mutation_attempted:
            return

        located = self._locate_owned_candidate()
        if located is None:
            self._prove_absent(
                identity=self._expected_identity,
                label="project lifecycle cleanup",
            )
            return

        identity, data = located
        self._require_expected_owned_project(identity, data)
        self._expected_identity = identity
        if self._before_delete is not None:
            self._before_delete(identity, data)
        try:
            # A lost response is not retried. The finally proof still runs,
            # while Python retains the original delete exception when absent.
            self._execute(
                self._repo_root,
                ["project", "delete", str(identity.value), "--force"],
                env_file=self._admin_env_file,
            )
        finally:
            self._prove_absent(
                identity=self._expected_identity,
                label="project lifecycle cleanup",
            )
        if expected_absent:
            message = "ETL project deletion left administrator-visible project residue"
            raise AssertionError(message)

    def _locate_owned_candidate(
        self,
    ) -> tuple[ProjectNativeIdentity, dict[str, object]] | None:
        located: dict[ProjectNativeIdentity, dict[str, object]] = {}
        for name in self._names:
            project = self._fresh_get(name)
            if project is None:
                continue
            identity, data = project
            if identity in located:
                message = "Both lifecycle names resolved to one project identity"
                raise AssertionError(message)
            located[identity] = data

        if len(located) > 1:
            message = "Project lifecycle cleanup found multiple project identities"
            raise AssertionError(message)
        return next(iter(located.items())) if located else None

    def _require_expected_owned_project(
        self,
        identity: ProjectNativeIdentity,
        data: Mapping[str, object],
    ) -> None:
        if self._expected_identity not in {None, identity}:
            message = "Project lifecycle cleanup found an unexpected native identity"
            raise AssertionError(message)
        if data.get("name") not in self._names:
            message = "Project lifecycle cleanup found an unexpected project name"
            raise AssertionError(message)
        if data.get("description") not in self._owned_descriptions:
            message = "Project lifecycle cleanup refused an unowned project residue"
            raise AssertionError(message)

    def _fresh_get(
        self,
        name: str,
    ) -> tuple[ProjectNativeIdentity, dict[str, object]] | None:
        result = self._execute(
            self._repo_root,
            ["project", "get", name],
            env_file=self._admin_env_file,
        )
        if result.exit_code != 0:
            require_error_payload(
                result,
                expected_action="project.get",
                expected_type="not_found",
                label=f"project lifecycle cleanup get {name}",
            )
            return None

        payload = require_ok_payload(
            result,
            expected_action="project.get",
            label=f"project lifecycle cleanup get {name}",
        )
        data = require_mapping(
            payload.get("data"),
            label=f"project lifecycle cleanup data {name}",
        )
        if data.get("name") != name:
            message = "Project lifecycle cleanup name readback differs"
            raise AssertionError(message)
        identity = _native_identity(payload, label="project lifecycle cleanup")
        return identity, data

    def _prove_absent(
        self,
        *,
        identity: ProjectNativeIdentity | None,
        label: str,
    ) -> None:
        for name in self._names:
            result = self._execute(
                self._repo_root,
                ["project", "get", name],
                env_file=self._admin_env_file,
            )
            require_error_payload(
                result,
                expected_action="project.get",
                expected_type="not_found",
                label=f"{label} get {name}",
            )

        if identity is not None:
            require_error_payload(
                self._execute(
                    self._repo_root,
                    ["project", "get", str(identity.value)],
                    env_file=self._admin_env_file,
                ),
                expected_action="project.get",
                expected_type="not_found",
                label=f"{label} get native identity",
            )

        for name in self._names:
            payload = require_ok_payload(
                self._execute(
                    self._repo_root,
                    [
                        "project",
                        "list",
                        "--search",
                        name,
                        "--page-no",
                        "1",
                        "--page-size",
                        "100",
                    ],
                    env_file=self._admin_env_file,
                ),
                expected_action="project.list",
                label=f"{label} empty search {name}",
            )
            resolved = require_mapping(
                payload.get("resolved"),
                label=f"{label} empty search resolved {name}",
            )
            data = require_mapping(
                payload.get("data"),
                label=f"{label} empty search data {name}",
            )
            rows = require_list(data.get("totalList"), label=f"{label} rows")
            page_size = data.get("pageSize")
            if (
                resolved.get("all") is not False
                or resolved.get("search") != name
                or rows != []
                or data.get("total") != 0
                or data.get("currentPage") != 1
                or data.get("pageNo") != 1
                or data.get("totalPage") not in {0, 1}
                or not isinstance(page_size, int)
                or isinstance(page_size, bool)
                or page_size <= 0
            ):
                message = "Project lifecycle search did not prove an empty first page"
                raise AssertionError(message)


def _native_identity(
    payload: Mapping[str, object],
    *,
    label: str,
) -> ProjectNativeIdentity:
    resolved = require_mapping(payload.get("resolved"), label=f"{label} resolved")
    project = require_mapping(
        resolved.get("project"),
        label=f"{label} resolved project",
    )
    present = [kind for kind in ("id", "code") if kind in project]
    if len(present) != 1:
        message = f"{label} did not expose exactly one native project identity"
        raise AssertionError(message)
    kind = present[0]
    value = project.get(kind)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        message = f"{label} native project {kind} must be a positive integer"
        raise AssertionError(message)
    return ProjectNativeIdentity(kind=cast("IdentityKind", kind), value=value)
