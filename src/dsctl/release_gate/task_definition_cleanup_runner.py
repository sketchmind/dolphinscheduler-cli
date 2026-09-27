from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from dsctl.client import DolphinSchedulerClient
from dsctl.config import ClusterProfile, load_profile
from dsctl.errors import ApiResultError
from dsctl.release_gate.task_definition_cleanup import (
    TaskDefinitionCleanupError,
    TaskDefinitionCleanupPort,
    TaskMutationAmbiguityUnresolvedError,
    cleanup_owned_task_residue,
    prove_owned_task_inventory,
)
from dsctl.upstream.task_definition_cleanup import (
    TaskDefinitionCleanupAdapter,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from dsctl.support.json_types import JsonObject


class ProfileLoader(Protocol):
    """Load one exact cluster profile selected by the private runner."""

    def __call__(self, env_file: Path) -> ClusterProfile: ...


class ClientFactory(Protocol):
    """Create one context-managed client for the selected profile."""

    def __call__(
        self,
        profile: ClusterProfile,
    ) -> AbstractContextManager[DolphinSchedulerClient]: ...


class PortFactory(Protocol):
    """Bind the exact generated cleanup port."""

    def __call__(
        self,
        profile: ClusterProfile,
        client: DolphinSchedulerClient,
    ) -> TaskDefinitionCleanupPort: ...


def _create_client(
    profile: ClusterProfile,
) -> AbstractContextManager[DolphinSchedulerClient]:
    """Create the concrete REST client for the selected exact profile."""
    return DolphinSchedulerClient(profile)


def main(
    argv: list[str] | None = None,
    *,
    profile_loader: ProfileLoader = load_profile,
    client_factory: ClientFactory = _create_client,
    port_factory: PortFactory | None = None,
) -> int:
    """Run one installed-wheel-only proof or cleanup command."""
    args = _parser().parse_args(argv)
    operation = cast("str", args.operation)
    project_code = cast("int", args.project_code)
    workflow_code = cast("int | None", args.workflow_code)
    run_id = cast("str", args.run_id)
    env_file = cast("Path", args.env_file)
    selected_port_factory = port_factory or _bind_generated_port
    ds_version: str | None = None
    try:
        profile = profile_loader(env_file)
        ds_version = _profile_version(profile)
        with client_factory(profile) as client:
            port = selected_port_factory(profile, client)
            if operation == "prove":
                inventory = prove_owned_task_inventory(
                    port,
                    project_code=project_code,
                    workflow_code=workflow_code,
                    run_id=run_id,
                )
                payload = {
                    **_success_header(
                        operation=operation,
                        ds_version=ds_version,
                    ),
                    "observed": len(inventory.tasks),
                    "released": 0,
                    "deleted": 0,
                    "remaining": len(inventory.tasks),
                    "remote_mutations": 0,
                }
            else:
                report = cleanup_owned_task_residue(
                    port,
                    project_code=project_code,
                    workflow_code=workflow_code,
                    run_id=run_id,
                )
                payload = {
                    **_success_header(
                        operation=operation,
                        ds_version=ds_version,
                    ),
                    "observed": report.observed,
                    "released": report.released,
                    "deleted": report.deleted,
                    "remaining": report.remaining,
                    "remote_mutations": report.remote_mutations,
                }
    except TaskMutationAmbiguityUnresolvedError as exc:
        _emit(
            _failure_payload(
                operation=operation,
                ds_version=ds_version,
                error_type="mutation_ambiguity_unresolved",
                message=str(exc),
                do_not_retry=True,
            )
        )
        return 2
    except ApiResultError as exc:
        result_code = exc.result_code if type(exc.result_code) is int else None
        deterministic_rejection = result_code == 50050
        _emit(
            _failure_payload(
                operation=operation,
                ds_version=ds_version,
                error_type="api_result_error",
                message=(
                    _bounded_api_result_message(result_code)
                    if deterministic_rejection or operation == "prove"
                    else (
                        "DolphinScheduler returned an unreviewed cleanup rejection; "
                        "reconcile task state before any retry"
                    )
                ),
                do_not_retry=operation == "cleanup" and not deterministic_rejection,
                result_code=result_code,
            )
        )
        return 2
    except TaskDefinitionCleanupError:
        _emit(
            _failure_payload(
                operation=operation,
                ds_version=ds_version,
                error_type="precondition_not_proven",
                message="task-definition cleanup precondition was not proven",
            )
        )
        return 2
    except Exception:
        _emit(
            _failure_payload(
                operation=operation,
                ds_version=ds_version,
                error_type="internal_error",
                message="task-definition cleanup failed",
            )
        )
        return 2
    _emit(payload)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -I -m dsctl.release_gate.task_definition_cleanup",
    )
    parser.add_argument("operation", choices=("prove", "cleanup"))
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--project-code", type=int, required=True)
    parser.add_argument("--workflow-code", type=int)
    parser.add_argument("--run-id", required=True)
    return parser


def _bind_generated_port(
    profile: ClusterProfile,
    client: DolphinSchedulerClient,
) -> TaskDefinitionCleanupPort:
    return TaskDefinitionCleanupAdapter.for_version(profile.ds_version).bind(
        profile, http_client=client
    )


def _profile_version(profile: ClusterProfile) -> str:
    if type(profile.ds_version) is not str or not profile.ds_version:
        message = "cleanup profile has no exact DS version"
        raise TaskDefinitionCleanupError(message)
    return profile.ds_version


def _success_header(
    *,
    operation: str,
    ds_version: str,
) -> JsonObject:
    return {
        "schema_version": 2,
        "ok": True,
        "operation": operation,
        "ds_version": ds_version,
    }


def _failure_payload(
    *,
    operation: str,
    ds_version: str | None,
    error_type: str,
    message: str,
    do_not_retry: bool = False,
    result_code: int | None = None,
) -> JsonObject:
    error: JsonObject = {"type": error_type, "message": message}
    if do_not_retry:
        error["do_not_retry"] = True
    if result_code is not None:
        error["result_code"] = result_code
    return {
        "schema_version": 2,
        "ok": False,
        "operation": operation,
        "ds_version": ds_version,
        "error": error,
    }


def _bounded_api_result_message(result_code: int | None) -> str:
    if result_code == 50050:
        return (
            "DolphinScheduler rejected task-definition cleanup because a task "
            "remains online; offline the task and retry cleanup"
        )
    return (
        "DolphinScheduler rejected task-definition cleanup; inspect task state "
        "and permissions before retrying"
    )


def _emit(payload: JsonObject) -> None:
    sys.stdout.write(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )


__all__ = ["main"]
