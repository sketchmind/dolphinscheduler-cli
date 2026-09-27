from __future__ import annotations

import os
import secrets
from typing import TYPE_CHECKING

import pytest

from tests.live.conformance_bundle_gate import (
    ConformanceBundleGateConfig,
    execute_conformance_bundle_scenario,
    load_conformance_bundle_gate_config,
    load_conformance_recovery_run_id,
    recover_existing_full_conformance_state,
    write_conformance_bundle_candidate,
)
from tests.live.support import (
    run_dsctl,
    run_dsctl_raw,
    run_task_definition_cleanup,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [
    pytest.mark.live,
    pytest.mark.live_developer,
    pytest.mark.live_exact_conformance,
    pytest.mark.destructive,
]


@pytest.fixture(scope="session")
def conformance_bundle_runtime() -> ConformanceBundleGateConfig:
    if not os.environ.get("DS_LIVE_CONFORMANCE_ENV_FILE", "").strip():
        pytest.skip(
            "Set the DS_LIVE_CONFORMANCE_* inputs through the installed-wheel "
            "runner to run this gate."
        )
    return load_conformance_bundle_gate_config(os.environ)


def test_exact_conformance_bundle_installed_wheel_gate(
    live_repo_root: Path,
    conformance_bundle_runtime: ConformanceBundleGateConfig,
) -> None:
    runtime = conformance_bundle_runtime
    recovery_run_id = load_conformance_recovery_run_id(os.environ)
    if recovery_run_id is not None:
        recover_existing_full_conformance_state(
            runtime,
            invoke=lambda argv: run_dsctl(
                live_repo_root,
                argv,
                env_file=runtime.env_file,
                executable=runtime.executable,
            ),
            invoke_task_cleanup=lambda operation, project_code, workflow_code, run_id: (
                run_task_definition_cleanup(
                    live_repo_root,
                    python=runtime.python,
                    env_file=runtime.env_file,
                    ds_version=runtime.ds_version,
                    operation=operation,
                    project_code=project_code,
                    workflow_code=workflow_code,
                    run_id=run_id,
                )
            ),
            run_id=recovery_run_id,
        )
        return
    result = execute_conformance_bundle_scenario(
        runtime,
        invoke=lambda argv: run_dsctl(
            live_repo_root,
            argv,
            env_file=runtime.env_file,
            executable=runtime.executable,
        ),
        invoke_raw=lambda argv: run_dsctl_raw(
            live_repo_root,
            argv,
            env_file=runtime.env_file,
            executable=runtime.executable,
        ),
        invoke_task_cleanup=lambda operation, project_code, workflow_code, run_id: (
            run_task_definition_cleanup(
                live_repo_root,
                python=runtime.python,
                env_file=runtime.env_file,
                ds_version=runtime.ds_version,
                operation=operation,
                project_code=project_code,
                workflow_code=workflow_code,
                run_id=run_id,
            )
        ),
        run_id=secrets.token_hex(8),
    )
    write_conformance_bundle_candidate(runtime, result=result)
