from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from tests.live.exact_read_gate import (
    ExactReadGateConfig,
    ExactReadRuntimeConfig,
    execute_exact_read_gate,
    inspect_installed_read,
    load_exact_read_gate_config,
    write_exact_read_evidence,
)
from tests.live.support import run_dsctl

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [
    pytest.mark.live,
    pytest.mark.live_developer,
    pytest.mark.live_exact_read,
]


@pytest.fixture(scope="session")
def exact_read_runtime() -> ExactReadRuntimeConfig:
    if not os.environ.get("DS_LIVE_EXACT_READ_ENV_FILE", "").strip():
        pytest.skip(
            "Set the DS_LIVE_EXACT_READ_* inputs documented in live-testing.md "
            "to run the generic exact-profile read gate."
        )
    return load_exact_read_gate_config(os.environ)


def test_exact_profile_installed_wheel_read_gate(
    live_repo_root: Path,
    exact_read_runtime: ExactReadRuntimeConfig,
) -> None:
    runtime = exact_read_runtime
    installation = inspect_installed_read(runtime)
    scenario = ExactReadGateConfig(
        ds_version=runtime.ds_version,
        family=installation.family,
        support_level=installation.support_level,
        tested=installation.tested,
        attestation_key=runtime.attestation_key,
        cluster=runtime.cluster,
        fixture=runtime.fixture,
    )

    result = execute_exact_read_gate(
        scenario,
        invoke=lambda argv: run_dsctl(
            live_repo_root,
            argv,
            env_file=runtime.env_file,
            executable=runtime.executable,
        ),
    )

    write_exact_read_evidence(
        runtime,
        installation=installation,
        result=result,
    )
