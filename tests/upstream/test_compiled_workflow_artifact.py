"""Installed workflow ownership and sharing without retired exact packages."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from ds_codegen.compiled_workflow_runtime import WORKFLOW_RUNTIME
from ds_codegen.runtime_bundles import _COMPILED_DOMAINS
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)
from dsctl.generated.wire_programs import _manifest
from live_gate.exact_profile_read_corpus import load_tracked_artifacts
from live_gate.runtime_ownership import load_current_runtime_ownership

_ROOT = Path(__file__).resolve().parents[2]
_PROBE = f"""
import sys
sys.path.insert(0, {str(_ROOT / "src")!r})

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.wire import WireExecutionMode, WireResultEnvelope

profiles = [WORKFLOW_PROGRAMS.profile(version) for version in TARGET_DS_VERSIONS]
assert sum(len(profile.programs) for profile in profiles) == 1182
# The 3.4.3 controller removes standalone task update; the other modern
# workflow primitives remain present, including native trigger lookup.
profile_343 = WORKFLOW_PROGRAMS.profile('3.4.3')
assert len(profile_343.programs) == 33
assert 'task_update' not in profile_343.programs
assert {{
    profile.ds_version for profile in profiles if 'instance_trigger' in profile.programs
}} == {{
    '3.2.0', '3.2.1', '3.2.2', '3.3.1', '3.3.2', '3.4.0', '3.4.1', '3.4.2', '3.4.3'
}}
for profile in profiles:
    assert WORKFLOW_PROGRAMS.profile(profile.ds_version) is profile
    sources = [program.source_operation for program in profile.programs.values()]
    assert len(sources) == len(set(sources))
    delete = profile.program('definition_delete')
    assert delete.execution_mode is WireExecutionMode.MUTATION_ONCE
    assert delete.result_envelope is WireResultEnvelope.REQUIRED
    detail = profile.program('definition_get')
    assert detail.execution_mode is WireExecutionMode.READ_RETRY_SAFE
    assert detail.result_envelope is WireResultEnvelope.OPTIONAL

assert not any(name.startswith('dsctl.generated.versions.') for name in sys.modules)
"""


def test_workflow_program_inventory_is_cached_and_independent_of_exact_packages() -> (
    None
):
    result = subprocess.run(  # noqa: S603 - fixed interpreter and compiler probe
        [sys.executable, "-I", "-c", _PROBE],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_static_ownership_proves_every_reviewed_workflow_root() -> None:
    expected_modules = tuple(
        sorted(f"{definition.name}.py" for definition in _COMPILED_DOMAINS)
    )
    assert expected_modules == _manifest.DOMAIN_MODULES
    ownership = load_current_runtime_ownership(
        _ROOT, contracts=load_tracked_artifacts(_ROOT).contracts
    )
    for version, owned in ownership.items():
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        expected = WORKFLOW_RUNTIME.semantic_operations & bindings.keys()
        assert expected <= owned
        assert ("workflow.inspect" in owned) is (version == "3.4.2")
