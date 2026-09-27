"""Recovery waits for a new execution marker, including unseen short runs."""

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
)
from tests.runtime_instance_domain_fakes import install_runtime_instance_domain_runtime
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping

from dsctl.errors import ApiTransportError, WaitTimeoutError
from dsctl.services import workflow_instance as service
from dsctl.services.selection import ResourceDefaults
from dsctl.services.workflow_instance import actions
from dsctl.services.workflow_instance._types import RuntimeInstanceServiceRuntime
from dsctl.upstream.runtime_instances import WorkflowInstanceSnapshot


def _instance(state: str, run_times: int) -> FakeWorkflowInstance:
    return FakeWorkflowInstance(
        id=901,
        project_code_value=7,
        workflow_definition_code_value=101,
        workflow_definition_version_value=1,
        name="daily-901",
        state_value=FakeEnumValue(state),
        run_times_value=run_times,
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeWorkflowInstanceAdapter,
    *,
    version: str = "3.4.1",
) -> None:
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="etl prod's")]
        ),
        workflow_instance_adapter=adapter,
        profile=make_profile(ds_version=version),
        context=ResourceDefaults(project="etl prod's"),
    )
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.watch.time.sleep", lambda _: None
    )


@pytest.mark.parametrize("final_state", ["SUCCESS", "FAILURE"])
def test_recovery_baseline_accepts_new_terminal_without_observing_running(
    monkeypatch: pytest.MonkeyPatch,
    final_state: str,
) -> None:
    old = _instance("FAILURE", 5)
    adapter = FakeWorkflowInstanceAdapter(
        [old],
        workflow_instance_sequences_by_id={
            901: [old, old, old, _instance(final_state, 6)],
        },
    )
    _install(monkeypatch, adapter)
    receipt = service.recover_failed_workflow_instance_result(901)
    assert receipt.resolved["execution_baseline"] == {"run_times": 5}
    assert assert_mapping(receipt.data)["state"] == "FAILURE"
    result = service.watch_workflow_instance_result(
        901, after_run_times=5, interval_seconds=1
    )
    assert assert_mapping(result.data)["runTimes"] == 6
    assert assert_mapping(result.data)["state"] == final_state


def test_watch_does_not_accept_old_terminal_while_command_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, FakeWorkflowInstanceAdapter([_instance("FAILURE", 5)]))
    times = iter([0.0, 2.0])
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.watch.time.monotonic", lambda: next(times)
    )
    with pytest.raises(WaitTimeoutError) as caught:
        service.watch_workflow_instance_result(
            901, after_run_times=5, timeout_seconds=1
        )
    assert caught.value.details["new_execution_observed"] is False
    assert caught.value.details["last_run_times"] == 5


@pytest.mark.parametrize("action", ["stop", "rerun", "recover-failed"])
def test_control_readback_failure_preserves_known_instance_and_replay_marker(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    original = _instance("RUNNING_EXECUTION" if action == "stop" else "FAILURE", 5)
    _install(monkeypatch, FakeWorkflowInstanceAdapter([original]))

    def fail_read(
        _runtime: RuntimeInstanceServiceRuntime,
        *,
        project_selector: str,
        workflow_instance_id: int,
    ) -> WorkflowInstanceSnapshot:
        del project_selector, workflow_instance_id
        message = "readback unavailable"
        raise ApiTransportError(message)

    monkeypatch.setattr(actions, "get_workflow_instance", fail_read)
    functions = {
        "stop": service.stop_workflow_instance_result,
        "rerun": service.rerun_workflow_instance_result,
        "recover-failed": service.recover_failed_workflow_instance_result,
    }
    with pytest.raises(ApiTransportError) as caught:
        functions[action](901)
    assert caught.value.details["mutation_applied"] is True
    assert caught.value.details["id"] == 901
    if action != "stop":
        assert caught.value.details["execution_baseline"] == {"run_times": 5}
