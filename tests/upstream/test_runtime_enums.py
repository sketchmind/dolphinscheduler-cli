from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from dsctl.generated.versions.ds_3_4_1.common.enums.task_execute_type import (
    TaskExecuteType,
)
from dsctl.generated.versions.ds_3_4_1.common.enums.workflow_execution_status import (
    WorkflowExecutionStatus,
)
from dsctl.generated.versions.ds_3_4_1.plugin.task_api.enums import (
    task_execution_status,
)
from dsctl.upstream import runtime_enums


def test_runtime_enum_domain_facts_match_stable_generated_contract() -> None:
    assert TaskExecuteType.BATCH.value == runtime_enums.TASK_EXECUTE_TYPE_BATCH_VALUE
    assert (
        WorkflowExecutionStatus.STOP.value
        == runtime_enums.WORKFLOW_EXECUTION_STOP_STATE
    )
    assert (
        WorkflowExecutionStatus.FAILURE.value
        == runtime_enums.WORKFLOW_EXECUTION_FAILURE_STATE
    )

    for task_status in task_execution_status.TaskExecutionStatus:
        assert (
            runtime_enums.task_execution_status_value(task_status.name)
            == task_status.value
        )
    for execute_type in TaskExecuteType:
        assert (
            runtime_enums.task_execute_type_value(execute_type.name)
            == execute_type.value
        )
    for workflow_status in WorkflowExecutionStatus:
        info = runtime_enums.workflow_execution_status_info(workflow_status.value)
        assert info is not None
        assert info.value == workflow_status.value
        assert info.can_stop is workflow_status.canStop
        assert info.final_state is workflow_status.finalState


def test_runtime_enum_domain_does_not_import_an_exact_package() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    probe = f"""
import json
import sys
sys.path.insert(0, {str(source_root)!r})
import dsctl.upstream.runtime_enums
print(json.dumps(sorted(
    name for name in sys.modules
    if name.startswith("dsctl.generated.versions.")
)))
"""

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", probe],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == []
