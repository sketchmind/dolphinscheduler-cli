from __future__ import annotations

import pytest
from pydantic import BaseModel

from dsctl.generated.task_definition_cleanup_profiles import (
    TARGET_TASK_DEFINITION_CLEANUP_VERSIONS as _CLEANUP_VERSIONS,
)
from dsctl.generated.task_definition_cleanup_profiles import (
    TASK_DEFINITION_CLEANUP_PROFILES,
)
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS


@pytest.mark.parametrize("version", _CLEANUP_VERSIONS)
def test_exact_cleanup_wire_is_generated_only_for_reviewed_versions(
    version: str,
) -> None:
    profile = WORKFLOW_PROGRAMS.profile(version)
    assert (
        profile.program("task_cleanup_page").source_operation
        == "TaskDefinitionController.queryTaskDefinitionListPaging"
    )
    assert (
        profile.program("task_get").source_operation
        == "TaskDefinitionController.queryTaskDefinitionDetail"
    )
    strategy = TASK_DEFINITION_CLEANUP_PROFILES[version]["strategy"]
    pre_delete_release = TASK_DEFINITION_CLEANUP_PROFILES[version]["pre_delete_release"]
    if pre_delete_release == "offline":
        assert (
            profile.program("task_cleanup_release").source_operation
            == "TaskDefinitionController.releaseTaskDefinition"
        )
    else:
        assert "task_cleanup_release" not in profile.programs
    if strategy == "direct-delete":
        assert (
            profile.program("task_cleanup_delete").source_operation
            == "TaskDefinitionController.deleteTaskDefinitionByCode"
        )
        assert "task_cleanup_history" not in profile.programs
    else:
        assert (
            profile.program("task_cleanup_history").source_operation
            == "TaskDefinitionController.queryTaskDefinitionVersions"
        )
        assert "task_cleanup_delete" not in profile.programs


@pytest.mark.parametrize("version", ["2.0.9", "3.0.0", "3.0.6", "3.1.0"])
def test_generated_delete_accepts_null_and_non_null_result_payloads(
    version: str,
) -> None:
    response = (
        WORKFLOW_PROGRAMS.profile(version)
        .program("task_cleanup_delete")
        .codec.response_adapter
    )
    assert response is not None
    assert response.validate_python(None) is None
    payload = response.validate_python({"name": "workflow"})
    assert isinstance(payload, BaseModel)
    assert payload.model_dump()["name"] == "workflow"


def test_generated_200_delete_has_void_result_payload() -> None:
    response = (
        WORKFLOW_PROGRAMS.profile("2.0.0")
        .program("task_cleanup_delete")
        .codec.response_adapter
    )
    assert response is None


@pytest.mark.parametrize("version", ["2.0.1", "2.0.2", "2.0.3"])
def test_generated_pre_delete_release_has_void_result_payload(version: str) -> None:
    response = (
        WORKFLOW_PROGRAMS.profile(version)
        .program("task_cleanup_release")
        .codec.response_adapter
    )
    assert response is None
