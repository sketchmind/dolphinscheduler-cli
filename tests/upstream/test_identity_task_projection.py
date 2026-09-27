from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from dsctl.models.task_spec import (
    DinkyJobTriggerTaskParamsSpec,
    DmsResumeExistingFullLoadTaskParamsSpec,
    OpenmldbLiteralSingleStatementTaskParamsSpec,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.task_spec import TaskParamsSpec
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection import ProjectionDirection


_VALID: dict[str, JsonObject] = {
    "DINKY": {"taskId": "daily-job_7", "address": "https://dinky.example.com/platform"},
    "DMS": {
        "replicationTaskArn": "arn:aws:dms:us-east-1:123456789012:task:EXAMPLE",
        "startReplicationTaskType": "resume-processing",
        "migrationType": "full-load",
        "isJsonFormat": False,
        "isRestartTask": True,
    },
    "OPENMLDB": {
        "sql": "SELECT 'e\u0301'\nFROM data\tWHERE id = 1",
        "executeMode": "offline",
        "zkPath": "/openmldb",
        "zk": "zk.example.com:2181",
    },
}
_MODELS: dict[str, type[TaskParamsSpec]] = {
    "DINKY": DinkyJobTriggerTaskParamsSpec,
    "DMS": DmsResumeExistingFullLoadTaskParamsSpec,
    "OPENMLDB": OpenmldbLiteralSingleStatementTaskParamsSpec,
}
_SUBSETS = {
    "DINKY": ("outside-reviewed-job-trigger-subset", "DINKY job-trigger projection"),
    "DMS": (
        "outside-reviewed-existing-full-load-resume-subset",
        "DMS resume projection",
    ),
    "OPENMLDB": (
        "outside-reviewed-literal-single-statement-subset",
        "OPENMLDB literal single-statement projection",
    ),
}


def _project(
    task_type: str,
    payload: JsonObject,
    *,
    direction: ProjectionDirection,
    source: ProjectionSource = ProjectionSource.TYPED_AUTHORING,
) -> JsonObject:
    project = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )
    return project(
        version="3.4.1",
        task_type=task_type,
        task_params=payload,
        refs=TaskRefIndex.from_code_by_name({}),
        source=source,
    ).task_params


@pytest.mark.parametrize("task_type", tuple(_VALID))
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_identity_projection_preserves_authored_spelling_and_member_order(
    task_type: str,
    direction: ProjectionDirection,
) -> None:
    authored = deepcopy(_VALID[task_type])
    expected = deepcopy(authored)
    if task_type == "DINKY":
        expected["online"] = False

    result = _project(task_type, authored, direction=direction)

    assert list(result.items()) == list(expected.items())
    assert authored == _VALID[task_type]
    assert result is not authored


@pytest.mark.parametrize("task_type", tuple(_VALID))
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_unowned_native_fields_precede_missing_fields_in_sorted_order(
    task_type: str,
    direction: ProjectionDirection,
) -> None:
    with pytest.raises(TaskParameterProjectionError) as exc_info:
        _project(task_type, {"zFuture": 1, "aFuture": 2}, direction=direction)

    reason, label = _SUBSETS[task_type]
    assert exc_info.value.details["field"] == "task_params.aFuture"
    assert exc_info.value.details["reason"] == reason
    assert str(exc_info.value) == f"{label} does not own fields: aFuture, zFuture"


@pytest.mark.parametrize(
    ("task_type", "native", "python_name"),
    [
        ("DINKY", "taskId", "task_id"),
        ("DMS", "replicationTaskArn", "replication_task_arn"),
        ("OPENMLDB", "zkPath", "zk_path"),
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_native_projection_does_not_accept_canonical_python_aliases(
    task_type: str,
    native: str,
    python_name: str,
    direction: ProjectionDirection,
) -> None:
    payload = deepcopy(_VALID[task_type])
    payload[python_name] = payload.pop(native)
    _MODELS[task_type].model_validate(payload, strict=True)

    with pytest.raises(TaskParameterProjectionError) as exc_info:
        _project(task_type, payload, direction=direction)

    assert exc_info.value.details["field"] == f"task_params.{python_name}"
    assert exc_info.value.details["reason"] == _SUBSETS[task_type][0]


@pytest.mark.parametrize(
    ("overrides", "removed", "field", "reason", "message"),
    [
        (
            {"isRestartTask": 1},
            (),
            "isRestartTask",
            "wrong-resume-mode",
            "DMS resume requires task_params.isRestartTask=True",
        ),
        (
            {"isJsonFormat": 0},
            (),
            "isJsonFormat",
            "wrong-resume-mode",
            "DMS resume requires task_params.isJsonFormat=False",
        ),
        (
            {"migrationType": 0},
            (),
            "migrationType",
            "wrong-resume-mode",
            "DMS resume requires task_params.migrationType='full-load'",
        ),
        (
            {"replicationTaskArn": None},
            (),
            "replicationTaskArn",
            "invalid-type",
            "DMS resume requires one literal replicationTaskArn string",
        ),
        (
            {"replicationTaskArn": "${arn}"},
            (),
            "replicationTaskArn",
            "unsafe-replication-task-arn",
            "DMS replicationTaskArn is outside the safe literal task subset",
        ),
        (
            {"replicationTaskArn": ""},
            (),
            "replicationTaskArn",
            "unsafe-replication-task-arn",
            "DMS replicationTaskArn is outside the safe literal task subset",
        ),
        (
            {"replicationTaskArn": "arn:aws:dms:us-east-1:123456789012:task:\ud800"},
            (),
            "replicationTaskArn",
            "unsafe-replication-task-arn",
            "DMS replicationTaskArn is outside the safe literal task subset",
        ),
        (
            {"isJsonFormat": True, "replicationTaskArn": None},
            ("isRestartTask",),
            "isRestartTask",
            "missing-required-field",
            "DMS resume requires task_params.isRestartTask",
        ),
        (
            {"migrationType": "full-load", "startReplicationTaskType": "reload-target"},
            (),
            "startReplicationTaskType",
            "wrong-resume-mode",
            "DMS resume requires task_params.startReplicationTaskType="
            "'resume-processing'",
        ),
        (
            {},
            ("replicationTaskArn",),
            "replicationTaskArn",
            "missing-required-field",
            "DMS resume requires one literal replicationTaskArn string",
        ),
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_dms_model_errors_keep_the_existing_projection_contract(
    overrides: JsonObject,
    removed: tuple[str, ...],
    field: str,
    reason: str,
    message: str,
    direction: ProjectionDirection,
) -> None:
    payload = {**_VALID["DMS"], **overrides}
    for key in removed:
        payload.pop(key)

    with pytest.raises(TaskParameterProjectionError) as exc_info:
        _project("DMS", payload, direction=direction)

    assert exc_info.value.details["field"] == f"task_params.{field}"
    assert exc_info.value.details["reason"] == reason
    assert str(exc_info.value) == message


@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_openmldb_sql_error_keeps_priority_over_execution_mode(
    direction: ProjectionDirection,
) -> None:
    payload = {**_VALID["OPENMLDB"], "sql": "SELECT 1;", "executeMode": None}

    with pytest.raises(TaskParameterProjectionError) as exc_info:
        _project("OPENMLDB", payload, direction=direction)

    assert exc_info.value.details["field"] == "task_params.sql"
    assert exc_info.value.details["reason"] == "unsafe-value"
    assert str(exc_info.value) == (
        "OPENMLDB task_params.sql is outside the safe literal subset"
    )


@pytest.mark.parametrize(
    ("task_type", "field", "value"),
    [
        ("DINKY", "address", "https://dinky.example.com/\ud800"),
        ("OPENMLDB", "sql", "\ud800"),
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
@pytest.mark.parametrize("source", list(ProjectionSource))
def test_legacy_projection_keeps_its_distinct_surrogate_boundary(
    task_type: str,
    field: str,
    value: str,
    direction: ProjectionDirection,
    source: ProjectionSource,
) -> None:
    payload = {**_VALID[task_type], field: value}
    with pytest.raises(ValidationError) as exc_info:
        _MODELS[task_type].model_validate(payload, strict=True)
    assert exc_info.value.errors()[0]["type"] == "string_unicode"

    projected = _project(task_type, payload, direction=direction, source=source)

    assert projected[field] == value
