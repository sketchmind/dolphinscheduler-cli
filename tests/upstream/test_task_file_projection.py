from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import UserInputError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)
from tests.value_shape_assertions import assert_mapping, assert_sequence

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

_TASK_REFS = TaskRefIndex.from_code_by_name({})
_FILE_REFS = TaskResourceRefIndex.from_resolved_files(
    ["/scripts/job.sh"],
    id_by_full_name={"/scripts/job.sh": 71},
    wire_full_name_by_full_name={"/scripts/job.sh": "/tenant/resources/scripts/job.sh"},
)


@pytest.mark.parametrize("task_type", ["SHELL", "PYTHON"])
@pytest.mark.parametrize(
    ("version", "attachment"),
    [
        ("2.0.0", {"id": 71}),
        ("3.1.9", {"id": 71}),
        ("3.2.0", {"resourceName": "/tenant/resources/scripts/job.sh"}),
        ("3.2.2", {"resourceName": "/tenant/resources/scripts/job.sh"}),
        ("3.3.1", {"resourceName": "/tenant/resources/scripts/job.sh"}),
        ("3.4.1", {"resourceName": "/tenant/resources/scripts/job.sh"}),
        ("3.4.3", {"resourceName": "/tenant/resources/scripts/job.sh"}),
    ],
)
def test_script_file_identity_matches_independent_native_epoch_and_round_trips(
    task_type: str, version: str, attachment: dict[str, int | str]
) -> None:
    canonical: JsonObject = {
        "rawScript": "scripts/job.sh",
        "resourceList": [{"resourceName": "/scripts/job.sh"}],
    }
    native = encode_task_parameters(
        version=version,
        task_type=task_type,
        task_params=canonical,
        refs=_TASK_REFS,
        resource_refs=_FILE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params
    assert native == {"rawScript": "scripts/job.sh", "resourceList": [attachment]}
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=task_type,
        task_params=native,
        refs=_TASK_REFS,
        resource_refs=_FILE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.task.task_params == canonical
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    unverified = decode_task_parameters_with_provenance(
        version=version,
        task_type=task_type,
        task_params=native,
        refs=_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert unverified.task.task_params == native
    assert unverified.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert (
        encode_task_parameters(
            version=version,
            task_type=task_type,
            task_params=unverified.task.task_params,
            refs=_TASK_REFS,
            source=unverified.reencode_source,
        ).task_params
        == native
    )


@pytest.mark.parametrize("version", ["3.1.9", "3.2.0", "3.4.1"])
def test_compilation_verifies_script_files_before_persistent_materialization(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "script-file"},
            "tasks": [
                {
                    "name": "run-script",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "bash scripts/job.sh",
                        "resourceList": [{"resourceName": "/scripts/job.sh"}],
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
        ),
    )
    prepared = prepare_workflow_create_compilation(spec, catalog=catalog)
    assert prepared.required_resource_full_names == ("/scripts/job.sh",)
    preview = prepared.preview()
    preview_task = assert_mapping(
        assert_sequence(json.loads(preview["taskDefinitionJson"]))[0]
    )
    preview_params = assert_mapping(json.loads(str(preview_task["taskParams"])))
    preview_attachment = assert_mapping(
        assert_sequence(preview_params["resourceList"])[0]
    )
    if version == "3.1.9":
        assert int(str(preview_attachment["id"])) >= 2_000_000_000
    else:
        assert preview_attachment == {
            "resourceName": "/__dsctl_preview__/resources/scripts/job.sh"
        }
    with pytest.raises(UserInputError, match="requires resolved task resource"):
        prepared.materialize([91_000])
    actual = prepared.materialize([91_000], resource_refs=_FILE_REFS)
    task = assert_mapping(assert_sequence(json.loads(actual["taskDefinitionJson"]))[0])
    params = assert_mapping(json.loads(str(task["taskParams"])))
    attachment = assert_mapping(assert_sequence(params["resourceList"])[0])
    assert attachment == (
        {"id": 71}
        if version == "3.1.9"
        else {"resourceName": "/tenant/resources/scripts/job.sh"}
    )


@pytest.mark.parametrize("version", ["3.1.9", "3.4.1"])
def test_script_resource_wire_never_uses_an_unverified_identity(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError, match="verified exact FILE"):
        encode_task_parameters(
            version=version,
            task_type="SHELL",
            task_params={
                "rawScript": "bash scripts/job.sh",
                "resourceList": [{"resourceName": "/scripts/job.sh"}],
            },
            refs=_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
