from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, Literal, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.models import WorkflowSpec
from dsctl.services._workflow.authoring import workflow_authoring_catalog_for_version
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.resolver import ResolvedProject

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue


DvcMode = Literal["Upload", "Download", "Init DVC"]

_DVC_FACET = "DVC/operation"
_DVC_VERSIONS = (
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_DVC_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_DVC_VARIANTS: tuple[tuple[str, DvcMode], ...] = (
    ("upload", "Upload"),
    ("download", "Download"),
    ("init", "Init DVC"),
)
_COMMON_FIELDS = {"dvcTaskType", "dvcRepository"}
_MODE_FIELDS: dict[DvcMode, set[str]] = {
    "Upload": {
        "dvcDataLocation",
        "dvcLoadSaveDataPath",
        "dvcVersion",
        "dvcMessage",
    },
    "Download": {
        "dvcDataLocation",
        "dvcLoadSaveDataPath",
        "dvcVersion",
    },
    "Init DVC": {"dvcStoreUrl"},
}
_ALL_TYPED_FIELDS = _COMMON_FIELDS | set().union(*_MODE_FIELDS.values())


def _canonical_params(mode: DvcMode) -> YamlObject:
    common: YamlObject = {
        "dvcTaskType": mode,
        "dvcRepository": "git@github.com:acme/ml-data.git",
    }
    if mode == "Upload":
        return {
            **common,
            "dvcDataLocation": "datasets/iris-v1.2",
            "dvcLoadSaveDataPath": "~/datasets/iris-v1.2",
            "dvcVersion": "iris_v2.3.1",
            "dvcMessage": "Release 2026-08-20: Bob's iris data",
        }
    if mode == "Download":
        return {
            **common,
            "dvcDataLocation": "datasets/iris-v1.2",
            "dvcLoadSaveDataPath": "/var/lib/dvc/iris-v1.2",
            "dvcVersion": "iris_v2.3.1",
        }
    return {
        **common,
        "dvcStoreUrl": "s3://acme-dvc/iris-v1.2",
    }


def _dvc_spec(task_params: YamlObject, *, suffix: str) -> WorkflowSpec:
    return WorkflowSpec.model_validate(
        {
            "workflow": {"name": f"dvc-{suffix}"},
            "tasks": [
                {
                    "name": "manage-versioned-data",
                    "type": "DVC",
                    "task_params": task_params,
                }
            ],
        }
    )


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    prepared = prepare_workflow_create_compilation(
        _dvc_spec(task_params, suffix=ds_version),
        catalog=get_task_authoring_catalog(ds_version),
    )
    payload = prepared.materialize([31_700])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "DVC"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _template_yaml(variant: str) -> str:
    return authoring_prep.template_yaml("DVC", "3.4.2", variant=variant)


def _template_task(variant: str) -> YamlObject:
    document = yaml.safe_load(_template_yaml(variant))
    assert isinstance(document, dict)
    return cast("YamlObject", document)


@pytest.mark.parametrize("ds_version", _DVC_VERSIONS)
def test_dvc_catalog_exposes_one_reviewed_exact_operation_facet(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    membership = catalog.require_facet("DVC", _DVC_FACET)
    source_review = catalog.task_type_facts["DVC"].typed_authoring_review

    assert catalog.supports_typed_authoring("DVC") is True
    assert catalog.supports_opaque_authoring("DVC") is True
    assert source_review is not None
    assert membership.contract.review == source_review.review
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _DVC_VERSIONS)
def test_dvc_schema_exposes_only_the_three_mode_shell_safe_surface(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "DVC",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_fields = {
        path.removeprefix("task_params."): field
        for path, field in fields.items()
        if path.startswith("task_params.")
    }

    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == _ALL_TYPED_FIELDS
    assert task_param_fields["dvcTaskType"]["required"] is True
    assert task_param_fields["dvcTaskType"]["choices"] == [
        "Upload",
        "Download",
        "Init DVC",
    ]
    assert task_param_fields["dvcRepository"]["required"] is True
    for field_name in set().union(*_MODE_FIELDS.values()):
        assert task_param_fields[field_name]["required"] is True
        assert task_param_fields[field_name]["active_when"]
    assert not any(path.startswith("task_params.localParams") for path in fields)
    assert not any(path.startswith("task_params.varPool") for path in fields)
    assert not any(path.startswith("task_params.resourceList") for path in fields)

    descriptions = " ".join(
        str(field.get("description", "")) for field in task_param_fields.values()
    ).lower()
    assert "shell" in descriptions
    assert "dvc" in descriptions
    assert "git" in descriptions
    assert "worker" in descriptions
    assert "credential" in descriptions


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_dvc_state_rules_lock_each_modes_required_and_inactive_fields(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "DVC",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    rules = {
        rule["when"]: rule
        for rule in result.data["state_rules"]
        if isinstance(rule, dict) and isinstance(rule.get("when"), str)
    }
    expected = {
        "task_params.dvcTaskType == Upload": (
            {
                "task_params.dvcDataLocation",
                "task_params.dvcLoadSaveDataPath",
                "task_params.dvcVersion",
                "task_params.dvcMessage",
            },
            {"task_params.dvcStoreUrl"},
        ),
        "task_params.dvcTaskType == Download": (
            {
                "task_params.dvcDataLocation",
                "task_params.dvcLoadSaveDataPath",
                "task_params.dvcVersion",
            },
            {"task_params.dvcMessage", "task_params.dvcStoreUrl"},
        ),
        "task_params.dvcTaskType == Init DVC": (
            {"task_params.dvcStoreUrl"},
            {
                "task_params.dvcDataLocation",
                "task_params.dvcLoadSaveDataPath",
                "task_params.dvcVersion",
                "task_params.dvcMessage",
            },
        ),
    }

    assert set(rules) == set(expected)
    for when, (active, inactive) in expected.items():
        assert rules[when]["condition_paths"] == ["task_params.dvcTaskType"]
        assert set(rules[when]["active_paths"]) == active
        assert set(rules[when]["inactive_paths"]) == inactive


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_dvc_json_schema_is_closed_without_inherited_runtime_fields(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "DVC",
        json_schema=True,
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)

    assert task_params["additionalProperties"] is False
    assert set(task_params["required"]) == {"dvcTaskType", "dvcRepository"}
    assert set(properties) == _ALL_TYPED_FIELDS
    mode_schema = properties["dvcTaskType"]
    assert isinstance(mode_schema, dict)
    mode_ref = mode_schema["$ref"]
    assert isinstance(mode_ref, str)
    nested_definitions = task_params["$defs"]
    assert isinstance(nested_definitions, dict)
    resolved_mode_schema = nested_definitions[mode_ref.rsplit("/", maxsplit=1)[-1]]
    assert isinstance(resolved_mode_schema, dict)
    assert resolved_mode_schema["enum"] == [
        "Upload",
        "Download",
        "Init DVC",
    ]
    assert "localParams" not in properties
    assert "varPool" not in properties
    assert "resourceList" not in properties


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_dvc_summary_and_compile_mappings_are_bounded_to_native_mode_fields(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("DVC", catalog=catalog)
    result = task_type_schema_result(
        "DVC",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_param_mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["kind"] == "typed"
    assert summary["variants"] == ["upload", "download", "init"]
    assert {
        "task_params.dvcTaskType",
        "task_params.dvcRepository",
    }.issubset(summary["required_paths"])
    assert set(task_param_mappings) == {
        f"task_params.{field_name}" for field_name in _ALL_TYPED_FIELDS
    }
    for authoring_path, payload_path in task_param_mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


@pytest.mark.parametrize(("variant", "mode"), _DVC_VARIANTS)
def test_dvc_mode_templates_are_canonical_shell_safe_and_compile_exactly(
    variant: str,
    mode: DvcMode,
) -> None:
    yaml_text = _template_yaml(variant)
    task = _template_task(variant)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "DVC"
    assert params["dvcTaskType"] == mode
    assert set(params) == _COMMON_FIELDS | _MODE_FIELDS[mode]
    assert _compiled_task_params("3.4.2", params) == params

    guidance = yaml_text.lower()
    assert "worker" in guidance
    assert "dvc" in guidance
    assert "git" in guidance
    assert "shell" in guidance
    assert "credential" in guidance


@pytest.mark.parametrize("ds_version", _DVC_VERSIONS)
@pytest.mark.parametrize("mode", ["Upload", "Download", "Init DVC"])
def test_dvc_prepared_compilation_preserves_exact_safe_native_identity(
    ds_version: str,
    mode: DvcMode,
) -> None:
    params = _canonical_params(mode)

    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
@pytest.mark.parametrize("mode", ["Upload", "Download", "Init DVC"])
def test_dvc_typed_create_and_edit_preserve_valid_values_exactly(
    intent: TaskAuthoringIntent,
    mode: DvcMode,
) -> None:
    params = _canonical_params(mode)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DVC",
        params,
        intent=intent,
    )

    assert normalized == params


@pytest.mark.parametrize(
    ("mode", "field"),
    [
        ("Upload", "dvcTaskType"),
        ("Upload", "dvcRepository"),
        ("Upload", "dvcDataLocation"),
        ("Upload", "dvcLoadSaveDataPath"),
        ("Upload", "dvcVersion"),
        ("Upload", "dvcMessage"),
        ("Download", "dvcDataLocation"),
        ("Download", "dvcLoadSaveDataPath"),
        ("Download", "dvcVersion"),
        ("Init DVC", "dvcStoreUrl"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dvc_typed_modes_require_every_active_field(
    mode: DvcMode,
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(mode)
    params.pop(field)

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("mode", "inactive_field"),
    [
        ("Upload", "dvcStoreUrl"),
        ("Download", "dvcMessage"),
        ("Download", "dvcStoreUrl"),
        ("Init DVC", "dvcDataLocation"),
        ("Init DVC", "dvcLoadSaveDataPath"),
        ("Init DVC", "dvcVersion"),
        ("Init DVC", "dvcMessage"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
@pytest.mark.parametrize("inactive_value", ["unused", None])
def test_dvc_typed_modes_require_inactive_fields_to_be_absent(
    mode: DvcMode,
    inactive_field: str,
    intent: TaskAuthoringIntent,
    inactive_value: YamlValue,
) -> None:
    params = _canonical_params(mode)
    params[inactive_field] = inactive_value

    with pytest.raises(ValueError, match=inactive_field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("invalid_mode", ["UPLOAD", "download", "Init", ""])
def test_dvc_task_type_values_are_exact_native_literals(invalid_mode: str) -> None:
    params = _canonical_params("Upload")
    params["dvcTaskType"] = invalid_mode

    with pytest.raises(ValueError, match="dvcTaskType"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("mode", "field", "value"),
    [
        ("Upload", "dvcRepository", "https://github.com/acme/ml-data.git"),
        ("Upload", "dvcDataLocation", "models/iris.csv"),
        ("Upload", "dvcLoadSaveDataPath", "~/datasets/iris-v1.2"),
        ("Download", "dvcVersion", "iris_v2.3.1"),
        ("Init DVC", "dvcStoreUrl", "s3://acme-dvc/iris-v1.2"),
    ],
)
def test_dvc_unquoted_shell_fields_accept_common_single_safe_tokens(
    mode: DvcMode,
    field: str,
    value: str,
) -> None:
    params = _canonical_params(mode)
    params[field] = value

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DVC",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "revision",
    ["feature/iris-v2", "0123456789abcdef", "iris_v2.3.1"],
)
def test_dvc_version_accepts_portable_git_refs(revision: str) -> None:
    params = _canonical_params("Download")
    params["dvcVersion"] = revision

    assert (
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == params
    )


@pytest.mark.parametrize(
    "revision",
    [
        "main..prod",
        "feature//iris",
        ".hidden",
        "feature/.hidden",
        "release.lock",
        "trailing.",
        "feature/",
        "v1~2",
    ],
)
def test_dvc_version_rejects_advanced_or_invalid_git_revspecs(
    revision: str,
) -> None:
    params = _canonical_params("Download")
    params["dvcVersion"] = revision

    with pytest.raises(ValueError, match="dvcVersion"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("mode", "field", "unsafe_value"),
    [
        ("Upload", "dvcRepository", "repo;touch/tmp/dvc-owned"),
        ("Upload", "dvcRepository", "-c"),
        ("Upload", "dvcDataLocation", "models/*.csv"),
        ("Upload", "dvcDataLocation", "-output"),
        ("Download", "dvcLoadSaveDataPath", "data path"),
        ("Download", "dvcLoadSaveDataPath", "-target"),
        ("Upload", "dvcVersion", "${release}"),
        ("Upload", "dvcVersion", "-v2"),
        ("Init DVC", "dvcStoreUrl", "$(id)"),
        ("Init DVC", "dvcStoreUrl", "-c"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dvc_unquoted_shell_fields_reject_injection_and_option_tokens(
    mode: DvcMode,
    field: str,
    unsafe_value: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(mode)
    params[field] = unsafe_value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("mode", "field", "credential_url"),
    [
        (
            "Upload",
            "dvcRepository",
            "https://alice:secret@example.com/acme/ml-data.git",
        ),
        (
            "Upload",
            "dvcDataLocation",
            "https://alice:secret@example.com/acme/data",
        ),
        (
            "Download",
            "dvcLoadSaveDataPath",
            "https://alice:secret@example.com/acme/output",
        ),
        (
            "Init DVC",
            "dvcStoreUrl",
            "s3://ACCESS:SECRET@acme-dvc/iris-v1.2",
        ),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dvc_typed_urls_reject_logged_userinfo_credentials(
    mode: DvcMode,
    field: str,
    credential_url: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(mode)
    params[field] = credential_url

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


def test_dvc_message_allows_safe_spaces_punctuation_and_unicode_exactly() -> None:
    params = _canonical_params("Upload")
    params["dvcMessage"] = "发布 iris 数据 v2.3.1: Bob's baseline & metrics; approved"

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DVC",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "unsafe_message",
    [
        'release"; touch /tmp/dvc-owned; #',
        "release\\",
        "release ${version}",
        "release $(id)",
        "release `id`",
        "release\nnext",
        "release\rnext",
        "release\tnext",
        "release\x00next",
        "release\x1fnext",
        "release\x7fnext",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dvc_double_quoted_message_rejects_shell_expansion_and_control_chars(
    unsafe_message: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("Upload")
    params["dvcMessage"] = unsafe_message

    with pytest.raises(ValueError, match="dvcMessage"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dvcRepository", "   "),
        ("dvcDataLocation", ""),
        ("dvcLoadSaveDataPath", ""),
        ("dvcVersion", ""),
        ("dvcMessage", ""),
        ("dvcMessage", "   "),
    ],
)
def test_dvc_upload_rejects_blank_active_values(field: str, value: str) -> None:
    params = _canonical_params("Upload")
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("futureField", {"enabled": True}),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dvc_typed_create_and_edit_reject_unowned_inherited_or_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("Upload")
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DVC",
            params,
            intent=intent,
        )


def _opaque_native_params() -> YamlObject:
    return {
        "dvcTaskType": "Upload",
        "dvcRepository": "repo; touch /tmp/native-value",
        "dvcDataLocation": "datasets/*.csv",
        "dvcLoadSaveDataPath": "data path",
        "dvcVersion": "${native_version}",
        "dvcMessage": "native $(message)",
        "dvcStoreUrl": "inactive-native-store",
        "localParams": [
            {
                "prop": "runtime",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/native/dvc.json",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "futureField": {"nested": ["native"]},
    }


@pytest.mark.parametrize("ds_version", _DVC_VERSIONS)
def test_dvc_opaque_runtime_inactive_and_future_fields_are_deep_preserved(
    ds_version: str,
) -> None:
    native = _opaque_native_params()
    expected = deepcopy(native)
    catalog = get_task_authoring_catalog(ds_version)

    preserved = catalog.normalize_task_params(
        "DVC",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    resources = native["resourceList"]
    assert isinstance(resources, list)
    resource = resources[0]
    assert isinstance(resource, dict)
    metadata = resource["futureMetadata"]
    assert isinstance(metadata, dict)
    metadata["checksum"] = "mutated"
    var_pool = native["varPool"]
    assert isinstance(var_pool, list)
    runtime = var_pool[0]
    assert isinstance(runtime, dict)
    runtime["value"] = {"state": "mutated"}

    assert preserved == expected


@pytest.mark.parametrize("ds_version", _DVC_VERSIONS)
def test_dvc_opaque_export_and_metadata_compile_are_lossless(
    ds_version: str,
) -> None:
    native_params = _opaque_native_params()
    task = FakeTaskDefinition(
        code=101,
        name="native-dvc-task",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="DVC",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="dvc-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert document["tasks"][0]["task_params"] == native_params

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native_params


@pytest.mark.parametrize("ds_version", _DVC_ABSENT_VERSIONS)
def test_dvc_is_unavailable_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("DVC") is False
    assert catalog.supports_opaque_authoring("DVC") is False
    assert "DVC" not in catalog.authoring_task_types
