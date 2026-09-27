from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_TASK_TYPE = "CHUNJUN"
_FACET = "CHUNJUN/literal_local_json_job"
_TYPED_VERSIONS = (
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
_REFS = TaskRefIndex.from_code_by_name({})
_JOB_JSON = """{
  "job": {
    "content": [],
    "setting": {"speed": {"channel": 1}}
  }
}"""


def _canonical(json_text: str = _JOB_JSON) -> YamlObject:
    return {"json": json_text}


def _native(json_text: str = _JOB_JSON) -> YamlObject:
    return {
        "customConfig": 1,
        "json": json_text,
        "deployMode": "local",
    }


def _spec(version: str, params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"chunjun-{version}"},
            "tasks": [
                {
                    "name": "run-chunjun-job",
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([41_000])["taskDefinitionJson"])[0]
    assert definition["taskType"] == _TASK_TYPE
    native = json.loads(definition["taskParams"])
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(
    version: str,
    params: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.TYPED_AUTHORING,
) -> YamlObject:
    encoded = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", encoded.task_params)


def _decode(
    version: str,
    native: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _fake_dag(native: YamlObject, *, workflow_name: str) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name="run-chunjun-job",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value=_TASK_TYPE,
                task_params_value=json.dumps(native),
                worker_group_value="default",
                fail_retry_times_value=0,
                fail_retry_interval_value=0,
                timeout=3600,
            )
        ],
        workflow_task_relation_list_value=[],
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_chunjun_catalog_exposes_one_literal_local_json_facet(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert profile.category == "Other"
    assert profile.kind == "typed"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert membership.contract.params_model is not None
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


def test_chunjun_schema_and_template_publish_only_literal_json() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["json"]
    assert set(task_params["properties"]) == {"json"}
    assert task_params["properties"]["json"]["type"] == "string"

    template = task_template_result(
        _TASK_TYPE,
        catalog=catalog,
    )
    assert isinstance(template.data, dict)
    task = yaml.safe_load(template.data["yaml"])
    assert task["type"] == _TASK_TYPE
    assert set(task["task_params"]) == {"json"}
    assert isinstance(json.loads(task["task_params"]["json"]), dict)
    assert _compiled("3.4.2", task["task_params"]) == _native(
        task["task_params"]["json"]
    )


@pytest.mark.parametrize(
    ("version", "epoch", "application_ids", "cancel_mode", "ui_default"),
    [
        (
            "3.1.0",
            "legacy-shell-file",
            "post-exit-log-discovery-nondurable",
            "wrapper-kill",
            False,
        ),
        ("3.1.9", "legacy-shell-file", "none", "wrapper-kill", True),
        ("3.2.0", "shell-interceptor", "none", "direct-process-destroy", True),
        ("3.2.1", "shell-interceptor", "none", "direct-process-destroy", True),
        ("3.2.2", "shell-interceptor", "none", "direct-process-destroy", True),
        (
            "3.3.1",
            "shell-interceptor",
            "none",
            "process-tree-and-application",
            True,
        ),
        (
            "3.3.2",
            "shell-interceptor",
            "none",
            "process-tree-and-application",
            True,
        ),
        (
            "3.4.0",
            "task-request",
            "none",
            "process-tree-and-application",
            True,
        ),
        (
            "3.4.1",
            "task-request",
            "none",
            "process-tree-and-application",
            True,
        ),
        (
            "3.4.2",
            "task-request",
            "none",
            "process-tree-and-application",
            True,
        ),
    ],
)
def test_chunjun_surface_records_exact_execution_epochs(
    version: str,
    epoch: str,
    application_ids: str,
    cancel_mode: str,
    ui_default: object,
) -> None:
    surface = get_task_authoring_surface(version).chunjun

    assert surface.available is True
    assert surface.execution_epoch == epoch
    assert surface.application_id_observation == application_ids
    assert surface.json_encoding == "utf-8"
    assert surface.line_separator == "lf"
    assert surface.parameter_substitution is True
    assert surface.local_params_supported is True
    assert surface.ui_custom_config_default is ui_default
    assert surface.builtin_mode_runnable is False
    assert surface.cancel_mode == cancel_mode
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


def test_chunjun_guidance_requires_foreground_and_excludes_parameters() -> None:
    contract = (
        get_task_authoring_catalog("3.1.0")
        .require_facet(
            _TASK_TYPE,
            _FACET,
        )
        .contract
    )
    state_text = " ".join(rule.description for rule in contract.state_rules)
    template_text = " ".join(template.yaml for template in contract.templates)

    assert "start-chunjun launcher must be patched to run in the foreground" in (
        state_text
    )
    assert "removing its trailing background '&'" in template_text
    assert "Upstream accepts localParams" in state_text
    assert "typed facet therefore excludes localParams" in state_text
    assert "without JSON-aware escaping" in state_text
    assert "UI defaults customConfig=false" in state_text


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_chunjun_canonical_json_compiles_exact_local_wire(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    authored = _canonical()

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == authored
    assert normalized is not authored
    assert _encode(version, normalized) == _native()
    assert _compiled(version, authored) == _native()


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_chunjun_typed_native_decode_is_a_projection_fixed_point(version: str) -> None:
    canonical, source = _decode(version, _native())

    assert canonical == _canonical()
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _encode(version, canonical, source=source) == _native()


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "ui_residue",
    [
        {"others": ""},
        {"localParams": []},
        {"resourceList": []},
        {"others": "", "localParams": []},
    ],
)
def test_chunjun_decode_strips_only_empty_ui_residue(
    version: str,
    ui_residue: YamlObject,
) -> None:
    canonical, source = _decode(version, {**_native(), **ui_residue})

    assert canonical == _canonical()
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _encode(version, canonical, source=source) == _native()


@pytest.mark.parametrize(
    "json_text",
    [
        "",
        "not-json",
        "[]",
        '"scalar"',
        '{"job":"${secret}"}',
        '{"job":"$[yyyyMMdd]"}',
        '{"job":"line\\rbreak"}',
        r'{"job":"\u0085"}',
        r'{"job":"\ud800"}',
        '{"job":NaN}',
        '{"job":{},"job":{"content":[]}}',
    ],
)
def test_chunjun_json_is_one_literal_unambiguous_object(json_text: str) -> None:
    with pytest.raises(ValueError, match="json"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            {"json": json_text},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("customConfig", 1),
        ("deployMode", "local"),
        ("others", ""),
        ("localParams", []),
        ("resourceList", []),
        ("dataSource", 7),
        ("futureField", {"native": True}),
    ],
)
def test_chunjun_typed_authoring_rejects_every_unowned_native_field(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "deploy_mode",
    ["standalone", "yarn-session", "yarn-per-job"],
)
@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_chunjun_nonlocal_custom_json_is_explicit_opaque_authoring(
    version: str,
    deploy_mode: str,
) -> None:
    native: YamlObject = {
        "customConfig": 1,
        "json": _JOB_JSON,
        "deployMode": deploy_mode,
        "others": "-p 2",
    }
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.normalize_task_params(
            _TASK_TYPE,
            native,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )
        == native
    )
    assert (
        _encode(
            version,
            native,
            source=ProjectionSource.OPAQUE_PRESERVE,
        )
        == native
    )


@pytest.mark.parametrize(
    "native",
    [
        {"customConfig": True, "json": _JOB_JSON, "deployMode": "standalone"},
        _native(),
        {"customConfig": 1, "json": _JOB_JSON, "deployMode": "unknown"},
    ],
)
def test_chunjun_opaque_authoring_selector_fails_closed(native: YamlObject) -> None:
    with pytest.raises(UnsupportedFeatureError, match="CHUNJUN"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            native,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )


@pytest.mark.parametrize(
    "native",
    [
        {"customConfig": 0, "dataSource": 7, "dataTarget": 8},
        {"customConfig": 1, "json": _JOB_JSON, "deployMode": "standlone"},
        {**_native(), "others": "-p 2"},
        {**_native(), "localParams": [{"prop": "unsafe"}]},
        {**_native(), "futureField": {"preserve": True}},
    ],
)
def test_chunjun_broken_builtin_and_richer_local_state_are_preserve_only(
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    decoded, source = _decode("3.4.2", native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    assert _encode("3.4.2", decoded, source=source) == native
    with pytest.raises(TaskParameterProjectionError):
        _decode("3.4.2", native, source=ProjectionSource.TYPED_AUTHORING)
    with pytest.raises(UnsupportedFeatureError, match="CHUNJUN"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            native,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6"])
def test_chunjun_is_absent_before_310(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert _TASK_TYPE not in catalog.upstream_task_types
    with pytest.raises(UnsupportedFeatureError, match="CHUNJUN"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            _canonical(),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_native(), _canonical(), ProjectionSource.TYPED_AUTHORING),
        (
            {**_native(), "futureField": {"preserve": True}},
            {**_native(), "futureField": {"preserve": True}},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
        (
            {"customConfig": 0, "dataSource": 7, "dataTarget": 8},
            {"customConfig": 0, "dataSource": 7, "dataTarget": 8},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_chunjun_export_distinguishes_typed_and_opaque_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(native, workflow_name="chunjun-export")

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert baseline.projection_sources["run-chunjun-job"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported
