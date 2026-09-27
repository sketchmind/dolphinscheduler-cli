from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject, JsonValue

_CANONICAL_SQOOP_PARAMS: JsonObject = {
    "subcommand": "import",
    "args": [
        "--connect",
        "jdbc:mysql://db.example.invalid:3306/source",
        "--query",
        "SELECT id FROM orders",
        "--target-dir",
        "hdfs:///warehouse/orders",
    ],
}
_NATIVE_SQOOP_PARAMS: JsonObject = {
    "jobType": "CUSTOM",
    "localParams": [],
    "customShell": (
        "sqoop import --connect jdbc:mysql://db.example.invalid:3306/source "
        "--query 'SELECT id FROM orders' --target-dir hdfs:///warehouse/orders"
    ),
}
_NO_TASK_REFS = TaskRefIndex.from_code_by_name({})


def test_sqoop_schema_exposes_one_closed_literal_command_intent() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    result = task_type_schema_result("SQOOP", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert catalog.supports_typed_authoring("SQOOP") is True
    assert {path for path in fields if path.startswith("task_params.")} == {
        "task_params.subcommand",
        "task_params.args",
        "task_params.args[]",
    }
    assert fields["task_params.subcommand"]["choices"] == ["import", "export"]
    assert fields["task_params.args"]["required"] is True
    assert "ASCII-only" in fields["task_params.args[]"]["description"]

    utf8_result = task_type_schema_result(
        "SQOOP",
        catalog=get_task_authoring_catalog("3.1.0"),
    )
    assert isinstance(utf8_result.data, dict)
    utf8_argument = next(
        field
        for field in utf8_result.data["fields"]
        if isinstance(field, dict) and field.get("path") == "task_params.args[]"
    )
    assert "safe Unicode" in utf8_argument["description"]


def test_sqoop_json_schema_is_closed_and_requires_one_nonempty_argument_list() -> None:
    result = task_type_schema_result(
        "SQOOP",
        json_schema=True,
        catalog=get_task_authoring_catalog("3.4.1"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["subcommand", "args"]
    assert set(task_params["properties"]) == {"subcommand", "args"}
    assert task_params["properties"]["subcommand"]["enum"] == ["import", "export"]
    assert task_params["properties"]["args"]["minItems"] == 1
    assert "minLength" not in task_params["properties"]["args"]["items"]


@pytest.mark.parametrize("version", ["3.1.0", "3.4.1"])
def test_sqoop_json_schema_rejects_interactive_and_inline_password_switches(
    version: str,
) -> None:
    result = task_type_schema_result(
        "SQOOP",
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]
    argument_pattern = task_params["properties"]["args"]["items"]["pattern"]

    assert re.fullmatch(argument_pattern, "") is not None
    assert re.fullmatch(argument_pattern, "--password-file") is not None
    for forbidden in ("-P", "--password", "--password=secret"):
        assert re.fullmatch(argument_pattern, forbidden) is None


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_sqoop_minimal_template_validates_on_every_exact_profile(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result("SQOOP", catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(result.data["yaml"])

    spec = validate_workflow_document(
        {
            "workflow": {"name": f"sqoop-template-{version}"},
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    task_params = spec.tasks[0].task_params
    assert task_params is not None
    assert task_params["subcommand"] == "import"
    args = task_params["args"]
    assert isinstance(args, list)
    assert "--password-file" in args


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_sqoop_workflow_compiles_the_same_closed_wire_on_every_profile(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"sqoop-import-{version}"},
            "tasks": [
                {
                    "name": "import-orders",
                    "type": "SQOOP",
                    "task_params": cast("YamlValue", _CANONICAL_SQOOP_PARAMS),
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(spec, catalog=catalog).materialize(
        [51_001]
    )
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert definition["taskType"] == "SQOOP"
    assert json.loads(definition["taskParams"]) == _NATIVE_SQOOP_PARAMS


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_sqoop_exact_custom_wire_round_trips_on_every_profile(version: str) -> None:
    exported = decode_task_parameters_with_provenance(
        version=version,
        task_type="SQOOP",
        task_params=_NATIVE_SQOOP_PARAMS,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported.task.task_params == _CANONICAL_SQOOP_PARAMS
    assert (
        encode_task_parameters(
            version=version,
            task_type="SQOOP",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            source=exported.reencode_source,
        ).task_params
        == _NATIVE_SQOOP_PARAMS
    )


@pytest.mark.parametrize(
    "task_params",
    [
        {"subcommand": "codegen", "args": ["--connect", "jdbc:mysql://db/source"]},
        {"subcommand": "import", "args": []},
        {"subcommand": "import", "args": "--table orders"},
        {"subcommand": "import", "args": [" leading-space"]},
        {"subcommand": "import", "args": ["trailing-space "]},
        {"subcommand": "import", "args": ["line\nbreak"]},
        {"subcommand": "import", "args": ["${password}"]},
        {"subcommand": "import", "args": ["$[yyyyMMdd]"]},
        {"subcommand": "import", "args": ["-P"]},
        {"subcommand": "import", "args": ["--password"]},
        {"subcommand": "import", "args": ["--password=secret"]},
        {"subcommand": "import", "args": ["--table", "orders"], "future": True},
    ],
)
def test_sqoop_typed_model_rejects_unowned_or_unsafe_input(
    task_params: YamlObject,
) -> None:
    with pytest.raises(ValueError):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "SQOOP",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("forbidden", ["-P", "--password", "--password=secret"])
def test_sqoop_password_switch_error_recommends_password_file(
    forbidden: str,
) -> None:
    with pytest.raises(ValueError, match="--password-file"):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "SQOOP",
            {"subcommand": "import", "args": [forbidden]},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_sqoop_typed_projection_quotes_shell_syntax_as_one_literal_argument() -> None:
    native = encode_task_parameters(
        version="3.4.1",
        task_type="SQOOP",
        task_params={
            "subcommand": "export",
            "args": ["--query", "SELECT '$CONDITIONS;still-literal'"],
        },
        refs=_NO_TASK_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params

    assert native == {
        "jobType": "CUSTOM",
        "localParams": [],
        "customShell": (
            "sqoop export --query 'SELECT '\"'\"'$CONDITIONS;still-literal'\"'\"''"
        ),
    }


def test_sqoop_empty_literal_argument_round_trips_through_posix_quoting() -> None:
    canonical: JsonObject = {
        "subcommand": "import",
        "args": ["--null-string", ""],
    }
    native: JsonObject = {
        "jobType": "CUSTOM",
        "localParams": [],
        "customShell": "sqoop import --null-string ''",
    }

    assert (
        encode_task_parameters(
            version="3.4.1",
            task_type="SQOOP",
            task_params=canonical,
            refs=_NO_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == native
    )
    exported = decode_task_parameters_with_provenance(
        version="3.4.1",
        task_type="SQOOP",
        task_params=native,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported.task.task_params == canonical


def test_sqoop_explicit_utf8_writer_epoch_accepts_unicode_literal_argument() -> None:
    canonical: JsonObject = {
        "subcommand": "import",
        "args": ["--table", "订单"],
    }

    assert (
        get_task_authoring_catalog("3.1.0").normalize_task_params(
            "SQOOP",
            cast("YamlObject", canonical),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == canonical
    )
    assert (
        encode_task_parameters(
            version="3.1.0",
            task_type="SQOOP",
            task_params=canonical,
            refs=_NO_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params["customShell"]
        == "sqoop import --table '订单'"
    )


def test_sqoop_platform_default_writer_epoch_rejects_unicode_literal_argument() -> None:
    canonical: JsonObject = {
        "subcommand": "import",
        "args": ["--table", "订单"],
    }

    with pytest.raises(ValueError, match="ASCII"):
        get_task_authoring_catalog("3.1.9").normalize_task_params(
            "SQOOP",
            cast("YamlObject", canonical),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    with pytest.raises(TaskParameterProjectionError, match="ASCII"):
        encode_task_parameters(
            version="3.1.9",
            task_type="SQOOP",
            task_params=canonical,
            refs=_NO_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("job_type", ["CUSTOM", "TEMPLATE"])
def test_sqoop_explicit_opaque_authoring_accepts_known_native_modes(
    job_type: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    native: YamlObject = {
        "jobType": job_type,
        "localParams": [],
        "customShell": "arbitrary shell" if job_type == "CUSTOM" else "",
    }

    assert (
        catalog.normalize_task_params(
            "SQOOP",
            native,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )
        == native
    )


def test_sqoop_explicit_opaque_authoring_rejects_unknown_native_mode() -> None:
    with pytest.raises(
        UnsupportedFeatureError,
        match="SQOOP/literal_command is unsupported",
    ):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "SQOOP",
            {"jobType": "FUTURE", "customShell": "sqoop import"},
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )


@pytest.mark.parametrize("job_type", [[], {}])
def test_sqoop_explicit_opaque_authoring_rejects_container_native_mode(
    job_type: YamlValue,
) -> None:
    with pytest.raises(UnsupportedFeatureError):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "SQOOP",
            {"jobType": job_type, "customShell": "sqoop import"},
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )


@pytest.mark.parametrize("subcommand", [[], {}])
def test_sqoop_typed_projection_rejects_container_subcommand(
    subcommand: YamlValue,
) -> None:
    with pytest.raises(TaskParameterProjectionError):
        encode_task_parameters(
            version="3.4.1",
            task_type="SQOOP",
            task_params={
                "subcommand": cast("JsonValue", subcommand),
                "args": ["--all"],
            },
            refs=_NO_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "native",
    [
        {**_NATIVE_SQOOP_PARAMS, "jobName": "richer"},
        {**_NATIVE_SQOOP_PARAMS, "localParams": [{"prop": "value"}]},
        {
            **_NATIVE_SQOOP_PARAMS,
            "customShell": (
                "sqoop import '--connect' jdbc:mysql://db.example.invalid:3306/source "
                "--query 'SELECT id FROM orders' --target-dir "
                "hdfs:///warehouse/orders"
            ),
        },
    ],
)
def test_sqoop_richer_or_noncanonical_native_state_remains_opaque_and_lossless(
    native: JsonObject,
) -> None:
    exported = decode_task_parameters_with_provenance(
        version="3.4.1",
        task_type="SQOOP",
        task_params=native,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert exported.task.task_params == native
    assert (
        encode_task_parameters(
            version="3.4.1",
            task_type="SQOOP",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            source=exported.reencode_source,
        ).task_params
        == native
    )


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_sqoop_review_is_materialized_on_every_exact_profile(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    fact = catalog.task_type_facts["SQOOP"]
    review = fact.typed_authoring_review

    assert review is not None
    assert review.source_task_type == "SQOOP"
    assert review.cli_task_type == "SQOOP"
    assert review.cli_model == "SqoopLiteralCommandTaskParamsSpec"
    assert review.review == (
        "exact-3.4.3-sqoop-source-closure"
        if version == "3.4.3"
        else "sqoop-literal-custom-command-exact-subset"
    )
    assert catalog.supports_typed_authoring("SQOOP") is True


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_sqoop_exact_surface_tracks_line_endings_and_lifecycle(version: str) -> None:
    surface = get_task_authoring_surface(version).sqoop

    assert surface.available is True
    assert surface.line_separator == ("lf" if version <= "3.1.9" else "system")
    assert surface.script_encoding == (
        "utf-8" if version <= "3.1.0" else "platform-default"
    )
    assert surface.completion_epoch == (
        "launcher-plus-yarn-final-state" if version == "1.3.9" else "launcher-exit-only"
    )
    assert surface.application_id_observation == (
        "post-exit-log-discovery-nondurable"
        if version <= "3.1.9"
        else (
            "post-exit-log-or-app-info-final-result"
            if version in {"3.2.0", "3.2.1", "3.2.2"}
            else "post-exit-context-transport-hole"
        )
    )
    assert surface.cancel_epoch == (
        "wrapper-kill-and-yarn-discovery"
        if version <= "3.1.9"
        else (
            "direct-process-and-outer-yarn-cancel"
            if version in {"3.2.0", "3.2.1", "3.2.2"}
            else "process-tree-and-generic-yarn-cancel"
        )
    )
    assert surface.parameter_substitution is True
    assert surface.late_password_mask is (version >= "3.2.0")
    assert surface.task_params_logged is True
    assert surface.command_logged is True
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True
