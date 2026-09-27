import json
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.models.task_spec import supported_typed_task_types
from dsctl.services.task_authoring_catalog import (
    ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET,
    MLFLOW_MODEL_SERVE_FACET,
    SQL_INLINE_FACET,
    SQL_RESOURCE_FILE_FACET,
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import generic_task_template_types

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


def test_task_authoring_catalog_caches_by_trimmed_exact_version() -> None:
    canonical = get_task_authoring_catalog("3.4.1")

    assert get_task_authoring_catalog(" 3.4.1 ") is canonical


def test_explicit_template_catalog_loads_only_its_selected_exact_package() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    probe = f"""
import json
import sys
sys.path.insert(0, {str(source_root)!r})
from dsctl.services._task_templates import supported_task_template_types
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog

catalog = get_task_authoring_catalog("3.2.0")
supported_task_template_types(catalog=catalog)
versions = sorted({{
    name.split(".")[3]
    for name in sys.modules
    if name.startswith("dsctl.generated.versions.")
}})
print(json.dumps(versions))
"""

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", probe],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == ["ds_3_2_0"]


@pytest.mark.parametrize(
    ("ds_version", "task_types"),
    [
        (
            "3.4.1",
            (
                "ALIYUN_SERVERLESS_SPARK",
                "CHUNJUN",
                "DATASYNC",
                "DATAX",
                "DATA_FACTORY",
                "DEPENDENT",
                "DINKY",
                "DMS",
                "DVC",
                "EMR",
                "FLINK",
                "GRPC",
                "HIVECLI",
                "JAVA",
                "JUPYTER",
                "K8S",
                "KUBEFLOW",
                "LINKIS",
                "MLFLOW",
                "MR",
                "OPENMLDB",
                "PROCEDURE",
                "SAGEMAKER",
                "SEATUNNEL",
                "SPARK",
                "SQL",
                "SQOOP",
                "ZEPPELIN",
            ),
        ),
        (
            "3.4.2",
            (
                "ALIYUN_SERVERLESS_SPARK",
                "CHUNJUN",
                "DATASYNC",
                "DATAX",
                "DATA_FACTORY",
                "DEPENDENT",
                "DINKY",
                "DMS",
                "DVC",
                "EMR",
                "EMR_SERVERLESS",
                "FLINK",
                "GRPC",
                "HIVECLI",
                "JAVA",
                "JUPYTER",
                "K8S",
                "KUBEFLOW",
                "LINKIS",
                "MLFLOW",
                "MR",
                "OPENMLDB",
                "PROCEDURE",
                "SAGEMAKER",
                "SEATUNNEL",
                "SPARK",
                "SQL",
                "SQOOP",
                "ZEPPELIN",
            ),
        ),
    ],
)
def test_exact_profiles_select_expected_catalog_and_reviewed_inline_sql_contract(
    ds_version: str,
    task_types: tuple[str, ...],
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    membership = catalog.require_facet("SQL", SQL_INLINE_FACET)

    assert catalog.profile_version == ds_version
    assert catalog.task_types == task_types
    assert membership.profile_version == ds_version
    assert membership.contract.facet_id == SQL_INLINE_FACET
    assert membership.contract.family == "sql-inline-script-v1"
    assert membership.contract.review == "sql-3.4.1-to-3.4.2"
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_preserve is True


def test_exact_profiles_share_inline_semantics_without_sharing_membership() -> None:
    ds_341 = get_task_authoring_catalog("3.4.1").require_facet(
        "SQL",
        SQL_INLINE_FACET,
    )
    ds_342 = get_task_authoring_catalog("3.4.2").require_facet(
        "SQL",
        SQL_INLINE_FACET,
    )

    assert ds_341 is not ds_342
    assert ds_341.contract is ds_342.contract
    assert ds_341.contract.fields == ds_342.contract.fields
    assert ds_341.contract.state_rules == ds_342.contract.state_rules
    assert ds_341.contract.templates == ds_342.contract.templates


def test_typed_memberships_come_only_from_exact_source_locked_reviews() -> None:
    ds_341 = get_task_authoring_catalog("3.4.1")
    ds_342 = get_task_authoring_catalog("3.4.2")

    assert ds_341.reviewed_typed_task_types == frozenset(supported_typed_task_types())
    assert ds_341.legacy_typed_task_types == (
        frozenset(supported_typed_task_types()) - frozenset(ds_341.task_types)
    )
    assert ds_341.opaque_authoring_task_types - ds_341.reviewed_typed_task_types == {
        "FLINK_STREAM",
    }
    ds_342_typed_task_types = frozenset(
        (*supported_typed_task_types(), "EMR_SERVERLESS")
    )
    assert ds_342.reviewed_typed_task_types == ds_342_typed_task_types
    assert ds_342.legacy_typed_task_types == (
        ds_342_typed_task_types - frozenset(ds_342.task_types)
    )
    assert ds_342.opaque_authoring_task_types - ds_342.reviewed_typed_task_types == {
        "FLINK_STREAM",
    }


def test_task_presence_does_not_grant_typed_authoring_but_opaque_is_lossless() -> None:
    params: YamlObject = {
        "mainClass": "com.example.Job",
        "pluginOwnedField": {"future": True},
    }
    catalog = get_task_authoring_catalog("2.0.9")

    assert "SPARK" in catalog.upstream_task_types
    assert "SPARK" in generic_task_template_types(catalog=catalog)
    with pytest.raises(UnsupportedFeatureError) as captured:
        catalog.normalize_task_params(
            "SPARK",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    assert captured.value.details["selected_version"] == "2.0.9"
    assert captured.value.details["task_type"] == "SPARK"
    assert captured.value.details["intent"] == "typed_create"
    for intent in (
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
        TaskAuthoringIntent.OPAQUE_PRESERVE,
    ):
        normalized = catalog.normalize_task_params("SPARK", params, intent=intent)

        assert normalized == params
        assert normalized is not params
        assert normalized["pluginOwnedField"] is not params["pluginOwnedField"]


@pytest.mark.parametrize(
    "ds_version",
    [
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    ],
)
def test_legacy_sub_process_source_fact_authorizes_canonical_sub_workflow(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert "SUB_WORKFLOW" in catalog.authoring_task_types
    assert "SUB_PROCESS" not in catalog.authoring_task_types
    assert "SUB_PROCESS" in catalog.upstream_task_types
    assert "SUB_WORKFLOW" not in catalog.upstream_task_types
    assert "SUB_PROCESS" in catalog.opaque_authoring_task_types
    assert "SUB_WORKFLOW" not in catalog.opaque_authoring_task_types
    assert "SUB_WORKFLOW" in catalog.reviewed_typed_task_types
    assert "SUB_PROCESS" not in catalog.reviewed_typed_task_types
    assert catalog.task_type_fact("SUB_PROCESS") is not None
    assert catalog.task_type_fact("SUB_WORKFLOW") is None
    assert catalog.source_task_type_for_cli("SUB_WORKFLOW") == "SUB_PROCESS"
    assert catalog.cli_task_type_for_source("SUB_PROCESS") == "SUB_WORKFLOW"
    assert catalog.supports_typed_authoring("SUB_WORKFLOW") is True
    assert catalog.supports_typed_authoring("SUB_PROCESS") is False


def test_catalog_rejects_nested_surface_that_disagrees_with_parameter_semantics() -> (
    None
):
    catalog = get_task_authoring_catalog("2.0.0")
    mismatched_surface = replace(
        catalog.authoring_surface,
        nested_workflow=replace(
            catalog.authoring_surface.nested_workflow,
            native_task_type="SUB_WORKFLOW",
            native_code_field="workflowDefinitionCode",
        ),
    )

    with pytest.raises(
        ValueError,
        match="Nested workflow authoring surface must match parameter semantics",
    ):
        replace(catalog, authoring_surface=mismatched_surface)


def test_catalog_rejects_sub_workflow_review_for_the_wrong_native_type() -> None:
    catalog = get_task_authoring_catalog("2.0.0")
    mismatched_semantics = replace(
        catalog.parameter_semantics,
        nested_workflow=replace(
            catalog.parameter_semantics.nested_workflow,
            task_type="SUB_WORKFLOW",
        ),
    )
    matching_surface = replace(
        catalog.authoring_surface,
        nested_workflow=replace(
            catalog.authoring_surface.nested_workflow,
            native_task_type="SUB_WORKFLOW",
            native_code_field="workflowDefinitionCode",
        ),
    )

    with pytest.raises(
        ValueError,
        match="SUB_WORKFLOW review source type must match nested workflow semantics",
    ):
        replace(
            catalog,
            parameter_semantics=mismatched_semantics,
            authoring_surface=matching_surface,
        )


def test_139_catalog_binds_nested_semantics_to_the_exact_positive_review() -> None:
    catalog = get_task_authoring_catalog("1.3.9")
    native_params: YamlObject = {"processDefinitionId": 202}

    review = catalog.task_type_facts["SUB_PROCESS"].typed_authoring_review
    assert review is not None
    assert review.cli_task_type == "SUB_WORKFLOW"
    assert catalog.parameter_semantics.nested_workflow.task_type == "SUB_PROCESS"
    assert catalog.supports_opaque_authoring("SUB_PROCESS") is False
    for intent in (
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError) as captured:
            catalog.normalize_task_params("SUB_PROCESS", native_params, intent=intent)
        assert captured.value.details["intent"] == intent.value
        assert "only opaque preservation" in str(captured.value.details["constraint"])

    assert catalog.normalize_task_params(
        "SUB_PROCESS",
        native_params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    ) == {"processDefinitionId": 202}


def test_139_reviews_include_http_and_canonical_script_subsets() -> None:
    catalog = get_task_authoring_catalog("1.3.9")
    canonical_params: YamlObject = {
        "rawScript": "echo ready",
        "resourceList": [],
        "localParams": [],
    }

    assert catalog.reviewed_typed_task_types == frozenset(
        {
            "CONDITIONS",
            "DATAX",
            "DEPENDENT",
            "HTTP",
            "MR",
            "PROCEDURE",
            "PYTHON",
            "SHELL",
            "SQL",
            "SQOOP",
            "SUB_WORKFLOW",
        }
    )
    assert catalog.supports_typed_authoring("SHELL") is True
    assert catalog.supports_typed_authoring("PYTHON") is True
    review = catalog.task_type_facts["SHELL"].typed_authoring_review
    assert review is not None
    assert review.semantic_fingerprint == (
        "sha256:3a2e0a984baae4de4fb00949fa917bfde3f97af4d7b3e932754fcfaf996d7270"
    )
    assert review.cli_model == "ScriptTaskParamsSpec"
    assert (
        catalog.normalize_task_params(
            "SHELL",
            canonical_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == canonical_params
    )

    with pytest.raises(UnsupportedFeatureError) as captured:
        catalog.normalize_task_params(
            "SHELL",
            {**canonical_params, "varPool": []},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    assert captured.value.details["field"] == "tasks[].task_params.varPool"

    with pytest.raises(ValueError, match=r"unsupported fields.*futureField"):
        catalog.normalize_task_params(
            "SHELL",
            {**canonical_params, "futureField": True},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "ds_version",
    [
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
    ],
)
def test_legacy_http_exact_review_authorizes_canonical_typed_model(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert "HTTP" in catalog.upstream_task_types
    assert "HTTP" in catalog.reviewed_typed_task_types
    assert catalog.supports_typed_authoring("HTTP") is True
    review = catalog.task_type_facts["HTTP"].typed_authoring_review
    assert review is not None
    assert review.source_task_type == "HTTP"
    assert review.cli_task_type == "HTTP"
    assert review.cli_model == "HttpTaskParamsSpec"


def _restricted_native_opaque_params(ds_version: str) -> dict[str, "YamlObject"]:
    """Independent native discriminators, including exact runtime exclusions."""
    restricted_opaque_params: dict[str, YamlObject] = {
        "ALIYUN_SERVERLESS_SPARK": {
            "type": "ALIYUN_SERVERLESS_SPARK",
            "codeType": "PYTHON",
        },
        "CHUNJUN": {
            "customConfig": 1,
            "json": '{"job":{"content":[]}}',
            "deployMode": "yarn-session",
        },
        "DATASYNC": {
            "jsonFormat": True,
            "json": '{"Name":"raw"}',
        },
        "MR": {
            "programType": "SCALA",
            "mainJar": {"resourceName": "/jobs/native-scala.jar"},
        },
        "SQOOP": {
            "jobType": "CUSTOM",
            "localParams": [],
            "customShell": "arbitrary shell",
        },
    }
    if ds_version == "1.3.9":
        restricted_opaque_params["SQL"] = {
            "type": "MYSQL",
            "datasource": 7,
            "sql": "select 1;",
            "sqlType": 0,
            "sendEmail": True,
            "displayRows": 10,
            "udfs": "",
            "showType": "TABLE",
            "connParams": "",
            "preStatements": [],
            "postStatements": [],
            "title": "",
            "receivers": "owner@example.com",
            "receiversCc": "",
            "limit": 0,
            "localParams": [],
        }
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        restricted_opaque_params["JAVA"] = {
            "runType": "JAVA",
            "rawScript": "public class Main {}",
        }
    elif ds_version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        restricted_opaque_params["JAVA"] = {"runType": "NORMAL_JAR"}
    if ds_version == "3.1.0":
        restricted_opaque_params["DATAX"] = {"customConfig": 0}
    if ds_version in {"3.1.0", "3.1.1", "3.1.2", "3.1.3"}:
        restricted_opaque_params["K8S"] = {
            "namespace": '{"name":"analytics","cluster":"production"}',
            "image": "busybox:1.36",
        }
    if ds_version == "3.1.1":
        restricted_opaque_params["FLINK"] = {"programType": "JAVA"}
    if ds_version in {"3.1.1", "3.1.2", "3.1.3", "3.1.4"}:
        restricted_opaque_params["FLINK_STREAM"] = {"programType": "JAVA"}
    return restricted_opaque_params


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_exact_native_task_types_respect_closed_and_discriminated_opaque_facets(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    closed_opaque_authoring = frozenset(
        {
            "BLOCKING",
            "DATA_FACTORY",
            "DATA_QUALITY",
            "DINKY",
            "DMS",
            "DYNAMIC",
            "GRPC",
            "KUBEFLOW",
            "LINKIS",
            "OPENMLDB",
            "PYTORCH",
            "SAGEMAKER",
            "SEATUNNEL",
            "WATERDROP",
        }
    ) | frozenset(
        task_type
        for task_type in ("K8S", "DATAX")
        if catalog.supports_typed_authoring(task_type)
    )
    if ds_version == "1.3.9":
        closed_opaque_authoring |= {"CONDITIONS", "DEPENDENT", "SUB_PROCESS"}
    restricted_opaque_params = _restricted_native_opaque_params(ds_version)
    params: YamlObject = {
        "nativeField": {"nested": [1, "two", True]},
        "futureField": None,
    }

    for task_type in catalog.upstream_task_types:
        advertised = catalog.supports_opaque_authoring(task_type)
        for intent in (
            TaskAuthoringIntent.OPAQUE_CREATE,
            TaskAuthoringIntent.OPAQUE_EDIT,
        ):
            if task_type in closed_opaque_authoring:
                assert advertised is False
                with pytest.raises(UnsupportedFeatureError):
                    catalog.normalize_task_params(
                        task_type,
                        params,
                        intent=intent,
                    )
                continue

            assert advertised is True
            selected_params = restricted_opaque_params.get(task_type)
            if selected_params is not None:
                with pytest.raises(UnsupportedFeatureError):
                    catalog.normalize_task_params(
                        task_type,
                        params,
                        intent=intent,
                    )
                normalized = catalog.normalize_task_params(
                    task_type,
                    selected_params,
                    intent=intent,
                )
                assert normalized == selected_params
                assert normalized is not selected_params
                continue

            normalized = catalog.normalize_task_params(
                task_type,
                params,
                intent=intent,
            )

            assert normalized == params
            assert normalized is not params
            assert normalized["nativeField"] is not params["nativeField"]


def test_opaque_selector_write_restriction_is_an_explicit_contract_policy() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    aliyun_contract = catalog.require_facet(
        "ALIYUN_SERVERLESS_SPARK",
        ALIYUN_SERVERLESS_SPARK_LITERAL_JAR_SUBMIT_FACET,
    ).contract
    mlflow_contract = catalog.require_facet(
        "MLFLOW",
        MLFLOW_MODEL_SERVE_FACET,
    ).contract

    assert aliyun_contract.opaque_authoring_selector is not None
    assert aliyun_contract.restrict_opaque_authoring_to_selector is True
    assert mlflow_contract.opaque_authoring_selector is not None
    assert mlflow_contract.restrict_opaque_authoring_to_selector is False


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_unknown_future_task_type_is_preserve_only(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    params: YamlObject = {"future": {"plugin": True}}

    preserved = catalog.normalize_task_params(
        "FUTURE_PLUGIN",
        params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    assert preserved == params
    assert preserved is not params

    for intent in (
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError) as captured:
            catalog.normalize_task_params(
                "FUTURE_PLUGIN",
                params,
                intent=intent,
            )
        assert captured.value.details["selected_version"] == ds_version
        assert captured.value.details["intent"] == intent.value


@pytest.mark.parametrize("ds_version", ["3.4.1", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_inline_sql_typed_authoring_has_the_same_normalized_semantics(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    normalized = catalog.normalize_task_params(
        "SQL",
        {
            "type": "MYSQL",
            "datasource": 1,
            "sql": "  select 1;  ",
            "sqlType": 0,
        },
        intent=intent,
    )

    assert normalized == {
        "type": "MYSQL",
        "datasource": 1,
        "sql": "select 1;",
        "sqlType": 0,
        "preStatements": [],
        "postStatements": [],
        "localParams": [],
        "varPool": [],
    }


@pytest.mark.parametrize("version", ["3.4.2", "3.4.3"])
def test_resource_file_is_unsupported_for_typed_authoring_but_preservable(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    membership = catalog.require_facet("SQL", SQL_RESOURCE_FILE_FACET)
    resource_params: YamlObject = {
        "type": "MYSQL",
        "datasource": 1,
        "sqlType": 0,
        "sqlSource": "FILE",
        "sqlResource": "/sql/report.sql",
        "futureNativeField": {"enabled": True},
    }

    assert membership.profile_version == version
    assert membership.typed_create is False
    assert membership.typed_edit is False
    assert membership.opaque_preserve is True
    assert membership.constraint == (
        f"SQL resource-file authoring has not passed the {version} profile gates."
    )

    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError) as captured:
            catalog.normalize_task_params(
                "SQL",
                resource_params,
                intent=intent,
            )

        assert captured.value.details == {
            "selected_version": version,
            "task_type": "SQL",
            "facet": SQL_RESOURCE_FILE_FACET,
            "intent": intent.value,
            "constraint": (
                "SQL resource-file authoring has not passed the "
                f"{version} profile gates."
            ),
        }

    preserved = catalog.normalize_task_params(
        "SQL",
        resource_params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == resource_params
    assert preserved is not resource_params
    assert isinstance(preserved["futureNativeField"], Mapping)
    assert preserved["futureNativeField"] is not resource_params["futureNativeField"]


def test_opaque_preserve_honors_known_facet_membership() -> None:
    source_catalog = get_task_authoring_catalog("3.4.2")
    source_entry = source_catalog.require_task_type("SQL")
    source_membership = source_entry.facets[SQL_INLINE_FACET]
    blocked_catalog = replace(
        source_catalog,
        entries={
            "SQL": replace(
                source_entry,
                facets={
                    **source_entry.facets,
                    SQL_INLINE_FACET: replace(
                        source_membership,
                        opaque_preserve=False,
                        constraint="Inline SQL preservation is disabled for this test.",
                    ),
                },
            )
        },
    )

    with pytest.raises(UnsupportedFeatureError) as captured:
        blocked_catalog.normalize_task_params(
            "SQL",
            {
                "type": "MYSQL",
                "datasource": 1,
                "sql": "select 1",
                "sqlType": 0,
            },
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )

    assert captured.value.details == {
        "selected_version": "3.4.2",
        "task_type": "SQL",
        "facet": SQL_INLINE_FACET,
        "intent": "opaque_preserve",
        "constraint": "Inline SQL preservation is disabled for this test.",
    }


@pytest.mark.parametrize(
    "resource_fields",
    [
        {"sqlSource": "SCRIPT"},
        {"sqlResource": "/sql/report.sql"},
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_341_resource_fields_do_not_gain_typed_authoring_via_extra_allow(
    resource_fields: dict[str, str],
    intent: TaskAuthoringIntent,
) -> None:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 1,
        "sql": "select 1",
        "sqlType": 0,
        **resource_fields,
    }

    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "SQL",
            params,
            intent=intent,
        )

    assert captured.value.details["facet"] == SQL_RESOURCE_FILE_FACET
    assert captured.value.details["selected_version"] == "3.4.1"


def test_opaque_preserve_keeps_an_unmaterialized_native_facet_losslessly() -> None:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 1,
        "sqlType": 0,
        "sqlSource": "FILE",
        "sqlResource": "/sql/report.sql",
    }

    preserved = get_task_authoring_catalog("3.4.1").normalize_task_params(
        "SQL",
        params,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == params
    assert preserved is not params


@pytest.mark.parametrize("ds_version", ["3.4.1", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_typed_sql_authoring_rejects_unknown_task_params_fields(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params: YamlObject = {
        "type": "MYSQL",
        "datasource": 1,
        "sql": "select 1",
        "sqlType": 0,
        "futureMode": "FILE",
        "typoField": 123,
    }

    with pytest.raises(
        ValueError,
        match=r"task_params contains unsupported fields for typed authoring: "
        r"futureMode, typoField",
    ):
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "SQL",
            params,
            intent=intent,
        )


def test_task_authoring_empty_defaults_are_yaml_safe_lists() -> None:
    contract = (
        get_task_authoring_catalog("3.4.1")
        .require_facet(
            "SQL",
            SQL_INLINE_FACET,
        )
        .contract
    )
    fields = {field.path: field.to_data() for field in contract.fields}

    assert fields["task_params.preStatements[]"]["default"] == []
    assert fields["task_params.postStatements[]"]["default"] == []
    assert isinstance(fields["task_params.preStatements[]"]["default"], list)
    assert isinstance(fields["task_params.postStatements[]"]["default"], list)
