from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pytest

from dsctl.models.task_spec import (
    SparkInlineLocalSqlTaskParamsSpec,
    normalize_typed_task_params,
    task_params_model_for_type,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject


_LEGACY_SPARK_VERSIONS = ("3.0.0", "3.0.6", "3.1.0", "3.1.9")
_SCRIPT_SPARK_VERSIONS = ("3.2.0", "3.2.1")
_MASTER_SPARK_VERSIONS = (
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_SPARK_VERSIONS = (
    *_LEGACY_SPARK_VERSIONS,
    *_SCRIPT_SPARK_VERSIONS,
    *_MASTER_SPARK_VERSIONS,
)


@pytest.fixture
def refs() -> TaskRefIndex:
    """SPARK has no task references but shares the pure projector seam."""
    return TaskRefIndex.from_code_by_name({})


def test_spark_inline_local_sql_model_preserves_one_safe_literal_script() -> None:
    authored: YamlObject = {
        "rawScript": "SELECT order_id\nFROM fact_orders\nWHERE paid = true",
    }

    model = task_params_model_for_type("spark")

    assert model is SparkInlineLocalSqlTaskParamsSpec
    assert normalize_typed_task_params(model, authored) == authored


@pytest.mark.parametrize(
    "raw_script",
    [
        "",
        " \t\r\n",
        "\u0085",
        "\ufeff",
        "SELECT ${business_date}",
        "SELECT $[yyyyMMdd-1]",
        "SELECT 1\x00",
        "SELECT 1\x01",
        "SELECT 1\x7f",
    ],
)
def test_spark_inline_local_sql_model_rejects_unsafe_script(
    raw_script: str,
) -> None:
    with pytest.raises(ValueError):
        normalize_typed_task_params(
            SparkInlineLocalSqlTaskParamsSpec,
            {"rawScript": raw_script},
        )


def test_spark_inline_local_sql_model_rejects_unowned_fields() -> None:
    with pytest.raises(ValueError):
        normalize_typed_task_params(
            SparkInlineLocalSqlTaskParamsSpec,
            {"rawScript": "SELECT 1", "localParams": []},
        )


@pytest.mark.parametrize("version", _LEGACY_SPARK_VERSIONS)
def test_spark_inline_sql_projects_legacy_spark2_wire(
    version: str,
    refs: TaskRefIndex,
) -> None:
    canonical: JsonObject = {"rawScript": "SELECT 1\n"}
    native: JsonObject = {
        "programType": "SQL",
        "sparkVersion": "SPARK2",
        "rawScript": "SELECT 1\n",
        "deployMode": "local",
    }

    assert (
        encode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=canonical,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == native
    )
    assert (
        decode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", _SCRIPT_SPARK_VERSIONS)
def test_spark_inline_sql_projects_script_wire(
    version: str,
    refs: TaskRefIndex,
) -> None:
    canonical: JsonObject = {"rawScript": "SELECT 2"}
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT 2",
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
    }

    assert (
        encode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=canonical,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == native
    )
    assert (
        decode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", _MASTER_SPARK_VERSIONS)
def test_spark_inline_sql_projects_explicit_local_master_wire(
    version: str,
    refs: TaskRefIndex,
) -> None:
    canonical: JsonObject = {"rawScript": "SELECT 3"}
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT 3",
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
        "master": "local",
    }

    assert (
        encode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=canonical,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == native
    )
    assert (
        decode_task_parameters(
            version=version,
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("master", [pytest.param(None, id="missing"), "", "local"])
def test_spark_decode_accepts_runtime_equivalent_local_master_forms(
    master: str | None,
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT 4",
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
    }
    if master is not None:
        native["master"] = master

    assert decode_task_parameters(
        version="3.4.2",
        task_type="SPARK",
        task_params=native,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params == {"rawScript": "SELECT 4"}


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_spark_typed_facet_fails_closed_before_3_0(
    version: str,
    direction: str,
    refs: TaskRefIndex,
) -> None:
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(
        TaskParameterProjectionError,
        match=f"SPARK inline local SQL is unavailable in DolphinScheduler {version}",
    ):
        projector(
            version=version,
            task_type="SPARK",
            task_params={"rawScript": "SELECT 1"},
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"rawScript": 7},
        {"rawScript": "  \n"},
        {"rawScript": "SELECT ${run_date}"},
        {"rawScript": "SELECT 1", "localParams": []},
    ],
)
def test_spark_typed_encode_rejects_invalid_canonical_without_downgrade(
    params: JsonObject,
    refs: TaskRefIndex,
) -> None:
    with pytest.raises(TaskParameterProjectionError):
        encode_task_parameters(
            version="3.4.2",
            task_type="SPARK",
            task_params=params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "override",
    [
        {"programType": "SCALA"},
        {"deployMode": "cluster"},
        {"sqlExecutionType": "FILE"},
        {"master": "spark://cluster:7077"},
        {"sparkVersion": "SPARK2"},
        {"futureField": {"native": True}},
        {"rawScript": "SELECT $[yyyyMMdd]"},
    ],
)
def test_spark_typed_decode_rejects_noncanonical_native_modes(
    override: JsonObject,
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT 1",
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
        "master": "local",
        **override,
    }

    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters(
            version="3.4.2",
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "native",
    [
        {"rawScript": "SELECT 5"},
        {"rawScript": "SELECT ${run_date}"},
    ],
)
def test_spark_opaque_encode_is_identity_without_decode_provenance(
    native: JsonObject,
    refs: TaskRefIndex,
) -> None:
    assert (
        encode_task_parameters(
            version="3.4.2",
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.OPAQUE_PRESERVE,
        ).task_params
        == native
    )


@pytest.mark.parametrize(
    "native",
    [
        {"programType": "JAVA", "mainJar": {"id": 7}},
        {"programType": "SCALA", "mainJar": {"id": 8}},
        {"programType": "PYTHON", "mainJar": {"id": 9}},
        {
            "programType": "SQL",
            "rawScript": "/resources/query.sql",
            "deployMode": "local",
            "sqlExecutionType": "FILE",
            "master": "local",
            "resourceList": [{"id": 10}],
        },
        {
            "programType": "SQL",
            "rawScript": "SELECT ${run_date}",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
            "master": "local",
        },
        {
            "programType": "SQL",
            "rawScript": "SELECT 1",
            "deployMode": "cluster",
            "sqlExecutionType": "SCRIPT",
            "master": "spark://cluster:7077",
        },
        {
            "programType": "SQL",
            "rawScript": "SELECT 1",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
            "master": "local",
            "futureState": {"attempt": 2},
        },
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_spark_opaque_native_provenance_is_identity_preserved(
    native: JsonObject,
    direction: str,
    refs: TaskRefIndex,
) -> None:
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )
    original = deepcopy(native)

    projected = projector(
        version="3.4.2",
        task_type="SPARK",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert projected.task_params == original
    native["mutated"] = True
    assert projected.task_params == original
    first_copy = projected.task_params
    first_copy["mutated"] = True
    assert projected.task_params == original


def test_spark_opaque_decode_canonicalizes_only_exact_safe_inline_native(
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT 6",
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
        "master": "",
    }

    decoded = decode_task_parameters_with_provenance(
        version="3.4.2",
        task_type="SPARK",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task.task_params == {"rawScript": "SELECT 6"}
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    "native",
    [
        {"rawScript": "SELECT 6"},
        {
            "programType": "SQL",
            "rawScript": "SELECT ${run_date}",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
            "master": "local",
        },
    ],
)
def test_spark_opaque_decode_marks_unrecognized_native_for_identity_reencode(
    native: JsonObject,
    refs: TaskRefIndex,
) -> None:
    decoded = decode_task_parameters_with_provenance(
        version="3.4.2",
        task_type="SPARK",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task.task_params == native
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_spark_opaque_wire_before_3_0_remains_lossless(
    direction: str,
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "programType": "SQL",
        "rawScript": "SELECT legacy",
        "deployMode": "local",
        "sparkVersion": "SPARK2",
        "futureState": {"keep": True},
    }
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    assert (
        projector(
            version="2.0.9",
            task_type="SPARK",
            task_params=native,
            refs=refs,
            source=ProjectionSource.OPAQUE_PRESERVE,
        ).task_params
        == native
    )
