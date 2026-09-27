from __future__ import annotations

from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_profiles import task_authoring_profile, task_profile_versions
from dsctl.versioning import DEFAULT_DS_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.models.common import YamlValue

from dsctl.services.task_authoring_catalog.aliyun_serverless_spark import (
    _aliyun_serverless_spark_authoring_profile,
)
from dsctl.services.task_authoring_catalog.blocking import (
    _blocking_authoring_profile,
)
from dsctl.services.task_authoring_catalog.catalog import (
    TaskAuthoringCatalog,
)
from dsctl.services.task_authoring_catalog.chunjun import (
    _chunjun_authoring_profile,
)
from dsctl.services.task_authoring_catalog.data_factory import (
    _data_factory_authoring_profile,
)
from dsctl.services.task_authoring_catalog.data_quality import (
    _data_quality_authoring_profile,
)
from dsctl.services.task_authoring_catalog.datasync import (
    _datasync_authoring_profile,
)
from dsctl.services.task_authoring_catalog.datax import (
    _datax_authoring_profile,
)
from dsctl.services.task_authoring_catalog.dependent import (
    _dependent_authoring_profile,
)
from dsctl.services.task_authoring_catalog.dinky import (
    _dinky_authoring_profile,
)
from dsctl.services.task_authoring_catalog.dms import (
    _dms_authoring_profile,
)
from dsctl.services.task_authoring_catalog.dvc import (
    _dvc_authoring_profile,
)
from dsctl.services.task_authoring_catalog.dynamic import (
    _dynamic_authoring_profile,
)
from dsctl.services.task_authoring_catalog.emr import (
    _emr_authoring_profile,
)
from dsctl.services.task_authoring_catalog.emr_serverless import (
    _emr_serverless_authoring_profile,
)
from dsctl.services.task_authoring_catalog.flink import (
    _flink_inline_sql_authoring_profile,
    _flink_stream_inline_sql_authoring_profile,
)
from dsctl.services.task_authoring_catalog.grpc import (
    _grpc_authoring_profile,
)
from dsctl.services.task_authoring_catalog.hivecli import (
    _hive_cli_authoring_profile,
)
from dsctl.services.task_authoring_catalog.java import (
    _java_authoring_profile,
)
from dsctl.services.task_authoring_catalog.jupyter import (
    _jupyter_authoring_profile,
)
from dsctl.services.task_authoring_catalog.k8s import (
    _k8s_authoring_profile,
)
from dsctl.services.task_authoring_catalog.kubeflow import (
    _kubeflow_authoring_profile,
)
from dsctl.services.task_authoring_catalog.linkis import (
    _linkis_runtime_exclusion_profile,
)
from dsctl.services.task_authoring_catalog.mlflow import (
    _mlflow_model_serve_authoring_profile,
)
from dsctl.services.task_authoring_catalog.mr import (
    _mr_authoring_profile,
)
from dsctl.services.task_authoring_catalog.openmldb import (
    _openmldb_authoring_profile,
)
from dsctl.services.task_authoring_catalog.pigeon import (
    _pigeon_authoring_profile,
)
from dsctl.services.task_authoring_catalog.procedure import (
    _procedure_authoring_profile,
)
from dsctl.services.task_authoring_catalog.pytorch import (
    _pytorch_authoring_profile,
)
from dsctl.services.task_authoring_catalog.sagemaker import (
    _sagemaker_authoring_profile,
)
from dsctl.services.task_authoring_catalog.seatunnel import (
    _seatunnel_authoring_profile,
)
from dsctl.services.task_authoring_catalog.spark import (
    _spark_inline_sql_authoring_profile,
)
from dsctl.services.task_authoring_catalog.sql import (
    _sql_authoring_profile,
)
from dsctl.services.task_authoring_catalog.sqoop import (
    _sqoop_authoring_profile,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskTypeAuthoringProfile,
    TaskTypedAuthoringReview,
    TaskTypeProfileFact,
)
from dsctl.services.task_authoring_catalog.waterdrop import (
    _waterdrop_authoring_profile,
)
from dsctl.services.task_authoring_catalog.zeppelin import (
    _zeppelin_authoring_profile,
)


def get_task_authoring_catalog(ds_version: str) -> TaskAuthoringCatalog:
    """Return the materialized task-authoring slice for one exact profile."""
    normalized = ds_version.strip()
    if normalized in task_profile_versions():
        return _catalog_for_exact_version(normalized)
    supported = ", ".join(task_profile_versions())
    message = (
        f"No task authoring catalog is materialized for {ds_version!r}; "
        f"available exact profiles: {supported}"
    )
    raise ValueError(message)


def default_task_authoring_catalog() -> TaskAuthoringCatalog:
    """Return the authoring catalog for the central stable runtime default."""
    return get_task_authoring_catalog(DEFAULT_DS_VERSION)


def _generated_task_type_facts(version: str) -> dict[str, TaskTypeProfileFact]:
    profile = cast("dict[str, object]", task_authoring_profile(version))
    raw_task_types = cast("dict[str, object]", profile["task_types"])
    raw_reviews = cast("dict[str, object]", profile["typed_authoring_reviews"])
    reviews_by_source: dict[str, TaskTypedAuthoringReview] = {}
    for cli_task_type, raw_review_value in raw_reviews.items():
        raw_review = cast("dict[str, str]", raw_review_value)
        source_task_type = raw_review.get("source_task_type", cli_task_type)
        if source_task_type in reviews_by_source:
            message = (
                f"Generated profile {version} repeats review source {source_task_type}"
            )
            raise ValueError(message)
        reviews_by_source[source_task_type] = TaskTypedAuthoringReview(
            source_task_type=source_task_type,
            cli_task_type=cli_task_type,
            review=raw_review["review"],
            cli_model=raw_review["cli_model"],
            semantic_fingerprint=raw_review["semantic_fingerprint"],
        )
    facts: dict[str, TaskTypeProfileFact] = {}
    for task_type, raw_fact_value in raw_task_types.items():
        raw_fact = cast("dict[str, str]", raw_fact_value)
        facts[task_type] = TaskTypeProfileFact(
            task_type=task_type,
            parameter_model_import=raw_fact["parameter_model_import"],
            semantic_fingerprint=raw_fact["semantic_fingerprint"],
            registration_kind=raw_fact["registration_kind"],
            typed_authoring_review=reviews_by_source.get(task_type),
        )
    missing_review_sources = sorted(set(reviews_by_source) - set(facts))
    if missing_review_sources:
        missing = ", ".join(missing_review_sources)
        message = f"Generated profile {version} reviews missing source facts: {missing}"
        raise ValueError(message)
    return facts


# None explicitly selects the historical model-only authoring path. A missing
# registration must never silently discard a reviewed family's facet policy.
_AUTHORING_PROFILE_BUILDERS: Mapping[
    str,
    Callable[[str], TaskTypeAuthoringProfile] | None,
] = MappingProxyType(
    {
        "ALIYUN_SERVERLESS_SPARK": _aliyun_serverless_spark_authoring_profile,
        "BLOCKING": _blocking_authoring_profile,
        "CHUNJUN": _chunjun_authoring_profile,
        "CONDITIONS": None,
        "DATA_FACTORY": _data_factory_authoring_profile,
        "DATA_QUALITY": _data_quality_authoring_profile,
        "DATAX": _datax_authoring_profile,
        "DATASYNC": _datasync_authoring_profile,
        "DEPENDENT": _dependent_authoring_profile,
        "DINKY": _dinky_authoring_profile,
        "DMS": _dms_authoring_profile,
        "DYNAMIC": _dynamic_authoring_profile,
        "DVC": _dvc_authoring_profile,
        "EMR": _emr_authoring_profile,
        "EMR_SERVERLESS": _emr_serverless_authoring_profile,
        "FLINK": _flink_inline_sql_authoring_profile,
        "FLINK_STREAM": _flink_stream_inline_sql_authoring_profile,
        "GRPC": _grpc_authoring_profile,
        "HIVECLI": _hive_cli_authoring_profile,
        "HTTP": None,
        "JAVA": _java_authoring_profile,
        "JUPYTER": _jupyter_authoring_profile,
        "K8S": _k8s_authoring_profile,
        "KUBEFLOW": _kubeflow_authoring_profile,
        "LINKIS": _linkis_runtime_exclusion_profile,
        "MLFLOW": _mlflow_model_serve_authoring_profile,
        "MR": _mr_authoring_profile,
        "OPENMLDB": _openmldb_authoring_profile,
        "PIGEON": _pigeon_authoring_profile,
        "PROCEDURE": _procedure_authoring_profile,
        "PYTHON": None,
        "PYTORCH": _pytorch_authoring_profile,
        "REMOTESHELL": None,
        "SAGEMAKER": _sagemaker_authoring_profile,
        "SEATUNNEL": _seatunnel_authoring_profile,
        "SHELL": None,
        "SPARK": _spark_inline_sql_authoring_profile,
        "SQOOP": _sqoop_authoring_profile,
        "WATERDROP": _waterdrop_authoring_profile,
        "SQL": _sql_authoring_profile,
        "SUB_WORKFLOW": None,
        "SWITCH": None,
        "ZEPPELIN": _zeppelin_authoring_profile,
    }
)


def _generated_catalog(version: str) -> TaskAuthoringCatalog:
    profile = cast("dict[str, object]", task_authoring_profile(version))
    raw_reviews = cast("dict[str, object]", profile["typed_authoring_reviews"])
    raw_exclusions = cast(
        "dict[str, object]",
        profile.get("typed_authoring_exclusions", {}),
    )
    entries: dict[str, TaskTypeAuthoringProfile] = {}
    for task_type in sorted(raw_reviews.keys() | raw_exclusions.keys()):
        if task_type in raw_reviews and task_type in raw_exclusions:
            message = (
                f"{task_type} {version} cannot be both typed and explicitly excluded"
            )
            raise ValueError(message)
        if task_type not in _AUTHORING_PROFILE_BUILDERS:
            message = (
                f"Generated task policy {version}.{task_type} has no registered "
                "authoring implementation"
            )
            raise ValueError(message)
        builder = _AUTHORING_PROFILE_BUILDERS[task_type]
        if builder is not None:
            entries[task_type] = builder(version)
        elif task_type in raw_exclusions:
            message = (
                f"Generated exclusion {version}.{task_type} has no materialized "
                "authoring policy"
            )
            raise ValueError(message)
    return TaskAuthoringCatalog(
        profile_version=version,
        entries=entries,
        parameter_semantics=get_parameter_semantics(version),
        authoring_surface=get_task_authoring_surface(version),
        task_type_facts=_generated_task_type_facts(version),
        source_contract_fingerprint=cast("str", profile["contract_fingerprint"]),
        source_provenance=cast(
            "Mapping[str, YamlValue]",
            profile["provenance"],
        ),
    )


@cache
def _catalog_for_exact_version(version: str) -> TaskAuthoringCatalog:
    return _generated_catalog(version)
