from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

AliyunServerlessSparkRuntimeEpoch = Literal[
    "legacy-single-submit",
    "retry-client-token",
    "retry-client-token-cause",
]
AliyunServerlessSparkFailedExit = Literal["KILL", "FAILURE"]
AliyunServerlessSparkCredentialSource = Literal["datasource"]


@dataclass(frozen=True, slots=True)
class AliyunServerlessSparkAuthoringSurface:
    """Aliyun JAR submission, retry, credentials, and recovery semantics."""

    available: bool
    runtime_epoch: AliyunServerlessSparkRuntimeEpoch | None
    template_fetch_always: bool
    template_may_mutate_submit_parameters: bool
    template_may_derive_release_and_fusion: bool
    retry_attempts: int
    retry_interval_ms: int
    template_retry: bool
    start_retry: bool
    status_retry: bool
    cancel_retry: bool
    client_token_within_attempt: bool
    failed_exit_status: AliyunServerlessSparkFailedExit | None
    start_exception_cause_preserved: bool
    cancel_failure_propagated: bool
    cancel_exception_cause_preserved: bool
    task_params_logged: bool
    job_id_logged: bool
    state_logged: bool
    credentials_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_may_duplicate: bool
    cancel_requires_in_memory_job_id: bool
    poll_interval_seconds: int
    poll_deadline: bool
    credential_source: AliyunServerlessSparkCredentialSource | None
    credential_keys: tuple[str, ...]
    custom_endpoint_supported: bool
    default_endpoint_template: str | None
    connectivity_check_verified: bool

    @property
    def failed_exit(self) -> AliyunServerlessSparkFailedExit | None:
        """Retain the concise discovery spelling for the exact exit mapping."""
        return self.failed_exit_status


_ALIYUN_SERVERLESS_SPARK_ABSENT = AliyunServerlessSparkAuthoringSurface(
    available=False,
    runtime_epoch=None,
    template_fetch_always=False,
    template_may_mutate_submit_parameters=False,
    template_may_derive_release_and_fusion=False,
    retry_attempts=0,
    retry_interval_ms=0,
    template_retry=False,
    start_retry=False,
    status_retry=False,
    cancel_retry=False,
    client_token_within_attempt=False,
    failed_exit_status=None,
    start_exception_cause_preserved=False,
    cancel_failure_propagated=False,
    cancel_exception_cause_preserved=False,
    task_params_logged=False,
    job_id_logged=False,
    state_logged=False,
    credentials_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_may_duplicate=False,
    cancel_requires_in_memory_job_id=False,
    poll_interval_seconds=0,
    poll_deadline=False,
    credential_source=None,
    credential_keys=(),
    custom_endpoint_supported=False,
    default_endpoint_template=None,
    connectivity_check_verified=False,
)
_ALIYUN_SERVERLESS_SPARK_LEGACY = AliyunServerlessSparkAuthoringSurface(
    available=True,
    runtime_epoch="legacy-single-submit",
    template_fetch_always=True,
    template_may_mutate_submit_parameters=True,
    template_may_derive_release_and_fusion=True,
    retry_attempts=11,
    retry_interval_ms=1000,
    template_retry=False,
    start_retry=False,
    status_retry=True,
    cancel_retry=False,
    client_token_within_attempt=False,
    failed_exit_status="KILL",
    start_exception_cause_preserved=False,
    cancel_failure_propagated=False,
    cancel_exception_cause_preserved=False,
    task_params_logged=True,
    job_id_logged=True,
    state_logged=True,
    credentials_logged=False,
    result_output_supported=False,
    durable_application_id=False,
    failover_supported=False,
    retry_may_duplicate=True,
    cancel_requires_in_memory_job_id=True,
    poll_interval_seconds=10,
    poll_deadline=False,
    credential_source="datasource",
    credential_keys=("accessKeyId", "accessKeySecret", "regionId"),
    custom_endpoint_supported=True,
    default_endpoint_template="emr-serverless-spark.%s.aliyuncs.com",
    connectivity_check_verified=False,
)
_ALIYUN_SERVERLESS_SPARK_RETRY = replace(
    _ALIYUN_SERVERLESS_SPARK_LEGACY,
    runtime_epoch="retry-client-token",
    template_retry=True,
    start_retry=True,
    cancel_retry=True,
    client_token_within_attempt=True,
    failed_exit_status="FAILURE",
    cancel_failure_propagated=True,
)
_ALIYUN_SERVERLESS_SPARK_RETRY_CAUSE = replace(
    _ALIYUN_SERVERLESS_SPARK_RETRY,
    runtime_epoch="retry-client-token-cause",
    start_exception_cause_preserved=True,
    cancel_exception_cause_preserved=True,
)


def _aliyun_serverless_spark_surface(
    version: str,
) -> AliyunServerlessSparkAuthoringSurface:
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    }:
        return _ALIYUN_SERVERLESS_SPARK_ABSENT
    if version == "3.3.1":
        return _ALIYUN_SERVERLESS_SPARK_LEGACY
    if version in {"3.3.2", "3.4.0", "3.4.1"}:
        return _ALIYUN_SERVERLESS_SPARK_RETRY
    if version in {"3.4.2", "3.4.3"}:
        return _ALIYUN_SERVERLESS_SPARK_RETRY_CAUSE
    message = (
        "No exact ALIYUN_SERVERLESS_SPARK authoring surface for "
        f"DolphinScheduler {version}"
    )
    raise ValueError(message)
