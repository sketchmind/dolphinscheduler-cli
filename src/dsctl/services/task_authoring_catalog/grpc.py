from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        GrpcAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

GRPC_LITERAL_UNARY_STRING_RECORD_CALL_FACET = "GRPC/literal_unary_string_record_call"


def _grpc_runtime_guidance(surface: GrpcAuthoringSurface) -> str:
    """Describe the exact unary-call runtime, transport, and recovery limits."""
    if not surface.available:
        return "GRPC is absent from this exact DolphinScheduler profile."
    if surface.wire_epoch != "literal-unary-string-record-call":
        message = "Available GRPC surface lacks its exact string-record wire epoch"
        raise ValueError(message)
    return (
        "DolphinScheduler logs the complete authored GRPC parameter object at "
        "INFO; every task field is not secret storage, and dsctl does not detect "
        "or redact secrets. This facet generates one package-free unary protobuf "
        "service with flat string request and response records, fixes response "
        "checking to STATUS_CODE_DEFAULT, and does not substitute DS placeholders. "
        "grpcConnectTimeoutMs is the whole RPC deadline. TLS_DEFAULT uses the "
        "worker JVM/system default trust roots and hostname verification; custom "
        "CA, mTLS client credentials, and request token authentication are not "
        "supported. INSECURE is explicit plaintext transport. The upstream UI "
        "writes grpcCredentialType instead of channelCredentialType, so editing "
        "this task there can silently downgrade TLS to INSECURE. The response is "
        "not propagated as DolphinScheduler output. Cancel is a no-op, there is "
        "no durable application id or failover resume, and retry or worker "
        "failover may duplicate a non-idempotent RPC. Upstream does not shut down "
        "the ManagedChannel or NioEventLoopGroup, so repeated calls can leak "
        "channel and event loop resources. Route to a worker with network reachability "
        "and a remote service whose schema exactly matches the generated record."
    )


def _grpc_fields(
    surface: GrpcAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    runtime = _grpc_runtime_guidance(surface)
    return (
        model_field(
            "task_params.url",
            compile_path="taskDefinitionJson[].taskParams.url",
            description=(
                "Literal DNS/IPv4 host:port target with no scheme, path, query, "
                f"userinfo, placeholder, or control characters. {runtime}"
            ),
        ),
        model_field(
            "task_params.channelCredentialType",
            compile_path=("taskDefinitionJson[].taskParams.channelCredentialType"),
            description=(
                "Exact Java wire credential enum; no transport default is "
                "injected by dsctl. INFO-logged; not secret storage."
            ),
        ),
        model_field(
            "task_params.serviceName",
            compile_path=("taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"),
            description=(
                "Non-reserved protobuf service identifier used to generate both "
                "the proto3 source and protobufjs descriptor. INFO-logged; not "
                "secret storage."
            ),
        ),
        model_field(
            "task_params.methodName",
            compile_path="taskDefinitionJson[].taskParams.methodName",
            description=(
                "Non-reserved protobuf unary method identifier compiled on the "
                "wire as Service/Method. INFO-logged; not secret storage."
            ),
        ),
        model_field(
            "task_params.requestFields",
            compile_path=("taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"),
            description=(
                "Ordered flat request record of uniquely named and numbered "
                "protobuf string fields; an empty record is valid. INFO-logged; "
                "not secret storage."
            ),
        ),
        model_field(
            "task_params.requestFields[]",
            required=False,
            description="One generated request string field.",
        ),
        model_field(
            "task_params.requestFields[].name",
            description=(
                "Unique non-reserved protobuf field identifier outside the "
                "case-insensitive exact denylist: apiKey, authToken, clientSecret, "
                "credential, password, passwd, privateKey, secret, and token."
            ),
        ),
        model_field(
            "task_params.requestFields[].number",
            description=(
                "Unique protobuf field number from 1 through 536870911, excluding "
                "the reserved 19000 through 19999 interval."
            ),
        ),
        model_field(
            "task_params.responseFields",
            compile_path=("taskDefinitionJson[].taskParams.grpcServiceDefinitionJSON"),
            description=(
                "Ordered flat response record of uniquely named and numbered "
                "protobuf string fields; an empty record is valid. INFO-logged; "
                "not secret storage."
            ),
        ),
        model_field(
            "task_params.responseFields[]",
            required=False,
            description="One generated response string field.",
        ),
        model_field(
            "task_params.responseFields[].name",
            description="Unique non-reserved protobuf field identifier.",
        ),
        model_field(
            "task_params.responseFields[].number",
            description=(
                "Unique protobuf field number from 1 through 536870911, excluding "
                "the reserved 19000 through 19999 interval."
            ),
        ),
        model_field(
            "task_params.message",
            compile_path="taskDefinitionJson[].taskParams.message",
            description=(
                "Literal string-valued JSON object whose key set exactly equals "
                "requestFields; placeholders and controls are rejected. "
                "INFO-logged; not secret storage."
            ),
        ),
        TaskAuthoringField(
            "task_params.message.*",
            "string",
            description=(
                "One logged literal request value; not secret storage and never "
                "redacted by dsctl."
            ),
        ),
        model_field(
            "task_params.grpcConnectTimeoutMs",
            compile_path=("taskDefinitionJson[].taskParams.grpcConnectTimeoutMs"),
            description=(
                "Strict positive Java-long RPC deadline in milliseconds; despite "
                "the upstream name, it covers the whole unary call. INFO-logged; "
                "not secret storage."
            ),
        ),
    )


def _grpc_templates(
    surface: GrpcAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    comments = (
        f"# Runtime prerequisite: {_grpc_runtime_guidance(surface)}\n"
        "# Typed scope: one generated package-free unary service with flat string "
        "request/response records and STATUS_CODE_DEFAULT.\n"
        "# Preservation: raw descriptors and complex native state are available "
        "only through unchanged/export opaque preservation.\n"
    )
    body = task_template_with_runtime_controls(
        """# Task template for one literal unary string-record GRPC call
name: call-grpc-service
type: GRPC
description: Invoke one generated unary string-record RPC
task_params:
  url: grpc.example.internal:7443
  channelCredentialType: TLS_DEFAULT
  serviceName: EchoService
  methodName: Echo
  requestFields:
    - name: account
      number: 1
    - name: region
      number: 2
  responseFields:
    - name: result
      number: 1
  message:
    account: alice
    region: cn
  grpcConnectTimeoutMs: 10000
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Call one generated unary GRPC string-record method.",
            payload_modes=("task_params",),
            yaml=f"{comments}{body}",
        ),
    )


def _grpc_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).grpc
    if not surface.available:
        message = f"GRPC is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=GRPC_LITERAL_UNARY_STRING_RECORD_CALL_FACET,
        family="grpc-literal-unary-string-record-call-v1",
        review="grpc-literal-unary-string-record-call-exact-subset",
        params_model=_family_model("GRPC"),
        fields=_grpc_fields(surface),
        state_rules=(),
        templates=_grpc_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "GRPC/literal_unary_string_record_call does not expose raw proto or "
            "descriptor create/edit; preserve complex native tasks unchanged."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="GRPC",
        category="Universal",
        kind="typed",
        default_facet=GRPC_LITERAL_UNARY_STRING_RECORD_CALL_FACET,
        facets={GRPC_LITERAL_UNARY_STRING_RECORD_CALL_FACET: membership},
    )
