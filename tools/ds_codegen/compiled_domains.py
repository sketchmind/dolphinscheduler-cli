"""Compile declarative domains into one shared exact wire-program format."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass, field, replace
from pprint import pformat
from textwrap import indent
from typing import TYPE_CHECKING, Literal, cast

from ds_codegen.adapter_wire_candidates import (
    effective_request_candidate,
    effective_response_candidate,
)
from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS, WireTypeRef
from ds_codegen.compiled_codec_records import render_codec_records
from ds_codegen.compiled_schema_names import schema_module_name, schema_symbol_name
from ds_codegen.compiled_schema_pool import unique_schema_modules
from ds_codegen.compiled_wire_artifacts import CompiledWireModule
from ds_codegen.contract_inputs import canonical_json_digest, contract_snapshot_digest
from ds_codegen.contract_visibility import (
    is_client_supplied_parameter,
    is_required_parameter,
)
from ds_codegen.operation_paths import (
    executable_path_operation,
    operation_path_arguments,
)
from ds_codegen.render.package import planner
from ds_codegen.render.package.executable_schema import (
    executable_schema_digest,
    render_operation_request_params,
    render_operation_response_module,
)
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
    slice_contract_for_bindings,
)
from dsctl.support.json_types import is_json_value

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ds_codegen.compatibility_impact import ReviewedBinding
    from ds_codegen.ir import ContractSnapshot, DtoSpec, ModelSpec, OperationSpec
    from ds_codegen.runtime_bundles import RuntimeBundle, RuntimeBundleMetadata
    from ds_codegen.snapshot_resolution import SnapshotTypeResolver
    from dsctl.support.json_types import JsonObject, JsonValue

_Status = Literal["supported", "upstream_absent"]
_Channel = Literal[
    "form",
    "json",
    "json_text",
    "multipart",
    "path",
    "path_form",
    "path_json",
    "path_json_text",
    "path_query",
    "query",
]
_RequestBinding = Literal["path_variable", "request_body", "request_param"]
_Method = Literal["DELETE", "GET", "POST", "PUT"]
_Envelope = Literal["optional", "required"]
_ABSENT: _Status = "upstream_absent"
_SUPPORTED: _Status = "supported"
_DOMAIN_NAME = re.compile(r"[a-z_][a-z0-9_]*\Z")
_RECIPE_ID = re.compile(r"[a-z][a-z0-9_]*\Z")
_SCHEMA_CONSTANT = re.compile(r"[A-Z][A-Z0-9_]*\Z")
_PATH_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PATH_ENCODING = "percent-encoded-utf8-segment-v1"
_LEGACY_PATH_ENCODING = "legacy-url-interpolation-v1"
_REVIEWED_OPERATION_NAMES = frozenset(
    name
    for version in REVIEWED_DS_VERSIONS
    for name in (
        *runtime_operation_bindings(version),
        *runtime_auxiliary_operation_bindings(version),
    )
)
_PATH_SEGMENT_COUNTS: dict[tuple[_Method, _Channel], frozenset[int]] = {
    ("DELETE", "path"): frozenset({1, 2}),
    ("GET", "path"): frozenset({1, 2}),
    ("GET", "path_query"): frozenset({1, 2}),
    ("POST", "path"): frozenset({2}),
    ("POST", "path_form"): frozenset({1, 2}),
    ("PUT", "path_form"): frozenset({1, 2}),
    ("PUT", "path_json"): frozenset({1}),
    ("PUT", "path_json_text"): frozenset({1}),
}
_CHANNEL_BINDINGS: dict[_Channel, tuple[_RequestBinding, ...]] = {
    "form": ("request_param",),
    "json": ("request_body",),
    "json_text": ("request_body",),
    "multipart": ("request_param",),
    "path": ("path_variable",),
    "path_form": ("path_variable", "request_param"),
    "path_json": ("path_variable", "request_body"),
    "path_json_text": ("path_variable", "request_body"),
    "path_query": ("path_variable", "request_param"),
    "query": ("request_param",),
}


@dataclass(frozen=True)
class CompiledRequestEpoch:
    """One reviewed request transport and executable-schema epoch."""

    method: _Method
    path: str
    channel: _Channel
    request_schema: str
    request_model: str
    request_fields: tuple[str, ...]
    path_fields: tuple[str, ...] = ()
    required_fields: frozenset[str] | None = None
    versions: frozenset[str] | None = None
    path_encoding: str = _PATH_ENCODING
    content_addressed: bool = False
    file_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompiledPrimitive:
    """One stable primitive and its finite reviewed request epochs."""

    name: str
    requests: tuple[CompiledRequestEpoch, ...]
    result_envelope: _Envelope
    absent_versions: frozenset[str] = frozenset()
    response_transport: Literal["json", "binary"] = "json"


@dataclass(frozen=True)
class CompiledOperationDependency:
    """One direct provider clause reviewed for explicit exact releases."""

    providers: tuple[str, ...]
    versions: frozenset[str]


CompiledDependencyDeclaration = (
    tuple[str, ...]
    | CompiledOperationDependency
    | tuple[CompiledOperationDependency, ...]
)


@dataclass(frozen=True)
class CompiledResponsePolicy:
    """Domain-owned response identity selected from one exact operation."""

    codec: str
    schema: str | None
    capture: JsonValue
    scalar_annotation: str | None = None
    content_addressed: bool = False
    response_transport: Literal["json", "binary"] = "json"


@dataclass(frozen=True)
class CompiledDomainDefinition:
    """Narrow source-policy interface consumed by the shared compiler."""

    name: str
    schema_constant: str
    schema_version: int
    semantic_operations: frozenset[str]
    absent_versions: frozenset[str]
    primitives: tuple[CompiledPrimitive, ...]
    classify_operation: Callable[[OperationSpec], str | None]
    response_policy: Callable[
        [ContractSnapshot, OperationSpec, str], CompiledResponsePolicy
    ]
    recipe_policy: Callable[[Mapping[str, str]], str]
    semantic_absent_versions: Mapping[str, frozenset[str]] = field(
        default_factory=dict, kw_only=True
    )


@dataclass(frozen=True)
class CompiledProgram:
    """One exact source operation bound to shared executable schemas."""

    source_operation: str
    codec: str
    codec_digest: str
    request_schema_digest: str
    response_digest: str
    response_schema_digest: str | None
    result_envelope: _Envelope
    program_digest: str

    def record(self) -> JsonObject:
        return {
            "source_operation": self.source_operation,
            "codec": self.codec,
            "codec_digest": self.codec_digest,
            "request_schema_digest": self.request_schema_digest,
            "response_digest": self.response_digest,
            "response_schema_digest": self.response_schema_digest,
            "result_envelope": self.result_envelope,
            "program_digest": self.program_digest,
        }


@dataclass(frozen=True)
class CompiledProfile:
    """One explicit exact-version compiled-domain decision."""

    version: str
    status: _Status
    source_tag: str
    source_commit: str
    source_tree: str
    source_contract_digest: str
    recipe_id: str | None = None
    programs: tuple[tuple[str, CompiledProgram], ...] = ()

    def record(self) -> JsonObject:
        record: JsonObject = {
            "status": self.status,
            "source": {
                "tag": self.source_tag,
                "commit": self.source_commit,
                "tree": self.source_tree,
                "contract_digest": self.source_contract_digest,
            },
            "recipe_id": self.recipe_id,
            "programs": {name: program.record() for name, program in self.programs},
        }
        record["profile_digest"] = canonical_json_digest(
            {"schema_version": 2, **record}
        )
        return record


@dataclass(frozen=True)
class CompiledRequest:
    schema: str
    class_name: str
    source: str
    executable_digest: str
    module_name: str | None = None
    content: str | None = None
    content_addressed: bool = False
    pool_modules: tuple[CompiledWireModule, ...] = ()


@dataclass(frozen=True)
class CompiledResponseModule:
    schema: str
    module_name: str
    root_annotation: str
    executable_digest: str
    content: str
    pool_modules: tuple[CompiledWireModule, ...] = ()


@dataclass(frozen=True)
class CompiledScalarResponse:
    schema: str
    annotation: str
    executable_digest: str


@dataclass(frozen=True)
class CompiledDomainPlan:
    definition: CompiledDomainDefinition
    profiles: tuple[CompiledProfile, ...]
    requests: tuple[CompiledRequest, ...]
    responses: tuple[CompiledResponseModule, ...]
    scalar_responses: tuple[CompiledScalarResponse, ...]
    codecs: tuple[tuple[str, JsonObject], ...]

    def profile(self, version: str) -> CompiledProfile:
        matches = tuple(item for item in self.profiles if item.version == version)
        if len(matches) != 1:
            message = (
                f"compiled {self.definition.name} plan has no unique "
                f"DS {version} profile"
            )
            raise ValueError(message)
        return matches[0]


@dataclass(frozen=True)
class CompiledDomainSet:
    """All domain plans plus the exact bundles left after union ownership."""

    plans: tuple[CompiledDomainPlan, ...]
    legacy_bundles: tuple[RuntimeBundle, ...]

    def plan(self, name: str) -> CompiledDomainPlan:
        matches = tuple(item for item in self.plans if item.definition.name == name)
        if len(matches) != 1:
            message = f"compiled domain set has no unique {name!r} plan"
            raise ValueError(message)
        return matches[0]

    def modules(self) -> tuple[CompiledWireModule, ...]:
        return unique_schema_modules(
            module for plan in self.plans for module in _compile_domain_modules(plan)
        )


def compile_domains(
    bundles: tuple[RuntimeBundle, ...],
    definitions: tuple[CompiledDomainDefinition, ...],
    *,
    operation_dependencies: Mapping[str, CompiledDependencyDeclaration] | None = None,
) -> CompiledDomainSet:
    """Compile every definition from raw bundles, then strip their ownership union."""
    _validate_definitions(definitions)
    # Bundles retain their snapshots for this invocation; no context survives it.
    source_contexts: dict[int, planner.PackageRenderContext] = {}
    plans = tuple(
        _compile_domain_plan(
            bundles,
            definition,
            source_contexts=source_contexts,
            operation_dependencies=operation_dependencies,
        )
        for definition in definitions
    )
    legacy_bundles = tuple(
        _strip_compiled_ownership(
            bundle, definitions, operation_dependencies=operation_dependencies
        )
        for bundle in bundles
    )
    return CompiledDomainSet(plans=plans, legacy_bundles=legacy_bundles)


def require_model(
    snapshot: ContractSnapshot,
    import_path: str,
    *,
    domain: str,
) -> DtoSpec | ModelSpec:
    """Return one exact structured model for a narrow domain policy."""
    matches: list[DtoSpec | ModelSpec] = []
    matches.extend(item for item in snapshot.dtos if item.import_path == import_path)
    matches.extend(item for item in snapshot.models if item.import_path == import_path)
    if len(matches) != 1:
        message = f"compiled {domain} requires one exact model {import_path}"
        raise ValueError(message)
    return matches[0]


def model_field_facts(
    model: DtoSpec | ModelSpec,
) -> tuple[tuple[str, str, bool, str | None, str | None], ...]:
    """Project exact model fields in their declared order for domain review."""
    return tuple(
        (
            field.wire_name,
            field.java_type,
            field.nullable,
            field.default_value,
            field.default_factory,
        )
        for field in model.fields
    )


def _compile_domain_plan(
    bundles: tuple[RuntimeBundle, ...],
    definition: CompiledDomainDefinition,
    *,
    source_contexts: dict[int, planner.PackageRenderContext],
    operation_dependencies: Mapping[str, CompiledDependencyDeclaration] | None = None,
) -> CompiledDomainPlan:
    by_version = {bundle.spec.version: bundle for bundle in bundles}
    if (
        not by_version
        or len(by_version) != len(bundles)
        or not by_version.keys() <= set(REVIEWED_DS_VERSIONS)
    ):
        message = (
            f"compiled {definition.name} requires unique reviewed exact runtime bundles"
        )
        raise ValueError(message)
    versions = tuple(sorted(by_version, key=_version_key))

    primitives = {item.name: item for item in definition.primitives}
    profiles: list[CompiledProfile] = []
    requests: dict[str, tuple[str, CompiledRequest]] = {}
    responses: dict[str, tuple[str, CompiledResponseModule]] = {}
    scalar_responses: dict[str, CompiledScalarResponse] = {}
    codecs: dict[str, tuple[str, JsonObject]] = {}
    used_request_roles: set[str] = set()
    for version in versions:
        bundle = by_version[version]
        all_bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        dependencies = _validate_operation_dependencies(
            version, all_bindings, operation_dependencies or {}
        )
        bindings = _domain_bindings(definition, version)
        if version in definition.absent_versions:
            if bindings:
                message = f"compiled {definition.name} DS {version} must be absent"
                raise ValueError(message)
            profiles.append(_profile(bundle.metadata, status=_ABSENT))
            continue
        expected_semantics = {
            operation
            for operation in definition.semantic_operations
            if version not in definition.semantic_absent_versions.get(operation, ())
        }
        if not expected_semantics or set(bindings) != expected_semantics:
            message = (
                f"compiled {definition.name} DS {version} semantic binding "
                "inventory is incomplete"
            )
            raise ValueError(message)
        operations = _domain_operations(
            bundle.snapshot,
            definition,
            bindings,
            version=version,
            all_bindings=all_bindings,
            operation_dependencies=dependencies,
        )
        request_epochs = {
            primitive_name: _select_request_epoch(
                definition,
                primitives[primitive_name],
                operation,
                bundle.snapshot,
            )
            for primitive_name, operation in operations.items()
        }
        source_context = source_contexts.get(id(bundle.snapshot))
        if source_context is None:
            source_context = planner.build_package_context(bundle.snapshot)
            source_contexts[id(bundle.snapshot)] = source_context
        request_by_primitive = {
            primitive_name: _compile_request(
                definition,
                primitives[primitive_name],
                request_epochs[primitive_name],
                bundle.snapshot,
                operation,
                version=version,
                requests=requests,
                source_context=source_context,
            )
            for primitive_name, operation in operations.items()
        }
        used_request_roles.update(
            epoch.request_schema for epoch in request_epochs.values()
        )
        request_epochs = {
            name: replace(epoch, request_schema=request_by_primitive[name].schema)
            for name, epoch in request_epochs.items()
        }
        resolver = source_context.type_resolver
        compiled_programs: list[tuple[str, CompiledProgram]] = []
        for primitive in definition.primitives:
            if version in primitive.absent_versions:
                continue
            operation = operations[primitive.name]
            policy = definition.response_policy(
                bundle.snapshot,
                operation,
                primitive.name,
            )
            _validate_response_policy(definition, policy)
            _validate_response_transport(primitive, policy, operation)
            policy, response_schema_digest = _compile_response(
                definition,
                bundle.snapshot,
                operation,
                policy,
                version=version,
                responses=responses,
                scalar_responses=scalar_responses,
                source_context=source_context,
            )
            request = request_by_primitive[primitive.name]
            codec = _codec_record(
                request_epochs[primitive.name],
                policy,
                response_projection=operation.response_projection,
                request_schema_digest=request.executable_digest,
                response_schema_digest=response_schema_digest,
            )
            previous_codec = codecs.get(policy.codec)
            if previous_codec is not None and previous_codec[1] != codec:
                message = (
                    f"compiled {definition.name} codec {policy.codec} differs between "
                    f"DS {previous_codec[0]} and {version}"
                )
                raise ValueError(message)
            codecs[policy.codec] = (version, codec)
            compiled_programs.append(
                (
                    primitive.name,
                    _compile_program(
                        definition,
                        bundle.snapshot,
                        operation,
                        primitive=primitive,
                        request_epoch=request_epochs[primitive.name],
                        policy=policy,
                        codec_record=codec,
                        resolver=resolver,
                        request_schema_digest=request.executable_digest,
                        response_schema_digest=response_schema_digest,
                    ),
                )
            )
        recipe_id = definition.recipe_policy(
            {name: program.codec for name, program in compiled_programs}
        )
        if _RECIPE_ID.fullmatch(recipe_id) is None:
            message = f"compiled {definition.name} recipe identity is invalid"
            raise ValueError(message)
        profiles.append(
            _profile(
                bundle.metadata,
                status=_SUPPORTED,
                recipe_id=recipe_id,
                programs=tuple(compiled_programs),
            )
        )

    actual_absent = {
        profile.version for profile in profiles if profile.status == _ABSENT
    }
    if actual_absent != set(definition.absent_versions) & set(versions):
        message = f"compiled {definition.name} absent-version decisions are not exact"
        raise ValueError(message)
    expected_requests = {
        request.request_schema
        for primitive in definition.primitives
        for request in primitive.requests
    }
    if (
        versions == REVIEWED_DS_VERSIONS and used_request_roles != expected_requests
    ) or not used_request_roles <= expected_requests:
        message = f"compiled {definition.name} request-schema inventory is incomplete"
        raise ValueError(message)
    used_response_schemas = {
        cast("str", record["response"])
        for _, record in codecs.values()
        if record["response"] is not None
    }
    actual_response_schemas = set(responses) | set(scalar_responses)
    if actual_response_schemas != used_response_schemas:
        message = f"compiled {definition.name} response-schema inventory is incomplete"
        raise ValueError(message)
    return CompiledDomainPlan(
        definition=definition,
        profiles=tuple(profiles),
        requests=tuple(requests[name][1] for name in sorted(requests)),
        responses=tuple(responses[name][1] for name in sorted(responses)),
        scalar_responses=tuple(
            scalar_responses[name] for name in sorted(scalar_responses)
        ),
        codecs=tuple((name, codecs[name][1]) for name in sorted(codecs)),
    )


def _compile_request(
    definition: CompiledDomainDefinition,
    primitive: CompiledPrimitive,
    request_epoch: CompiledRequestEpoch,
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    version: str,
    requests: dict[str, tuple[str, CompiledRequest]],
    source_context: planner.PackageRenderContext | None = None,
) -> CompiledRequest:
    _request_record(definition, primitive.name, request_epoch, operation, snapshot)
    rendered = render_operation_request_params(
        snapshot,
        replace(
            operation,
            parameters=[
                parameter
                for parameter in operation.parameters
                if parameter.wire_name not in request_epoch.file_fields
            ],
        )
        if request_epoch.file_fields
        else operation,
        class_name=request_epoch.request_model,
        parameter_bindings=_CHANNEL_BINDINGS[request_epoch.channel],
        module_parts=(
            "wire_programs",
            "_schemas",
            definition.name,
            request_epoch.request_schema,
        ),
        root_model_module_parts=("wire_runtime", "_models"),
        source_context=source_context,
    )
    schema = _compiled_request_schema_name(request_epoch, rendered.executable_digest)
    module_name = schema_module_name(
        definition.name, "request", schema, rendered.executable_digest
    )
    content = (
        rendered.support_source
        + f"EXECUTABLE_SCHEMA_DIGEST = {rendered.executable_digest!r}\n"
        if rendered.support_source is not None
        else None
    )
    if content is not None:
        compile(content, f"<{module_name}>", "exec")
    request = CompiledRequest(
        schema=schema,
        class_name=request_epoch.request_model,
        source=rendered.source,
        executable_digest=rendered.executable_digest,
        module_name=module_name if content is not None else None,
        content=content,
        content_addressed=request_epoch.content_addressed,
        pool_modules=rendered.pool_modules,
    )
    previous = requests.get(request.schema)
    if previous is not None and previous[1] != request:
        message = (
            f"compiled {definition.name} request schema {request.schema} differs "
            f"between DS {previous[0]} and {version}"
        )
        raise ValueError(message)
    requests[request.schema] = (version, request)
    return request


def _compiled_request_schema_name(epoch: CompiledRequestEpoch, digest: str) -> str:
    """Resolve only artifact sharing; reviewed request selection remains separate."""
    if epoch.content_addressed:
        return f"{epoch.request_schema}_{digest.removeprefix('sha256:')}"
    return epoch.request_schema


def _compile_response(
    definition: CompiledDomainDefinition,
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    policy: CompiledResponsePolicy,
    *,
    version: str,
    responses: dict[str, tuple[str, CompiledResponseModule]],
    scalar_responses: dict[str, CompiledScalarResponse],
    source_context: planner.PackageRenderContext | None = None,
) -> tuple[CompiledResponsePolicy, str | None]:
    if policy.schema is None:
        if policy.scalar_annotation is not None:
            message = f"compiled {definition.name} response policy is inconsistent"
            raise ValueError(message)
        return policy, None
    if policy.scalar_annotation is not None:
        scalar_response = CompiledScalarResponse(
            schema=policy.schema,
            annotation=policy.scalar_annotation,
            executable_digest=executable_schema_digest(
                kind="response",
                source="",
                root_annotation=policy.scalar_annotation,
            ),
        )
        previous_scalar = scalar_responses.get(policy.schema)
        if previous_scalar is not None and previous_scalar != scalar_response:
            message = f"compiled {definition.name} scalar response changed"
            raise ValueError(message)
        if policy.schema in responses:
            message = f"compiled {definition.name} response schema kind changed"
            raise ValueError(message)
        scalar_responses[policy.schema] = scalar_response
        return policy, scalar_response.executable_digest
    if policy.schema in scalar_responses:
        message = f"compiled {definition.name} response schema kind changed"
        raise ValueError(message)
    schema = policy.schema
    rendered = render_operation_response_module(
        snapshot,
        operation,
        module_parts=("wire_programs", "_schemas", definition.name, schema),
        root_model_module_parts=("wire_runtime", "_models"),
        source_context=source_context,
    )
    if policy.content_addressed:
        schema = f"{schema}_{rendered.executable_digest.removeprefix('sha256:')}"
        policy = replace(policy, schema=schema)
    module_name = schema_module_name(
        definition.name, "response", schema, rendered.executable_digest
    )
    content = (
        rendered.source
        + "\n"
        + f"RESPONSE_TYPE = {rendered.adapter_annotation}\n"
        + f"EXECUTABLE_SCHEMA_DIGEST = {rendered.executable_digest!r}\n"
    )
    compile(content, f"<{module_name}>", "exec")
    structured_response = CompiledResponseModule(
        schema=schema,
        module_name=module_name,
        root_annotation=rendered.adapter_annotation,
        executable_digest=rendered.executable_digest,
        content=content,
        pool_modules=rendered.pool_modules,
    )
    previous_structured = responses.get(schema)
    if (
        previous_structured is not None
        and previous_structured[1] != structured_response
    ):
        message = (
            f"compiled {definition.name} response closure {policy.schema} differs "
            f"between DS {previous_structured[0]} and {version}"
        )
        raise ValueError(message)
    responses[schema] = (version, structured_response)
    return policy, structured_response.executable_digest


def _compile_program(
    definition: CompiledDomainDefinition,
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    primitive: CompiledPrimitive,
    request_epoch: CompiledRequestEpoch,
    policy: CompiledResponsePolicy,
    codec_record: JsonObject,
    resolver: SnapshotTypeResolver,
    request_schema_digest: str,
    response_schema_digest: str | None,
) -> CompiledProgram:
    _validate_response_transport(primitive, policy, operation)
    request = _request_record(
        definition,
        primitive.name,
        request_epoch,
        operation,
        snapshot,
    )
    response = effective_response_candidate(operation, snapshot, resolver=resolver)
    codec_digest = canonical_json_digest(
        {
            "schema_version": definition.schema_version,
            "request": effective_request_candidate(
                operation,
                snapshot,
                resolver=resolver,
            ),
            "request_codec": request,
            "request_schema_digest": request_schema_digest,
            "response": response,
            "response_codec": policy.codec,
            "response_schema_digest": response_schema_digest,
            "result_envelope": primitive.result_envelope,
            "omission": "pydantic-by-alias-exclude-none-exclude-unset",
        }
    )
    response_digest = canonical_json_digest(response)
    record: JsonObject = {
        "source_operation": operation.operation_id,
        "codec": policy.codec,
        "codec_digest": codec_digest,
        "request_schema_digest": request_schema_digest,
        "response_digest": response_digest,
        "response_schema_digest": response_schema_digest,
        "result_envelope": primitive.result_envelope,
    }
    return CompiledProgram(
        source_operation=operation.operation_id,
        codec=policy.codec,
        codec_digest=codec_digest,
        request_schema_digest=request_schema_digest,
        response_digest=response_digest,
        response_schema_digest=response_schema_digest,
        result_envelope=primitive.result_envelope,
        program_digest=canonical_json_digest(
            {
                "schema_version": 4,
                "codec_record": codec_record,
                **record,
            }
        ),
    )


def _request_record(
    definition: CompiledDomainDefinition,
    primitive_name: str,
    request_epoch: CompiledRequestEpoch,
    operation: OperationSpec,
    snapshot: ContractSnapshot,
) -> JsonObject:
    shape = _operation_request_shape(definition, primitive_name, operation, snapshot)
    expected = (
        request_epoch.method,
        request_epoch.path,
        request_epoch.channel,
        request_epoch.request_fields,
    )
    if (
        shape[:4] != expected
        or set(shape[4]) != set(request_epoch.path_fields)
        or _request_path_encoding(request_epoch) != _operation_path_encoding(operation)
        or request_epoch.file_fields != _operation_file_fields(operation)
    ):
        message = f"compiled {definition.name} {primitive_name} transport changed"
        raise ValueError(message)
    record: JsonObject = {
        "method": request_epoch.method,
        "path": request_epoch.path,
        "channel": request_epoch.channel,
        "path_encoding": _request_path_encoding(request_epoch),
        "fields": _request_field_records(request_epoch),
        "path_fields": list(request_epoch.path_fields),
    }
    if request_epoch.channel == "multipart":
        record["file_fields"] = list(request_epoch.file_fields)
    return record


def _request_path_encoding(request_epoch: CompiledRequestEpoch) -> str | None:
    return request_epoch.path_encoding if request_epoch.path_fields else None


def _operation_path_encoding(operation: OperationSpec) -> str | None:
    arguments = operation_path_arguments(operation)
    if not arguments:
        return None
    if any(
        argument.parameter is not None and argument.parameter.java_type == "String"
        for argument in arguments
    ):
        return _LEGACY_PATH_ENCODING
    return _PATH_ENCODING


def _executable_request_operation(operation: OperationSpec) -> OperationSpec:
    implicit = tuple(
        argument.name
        for argument in operation_path_arguments(operation)
        if argument.parameter is None
    )
    if implicit and (
        implicit != ("projectCode",)
        or any(
            parameter.name == "projectCode" or parameter.wire_name == "projectCode"
            for parameter in operation.parameters
        )
    ):
        message = (
            f"compiled {operation.operation_id} implicit path arguments are unsupported"
        )
        raise ValueError(message)
    return executable_path_operation(operation)


def _request_field_records(request_epoch: CompiledRequestEpoch) -> list[JsonValue]:
    body_binding = "request_body" in _CHANNEL_BINDINGS[request_epoch.channel]
    value_binding = "request_body" if body_binding else "request_param"
    return [
        {
            "name": wire_name,
            "binding": (
                "path_variable"
                if wire_name in request_epoch.path_fields
                else value_binding
            ),
        }
        for wire_name in request_epoch.request_fields
    ]


def _operation_file_fields(operation: OperationSpec) -> tuple[str, ...]:
    names = tuple(
        parameter.wire_name
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
        and parameter.java_type
        in {"MultipartFile", "org.springframework.web.multipart.MultipartFile"}
    )
    if any(not isinstance(name, str) or not name for name in names):
        message = f"compiled {operation.operation_id} multipart field names are invalid"
        raise ValueError(message)
    return cast("tuple[str, ...]", names)


def _select_request_epoch(
    definition: CompiledDomainDefinition,
    primitive: CompiledPrimitive,
    operation: OperationSpec,
    snapshot: ContractSnapshot,
) -> CompiledRequestEpoch:
    shape = _operation_request_shape(definition, primitive.name, operation, snapshot)
    required_fields = frozenset(
        parameter.wire_name
        for parameter in _executable_request_operation(operation).parameters
        if is_client_supplied_parameter(parameter) and is_required_parameter(parameter)
    )
    matches = tuple(
        request
        for request in primitive.requests
        if (
            request.method,
            request.path,
            request.channel,
            request.request_fields,
        )
        == shape[:4]
        and set(request.path_fields) == set(shape[4])
        and request.file_fields == _operation_file_fields(operation)
        and _request_path_encoding(request) == _operation_path_encoding(operation)
        and (
            request.required_fields is None
            or request.required_fields == required_fields
        )
        and (request.versions is None or snapshot.ds_version in request.versions)
    )
    if len(matches) != 1:
        message = (
            f"compiled {definition.name} {primitive.name} request shape has "
            f"{len(matches)} matching epochs"
        )
        raise ValueError(message)
    return matches[0]


def _operation_request_shape(
    definition: CompiledDomainDefinition,
    primitive_name: str,
    operation: OperationSpec,
    snapshot: ContractSnapshot,
) -> tuple[_Method, str, _Channel, tuple[str, ...], tuple[str, ...]]:
    parameters = tuple(
        parameter
        for parameter in _executable_request_operation(operation).parameters
        if is_client_supplied_parameter(parameter)
    )
    bindings = {parameter.binding for parameter in parameters}
    method = operation.http_method
    file_fields = _operation_file_fields(operation)
    channel: _Channel
    if file_fields:
        files = tuple(
            parameter for parameter in parameters if parameter.wire_name in file_fields
        )
        if (
            method != "POST"
            or bindings != {"request_param"}
            or operation.consumes not in ([], ["multipart/form-data"])
            or any(
                not is_required_parameter(parameter)
                or parameter.default_value is not None
                for parameter in files
            )
        ):
            message = (
                f"compiled {definition.name} {primitive_name} multipart transport "
                "is unsupported"
            )
            raise ValueError(message)
        channel = "multipart"
    elif (method == "GET" and bindings <= {"request_param"}) or (
        method == "DELETE" and bindings == {"request_param"}
    ):
        channel = "query"
    elif method == "GET" and bindings == {"path_variable", "request_param"}:
        channel = "path_query"
    elif method == "POST" and bindings == {"request_param"}:
        channel = "form"
    elif method in {"DELETE", "GET", "POST"} and bindings == {"path_variable"}:
        channel = "path"
    elif method in {"POST", "PUT"} and bindings == {"path_variable", "request_param"}:
        channel = "path_form"
    elif (method == "POST" and bindings == {"request_body"}) or (
        method == "PUT" and bindings == {"path_variable", "request_body"}
    ):
        bodies = tuple(
            parameter for parameter in parameters if parameter.binding == "request_body"
        )
        if len(bodies) != 1 or operation.consumes not in ([], ["application/json"]):
            message = (
                f"compiled {definition.name} {primitive_name} requires one JSON body"
            )
            raise ValueError(message)
        if not is_required_parameter(bodies[0]) or bodies[0].default_value is not None:
            message = (
                f"compiled {definition.name} {primitive_name} request body must be "
                "mandatory without a default"
            )
            raise ValueError(message)
        body_type = bodies[0].java_type
        if body_type == "String":
            channel = "json_text" if method == "POST" else "path_json_text"
        elif any(dto.import_path == body_type for dto in snapshot.dtos) or any(
            model.import_path == body_type for model in snapshot.models
        ):
            channel = "json" if method == "POST" else "path_json"
        else:
            message = (
                f"compiled {definition.name} {primitive_name} request body must be "
                "a resolved structured type or native String"
            )
            raise ValueError(message)
    else:
        message = (
            f"compiled {definition.name} {primitive_name} request transport "
            "is unsupported"
        )
        raise ValueError(message)
    wire_names = tuple(parameter.wire_name for parameter in parameters)
    if not all(isinstance(item, str) and item for item in wire_names) or len(
        wire_names
    ) != len(set(wire_names)):
        message = (
            f"compiled {definition.name} {primitive_name} request fields are invalid"
        )
        raise ValueError(message)
    fields = cast("tuple[str, ...]", wire_names)
    path_fields = cast(
        "tuple[str, ...]",
        tuple(
            parameter.wire_name
            for parameter in parameters
            if parameter.binding == "path_variable"
        ),
    )
    placeholders = _path_placeholders(operation.path)
    if "path_variable" in _CHANNEL_BINDINGS[channel]:
        if len(placeholders) != len(set(placeholders)) or set(placeholders) != set(
            path_fields
        ):
            message = (
                f"compiled {definition.name} {primitive_name} path variables changed"
            )
            raise ValueError(message)
        path_parameters = tuple(
            parameter
            for parameter in parameters
            if parameter.binding == "path_variable"
        )
        symbolic_enum_path = (
            method == "GET"
            and channel == "path"
            and len(path_parameters) == 1
            and any(
                enum.import_path == path_parameters[0].java_type
                and enum.json_value_field is None
                and enum.values
                for enum in snapshot.enums
            )
        )
        integer_path = all(
            parameter.java_type in {"Integer", "int", "Long", "long"}
            for parameter in path_parameters
        )
        legacy_string_path = (
            snapshot.ds_version == "1.3.9"
            and (method, channel)
            in {("GET", "path"), ("GET", "path_query"), ("POST", "path_form")}
            and len(path_parameters) == 1
            and path_parameters[0].wire_name == "projectName"
            and path_parameters[0].java_type == "String"
        )
        if len(path_parameters) not in _PATH_SEGMENT_COUNTS.get(
            (cast("_Method", method), channel), ()
        ) or not (integer_path or symbolic_enum_path or legacy_string_path):
            message = (
                f"compiled {definition.name} {primitive_name} path transport "
                "must use a reviewed integer-segment shape or a single native "
                "enum for GET path or the legacy project-name interpolation"
            )
            raise ValueError(message)
        if channel == "path" and path_fields != fields:
            message = f"compiled {definition.name} {primitive_name} path fields changed"
            raise ValueError(message)
        if channel != "path" and len(path_fields) == len(fields):
            message = (
                f"compiled {definition.name} {primitive_name} mixed fields changed"
            )
            raise ValueError(message)
    elif placeholders:
        message = f"compiled {definition.name} {primitive_name} path variables changed"
        raise ValueError(message)
    return cast("_Method", method), operation.path, channel, fields, path_fields


def _path_placeholders(path: str) -> tuple[str, ...]:
    if "?" in path or "#" in path:
        message = f"compiled request path contains query or fragment syntax: {path!r}"
        raise ValueError(message)
    placeholders = tuple(_PATH_PLACEHOLDER.findall(path))
    remainder = _PATH_PLACEHOLDER.sub("", path)
    if "{" in remainder or "}" in remainder:
        message = f"compiled request path has malformed placeholders: {path!r}"
        raise ValueError(message)
    return placeholders


def _codec_record(
    request_epoch: CompiledRequestEpoch,
    policy: CompiledResponsePolicy,
    *,
    response_projection: str,
    request_schema_digest: str,
    response_schema_digest: str | None,
) -> JsonObject:
    if response_projection not in {"direct", "status_data", "single_data"}:
        message = (
            f"compiled response projection is unsupported: {response_projection!r}"
        )
        raise ValueError(message)
    record: JsonObject = {
        "method": request_epoch.method,
        "path": request_epoch.path,
        "channel": request_epoch.channel,
        "path_encoding": _request_path_encoding(request_epoch),
        "fields": _request_field_records(request_epoch),
        "path_fields": list(request_epoch.path_fields),
        "params": request_epoch.request_schema,
        "response": policy.schema,
        "response_projection": response_projection,
        "capture": policy.capture,
        "request_schema_digest": request_schema_digest,
        "response_schema_digest": response_schema_digest,
    }
    if request_epoch.channel == "multipart":
        record["file_fields"] = list(request_epoch.file_fields)
    if policy.response_transport == "binary":
        record["response_transport"] = "binary"
    return record


def _domain_operations(
    snapshot: ContractSnapshot,
    definition: CompiledDomainDefinition,
    bindings: Mapping[str, ReviewedBinding],
    *,
    version: str,
    all_bindings: Mapping[str, ReviewedBinding] | None = None,
    operation_dependencies: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, OperationSpec]:
    source_ids = {
        source_id
        for binding in bindings.values()
        for source_id in binding.source_operations
    }
    source_operations = {
        operation.operation_id: operation
        for operation in snapshot.operations
        if operation.operation_id in source_ids
    }
    if set(source_operations) != source_ids:
        missing = sorted(source_ids - set(source_operations))
        message = (
            f"compiled {definition.name} snapshot is missing operations: {missing!r}"
        )
        raise ValueError(message)
    if operation_dependencies and any(
        operation in operation_dependencies for operation in bindings
    ):
        local_sources, local_types = _local_binding_roots(
            bindings,
            all_bindings=all_bindings if all_bindings is not None else bindings,
            operation_dependencies=operation_dependencies,
        )
        validated = slice_contract_for_bindings(
            snapshot,
            bindings,
            execution_operation_ids=local_sources,
            execution_type_refs=local_types,
        )
        source_ids = local_sources
        source_operations = {
            operation.operation_id: operation for operation in validated.operations
        }
    source_controllers = {
        source_id.rsplit(".", 1)[0] for source_id in source_ids if "." in source_id
    }
    classified_candidates = {
        operation.operation_id: operation
        for operation in snapshot.operations
        if operation.operation_id.rsplit(".", 1)[0] in source_controllers
        and definition.classify_operation(operation) is not None
    }
    unexpected = sorted(set(classified_candidates) - source_ids)
    if unexpected:
        message = (
            f"compiled {definition.name} snapshot has unowned classified "
            f"operations: {unexpected!r}"
        )
        raise ValueError(message)
    by_primitive: dict[str, list[OperationSpec]] = {
        primitive.name: [] for primitive in definition.primitives
    }
    for operation in source_operations.values():
        primitive = definition.classify_operation(operation)
        if primitive is None:
            message = (
                f"compiled {definition.name} owned source operation "
                f"{operation.operation_id!r} is unclassified"
            )
            raise ValueError(message)
        if primitive not in by_primitive:
            message = f"compiled {definition.name} classified an unknown primitive"
            raise ValueError(message)
        by_primitive[primitive].append(operation)
    invalid_counts = {
        primitive.name: len(by_primitive[primitive.name])
        for primitive in definition.primitives
        if len(by_primitive[primitive.name])
        != (0 if version in primitive.absent_versions else 1)
    }
    if invalid_counts:
        counts = {name: len(operations) for name, operations in by_primitive.items()}
        message = (
            f"compiled {definition.name} operation mapping is ambiguous: {counts!r}"
        )
        raise ValueError(message)
    return {
        name: operations[0] for name, operations in by_primitive.items() if operations
    }


def _domain_bindings(
    definition: CompiledDomainDefinition,
    version: str,
) -> dict[str, ReviewedBinding]:
    all_bindings = {
        **runtime_operation_bindings(version),
        **runtime_auxiliary_operation_bindings(version),
    }
    return {
        operation: binding
        for operation, binding in all_bindings.items()
        if operation in definition.semantic_operations
    }


def _strip_compiled_ownership(
    bundle: RuntimeBundle,
    definitions: tuple[CompiledDomainDefinition, ...],
    *,
    operation_dependencies: Mapping[str, CompiledDependencyDeclaration] | None = None,
) -> RuntimeBundle:
    version = bundle.spec.version
    all_bindings = {
        **runtime_operation_bindings(version),
        **runtime_auxiliary_operation_bindings(version),
    }
    dependencies = _validate_operation_dependencies(
        version, all_bindings, operation_dependencies or {}
    )
    compiled_semantics = set().union(
        *(definition.semantic_operations for definition in definitions)
    )
    legacy_bindings = {
        operation: binding
        for operation, binding in all_bindings.items()
        if operation not in compiled_semantics
    }
    compiled_by_domain = {
        definition.name: _domain_bindings(definition, version)
        for definition in definitions
    }
    _require_exclusive_source_ownership(
        version,
        compiled_by_domain=compiled_by_domain,
        legacy=legacy_bindings,
        all_bindings=all_bindings,
        operation_dependencies=dependencies,
    )
    local_sources, local_types = _local_binding_roots(
        legacy_bindings,
        all_bindings=all_bindings,
        operation_dependencies=dependencies,
    )
    snapshot = slice_contract_for_bindings(
        bundle.snapshot,
        legacy_bindings,
        additional_type_refs={
            _enum_type_ref(item.import_path) for item in bundle.snapshot.enums
        },
        execution_operation_ids=local_sources,
        execution_type_refs=local_types,
    )
    semantic_operations = tuple(sorted(legacy_bindings))
    metadata = replace(
        bundle.metadata,
        semantic_operations=semantic_operations,
        rendered_contract_digest=contract_snapshot_digest(snapshot),
        operation_count=snapshot.operation_count,
        enum_count=snapshot.enum_count,
        dto_count=snapshot.dto_count,
        model_count=snapshot.model_count,
    )
    return replace(bundle, snapshot=snapshot, metadata=metadata)


def _validate_operation_dependencies(
    version: str,
    bindings: Mapping[str, ReviewedBinding],
    dependencies: Mapping[str, CompiledDependencyDeclaration],
) -> dict[str, tuple[str, ...]]:
    """Validate declarations, then project active direct calls for this release."""
    active: dict[str, tuple[str, ...]] = {}
    for consumer, dependency in dependencies.items():
        legacy_shorthand = False
        clauses: tuple[CompiledOperationDependency, ...]
        if isinstance(dependency, CompiledOperationDependency):
            clauses = (dependency,)
        elif isinstance(dependency, tuple) and all(
            isinstance(item, str) for item in dependency
        ):
            legacy_shorthand = True
            clauses = (
                CompiledOperationDependency(
                    providers=cast("tuple[str, ...]", dependency),
                    versions=frozenset(REVIEWED_DS_VERSIONS),
                ),
            )
        elif isinstance(dependency, tuple) and all(
            isinstance(item, CompiledOperationDependency) for item in dependency
        ):
            clauses = cast("tuple[CompiledOperationDependency, ...]", dependency)
        else:
            message = (
                f"DS {version} operation dependency consumer is invalid: {consumer!r}"
            )
            raise ValueError(message)

        # A provider owns one reviewed version set, even across disjoint clauses.
        declared_providers: set[str] = set()
        for clause in clauses:
            if (
                not isinstance(clause.versions, frozenset)
                or not clause.versions
                or not clause.versions <= set(REVIEWED_DS_VERSIONS)
            ):
                message = (
                    f"DS {version} operation dependency versions are invalid: "
                    f"{consumer!r}"
                )
                raise ValueError(message)
            providers = clause.providers
            if (
                consumer not in _REVIEWED_OPERATION_NAMES
                or not isinstance(providers, tuple)
                or not providers
                or not all(isinstance(provider, str) for provider in providers)
                or len(set(providers)) != len(providers)
                or declared_providers.intersection(providers)
            ):
                message = (
                    f"DS {version} operation dependency consumer is invalid: "
                    f"{consumer!r}"
                )
                raise ValueError(message)
            declared_providers.update(providers)
            for provider in providers:
                if provider not in _REVIEWED_OPERATION_NAMES:
                    message = (
                        f"DS {version} operation dependency provider is missing: "
                        f"{provider!r}"
                    )
                    raise ValueError(message)
                if provider in dependencies:
                    message = (
                        f"DS {version} operation dependency requires a direct "
                        f"provider; chains and cycles are unsupported: {provider!r}"
                    )
                    raise ValueError(message)
                if consumer.split(".", 1)[0] == provider.split(".", 1)[0]:
                    message = (
                        f"DS {version} operation dependency must cross domains: "
                        f"{consumer!r} -> {provider!r}"
                    )
                    raise ValueError(message)
            if version not in clause.versions:
                continue
            if consumer not in bindings:
                if legacy_shorthand:
                    continue
                message = (
                    f"DS {version} operation dependency selects an absent consumer: "
                    f"{consumer!r}"
                )
                raise ValueError(message)
            for provider in providers:
                if provider not in bindings:
                    message = (
                        f"DS {version} operation dependency provider is missing: "
                        f"{provider!r}"
                    )
                    raise ValueError(message)
            active[consumer] = (*active.get(consumer, ()), *providers)

    for consumer, providers in active.items():
        delegated: set[str] = set()
        for provider in providers:
            sources = set(bindings[provider].source_operations)
            if not sources or not sources <= set(bindings[consumer].source_operations):
                message = (
                    f"DS {version} operation dependency is not covered by "
                    f"{consumer!r}: {provider!r}"
                )
                raise ValueError(message)
            if delegated & sources:
                message = (
                    f"DS {version} operation dependency ownership is ambiguous: "
                    f"{consumer!r}"
                )
                raise ValueError(message)
            delegated.update(sources)
        if delegated == set(bindings[consumer].source_operations):
            message = (
                f"DS {version} operation dependency leaves no local source: "
                f"{consumer!r}"
            )
            raise ValueError(message)
    return active


def _local_binding_roots(
    bindings: Mapping[str, ReviewedBinding],
    *,
    all_bindings: Mapping[str, ReviewedBinding],
    operation_dependencies: Mapping[str, tuple[str, ...]],
) -> tuple[set[str], set[WireTypeRef]]:
    """Project local execution roots per consumer without changing audit bindings."""
    source_roots: set[str] = set()
    type_roots: set[WireTypeRef] = set()
    for semantic_operation, binding in bindings.items():
        local_sources = set(binding.source_operations)
        local_types = set(binding.type_closure)
        for provider in operation_dependencies.get(semantic_operation, ()):
            dependency = all_bindings[provider]
            local_sources.difference_update(dependency.source_operations)
            local_types.difference_update(dependency.type_closure)
        source_roots.update(local_sources)
        type_roots.update(local_types)
    return source_roots, type_roots


def _require_exclusive_source_ownership(
    version: str,
    *,
    compiled_by_domain: Mapping[str, Mapping[str, ReviewedBinding]],
    legacy: Mapping[str, ReviewedBinding],
    all_bindings: Mapping[str, ReviewedBinding],
    operation_dependencies: Mapping[str, tuple[str, ...]] | None = None,
) -> None:
    compiled_sources_by_domain = {
        name: _local_binding_roots(
            bindings,
            all_bindings=all_bindings,
            operation_dependencies=operation_dependencies or {},
        )[0]
        for name, bindings in compiled_by_domain.items()
    }
    source_owners = Counter(
        source for sources in compiled_sources_by_domain.values() for source in sources
    )
    overlap = sorted(source for source, count in source_owners.items() if count > 1)
    legacy_sources, _legacy_types = _local_binding_roots(
        legacy,
        all_bindings=all_bindings,
        operation_dependencies=operation_dependencies or {},
    )
    compiled_sources = set(source_owners)
    overlap.extend(sorted(compiled_sources & legacy_sources))
    if overlap:
        message = f"DS {version} source operations have multiple owners: {overlap!r}"
        raise ValueError(message)
    expected = {
        source
        for binding in all_bindings.values()
        for source in binding.source_operations
    }
    if compiled_sources | legacy_sources != expected:
        message = f"DS {version} source operation ownership is incomplete"
        raise ValueError(message)


def _compile_domain_modules(
    plan: CompiledDomainPlan,
) -> tuple[CompiledWireModule, ...]:
    # A short name is a display choice, never proof that two schemas are equal.
    identities: dict[str, str] = {}
    for role, schemas in (("request", plan.requests), ("response", plan.responses)):
        for item in schemas:
            symbol = f"{role}_{schema_symbol_name(item.schema, item.executable_digest)}"
            previous = identities.setdefault(symbol, item.executable_digest)
            if previous != item.executable_digest:
                message = f"compiled schema short-name collision: {symbol}"
                raise ValueError(message)
    responses_by_digest: dict[str, CompiledResponseModule] = {}
    response_contents: dict[str, str] = {}
    response_names: dict[str, str] = {}
    for item in sorted(plan.responses, key=lambda response: response.module_name):
        previous_content = response_contents.setdefault(item.module_name, item.content)
        if previous_content != item.content:
            message = (
                f"compiled response module {item.module_name} has conflicting content"
            )
            raise ValueError(message)
        digest = hashlib.sha256(item.content.encode("utf-8")).hexdigest()
        canonical = responses_by_digest.setdefault(digest, item)
        if canonical.content != item.content:
            message = "compiled response module content digest collision"
            raise ValueError(message)
        response_names[item.module_name] = canonical.module_name
    rendered_plan = replace(
        plan,
        responses=tuple(
            replace(item, module_name=response_names[item.module_name])
            for item in plan.responses
        ),
    )
    schema_closures: tuple[CompiledRequest | CompiledResponseModule, ...] = (
        *plan.requests,
        *plan.responses,
    )
    return (
        CompiledWireModule(
            name=plan.definition.name,
            content=_render_domain_module(rendered_plan),
            kind="domain",
        ),
        *(
            CompiledWireModule(
                name=item.module_name,
                content=item.content,
                kind="support",
            )
            for item in plan.requests
            if item.module_name is not None and item.content is not None
        ),
        *(
            CompiledWireModule(
                name=item.module_name,
                content=item.content,
                kind="support",
            )
            for item in responses_by_digest.values()
        ),
        *unique_schema_modules(
            module for item in schema_closures for module in item.pool_modules
        ),
    )


def _render_domain_module(plan: CompiledDomainPlan) -> str:
    definition = plan.definition
    profiles = {profile.version: profile.record() for profile in plan.profiles}
    request_symbols = {
        item.schema: schema_symbol_name(item.schema, item.executable_digest)
        for item in plan.requests
    }
    response_symbols = {
        item.schema: schema_symbol_name(item.schema, item.executable_digest)
        for item in plan.responses
    }
    response_imports = tuple(
        "\n".join(
            (
                f"from .{item.module_name} import (",
                "    EXECUTABLE_SCHEMA_DIGEST as "
                f"_{response_symbols[item.schema]}_schema_digest,",
                f"    RESPONSE_TYPE as _{response_symbols[item.schema]}_response,",
                ")",
            )
        )
        for item in plan.responses
    )
    request_imports = tuple(
        "\n".join(
            (
                f"from .{item.module_name} import (",
                (
                    "    EXECUTABLE_SCHEMA_DIGEST as "
                    f"_{request_symbols[item.schema]}_request_schema_digest,"
                ),
                f"    {item.class_name} as "
                f"_{request_symbols[item.schema]}_request_model,",
                ")",
            )
        )
        for item in plan.requests
        if item.module_name is not None
    )
    request_entries = tuple(
        "\n".join(
            (
                f'    "{item.schema}": CompiledRequestSchema(',
                (
                    f"        _{request_symbols[item.schema]}_request_model,"
                    if item.module_name is not None or item.content_addressed
                    else f"        {item.class_name},"
                ),
                (
                    f"        _{request_symbols[item.schema]}_request_schema_digest,"
                    if item.module_name is not None
                    else f"        {item.executable_digest!r},"
                ),
                "    ),",
            )
        )
        for item in plan.requests
    )
    scalar_entries = tuple(
        "\n".join(
            (
                f'    "{item.schema}": CompiledResponseSchema(',
                f"        {item.annotation},",
                f"        {item.executable_digest!r},",
                "    ),",
            )
        )
        for item in plan.scalar_responses
    )
    structured_entries = tuple(
        "\n".join(
            (
                f'    "{item.schema}": CompiledResponseSchema(',
                f"        _{response_symbols[item.schema]}_response,",
                f"        _{response_symbols[item.schema]}_schema_digest,",
                "    ),",
            )
        )
        for item in plan.responses
    )
    return "\n".join(
        (
            "from __future__ import annotations",
            "",
            "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
            "from copy import deepcopy",
            "from pydantic import Field",
            "from dsctl.support.json_types import JsonObject",
            "",
            "from dsctl.generated.wire_runtime.api.operations._base import (",
            "    BaseParamsModel,",
            ")",
            "from dsctl.generated.wire_runtime._compiled_schema import (",
            "    CompiledRequestSchema,",
            "    CompiledResponseSchema,",
            ")",
            "",
            *response_imports,
            *request_imports,
            "",
            *(
                (
                    (
                        f"class _{request_symbols[item.schema]}_request_types:\n"
                        + indent(item.source, "    ")
                        + "\n"
                        + f"_{request_symbols[item.schema]}_request_model = "
                        + f"_{request_symbols[item.schema]}_request_types."
                        + f"{item.class_name}\n"
                    )
                    if item.content_addressed
                    else item.source
                )
                + "\n"
                for item in plan.requests
                if item.module_name is None
            ),
            "",
            "REQUEST_SCHEMAS = {",
            *request_entries,
            "}",
            "RESPONSE_SCHEMAS = {",
            *scalar_entries,
            *structured_entries,
            "}",
            render_codec_records(plan.codecs),
            f"{definition.schema_constant} = {definition.schema_version}",
            "TARGET_DS_VERSIONS = (",
            *(f"    {json.dumps(profile.version)}," for profile in plan.profiles),
            ")",
            "PROFILES = " + pformat(profiles, sort_dicts=False, width=88),
            "",
            "__all__ = [",
            '    "CODECS",',
            f'    "{definition.schema_constant}",',
            '    "PROFILES",',
            '    "REQUEST_SCHEMAS",',
            '    "RESPONSE_SCHEMAS",',
            '    "TARGET_DS_VERSIONS",',
            "]",
            "",
        )
    )


def _validate_definitions(
    definitions: tuple[CompiledDomainDefinition, ...],
) -> None:
    if not definitions or len({item.name for item in definitions}) != len(definitions):
        message = "compiled domain definitions must be nonempty and uniquely named"
        raise ValueError(message)
    semantic_operations = [
        operation
        for definition in definitions
        for operation in definition.semantic_operations
    ]
    if len(semantic_operations) != len(set(semantic_operations)):
        message = "compiled domain semantic operations must have one owner"
        raise ValueError(message)
    for definition in definitions:
        primitive_names = tuple(item.name for item in definition.primitives)
        if (
            _DOMAIN_NAME.fullmatch(definition.name) is None
            or _SCHEMA_CONSTANT.fullmatch(definition.schema_constant) is None
            or definition.schema_version < 1
            or not definition.semantic_operations
            or not definition.primitives
            or len(primitive_names) != len(set(primitive_names))
            or not definition.absent_versions <= set(REVIEWED_DS_VERSIONS)
        ):
            message = f"compiled domain definition {definition.name!r} is invalid"
            raise ValueError(message)
        if any(
            operation not in definition.semantic_operations
            or not isinstance(versions, frozenset)
            or not versions
            or not versions <= set(REVIEWED_DS_VERSIONS)
            or bool(versions & definition.absent_versions)
            for operation, versions in definition.semantic_absent_versions.items()
        ):
            message = (
                f"compiled {definition.name} semantic absence declaration is invalid"
            )
            raise ValueError(message)
        for primitive in definition.primitives:
            if (
                primitive.result_envelope not in {"optional", "required"}
                or primitive.response_transport not in {"json", "binary"}
                or (
                    primitive.response_transport == "binary"
                    and (
                        primitive.result_envelope != "optional"
                        or any(
                            request.method != "GET"
                            or request.channel not in {"query", "path", "path_query"}
                            for request in primitive.requests
                        )
                    )
                )
                or _DOMAIN_NAME.fullmatch(primitive.name) is None
                or not primitive.requests
                or len(primitive.requests) != len(set(primitive.requests))
                or not primitive.absent_versions <= set(REVIEWED_DS_VERSIONS)
                or bool(primitive.absent_versions & definition.absent_versions)
            ):
                message = (
                    f"compiled {definition.name} primitive "
                    f"{primitive.name!r} is invalid"
                )
                raise ValueError(message)
            for request in primitive.requests:
                _validate_request_epoch(definition, primitive.name, request)
                if request.versions is not None and request.versions & (
                    definition.absent_versions | primitive.absent_versions
                ):
                    message = (
                        f"compiled {definition.name} {primitive.name} request "
                        "epoch selects an absent version"
                    )
                    raise ValueError(message)


def _validate_request_epoch(
    definition: CompiledDomainDefinition,
    primitive_name: str,
    request: CompiledRequestEpoch,
) -> None:
    placeholders = _path_placeholders(request.path)
    shape_is_supported = (request.method, request.channel) in _PATH_SEGMENT_COUNTS or (
        request.method,
        request.channel,
    ) in {
        ("DELETE", "query"),
        ("GET", "query"),
        ("POST", "form"),
        ("POST", "json"),
        ("POST", "json_text"),
        ("POST", "multipart"),
    }
    path_is_consistent = not placeholders and not request.path_fields
    if (request.method, request.channel) in _PATH_SEGMENT_COUNTS:
        path_is_consistent = (
            bool(placeholders)
            and len(request.path_fields)
            in _PATH_SEGMENT_COUNTS.get((request.method, request.channel), ())
            and len(placeholders) == len(set(placeholders))
            and len(request.path_fields) == len(set(request.path_fields))
            and set(placeholders) == set(request.path_fields)
            and set(request.path_fields) <= set(request.request_fields)
            and (
                set(request.path_fields) == set(request.request_fields)
                if request.channel == "path"
                else len(request.path_fields) < len(request.request_fields)
            )
        )
    if (
        not shape_is_supported
        or not path_is_consistent
        or not isinstance(request.content_addressed, bool)
        or not isinstance(request.file_fields, tuple)
        or len(request.file_fields) != len(set(request.file_fields))
        or not set(request.file_fields) <= set(request.request_fields)
        or bool(request.file_fields) != (request.channel == "multipart")
        or request.path_encoding not in {_PATH_ENCODING, _LEGACY_PATH_ENCODING}
        or (
            request.path_encoding == _LEGACY_PATH_ENCODING
            and (
                request.path_fields != ("projectName",)
                or request.versions != frozenset({"1.3.9"})
                or (request.method, request.channel)
                not in {("GET", "path"), ("GET", "path_query"), ("POST", "path_form")}
            )
        )
        or (
            request.channel in {"json", "json_text", "path_json", "path_json_text"}
            and len(request.request_fields) - len(request.path_fields) != 1
        )
        or not request.path
        or request.path.startswith("/")
        or (
            not request.request_fields
            and (request.method, request.channel) != ("GET", "query")
        )
        or len(request.request_fields) != len(set(request.request_fields))
        or (
            request.required_fields is not None
            and not request.required_fields <= set(request.request_fields)
        )
        or (
            request.versions is not None
            and (
                not request.versions
                or not request.versions <= set(REVIEWED_DS_VERSIONS)
            )
        )
        or _DOMAIN_NAME.fullmatch(request.request_schema) is None
        or not request.request_model.isidentifier()
    ):
        message = (
            f"compiled {definition.name} {primitive_name} request epoch is invalid"
        )
        raise ValueError(message)


def _validate_response_policy(
    definition: CompiledDomainDefinition,
    policy: CompiledResponsePolicy,
) -> None:
    if (
        _DOMAIN_NAME.fullmatch(policy.codec) is None
        or policy.response_transport not in {"json", "binary"}
        or (
            policy.response_transport == "binary"
            and (policy.schema is not None or policy.capture is not None)
        )
        or not isinstance(policy.content_addressed, bool)
        or (policy.schema is not None and _DOMAIN_NAME.fullmatch(policy.schema) is None)
        or (policy.schema is None and policy.scalar_annotation is not None)
        or policy.scalar_annotation not in {None, "bool", "float", "int", "str"}
        or (
            policy.content_addressed
            and (policy.schema is None or policy.scalar_annotation is not None)
        )
        or not is_json_value(policy.capture)
    ):
        message = f"compiled {definition.name} response policy is invalid"
        raise ValueError(message)


def _validate_response_transport(
    primitive: CompiledPrimitive,
    policy: CompiledResponsePolicy,
    operation: OperationSpec,
) -> None:
    if policy.response_transport != primitive.response_transport or (
        policy.response_transport == "binary"
        and (
            operation.http_method != "GET" or operation.response_projection != "direct"
        )
    ):
        message = f"compiled {primitive.name} response transport changed"
        raise ValueError(message)


def _profile(
    metadata: RuntimeBundleMetadata,
    *,
    status: _Status,
    recipe_id: str | None = None,
    programs: tuple[tuple[str, CompiledProgram], ...] = (),
) -> CompiledProfile:
    return CompiledProfile(
        version=metadata.version,
        status=status,
        source_tag=metadata.source_tag,
        source_commit=metadata.source_commit,
        source_tree=metadata.source_tree,
        source_contract_digest=metadata.source_contract_digest,
        recipe_id=recipe_id,
        programs=programs,
    )


def _enum_type_ref(import_path: str) -> WireTypeRef:
    return WireTypeRef("enums", import_path)


def _version_key(version: str) -> tuple[int, int, int]:
    return cast("tuple[int, int, int]", tuple(int(part) for part in version.split(".")))


__all__ = [
    "CompiledDependencyDeclaration",
    "CompiledDomainDefinition",
    "CompiledDomainPlan",
    "CompiledDomainSet",
    "CompiledOperationDependency",
    "CompiledPrimitive",
    "CompiledRequestEpoch",
    "CompiledResponsePolicy",
    "compile_domains",
    "model_field_facts",
    "require_model",
]
