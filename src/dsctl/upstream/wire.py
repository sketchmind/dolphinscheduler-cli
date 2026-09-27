from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from functools import cache
from importlib import resources
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Generic,
    NoReturn,
    Protocol,
    TypeVar,
    cast,
)
from urllib.parse import quote

from pydantic import BaseModel, TypeAdapter, ValidationError

from dsctl.errors import ApiResultError, ApiTransportError, UnsupportedFeatureError
from dsctl.generated.wire_runtime._compiled_schema import (
    CompiledRequestSchema,
    CompiledResponseSchema,
)
from dsctl.generated.wire_runtime.api.operations._base import (
    RequestMapping as _GeneratedRequestMapping,
)
from dsctl.generated.wire_runtime.api.operations._base import (
    _require_json_object,
    _require_json_value,
    _require_request_mapping,
)
from dsctl.support.json_types import JsonObject, JsonValue, is_json_value

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable
    from types import ModuleType

    from dsctl.client import (
        BinaryResponse,
        DolphinSchedulerClient,
        HttpFormData,
        HttpQueryParams,
        MultipartFiles,
    )
    from dsctl.upstream._generated_types import OpaqueGeneratedValue

PayloadT = TypeVar("PayloadT")


class WireContractError(RuntimeError):
    """Raised when a generated wire program violates its execution contract."""


class WireResponseDecodeError(ApiTransportError):
    """A completed logical response failed its generated payload contract."""


class WireExecutionMode(StrEnum):
    """Reviewed side-effect and transport-retry policy for one exact exchange."""

    READ_RETRY_SAFE = "read_retry_safe"
    READ_ONCE = "read_once"
    MUTATION_ONCE = "mutation_once"
    IDEMPOTENT_MUTATION_RETRY_SAFE = "idempotent_mutation_retry_safe"

    @property
    def mutates(self) -> bool:
        """Return whether executing this wire program may change server state."""
        return self in {
            WireExecutionMode.MUTATION_ONCE,
            WireExecutionMode.IDEMPOTENT_MUTATION_RETRY_SAFE,
        }

    @property
    def retryable(self) -> bool:
        """Return whether reviewed evidence permits transport retry."""
        return self in {
            WireExecutionMode.READ_RETRY_SAFE,
            WireExecutionMode.IDEMPOTENT_MUTATION_RETRY_SAFE,
        }


class WireResultEnvelope(StrEnum):
    """Whether a completed response must use the DolphinScheduler envelope."""

    OPTIONAL = "optional"
    REQUIRED = "required"


@dataclass(frozen=True)
class WireRequest:
    """Authentication-free request emitted by one generated operation."""

    method: str
    path: str
    query: HttpQueryParams | None
    form: HttpFormData | None
    json: JsonValue | None
    content: str | None


class PreparedWireCallToken(Protocol):
    """Opaque prepared-call identity exposed above a concrete wire program."""

    @property
    def ds_version(self) -> str:
        """Return the exact profile that prepared this call."""
        ...

    @property
    def program_fingerprint(self) -> str:
        """Return the generated program identity bound to this call."""
        ...

    @property
    def request(self) -> WireRequest:
        """Return the prepared authentication-free request."""
        ...


@dataclass(frozen=True)
class WireExecution(Generic[PayloadT]):
    """Typed and lossless result of one generated wire exchange."""

    payload: PayloadT
    raw_payload: JsonValue
    request: WireRequest


@dataclass(frozen=True)
class PreparedCompiledWireCall:
    """Detached compiled request; public previews cannot mutate execution state."""

    ds_version: str
    program_fingerprint: str
    _request: WireRequest = field(repr=False)
    _request_fingerprint: str = field(repr=False)
    _codec: CompiledWireCodec = field(repr=False)

    @property
    def request(self) -> WireRequest:
        """Return a copy of the exact request that will be sent."""
        return deepcopy(self._request)


@dataclass(frozen=True)
class CompiledWireProgram:
    """One compiled exchange with independent request and response codecs."""

    ds_version: str
    source_operation: str
    source_contract_digest: str
    execution_mode: WireExecutionMode
    request_codec_fingerprint: str
    result_envelope: WireResultEnvelope
    codec: CompiledWireCodec = field(kw_only=True, repr=False)

    @property
    def fingerprint(self) -> str:
        """Return a stable identity for the exact source and response contract."""
        identity = "\0".join(
            (
                self.ds_version,
                self.source_operation,
                self.source_contract_digest,
                self.execution_mode.value,
                self.request_codec_fingerprint,
                self.result_envelope.value,
                self.codec.response_fingerprint,
            )
        )
        return f"sha256:{hashlib.sha256(identity.encode()).hexdigest()}"

    def __post_init__(self) -> None:
        """Keep the legacy URL policy bound to its reviewed exact release."""
        if (
            self.codec.path_encoding == _LEGACY_PATH_ENCODING
            and self.ds_version != "1.3.9"
        ):
            message = "Legacy URL interpolation is restricted to exact DS 1.3.9"
            raise WireContractError(message)

    def prepare(self, args: JsonObject) -> PreparedCompiledWireCall:
        """Validate once and detach the generated request from mutable input."""
        request = self.codec.encode(args)
        fingerprint = self.fingerprint
        return PreparedCompiledWireCall(
            ds_version=self.ds_version,
            program_fingerprint=fingerprint,
            _request=request,
            _request_fingerprint=_compiled_request_fingerprint(fingerprint, request),
            _codec=self.codec,
        )


@dataclass(frozen=True)
class CompiledWireCodec:
    """Content-bound request encoder and response validator for one exchange."""

    method: str
    path: str
    channel: str
    path_encoding: str | None
    field_bindings: tuple[tuple[str, str], ...]
    path_fields: tuple[str, ...]
    params_model: type[BaseModel]
    response_adapter: TypeAdapter[OpaqueGeneratedValue] | None
    capture_payload: JsonValue
    request_fingerprint: str
    request_schema_fingerprint: str
    response_fingerprint: str
    response_schema_fingerprint: str | None
    result_envelope: WireResultEnvelope
    response_projection: str = "direct"
    file_fields: tuple[str, ...] = ()
    response_transport: str = "json"

    def encode(self, args: JsonObject) -> WireRequest:
        """Project generated request fields without invoking or decoding a call."""
        params = self.params_model.model_validate(dict(args))
        dumped = params.model_dump(
            by_alias=True,
            exclude_none=True,
            exclude_unset=True,
            mode="json",
        )
        if self.channel in {"json", "path_json", "json_text", "path_json_text"}:
            return self._encode_body(
                _require_json_object(dumped, label="request payload")
            )
        mapping = _require_request_mapping(
            dumped,
            label="request payload",
        )
        mapping = {name: value for name, value in mapping.items() if value is not None}
        path = self.path
        if self.path_fields:
            # JSON serialization erases StrEnum identity; retain validated native
            # path values so plain strings cannot widen the closed transport ABI.
            path_values: _GeneratedRequestMapping = {
                name: mapping[name] for name in self.path_fields
            }
            for name, model_field in self.params_model.model_fields.items():
                wire_name = model_field.serialization_alias or model_field.alias or name
                if wire_name in self.path_fields:
                    native_value = getattr(params, name)
                    if isinstance(native_value, StrEnum):
                        path_values[wire_name] = native_value
            path = _render_compiled_path(
                path,
                path_values,
                legacy_text=self.path_encoding == _LEGACY_PATH_ENCODING,
                allow_enum=(
                    self.method == "GET"
                    and self.channel == "path"
                    and len(self.path_fields) == 1
                ),
            )
        payload = {
            name: value
            for name, value in mapping.items()
            if name not in self.path_fields
        }
        return WireRequest(
            method=self.method,
            path=f"/{path}",
            query=(
                deepcopy(payload)
                if self.channel in {"query", "path_query"} and self.field_bindings
                else None
            ),
            form=(
                deepcopy(payload)
                if self.channel in {"form", "path_form", "multipart"}
                else None
            ),
            json=None,
            content=None,
        )

    def _encode_body(self, mapping: Mapping[str, JsonValue]) -> WireRequest:
        path = self.path
        if self.path_fields:
            path = _render_compiled_path(
                path,
                _require_request_mapping(
                    {name: mapping[name] for name in self.path_fields},
                    label="request path",
                ),
                allow_enum=False,
            )
        body_name = next(
            name for name, binding in self.field_bindings if binding == "request_body"
        )
        body = mapping.get(body_name)
        if self.channel in {"json", "path_json"}:
            json_body = _require_json_object(body, label="request body")
            content = None
        else:
            if not isinstance(body, str):
                message = "Compiled wire JSON text body must be a string"
                raise WireContractError(message)
            json_body = None
            content = body
        return WireRequest(
            method=self.method,
            path=f"/{path}",
            query=None,
            form=None,
            json=deepcopy(json_body),
            content=content,
        )

    def decode(
        self,
        payload: JsonValue,
    ) -> OpaqueGeneratedValue:
        """Validate the received payload with the generated exact response schema."""
        if self.response_transport != "json":
            message = "Binary wire responses cannot be decoded as JSON"
            raise WireContractError(message)
        if self.response_projection == "status_data" and isinstance(payload, dict):
            status = payload.get("status")
            if status is not None:
                status_payload = _require_json_object(payload, label="status payload")
                data = status_payload.get("data")
                if data is None and "data" not in status_payload:
                    data = status_payload.get("dataList")
                if status != "SUCCESS":
                    raise ApiResultError(
                        result_code=None,
                        result_message=str(status_payload.get("msg") or "DS API error"),
                        data=data,
                    )
                payload = _require_json_value(data, label="status payload data")
        if self.response_projection == "single_data" and isinstance(payload, dict):
            if "data" not in payload:
                _raise_payload_error("single-field payload must contain data")
            payload = _require_json_value(
                payload["data"], label="single-field payload data"
            )
        if self.response_adapter is None:
            return None
        try:
            return self.response_adapter.validate_python(payload)
        except ValidationError as exc:
            _raise_payload_error(
                "Response payload did not match generated API contract",
                cause=exc,
            )

    def __post_init__(self) -> None:
        """Reject transport shapes and fingerprints outside the runtime ABI."""
        self._validate_transfer_shape()
        self._validate_transport_shape()

    def _validate_transfer_shape(self) -> None:
        if self.response_transport not in {"json", "binary"}:
            message = "Compiled wire codec response transport is unsupported"
            raise WireContractError(message)
        if self.response_transport == "binary" and (
            self.method != "GET"
            or self.response_adapter is not None
            or self.response_projection != "direct"
            or self.result_envelope is not WireResultEnvelope.OPTIONAL
        ):
            message = "Compiled binary transport must be a raw GET response"
            raise WireContractError(message)
        if self.channel == "multipart":
            if (
                self.file_fields != ("file",)
                or self.response_transport != "json"
                or self.result_envelope is not WireResultEnvelope.REQUIRED
                or self.response_adapter is not None
                or self.response_projection != "direct"
            ):
                message = (
                    "Compiled multipart transport requires "
                    "one file and a result envelope"
                )
                raise WireContractError(message)
        elif self.file_fields:
            message = "Compiled file fields require multipart transport"
            raise WireContractError(message)

    def _validate_transport_shape(self) -> None:
        if self.response_projection not in {"direct", "status_data", "single_data"}:
            message = "Compiled wire codec response projection is unsupported"
            raise WireContractError(message)
        if (self.method, self.channel) not in {
            ("DELETE", "path"),
            ("DELETE", "query"),
            ("GET", "path"),
            ("GET", "path_query"),
            ("GET", "query"),
            ("POST", "form"),
            ("POST", "multipart"),
            ("POST", "path"),
            ("POST", "path_form"),
            ("POST", "json"),
            ("POST", "json_text"),
            ("PUT", "path_form"),
            ("PUT", "path_json"),
            ("PUT", "path_json_text"),
        }:
            message = "Compiled wire codec has an unsupported transport shape"
            raise WireContractError(message)
        if not self.path or self.path.startswith("/"):
            message = "Compiled wire codec path must be relative"
            raise WireContractError(message)
        supported_encodings = (
            {_PATH_ENCODING, _LEGACY_PATH_ENCODING} if self.path_fields else {None}
        )
        if self.path_encoding not in supported_encodings:
            message = "Compiled wire codec path encoding is unsupported"
            raise WireContractError(message)
        _validate_compiled_path_binding(self)
        if self.path_encoding == _LEGACY_PATH_ENCODING:
            _validate_legacy_path_binding(self)
        for label, value in (
            ("request", self.request_fingerprint),
            ("request schema", self.request_schema_fingerprint),
            ("response", self.response_fingerprint),
        ):
            if _SHA256.fullmatch(value) is None:
                message = f"Compiled wire codec {label} fingerprint is invalid"
                raise WireContractError(message)
        if (self.response_adapter is None) != (
            self.response_schema_fingerprint is None
        ):
            message = "Compiled wire codec response schema identity is inconsistent"
            raise WireContractError(message)
        if (
            self.response_schema_fingerprint is not None
            and _SHA256.fullmatch(self.response_schema_fingerprint) is None
        ):
            message = "Compiled wire codec response schema fingerprint is invalid"
            raise WireContractError(message)


@dataclass(frozen=True)
class CompiledWireProfile:
    """One validated exact-version profile from a compiled domain artifact."""

    ds_version: str
    status: str
    recipe_id: str | None
    source_commit: str
    source_tree: str
    source_contract_digest: str
    codec_names: Mapping[str, str]
    programs: Mapping[str, CompiledWireProgram]

    def program(
        self,
        primitive: str,
    ) -> CompiledWireProgram:
        """Return one validated primitive or reject an incomplete profile."""
        try:
            return self.programs[primitive]
        except KeyError as exc:
            message = f"Compiled {self.ds_version} profile has no {primitive!r} program"
            raise WireContractError(message) from exc


@dataclass(frozen=True)
class _CompiledWireRegistries:
    request_schemas: Mapping[str, CompiledRequestSchema]
    response_schemas: Mapping[str, CompiledResponseSchema]
    codecs: JsonObject


@dataclass(frozen=True)
class ValidatedCompiledWireInstallation:
    """One process's verified, fixed installation of compiled wire artifacts."""

    domain_modules: tuple[str, ...]
    content_digest: str
    runtime_content_digest: str

    def load_module(self, name: str) -> ModuleType:
        """Load an owned domain without rechecking unchanged installation bytes."""
        if _PYTHON_MODULE.fullmatch(name) is None:
            message = "Compiled wire module name is invalid"
            raise WireContractError(message)
        if f"{name}.py" not in self.domain_modules:
            message = f"Compiled wire module {name!r} is not in the domain inventory"
            raise WireContractError(message)
        module = importlib.import_module(f"dsctl.generated.wire_programs.{name}")
        _require_canonical_module(
            module,
            relative_path=f"dsctl/generated/wire_programs/{name}.py",
            label=f"compiled wire module {name}",
        )
        return module


def load_compiled_wire_profiles(
    name: str,
    *,
    schema_constant: str,
    schema_version: int,
    target_versions: tuple[str, ...],
    primitives: tuple[str, ...],
    execution_modes: Mapping[str, WireExecutionMode],
    result_envelopes: Mapping[str, WireResultEnvelope],
    primitive_absent_versions: Mapping[str, frozenset[str]],
    installation: ValidatedCompiledWireInstallation | None = None,
) -> Mapping[str, CompiledWireProfile]:
    """Validate and bind one complete compiled domain program set.

    Domain adapters receive immutable, executable profiles; artifact shape,
    provenance, codec registries, digests, and ownership policy stay behind
    this shared runtime seam. Without an installation handle, every call
    freshly validates the complete installed artifact and shared runtime.
    """
    _validate_compiled_loader_contract(
        schema_constant=schema_constant,
        primitives=primitives,
        execution_modes=execution_modes,
        result_envelopes=result_envelopes,
        primitive_absent_versions=primitive_absent_versions,
        target_versions=target_versions,
    )
    module = (
        load_compiled_wire_module(name)
        if installation is None
        else installation.load_module(name)
    )
    if getattr(module, schema_constant, None) != schema_version:
        message = f"Compiled {name} artifact schema is unsupported"
        raise WireContractError(message)
    if getattr(module, "TARGET_DS_VERSIONS", None) != target_versions:
        message = f"Compiled {name} exact-version inventory is incomplete"
        raise WireContractError(message)

    profile_records = _compiled_mapping(
        getattr(module, "PROFILES", None),
        label=f"{name} profiles",
    )
    if tuple(profile_records) != target_versions:
        message = f"Compiled {name} profiles are not exact and ordered"
        raise WireContractError(message)
    registries = _load_compiled_registries(
        module,
        name=name,
    )
    profiles: dict[str, CompiledWireProfile] = {}
    used_codecs: set[str] = set()
    used_requests: set[str] = set()
    used_responses: set[str] = set()
    for version, value in profile_records.items():
        profile, profile_codecs, profile_requests, profile_responses = (
            _load_compiled_profile(
                name=name,
                ds_version=version,
                value=value,
                primitives=primitives,
                registries=registries,
                execution_modes=execution_modes,
                result_envelopes=result_envelopes,
                primitive_absent_versions=primitive_absent_versions,
            )
        )
        profiles[version] = profile
        used_codecs.update(profile_codecs)
        used_requests.update(profile_requests)
        used_responses.update(profile_responses)

    if set(registries.codecs) != used_codecs:
        message = f"Compiled {name} codec inventory has unreachable entries"
        raise WireContractError(message)
    if set(registries.response_schemas) != used_responses:
        message = f"Compiled {name} response-schema inventory has unreachable entries"
        raise WireContractError(message)
    if set(registries.request_schemas) != used_requests:
        message = f"Compiled {name} request-schema inventory has unreachable entries"
        raise WireContractError(message)
    return MappingProxyType(profiles)


def _validate_compiled_loader_contract(
    *,
    schema_constant: str,
    primitives: tuple[str, ...],
    execution_modes: Mapping[str, WireExecutionMode],
    result_envelopes: Mapping[str, WireResultEnvelope],
    primitive_absent_versions: Mapping[str, frozenset[str]],
    target_versions: tuple[str, ...],
) -> None:
    expected = set(primitives)
    if _UPPER_CONSTANT.fullmatch(schema_constant) is None:
        message = "Compiled wire schema constant is invalid"
        raise WireContractError(message)
    if (
        len(expected) != len(primitives)
        or set(execution_modes) != expected
        or set(result_envelopes) != expected
        or set(primitive_absent_versions) != expected
        or any(
            not isinstance(versions, frozenset) or not versions <= set(target_versions)
            for versions in primitive_absent_versions.values()
        )
    ):
        message = "Compiled wire loader contract inventory is inconsistent"
        raise WireContractError(message)


def _load_compiled_registries(
    module: ModuleType,
    *,
    name: str,
) -> _CompiledWireRegistries:
    return _CompiledWireRegistries(
        request_schemas=_compiled_request_schemas(
            getattr(module, "REQUEST_SCHEMAS", None),
            label=f"{name} request schemas",
        ),
        response_schemas=_compiled_response_schemas(
            getattr(module, "RESPONSE_SCHEMAS", None),
            label=f"{name} response schemas",
        ),
        codecs=_compiled_mapping(
            getattr(module, "CODECS", None),
            label=f"{name} codecs",
        ),
    )


def _load_compiled_profile(
    *,
    name: str,
    ds_version: str,
    value: JsonValue,
    primitives: tuple[str, ...],
    registries: _CompiledWireRegistries,
    execution_modes: Mapping[str, WireExecutionMode],
    result_envelopes: Mapping[str, WireResultEnvelope],
    primitive_absent_versions: Mapping[str, frozenset[str]],
) -> tuple[CompiledWireProfile, set[str], set[str], set[str]]:
    record = _compiled_mapping(value, label=f"{name} profile {ds_version}")
    if set(record) != {"status", "source", "recipe_id", "programs", "profile_digest"}:
        message = f"Compiled {name} profile {ds_version} has unsupported fields"
        raise WireContractError(message)
    profile_digest = _required_sha256(
        record.get("profile_digest"),
        label=f"{name} profile {ds_version}",
    )
    expected_profile_digest = _canonical_json_digest(
        {
            "schema_version": 2,
            "status": record.get("status"),
            "source": record.get("source"),
            "recipe_id": record.get("recipe_id"),
            "programs": record.get("programs"),
        },
        label=f"{name} profile {ds_version}",
    )
    if profile_digest != expected_profile_digest:
        message = f"Compiled {name} profile {ds_version} digest does not match"
        raise WireContractError(message)
    status = record.get("status")
    if not isinstance(status, str) or status not in {
        "supported",
        "upstream_absent",
    }:
        message = f"Compiled {name} profile {ds_version} status is invalid"
        raise WireContractError(message)
    recipe_id = _compiled_recipe_id(
        record.get("recipe_id"),
        status=status,
        label=f"{name} profile {ds_version}",
    )
    commit, tree, source_digest = _compiled_source_identity(
        name=name,
        ds_version=ds_version,
        value=record.get("source"),
    )
    program_records = _compiled_mapping(
        record.get("programs"),
        label=f"{name} profile {ds_version} programs",
    )
    if status == "upstream_absent":
        if program_records:
            message = f"Compiled absent {name} profile {ds_version} has programs"
            raise WireContractError(message)
        return (
            CompiledWireProfile(
                ds_version=ds_version,
                status=status,
                recipe_id=None,
                source_commit=commit,
                source_tree=tree,
                source_contract_digest=source_digest,
                codec_names=MappingProxyType({}),
                programs=MappingProxyType({}),
            ),
            set(),
            set(),
            set(),
        )
    expected_primitives = {
        primitive
        for primitive in primitives
        if ds_version not in primitive_absent_versions[primitive]
    }
    if not expected_primitives or set(program_records) != expected_primitives:
        message = f"Compiled {name} profile {ds_version} programs are incomplete"
        raise WireContractError(message)

    bound_programs: dict[str, CompiledWireProgram] = {}
    codec_names: dict[str, str] = {}
    used_requests: set[str] = set()
    used_responses: set[str] = set()
    for primitive in primitives:
        if primitive not in expected_primitives:
            continue
        program, codec_name, request_name, response_name = _load_compiled_program(
            name=name,
            ds_version=ds_version,
            primitive=primitive,
            source_digest=source_digest,
            record=_compiled_mapping(
                program_records[primitive],
                label=f"{name} {ds_version} {primitive} program",
            ),
            request_schemas=registries.request_schemas,
            response_schemas=registries.response_schemas,
            codecs=registries.codecs,
            execution_mode=execution_modes[primitive],
            expected_envelope=result_envelopes[primitive],
        )
        bound_programs[primitive] = program
        codec_names[primitive] = codec_name
        used_requests.add(request_name)
        if response_name is not None:
            used_responses.add(response_name)
    return (
        CompiledWireProfile(
            ds_version=ds_version,
            status=status,
            recipe_id=recipe_id,
            source_commit=commit,
            source_tree=tree,
            source_contract_digest=source_digest,
            codec_names=MappingProxyType(codec_names),
            programs=MappingProxyType(bound_programs),
        ),
        set(codec_names.values()),
        used_requests,
        used_responses,
    )


def _compiled_recipe_id(value: JsonValue, *, status: str, label: str) -> str | None:
    if status == "upstream_absent" and value is None:
        return None
    if status == "supported" and isinstance(value, str) and _RECIPE_ID.fullmatch(value):
        return value
    message = f"Compiled {label} recipe is invalid"
    raise WireContractError(message)


def _compiled_source_identity(
    *,
    name: str,
    ds_version: str,
    value: JsonValue,
) -> tuple[str, str, str]:
    source = _compiled_mapping(
        value,
        label=f"{name} profile {ds_version} source",
    )
    if (
        set(source) != {"tag", "commit", "tree", "contract_digest"}
        or source.get("tag") != ds_version
    ):
        message = f"Compiled {name} profile {ds_version} source is incomplete"
        raise WireContractError(message)
    return (
        _required_git_object(
            source.get("commit"),
            label=f"{name} profile {ds_version} commit",
        ),
        _required_git_object(
            source.get("tree"),
            label=f"{name} profile {ds_version} tree",
        ),
        _required_sha256(
            source.get("contract_digest"),
            label=f"{name} profile {ds_version} source contract",
        ),
    )


def _load_compiled_program(
    *,
    name: str,
    ds_version: str,
    primitive: str,
    source_digest: str,
    record: JsonObject,
    request_schemas: Mapping[str, CompiledRequestSchema],
    response_schemas: Mapping[str, CompiledResponseSchema],
    codecs: JsonObject,
    execution_mode: WireExecutionMode,
    expected_envelope: WireResultEnvelope,
) -> tuple[CompiledWireProgram, str, str, str | None]:
    """Validate and bind one data-only compiled program record."""
    source_operation, codec_name = _compiled_program_identity(
        name=name,
        primitive=primitive,
        record=record,
    )
    (
        codec_record,
        request_name,
        request_schema_digest,
        response_name,
        response_schema_digest,
    ) = _compiled_codec_record(
        name=name,
        codec_name=codec_name,
        value=codecs.get(codec_name),
        request_schemas=request_schemas,
        response_schemas=response_schemas,
    )
    _require_compiled_program_digest(
        name=name,
        primitive=primitive,
        record=record,
        codec_record=codec_record,
    )
    response_adapter = _compiled_response_adapter(
        response_schemas,
        name=name,
        codec_name=codec_name,
        response_name=response_name,
    )
    program_response_schema_digest = _optional_sha256(
        record.get("response_schema_digest"),
        label=f"{name} {primitive} response schema",
    )
    if program_response_schema_digest != response_schema_digest:
        message = f"Compiled {name} {primitive} response schema does not match codec"
        raise WireContractError(message)
    program_request_schema_digest = _required_sha256(
        record.get("request_schema_digest"),
        label=f"{name} {primitive} request schema",
    )
    if program_request_schema_digest != request_schema_digest:
        message = f"Compiled {name} {primitive} request schema does not match codec"
        raise WireContractError(message)
    result_envelope = _compiled_result_envelope(
        record.get("result_envelope"),
        name=name,
        primitive=primitive,
        expected=expected_envelope,
    )
    method = codec_record.get("method")
    path = codec_record.get("path")
    channel = codec_record.get("channel")
    path_encoding = codec_record.get("path_encoding")
    field_bindings = _compiled_field_bindings(
        codec_record.get("fields"),
        name=name,
        codec_name=codec_name,
    )
    path_fields = _compiled_path_fields(
        codec_record.get("path_fields"),
        name=name,
        codec_name=codec_name,
    )
    capture = codec_record.get("capture")
    if not all(isinstance(value, str) for value in (method, path, channel)):
        message = f"Compiled {name} codec {codec_name} transport is invalid"
        raise WireContractError(message)
    if path_encoding is not None and not isinstance(path_encoding, str):
        message = f"Compiled {name} codec {codec_name} path encoding is invalid"
        raise WireContractError(message)
    if not is_json_value(capture):
        message = f"Compiled {name} codec {codec_name} capture is not JSON"
        raise WireContractError(message)
    codec = CompiledWireCodec(
        method=cast("str", method),
        path=cast("str", path),
        channel=cast("str", channel),
        path_encoding=path_encoding if isinstance(path_encoding, str) else None,
        field_bindings=field_bindings,
        path_fields=path_fields,
        params_model=request_schemas[request_name].model,
        response_adapter=response_adapter,
        capture_payload=capture,
        request_fingerprint=_required_sha256(
            record.get("codec_digest"),
            label=f"{name} {primitive} codec",
        ),
        request_schema_fingerprint=request_schema_digest,
        response_fingerprint=_required_sha256(
            record.get("response_digest"),
            label=f"{name} {primitive} response",
        ),
        response_schema_fingerprint=response_schema_digest,
        result_envelope=result_envelope,
        response_projection=cast("str", codec_record["response_projection"]),
        file_fields=_compiled_path_fields(
            codec_record.get("file_fields", []), name=name, codec_name=codec_name
        ),
        response_transport=cast("str", codec_record.get("response_transport", "json")),
    )
    return (
        compiled_wire_program(
            ds_version=ds_version,
            source_operation=source_operation,
            source_contract_digest=source_digest,
            execution_mode=execution_mode,
            codec=codec,
        ),
        codec_name,
        request_name,
        response_name,
    )


def _compiled_program_identity(
    *,
    name: str,
    primitive: str,
    record: JsonObject,
) -> tuple[str, str]:
    if set(record) != {
        "source_operation",
        "codec",
        "codec_digest",
        "request_schema_digest",
        "response_digest",
        "response_schema_digest",
        "result_envelope",
        "program_digest",
    }:
        message = f"Compiled {name} {primitive} program has unsupported fields"
        raise WireContractError(message)
    source_operation = record.get("source_operation")
    codec_name = record.get("codec")
    if not isinstance(source_operation, str) or not source_operation:
        message = f"Compiled {name} {primitive} source operation is invalid"
        raise WireContractError(message)
    if not isinstance(codec_name, str):
        message = f"Compiled {name} {primitive} codec identity is invalid"
        raise WireContractError(message)
    return source_operation, codec_name


def _require_compiled_program_digest(
    *,
    name: str,
    primitive: str,
    record: JsonObject,
    codec_record: JsonObject,
) -> None:
    program_digest = _required_sha256(
        record.get("program_digest"),
        label=f"{name} {primitive} program",
    )
    expected_program_digest = _canonical_json_digest(
        {
            "schema_version": 4,
            "codec_record": codec_record,
            "source_operation": record.get("source_operation"),
            "codec": record.get("codec"),
            "codec_digest": record.get("codec_digest"),
            "request_schema_digest": record.get("request_schema_digest"),
            "response_digest": record.get("response_digest"),
            "response_schema_digest": record.get("response_schema_digest"),
            "result_envelope": record.get("result_envelope"),
        },
        label=f"{name} {primitive} program",
    )
    if program_digest != expected_program_digest:
        message = f"Compiled {name} {primitive} program digest does not match"
        raise WireContractError(message)


def _compiled_codec_record(
    *,
    name: str,
    codec_name: str,
    value: JsonValue,
    request_schemas: Mapping[str, CompiledRequestSchema],
    response_schemas: Mapping[str, CompiledResponseSchema],
) -> tuple[JsonObject, str, str, str | None, str | None]:
    record = _compiled_mapping(value, label=f"{name} codec {codec_name}")
    optional_keys = {
        key
        for key, applies in (
            ("file_fields", record.get("channel") == "multipart"),
            ("response_transport", record.get("response_transport") == "binary"),
        )
        if applies
    }
    if set(record) - optional_keys != {
        "method",
        "path",
        "channel",
        "path_encoding",
        "fields",
        "path_fields",
        "params",
        "request_schema_digest",
        "response",
        "response_schema_digest",
        "response_projection",
        "capture",
    }:
        message = f"Compiled {name} codec {codec_name} has unsupported fields"
        raise WireContractError(message)
    projection = record.get("response_projection")
    if not isinstance(projection, str) or projection not in {
        "direct",
        "status_data",
        "single_data",
    }:
        message = (
            f"Compiled {name} codec {codec_name} response projection is unsupported"
        )
        raise WireContractError(message)
    request_name = record.get("params")
    if not isinstance(request_name, str) or not request_name:
        message = f"Compiled {name} codec {codec_name} parameters are invalid"
        raise WireContractError(message)
    request_schema_digest = _required_sha256(
        record.get("request_schema_digest"),
        label=f"{name} codec {codec_name} request schema",
    )
    request_schema = request_schemas.get(request_name)
    if request_schema is None or request_schema.digest != request_schema_digest:
        message = f"Compiled {name} codec {codec_name} request schema is invalid"
        raise WireContractError(message)
    response_name = record.get("response")
    if response_name is not None and not isinstance(response_name, str):
        message = f"Compiled {name} codec {codec_name} response is invalid"
        raise WireContractError(message)
    response_schema_digest = _optional_sha256(
        record.get("response_schema_digest"),
        label=f"{name} codec {codec_name} response schema",
    )
    if response_name is None:
        if response_schema_digest is not None:
            message = f"Compiled {name} codec {codec_name} has an unused schema"
            raise WireContractError(message)
    else:
        response_schema = response_schemas.get(response_name)
        if response_schema is None or response_schema.digest != response_schema_digest:
            message = f"Compiled {name} codec {codec_name} response schema is invalid"
            raise WireContractError(message)
    return (
        record,
        request_name,
        request_schema_digest,
        response_name,
        response_schema_digest,
    )


def _compiled_field_bindings(
    value: JsonValue,
    *,
    name: str,
    codec_name: str,
) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        message = f"Compiled {name} codec {codec_name} fields are invalid"
        raise WireContractError(message)
    bindings: list[tuple[str, str]] = []
    for index, item in enumerate(value):
        record = _compiled_mapping(
            item,
            label=f"{name} codec {codec_name} field {index}",
        )
        field_name = record.get("name")
        binding = record.get("binding")
        if (
            set(record) != {"name", "binding"}
            or not isinstance(field_name, str)
            or not field_name
            or binding not in {"path_variable", "request_param", "request_body"}
        ):
            message = f"Compiled {name} codec {codec_name} fields are invalid"
            raise WireContractError(message)
        bindings.append((field_name, cast("str", binding)))
    return tuple(bindings)


def _compiled_path_fields(
    value: JsonValue,
    *,
    name: str,
    codec_name: str,
) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        message = f"Compiled {name} codec {codec_name} path fields are invalid"
        raise WireContractError(message)
    fields = cast("list[str]", value)
    if len(fields) != len(set(fields)):
        message = f"Compiled {name} codec {codec_name} path fields are invalid"
        raise WireContractError(message)
    return tuple(fields)


def _compiled_response_adapter(
    response_schemas: Mapping[str, CompiledResponseSchema],
    *,
    name: str,
    codec_name: str,
    response_name: str | None,
) -> TypeAdapter[OpaqueGeneratedValue] | None:
    if response_name is None:
        return None
    candidate = response_schemas.get(response_name)
    if candidate is None:
        message = f"Compiled {name} codec {codec_name} response adapter is invalid"
        raise WireContractError(message)
    try:
        return TypeAdapter(candidate.annotation)
    except (TypeError, ValueError) as exc:
        message = f"Compiled {name} codec {codec_name} response adapter is invalid"
        raise WireContractError(message) from exc


def _compiled_result_envelope(
    value: JsonValue,
    *,
    name: str,
    primitive: str,
    expected: WireResultEnvelope,
) -> WireResultEnvelope:
    if not isinstance(value, str):
        message = f"Compiled {name} {primitive} result envelope is invalid"
        raise WireContractError(message)
    try:
        envelope = WireResultEnvelope(value)
    except ValueError as exc:
        message = f"Compiled {name} {primitive} result envelope is invalid"
        raise WireContractError(message) from exc
    if envelope is not expected:
        message = f"Compiled {name} {primitive} result-envelope policy changed"
        raise WireContractError(message)
    return envelope


def compiled_wire_program(
    *,
    ds_version: str,
    source_operation: str,
    source_contract_digest: str,
    execution_mode: WireExecutionMode,
    codec: CompiledWireCodec,
) -> CompiledWireProgram:
    """Bind one generated data-only codec to the shared wire executor."""
    return CompiledWireProgram(
        ds_version=ds_version,
        source_operation=source_operation,
        source_contract_digest=source_contract_digest,
        execution_mode=execution_mode,
        codec=codec,
        request_codec_fingerprint=codec.request_fingerprint,
        result_envelope=codec.result_envelope,
    )


def _validate_compiled_path_binding(codec: CompiledWireCodec) -> None:
    placeholders = _path_placeholders(codec.path)
    model_fields = tuple(
        field.serialization_alias or field.alias or name
        for name, field in codec.params_model.model_fields.items()
    )
    field_names = tuple(name for name, _binding in codec.field_bindings)
    nonfile_fields = tuple(
        name for name in field_names if name not in codec.file_fields
    )
    path_fields = tuple(
        name for name, binding in codec.field_bindings if binding == "path_variable"
    )
    bindings = {binding for _name, binding in codec.field_bindings}
    if (
        (not field_names and (codec.method, codec.channel) != ("GET", "query"))
        or len(field_names) != len(set(field_names))
        or nonfile_fields != model_fields
        or any(
            (name, "request_param") not in codec.field_bindings
            for name in codec.file_fields
        )
        or not bindings <= {"path_variable", "request_param", "request_body"}
        or len(codec.path_fields) != len(set(codec.path_fields))
        or set(path_fields) != set(codec.path_fields)
    ):
        message = "Compiled wire codec field-binding inventory is inconsistent"
        raise WireContractError(message)
    if codec.channel in {"json", "path_json", "json_text", "path_json_text"}:
        _validate_compiled_body_binding(codec, placeholders)
        return
    if codec.channel in {"path", "path_form", "path_query"}:
        if (
            len(placeholders)
            not in _PATH_SEGMENT_COUNTS.get((codec.method, codec.channel), ())
            or len(placeholders) != len(set(placeholders))
            or set(placeholders) != set(codec.path_fields)
            or (
                bindings != {"path_variable"}
                if codec.channel == "path"
                else bindings != {"path_variable", "request_param"}
            )
        ):
            message = "Compiled wire codec path-variable inventory is inconsistent"
            raise WireContractError(message)
    elif (
        placeholders
        or codec.path_fields
        or bindings != ({"request_param"} if field_names else set())
    ):
        message = "Compiled non-path wire codec contains path placeholders"
        raise WireContractError(message)


def _validate_legacy_path_binding(codec: CompiledWireCodec) -> None:
    if codec.path_fields != ("projectName",) or (codec.method, codec.channel) not in {
        ("GET", "path"),
        ("GET", "path_query"),
        ("POST", "path_form"),
    }:
        message = "Legacy URL interpolation requires one legacy project-name path"
        raise WireContractError(message)
    path_field = next(
        field
        for name, field in codec.params_model.model_fields.items()
        if (field.serialization_alias or field.alias or name) == "projectName"
    )
    if path_field.annotation is not str or not path_field.is_required():
        message = "Legacy URL interpolation requires a mandatory native string"
        raise WireContractError(message)


def _validate_compiled_body_binding(
    codec: CompiledWireCodec,
    placeholders: tuple[str, ...],
) -> None:
    body_fields = tuple(
        name for name, binding in codec.field_bindings if binding == "request_body"
    )
    has_path = codec.channel in {"path_json", "path_json_text"}
    if (
        len(body_fields) != 1
        or any(binding == "request_param" for _name, binding in codec.field_bindings)
        or len(placeholders) != int(has_path)
        or set(placeholders) != set(codec.path_fields)
    ):
        message = "Compiled wire codec body-binding inventory is inconsistent"
        raise WireContractError(message)
    body_field = next(
        field
        for name, field in codec.params_model.model_fields.items()
        if (field.serialization_alias or field.alias or name) == body_fields[0]
    )
    annotation = body_field.annotation
    is_json_text = codec.channel in {"json_text", "path_json_text"}
    valid_type = (
        annotation is str
        if is_json_text
        else isinstance(annotation, type) and issubclass(annotation, BaseModel)
    )
    if not body_field.is_required() or not valid_type:
        message = "Compiled wire codec body must be a required native model or string"
        raise WireContractError(message)


def _render_compiled_path(
    template: str,
    values: _GeneratedRequestMapping,
    *,
    allow_enum: bool,
    legacy_text: bool = False,
) -> str:
    placeholders = _path_placeholders(template)
    if set(placeholders) != set(values):
        message = "Compiled wire path values do not match its placeholders"
        raise WireContractError(message)

    def encoded(match: re.Match[str]) -> str:
        value = values[match.group(1)]
        if legacy_text:
            if type(value) is not str:
                message = "Legacy URL interpolation requires a native string value"
                raise WireContractError(message)
            return value
        if isinstance(value, bool) or not (
            isinstance(value, int) or (allow_enum and isinstance(value, StrEnum))
        ):
            message = (
                "Compiled wire path value must be an integer segment "
                "or a native enum for single-segment GET path"
            )
            raise WireContractError(message)
        return quote(str(value), safe="")

    return _PATH_PLACEHOLDER.sub(encoded, template)


def _path_placeholders(path: str) -> tuple[str, ...]:
    if any(character in path for character in "?#\t\r\n"):
        message = (
            "Compiled wire codec path contains query, fragment, or URL control syntax"
        )
        raise WireContractError(message)
    placeholders = tuple(_PATH_PLACEHOLDER.findall(path))
    remainder = _PATH_PLACEHOLDER.sub("", path)
    if "{" in remainder or "}" in remainder:
        message = "Compiled wire codec path placeholders are malformed"
        raise WireContractError(message)
    return placeholders


def load_compiled_wire_module(name: str) -> ModuleType:
    """Validate the complete central artifact, then load one generated module."""
    if _PYTHON_MODULE.fullmatch(name) is None:
        message = "Compiled wire module name is invalid"
        raise WireContractError(message)
    return validate_compiled_wire_installation().load_module(name)


@cache
def installed_compiled_wire_installation() -> ValidatedCompiledWireInstallation:
    """Verify installation bytes once; running processes do not reload packages."""
    return validate_compiled_wire_installation()


def validate_compiled_wire_installation() -> ValidatedCompiledWireInstallation:
    """Perform a fresh complete installation check without reading any cache."""
    package_root = "dsctl.generated.wire_programs"
    try:
        manifest = importlib.import_module(f"{package_root}._manifest")
    except ModuleNotFoundError as exc:
        message = "Compiled wire artifact manifest is not installed"
        raise WireContractError(message) from exc
    expected_constants = {
        "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
        "DOMAIN_MODULES",
        "RENDERER_ABI",
        "MODULES",
        "SUPPORT_MODULES",
        "CONTENT_DIGEST",
        "WIRE_RUNTIME_CONTENT_DIGEST",
    }
    if {item for item in dir(manifest) if item.isupper()} != expected_constants:
        message = "Compiled wire artifact manifest inventory is unsupported"
        raise WireContractError(message)
    if getattr(manifest, "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION", None) != 5:
        message = "Compiled wire artifact schema is unsupported"
        raise WireContractError(message)
    if getattr(manifest, "RENDERER_ABI", None) != 15:
        message = "Compiled wire artifact renderer ABI is unsupported"
        raise WireContractError(message)
    modules = _compiled_module_inventory(getattr(manifest, "MODULES", None))
    domain_modules = _compiled_module_inventory(
        getattr(manifest, "DOMAIN_MODULES", None)
    )
    support_modules = _compiled_module_inventory(
        getattr(manifest, "SUPPORT_MODULES", None),
        allow_empty=True,
    )
    if tuple(sorted(("__init__.py", *domain_modules, *support_modules))) != modules:
        message = "Compiled wire domain/support module inventory is inconsistent"
        raise WireContractError(message)
    expected_digest = _required_sha256(
        getattr(manifest, "CONTENT_DIGEST", None),
        label="compiled wire content",
    )
    _validate_installed_artifact(
        package_root,
        modules,
        expected_digest=expected_digest,
        domain=_COMPILED_WIRE_CONTENT_DIGEST_DOMAIN,
        label="compiled wire artifact",
    )
    runtime_digest = _required_sha256(
        getattr(manifest, "WIRE_RUNTIME_CONTENT_DIGEST", None),
        label="compiled wire runtime",
    )
    _validate_compiled_wire_runtime(runtime_digest)
    return ValidatedCompiledWireInstallation(
        domain_modules=domain_modules,
        content_digest=expected_digest,
        runtime_content_digest=runtime_digest,
    )


class WireExecutor:
    """Execute prepared wire calls through one exact-profile shared client."""

    def __init__(
        self,
        client: DolphinSchedulerClient,
        *,
        source_contract_digest: str,
    ) -> None:
        """Bind execution to one client profile and generated source digest."""
        self._client = client
        self._source_contract_digest = source_contract_digest

    def execute(
        self,
        program: CompiledWireProgram,
        prepared: PreparedCompiledWireCall,
    ) -> WireExecution[OpaqueGeneratedValue]:
        """Validate the binding and send exactly the prepared generated request."""
        self._validate_binding(program, prepared)
        request = prepared.request
        _validate_compiled_request(program, prepared, request)
        if (
            program.codec.channel == "multipart"
            or program.codec.response_transport != "json"
        ):
            message = "Compiled file transfer requires its dedicated executor method"
            raise WireContractError(message)

        headers = {"accept": "application/json"}
        if request.content is not None:
            headers["Content-Type"] = "application/json"
        send = (
            self._client.request_result
            if program.result_envelope is WireResultEnvelope.REQUIRED
            else self._client.request_payload
        )
        # Preparation fixes the sole exchange; response decoding has no transport
        # capability and therefore cannot issue another request.
        raw_payload = send(
            request.method,
            request.path.lstrip("/"),
            params=request.query,
            form_data=request.form,
            json_body=request.json,
            content=request.content,
            headers=headers,
            retryable=program.execution_mode.retryable,
        )
        preserved_payload = deepcopy(raw_payload)
        try:
            payload = program.codec.decode(raw_payload)
        except ApiTransportError as exc:
            raise WireResponseDecodeError(
                exc.message,
                details={
                    **exc.details,
                    "wire_response_received": True,
                },
                source=exc.source,
                suggestion=exc.suggestion,
            ) from exc
        return WireExecution(
            payload=payload,
            raw_payload=preserved_payload,
            request=request,
        )

    def upload(
        self,
        program: CompiledWireProgram,
        prepared: PreparedCompiledWireCall,
        *,
        files: MultipartFiles,
    ) -> WireExecution[OpaqueGeneratedValue]:
        """Send a validated form with live file handles kept outside JSON state."""
        self._validate_binding(program, prepared)
        request = prepared.request
        _validate_compiled_request(program, prepared, request)
        if (
            program.codec.channel != "multipart"
            or set(files) != set(program.codec.file_fields)
            or program.execution_mode is not WireExecutionMode.MUTATION_ONCE
        ):
            message = "Compiled upload requires the reviewed multipart file inventory"
            raise WireContractError(message)
        payload = self._client.request_result(
            request.method,
            request.path,
            form_data=request.form,
            files=files,
            retryable=False,
        )
        # The reviewed native upload wrapper checks the Result envelope and
        # ignores its data. File handles remain caller-owned and are not copied.
        return WireExecution(payload=None, raw_payload=payload, request=request)

    def download(
        self,
        program: CompiledWireProgram,
        prepared: PreparedCompiledWireCall,
    ) -> BinaryResponse:
        """Return raw bytes and HTTP metadata without a JSON projection."""
        self._validate_binding(program, prepared)
        request = prepared.request
        _validate_compiled_request(program, prepared, request)
        if program.codec.response_transport != "binary":
            message = "Compiled download requires the reviewed binary transport"
            raise WireContractError(message)
        return self._client.get_binary(
            request.path,
            params=request.query,
            retryable=program.execution_mode.retryable,
        )

    def _validate_binding(
        self,
        program: CompiledWireProgram,
        prepared: PreparedWireCallToken,
    ) -> None:
        read_policy = getattr(self._client, "read_policy", None)
        if read_policy is not None and (
            program.execution_mode.mutates
            or program.fingerprint not in read_policy.program_fingerprints
        ):
            message = "Wire call is outside the admitted read compatibility closure"
            raise UnsupportedFeatureError(
                message,
                details={
                    "reason": "read_operation_not_admitted",
                    "action": read_policy.action,
                    "source_operation": program.source_operation,
                },
                suggestion=(
                    "Run `dsctl doctor` to inspect available read commands, or set "
                    "DS_VERSION to the deployment's actual release."
                ),
            )
        profile_version = self._client.profile.ds_version
        if (
            program.ds_version != profile_version
            or prepared.ds_version != profile_version
        ):
            message = "Prepared wire call does not match the exact client profile"
            raise WireContractError(message)
        if program.source_contract_digest != self._source_contract_digest:
            message = "Wire program source contract does not match the executor"
            raise WireContractError(message)
        if prepared.program_fingerprint != program.fingerprint:
            message = "Prepared wire call does not match the selected program"
            raise WireContractError(message)


def _validate_compiled_request(
    program: CompiledWireProgram,
    prepared: PreparedWireCallToken,
    request: WireRequest,
) -> None:
    if not isinstance(prepared, PreparedCompiledWireCall):
        message = "Compiled wire program requires an encoded request"
        raise WireContractError(message)
    if prepared._codec is not program.codec:
        message = "Prepared compiled request does not match the selected codec"
        raise WireContractError(message)
    actual = _compiled_request_fingerprint(prepared.program_fingerprint, request)
    if actual != prepared._request_fingerprint:
        message = "Prepared compiled request no longer matches its encoded content"
        raise WireContractError(message)


def _compiled_request_fingerprint(program: str, request: WireRequest) -> str:
    return _canonical_json_digest(
        {"program": program, "request": asdict(request)},
        label="prepared compiled request",
    )


def _raise_payload_error(
    message: str,
    *,
    cause: Exception | None = None,
) -> NoReturn:
    """Translate one generated response-contract failure."""
    details: JsonObject = {"validation_message": message}
    cause_error_count = (
        cause.error_count() if isinstance(cause, ValidationError) else None
    )
    if cause_error_count is not None:
        details["validation_error_count"] = cause_error_count
    validation_errors = _bounded_validation_errors(cause)
    if validation_errors:
        details["validation_errors"] = validation_errors
    if cause_error_count is not None and cause_error_count > _MAX_VALIDATION_ERRORS:
        details["validation_errors_truncated"] = True
    error = ApiTransportError(
        ("DolphinScheduler response payload did not match the generated API contract."),
        details=details,
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
        suggestion=(
            "Verify DS_VERSION matches the server, check DolphinScheduler "
            "API health, then retry."
        ),
    )
    if cause is None:
        raise error
    raise error from cause


_MAX_VALIDATION_ERRORS = 5
_MAX_VALIDATION_PATH_PARTS = 12
_MAX_VALIDATION_PATH_CHARS = 256
_SAFE_VALIDATION_PATH_PART = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")


def _bounded_validation_errors(cause: Exception | None) -> list[JsonObject]:
    """Return bounded, value-free Pydantic error facts for CLI diagnostics."""
    if not isinstance(cause, ValidationError):
        return []
    if cause.error_count() > _MAX_VALIDATION_ERRORS:
        return []
    errors = cause.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    )
    return [
        {
            "field": _validation_field_path(error.get("loc", ())),
            "type": str(error.get("type", "unknown")),
            "message": _safe_validation_message(str(error.get("type", "unknown"))),
        }
        for error in errors
    ]


def _safe_validation_message(error_type: str) -> str:
    """Return a static diagnostic that cannot echo a rejected response value."""
    if error_type == "missing":
        return "Required response field is missing."
    if error_type == "enum":
        return "Response value is not an allowed enum member."
    if error_type == "literal_error":
        return "Response value does not match the required literal."
    if error_type.endswith("_type"):
        return "Response value has the wrong type."
    return "Response value does not match the generated field contract."


def _validation_field_path(location: Sequence[str | int]) -> str:
    """Render one Pydantic location as a compact dotted/indexed field path."""
    path = ""
    for part in location[:_MAX_VALIDATION_PATH_PARTS]:
        if isinstance(part, int):
            path += f"[{part}]"
            continue
        text = (
            part
            if _SAFE_VALIDATION_PATH_PART.fullmatch(part) is not None
            else "<dynamic-key>"
        )
        path += f".{text}" if path else text
    if len(location) > _MAX_VALIDATION_PATH_PARTS:
        path += ".<truncated>"
    if len(path) > _MAX_VALIDATION_PATH_CHARS:
        path = f"{path[: _MAX_VALIDATION_PATH_CHARS - 3]}..."
    return path or "$"


def _compiled_module_inventory(
    value: JsonValue,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, tuple) or (not value and not allow_empty):
        message = "Compiled wire MODULES must be a nonempty tuple"
        raise WireContractError(message)
    modules: list[str] = []
    for item in value:
        if not isinstance(item, str):
            message = "Compiled wire module path must be text"
            raise WireContractError(message)
        path = PurePosixPath(item)
        if (
            path.is_absolute()
            or path.as_posix() != item
            or path.suffix != ".py"
            or item == "_manifest.py"
            or not all(
                _PYTHON_MODULE.fullmatch(part) for part in path.with_suffix("").parts
            )
        ):
            message = "Compiled wire module path is unsafe"
            raise WireContractError(message)
        modules.append(item)
    if modules != sorted(set(modules)):
        message = "Compiled wire MODULES must be sorted and unique"
        raise WireContractError(message)
    return tuple(modules)


def _validate_installed_artifact(
    package_root: str,
    modules: tuple[str, ...],
    *,
    expected_digest: str,
    domain: bytes,
    label: str,
) -> None:
    root = resources.files(package_root)
    actual_inventory = tuple(sorted(_walk_importable_modules(root)))
    if actual_inventory != tuple(sorted((*modules, "_manifest.py"))):
        message = f"Generated {label} inventory does not match installed modules"
        raise WireContractError(message)
    digest = hashlib.sha256()
    digest.update(domain)
    for module in modules:
        path = PurePosixPath(module)
        try:
            content = root.joinpath(*path.parts).read_bytes()
        except (FileNotFoundError, OSError) as exc:
            message = f"Generated {label} module is not installed: {module}"
            raise WireContractError(message) from exc
        digest.update(module.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    actual_digest = f"sha256:{digest.hexdigest()}"
    if actual_digest != expected_digest:
        message = f"Generated {label} content digest does not match"
        raise WireContractError(message)


def _validate_compiled_wire_runtime(expected_digest: str) -> None:
    package_root = "dsctl.generated.wire_runtime"
    try:
        manifest = importlib.import_module(f"{package_root}._manifest")
    except ModuleNotFoundError as exc:
        message = "Compiled wire runtime manifest is not installed"
        raise WireContractError(message) from exc
    if getattr(manifest, "WIRE_RUNTIME_SCHEMA_VERSION", None) != 1:
        message = "Compiled wire runtime schema is unsupported"
        raise WireContractError(message)
    if getattr(manifest, "RENDERER_ABI", None) != 3:
        message = "Compiled wire runtime renderer ABI is unsupported"
        raise WireContractError(message)
    manifest_digest = _required_sha256(
        getattr(manifest, "CONTENT_DIGEST", None),
        label="wire runtime content",
    )
    if manifest_digest != expected_digest:
        message = "Compiled wire artifact targets a different wire runtime"
        raise WireContractError(message)
    modules = _compiled_module_inventory(getattr(manifest, "MODULES", None))
    _validate_installed_artifact(
        package_root,
        modules,
        expected_digest=expected_digest,
        domain=_WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN,
        label="wire runtime",
    )


def _walk_importable_modules(
    directory: Traversable,
    *,
    prefix: tuple[str, ...] = (),
) -> tuple[str, ...]:
    modules: list[str] = []
    for child in sorted(directory.iterdir(), key=lambda item: item.name):
        parts = (*prefix, child.name)
        if child.is_dir() and child.name != "__pycache__":
            modules.extend(_walk_importable_modules(child, prefix=parts))
        elif child.is_file() and child.name.endswith(_IMPORTABLE_MODULE_SUFFIXES):
            modules.append(PurePosixPath(*parts).as_posix())
    return tuple(modules)


def _require_canonical_module(
    module: ModuleType,
    *,
    relative_path: str,
    label: str,
) -> None:
    spec = getattr(module, "__spec__", None)
    origin = getattr(spec, "origin", None)
    normalized = origin.replace("\\", "/") if isinstance(origin, str) else ""
    if normalized != relative_path and not normalized.endswith(f"/{relative_path}"):
        message = f"Generated {label} is not loaded from its canonical source module"
        raise WireContractError(message)


def _required_sha256(value: JsonValue, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        message = f"Generated {label} digest is invalid"
        raise WireContractError(message)
    return value


def _optional_sha256(value: JsonValue, *, label: str) -> str | None:
    if value is None:
        return None
    return _required_sha256(value, label=label)


def _required_git_object(value: JsonValue, *, label: str) -> str:
    if not isinstance(value, str) or _GIT_OBJECT_ID.fullmatch(value) is None:
        message = f"Generated {label} identity is invalid"
        raise WireContractError(message)
    return value


def _compiled_request_schemas(
    value: OpaqueGeneratedValue,
    *,
    label: str,
) -> Mapping[str, CompiledRequestSchema]:
    if not isinstance(value, Mapping):
        message = f"Compiled {label} must be a request-schema registry"
        raise WireContractError(message)
    schemas: dict[str, CompiledRequestSchema] = {}
    for key, candidate in value.items():
        if (
            not isinstance(key, str)
            or not isinstance(candidate, CompiledRequestSchema)
            or not isinstance(candidate.model, type)
            or not issubclass(candidate.model, BaseModel)
        ):
            message = f"Compiled {label} contains an invalid request schema"
            raise WireContractError(message)
        _required_sha256(candidate.digest, label=f"{label} {key}")
        schemas[key] = candidate
    return MappingProxyType(schemas)


def _compiled_response_schemas(
    value: OpaqueGeneratedValue,
    *,
    label: str,
) -> Mapping[str, CompiledResponseSchema]:
    if not isinstance(value, Mapping):
        message = f"Compiled {label} must be a response-schema registry"
        raise WireContractError(message)
    schemas: dict[str, CompiledResponseSchema] = {}
    for key, candidate in value.items():
        if not isinstance(key, str) or not isinstance(
            candidate,
            CompiledResponseSchema,
        ):
            message = f"Compiled {label} contains an invalid response schema"
            raise WireContractError(message)
        _required_sha256(candidate.digest, label=f"{label} {key}")
        schemas[key] = candidate
    return MappingProxyType(schemas)


def _compiled_mapping(value: JsonValue, *, label: str) -> JsonObject:
    if not isinstance(value, dict) or not is_json_value(value):
        message = f"Compiled {label} must be a string-keyed object"
        raise WireContractError(message)
    return cast("JsonObject", value)


def _canonical_json_digest(value: JsonValue, *, label: str) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        message = f"Compiled {label} is not canonical JSON"
        raise WireContractError(message) from exc
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_GIT_OBJECT_ID = re.compile(r"[0-9a-f]{40}\Z")
_PYTHON_MODULE = re.compile(r"[a-z_][a-z0-9_]*\Z")
_UPPER_CONSTANT = re.compile(r"[A-Z][A-Z0-9_]*\Z")
_PATH_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PATH_ENCODING = "percent-encoded-utf8-segment-v1"
_LEGACY_PATH_ENCODING = "legacy-url-interpolation-v1"
_PATH_SEGMENT_COUNTS = {
    ("DELETE", "path"): frozenset({1, 2}),
    ("GET", "path"): frozenset({1, 2}),
    ("GET", "path_query"): frozenset({1, 2}),
    ("POST", "path"): frozenset({2}),
    ("POST", "path_form"): frozenset({1, 2}),
    ("PUT", "path_form"): frozenset({1, 2}),
}
_IMPORTABLE_MODULE_SUFFIXES = (".py", ".pyc", *EXTENSION_SUFFIXES)
_COMPILED_WIRE_CONTENT_DIGEST_DOMAIN = b"dsctl-compiled-wire-artifact-v5\0"
_RECIPE_ID = re.compile(r"[a-z][a-z0-9_]*\Z")
_WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN = b"dsctl-wire-runtime-v1\0"


__all__ = [
    "CompiledWireCodec",
    "CompiledWireProfile",
    "CompiledWireProgram",
    "PreparedWireCallToken",
    "ValidatedCompiledWireInstallation",
    "WireContractError",
    "WireExecution",
    "WireExecutionMode",
    "WireExecutor",
    "WireRequest",
    "WireResultEnvelope",
    "compiled_wire_program",
    "installed_compiled_wire_installation",
    "load_compiled_wire_module",
    "load_compiled_wire_profiles",
    "validate_compiled_wire_installation",
]
