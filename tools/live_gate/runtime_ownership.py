"""Prove current runtime ownership without importing candidate generated code.

Receipts continue to bind the legacy manifest verbatim. This current-truth
projection additionally accounts for whole domains that the compiler moved into
the separately content-bound wire artifact. It is not a replacement for runtime
executable-schema validation or the source-contract release lane.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING

from ds_codegen.compiled_literals import read_compiled_literals
from runtime_bundle_manifest import runtime_bundle_versions

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.compiled_domains import CompiledDomainDefinition, CompiledPrimitive

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_MODULE = re.compile(r"[a-z_][a-z0-9_]*\Z")
_PROGRAM_KEYS = frozenset(
    {
        "source_operation",
        "codec",
        "codec_digest",
        "request_schema_digest",
        "response_digest",
        "response_schema_digest",
        "result_envelope",
        "program_digest",
    }
)
_CODEC_KEYS = frozenset(
    {
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
    }
)


def load_current_runtime_ownership(
    source_root: Path, *, contracts: Mapping[str, Mapping[str, object]]
) -> Mapping[str, frozenset[str]]:
    """Return exact executable roots backed by candidate bytes and reviewed owners."""
    # The checker remains importable without importing any generated package.
    # Only an explicit proof loads the tool's trusted compiler policies; it never
    # adds the candidate source root to Python's import path.
    from ds_codegen.compiled_domains import (  # noqa: PLC0415
        _local_binding_roots,
        _validate_operation_dependencies,
    )
    from ds_codegen.compiled_wire_artifacts import (  # noqa: PLC0415
        _ARTIFACT_DIGEST_DOMAIN,
        COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION,
        COMPILED_WIRE_RENDERER_ABI,
    )
    from ds_codegen.runtime_artifacts import (  # noqa: PLC0415
        _WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN,
        WIRE_ARTIFACT_RENDERER_ABI,
        WIRE_RUNTIME_SCHEMA_VERSION,
    )
    from ds_codegen.runtime_bundles import (  # noqa: PLC0415
        _COMPILED_DOMAINS,
        _COMPILED_OPERATION_DEPENDENCIES,
    )
    from ds_codegen.runtime_contract import (  # noqa: PLC0415
        runtime_auxiliary_operation_bindings,
        runtime_operation_bindings,
    )

    if tuple(contracts) != runtime_bundle_versions():
        msg = "runtime ownership requires the complete exact contract matrix"
        raise ValueError(msg)
    root = source_root / "src" / "dsctl" / "generated"
    runtime_root = root / "wire_runtime"
    runtime = read_compiled_literals(
        runtime_root / "_manifest.py",
        {
            "WIRE_RUNTIME_SCHEMA_VERSION",
            "RENDERER_ABI",
            "CONTENT_DIGEST",
            "MODULES",
        },
    )
    _equal(
        runtime["WIRE_RUNTIME_SCHEMA_VERSION"],
        WIRE_RUNTIME_SCHEMA_VERSION,
        "wire runtime schema",
    )
    _equal(runtime["RENDERER_ABI"], WIRE_ARTIFACT_RENDERER_ABI, "wire runtime ABI")
    _artifact(runtime_root, runtime, domain=_WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN)
    compiled_root = root / "wire_programs"
    artifact = read_compiled_literals(
        compiled_root / "_manifest.py",
        {
            "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
            "RENDERER_ABI",
            "CONTENT_DIGEST",
            "WIRE_RUNTIME_CONTENT_DIGEST",
            "MODULES",
            "DOMAIN_MODULES",
            "SUPPORT_MODULES",
        },
    )
    _equal(
        artifact["COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION"],
        COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION,
        "compiled artifact schema",
    )
    _equal(
        artifact["RENDERER_ABI"], COMPILED_WIRE_RENDERER_ABI, "compiled artifact ABI"
    )
    _equal(
        artifact["WIRE_RUNTIME_CONTENT_DIGEST"],
        runtime["CONTENT_DIGEST"],
        "compiled artifact runtime binding",
    )
    modules = _artifact(compiled_root, artifact, domain=_ARTIFACT_DIGEST_DOMAIN)
    domains = _modules(artifact["DOMAIN_MODULES"])
    support = _modules(artifact["SUPPORT_MODULES"], allow_empty=True)
    _equal(
        domains,
        tuple(sorted(f"{item.name}.py" for item in _COMPILED_DOMAINS)),
        "compiled domain owners",
    )
    _equal(
        tuple(sorted(("__init__.py", *domains, *support))),
        modules,
        "compiled artifact domain/support inventory",
    )
    data = {
        definition.name: read_compiled_literals(
            compiled_root / f"{definition.name}.py",
            {
                definition.schema_constant,
                "TARGET_DS_VERSIONS",
                "PROFILES",
                "CODECS",
            },
        )
        for definition in _COMPILED_DOMAINS
    }
    for definition in _COMPILED_DOMAINS:
        _equal(
            data[definition.name][definition.schema_constant],
            definition.schema_version,
            f"{definition.name} schema",
        )
        _equal(
            data[definition.name]["TARGET_DS_VERSIONS"],
            runtime_bundle_versions(),
            f"{definition.name} target versions",
        )
        _equal(
            tuple(_mapping(data[definition.name]["PROFILES"])),
            runtime_bundle_versions(),
            f"{definition.name} profile inventory",
        )

    result: dict[str, frozenset[str]] = {}
    for version, contract in contracts.items():
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        dependencies = _validate_operation_dependencies(
            version, bindings, _COMPILED_OPERATION_DEPENDENCIES
        )
        legacy = contract.get("semantic_operations")
        if not isinstance(legacy, (list, tuple)) or not all(
            isinstance(item, str) for item in legacy
        ):
            msg = "runtime ownership legacy semantic roots are invalid"
            raise ValueError(msg)
        owned = set(legacy)
        source_owners: set[str] = set()
        for definition in _COMPILED_DOMAINS:
            selected = {
                name: binding
                for name, binding in bindings.items()
                if name in definition.semantic_operations
            }
            expected_sources, _types = _local_binding_roots(
                selected,
                all_bindings=bindings,
                operation_dependencies=dependencies,
            )
            if source_owners & expected_sources or owned & selected.keys():
                msg = "runtime ownership has duplicate compiled owners"
                raise ValueError(msg)
            source_owners.update(expected_sources)
            _profile(
                definition,
                data[definition.name],
                version=version,
                contract=contract,
                expected_sources=expected_sources,
            )
            owned.update(selected)
        result[version] = frozenset(owned)
    return MappingProxyType(result)


def load_wheel_runtime_ownership(wheel: Path) -> Mapping[str, frozenset[str]]:
    """Prove the wheel's own generated bytes, never the checkout's installation."""
    from live_gate.exact_profile_read_corpus import _load_contract  # noqa: PLC0415

    manifests = {
        f"dsctl/generated/versions/ds_{version.replace('.', '_')}/_manifest.py"
        for version in runtime_bundle_versions()
    }
    artifact_prefixes = (
        "dsctl/generated/wire_programs/",
        "dsctl/generated/wire_runtime/",
    )
    with (
        zipfile.ZipFile(wheel) as archive,
        tempfile.TemporaryDirectory(prefix="dsctl-wheel-ownership-") as temporary,
    ):
        root = Path(temporary)
        selected: set[str] = set()
        for info in archive.infolist():
            name = info.filename
            if name not in manifests and not (
                name.startswith(artifact_prefixes) and name.endswith(".py")
            ):
                continue
            path = PurePosixPath(name)
            if (
                name in selected
                or path.is_absolute()
                or path.as_posix() != name
                or ".." in path.parts
                or info.is_dir()
                or ((info.external_attr >> 16) & 0o170000) == 0o120000
            ):
                message = "wheel runtime ownership member is duplicate or unsafe"
                raise ValueError(message)
            selected.add(name)
            target = root / "src" / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(info))
        if not manifests <= selected:
            message = "wheel runtime ownership exact manifests are incomplete"
            raise ValueError(message)
        contracts = {
            version: _load_contract(root, version=version)
            for version in runtime_bundle_versions()
        }
        return load_current_runtime_ownership(root, contracts=contracts)


def _profile(
    definition: CompiledDomainDefinition,
    data: Mapping[str, object],
    *,
    version: str,
    contract: Mapping[str, object],
    expected_sources: set[str],
) -> None:
    label = f"runtime ownership {definition.name} {version}"
    profile = _mapping(_mapping(data["PROFILES"])[version])
    _equal(
        set(profile),
        {"status", "source", "recipe_id", "programs", "profile_digest"},
        f"{label} profile fields",
    )
    _equal(
        profile["profile_digest"],
        _digest(
            {
                "schema_version": 2,
                **{
                    key: value
                    for key, value in profile.items()
                    if key != "profile_digest"
                },
            }
        ),
        f"{label} profile digest",
    )
    _equal(
        profile["source"],
        {
            "tag": contract.get("source_tag"),
            "commit": contract.get("source_commit"),
            "tree": contract.get("source_tree"),
            "contract_digest": contract.get("source_contract_digest"),
        },
        f"{label} source identity",
    )
    _sha256(
        contract.get("source_contract_digest"), label=f"{label} source contract digest"
    )
    absent = version in definition.absent_versions
    _equal(
        profile["status"],
        "upstream_absent" if absent else "supported",
        f"{label} status",
    )
    programs = _mapping(profile["programs"])
    primitives = {
        item.name: item
        for item in definition.primitives
        if version not in item.absent_versions and not absent
    }
    _equal(set(programs), set(primitives), f"{label} primitive inventory")
    if absent:
        _equal(profile["recipe_id"], None, f"{label} absent recipe")
        _equal(expected_sources, set(), f"{label} absent sources")
        return
    codecs = _mapping(data["CODECS"])
    sources: set[str] = set()
    selected_codecs: dict[str, str] = {}
    for name, primitive in primitives.items():
        program = _mapping(programs[name])
        source, codec_name = _program(
            definition, primitive, program, codecs, label=label, version=version
        )
        if source in sources:
            msg = f"{label} source operation is duplicated"
            raise ValueError(msg)
        sources.add(source)
        selected_codecs[name] = codec_name
    _equal(sources, expected_sources, f"{label} local source closure")
    _equal(
        profile["recipe_id"],
        definition.recipe_policy(selected_codecs),
        f"{label} recipe",
    )


def _program(
    definition: CompiledDomainDefinition,
    primitive: CompiledPrimitive,
    program: Mapping[str, object],
    codecs: Mapping[str, object],
    *,
    label: str,
    version: str | None = None,
) -> tuple[str, str]:
    from ds_codegen.compiled_domains import (  # noqa: PLC0415
        _compiled_request_schema_name,
        _request_field_records,
        _request_path_encoding,
    )
    from ds_codegen.ir import OperationSpec  # noqa: PLC0415

    label = f"{label} {primitive.name}"
    _equal(set(program), _PROGRAM_KEYS, f"{label} program fields")
    source, codec_name = program["source_operation"], program["codec"]
    if not isinstance(source, str) or not isinstance(codec_name, str):
        msg = f"{label} program identity is invalid"
        raise TypeError(msg)
    controller, separator, method = source.partition(".")
    if not separator:
        msg = f"{label} source operation is invalid"
        raise ValueError(msg)
    codec = _mapping(codecs.get(codec_name))
    expected_codec_keys = _CODEC_KEYS
    if codec.get("channel") == "multipart":
        expected_codec_keys |= {"file_fields"}
    if primitive.response_transport == "binary":
        expected_codec_keys |= {"response_transport"}
    _equal(set(codec), expected_codec_keys, f"{label} codec fields")
    if primitive.response_transport == "binary":
        _equal(codec["response_transport"], "binary", f"{label} response transport")
        _equal(
            (
                codec["method"],
                codec["response"],
                codec["response_schema_digest"],
                codec["capture"],
                codec["response_projection"],
            ),
            ("GET", None, None, None, "direct"),
            f"{label} binary response contract",
        )
    projection = codec["response_projection"]
    if not isinstance(projection, str) or projection not in {
        "direct",
        "status_data",
        "single_data",
    }:
        msg = f"{label} response projection is unsupported"
        raise ValueError(msg)
    _equal(
        program["result_envelope"],
        primitive.result_envelope,
        f"{label} result envelope",
    )
    for field in (
        "codec_digest",
        "request_schema_digest",
        "response_digest",
        "program_digest",
    ):
        _sha256(program[field], label=f"{label} {field}")
    if program["response_schema_digest"] is not None:
        _sha256(
            program["response_schema_digest"], label=f"{label} response_schema_digest"
        )
    _equal(
        program["request_schema_digest"],
        codec["request_schema_digest"],
        f"{label} request schema digest",
    )
    _equal(
        program["response_schema_digest"],
        codec["response_schema_digest"],
        f"{label} response schema digest",
    )
    _equal(
        program["program_digest"],
        _digest(
            {
                "schema_version": 4,
                "codec_record": codec,
                **{
                    key: value
                    for key, value in program.items()
                    if key != "program_digest"
                },
            }
        ),
        f"{label} program digest",
    )
    candidates = [
        request
        for request in primitive.requests
        if (
            codec["method"],
            codec["path"],
            codec["channel"],
            codec["params"],
            codec["path_fields"],
            codec["fields"],
            codec["path_encoding"],
            codec.get("file_fields", []),
        )
        == (
            request.method,
            request.path,
            request.channel,
            _compiled_request_schema_name(request, str(codec["request_schema_digest"])),
            list(request.path_fields),
            _request_field_records(request),
            _request_path_encoding(request),
            list(request.file_fields),
        )
        and (version is None or request.versions is None or version in request.versions)
    ]
    if len(candidates) != 1:
        msg = f"{label} request epoch is not uniquely reviewed"
        raise ValueError(msg)
    # Classifiers may distinguish methods on the same source coordinate. Only
    # the uniquely reviewed epoch supplies that transport, never candidate text.
    request = candidates[0]
    coordinate = OperationSpec(
        operation_id=source,
        controller=controller,
        method_name=method,
        api_group="",
        http_method=request.method,
        path=request.path,
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="",
        inferred_return_type=None,
        logical_return_type="",
        response_projection="direct",
        parameters=[],
    )
    _equal(
        definition.classify_operation(coordinate),
        primitive.name,
        f"{label} source primitive",
    )
    return source, codec_name


def _artifact(
    root: Path, manifest: Mapping[str, object], *, domain: bytes
) -> tuple[str, ...]:
    modules = _modules(manifest["MODULES"])
    actual = tuple(
        sorted(path.relative_to(root).as_posix() for path in root.rglob("*.py"))
    )
    _equal(
        actual,
        tuple(sorted((*modules, "_manifest.py"))),
        f"{root.name} artifact inventory",
    )
    digest = hashlib.sha256(domain)
    resolved_root = root.resolve()
    for module in modules:
        path = root / module
        if not path.resolve().is_relative_to(resolved_root):
            msg = f"{root.name} artifact path escapes candidate root"
            raise ValueError(msg)
        digest.update(module.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    _equal(
        manifest["CONTENT_DIGEST"],
        f"sha256:{digest.hexdigest()}",
        f"{root.name} artifact content digest",
    )
    return modules


def _modules(value: object, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, tuple) or (not value and not allow_empty):
        msg = "runtime ownership module inventory must be a tuple"
        raise ValueError(msg)
    modules: list[str] = []
    for item in value:
        if not isinstance(item, str):
            msg = "runtime ownership module path must be text"
            raise TypeError(msg)
        path = PurePosixPath(item)
        if (
            path.is_absolute()
            or path.as_posix() != item
            or path.suffix != ".py"
            or item == "_manifest.py"
            or not all(_MODULE.fullmatch(part) for part in path.with_suffix("").parts)
        ):
            msg = "runtime ownership module path is unsafe"
            raise ValueError(msg)
        modules.append(item)
    _equal(
        modules, sorted(set(modules)), "runtime ownership canonical module inventory"
    )
    return tuple(modules)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        msg = "runtime ownership record must be a string-keyed mapping"
        raise ValueError(msg)
    return value


def _digest(value: Mapping[str, object]) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest()
    )


def _sha256(value: object, *, label: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        msg = f"{label} is not a SHA-256 digest"
        raise ValueError(msg)


def _equal(actual: object, expected: object, label: str) -> None:
    if actual != expected or (type(expected) is int and type(actual) is not int):
        msg = f"{label} does not match current runtime ownership"
        raise ValueError(msg)


__all__ = ["load_current_runtime_ownership", "load_wheel_runtime_ownership"]
