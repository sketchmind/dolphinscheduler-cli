"""Render compiled runtime support and bind exact profile/recipe identities."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, cast

from ds_codegen.contract_inputs import canonical_json_digest, natural_version_key
from ds_codegen.render.package.runtime_renderer import (
    write_base_operations_module,
    write_compiled_schema_module,
    write_model_base_module,
)
from ds_codegen.render.package.surface_renderer import write_recursive_package_inits
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)
from runtime_bundle_manifest import runtime_bundle_package_slug

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.compatibility_impact import ReviewedBinding
    from ds_codegen.runtime_bundles import RuntimeBundle

RUNTIME_ARTIFACT_SCHEMA_VERSION = 4
WIRE_RUNTIME_SCHEMA_VERSION = 1
WIRE_ARTIFACT_RENDERER_ABI = 3
_FINGERPRINT_AXES = ("source", "effective_wire", "consumed_projection", "preservation")
_PYTHON_MODULE = re.compile(r"[a-z_][a-z0-9_]*\Z")
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN = b"dsctl-wire-runtime-v1\0"


@dataclass(frozen=True)
class ExactArtifactPackage:
    """Semantic identity and planned exchanges for one exact package."""

    version: str
    package_slug: str
    profile_digest: str
    recipe_digest: str


@dataclass(frozen=True)
class RenderedExactArtifact:
    """Final digests copied into an exact bundle manifest."""

    version: str
    profile_digest: str
    recipe_digest: str
    binding_digest: str


def render_runtime_artifacts(
    bundles: tuple[RuntimeBundle, ...],
    output_root: Path,
    *,
    profile_data: Mapping[str, object],
) -> dict[str, RenderedExactArtifact]:
    """Validate compiled ownership, then publish content-bound exact identities."""
    versions = [bundle.spec.version for bundle in bundles]
    if len(versions) != len(set(versions)):
        message = "runtime artifacts contain duplicate exact bundles"
        raise ValueError(message)
    profiles = _mapping(profile_data.get("profiles"), label="profile data.profiles")
    packages: list[ExactArtifactPackage] = []
    for bundle in sorted(
        bundles, key=lambda item: natural_version_key(item.spec.version)
    ):
        version = bundle.spec.version
        if bundle.snapshot.operations:
            message = f"DS {version} has executable operations outside compiled domains"
            raise ValueError(message)
        profile = _mapping(
            profiles.get(version), label=f"profile data.profiles.{version}"
        )
        packages.append(
            ExactArtifactPackage(
                version=version,
                package_slug=runtime_bundle_package_slug(version),
                profile_digest=profile_semantic_digest(profile),
                recipe_digest=recipe_semantic_digest(
                    version, profile_data=profile_data
                ),
            )
        )
    wire_runtime_content_digest = _write_shared_runtime_support(output_root)
    rendered: dict[str, RenderedExactArtifact] = {}
    for package in packages:
        exact_binding_digest = binding_digest(
            package.version,
            wire_runtime_content_digest=wire_runtime_content_digest,
            profile_digest=package.profile_digest,
            recipe_digest=package.recipe_digest,
        )
        _write_exact_artifact_manifest(
            output_root / "generated" / "versions" / package.package_slug,
            package,
            binding_digest_value=exact_binding_digest,
            wire_runtime_content_digest=wire_runtime_content_digest,
        )
        rendered[package.version] = RenderedExactArtifact(
            version=package.version,
            profile_digest=package.profile_digest,
            recipe_digest=package.recipe_digest,
            binding_digest=exact_binding_digest,
        )
    return rendered


def profile_semantic_digest(profile: Mapping[str, object]) -> str:
    """Hash the complete compiled exact profile without another truth model."""
    return canonical_json_digest(profile)


def recipe_semantic_digest(
    version: str,
    *,
    profile_data: Mapping[str, object],
) -> str:
    """Hash executable runtime and auxiliary recipe facts for one release."""
    return canonical_json_digest(
        {
            "schema_version": 1,
            "ds_version": version,
            "consumers": list(_consumer_records(version, profile_data=profile_data)),
        }
    )


def binding_digest(
    version: str,
    *,
    wire_runtime_content_digest: str,
    profile_digest: str,
    recipe_digest: str,
) -> str:
    """Hash resolved exact memberships using the loader's schema-4 protocol."""
    for label, value in (
        ("wire runtime content", wire_runtime_content_digest),
        ("profile", profile_digest),
        ("recipe", recipe_digest),
    ):
        _digest(value, label=f"DS {version} {label} digest")
    return canonical_json_digest(
        {
            "schema_version": RUNTIME_ARTIFACT_SCHEMA_VERSION,
            "renderer_abi": WIRE_ARTIFACT_RENDERER_ABI,
            "ds_version": version,
            "wire_runtime_content_digest": wire_runtime_content_digest,
            "profile_digest": profile_digest,
            "recipe_digest": recipe_digest,
            # Retain the schema-4 identity protocol after family retirement.
            "family_bindings": {},
        }
    )


def _consumer_records(
    version: str,
    *,
    profile_data: Mapping[str, object],
) -> tuple[dict[str, object], ...]:
    profiles = _mapping(profile_data.get("profiles"), label="profile data.profiles")
    profile = _mapping(profiles.get(version), label=f"profile data.profiles.{version}")
    decisions = _mapping(
        profile.get("build_decisions"),
        label=f"profile data.profiles.{version}.build_decisions",
    )
    runtime_bindings = runtime_operation_bindings(version)
    auxiliary_bindings = runtime_auxiliary_operation_bindings(version)
    overlap = set(runtime_bindings) & set(auxiliary_bindings)
    if overlap:
        message = (
            f"DS {version} repeats runtime and auxiliary recipes: "
            f"{', '.join(sorted(overlap))}"
        )
        raise ValueError(message)

    records: list[dict[str, object]] = []
    for kind, bindings in (
        ("runtime", runtime_bindings),
        ("auxiliary", auxiliary_bindings),
    ):
        for semantic_operation, reviewed_binding in sorted(bindings.items()):
            profile_fingerprints: dict[str, str] | None
            if kind == "runtime":
                profile_fingerprints = _runtime_consumer_fingerprints(
                    decisions,
                    version=version,
                    semantic_operation=semantic_operation,
                    binding=reviewed_binding,
                )
            else:
                if semantic_operation in decisions:
                    message = (
                        f"DS {version} auxiliary recipe {semantic_operation} "
                        "unexpectedly has a profile decision"
                    )
                    raise ValueError(message)
                profile_fingerprints = None
            records.append(
                {
                    "kind": kind,
                    "semantic_operation": semantic_operation,
                    "source_operations": list(reviewed_binding.source_operations),
                    "type_closure": [
                        {"surface": item.surface, "key": item.key}
                        for item in sorted(
                            reviewed_binding.type_closure,
                            key=lambda item: (item.surface, item.key),
                        )
                    ],
                    "selector_semantics": _canonical_selectors(reviewed_binding),
                    "profile_fingerprints": profile_fingerprints,
                }
            )
    return tuple(
        sorted(
            records,
            key=lambda item: (
                cast("str", item["kind"]),
                cast("str", item["semantic_operation"]),
            ),
        )
    )


def _runtime_consumer_fingerprints(
    decisions: Mapping[str, object],
    *,
    version: str,
    semantic_operation: str,
    binding: ReviewedBinding,
) -> dict[str, str]:
    decision = _mapping(
        decisions.get(semantic_operation),
        label=f"profile data {version}:{semantic_operation}",
    )
    if decision.get("semantic_operation") != semantic_operation:
        message = f"profile data {version}:{semantic_operation} identity is stale"
        raise ValueError(message)
    expected_sources = list(binding.source_operations)
    expected_types = [
        {"surface": item.surface, "key": item.key} for item in binding.type_closure
    ]
    expected_selectors = [_selector_record(item) for item in binding.selector_semantics]
    if decision.get("source_operations") != expected_sources:
        message = f"profile data {version}:{semantic_operation} sources are stale"
        raise ValueError(message)
    if decision.get("type_closure") != expected_types:
        message = f"profile data {version}:{semantic_operation} types are stale"
        raise ValueError(message)
    if decision.get("selector_semantics") != expected_selectors:
        message = f"profile data {version}:{semantic_operation} selectors are stale"
        raise ValueError(message)
    fingerprints = _mapping(
        decision.get("fingerprints"),
        label=f"profile data {version}:{semantic_operation}.fingerprints",
    )
    if set(fingerprints) != set(_FINGERPRINT_AXES):
        message = (
            f"profile data {version}:{semantic_operation} has invalid fingerprint axes"
        )
        raise ValueError(message)
    return {
        axis: _digest(
            fingerprints[axis],
            label=f"profile data {version}:{semantic_operation}.{axis}",
        )
        for axis in _FINGERPRINT_AXES
    }


def _canonical_selectors(binding: ReviewedBinding) -> list[dict[str, object]]:
    selectors = [_selector_record(item) for item in binding.selector_semantics]
    return sorted(
        selectors,
        key=lambda item: json.dumps(
            item,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def _selector_record(value: Any) -> dict[str, object]:
    return {
        "resource": value.resource,
        "consumed_selectors": list(value.consumed_selectors),
        "exposed_identities": list(value.exposed_identities),
        "native_identity": value.native_identity,
        "resolution": value.resolution,
        "parent_identity": value.parent_identity,
    }


def _write_shared_runtime_support(
    output_root: Path,
) -> str:
    package_root = output_root / "generated" / "wire_runtime"
    if package_root.exists():
        shutil.rmtree(package_root)
    package_root.mkdir(parents=True)
    _write_namespace_init(package_root.parent)
    _write_namespace_init(package_root)
    package_exports: dict[tuple[str, ...], dict[str, list[str]]] = defaultdict(dict)
    write_compiled_schema_module(package_root)
    write_model_base_module(
        package_root,
        base_contract_model_name="BaseContractModel",
        base_view_model_name="BaseViewModel",
        base_entity_model_name="BaseEntityModel",
    )
    write_base_operations_module(
        package_root,
        package_exports,
        requests_base_class_name="BaseRequestsClient",
        base_params_model_name="BaseParamsModel",
    )
    write_recursive_package_inits(package_root, package_exports)
    modules = tuple(
        path.relative_to(package_root).as_posix()
        for path in sorted(package_root.rglob("*.py"))
    )
    content_digest = _module_content_digest(
        package_root,
        modules,
        domain=_WIRE_RUNTIME_CONTENT_DIGEST_DOMAIN,
        label="wire runtime",
    )
    _write_wire_runtime_manifest(
        package_root,
        modules=modules,
        content_digest=content_digest,
    )
    return content_digest


def _write_wire_runtime_manifest(
    package_root: Path,
    *,
    modules: tuple[str, ...],
    content_digest: str,
) -> None:
    lines = [
        "from __future__ import annotations",
        "",
        "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
        f"WIRE_RUNTIME_SCHEMA_VERSION = {WIRE_RUNTIME_SCHEMA_VERSION}",
        f"RENDERER_ABI = {WIRE_ARTIFACT_RENDERER_ABI}",
        f"CONTENT_DIGEST = {json.dumps(content_digest)}",
        "MODULES = (",
        *(f"    {json.dumps(module)}," for module in modules),
        ")",
        "",
    ]
    (package_root / "_manifest.py").write_text("\n".join(lines), encoding="utf-8")


def _write_exact_artifact_manifest(
    package_root: Path,
    package: ExactArtifactPackage,
    *,
    binding_digest_value: str,
    wire_runtime_content_digest: str,
) -> None:
    lines = [
        "from __future__ import annotations",
        "",
        "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
        f"RUNTIME_ARTIFACT_SCHEMA_VERSION = {RUNTIME_ARTIFACT_SCHEMA_VERSION}",
        f"RENDERER_ABI = {WIRE_ARTIFACT_RENDERER_ABI}",
        f"WIRE_RUNTIME_CONTENT_DIGEST = {json.dumps(wire_runtime_content_digest)}",
        f"PROFILE_DIGEST = {json.dumps(package.profile_digest)}",
        f"RECIPE_DIGEST = {json.dumps(package.recipe_digest)}",
        f"BINDING_DIGEST = {json.dumps(binding_digest_value)}",
        "FAMILY_IDS = ()",
        "",
    ]
    package_root.mkdir(parents=True, exist_ok=True)
    (package_root / "_artifact.py").write_text("\n".join(lines), encoding="utf-8")


def _module_content_digest(
    package_root: Path,
    modules: tuple[str, ...],
    *,
    domain: bytes,
    label: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    for module in modules:
        relative_path = _safe_published_module(module, label=label)
        module_path = package_root.joinpath(*relative_path.parts)
        if not module_path.is_file():
            message = f"{label} module is missing: {module}"
            raise ValueError(message)
        digest.update(module.encode("utf-8"))
        digest.update(b"\0")
        digest.update(module_path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def _write_namespace_init(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    init_path = path / "__init__.py"
    if not init_path.exists():
        init_path.write_text("")


def _safe_published_module(value: str, *, label: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or path.suffix != ".py"
        or ".." in path.parts
        or not all(
            _PYTHON_MODULE.fullmatch(part) for part in path.with_suffix("").parts
        )
    ):
        message = f"{label} has unsafe published module {value!r}"
        raise ValueError(message)
    return path


def _mapping(value: object, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        message = f"{label} must be an object"
        raise TypeError(message)
    return value


def _text(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        message = f"{label} must be nonempty text"
        raise TypeError(message)
    return value


def _digest(value: object, *, label: str) -> str:
    text = _text(value, label=label)
    if _SHA256.fullmatch(text) is None:
        message = f"{label} must be a sha256 fingerprint"
        raise ValueError(message)
    return text
