"""Build the complete set of generated runtime wire bundles."""

from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_access_tokens import ACCESS_TOKEN_COMPILED_DOMAIN
from ds_codegen.compiled_alert_groups import ALERT_GROUP_COMPILED_DOMAIN
from ds_codegen.compiled_alert_plugins import ALERT_PLUGIN_COMPILED_DOMAIN
from ds_codegen.compiled_audit import AUDIT_COMPILED_DOMAIN
from ds_codegen.compiled_clusters import CLUSTER_COMPILED_DOMAIN
from ds_codegen.compiled_datasources import DATASOURCE_COMPILED_DOMAIN
from ds_codegen.compiled_domains import (
    CompiledDependencyDeclaration,
    CompiledOperationDependency,
    compile_domains,
)
from ds_codegen.compiled_environments import ENVIRONMENT_COMPILED_DOMAIN
from ds_codegen.compiled_monitor import MONITOR_COMPILED_DOMAIN
from ds_codegen.compiled_namespaces import NAMESPACE_COMPILED_DOMAIN
from ds_codegen.compiled_project_parameters import PROJECT_PARAMETER_COMPILED_DOMAIN
from ds_codegen.compiled_project_preferences import PROJECT_PREFERENCE_COMPILED_DOMAIN
from ds_codegen.compiled_project_worker_groups import (
    PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
)
from ds_codegen.compiled_projects import PROJECT_COMPILED_DOMAIN
from ds_codegen.compiled_queues import QUEUE_COMPILED_DOMAIN
from ds_codegen.compiled_resources import RESOURCE_COMPILED_DOMAIN
from ds_codegen.compiled_task_groups import TASK_GROUP_COMPILED_DOMAIN
from ds_codegen.compiled_task_types import TASK_TYPE_COMPILED_DOMAIN
from ds_codegen.compiled_tenants import TENANT_COMPILED_DOMAIN
from ds_codegen.compiled_users import USER_COMPILED_DOMAIN
from ds_codegen.compiled_wire_artifacts import render_compiled_wire_artifact
from ds_codegen.compiled_worker_groups import WORKER_GROUP_COMPILED_DOMAIN
from ds_codegen.compiled_workflow_runtime import WORKFLOW_RUNTIME
from ds_codegen.conformance_bundles import write_conformance_bundle_data
from ds_codegen.contract_inputs import (
    ContractInput,
    ExactGitSourceIdentity,
    LoadedContract,
    build_source_provenance,
    contract_extractor_fingerprint,
    contract_snapshot_digest,
    load_contract_input,
    require_exact_git_source_identity,
    require_exact_provenance,
    write_contract_snapshot_document,
)
from ds_codegen.datasource_profiles import (
    DataSourceProfile,
    compile_datasource_profile,
    load_datasource_profile_reviews,
    write_datasource_profiles,
)
from ds_codegen.profile_ledger import reviewed_profile_versions, select_exact_versions
from ds_codegen.render.package import write_generated_package
from ds_codegen.runtime_artifacts import (
    RenderedExactArtifact,
    render_runtime_artifacts,
)
from ds_codegen.runtime_contract import (
    PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,
    PROJECT_PREFERENCE_READ_VERSIONS,
    configured_runtime_contract_slice,
    runtime_semantic_operations,
)
from ds_codegen.runtime_instance_contract import (
    validate_schedule_environment_sources,
    write_runtime_instance_profiles,
)
from ds_codegen.snapshot_resolution import SnapshotTypeResolver
from ds_codegen.task_definition_cleanup_contract import (
    write_task_definition_cleanup_profiles,
)
from ds_codegen.task_definition_contract import write_task_definition_profiles
from ds_codegen.task_profiles import (
    DEFAULT_TASK_PROFILE_FACTS,
    DEFAULT_TASK_PROFILE_REVIEWS,
    load_task_profile_document,
    write_task_profile_data,
)
from ds_codegen.version_discovery import (
    DiscoveryProfile,
    compile_discovery_profile,
    write_version_discovery,
)
from ds_codegen.version_profiles import (
    compile_version_profile_data,
    load_version_profile_ledger,
    write_version_profiles,
)
from ds_codegen.workflow_contract import write_workflow_profiles
from dsctl.cli_surface import stable_leaf_actions
from runtime_bundle_manifest import (
    DEFAULT_RUNTIME_BUNDLE_MANIFEST,
    RuntimeBundleSelection,
    RuntimeBundleSpec,
    load_runtime_bundle_specs,
    natural_version_key,
    runtime_bundle_package_slug,
)

if TYPE_CHECKING:
    from ds_codegen.ir import ContractSnapshot

RuntimeBundleInputKind = Literal["snapshot", "source"]
RuntimeSnapshotMode = Literal["prefer", "require", "source"]
RUNTIME_SNAPSHOT_MODES = frozenset({"prefer", "require", "source"})
DEFAULT_RUNTIME_SNAPSHOT_DIR = Path("build/ds_contract/snapshots-v2")
_COMPILED_DOMAINS = (
    CLUSTER_COMPILED_DOMAIN,
    ENVIRONMENT_COMPILED_DOMAIN,
    WORKER_GROUP_COMPILED_DOMAIN,
    ALERT_GROUP_COMPILED_DOMAIN,
    TENANT_COMPILED_DOMAIN,
    QUEUE_COMPILED_DOMAIN,
    ALERT_PLUGIN_COMPILED_DOMAIN,
    ACCESS_TOKEN_COMPILED_DOMAIN,
    NAMESPACE_COMPILED_DOMAIN,
    TASK_GROUP_COMPILED_DOMAIN,
    USER_COMPILED_DOMAIN,
    AUDIT_COMPILED_DOMAIN,
    TASK_TYPE_COMPILED_DOMAIN,
    MONITOR_COMPILED_DOMAIN,
    PROJECT_PARAMETER_COMPILED_DOMAIN,
    PROJECT_PREFERENCE_COMPILED_DOMAIN,
    PROJECT_WORKER_GROUP_COMPILED_DOMAIN,
    PROJECT_COMPILED_DOMAIN,
    DATASOURCE_COMPILED_DOMAIN,
    WORKFLOW_RUNTIME,
    RESOURCE_COMPILED_DOMAIN,
)

# Runtime domain composition delegates these reads through the provider's port.
# Complete source/selector/preservation evidence stays in the reviewed bindings.
_COMPILED_OPERATION_DEPENDENCIES: dict[str, CompiledDependencyDeclaration] = {
    "tenant.create": ("queue.page",),
    "tenant.update": ("queue.page",),
    "user.create": ("tenant.page",),
    "user.update": ("tenant.page",),
    "access-token.create": (
        CompiledOperationDependency(
            providers=("user.get",),
            versions=frozenset(REVIEWED_DS_VERSIONS) - {"3.4.3"},
        ),
        CompiledOperationDependency(
            providers=("user.identity",), versions=frozenset({"3.4.3"})
        ),
    ),
    "access-token.update": (
        CompiledOperationDependency(
            providers=("user.get",),
            versions=frozenset(REVIEWED_DS_VERSIONS) - {"3.4.3"},
        ),
        CompiledOperationDependency(
            providers=("user.identity",), versions=frozenset({"3.4.3"})
        ),
    ),
    "access-token.generate": (
        CompiledOperationDependency(
            providers=("user.get",),
            versions=frozenset(REVIEWED_DS_VERSIONS) - {"3.4.3"},
        ),
        CompiledOperationDependency(
            providers=("user.identity",), versions=frozenset({"3.4.3"})
        ),
    ),
    "project-parameter.page": ("project.get",),
    "project-parameter.get": ("project.get",),
    "project-parameter.create": ("project.get",),
    "project-parameter.update": ("project.get",),
    "project-parameter.delete": ("project.get",),
    "project-preference.get": ("project.get",),
    "project-preference.update": ("project.get",),
    "project-preference.enable": ("project.get",),
    "project-preference.disable": ("project.get",),
    "project-worker-group.page": ("project.get",),
    "project-worker-group.set": ("project.get",),
    "project-worker-group.clear": ("project.get",),
    "workflow.page": ("project.get",),
    "workflow.get": ("project.get",),
    "workflow.delete": ("project.get",),
    "workflow.online": ("project.get",),
    "workflow.offline": ("project.get",),
    "workflow.create": ("project.get", "datasource.get"),
    "workflow.edit": ("project.get", "datasource.get"),
    "workflow.describe": ("project.get",),
    "workflow.digest": ("project.get",),
    "workflow.export": ("project.get",),
    "workflow.inspect": ("project.get",),
    "workflow.lineage.list": ("project.get",),
    "workflow.lineage.get": ("project.get",),
    "workflow.lineage.dependent-tasks": ("project.get",),
    "workflow-instance.list": ("project.get",),
    "workflow-instance.get": ("project.get",),
    "workflow-instance.parent": ("project.get",),
    "workflow-instance.digest": ("project.get",),
    "workflow-instance.watch": ("project.get",),
    "workflow-instance.stop": ("project.get",),
    "workflow-instance.rerun": ("project.get",),
    "workflow-instance.recover-failed": ("project.get",),
    "workflow-instance.export": ("project.get",),
    "workflow-instance.edit": (
        "project.get",
        "datasource.get",
    ),
    "workflow-instance.execute-task": ("project.get",),
    "task-instance.list": ("project.get",),
    "task-instance.get": ("project.get",),
    "task-instance.watch": ("project.get",),
    "task-instance.sub-workflow": ("project.get",),
    "task-instance.force-success": ("project.get",),
    "task-instance.savepoint": ("project.get",),
    "task-instance.stop": ("project.get",),
    "task.list": ("project.get",),
    "task.get": ("project.get",),
    "task.update": ("project.get",),
    "schedule.create": CompiledOperationDependency(
        providers=(PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,),
        versions=PROJECT_PREFERENCE_READ_VERSIONS,
    ),
    "schedule.explain": CompiledOperationDependency(
        providers=(PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,),
        versions=PROJECT_PREFERENCE_READ_VERSIONS,
    ),
    "workflow.run": (
        CompiledOperationDependency(("project.get",), frozenset(REVIEWED_DS_VERSIONS)),
        CompiledOperationDependency(
            (PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,),
            PROJECT_PREFERENCE_READ_VERSIONS,
        ),
    ),
    "workflow.backfill": (
        CompiledOperationDependency(("project.get",), frozenset(REVIEWED_DS_VERSIONS)),
        CompiledOperationDependency(
            (PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,),
            PROJECT_PREFERENCE_READ_VERSIONS,
        ),
    ),
    "workflow.run-task": (
        CompiledOperationDependency(("project.get",), frozenset(REVIEWED_DS_VERSIONS)),
        CompiledOperationDependency(
            (PROJECT_PREFERENCE_READ_SEMANTIC_OPERATION,),
            PROJECT_PREFERENCE_READ_VERSIONS,
        ),
    ),
}


@dataclass(frozen=True)
class RuntimeBundleMetadata:
    """Auditable identity embedded beside one rendered wire bundle."""

    version: str
    selection: RuntimeBundleSelection
    semantic_operations: tuple[str, ...]
    source_tag: str
    source_commit: str
    source_tree: str
    source_contract_digest: str
    rendered_contract_digest: str
    operation_count: int
    enum_count: int
    dto_count: int
    model_count: int


@dataclass(frozen=True)
class RuntimeBundle:
    """One validated source contract compiled for runtime generation."""

    spec: RuntimeBundleSpec
    snapshot: ContractSnapshot
    metadata: RuntimeBundleMetadata
    input_kind: RuntimeBundleInputKind = "source"
    snapshot_fallback_reason: str | None = None
    discovery_profile: DiscoveryProfile | None = None


@dataclass(frozen=True)
class PreparedRuntimeBundles:
    """Full validated contracts paired with their compiled runtime bundles."""

    bundles: tuple[RuntimeBundle, ...]
    loaded_contracts: tuple[LoadedContract, ...]


@dataclass(frozen=True)
class RuntimeBundleSourceIdentity:
    """Extraction-free identity used to key freshness work."""

    version: str
    selection: RuntimeBundleSelection
    source_tag: str
    source_commit: str
    source_tree: str


class RuntimeSnapshotCacheError(ValueError):
    """Raised when a required exact runtime snapshot cannot be trusted."""


def load_runtime_bundle_source_identities(
    repo_root: Path,
    manifest_path: Path | None = None,
) -> tuple[RuntimeBundleSourceIdentity, ...]:
    """Validate exact sources and return their identity without extraction."""
    identities: list[RuntimeBundleSourceIdentity] = []
    for spec in load_runtime_bundle_specs(repo_root, manifest_path):
        identity = require_exact_git_source_identity(
            spec.source_root,
            label=spec.version,
        )
        identities.append(
            RuntimeBundleSourceIdentity(
                version=spec.version,
                selection=spec.selection,
                source_tag=identity.tag,
                source_commit=identity.commit,
                source_tree=identity.tree,
            )
        )
    return tuple(identities)


def compile_runtime_bundle(
    spec: RuntimeBundleSpec,
    loaded: LoadedContract,
) -> RuntimeBundle:
    """Compile one exact loaded contract into its configured runtime surface."""
    if loaded.label != spec.version or loaded.snapshot.ds_version != spec.version:
        message = (
            f"runtime bundle {spec.version} does not match loaded contract "
            f"{loaded.label}/{loaded.snapshot.ds_version}"
        )
        raise ValueError(message)
    require_exact_provenance(loaded.provenance, label=spec.version)
    source_digest = contract_snapshot_digest(loaded.snapshot)
    if loaded.provenance.get("contract_digest") != source_digest:
        message = f"runtime bundle {spec.version} source contract digest is stale"
        raise ValueError(message)
    resolver = SnapshotTypeResolver.compile(loaded.snapshot)

    if spec.selection == "full":
        snapshot = loaded.snapshot
        semantic_operations: tuple[str, ...] = ()
    else:
        snapshot = configured_runtime_contract_slice(
            loaded.snapshot,
            resolver=resolver,
        )
        semantic_operations = runtime_semantic_operations(spec.version)

    source_tag, source_commit, source_tree = _exact_git_identity(loaded)
    return RuntimeBundle(
        spec=spec,
        snapshot=snapshot,
        metadata=RuntimeBundleMetadata(
            version=spec.version,
            selection=spec.selection,
            semantic_operations=semantic_operations,
            source_tag=source_tag,
            source_commit=source_commit,
            source_tree=source_tree,
            source_contract_digest=source_digest,
            rendered_contract_digest=contract_snapshot_digest(snapshot),
            operation_count=snapshot.operation_count,
            enum_count=snapshot.enum_count,
            dto_count=snapshot.dto_count,
            model_count=snapshot.model_count,
        ),
    )


def load_runtime_bundles(
    repo_root: Path,
    manifest_path: Path | None = None,
    *,
    snapshot_dir: Path | None = None,
    snapshot_mode: RuntimeSnapshotMode = "source",
) -> tuple[RuntimeBundle, ...]:
    """Load exact bundles from source or a source-verified snapshot cache."""
    return load_prepared_runtime_bundles(
        repo_root,
        manifest_path,
        snapshot_dir=snapshot_dir,
        snapshot_mode=snapshot_mode,
    ).bundles


def load_prepared_runtime_bundles(
    repo_root: Path,
    manifest_path: Path | None = None,
    *,
    snapshot_dir: Path | None = None,
    snapshot_mode: RuntimeSnapshotMode = "source",
) -> PreparedRuntimeBundles:
    """Load full contracts once and compile their runtime bundle projections."""
    if snapshot_mode not in RUNTIME_SNAPSHOT_MODES:
        message = f"unsupported runtime snapshot mode {snapshot_mode!r}"
        raise ValueError(message)
    specs = load_runtime_bundle_specs(repo_root, manifest_path)
    select_exact_versions(
        reviewed_profile_versions(),
        (spec.version for spec in specs),
        label="version profile ledger",
    )
    source_identities = {
        spec.version: require_exact_git_source_identity(
            spec.source_root,
            label=spec.version,
        )
        for spec in specs
    }
    resolved_snapshot_dir = _resolve_snapshot_dir(repo_root, snapshot_dir)
    bundles: list[RuntimeBundle] = []
    loaded_contracts: list[LoadedContract] = []
    cache_updates: list[
        tuple[RuntimeBundleSpec, ExactGitSourceIdentity, Path, LoadedContract]
    ] = []
    for spec in specs:
        source_identity = source_identities[spec.version]
        fallback_reason: str | None = None
        if snapshot_mode != "source":
            snapshot_path = _runtime_snapshot_path(
                resolved_snapshot_dir,
                spec.version,
            )
            try:
                loaded_snapshot = load_contract_input(
                    ContractInput(spec.version, snapshot_path, "snapshot"),
                    require_exact=True,
                )
                _require_current_runtime_snapshot(
                    loaded_snapshot,
                    source_identity=source_identity,
                )
                bundle = compile_runtime_bundle(spec, loaded_snapshot)
            except (OSError, TypeError, ValueError) as exc:
                fallback_reason = _snapshot_failure_reason(exc)
                if snapshot_mode == "require":
                    message = (
                        f"runtime snapshot cache for DS {spec.version} cannot be "
                        f"used: {fallback_reason}"
                    )
                    raise RuntimeSnapshotCacheError(message) from exc
            else:
                bundles.append(replace(bundle, input_kind="snapshot"))
                loaded_contracts.append(loaded_snapshot)
                continue

        loaded_source = load_contract_input(
            ContractInput(spec.version, spec.source_root, "ds-source"),
            require_exact=True,
        )
        _require_loaded_source_identity(
            loaded_source,
            source_identity=source_identity,
        )
        bundles.append(
            replace(
                compile_runtime_bundle(spec, loaded_source),
                input_kind="source",
                snapshot_fallback_reason=fallback_reason,
            )
        )
        loaded_contracts.append(loaded_source)
        if fallback_reason is not None:
            cache_updates.append(
                (
                    spec,
                    source_identity,
                    _runtime_snapshot_path(resolved_snapshot_dir, spec.version),
                    loaded_source,
                )
            )

    _refresh_runtime_snapshot_cache(cache_updates)
    return PreparedRuntimeBundles(
        bundles=tuple(
            replace(
                bundle,
                discovery_profile=compile_discovery_profile(
                    loaded.snapshot, bundle.spec.source_root
                ),
            )
            for bundle, loaded in zip(bundles, loaded_contracts, strict=True)
        ),
        loaded_contracts=tuple(loaded_contracts),
    )


def _resolve_snapshot_dir(repo_root: Path, snapshot_dir: Path | None) -> Path:
    path = snapshot_dir or DEFAULT_RUNTIME_SNAPSHOT_DIR
    if path.is_absolute():
        return path
    return repo_root / path


def _runtime_snapshot_path(snapshot_dir: Path, version: str) -> Path:
    return snapshot_dir / f"ds-{version}-contract.json"


def _require_current_runtime_snapshot(
    loaded: LoadedContract,
    *,
    source_identity: ExactGitSourceIdentity,
) -> None:
    recorded_fingerprint = loaded.provenance.get("extractor_fingerprint")
    current_fingerprint = contract_extractor_fingerprint()
    if recorded_fingerprint != current_fingerprint:
        message = "snapshot extractor fingerprint does not match the current compiler"
        raise RuntimeSnapshotCacheError(message)
    cached_identity = _exact_git_identity(loaded)
    expected_identity = (
        source_identity.tag,
        source_identity.commit,
        source_identity.tree,
    )
    if cached_identity != expected_identity:
        message = "snapshot source identity does not match the current exact source"
        raise RuntimeSnapshotCacheError(message)


def _require_loaded_source_identity(
    loaded: LoadedContract,
    *,
    source_identity: ExactGitSourceIdentity,
) -> None:
    loaded_identity = _exact_git_identity(loaded)
    expected_identity = (
        source_identity.tag,
        source_identity.commit,
        source_identity.tree,
    )
    if loaded_identity != expected_identity:
        message = "exact source identity changed while its contract was extracted"
        raise RuntimeError(message)


def _snapshot_failure_reason(exc: Exception) -> str:
    detail = " ".join(str(exc).split())
    return detail or type(exc).__name__


def _refresh_runtime_snapshot_cache(
    updates: list[
        tuple[RuntimeBundleSpec, ExactGitSourceIdentity, Path, LoadedContract]
    ],
) -> None:
    if not updates:
        return
    for spec, expected_identity, _, loaded in updates:
        current_identity = require_exact_git_source_identity(
            spec.source_root,
            label=spec.version,
        )
        if current_identity != expected_identity:
            message = (
                f"runtime bundle {spec.version} source identity changed before "
                "its snapshot cache was refreshed"
            )
            raise RuntimeError(message)
        _require_loaded_source_identity(
            loaded,
            source_identity=current_identity,
        )
    for _, _, snapshot_path, loaded in updates:
        _write_snapshot_atomically(snapshot_path, loaded)


def _write_snapshot_atomically(path: Path, loaded: LoadedContract) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
        write_contract_snapshot_document(
            loaded.snapshot,
            temporary_path,
            provenance=loaded.provenance,
        )
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def render_runtime_bundles(
    bundles: tuple[RuntimeBundle, ...],
    output_root: Path,
) -> None:
    """Render a complete bundle set into a disposable output root."""
    stable_actions = stable_leaf_actions()
    profile_ledger = load_version_profile_ledger()
    versions = tuple(
        sorted((bundle.spec.version for bundle in bundles), key=natural_version_key)
    )
    profile_data = compile_version_profile_data(
        stable_actions=stable_actions,
        ledger=profile_ledger,
        versions=versions,
    )
    # Authoring facts belong to the original contracts, not the executable
    # ownership slices left after whole-domain compilation.
    datasource_reviews = load_datasource_profile_reviews()
    select_exact_versions(
        datasource_reviews, versions, label="datasource profile reviews"
    )
    datasource_profiles: list[DataSourceProfile] = [
        compile_datasource_profile(
            bundle.snapshot,
            plugin_fields_by_type=datasource_reviews[bundle.spec.version],
        )
        for bundle in sorted(
            bundles, key=lambda item: natural_version_key(item.spec.version)
        )
    ]
    compiled_domains = compile_domains(
        bundles,
        _COMPILED_DOMAINS,
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )
    legacy_bundles = compiled_domains.legacy_bundles
    generated_root = output_root / "generated"
    if generated_root.exists():
        shutil.rmtree(generated_root)
    for bundle in sorted(
        legacy_bundles,
        key=lambda item: natural_version_key(item.spec.version),
    ):
        write_generated_package(
            bundle.snapshot,
            output_root,
            shared_runtime=True,
        )
    artifact_result = render_runtime_artifacts(
        legacy_bundles,
        output_root,
        profile_data=profile_data,
    )
    render_compiled_wire_artifact(
        compiled_domains.modules(),
        output_root,
    )
    for bundle in legacy_bundles:
        _write_bundle_manifest(
            output_root,
            bundle.metadata,
            artifact=artifact_result[bundle.spec.version],
        )
    write_version_profiles(
        output_root,
        stable_actions=stable_actions,
        ledger=profile_ledger,
        source_identities={
            bundle.metadata.version: {
                "tag": bundle.metadata.source_tag,
                "commit": bundle.metadata.source_commit,
                "tree": bundle.metadata.source_tree,
            }
            for bundle in bundles
        },
        profile_data=profile_data,
        versions=versions,
    )
    write_conformance_bundle_data(
        output_root,
        profile_data,
    )
    validate_schedule_environment_sources(
        {bundle.spec.version: bundle.spec.source_root for bundle in bundles}
    )
    write_runtime_instance_profiles(output_root, versions=versions)
    write_task_definition_profiles(output_root, versions=versions)
    write_task_definition_cleanup_profiles(output_root, versions=versions)
    write_workflow_profiles(output_root, versions=versions)
    write_datasource_profiles(output_root, tuple(datasource_profiles))
    write_task_profile_data(
        generated_root / "task_profiles.py",
        facts=load_task_profile_document(DEFAULT_TASK_PROFILE_FACTS),
        reviews=load_task_profile_document(DEFAULT_TASK_PROFILE_REVIEWS),
        versions=versions,
    )
    write_version_discovery(
        output_root,
        tuple(
            bundle.discovery_profile
            for bundle in bundles
            if bundle.discovery_profile is not None
        ),
    )


def require_runtime_bundle_sources_unchanged(
    bundles: tuple[RuntimeBundle, ...],
) -> None:
    """Require source tags and Git identities to match their compiled bundles."""
    for bundle in bundles:
        current_provenance = build_source_provenance(
            bundle.spec.source_root,
            label=bundle.spec.version,
            snapshot=bundle.snapshot,
        )
        require_exact_provenance(current_provenance, label=bundle.spec.version)
        current_identity = _exact_git_identity(
            LoadedContract(
                label=bundle.spec.version,
                snapshot=bundle.snapshot,
                provenance=current_provenance,
            )
        )
        expected_identity = (
            bundle.metadata.source_tag,
            bundle.metadata.source_commit,
            bundle.metadata.source_tree,
        )
        if current_identity != expected_identity:
            message = (
                f"runtime bundle {bundle.spec.version} source identity changed "
                "while bundles were generated"
            )
            raise RuntimeError(message)


def _write_bundle_manifest(
    output_root: Path,
    metadata: RuntimeBundleMetadata,
    *,
    artifact: RenderedExactArtifact,
) -> None:
    if artifact.version != metadata.version:
        message = "runtime bundle and artifact manifest versions do not match"
        raise ValueError(message)
    package_slug = runtime_bundle_package_slug(metadata.version)
    output_path = output_root / "generated" / "versions" / package_slug / "_manifest.py"
    output_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "# Generated by tools/generate_ds_runtime_bundles.py; do not edit.",
                "BUNDLE_MANIFEST_SCHEMA_VERSION = 2",
                f"DS_VERSION = {json.dumps(metadata.version)}",
                f"SELECTION = {json.dumps(metadata.selection)}",
                *_render_semantic_operations(metadata.semantic_operations),
                f"SOURCE_TAG = {json.dumps(metadata.source_tag)}",
                f"SOURCE_COMMIT = {json.dumps(metadata.source_commit)}",
                f"SOURCE_TREE = {json.dumps(metadata.source_tree)}",
                *_render_string_assignment(
                    "SOURCE_CONTRACT_DIGEST",
                    metadata.source_contract_digest,
                ),
                *_render_string_assignment(
                    "RENDERED_CONTRACT_DIGEST",
                    metadata.rendered_contract_digest,
                ),
                *_render_string_assignment("PROFILE_DIGEST", artifact.profile_digest),
                *_render_string_assignment("RECIPE_DIGEST", artifact.recipe_digest),
                *_render_string_assignment("BINDING_DIGEST", artifact.binding_digest),
                f"OPERATION_COUNT = {metadata.operation_count}",
                f"ENUM_COUNT = {metadata.enum_count}",
                f"DTO_COUNT = {metadata.dto_count}",
                f"MODEL_COUNT = {metadata.model_count}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _render_semantic_operations(operations: tuple[str, ...]) -> list[str]:
    if not operations:
        return ["SEMANTIC_OPERATIONS = ()"]
    return [
        "SEMANTIC_OPERATIONS = (",
        *(f"    {json.dumps(operation)}," for operation in operations),
        ")",
    ]


def _render_string_assignment(name: str, value: str) -> list[str]:
    return [f"{name} = (", f"    {json.dumps(value)}", ")"]


def _exact_git_identity(loaded: LoadedContract) -> tuple[str, str, str]:
    origin = loaded.provenance.get("origin")
    if not isinstance(origin, dict):
        message = f"runtime bundle {loaded.label} has no source origin"
        raise TypeError(message)
    git = origin.get("git")
    if not isinstance(git, dict):
        message = f"runtime bundle {loaded.label} has no Git source identity"
        raise TypeError(message)
    tag = git.get("tag")
    commit = git.get("commit")
    tree = git.get("tree")
    if not all(isinstance(value, str) for value in (tag, commit, tree)):
        message = f"runtime bundle {loaded.label} has incomplete Git source identity"
        raise ValueError(message)
    return cast("str", tag), cast("str", commit), cast("str", tree)


__all__ = [
    "DEFAULT_RUNTIME_BUNDLE_MANIFEST",
    "DEFAULT_RUNTIME_SNAPSHOT_DIR",
    "RUNTIME_SNAPSHOT_MODES",
    "PreparedRuntimeBundles",
    "RuntimeBundle",
    "RuntimeBundleInputKind",
    "RuntimeBundleMetadata",
    "RuntimeBundleSelection",
    "RuntimeBundleSourceIdentity",
    "RuntimeBundleSpec",
    "RuntimeSnapshotCacheError",
    "RuntimeSnapshotMode",
    "compile_runtime_bundle",
    "load_prepared_runtime_bundles",
    "load_runtime_bundle_source_identities",
    "load_runtime_bundle_specs",
    "load_runtime_bundles",
    "render_runtime_bundles",
    "require_runtime_bundle_sources_unchanged",
]
