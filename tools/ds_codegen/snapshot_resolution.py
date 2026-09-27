"""Resolve contract type identities without consulting DolphinScheduler source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from ds_codegen.contract_type_refs import (
    build_contract_type_candidate_catalog,
    canonicalize_builtin_type_expression,
    collect_type_reference_names,
    is_fully_qualified_reference_name,
)
from ds_codegen.contract_visibility import (
    has_executable_response_type,
    is_client_supplied_parameter,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from ds_codegen.ir import (
        ContractSnapshot,
        DtoSpec,
        EnumSpec,
        ModelSpec,
        ReferenceOwnerKind,
    )


_ResolutionState = Literal["missing", "unique", "ambiguous"]
_CONTRACT_OWNED_REFERENCE_PREFIXES = (
    "generated.view.",
    "org.apache.dolphinscheduler.",
)


class SnapshotResolutionError(ValueError):
    """Base error for an invalid or incomplete snapshot resolution graph."""


class AmbiguousTypeReferenceError(SnapshotResolutionError):
    """Raised when an ambiguous type use-site has no exact decision."""


class UnresolvedTypeReferenceError(SnapshotResolutionError):
    """Raised when a rendered reference has no contract type target."""


@dataclass(frozen=True)
class ResolutionScope:
    """Language-neutral identity of one contract type-use owner."""

    owner_kind: ReferenceOwnerKind
    owner_ref: str


@dataclass(frozen=True)
class ScopedTypeUse:
    """One normalized contract type expression and its resolution owner."""

    java_type: str
    scope: ResolutionScope


@dataclass(frozen=True)
class ResolvedReferenceEdge:
    """One actual type-identity edge traversed through the contract graph."""

    owner_kind: ReferenceOwnerKind
    owner_ref: str
    reference_name: str
    import_path: str


@dataclass(frozen=True)
class ResolvedTypeGraph:
    """Exact reachable type identities and their scoped resolution edges."""

    import_paths: frozenset[str]
    edges: frozenset[ResolvedReferenceEdge]


class SnapshotTypeResolver:
    """Compile and validate the narrow type-resolution seam for one snapshot."""

    def __init__(self, snapshot: ContractSnapshot) -> None:
        self._candidates_by_name = _snapshot_type_candidates(snapshot)
        self._structured_import_paths = frozenset(
            spec.import_path for spec in _iter_structured_specs(snapshot)
        )
        self._specs_by_import_path: dict[str, EnumSpec | DtoSpec | ModelSpec] = {}
        surfaces_by_import_path: dict[str, str] = {}
        for surface, spec in _iter_type_specs_with_surface(snapshot):
            previous_surface = surfaces_by_import_path.get(spec.import_path)
            if previous_surface is not None:
                message = (
                    "contract type import path appears on multiple surfaces: "
                    f"{spec.import_path} ({previous_surface}, {surface})"
                )
                raise SnapshotResolutionError(message)
            surfaces_by_import_path[spec.import_path] = surface
            self._specs_by_import_path[spec.import_path] = spec
        uses = _snapshot_reference_uses(snapshot)
        ambiguous = sorted(
            key for key in uses if len(self._candidates_by_name.get(key[2], ())) > 1
        )
        if ambiguous:
            owner_kind, owner_ref, reference_name = ambiguous[0]
            rendered_candidates = sorted(self._candidates_by_name[reference_name])
            message = (
                "ambiguous contract snapshot reference is not fully qualified: "
                f"{owner_kind}:{owner_ref}:{reference_name}; "
                f"candidates={rendered_candidates!r}; refresh the snapshot from "
                "exact source"
            )
            raise AmbiguousTypeReferenceError(message)

        unresolved = sorted(
            key
            for key in uses
            if not self._candidates_by_name.get(key[2])
            and not self.is_opaque_external_reference(key[2])
        )
        if unresolved:
            owner_kind, owner_ref, reference_name = unresolved[0]
            message = (
                "unqualified contract snapshot reference has no exact target: "
                f"{owner_kind}:{owner_ref}:{reference_name}; regenerate the "
                "snapshot from exact source"
            )
            raise UnresolvedTypeReferenceError(message)

    @classmethod
    def compile(cls, snapshot: ContractSnapshot) -> SnapshotTypeResolver:
        return cls(snapshot)

    def state(self, reference_name: str) -> _ResolutionState:
        candidates = self._candidates_by_name.get(reference_name, frozenset())
        if not candidates:
            return "missing"
        if len(candidates) == 1:
            return "unique"
        return "ambiguous"

    def candidates(self, reference_name: str) -> frozenset[str]:
        return self._candidates_by_name.get(reference_name, frozenset())

    def is_opaque_external_reference(self, reference_name: str) -> bool:
        """Return whether source preserved a qualified, unmaterialized identity."""

        canonical_name = canonicalize_builtin_type_expression(reference_name)
        return (
            not self.candidates(reference_name)
            and canonical_name == reference_name
            and is_fully_qualified_reference_name(reference_name)
            and not reference_name.startswith(_CONTRACT_OWNED_REFERENCE_PREFIXES)
        )

    def resolve(
        self,
        reference_name: str,
        *,
        scope: ResolutionScope,
    ) -> str:
        candidates = self._candidates_by_name.get(reference_name, frozenset())
        if len(candidates) == 1:
            return next(iter(candidates))
        if not candidates:
            message = (
                f"unresolved contract snapshot reference {reference_name!r} at "
                f"{scope.owner_kind}:{scope.owner_ref}"
            )
            raise UnresolvedTypeReferenceError(message)
        message = (
            f"ambiguous contract snapshot reference {reference_name!r} at "
            f"{scope.owner_kind}:{scope.owner_ref}; "
            f"candidates={sorted(candidates)!r}"
        )
        raise AmbiguousTypeReferenceError(message)

    def resolve_type_graph(
        self,
        uses: Iterable[ScopedTypeUse],
        *,
        root_import_paths: Iterable[str] = (),
    ) -> ResolvedTypeGraph:
        """Expand exact dependencies and expose every traversed identity edge."""

        closure = set(root_import_paths)
        unknown_roots = closure - self._specs_by_import_path.keys()
        if unknown_roots:
            message = (
                "contract type closure contains unknown roots: "
                f"{sorted(unknown_roots)!r}"
            )
            raise UnresolvedTypeReferenceError(message)
        pending_import_paths = list(closure)
        pending_uses = list(uses)
        expanded_import_paths: set[str] = set()
        expanded_uses: set[ScopedTypeUse] = set()
        edges: set[ResolvedReferenceEdge] = set()
        while pending_uses or pending_import_paths:
            while pending_uses:
                use = pending_uses.pop()
                if use in expanded_uses:
                    continue
                expanded_uses.add(use)
                for reference_name in collect_type_reference_names(use.java_type):
                    if self.state(reference_name) == "missing":
                        if self.is_opaque_external_reference(reference_name):
                            continue
                        message = (
                            "unresolved contract snapshot reference "
                            f"{reference_name!r} at {use.scope.owner_kind}:"
                            f"{use.scope.owner_ref}"
                        )
                        raise UnresolvedTypeReferenceError(message)
                    import_path = self.resolve(reference_name, scope=use.scope)
                    edges.add(
                        ResolvedReferenceEdge(
                            owner_kind=use.scope.owner_kind,
                            owner_ref=use.scope.owner_ref,
                            reference_name=reference_name,
                            import_path=import_path,
                        )
                    )
                    if import_path not in closure:
                        closure.add(import_path)
                        pending_import_paths.append(import_path)
            if not pending_import_paths:
                continue
            import_path = pending_import_paths.pop()
            if import_path in expanded_import_paths:
                continue
            expanded_import_paths.add(import_path)
            spec = self._specs_by_import_path[import_path]
            if import_path not in self._structured_import_paths:
                continue
            scope = ResolutionScope("structured_type", import_path)
            extends = getattr(spec, "extends", None)
            if isinstance(extends, str):
                pending_uses.append(ScopedTypeUse(extends, scope))
            pending_uses.extend(
                ScopedTypeUse(field.java_type, scope)
                for field in getattr(spec, "fields", ())
            )
        return ResolvedTypeGraph(
            import_paths=frozenset(closure),
            edges=frozenset(edges),
        )


def _snapshot_type_candidates(
    snapshot: ContractSnapshot,
) -> dict[str, frozenset[str]]:
    return build_contract_type_candidate_catalog(
        (spec.name, spec.import_path) for spec in _iter_type_specs(snapshot)
    )


def _snapshot_reference_uses(
    snapshot: ContractSnapshot,
) -> frozenset[tuple[str, str, str]]:
    uses: set[tuple[str, str, str]] = set()
    for operation in snapshot.operations:
        if has_executable_response_type(operation):
            _add_java_type_uses(
                uses,
                owner_kind="operation_response",
                owner_ref=operation.operation_id,
                java_types=(operation.logical_return_type,),
            )
        _add_java_type_uses(
            uses,
            owner_kind="operation_request",
            owner_ref=operation.operation_id,
            java_types=(
                parameter.java_type
                for parameter in operation.parameters
                if is_client_supplied_parameter(parameter)
            ),
        )
    for spec in _iter_structured_specs(snapshot):
        java_types: list[str] = [field.java_type for field in spec.fields]
        if spec.extends is not None:
            java_types.append(spec.extends)
        _add_java_type_uses(
            uses,
            owner_kind="structured_type",
            owner_ref=spec.import_path,
            java_types=java_types,
        )
    return frozenset(uses)


def _add_java_type_uses(
    uses: set[tuple[str, str, str]],
    *,
    owner_kind: ReferenceOwnerKind,
    owner_ref: str,
    java_types: Iterable[str],
) -> None:
    for java_type in java_types:
        for reference_name in collect_type_reference_names(java_type):
            uses.add((owner_kind, owner_ref, reference_name))


def _iter_type_specs(
    snapshot: ContractSnapshot,
) -> Iterator[EnumSpec | DtoSpec | ModelSpec]:
    yield from snapshot.enums
    yield from snapshot.dtos
    yield from snapshot.models


def _iter_structured_specs(
    snapshot: ContractSnapshot,
) -> Iterator[DtoSpec | ModelSpec]:
    yield from snapshot.dtos
    yield from snapshot.models


def _iter_type_specs_with_surface(
    snapshot: ContractSnapshot,
) -> Iterator[tuple[str, EnumSpec | DtoSpec | ModelSpec]]:
    yield from (("enums", spec) for spec in snapshot.enums)
    yield from (("dtos", spec) for spec in snapshot.dtos)
    yield from (("models", spec) for spec in snapshot.models)


__all__ = [
    "AmbiguousTypeReferenceError",
    "ResolutionScope",
    "ResolvedReferenceEdge",
    "ResolvedTypeGraph",
    "ScopedTypeUse",
    "SnapshotResolutionError",
    "SnapshotTypeResolver",
    "UnresolvedTypeReferenceError",
]
