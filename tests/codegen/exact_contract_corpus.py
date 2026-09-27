"""One explicit test seam for the prepared exact-source contract corpus."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

_TOOLS_DIR = Path(__file__).resolve().parents[2] / "tools"
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from ds_codegen.contract_inputs import ContractInput, LoadedContract
    from ds_codegen.ir import ContractSnapshot
    from ds_codegen.runtime_bundles import RuntimeBundle


class ExactContractCorpusError(RuntimeError):
    """Raised when the explicitly prepared exact-source corpus is incomplete."""


@dataclass(frozen=True)
class ExactContractCorpus:
    """Validated exact sources and typed cached contracts for source-backed tests."""

    repo_root: Path
    versions: tuple[str, ...]
    _source_roots: Mapping[str, Path]
    _snapshot_paths: Mapping[str, Path]
    _loaded_contracts: Mapping[str, LoadedContract]
    _runtime_bundles: tuple[RuntimeBundle, ...]

    @classmethod
    def load(cls, repo_root: Path) -> ExactContractCorpus:
        """Load the complete corpus, failing clearly when preparation is incomplete."""
        runtime_bundles = importlib.import_module("ds_codegen.runtime_bundles")
        manifest = importlib.import_module("runtime_bundle_manifest")
        specs = manifest.load_runtime_bundle_specs(repo_root)
        snapshot_root = repo_root / "build/ds_contract/snapshots-v2"
        missing_sources = [
            spec.source_root for spec in specs if not spec.source_root.is_dir()
        ]
        snapshot_paths = {
            spec.version: snapshot_root / f"ds-{spec.version}-contract.json"
            for spec in specs
        }
        missing_snapshots = [
            path for path in snapshot_paths.values() if not path.is_file()
        ]
        if missing_sources or missing_snapshots:
            missing = [
                *(path.relative_to(repo_root).as_posix() for path in missing_sources),
                *(path.relative_to(repo_root).as_posix() for path in missing_snapshots),
            ]
            detail = "\n".join(f"- {path}" for path in missing)
            message = (
                "exact source-contract corpus is incomplete; prepare all exact "
                "upstream sources and snapshots before running a source_contract "
                "or source_rebuild "
                f"lane:\n{detail}"
            )
            raise ExactContractCorpusError(message)

        try:
            prepared = runtime_bundles.load_prepared_runtime_bundles(
                repo_root,
                snapshot_dir=snapshot_root,
                snapshot_mode="require",
            )
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            message = (
                "exact source-contract corpus does not match the current exact "
                f"sources and extractor: {error}"
            )
            raise ExactContractCorpusError(message) from error

        source_roots = {
            bundle.spec.version: bundle.spec.source_root for bundle in prepared.bundles
        }
        loaded_contracts = {
            loaded.label: loaded for loaded in prepared.loaded_contracts
        }
        versions = tuple(bundle.spec.version for bundle in prepared.bundles)
        return cls(
            repo_root=repo_root,
            versions=versions,
            _source_roots=MappingProxyType(source_roots),
            _snapshot_paths=MappingProxyType(snapshot_paths),
            _loaded_contracts=MappingProxyType(loaded_contracts),
            _runtime_bundles=prepared.bundles,
        )

    def contract_input(self, version: str) -> ContractInput:
        """Return one exact cached-snapshot input for analyzers that load inputs."""
        self.snapshot(version)
        contract_inputs = importlib.import_module("ds_codegen.contract_inputs")
        return cast(
            "ContractInput",
            contract_inputs.ContractInput(
                version,
                self._snapshot_paths[version],
                "snapshot",
            ),
        )

    def snapshot(self, version: str) -> ContractSnapshot:
        """Return one typed contract snapshot."""
        try:
            return self._loaded_contracts[version].snapshot
        except KeyError as error:
            message = f"exact source-contract corpus has no version {version!r}"
            raise ValueError(message) from error

    def runtime_bundles(self) -> tuple[RuntimeBundle, ...]:
        """Return a deep-isolated copy of every compiled runtime bundle."""
        return deepcopy(self._runtime_bundles)

    def snapshot_json(self, version: str) -> dict[str, Any]:
        """Return the typed snapshot's JSON projection for shape assertions."""
        return cast("dict[str, Any]", self.snapshot(version).to_json_dict())

    def source_root(self, version: str) -> Path:
        """Return one validated exact source root."""
        try:
            return self._source_roots[version]
        except KeyError as error:
            message = f"exact source-contract corpus has no version {version!r}"
            raise ValueError(message) from error

    def source_file(self, version: str, relative_path: str) -> Path:
        """Return one required evidence file under an exact source root."""
        path = self.source_root(version) / relative_path
        if not path.is_file():
            message = f"exact source evidence file does not exist: {path}"
            raise ExactContractCorpusError(message)
        return path

    @contextmanager
    def codegen_repo_root(self, version: str) -> Iterator[Path]:
        """Adapt one exact source root to the extractor's repo-root interface."""
        source = importlib.import_module("ds_codegen.source")
        with source.codegen_repo_root_for_ds_source(self.source_root(version)) as root:
            yield cast("Path", root)


__all__ = ["ExactContractCorpus", "ExactContractCorpusError"]
