from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pytest

from ds_codegen import runtime_bundles
from ds_codegen.compiled_domains import compile_domains
from ds_codegen.runtime_bundles import (
    _COMPILED_DOMAINS,
    _COMPILED_OPERATION_DEPENDENCIES,
)
from ds_codegen.version_discovery import DiscoveryProfile

if TYPE_CHECKING:
    from pathlib import Path

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.ir import ContractSnapshot


@pytest.fixture(scope="session")
def session_compiled_domains(
    exact_contract_corpus: ExactContractCorpus,
) -> CompiledDomainSet:
    """Compile the unmodified complete registry once, without compiler caching."""
    return compile_domains(
        exact_contract_corpus.runtime_bundles(),
        _COMPILED_DOMAINS,
        operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
    )


@pytest.fixture(scope="module")
def compiled_all_domains(
    session_compiled_domains: CompiledDomainSet,
) -> CompiledDomainSet:
    """Isolate mutable nested plans and snapshots between test modules."""
    return deepcopy(session_compiled_domains)


@pytest.fixture
def stub_discovery_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[ContractSnapshot, Path]]:
    """Isolate source discovery in bundle/cache tests with synthetic source trees."""
    calls: list[tuple[ContractSnapshot, Path]] = []

    def compile_profile(
        snapshot: ContractSnapshot, source_root: Path
    ) -> DiscoveryProfile:
        calls.append((snapshot, source_root))
        return DiscoveryProfile(
            version=snapshot.ds_version,
            product_path=None,
            product_version_field=None,
            openapi=False,
            evidence=(),
        )

    monkeypatch.setattr(runtime_bundles, "compile_discovery_profile", compile_profile)
    return calls
