from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests.codegen.exact_contract_corpus import (
    ExactContractCorpus,
    ExactContractCorpusError,
)

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.compiled_domains import CompiledDomainSet


def test_incomplete_exact_contract_corpus_fails_with_preparation_guidance(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ExactContractCorpusError,
        match="prepare all exact upstream sources and snapshots",
    ):
        ExactContractCorpus.load(tmp_path)


@pytest.mark.source_contract
def test_runtime_bundles_are_deeply_isolated_from_the_session_corpus(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    first = exact_contract_corpus.runtime_bundles()
    second = exact_contract_corpus.runtime_bundles()
    version = first[0].spec.version
    full_operation_count = len(exact_contract_corpus.snapshot(version).operations)

    assert first is not second
    assert first[0] is not second[0]
    assert first[0].snapshot is not second[0].snapshot
    assert first[0].snapshot.operations is not second[0].snapshot.operations

    first[0].snapshot.operations.clear()

    assert second[0].snapshot.operations
    assert (
        len(exact_contract_corpus.snapshot(version).operations) == full_operation_count
    )


@pytest.mark.source_contract
def test_compiled_domains_are_isolated_from_session_preparation(
    compiled_all_domains: CompiledDomainSet,
    session_compiled_domains: CompiledDomainSet,
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    local = compiled_all_domains.legacy_bundles[0].snapshot
    shared = session_compiled_domains.legacy_bundles[0].snapshot
    version = compiled_all_domains.legacy_bundles[0].spec.version
    original = exact_contract_corpus.snapshot(version)
    shared_count = len(shared.operations)
    original_count = len(original.operations)

    assert compiled_all_domains is not session_compiled_domains
    assert local is not shared
    assert local.operations is not shared.operations
    assert original_count > 0
    local.operations.append(original.operations[0])

    assert len(shared.operations) == shared_count
    assert len(original.operations) == original_count
