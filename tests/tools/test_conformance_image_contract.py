from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from live_gate.conformance_image_contract import API_IMAGE_REPOSITORIES
from live_gate.conformance_image_provenance import validate_image_reference

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

_ADDED_VERSIONS = (
    *(f"2.0.{patch}" for patch in range(1, 9)),
    *(f"3.0.{patch}" for patch in range(1, 6)),
    *(f"3.1.{patch}" for patch in range(1, 9)),
)


@pytest.mark.source_contract
@pytest.mark.parametrize("version", _ADDED_VERSIONS)
def test_added_image_repository_matches_exact_deployment_source(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
) -> None:
    source = exact_contract_corpus.source_root(version)
    relative = (
        "docker/docker-swarm/docker-stack.yml"
        if version.startswith("2.")
        else "deploy/docker/docker-stack.yml"
    )
    repositories = {
        line.strip().removeprefix("image: ").partition(":")[0]
        for line in (source / relative).read_text().splitlines()
        if line.strip().startswith("image: apache/dolphinscheduler")
    }
    if version in {"2.0.4", "2.0.7", "2.0.8"}:
        upstream_repository = "apache/dolphinscheduler"
    elif version in {"3.0.2", "3.1.2"}:
        upstream_repository = "apache/dolphinscheduler-api"
    else:
        upstream_repository = API_IMAGE_REPOSITORIES[version]
    assert upstream_repository in repositories
    if version in {"2.0.4", "2.0.7", "2.0.8"}:
        assert API_IMAGE_REPOSITORIES[version] == "dsmatrix-local/dolphinscheduler"
    if version in {"3.0.2", "3.1.2"}:
        assert API_IMAGE_REPOSITORIES[version] == "dsmatrix-local/dolphinscheduler-api"


@pytest.mark.parametrize(
    ("version", "stale_tag"),
    [("2.0.5", "2.0.4"), ("2.0.7", "2.0.6"), ("2.0.8", "2.0.7")],
)
def test_stale_upstream_deployment_example_is_not_exact_image_evidence(
    version: str,
    stale_tag: str,
) -> None:
    repository = API_IMAGE_REPOSITORIES[version]
    with pytest.raises(ValueError, match="must identify exact DS"):
        validate_image_reference(f"{repository}:{stale_tag}", ds_version=version)
    assert (
        validate_image_reference(f"{repository}:{version}", ds_version=version)
        == f"{repository}:{version}"
    )


@pytest.mark.parametrize("version", ["2.0.4", "2.0.7", "2.0.8"])
def test_exact_source_build_admission_rejects_public_namespace(version: str) -> None:
    with pytest.raises(ValueError, match="must identify exact DS"):
        validate_image_reference(
            f"apache/dolphinscheduler:{version}", ds_version=version
        )
    assert (
        validate_image_reference(
            f"dsmatrix-local/dolphinscheduler:{version}", ds_version=version
        )
        == f"dsmatrix-local/dolphinscheduler:{version}"
    )


@pytest.mark.parametrize("version", ["3.0.2", "3.1.2"])
def test_exact_modern_source_build_admission_rejects_public_namespace(
    version: str,
) -> None:
    with pytest.raises(ValueError, match="must identify exact DS"):
        validate_image_reference(
            f"apache/dolphinscheduler-api:{version}", ds_version=version
        )
    assert (
        validate_image_reference(
            f"dsmatrix-local/dolphinscheduler-api:{version}", ds_version=version
        )
        == f"dsmatrix-local/dolphinscheduler-api:{version}"
    )
