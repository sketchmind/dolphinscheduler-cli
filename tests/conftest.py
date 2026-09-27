from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from tests.codegen.exact_contract_corpus import ExactContractCorpus

if TYPE_CHECKING:
    from ds_codegen.runtime_bundles import RuntimeBundle

_PROFILE_ENV_KEYS = (
    "DSCTL_CONTEXT",
    "DSCTL_ENV_FILE",
    "DS_API_URL",
    "DS_API_TOKEN",
    "DS_VERSION",
    "DS_API_RETRY_ATTEMPTS",
    "DS_API_RETRY_BACKOFF_MS",
    "DS_API_TIMEOUT_SECONDS",
)


@pytest.fixture(scope="session")
def exact_contract_corpus() -> ExactContractCorpus:
    """Load the complete explicitly prepared exact-source contract corpus."""
    return ExactContractCorpus.load(Path(__file__).resolve().parents[1])


@pytest.fixture(scope="module")
def exact_runtime_bundles(
    exact_contract_corpus: ExactContractCorpus,
) -> tuple[RuntimeBundle, ...]:
    """Return module-local runtime bundles isolated from the session corpus."""
    return exact_contract_corpus.runtime_bundles()


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Require every corpus consumer to declare its non-portable test lane."""
    source_lanes = ("source_contract", "source_rebuild")
    missing_marker = [
        item.nodeid
        for item in items
        if "exact_contract_corpus" in getattr(item, "fixturenames", ())
        and all(item.get_closest_marker(lane) is None for lane in source_lanes)
    ]
    if missing_marker:
        joined = "\n".join(f"- {nodeid}" for nodeid in missing_marker)
        message = (
            "exact contract corpus tests require "
            f"@pytest.mark.source_contract or @pytest.mark.source_rebuild:\n{joined}"
        )
        raise pytest.UsageError(message)
    duplicate_marker = [
        item.nodeid
        for item in items
        if all(item.get_closest_marker(lane) is not None for lane in source_lanes)
    ]
    if duplicate_marker:
        joined = "\n".join(f"- {nodeid}" for nodeid in duplicate_marker)
        message = (
            f"source_contract and source_rebuild lanes must be disjoint:\n{joined}"
        )
        raise pytest.UsageError(message)


@pytest.fixture
def isolated_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Run a test from an empty temporary working directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture(autouse=True)
def isolate_ds_profile_environment(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Keep offline tests independent from a developer's active DS profile."""
    if request.node.get_closest_marker("live") is not None:
        return
    for key in _PROFILE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    # Pure tests need no per-test directory; registry writers create it on demand.
    config_home = tmp_path_factory.getbasetemp() / f"user-config-{uuid4().hex}"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
