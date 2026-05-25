from __future__ import annotations

import pytest

_PROFILE_ENV_KEYS = (
    "DS_API_URL",
    "DS_API_TOKEN",
    "DS_VERSION",
    "DS_API_RETRY_ATTEMPTS",
    "DS_API_RETRY_BACKOFF_MS",
)


@pytest.fixture(autouse=True)
def isolate_ds_profile_environment(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Keep offline tests independent from a developer's active DS profile."""
    if request.node.get_closest_marker("live") is not None:
        return
    for key in _PROFILE_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
