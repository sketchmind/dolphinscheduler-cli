from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.services import runtime as runtime_service

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch


@dataclass(frozen=True)
class FakeRuntimeAdapter:
    seen_versions: list[str]

    def bind(self, profile: object, *, http_client: object) -> object:
        return {"profile": profile, "http_client": http_client}


class FakeRuntimeHttpClient:
    def __init__(self, profile: object) -> None:
        self.profile = profile

    def __enter__(self) -> FakeRuntimeHttpClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def healthcheck(self) -> dict[str, object]:
        return {"status": "UP"}


def test_open_service_runtime_uses_profile_ds_version(
    monkeypatch: MonkeyPatch,
) -> None:
    seen_versions: list[str] = []

    def fake_get_adapter(version: str) -> FakeRuntimeAdapter:
        seen_versions.append(version)
        return FakeRuntimeAdapter(seen_versions)

    monkeypatch.setattr(
        runtime_service,
        "load_profile",
        lambda env_file=None: make_profile(ds_version="3.3.2"),
    )
    monkeypatch.setattr(
        runtime_service, "load_context", lambda cwd=None: SessionContext()
    )
    monkeypatch.setattr(runtime_service, "get_adapter", fake_get_adapter)
    monkeypatch.setattr(
        runtime_service, "DolphinSchedulerClient", FakeRuntimeHttpClient
    )

    with runtime_service.open_service_runtime() as runtime:
        assert runtime.profile.ds_version == "3.3.2"

    assert seen_versions == ["3.3.2"]
