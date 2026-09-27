from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from dsctl import context as context_store
from dsctl.context import ContextRegistry, NamedContext
from dsctl.errors import ConfigError
from dsctl.services.context import get_saved_context_result, list_contexts_result

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("action", ["list", "get"])
def test_context_inspection_keeps_entry_and_default_in_one_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, action: str
) -> None:
    production = NamedContext(
        "production",
        tmp_path / "production.env",
        "https://production.example",
        project="old-project",
    )
    staging = NamedContext(
        "staging",
        tmp_path / "staging.env",
        "https://staging.example",
    )
    initial = ContextRegistry(
        contexts={"staging": staging, "production": production},
        default_context="staging",
    )
    changed = ContextRegistry(
        contexts={
            "production": NamedContext(
                "production",
                production.env_file,
                production.api_url,
                project="new-project",
            ),
            "staging": staging,
        },
        default_context="production",
    )
    reads = 0

    def read_registry() -> ContextRegistry:
        nonlocal reads
        reads += 1
        # A concurrent writer replaces the registry after the first read.
        return initial if reads == 1 else changed

    monkeypatch.setattr(context_store, "load_registry", read_registry)
    result = (
        list_contexts_result()
        if action == "list"
        else get_saved_context_result("production")
    )

    expected_production = {
        "name": "production",
        "env_file": str(production.env_file),
        "api_url": "https://production.example",
        "project": "old-project",
        "default": False,
    }
    expected = (
        [
            expected_production,
            {
                "name": "staging",
                "env_file": str(staging.env_file),
                "api_url": "https://staging.example",
                "project": None,
                "default": True,
            },
        ]
        if action == "list"
        else expected_production
    )
    assert result.data == expected
    assert reads == 1


def test_saved_context_snapshot_retains_missing_name_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(context_store, "load_registry", ContextRegistry)

    with pytest.raises(ConfigError) as exc_info:
        get_saved_context_result("missing")

    assert exc_info.value.details == {"context_name": "missing"}
    assert exc_info.value.suggestion == (
        "Run `dsctl context list` to inspect registered names."
    )
