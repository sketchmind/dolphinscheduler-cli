"""Typed plumbing helpers for conformance-bundle tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from tests.live.conformance_bundle_gate import (
        ConformanceBundleGateConfig,
        ConformanceScenarioResult,
    )
    from tests.live.support import (
        DsctlCommandResult,
        TaskDefinitionCleanupInvocation,
    )


RUN_ID = "0123456789abcdef"

SummaryT_co = TypeVar("SummaryT_co", covariant=True)


class EvidenceValidator(Protocol[SummaryT_co]):
    """Callable shape shared by prepared and source-root validators."""

    def __call__(
        self,
        receipt: object,
        *,
        expected_ds_version: str | None = None,
        expected_bundle: str | None = None,
        expected_wheel_filename: str | None = None,
        expected_wheel_sha256: str | None = None,
        source_root: Path | None = None,
    ) -> SummaryT_co: ...


@dataclass
class EvidenceCase(Generic[SummaryT_co]):
    """Keep receipt navigation, signing, and validation plumbing together."""

    receipt: dict[str, object]
    validator: EvidenceValidator[SummaryT_co]
    refresh_receipt: Callable[[dict[str, object]], None]
    source_root: Path | None = None

    def mapping(self, *path: str | int) -> dict[str, object]:
        """Return one required mapping without weakening its runtime check."""
        value = self._at(path)
        assert isinstance(value, dict)
        return value

    def sequence(self, *path: str | int) -> list[object]:
        """Return one required list without weakening its runtime check."""
        value = self._at(path)
        assert isinstance(value, list)
        return value

    def validate(
        self,
        *,
        expected_ds_version: str | None = None,
        expected_bundle: str | None = None,
        expected_wheel_filename: str | None = None,
        expected_wheel_sha256: str | None = None,
    ) -> SummaryT_co:
        """Validate through the case's prepared or source-root truth."""
        return self.validator(
            self.receipt,
            expected_ds_version=expected_ds_version,
            expected_bundle=expected_bundle,
            expected_wheel_filename=expected_wheel_filename,
            expected_wheel_sha256=expected_wheel_sha256,
            source_root=self.source_root,
        )

    def assert_rejected(
        self,
        match: str | None = None,
        *,
        error: type[BaseException] | tuple[type[BaseException], ...] = ValueError,
        rehash: bool = False,
        expected_ds_version: str | None = None,
        expected_bundle: str | None = None,
        expected_wheel_filename: str | None = None,
        expected_wheel_sha256: str | None = None,
    ) -> None:
        """Optionally re-sign, then require one evidence validation rejection."""
        if rehash:
            self.refresh_receipt(self.receipt)
        with pytest.raises(error, match=match):
            self.validate(
                expected_ds_version=expected_ds_version,
                expected_bundle=expected_bundle,
                expected_wheel_filename=expected_wheel_filename,
                expected_wheel_sha256=expected_wheel_sha256,
            )

    def _at(self, path: tuple[str | int, ...]) -> object:
        value: object = self.receipt
        for part in path:
            if isinstance(part, int):
                assert isinstance(value, list)
                value = value[part]
            else:
                assert isinstance(value, dict)
                value = value[part]
        return value


@dataclass
class ScenarioCase:
    """Bind fixed scenario invocation plumbing to one fake runtime."""

    config: ConformanceBundleGateConfig
    invoke: Callable[[list[str]], DsctlCommandResult]
    execute: Callable[..., ConformanceScenarioResult]
    invoke_raw: Callable[[list[str]], DsctlCommandResult] | None = None
    invoke_task_cleanup: (
        Callable[
            [str, int, int | None, str],
            TaskDefinitionCleanupInvocation,
        ]
        | None
    ) = None
    run_id: str = RUN_ID

    def run(self) -> ConformanceScenarioResult:
        """Execute the scenario with its bound invokers."""
        return self.execute(
            self.config,
            invoke=self.invoke,
            invoke_raw=self.invoke_raw,
            invoke_task_cleanup=self.invoke_task_cleanup,
            run_id=self.run_id,
        )

    def assert_run_rejected(
        self,
        error: type[BaseException],
        match: str | None = None,
    ) -> None:
        """Require one scenario execution failure without hiding its fault setup."""
        with pytest.raises(error, match=match):
            self.run()
