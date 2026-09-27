from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Never

import pytest

from tests.live.journey_support import Journey

if TYPE_CHECKING:
    from collections.abc import Iterator


def _journey() -> Journey:
    return Journey(
        repo_root=Path("/repo"),
        executable=Path("/wheel/bin/dsctl"),
        env_file=Path("/profile.env"),
        ds_version="3.4.1",
        workspace=Path("/workspace"),
        prefix="safety-test",
    )


@pytest.mark.parametrize("trigger", [601, None])
def test_pending_run_lookup_uses_trigger_or_workflow_scope(
    monkeypatch: pytest.MonkeyPatch, trigger: int | None
) -> None:
    journey = _journey()
    journey._workflow_run_unresolved = True
    calls: list[list[str]] = []
    if trigger is None:
        pages: Iterator[list[dict[str, object]]] = iter([[], [{"id": 71}]])

        def rows(argv: list[str]) -> list[dict[str, object]]:
            calls.append(argv)
            return next(pages)

        monkeypatch.setattr(journey, "rows", rows)
    else:
        resolutions: Iterator[dict[str, object]] = iter(
            [
                {
                    "triggerCode": trigger,
                    "instanceResolution": "pending",
                    "totalList": [],
                },
                {
                    "triggerCode": trigger,
                    "instanceResolution": "resolved",
                    "totalList": [{"id": 71}],
                },
            ]
        )

        def data(
            argv: list[str], *, timeout_seconds: float = 60.0
        ) -> dict[str, object]:
            del timeout_seconds
            calls.append(argv)
            return next(resolutions)

        monkeypatch.setattr(journey, "data", data)
        monkeypatch.setattr(
            journey,
            "rows",
            lambda argv: pytest.fail(f"trigger lookup used paginated rows: {argv}"),
        )
    monkeypatch.setattr(
        "tests.live.journey_support.time.monotonic", iter([0.0, 1.0]).__next__
    )
    monkeypatch.setattr("tests.live.journey_support.time.sleep", lambda _: None)
    instance_id = journey.resolve_run(
        {
            "accepted": True,
            "instanceResolution": "pending" if trigger is not None else "unavailable",
            "workflowInstanceIds": [],
            "triggerCode": trigger,
        }
    )

    scope = (
        ["--trigger-code", "601"]
        if trigger is not None
        else ["--workflow", journey.workflow, "--all"]
    )
    expected = ["workflow-instance", "list", "--project", journey.project, *scope]
    assert calls == [expected, expected]
    assert instance_id == 71
    assert journey._workflow_run_unresolved is False
    assert journey._workflow_run_instances == {71}


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (
            {
                "triggerCode": 602,
                "instanceResolution": "resolved",
                "totalList": [{"id": 71}],
            },
            "changed identity",
        ),
        (
            {
                "triggerCode": 601,
                "instanceResolution": "unavailable",
                "totalList": [],
            },
            "omitted its instance resolution",
        ),
        (
            {
                "triggerCode": 601,
                "instanceResolution": "pending",
                "totalList": [{"id": 71}],
            },
            "pending trigger query returned instance rows",
        ),
        (
            {
                "triggerCode": 601,
                "instanceResolution": "resolved",
                "totalList": [],
            },
            "requires exactly one instance",
        ),
    ],
)
def test_trigger_run_lookup_rejects_invalid_resolution_shape(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, object],
    error: str,
) -> None:
    journey = _journey()
    journey._workflow_run_unresolved = True
    monkeypatch.setattr(journey, "data", lambda argv: response)
    monkeypatch.setattr("tests.live.journey_support.time.monotonic", lambda: 0.0)

    with pytest.raises(AssertionError, match=error):
        journey.resolve_run(
            {
                "accepted": True,
                "instanceResolution": "pending",
                "workflowInstanceIds": [],
                "triggerCode": 601,
            }
        )

    assert journey._workflow_run_unresolved is True


def test_workflow_run_timeout_keeps_cleanup_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journey = _journey()

    def timeout(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise subprocess.TimeoutExpired(["dsctl", "workflow", "run"], 1)

    monkeypatch.setattr("tests.live.journey_support.run_dsctl", timeout)

    with pytest.raises(subprocess.TimeoutExpired):
        journey.call(["workflow", "run", journey.workflow])

    assert journey._workflow_run_unresolved is True


def test_unresolved_run_without_instance_refuses_definition_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journey = _journey()
    journey._workflow_run_unresolved = True
    moments = iter([0.0, 121.0])

    monkeypatch.setattr(journey, "rows", lambda argv: [])
    monkeypatch.setattr("tests.live.journey_support.time.monotonic", moments.__next__)
    monkeypatch.setattr("tests.live.journey_support.time.sleep", lambda _: None)

    with pytest.raises(AssertionError, match="outcome is still unresolved"):
        journey._reconcile_uncertain_workflow_run()

    assert journey._workflow_run_unresolved is True
    assert journey.cleanup_evidence["uncertain_workflow_run_reconciled"] is False


def test_unresolved_run_clears_only_after_instance_materializes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journey = _journey()
    journey._workflow_run_unresolved = True
    pages: Iterator[list[dict[str, object]]] = iter(
        [[], [{"id": 71, "state": "SUCCESS"}]]
    )

    monkeypatch.setattr(journey, "rows", lambda argv: next(pages))
    monkeypatch.setattr(
        "tests.live.journey_support.time.monotonic",
        iter([0.0, 1.0]).__next__,
    )
    monkeypatch.setattr("tests.live.journey_support.time.sleep", lambda _: None)

    journey._reconcile_uncertain_workflow_run()

    assert journey._workflow_run_unresolved is False
    assert journey._workflow_run_instances == {71}
    assert journey.cleanup_evidence["uncertain_workflow_run_reconciled"] is True


def test_resolved_run_id_must_still_appear_before_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journey = _journey()
    journey._workflow_run_instances.add(72)
    moments = iter([0.0, 121.0])

    monkeypatch.setattr(journey, "rows", lambda argv: [])
    monkeypatch.setattr("tests.live.journey_support.time.monotonic", moments.__next__)
    monkeypatch.setattr("tests.live.journey_support.time.sleep", lambda _: None)

    with pytest.raises(AssertionError, match="outcome is still unresolved"):
        journey._reconcile_uncertain_workflow_run()

    assert journey._workflow_run_instances == {72}
    assert journey.cleanup_evidence["uncertain_workflow_run_reconciled"] is False
