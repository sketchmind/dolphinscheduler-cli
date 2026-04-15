from __future__ import annotations

from datetime import UTC, datetime
from itertools import pairwise
from typing import Literal, TypedDict

PREVIEW_LIMIT = 5
HIGH_FREQUENCY_CONFIRM_THRESHOLD_SECONDS = 600
SCHEDULE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
RiskLevel = Literal["none", "high"]


class ScheduleRiskAnalysis(TypedDict):
    """Structured schedule risk analysis derived from preview fire times."""

    preview_count: int
    preview_limit: int
    min_interval_seconds: int | None
    risk_level: RiskLevel
    risk_type: str | None
    requires_confirmation: bool
    threshold_seconds: int
    reason: str | None


class SchedulePreviewData(TypedDict):
    """Preview payload returned by schedule preview and risk checks."""

    times: list[str]
    count: int
    analysis: ScheduleRiskAnalysis


def build_schedule_preview_data(
    times: list[str],
    *,
    threshold_seconds: int = HIGH_FREQUENCY_CONFIRM_THRESHOLD_SECONDS,
) -> SchedulePreviewData:
    """Build preview times plus one risk analysis summary."""
    rendered_times = list(times)
    min_interval_seconds = _min_interval_seconds(rendered_times)
    requires_confirmation = (
        min_interval_seconds is not None and min_interval_seconds < threshold_seconds
    )
    risk_level: RiskLevel = "high" if requires_confirmation else "none"
    return {
        "times": rendered_times,
        "count": len(rendered_times),
        "analysis": {
            "preview_count": len(rendered_times),
            "preview_limit": PREVIEW_LIMIT,
            "min_interval_seconds": min_interval_seconds,
            "risk_level": risk_level,
            "risk_type": ("high_frequency_schedule" if requires_confirmation else None),
            "requires_confirmation": requires_confirmation,
            "threshold_seconds": threshold_seconds,
            "reason": _risk_reason(
                min_interval_seconds=min_interval_seconds,
                threshold_seconds=threshold_seconds,
            ),
        },
    }


def confirmed_high_frequency_warning(preview: SchedulePreviewData) -> str | None:
    """Return one warning after a high-frequency schedule was explicitly confirmed."""
    analysis = preview["analysis"]
    min_interval_seconds = analysis["min_interval_seconds"]
    if min_interval_seconds is None or not analysis["requires_confirmation"]:
        return None
    return (
        "confirmed high-frequency schedule risk: minimum interval is "
        f"{min_interval_seconds} seconds and the confirmation threshold is "
        f"{analysis['threshold_seconds']} seconds"
    )


def _min_interval_seconds(times: list[str]) -> int | None:
    if len(times) < 2:
        return None
    parsed = [
        datetime.strptime(value, SCHEDULE_TIME_FORMAT).replace(tzinfo=UTC)
        for value in times
    ]
    intervals = [
        int((current - previous).total_seconds())
        for previous, current in pairwise(parsed)
    ]
    if not intervals:
        return None
    return min(intervals)


def _risk_reason(
    *,
    min_interval_seconds: int | None,
    threshold_seconds: int,
) -> str | None:
    if min_interval_seconds is None or min_interval_seconds >= threshold_seconds:
        return None
    return (
        "This schedule triggers more frequently than the confirmation threshold "
        f"of {threshold_seconds} seconds."
    )
