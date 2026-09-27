from copy import deepcopy

import pytest

from dsctl.errors import ConfirmationRequiredError
from dsctl.output import JsonValue
from dsctl.services._schedule_support import (
    require_high_frequency_confirmation,
    schedule_confirmation_data,
)
from dsctl.services.schedule_analysis import build_schedule_preview_data


def _payload() -> dict[str, JsonValue]:
    return {
        "schedule_id": 1,
        "schedule": {
            "crontab": "0 * * * * ?",
            "startTime": "2024-01-01 00:00:00",
            "endTime": "2025-01-01 00:00:00",
            "timezoneId": "Asia/Shanghai",
            "environmentCode": 0,
        },
    }


@pytest.mark.parametrize(
    "action", ["schedule.create", "schedule.update", "workflow.create"]
)
@pytest.mark.parametrize(
    "later_times",
    [
        ["2024-01-01 00:01:00", "2024-01-01 00:02:00"],
        # An irregular cron's sample can also have a different shortest interval.
        ["2024-01-02 00:00:00", "2024-01-02 00:05:00", "2024-01-02 00:10:00"],
    ],
)
def test_same_mutation_confirmation_survives_preview_refresh(
    action: str, later_times: list[str]
) -> None:
    initial = build_schedule_preview_data(
        ["2024-01-01 00:00:00", "2024-01-01 00:01:00"]
    )
    confirmation = schedule_confirmation_data(
        action=action, preview=initial, schedule_payload=_payload()
    )
    require_high_frequency_confirmation(
        action=action,
        confirmation=confirmation["token"],
        preview=build_schedule_preview_data(later_times),
        schedule_payload=_payload(),
    )


@pytest.mark.parametrize(
    "changed_field",
    [
        "action",
        "schedule_id",
        "crontab",
        "startTime",
        "endTime",
        "timezoneId",
        "environmentCode",
        "threshold",
    ],
)
def test_changed_mutation_or_policy_requires_new_confirmation(
    changed_field: str,
) -> None:
    preview = build_schedule_preview_data(
        ["2024-01-01 00:00:00", "2024-01-01 00:01:00"]
    )
    payload = _payload()
    confirmation = schedule_confirmation_data(
        action="schedule.update", preview=preview, schedule_payload=payload
    )
    changed = deepcopy(payload)
    action = "schedule.update"
    if changed_field == "action":
        action = "schedule.create"
    elif changed_field == "schedule_id":
        changed["schedule_id"] = 2
    elif changed_field == "threshold":
        preview = build_schedule_preview_data(preview["times"], threshold_seconds=300)
    else:
        schedule = changed["schedule"]
        assert isinstance(schedule, dict)
        schedule[changed_field] = 7 if changed_field == "environmentCode" else "changed"
    with pytest.raises(ConfirmationRequiredError):
        require_high_frequency_confirmation(
            action=action,
            confirmation=confirmation["token"],
            preview=preview,
            schedule_payload=changed,
        )
