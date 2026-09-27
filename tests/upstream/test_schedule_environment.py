from __future__ import annotations

import pytest

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.schedule_environment import (
    schedule_environment_inheritance_supported,
    schedule_environment_limitation,
    workflow_environment_inheritance_supported,
)


@pytest.mark.parametrize(
    ("version", "limitation"),
    [
        ("1.3.9", "schedule_environment_unavailable"),
        ("2.0.0", "scheduler_omits_environment"),
        ("3.0.2", "scheduler_omits_environment"),
        ("3.0.3", "task_does_not_inherit_environment"),
        ("3.1.0", "scheduler_omits_environment"),
        ("3.1.1", "scheduler_omits_environment"),
        ("3.1.2", "task_does_not_inherit_environment"),
        ("3.2.1", "task_does_not_inherit_environment"),
        ("3.2.2", None),
        ("3.4.3", None),
    ],
)
def test_schedule_environment_limitation_is_exact(
    version: str, limitation: str | None
) -> None:
    assert schedule_environment_limitation(version) == limitation
    assert schedule_environment_inheritance_supported(version) is (limitation is None)


def test_unknown_schedule_environment_profile_is_rejected() -> None:
    with pytest.raises(UnsupportedFeatureError):
        schedule_environment_inheritance_supported("3.4.4")
    with pytest.raises(UnsupportedFeatureError):
        workflow_environment_inheritance_supported("3.4.4")


def test_workflow_environment_inheritance_uses_task_boundary_only() -> None:
    assert workflow_environment_inheritance_supported("3.1.0") is False
    assert workflow_environment_inheritance_supported("3.2.1") is False
    assert workflow_environment_inheritance_supported("3.2.2") is True
