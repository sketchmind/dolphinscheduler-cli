from typing import Literal

import pytest

from dsctl.errors import ConfigError
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    VersionSupport,
    get_version_support,
    supported_version_metadata,
)
from dsctl.upstream.adapters.ds_3_4_1 import DS341Adapter

EXPECTED_VERSION_METADATA = (
    {
        "server_version": "3.3.2",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
    },
    {
        "server_version": "3.4.0",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "experimental",
        "tested": False,
    },
    {
        "server_version": "3.4.1",
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "support_level": "full",
        "tested": True,
    },
)


def test_version_support_metadata_describes_default_adapter() -> None:
    support = get_version_support("ds_3_4_1")

    assert support.server_version == "3.4.1"
    assert support.contract_version == "3.4.1"
    assert support.family == "workflow-3.3-plus"
    assert support.support_level == "full"
    assert support.tested is True
    assert isinstance(support.adapter, DS341Adapter)
    assert SUPPORTED_VERSIONS == ("3.3.2", "3.4.0", "3.4.1")
    assert supported_version_metadata() == EXPECTED_VERSION_METADATA


def test_compatible_server_versions_reuse_current_contract_adapter() -> None:
    support = get_version_support("3.3.2")

    assert support.server_version == "3.3.2"
    assert support.contract_version == "3.4.1"
    assert support.family == "workflow-3.3-plus"
    assert support.support_level == "experimental"
    assert support.tested is False
    assert isinstance(support.adapter, DS341Adapter)


@pytest.mark.parametrize("support_level", ["full", "legacy_core"])
def test_stable_support_levels_require_live_test_evidence(
    support_level: Literal["full", "legacy_core"],
) -> None:
    with pytest.raises(ValueError, match="requires tested=True"):
        VersionSupport(
            server_version="3.4.0",
            adapter=get_version_support("3.4.1").adapter,
            contract_version="3.4.1",
            family="workflow-3.3-plus",
            support_level=support_level,
            tested=False,
        )


def test_get_version_support_rejects_unknown_versions() -> None:
    with pytest.raises(ConfigError) as exc_info:
        get_version_support("3.2.2")

    assert exc_info.value.details == {
        "version": "3.2.2",
        "supported_versions": "3.3.2, 3.4.0, 3.4.1",
    }
