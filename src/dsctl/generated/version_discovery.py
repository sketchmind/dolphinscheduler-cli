"""Generated connection discovery facts; do not edit by hand."""

from __future__ import annotations

from dataclasses import dataclass

from ._discovery_evidence import SOURCE_EVIDENCE as SOURCE_EVIDENCE
from ._discovery_operations import OPERATION_CONTRACTS as OPERATION_CONTRACTS
from ._discovery_parameters import PARAMETER_CONTRACTS as PARAMETER_CONTRACTS
from ._discovery_profiles import (
    CONTRACT_PROFILES as CONTRACT_PROFILES,
    DISCOVERY_CONTRACT_DIGEST as DISCOVERY_CONTRACT_DIGEST,
    DOCUMENT_PROBES as DOCUMENT_PROBES,
)
from ._discovery_types import (
    ApiContractProfile as ApiContractProfile,
    ApiOperationContract as ApiOperationContract,
    ApiParameterContract as ApiParameterContract,
    DocumentProbe as DocumentProbe,
)


@dataclass(frozen=True)
class ProbeContract:
    """A reviewed GET probe for database-recorded product versions."""

    source: str
    path: str
    version_fields: tuple[str, ...]
    exact_versions: tuple[str, ...]
    success_code: int | None
    route_miss_codes: tuple[int, ...]
    route_miss_versions: tuple[str, ...]


PROBES: tuple[ProbeContract, ...] = (
    ProbeContract(
        source='product_info',
        path='ui-plugins/query-product-info',
        version_fields=('data', 'version'),
        exact_versions=(
            '3.3.1',
            '3.3.2',
            '3.4.0',
            '3.4.1',
            '3.4.2',
            '3.4.3',
        ),
        success_code=0,
        route_miss_codes=(110003,),
        route_miss_versions=(
            '2.0.0',
            '2.0.1',
            '2.0.2',
            '2.0.3',
            '2.0.4',
            '2.0.5',
            '2.0.6',
            '2.0.7',
            '2.0.8',
            '2.0.9',
            '3.0.0',
            '3.0.1',
            '3.0.2',
            '3.0.3',
            '3.0.4',
            '3.0.5',
            '3.0.6',
            '3.1.0',
            '3.1.1',
            '3.1.2',
            '3.1.3',
            '3.1.4',
            '3.1.5',
            '3.1.6',
            '3.1.7',
            '3.1.8',
            '3.1.9',
            '3.2.0',
            '3.2.1',
            '3.2.2',
        ),
    ),
    ProbeContract(
        source='openapi',
        path='v3/api-docs',
        version_fields=('info', 'version'),
        exact_versions=(
            '3.2.0',
            '3.2.1',
            '3.3.1',
            '3.3.2',
            '3.4.0',
            '3.4.1',
            '3.4.2',
            '3.4.3',
        ),
        success_code=None,
        route_miss_codes=(),
        route_miss_versions=(
        ),
    ),
)

# Database product version can differ from the exact release tag.
DATABASE_PRODUCT_VERSIONS: dict[str, str] = {
    '2.0.1': '2.0.1',
    '2.0.2': '2.0.2',
    '2.0.3': '2.0.3',
    '2.0.4': '2.0.4',
    '2.0.5': '2.0.5',
    '2.0.6': '2.0.6',
    '2.0.7': '2.0.7',
    '2.0.8': '2.0.7',
    '2.0.9': '2.0.9',
    '3.0.0': '3.0.0',
    '3.0.1': '3.0.1',
    '3.0.2': '3.0.2',
    '3.0.3': '3.0.2',
    '3.0.4': '3.0.4',
    '3.0.5': '3.0.5',
    '3.0.6': '3.0.6',
    '3.1.0': '3.1.0',
    '3.1.1': '3.1.1',
    '3.1.2': '3.1.2',
    '3.1.3': '3.1.3',
    '3.1.4': '3.1.4',
    '3.1.5': '3.1.5',
    '3.1.6': '3.1.6',
    '3.1.7': '3.1.7',
    '3.1.8': '3.1.8',
    '3.1.9': '3.1.9',
    '3.2.0': '3.2.0',
    '3.2.1': '3.2.1',
    '3.2.2': '3.3.0',
    '3.3.1': '3.3.1',
    '3.3.2': '3.3.2',
    '3.4.0': '3.4.0',
    '3.4.1': '3.4.1',
    '3.4.2': '3.4.2',
    '3.4.3': '3.4.3',
}
