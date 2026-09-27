"""Declarative public image identities for the exact conformance matrix."""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import cast

_CONFORMANCE_IMAGE_CONTRACT_JSON = r"""{
  "api_image_repositories": {
    "1.3.9": "apache/dolphinscheduler",
    "2.0.0": "dsmatrix-local/dolphinscheduler",
    "2.0.1": "apache/dolphinscheduler",
    "2.0.2": "apache/dolphinscheduler",
    "2.0.3": "apache/dolphinscheduler",
    "2.0.4": "dsmatrix-local/dolphinscheduler",
    "2.0.5": "apache/dolphinscheduler",
    "2.0.6": "apache/dolphinscheduler",
    "2.0.7": "dsmatrix-local/dolphinscheduler",
    "2.0.8": "dsmatrix-local/dolphinscheduler",
    "2.0.9": "dsmatrix-local/dolphinscheduler",
    "3.0.0": "apache/dolphinscheduler-api",
    "3.0.1": "apache/dolphinscheduler-api",
    "3.0.2": "dsmatrix-local/dolphinscheduler-api",
    "3.0.3": "apache/dolphinscheduler-api",
    "3.0.4": "apache/dolphinscheduler-api",
    "3.0.5": "apache/dolphinscheduler-api",
    "3.0.6": "apache/dolphinscheduler-api",
    "3.1.0": "apache/dolphinscheduler-api",
    "3.1.1": "apache/dolphinscheduler-api",
    "3.1.2": "dsmatrix-local/dolphinscheduler-api",
    "3.1.3": "apache/dolphinscheduler-api",
    "3.1.4": "apache/dolphinscheduler-api",
    "3.1.5": "apache/dolphinscheduler-api",
    "3.1.6": "apache/dolphinscheduler-api",
    "3.1.7": "apache/dolphinscheduler-api",
    "3.1.8": "apache/dolphinscheduler-api",
    "3.1.9": "apache/dolphinscheduler-api",
    "3.2.0": "apache/dolphinscheduler-api",
    "3.2.1": "apache/dolphinscheduler-api",
    "3.2.2": "apache/dolphinscheduler-api",
    "3.3.1": "apache/dolphinscheduler-api",
    "3.3.2": "apache/dolphinscheduler-api",
    "3.4.0": "apache/dolphinscheduler-api",
    "3.4.1": "apache/dolphinscheduler-api",
    "3.4.2": "apache/dolphinscheduler-api",
    "3.4.3": "apache/dolphinscheduler-api"
  },
  "managed_api_versions": [
    "1.3.9", "2.0.0", "2.0.4", "2.0.7", "2.0.8", "2.0.9", "3.0.2", "3.1.2"
  ],
  "managed_lock_presence": {
    "1.3.9": {
      "base_manifest": false,
      "binary_sha512": false,
      "published_manifest": true,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": false
    },
    "2.0.0": {
      "base_manifest": true,
      "binary_sha512": false,
      "published_manifest": false,
      "schema_sha256": true,
      "source_commit": true,
      "source_sha512": true
    },
    "2.0.4": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    },
    "2.0.7": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    },
    "2.0.8": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    },
    "2.0.9": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    },
    "3.0.2": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    },
    "3.1.2": {
      "base_manifest": true,
      "binary_sha512": true,
      "published_manifest": false,
      "schema_sha256": false,
      "source_commit": true,
      "source_sha512": true
    }
  },
  "managed_provenance_labels": [
    "org.apache.dolphinscheduler.matrix.base-digest",
    "org.apache.dolphinscheduler.matrix.binary-sha512",
    "org.apache.dolphinscheduler.matrix.source-commit",
    "org.apache.dolphinscheduler.matrix.source-sha512",
    "org.apache.dolphinscheduler.matrix.source-tag"
  ],
  "schema_version": 1
}"""
_CONTRACT = cast("dict[str, object]", json.loads(_CONFORMANCE_IMAGE_CONTRACT_JSON))
API_IMAGE_REPOSITORIES = MappingProxyType(
    cast("dict[str, str]", _CONTRACT["api_image_repositories"])
)
MANAGED_API_VERSIONS = frozenset(cast("list[str]", _CONTRACT["managed_api_versions"]))
MANAGED_LOCK_PRESENCE = MappingProxyType(
    {
        version: MappingProxyType(fields)
        for version, fields in cast(
            "dict[str, dict[str, bool]]", _CONTRACT["managed_lock_presence"]
        ).items()
    }
)
MANAGED_PROVENANCE_LABELS = frozenset(
    cast("list[str]", _CONTRACT["managed_provenance_labels"])
)


def expected_api_image_repository(ds_version: str) -> str:
    """Return the only API-image repository accepted for one exact release."""
    try:
        return API_IMAGE_REPOSITORIES[ds_version]
    except KeyError as error:
        message = f"No conformance API image repository is declared for DS {ds_version}"
        raise ValueError(message) from error


__all__ = [
    "API_IMAGE_REPOSITORIES",
    "MANAGED_API_VERSIONS",
    "MANAGED_LOCK_PRESENCE",
    "MANAGED_PROVENANCE_LABELS",
    "expected_api_image_repository",
]
