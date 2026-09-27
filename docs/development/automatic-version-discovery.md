# Automatic Exact Version Discovery

This document records the metadata-only implementation and its 2026-09-08
installed-wheel receipt. The current [contract read discovery extension](contract-read-discovery.md)
adds candidate-based reads without changing the seven exact metadata mappings.
The historical results below are not acceptance evidence for that extension.

Implemented in source commit `38749c2e4d39c738e90f7234aae9dedf4aa043e2`.
The immutable candidate was built from a clean Git archive:

- Wheel: `dolphinscheduler_cli-0.4.0-py3-none-any.whl`
- SHA-256: `44ad3bdabe0d0778826bfca54fef311b21d55c4c5dbd802c1422eec8b8443764`
- Local evidence directory: `build/release-candidates/version-discovery-2026-09-08/`

## Product Behavior in the Recorded Candidate

URL and token are sufficient for supported deployments that expose reliable
version metadata. Omitted `DS_VERSION` and `DS_VERSION=auto` have the same
runtime behavior. An explicit exact version remains authoritative.

One invocation resolves the target before capability checks, task authoring and
runtime binding. Remote commands and automatic `doctor` use fresh observations;
local discovery can reuse a credential-scoped observation for five minutes.
Failed remote discovery never falls back to an old observation or a default
server profile. Help and context remain offline, and completely untargeted
local authoring keeps the documented `3.4.1` baseline.

See [Configuration](../user/configuration.md#version-selection) for public
behavior and [Architecture](architecture.md#target-version-resolution) for
module ownership. No new command catalog, domain operation, CRUD facade, or
provisional runtime adapter was introduced.

## Source and Runtime Findings

Generated bootstrap facts derive product-information paths and response fields
from full controller snapshots. OpenAPI configuration, database version
initialization, and the legacy plugin route collision retain exact source
membership and evidence.

- Older plugin controllers interpret `query-product-info` as a plugin ID. Only
  their reviewed `110003` result with absent/null data permits fallback to
  OpenAPI; arbitrary business errors do not.
- Swagger API group labels such as `V1` and `V2` do not identify a DS release.
- Stock `3.2.2` writes `3.3.0` into `sql/soft_version` and database initialization
  SQL. Its REST metadata cannot establish exact `3.2.2`; no alias is inferred.
- Product information is a small request; the first OpenAPI document can take
  longer to initialize. Its read timeout is bounded at 30 seconds, with
  five-second connection timeouts and an overall discovery deadline.
- Invalid tokens return HTTP 401 in the reviewed authentication interceptors;
  the CLI advises checking the token, expiration and account status.

## Installed Server Acceptance

On 2026-09-08, the installed candidate wheel ran 45 CLI processes against the
existing 15-version matrix. Only read requests were used, and matrix
configuration was mounted read-only. The receipt contains no tokens or project
payloads.

| Exact releases | Explicit project read | Automatic selection |
| --- | --- | --- |
| `1.3.9`, `2.0.0`, `2.0.9`, `3.0.0`, `3.0.6`, `3.1.0`, `3.1.9` | Passed, all 7 | Expected rejection: no reliable product-version field |
| `3.2.0`, `3.2.1` | Passed, both | Passed through OpenAPI |
| `3.2.2` | Passed | Expected rejection: reports `3.3.0`; set `DS_VERSION=3.2.2` |
| `3.3.1`, `3.3.2`, `3.4.0`, `3.4.1`, `3.4.2` | Passed, all 5 | Passed through product information |

All seven automatically identified profiles also passed local cache and fresh
`doctor` version checks. The invalid-token negative case returned the expected
credential-specific error. Experimental profiles retained their existing
`doctor` warnings; `3.4.1` remained the only stable profile.

The receipt is `live-version-discovery.json` in the evidence directory. It binds
these bounded observations to the wheel digest. It is not a release-promotion
or mutation-conformance receipt, and it does not renew historical release-gate
receipts.

## Quality and Packaging

The complete development gate passed: 13,582 portable tests, 1,045 source-contract
tests and three source-rebuild tests (14,630 total). All seven architecture
contracts, generated freshness, packaging/governance checks, Ruff, formatting,
Codespell, whole-project mypy (2,115 files) and the dedicated generated typing
check (1,112 files) passed. Existing generated artifacts were unchanged; the
new bootstrap manifest was added by atomic regeneration.

Minimum-dependency installation used Python 3.11.15. All 1,533 installed package
files matched wheel bytes; the installed Python module set also matched exactly.
The 315 domain/profile combinations, all 174 command helps, 135 local CLI
processes, 116 installed tests and 10 real-process loopback tests passed with
warnings treated as errors and no workspace imports. Server acceptance used
Python 3.12.13 and the same wheel digest.
