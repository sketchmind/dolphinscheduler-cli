# Live Testing

## Purpose

Live tests validate `dsctl` against a real DolphinScheduler cluster through the
same REST surface and CLI commands that users rely on.

They exist to catch issues that fixture-based tests cannot prove:

- auth and permission mismatches
- DS-native request/response contract drift
- async runtime state transitions
- schedule materialization and execution timing
- plugin and cluster-topology differences

Live tests are opt-in, slower, and potentially destructive. They complement
unit tests, command tests, adapter tests, and generator checks. They do not
replace them.

## Current Reality

The current offline suite is strong for local correctness, but it is still
offline:

- command tests use `CliRunner`
- service tests use fake adapters and mock runtimes
- adapter tests use mock transports

That is necessary, but it is not sufficient to prove that `dsctl` works
correctly against a real DolphinScheduler cluster.

Offline tests can prove:

- local validation logic
- output envelopes
- resolver and pagination logic
- type contracts and import boundaries
- handwritten request shaping against mocked transports

Offline tests cannot fully prove:

- the real DS request and response contract
- auth and permission behavior
- cluster-specific defaults and restrictions
- runtime state transitions and scheduling behavior
- plugin availability and capability differences

Therefore, no cluster-interacting surface is considered fully validated until
it has live coverage.

## Offline Gate Before Live Runs

Run the normal offline quality gate before spending time on a real cluster.

Minimum expectation:

- `python tools/check_quality_gate.py`
- `python tools/check_quality_gate.py --include-live`
  Use this only after exporting the required live-test environment variables.
  The quality gate now fails fast when the live enable flags or admin live
  profile are missing, so `--include-live` cannot silently pass by skipping the
  real-cluster suite.
- `ruff check src tests tools`
- `ruff format --check src tests tools`
- `mypy src tests tools`
- `lint-imports`
- `python tools/check_generated_freshness.py`
- `python tools/check_error_translation_governance.py`
- relevant `pytest` targets

When the change touches DS-facing failure handling, review the current error
model before or alongside the live run:

- `python tools/extract_ds_api_error_inventory.py --format summary`
- `python tools/audit_dsctl_error_translation.py --format summary`
- `python tools/extract_dsctl_error_translation_matrix.py --format summary`

Those tools answer different questions:

- inventory: what upstream DS exposes in source
- audit: where `dsctl` currently translates or leaves errors raw
- matrix: which DS codes currently map to which `dsctl` error types

## Core Principles

- Use `dsctl` itself as the black-box client. Do not bypass the CLI with
  handwritten HTTP calls unless the purpose is debugging a failing live test.
- Stay REST-only. Do not use Py4J, PyDolphinScheduler, or direct database
  mutation in the test path.
- Preserve DS-native semantics. When a live test exposes a mismatch, prefer
  fixing the generator, adapter, or service layer over inventing CLI-only
  behavior.
- Separate bootstrap authority from business-user execution.
- Make every live run traceable and cleanable with unique resource prefixes.
- Treat cluster-specific or plugin-specific capabilities as optional and
  explicitly labeled, not as silent assumptions.

## Coverage Policy

### Rule 1: Every Cluster-Interacting Command Needs Live Coverage

Every command that sends a request to a real DS cluster must have at least one
live test.

Grouped scenario coverage is allowed for efficiency, but grouping does not
remove the requirement. If a table row below lists multiple subcommands, each
listed subcommand still needs at least one live assertion somewhere in the
suite.

This rule is also enforced offline by a static governance test that compares
registered cluster-interacting command paths with the command paths referenced
from `tests/live/`.

### Rule 2: Local-Only Commands Do Not Need Live Coverage

Commands that do not talk to the cluster should stay covered by offline tests.

### Rule 3: Mutations Need More Than One Happy-Path Check

For cluster-interacting mutations, the minimum live obligation is:

- one successful mutation case
- one read-back assertion proving the mutation took effect
- one negative, permission, or lifecycle-precondition case where applicable
- cleanup or idempotent teardown

### Rule 4: Runtime And Scheduling Need Scenario Coverage

For runtime and schedule commands, one shallow contract check is not enough.
These surfaces also need live scenario coverage because correctness depends on
time, state transitions, and DS scheduler behavior.

## Definition Of Done

### Local-Only Surface

Done when:

- offline tests exist
- command and service behavior is covered
- output contract is stable

### Cluster-Interacting Read Surface

Done when:

- offline tests exist
- at least one live contract test exists against a real cluster
- the live case proves the projected payload shape and selector semantics

### Cluster-Interacting Mutation Surface

Done when:

- offline tests exist
- live round-trip coverage exists
- live negative or precondition coverage exists
- cleanup is proven

### Runtime Or Schedule Surface

Done when:

- offline tests exist
- live contract coverage exists
- live scenario coverage exists for state transitions

## Test Personas

Live testing should use at least two personas.

### `admin-bootstrap`

Use the cluster-provided admin token only for:

- initial connectivity verification
- creating or validating the tenant, user, and access token used by the test
  suite
- explicitly admin-only governance tests such as user, tenant, token, and
  permission administration

Do not use the admin token for the main workflow, schedule, and runtime test
paths. That would hide real permission problems and produce a false sense of
coverage.

### `etl-developer`

Use a normal non-admin user token for the main live suite:

- project lifecycle
- workflow authoring and reads
- workflow run and instance inspection
- task-instance inspection and control
- schedule lifecycle and schedule-triggered runtime behavior
- resource, datasource, namespace, and other day-to-day ETL operations as
  allowed by cluster policy

This persona should reflect the real operator or ETL developer experience.

## Bootstrap Flow

Keep separate env files for the two personas.

Example `live-admin.env`:

```dotenv
DS_API_URL=http://cluster.example/dolphinscheduler
DS_API_TOKEN=admin-token
```

Example `live-etl.env`:

```dotenv
DS_API_URL=http://cluster.example/dolphinscheduler
DS_API_TOKEN=etl-user-token
```

The env files passed to `dsctl --env-file` should contain only runtime profile
settings. Live-harness metadata belongs in the process environment or in the
source file consumed by the harness before it materializes a clean temporary
profile.

Example harness settings:

```bash
export DSCTL_RUN_LIVE_TESTS=1
export DSCTL_RUN_LIVE_ADMIN_TESTS=1
export DS_LIVE_ADMIN_ENV_FILE=/path/to/local/live-admin.env
export DS_LIVE_TENANT_CODE=dsctl-live
```

`DS_LIVE_TENANT_CODE` is live-harness metadata only. The CLI runtime itself does
not read tenant defaults from profile config.

Optional datasource lifecycle coverage requires a disposable external
datasource target. Configure it through process environment variables, not
checked-in env files:

```bash
export DS_LIVE_DATASOURCE_HOST=database.example
export DS_LIVE_DATASOURCE_PORT=3306
export DS_LIVE_DATASOURCE_DATABASE=dolphinscheduler
export DS_LIVE_DATASOURCE_USER=dsctl_live
export DS_LIVE_DATASOURCE_PASSWORD='...'
```

`DS_LIVE_DATASOURCE_TYPE` defaults to `MYSQL`. If any required datasource
setting is absent, the optional datasource lifecycle test is skipped.

Recommended bootstrap flow:

1. Validate connectivity with the admin token.
2. Create or verify a dedicated live-test tenant.
3. Create or verify a dedicated non-admin live-test user.
4. Create an access token for that user through `dsctl`.
5. Persist the user token into a separate env file.
6. Run the main live suite with the non-admin env file.

Minimal bootstrap commands:

```bash
dsctl --env-file live-admin.env doctor
dsctl --env-file live-admin.env tenant create \
  --tenant-code dsctl-live \
  --queue default
dsctl --env-file live-admin.env user create \
  --user-name dsctl_live_etl \
  --password 'change-me-now' \
  --email dsctl-live@example.com \
  --tenant dsctl-live \
  --state 1
dsctl --env-file live-admin.env access-token create \
  --user dsctl_live_etl \
  --expire-time '2027-12-31 00:00:00'
```

The created token should then be written into `live-etl.env`. The main suite
should not fall back to the admin env file.

## Current Surface Matrix

The tables below define the minimum live obligation for the current CLI
surface.

### Local-Only Or Local-First Surfaces

These commands do not require live coverage unless they grow cluster I/O in the
future.

| Surface | Commands | Live required | Notes |
| --- | --- | --- | --- |
| meta | `version`, `context` | No | local metadata and merged local context |
| schema | `schema`, `capabilities` | No | static CLI self-description |
| enum | `enum names`, `enum list` | No | generated enum metadata |
| use | `use project|workflow|--clear` | No | local context persistence |
| lint | `lint workflow FILE` | No | local validation only |
| template | `template workflow|workflow-patch|workflow-instance-patch|params|environment|cluster|datasource|task` | No | local template rendering |

### Meta And Diagnostics

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| doctor | `doctor` | `admin-bootstrap` and `etl-developer` | Yes | healthy preflight and broken-auth or broken-config variant |
| task-type | `task-type list` | `etl-developer` | Yes | real task-type discovery and category projection |
| task-type | `task-type get|schema` | No profile needed | No | local authoring summary and schema contracts |
| monitor | `monitor health`, `monitor server`, `monitor database` | `admin-bootstrap` | Yes | remote health, server, and database payloads reachable and shaped |
| audit | `audit list`, `audit model-types`, `audit operation-types` | `admin-bootstrap` or delegated governance user | Yes | remote filter metadata plus list query with real payload shape |

### Governance Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| environment | `environment list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip plus not-found or validation case |
| cluster | `cluster list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip plus delete cleanup |
| datasource | `datasource list|get|create|update|delete|test` | delegated governance user | Yes | CRUD round-trip and real connectivity test where the capability exists |
| namespace | `namespace list|get|available|create|delete` | delegated governance user | Yes | list and availability plus create/delete round-trip |
| resource | `resource list|view|upload|create|mkdir|download|delete` | `etl-developer` | Yes | file and directory lifecycle with content round-trip |
| queue | `queue list|get|create|update|delete` | `admin-bootstrap` or delegated governance user | Yes | CRUD round-trip plus permission boundary |
| worker-group | `worker-group list|get|create|update|delete` | `admin-bootstrap` or delegated governance user | Yes | CRUD round-trip plus selector correctness |
| alert-plugin | `alert-plugin list|get|definition list|schema|create|update|delete|test` | `admin-bootstrap` or delegated governance user | Yes | definition/schema/read paths plus create/update/delete and plugin test where installed |
| alert-group | `alert-group list|get|create|update|delete` | delegated governance user | Yes | CRUD round-trip and referenceable group payload shape |
| tenant | `tenant list|get|create|update|delete` | `admin-bootstrap` | Yes | CRUD round-trip plus non-admin denial case |
| user | `user list|get|create|update|delete|grant project|datasource|namespace|revoke project|datasource|namespace` | `admin-bootstrap` | Yes | CRUD plus grant/revoke effect and non-admin denial case |
| access-token | `access-token list|get|create|update|delete|generate` | `admin-bootstrap` for bootstrap, `etl-developer` for self-use verification | Yes | token lifecycle plus created token actually authenticates against the cluster |

### Project Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| project | `project list|get|create|update|delete` | `etl-developer` | Yes | CRUD round-trip and context-independent selection |
| project-parameter | `project-parameter list|get|create|update|delete` | `etl-developer` | Yes | CRUD round-trip inside one live project |
| project-preference | `project-preference get|update|enable|disable` | `etl-developer` | Yes | get/update plus enable/disable state reflection |
| project-worker-group | `project-worker-group list|set|clear` | project owner or delegated governance user | Yes | assignment effect visible on read-back |

### Design And Runtime Surfaces

| Surface | Commands | Default persona | Live required | Minimum live coverage |
| --- | --- | --- | --- | --- |
| workflow | `workflow list|get|describe|digest|create|edit|online|offline|run|run-task|backfill` | `etl-developer` | Yes | authoring, read-back, release-state transition, run, task-scoped run, backfill dry-run, and dry-run consistency |
| task | `task list|get|update` | `etl-developer` | Yes | live task projection and safe update reflected by later reads |
| schedule | `schedule list|get|preview|explain|create|update|delete|online|offline` | `etl-developer` | Yes | preview and explain aligned with create/update, and online schedules produce real workflow instances |
| workflow-instance | `workflow-instance list|get|parent|digest|edit|watch|stop|rerun|recover-failed|execute-task` | `etl-developer` | Yes | instance lifecycle transitions, finished-instance DAG edit semantics, runtime control, and parent/sub-workflow relation reads under real runtime conditions |
| task-instance | `task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop` | `etl-developer` | Yes | instance inspection, child relation reads, log retrieval, and control actions under real runtime conditions |

### Notes On Capability-Gated Resources

Some resources still require live coverage, but only in a compatible cluster:

- datasource connection tests need a reachable backend
- alert-plugin tests need installed plugin instances or plugin backends
- namespace, resource, environment, and cluster operations may depend on deployment
  topology and storage configuration

The rule is not “skip forever”. The rule is:

- every cluster-interacting command needs live coverage
- capability-gated commands may run in a dedicated compatible environment
- any skip must be explicit and explained by missing cluster capability

## Current Coverage Snapshot

The current live suite covers these black-box CLI files:

- `tests/live/test_preflight.py`
- `tests/live/test_admin_surfaces.py`
- `tests/live/test_governance_surfaces.py`
- `tests/live/test_governance_optional_surfaces.py`
- `tests/live/test_project_surfaces.py`
- `tests/live/test_runtime_surfaces.py`
- `tests/live/test_runtime_control_surfaces.py`
- `tests/live/test_schedule_surfaces.py`
- `tests/live/test_workflow_lineage_surfaces.py`
- `tests/live/test_workflow_runtime_surfaces.py`

Current verified coverage includes:

- preflight: `doctor`, `monitor health`, ETL token bootstrap, and non-admin
  denial
- admin read surfaces: `monitor server`, `monitor database`, `audit list`,
  `audit model-types`, `audit operation-types`
- admin governance: `access-token`, `queue`, `worker-group`, `tenant`, `user`
  plus project grant effects
- optional governance: `datasource`, `alert-plugin`, `alert-group`, and
  namespace capability/error paths
- project surfaces: `task-type`, `project`, `project-parameter`,
  `project-preference`, `project-worker-group`
- runtime-adjacent governance: `cluster`, `environment`, `resource`
- workflow runtime surfaces: `workflow`, `task`, `workflow-instance`,
  `task-instance`, parent/sub-workflow relation reads, finished-instance DAG
  update with and without definition sync, schedule-triggered runtime,
  workflow lineage, and task-group queue controls

The current shared cluster now passes:

- `32` live tests across the files above

## Current Environment Notes

These findings came from the current shared cluster and should guide the next
live additions.

- `resource list` must send `searchVal=""` when no search term is provided.
  Treat this as an upstream 3.4.1 contract quirk, not a CLI feature choice.
- `resource view` is not reliable through the DS view endpoint because the
  upstream controller misuses the `limit` parameter. The adapter now reads the
  download endpoint and applies the line window client-side.
- `task-instance list` in DS 3.4.1 is backed by the project-scoped
  `GET /projects/{projectCode}/task-instances` paging query. The CLI narrows
  the common per-run inspection path by sending `workflowInstanceId`, and it
  uses the same path for broader project-scoped runtime triage filters.
  Workflow-definition filtering is intentionally not exposed here because the
  upstream BATCH query does not reliably apply `workflowDefinitionName`.
- `workflow describe` returns one root sentinel relation with
  `preTaskCode=0`. That row is part of the DS DAG encoding and should not be
  confused with a user-authored dependency edge.
- `project delete` can briefly return DS result code `10137` immediately after
  `workflow delete`. Treat this as a short eventual-consistency window and
  retry cleanup before declaring failure.
- If a newly created workflow instance reports `SUCCESS` but produces zero
  `task-instance` rows, inspect the compiled `taskDefinitionJson` before
  blaming the runtime read paths. In DS 3.4.1, `taskDefinitionJson.flag` must
  be emitted as the DS-native enabled state (`YES`). Emitting the disabled
  state (`NO`) yields a valid-looking workflow definition whose DAG nodes are
  marked forbidden, so the workflow completes without creating task instances.
- generated boolean fields such as Java `isDirectory` map to DS wire names like
  `directory`. The generator must preserve the Python attribute while emitting
  the correct alias.
- `PageInfo.currentPage` is nullable in real DS responses and must stay
  nullable in generated contracts.
- `cluster update` may require a valid live kubeconfig even when `cluster
  create` succeeds. The current live test accepts either a successful update or
  the stable upstream DS result code `120024`. This is an environment precondition,
  not a reason to weaken the CLI contract.
- schedule create/preview/explain must use Quartz-style cron expressions in the
  current 3.4.1 cluster. The live suite uses forms such as `0 * * * * ?`
  rather than five-field cron strings.
- schedule create also needs the workflow definition to be `ONLINE`, and the
  current cluster requires explicit `tenantCode` plus a valid nonzero
  `environmentCode`.
- `alert-plugin definition list` discovers supported plugin definitions, while
  `alert-plugin schema PLUGIN` fetches the full DS UI parameter form for one
  definition when the upstream detail endpoint exposes it.
- `alert-plugin test` against the current Script plugin returns DS result code
  `110014` when no executable script backend is configured. This is a valid
  cluster capability failure, not a transport error.
- positive `namespace create/delete` coverage still depends on compatible
  Kubernetes integration. The current shared cluster returns DS result code
  `1300006` for namespace creation because no usable K8s namespace backend is
  configured.
- workflow-instance stop and task-instance stop/savepoint are request surfaces,
  not guaranteed immediate state transitions. In the current cluster they
  accept the request, while the eventual runtime may still complete with
  `SUCCESS` or linger in intermediate states such as `READY_STOP`.
- `workflow-instance execute-task` on a completed successful instance can push
  that same instance back into `FAILURE` before `workflow-instance rerun`
  returns it to `SUCCESS`. Live assertions should follow the real state machine
  instead of assuming a no-op on finished instances.
- `workflow-instance edit` in DS 3.4.1 is a finished-instance save path, not
  a live-definition edit path. The server requires final-state instances with
  usable `dagData`; `syncDefine=false` keeps the current workflow definition
  unchanged, while `syncDefine=true` also persists the saved DAG back onto the
  definition and bumps its version.
- positive `task-group queue force-start/set-priority` coverage requires real
  tasks bound with `taskGroupId/taskGroupPriority`. The CLI workflow YAML now
  supports that path directly, and the live suite uses it instead of any
  database shortcut.

## What A Professional Live Suite Should Cover

The upstream DS tests are a strong starting point, but a professional CLI live
suite should cover more than basic create/list/delete flows.

### 1. Connectivity and Authentication

- `doctor` and `monitor health`
- invalid token behavior
- expired token behavior
- permission denied behavior for non-admin users

### 2. Governance Bootstrap and Permission Boundaries

- tenant, user, and access-token lifecycle under the admin persona
- grant and revoke flows where the cluster policy allows them
- negative tests proving the ETL persona cannot perform admin-only mutations

### 3. Definition Lifecycle

- project create/get/list/update/delete
- workflow create, get, describe, digest, online, offline
- task read and safe task update paths
- resource upload/view/download/delete

### 4. Runtime Lifecycle

- workflow run and backfill dry-run
- workflow-instance list/get/digest/edit/watch
- task-instance list/get/log
- stop, rerun, recover-failed, execute-task

### 5. Schedule Semantics

- create, preview, explain, online, offline, update, delete
- schedule-generated workflow instances actually appear
- next-fire-time or preview output matches server behavior closely enough for
  safe operator use
- high-frequency schedule warning and confirmation paths

### 6. Negative and Error Paths

- not-found selectors
- offline-only and online-only lifecycle violations
- delete while still referenced or still online
- malformed workflow spec rejected by lint or create
- empty result pages and out-of-range pagination

### 7. Recovery and State Transitions

- paused, stopped, rerun, and recover-failed runtime transitions
- sub-workflow behavior
- schedule on/off toggling under existing runtime load
- behavior after partially failed mutations

### 8. Real Data and Encoding Edges

- Unicode names and descriptions
- time zone handling
- date/time input round-tripping
- large payloads and large page sizes

### 9. Observability and Diagnostics

- command warnings are useful and specific
- `resolved` metadata is sufficient to explain what the CLI actually selected
- error `suggestion` values point to concrete next actions

### 10. Cleanup Robustness

- resources created during a failing run are still discoverable and removable
- cleanup remains idempotent
- orphaned resources are reported, not silently ignored

## Recommended Suite Tiers

A practical live program should be tiered.

### Tier 0: Preflight

Run on every live invocation.

- `doctor`
- `monitor health`
- current user identity and permission sanity checks
- unique run prefix generation

### Tier 1: Fast Smoke

Run in manual verification and CI against a stable shared cluster.

- project lifecycle
- workflow create and get
- workflow run and successful completion
- workflow backfill dry-run
- workflow-instance and task-instance read paths

### Tier 2: Developer Lifecycle

Run manually and before releases.

- context-aware selection
- workflow edit
- schedule preview, explain, create, online, offline
- resource and datasource basics when available

### Tier 3: Runtime Semantics

Run nightly or before significant runtime changes.

- rerun
- recover-failed
- execute-task
- stop and watch semantics
- sub-workflow and dependency-sensitive flows

### Tier 4: Governance and Admin

Run separately because it requires elevated credentials.

- user, tenant, token administration
- grants and revokes
- admin-only negative tests

### Tier 5: Optional Capability Suites

Run only when the cluster provides the dependency.

- datasource-specific suites
- alert plugin suites
- namespace and resource integrations
- task-plugin-specific workflow suites

## Execution Model

Recommended markers:

- `live`
- `live_admin`
- `live_developer`
- `destructive`
- `slow`
- `optional_capability`

Recommended run naming:

- one unique prefix per run, such as `dsctl-live-20260412-153000-ab12`
- use that prefix in tenant codes, project names, workflow names, and resource
  paths

Recommended cleanup policy:

- register cleanup as soon as a resource is created
- cleanup should run in reverse dependency order
- cleanup failures should fail the suite and print exact leftover identifiers

## Execution Policy

The live-testing program should usually proceed without conversational pauses.

### Continue Without Asking

Proceed autonomously when the next step is a straightforward execution of the
documented roadmap:

- creating the live harness
- adding live markers, fixtures, and helper utilities
- adding live contract cases for already-decided command surfaces
- running live tests against the provided cluster credentials
- debugging failures and fixing the lowest correct layer
- updating docs and offline regression coverage after a live finding

### Stop And Ask Only When Truly Blocked

Pause only when one of these conditions is true:

- required credentials or env files are missing
- the shared cluster contains safety constraints that make destructive cleanup
  uncertain
- a capability-gated surface depends on infrastructure that is unavailable and
  no compatible environment is known
- the observed upstream behavior creates a real product decision rather than an
  implementation bug
- a failure suggests conflicting interpretations of DS semantics that cannot be
  resolved from upstream source or cluster behavior alone

### Default Working Assumption

If none of the stop conditions above applies, continue the live-testing
roadmap and treat the work as implementation, not discussion.

## Failure Analysis And Repair Principles

When a live test fails, do not jump straight to patching the command that
surfaced the failure. Classify the failure first.

### Step 1: Reproduce And Narrow

- rerun the exact CLI command with the same env file
- inspect `doctor` and health output
- confirm whether the failure is deterministic
- confirm whether the failure happens only for one persona or both

### Step 2: Classify The Failure

Typical buckets:

- cluster health or environment issue
- permission model mismatch
- generator contract mismatch
- adapter request mapping bug
- service-layer selection, validation, pagination, or shaping bug
- runtime polling or state-mapping bug
- output contract or warning-quality issue
- bad test assumption
- real DS upstream bug or cluster-specific constraint

### Step 3: Fix At The Lowest Correct Layer

- wrong DS request or response shape: fix the generator first
- version-specific contract mismatch: fix the adapter
- selection or lifecycle logic error: fix the service layer
- bad suggestion, warning, or envelope detail: fix output shaping
- bootstrap or persona misuse: fix the live harness or docs

Do not paper over a lower-layer bug with a higher-layer workaround unless that
workaround is the explicit product decision.

### Step 4: Preserve DS Semantics

Do not “fix” a failing live test by inventing a CLI translation that hides the
actual DS behavior. If the upstream object is `OFFLINE`, `SERIAL_WAIT`, or a
DS-native enum or field shape, the CLI should stay honest about that.

If the failure is an unhelpful raw DS result error, classify it before adding a
new translation:

- use `extract_ds_api_error_inventory.py` to confirm whether the code is a
  stable upstream status or only a generic fallback
- use `extract_dsctl_error_translation_matrix.py` to see whether the same code
  is already translated elsewhere
- use `audit_dsctl_error_translation.py` to find the exact service boundary
  where translation is missing
- update `check_error_translation_governance.py` allowlist only for reviewed
  raw cases that should intentionally stay raw

Do not add allowlist entries just to silence a live failure. The allowlist is
for reviewed exceptions, not for unknown behavior.

### Step 5: Add Regression Coverage

After fixing a live failure:

- add or tighten a non-live regression test whenever the behavior can be
  modeled offline
- keep the live case if the bug depended on real permissions, timing,
  scheduling, plugin availability, or cluster topology

### Step 6: Update The Docs Or Capability Surface

If the failure reveals a real environment constraint:

- document it in the live-testing notes
- expose it in `doctor`, `capabilities`, or command warnings when useful
- mark the relevant live suite as optional if it depends on cluster-specific
  components

## What Live Findings Should Improve In The CLI

Live testing is not just a pass/fail gate. It should feed product improvements.

### `doctor` And `monitor`

Improve:

- auth diagnostics
- permission diagnostics
- dependency or optional capability detection
- clearer cluster-health failure messages

### Selection And Resolution

Improve:

- `resolved` metadata
- name-vs-id ambiguity handling
- context fallback visibility
- missing-project or missing-workflow guidance

### Explain, Preview, And Dry Run

Improve:

- effective defaults shown to the user
- request-shape previews
- schedule risk warnings
- offline or online precondition visibility

### Runtime Commands

Improve:

- watch polling behavior
- terminal-state detection
- stalled or long-running instance diagnosis
- task-instance log and control ergonomics

### Error Quality

Improve:

- structured details
- precise `suggestion` values
- distinction between auth, permission, not-found, conflict, and unsupported
  capability failures

### Templates And Validation

Improve:

- workflow and task templates when real clusters repeatedly reject common
  authoring patterns
- local lint coverage for issues that currently surface only in live runs

## What Not To Do

- do not run the whole suite as admin
- do not reuse shared long-lived project names
- do not rely on direct DB writes to prepare state
- do not silently skip cleanup
- do not encode plugin-specific assumptions into the generic live suite
- do not treat a passing live run as a substitute for offline regression tests

## Relationship To Upstream DS Tests

Local upstream source checkouts are useful in three different ways. During
development they are usually mounted under `references/`, but that directory is
ignored, not packaged, and not required for installed CLI usage.

- `dolphinscheduler-api-test`: best reference for our REST black-box flows
- `dolphinscheduler-master` integration cases: best reference for runtime and
  scheduling semantics
- `dolphinscheduler-e2e`: useful for scenario discovery, but not the right
  harness model for our CLI

Use the upstream cases to discover missing scenarios. Do not copy their
assertion style blindly.
