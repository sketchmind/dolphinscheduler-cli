# Changelog

All notable changes to this project will be documented in this file.

This project follows a simple public-release changelog until a stronger
versioning policy is needed.

## Unreleased

- Block `workflow-instance execute-task` on `3.3.1`, `3.3.2`, `3.4.0` and
  `3.4.1` before dispatch: their API accepts the command but their master has
  no `EXECUTE_TASK` handler. Capabilities and schema expose the limitation while
  preserving the exact wire contract.
- Explain native resource-storage-disabled errors as `invalid_state`, retaining
  the upstream code and administrator guidance, including default-directory and
  task-resource resolution failures.
- Preserve the stored calendar when updating only schedule execution or warning
  settings, including schedules whose original start time has already passed.
- Keep schedule risk confirmation valid when preview times advance. Tokens still
  bind the action, target, effective configuration and risk policy, including
  workflow creation with an embedded schedule.
- Show effective project schedule preferences in workflow creation previews and
  bind confirmation to them. Stop schedule creation if those settings change
  after the workflow was created, reporting the completed steps.

## 0.4.0 - Unreleased

### Compatibility

- Reject ineffective positive schedule environment selections on `2.0.0`–`3.2.1`,
  including project preferences, before mutation. Preserve existing schedules
  with a runtime warning; expose the limitation in schema and capabilities.
  `3.2.2` onward retains schedule-to-task environment inheritance.
- Apply the same task-inheritance check to manual workflow runs, task runs,
  and backfill, including resolved project preferences and dry runs.

- Accept source-proven empty alert-plugin pages on `3.1.0`–`3.1.7`, where
  upstream returns `totalList: null`. Keep invalid response types and
  contradictory page counts visible instead of reporting false absence.
- Verify alert-plugin mutations against upstream parameter values instead of
  raw UI JSON text. Preserve empty configurations across create, rename and
  inline parameter updates, while rejecting malformed input before dispatch.
- Use complete paginated alert-plugin reads for resolution and mutation
  verification so missing numeric IDs return `not_found` on older releases.
- Added exact compatibility profiles for all 37 final DolphinScheduler releases
  from `1.3.9` through `3.4.3`. The shared 181-action CLI surface has 6,697
  terminal version decisions: 5,797 supported, 5 limited and 895 absent upstream.
- Marked `project-worker-group.clear` as limited on `3.2.2`, whose API rejects
  empty assignments. The CLI blocks it before transport; list and nonempty set
  remain supported. Capability errors require the profile to match the actual
  server, and assignment errors provide operation-specific guidance.
- Kept DolphinScheduler `3.4.1` as the stable runtime target. The other 36
  exact profiles remain experimental until their release-specific verification
  and promotion gates pass; a terminal decision is not itself live evidence.
- Adapted `3.4.3` to its own schedule missed-fire policies, whole-workflow
  task-update transaction, workflow-instance summary responses, native resource
  preview and governance identity projections. Kept non-default schedule
  policies through export and edit snapshot checks; did not add live claims.
- Fixed script and JAR resources to resolve canonical FILE names to each
  exact release's native IDs or storage paths. Reject invalid script references
  before deferred datasource-name resolution. Preserve native opaque baselines.
- Retained exact DATAX validation differences: `3.4.3` rejects an empty JSON
  object in the typed inline custom-config facet without inventing a file path.
- Made legacy `1.3.9` project and datasource GET deletions execute once and
  preserve uncertain mutation outcomes. Namespace permissions no longer
  fabricate a cluster code when older native responses omit it.
- Corrected workflow publication and startup across exact profiles: legacy
  graphs retain their native layout, optional warning groups retain native
  omission semantics, and startup timeouts use upstream defaults. Execution
  dates follow the selected release's comma-range or JSON contract.
- Report ambiguous workflow-instance stop results as
  `mutation_outcome_unknown` on exact releases `3.2.0` through `3.4.3`, where
  upstream code `50015` cannot prove whether stop took effect. Preserve the
  upstream source and instance scope, and direct callers to reconcile with
  `get` or `watch` without replaying stop automatically.
- Preserve instance tenants during edits on older releases whose detail APIs
  omit the tenant code. Explicit tenants are recovered only from a matching
  tenant identity; the native inherited-tenant sentinel remains inherited.
- Verify equivalent offset-bearing schedule dates on `1.3.9` by their instant,
  while retaining strict checks when the server-local timezone is unknown.
- Raised the Typer dependency floor to `0.26` to use its vendored Click and
  exclude older combinations that break CLI imports and argument help with
  external Click `8.5`.

### Architecture and code generation

- Generalized the compatibility compiler to project 153 semantic operations
  into exact action catalogs, generated runtime slices, and version-selected
  task, workflow, and runtime-instance domains.
- Added explicit source, decision, dependency, support-coverage, and generated
  profile artifacts so upstream differences and terminal upstream limitations
  can be reviewed mechanically instead of being hidden in adapter aliases.
- Unified command invocation facts in one catalog and moved all 21 domains to
  compiled wire plans; retired generated-session and obsolete family paths.
- Require explicit authoring implementation registration for every exact task
  review or exclusion, preventing omitted builders from bypassing facet policy.
- Admitted the 21 intermediate releases through the existing 21 compiled
  domains, with independent source reviews and 449 additional typed task
  memberships. Source-analysis inventory and runtime admission remain separate.
- Improved source extraction for locally constructed array responses and
  generated only the reviewed semantic closure needed by each runtime profile.

### CLI contract

- Preserve page coverage for project and workflow lists, including original
  totals, observed pages and changes during a read. Aggregated counts remain
  distinct from completeness of the requested range. Project-selector help
  identifies the native `id` on `1.3.9` and `code` on newer releases.
- Renamed the global display option from `--output-format` to `--format`.
  Update scripts to use `--format json|json-compact|table|tsv`; the old name is not retained
  as an alias. `resource download --output PATH` still selects the download
  destination.
- Replaced `--compact` with `--format json-compact`. Compact output encodes
  explicitly declared business lists as `columns` plus positional `rows`,
  including empty and single-row results. It preserves all fields by default,
  nested JSON values, pagination, warnings and navigation; other views and
  errors retain their structures with compact whitespace. Ordinary `json`
  retains its object structure. Declared nested business collections in
  alert-plugin definitions, task-type lists and workflow lineage also use
  compact rows while retaining sibling data. Compact lists accept top-level
  `--columns` and `*`; dotted projection remains available with `json`.
  Broken declared paths or row shapes return `output_contract_error`; invalid
  dotted selectors remain `user_input_error` before service execution.
- Workflow-instance snapshots retain null definition identities using the
  selected exact recipe's native ID or code fields in both JSON formats.
  Namespace lists preserve supported null fields and omit fields absent from
  the selected exact native model; this shared projection correction also
  applies to ordinary JSON.
- Consolidated output warnings into one optional structured `warnings` array;
  removed the duplicate `warning_details` field and empty warning arrays.
  Each warning retains its code, message and diagnostic facts.
- Corrected schedule-update navigation to require known offline state, and
  linked schedule, workflow and queue results to scoped follow-up reads.
  Workflow publication distinguishes manual runs from schedule activation.
- List action indexes now use `targets: "all"` only for all returned rows.
  Truncated or ambiguous sets carry explicit ids; schema no longer advertises
  the retired `action_index.schema_command` alias.
- Fixed project grants to upgrade existing read-only membership on applicable
  releases and preserve other memberships on whole-set replacement APIs.
  Exact insert-only releases retain no-op behavior for existing grants;
  verification explicitly reports its membership-only scope.
- Task-group force-start now returns `accepted: true` instead of
  `forceStarted: true`, leaving actual task execution to subsequent observation.
- Added named contexts and a saved `default-context` in one user registry.
  Contexts reference complete env files without copying credentials, bind
  project defaults to the saved endpoint, and reject external endpoint changes
  until explicitly rebound.
- Added `--context`, `DSCTL_CONTEXT` and `DSCTL_ENV_FILE`. Explicit selectors
  override process selectors; present process connection values override the
  saved default. Each selected source is complete and never merges or falls
  back to another source.
- Removed `use`, directory-scoped context lookup, legacy context-file fallback
  and ambient workflow defaults. Register an existing env file with
  `context create`, choose `config set default-context`, and pass workflow
  selectors explicitly. Local writes distinguish saved and effective state.
- Made canonical workflow selectors parser-required before configuration
  resolution for the 13 positional workflow actions, task `list`, `get`, and
  `update`, and `schedule create`. Kept `schedule explain` conditional and
  genuine list and runtime filters optional.
- Reworked help so root help explains global placement and output tradeoffs
  once, while leaf help uses a compact canonical `Global options` panel, a
  short action-schema footer, and only applicable pagination or dry-run hints.
  Renamed the `--columns` metavar to `FIELDS` and clarified exact raw output
  behavior for exports, templates, and task logs.
- Extended automatic discovery with reviewed public API-contract evidence for
  compatible reads when metadata cannot establish an exact release. Candidate
  versions remain distinct from server identity; authoring and writes still
  require an exact version.
- Added exact-version workflow skeletons and complete output, branch, child,
  and dependent examples through `template workflow --example`. Consolidated
  near-identical task variants into their main templates. Removed `minimal`,
  `params`, and pure alias selectors; ordinary optional inputs now use short
  field hints. Retained distinct business operations, resource/output
  compositions and dependency-target scenarios, including DVC
  `upload`/`download`/`init` with upload as the default.
- Removed duplicate YAML line arrays from concrete task/workflow templates.
  JSON returns `data.yaml`; table/TSV and explicit line columns derive rows.
- Aligned datasource JSON Schemas with positive IDs and exact names in selected
  K8S, SAGEMAKER and ZEPPELIN task profiles. Enforced SageMaker datasource
  requiredness during local validation only in the applicable exact profiles.
- Kept context/env-file selection in concrete discovery commands and derived
  monitor choices from the selected runtime recipe. Corrected datasource
  guidance and task-list patterns to match accepted CLI syntax.
- Kept task runtime constraints and legacy representability checks active during
  local lint when datasource names defer remote binding and wire compilation.
- Normalized preview requests to ordered `data.requests`, removing the singular
  `data.request` alias. Default dry-run output shows semantic changes, effective
  settings, and stage order; `--columns requests` or `--columns '*'` expands the
  prepared wire requests. Apply prepares again and does not reserve server state.
- Distinguished accepted execution, pending trigger resolution, and real instance
  IDs. Added project-scoped `workflow-instance list --trigger-code`, replay
  baselines for `watch --after-run-times`, and structured partial-mutation facts.
- Added exact time-filter semantics and page coverage evidence, including partial
  reads and changing totals. Added source-positioned task log windows with
  `--start-line` and `--limit`, clipping facts, and continuation commands.
- Exposed shared output controls in leaf help and preserved error details and
  coverage diagnostics in table/TSV output. Navigation now shares reviewed state
  facts and catalog-bound invocation syntax, including legacy name selectors.
- Unified parser errors under the structured error envelope (exit 2); table/TSV
  show Error/Hint text. Failed doctor checks retain the report and exit 1;
  `watch --exit-status` makes terminal execution success a CI condition.
- Added explicit command effects to schema 3 and removed duplicate placeholder
  command aliases. Default capabilities omit the expanded version inventory.
- Added dotted field projection, CJK table alignment and sectioned detail tables;
  compact coverage footers retain completeness and observation limitations.
- Added password file/stdin input for user create/update and bounded optional
  project scope for schedule ID commands. Null project preferences now mean no
  configured defaults.
- Aggregated lint findings, added local definition/instance patch lint, and
  rendered multiline exported YAML text as literal blocks.
- Added `-h`, local `--version`, grouped root help and explicit
  `--show-completion SHELL` / `--install-completion SHELL`, with no parent-shell
  probing or DolphinScheduler requests.
- Translate project list and get permission failures into `permission_denied`
  instead of exposing raw API result errors.
- Kept URL/token/version together in the selected connection source. Retry and
  timeout policies resolve independently, so policy-only environment settings
  no longer hide a saved context. Added `DS_API_TIMEOUT_SECONDS`.
- Tenant codes are now treated as immutable DS identities selected at create
  time. `tenant update` exposes only queue and description changes, while its
  previously published `--tenant-code` option remains as a hidden deprecated
  parser compatibility alias: the same code is accepted and a rename returns a
  structured input error. This matches every supported DolphinScheduler UI and
  avoids an unsafe inverted tenant-code existence check in `1.3.9`.
- Made workflow-instance and task-instance operations resolve an explicit
  `--project` or stored project context before I/O and use project-scoped
  routes. `task-instance log` remains the intentional projectless exception.
- Added selected-version preflight for task mutation epochs and workflow wire
  fields so unsupported dependencies, tenant fields, and expected parallelism
  fail before transport instead of being guessed or silently dropped.
- Corrected task-update safety boundaries after reviewing service implementations:
  `2.0.0`–`2.0.3` use complete workflow updates; `2.0.3` rejects dependency
  changes, and `2.0.4`–`3.2.0` require a unique workflow binding for ordinary
  field edits and reject explicit task dependency updates. Workflow-definition
  graph edits are also conservatively blocked on `2.0.3`.
- Preserved intermediate task-plugin defects and parameter-propagation changes,
  including DATAX parameter restrictions and the `3.1.6` SeaTunnel wire.
- Translate schedule start-time rejections into actionable `user_input_error`
  messages, and correctly project resource content maps on `2.0.1`–`2.0.5`.

### Verification

- Treat output-size budgets as review metrics; retain semantic completeness,
  command-reference, and architecture checks. Batch focused regressions before
  the full portable, source-contract, and source-rebuild lanes.
- Added real-process REST journeys and checks for static completion,
  noninteractive mutation refusal, closed pipes and SIGINT behavior. Dependency
  CI checks minimum and known-current installations alongside external Click.
- Preserved existing exact-version receipts as historical evidence tied to the
  wheel and manifest they tested. In particular, the DolphinScheduler `3.4.2`
  schema-v4 receipt does not attest a newly generated profile manifest; a fresh
  installed-wheel receipt is required before release promotion.
- Preserved exact `DS_VERSION` selection in live-test profiles and isolated
  subprocess profile variables from inherited environment overrides.
- Added a secret-safe, machine-readable exact-profile evidence receipt that
  binds the tested wheel digest, immutable DolphinScheduler image, generated
  source identity, adapter roles, CLI action trace, and cleanup result.
- Changed publishing to promote one live-tested canonical wheel/sdist pair
  through TestPyPI and PyPI without rebuilding it between indexes. Final
  artifact validation binds conformance, exact-read and exact `3.4.2` mutation
  evidence to that wheel's full SHA-256 and basename.

## 0.3.0 - 2026-07-13

### CLI output and discovery

- Added compact UTF-8 JSON output for agent and scripted use.
- Routed structured command failures to stderr while preserving stdout for
  successful data and raw artifacts. Table and TSV page diagnostics and
  warnings now use stderr so row output remains valid data.
- Changed `dsctl schema` to schema version 2. Its default is a bounded action
  index, group and command views are progressive and action-local, and the
  expanded whole-surface representation is available through `--full`.
- Added bounded schema typo candidates, explicit invocation and cross-field
  constraint metadata, view-aware output shapes, and response-size budgets.
- Made task-type authoring discovery progressive with direct field, JSON
  Schema, and compile-mapping views; `--full` retains the expanded projection.
- Changed `capabilities` to return the bounded summary by default while keeping
  `--summary` as an explicit spelling and `--full` for the expanded inventory.
- Added bounded lifecycle `next_actions` and list-level `action_index` guidance
  derived from completed results without changing table, TSV, or raw output.
- Allowed global display and profile options before or after the command path.

### Workflow and schedule behavior

- Corrected task-authoring JSON Schema nesting, local references, retry and
  dependency objects, plugin parameter models, and sub-workflow parameters.
- Changed `workflow list` to return the rich paged result under
  `data.totalList`, with paging and authoritative schedule state.
- Made workflow reads, exports, lifecycle operations, edits, and deletes use
  the independently persisted attached schedule as their authoritative state.
- Returned the created schedule from workflow creation and distinguished a
  successful workflow mutation from a failed post-mutation schedule refresh.
- Preserved no-environment schedules and zero-valued schedule update fields by
  selecting the compatible DolphinScheduler 3.4.1 REST operation internally.
- Kept exported schedule blocks as verified read-only snapshots during workflow
  edit; schedule mutations remain explicit schedule commands.
- Improved workflow/template discovery, dry-run navigation, positional recovery
  hints, resolver suggestions, numeric bounds, and stable upstream error
  translation.
- Translated unavailable-master runtime failures into actionable
  `invalid_state` errors without automatically retrying non-idempotent workflow
  actions; parallel backfill failures now require callers to verify whether
  earlier partitions were already dispatched before retrying.

### Local context and agent guidance

- Made project/workflow context a consistent scoped tuple with atomic,
  symlink-preserving persistence, source metadata, effective readback, and
  shadowing diagnostics.
- Added atomic `use workflow NAME --project PROJECT` binding and prevented a
  workflow saved for one project from leaking into an explicitly selected
  project.
- Rejected ambiguous or misplaced `use` options that older parsers could
  silently ignore.
- Added the independently installable `skills/dsctl` agent skill with focused
  workflow, schedule, runtime, and error-recovery references. The skill remains
  separate from the PyPI wheel and source distribution.

### Compatibility and migration

- Consumers of the former expanded `schema`, `capabilities`, and task-type
  schema defaults should request `--full` when they require the complete
  representation.
- Consumers of `workflow list` should read rows from `data.totalList` rather
  than treating `data` as an array.
- Scripts that consumed structured failures from stdout must read stderr while
  continuing to use the nonzero exit status.
- DolphinScheduler `3.4.0` and `3.3.2` remain selectable but are now reported as
  experimental until their live suites pass; `3.4.1` remains fully tested.
- Older v0.2 context files containing a workflow without a project binding now
  fail with `config_error`; follow the configuration guide to repair or clear
  the affected scope.
- Workflow operations that require attached-schedule state now fail closed if
  that authoritative lookup cannot be completed.
- Raised the minimum Typer version from `0.12` to `0.24.1`.

### Internal quality

- Unified help, schema, and navigation projections around a canonical command
  contract, extracted the generated-session adapter, and split upstream
  protocols by DolphinScheduler domain.
- Removed unreachable runtime helpers and strengthened generated freshness,
  package version, architecture, and error-governance checks to fail closed.
- Removed a private Typer typing import and hardened live-test handling for
  optional alert-server state, asynchronous task-group queues, and retryable
  workflow cleanup.

## 0.2.0 - 2026-04-20

- Added typed task authoring schema discovery for workflow YAML creation.
- Added workflow and workflow-instance export commands for editable YAML.
- Added workflow and workflow-instance full-document edit flows alongside patch
  edit templates.
- Improved schema, capabilities, README onboarding, and output-shape
  documentation for agent and scripted use.

## 0.1.0 - 2026-04-15

- Generated-first DolphinScheduler REST contract runtime.
- Stable `dsctl` command groups for project, governance, authoring, schedule,
  workflow runtime, task runtime, schema, capabilities, doctor, and lint flows.
- Version-aware runtime selection for DolphinScheduler `3.4.1`, `3.4.0`, and
  `3.3.2`.
- Local workflow YAML linting and workflow authoring templates.
- Strict quality gates for code style, typing, generated freshness, layer
  boundaries, and error translation governance.
