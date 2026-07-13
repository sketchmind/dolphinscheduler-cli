# Changelog

All notable changes to this project will be documented in this file.

This project follows a simple public-release changelog until a stronger
versioning policy is needed.

## Unreleased

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
