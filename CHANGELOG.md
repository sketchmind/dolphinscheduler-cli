# Changelog

All notable changes to this project will be documented in this file.

This project follows a simple public-release changelog until a stronger
versioning policy is needed.

## Unreleased

- Added compact UTF-8 JSON output for agent and scripted use.
- Routed structured command failures to stderr while preserving stdout for
  successful data and raw artifacts.
- Added table and TSV page/warning diagnostics on stderr.
- Changed `dsctl schema` to schema version 2: the default is now a bounded
  action index, group and command views are progressive/action-local, and the
  former expanded representation is available explicitly through `--full`.
- Added bounded schema typo candidates, view-aware JSON/table/TSV projection,
  explicit invocation and cross-field constraint metadata, and task-level
  response-size budgets while retaining the detailed `--list-commands`
  compatibility inventory.
- Corrected task-authoring JSON Schema nesting and local references for retry
  objects, dependency arrays, and plugin parameter models; removed the
  duplicate `rows` copy of the canonical field list.
- Improved workflow/template discovery, positional workflow recovery hints,
  scoped resolver suggestions, runtime numeric bounds, and task-instance
  missing/log error translation without masking generic upstream failures.

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
