# Architecture

## Goal

Build `dolphinscheduler-cli` around generated, versioned DS-native contracts,
with handwritten compatibility and command logic layered above them.

The documentation set is split by audience:

- `README.md`
- `docs/user/` for installation, configuration, command usage, workflow
  authoring, runtime operations, and supported DS versions
- `docs/development/` for architecture, generated contracts, testing, release,
  and roadmap material
- `docs/reference/` for stable machine-readable contracts, domain models, and
  error semantics

Reference-only grounding lives under `docs/reference/`, especially:

- `docs/reference/domain-model.md`
- `docs/reference/error-model.md`

## Current Stable Surface

The current stable CLI surface is:

- `dsctl version`
- `dsctl context`
- `dsctl doctor`
- `dsctl schema`
- `dsctl capabilities`
- `dsctl enum names`
- `dsctl enum list ENUM`
- `dsctl lint workflow FILE`
- `dsctl task-type list|get|schema`
- `dsctl environment list|get|create|update|delete`
- `dsctl cluster list|get|create|update|delete`
- `dsctl datasource list|get|create|update|delete|test`
- `dsctl namespace list|get|available|create|delete`
- `dsctl resource list|view|upload|create|mkdir|download|delete`
- `dsctl queue list|get|create|update|delete`
- `dsctl worker-group list|get|create|update|delete`
- `dsctl task-group list|get|create|update|close|start`
- `dsctl task-group queue list|force-start|set-priority`
- `dsctl alert-plugin list|get|schema|create|update|delete|test`
- `dsctl alert-plugin definition list`
- `dsctl alert-group list|get|create|update|delete`
- `dsctl tenant list|get|create|update|delete`
- `dsctl user list|get|create|update|delete`
- `dsctl user grant project|datasource|namespace`
- `dsctl user revoke project|datasource|namespace`
- `dsctl access-token list|get|create|update|delete|generate`
- `dsctl monitor health|server|database`
- `dsctl audit list|model-types|operation-types`
- `dsctl use project|workflow|--clear`
- `dsctl project list|get|create|update|delete`
- `dsctl project-parameter list|get|create|update|delete`
- `dsctl project-preference get|update|enable|disable`
- `dsctl project-worker-group list|set|clear`
- `dsctl schedule list|get|preview|explain|create|update|delete|online|offline`
- `dsctl template workflow|workflow-patch|workflow-instance-patch|params|environment|cluster|datasource|task`
- `dsctl workflow list|get|export|describe|digest|create|edit|online|offline|run|run-task|backfill|delete`
- `dsctl workflow lineage list|get|dependent-tasks`
- `dsctl workflow-instance list|get|export|parent|digest|edit|watch|stop|rerun|recover-failed|execute-task`
- `dsctl task list|get|update`
- `dsctl task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop`

Everything else remains roadmap work.

## DS Model Lens

`dsctl` follows the DS object model rather than UI menus or controller names.
The most useful split is:

| Plane | Core objects |
| --- | --- |
| Governance | user, access-token, tenant, queue, worker group, environment, cluster, datasource, namespace, resource, alert plugin, alert group |
| Project | project, project parameter, project preference, project worker-group, task group |
| Design | workflow, task, relation, workflow lineage, schedule |
| Runtime | command, workflow-instance, task-instance, audit log, logs, health |

Meta/self-description command groups such as `use`, `template`, `enum`,
`lint`, and `doctor` sit above those DS resource planes and describe or steer
the CLI itself.

Useful term mapping:

| `dsctl` term | DS term |
| --- | --- |
| workflow | workflow definition / process definition |
| workflow-instance | process instance / workflow instance |
| task-instance | task instance |

See `docs/reference/domain-model.md` for the grounded domain model.

Schedule remains a top-level design resource. In DS 3.4.1 it is persisted
separately from workflow definitions, created and updated through the schedule
API, and brought into effect by a separate online/offline lifecycle that
materializes Quartz jobs.

## Project Structure

```text
dolphinscheduler-cli/
├── README.md
├── AGENTS.md
├── docs/
│   ├── user/
│   │   ├── installation.md
│   │   ├── configuration.md
│   │   ├── commands.md
│   │   ├── workflow-authoring.md
│   │   ├── runtime.md
│   │   └── version-compatibility.md
│   ├── development/
│   │   ├── architecture.md
│   │   ├── codegen.md
│   │   ├── live-testing.md
│   │   ├── release.md
│   │   ├── tooling.md
│   │   └── roadmap.md
│   └── reference/
│       ├── cli-contract.md
│       ├── domain-model.md
│       ├── error-model.md
│       └── future-capabilities.md
├── src/dsctl/
│   ├── commands/
│   ├── services/
│   ├── support/
│   ├── upstream/
│   ├── generated/
│   ├── app.py
│   ├── client.py
│   ├── cli_runtime.py
│   ├── config.py
│   ├── context.py
│   ├── errors.py
│   ├── output.py
│   └── result_navigation.py
├── tools/
│   ├── ds_codegen/
│   ├── generate_ds_contract.py
│   ├── check_quality_gate.py
│   ├── extract_ds_api_error_inventory.py
│   ├── audit_dsctl_error_translation.py
│   ├── extract_dsctl_error_translation_matrix.py
│   ├── check_error_translation_governance.py
│   ├── check_generated_freshness.py
│   ├── check_explicit_object.py
│   └── check_project_layout.py
└── tests/
```

`references/` is not part of the tracked product tree. When present, it is an
ignored local workspace for upstream DolphinScheduler source checkouts used by
code generation and source review. The installed CLI and published packages do
not depend on it.

## Layer Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│ commands/          CLI surface                              │
│ Parse options → call service → emit rendered command        │
├──────────────────────────────────────────────────────────────┤
│ services/          business logic                           │
│ Selection, resolution, validation, pagination, YAML shaping │
├──────────────────────────────────────────────────────────────┤
│ upstream/          version adapter                          │
│ Bind transport → expose version-stable operation groups     │
├──────────────────────────────────────────────────────────────┤
│ generated/         generated DS contracts                   │
│ Typed requests, responses, enums, operation clients         │
├──────────────────────────────────────────────────────────────┤
│ foundation         transport and runtime support            │
│ client/config/context/errors/output/support                 │
└──────────────────────────────────────────────────────────────┘
```

### Foundation

Foundation owns:

- HTTP transport and retry
- auth header construction
- config loading
- context persistence
- error types
- JSON/YAML boundary helpers
- output adapters and exact stdout/stderr/exit-code routing
- pure, bounded lifecycle navigation derived from an already completed command
  result

Foundation does not import upward into commands, services, or upstream.

`result_navigation.py` is the single deep module for JSON `next_actions`. Its
public interface accepts `action`, `resolved`, `data`, and the optional explicit
env-file target; its private rules own safe fact extraction, lifecycle-state
decisions, numeric selector preference, target preservation, shell encoding,
deterministic ordering, de-duplication, and output limits. It performs no I/O
and imports no service, upstream, or generated contracts. The standard
success-envelope builder calls it once, so domain services do not need to
remember an output-navigation hook and table/TSV/raw renderers remain
row/artifact-only.

### Generated

`src/dsctl/generated/` is tracked runtime code produced by the generator.

Rules:

- never hand-edit generated packages
- improve the generator first when DS-facing shapes are wrong
- use `tools/check_generated_freshness.py` to keep generated output in sync

The generated runtime routes response-contract and result-envelope failures
through explicit `SessionLike` hooks. Its standalone default adapter raises
self-contained generated exceptions, while the production
`GeneratedSessionAdapter` translates the same hooks into stable `dsctl`
transport and result errors. Resource adapters must not add one-off catches for
generated payload validation.

### Upstream

`upstream/` owns:

- binding a shared `DolphinSchedulerClient`
- adapting generated clients into version-stable operation groups
- normalizing version differences at the handwritten bridge
- resolving `DS_VERSION` to support metadata, adapter family, and generated
  contract version
- exposing version-specific DS enum semantics needed by services without
  leaking generated imports upward

The upstream package is the only handwritten place that imports generated
packages.

Version support is additive and registry-driven. A selectable DS server version
must have support metadata before services can bind it. Compatibility families
such as `workflow-3.3-plus` can share adapter logic when upstream REST
semantics are stable; older `process-*` API families should be implemented as
separate legacy adapters instead of service-layer branches.

### Services

`services/` owns:

- `flag > context` selection rules
- name/code resolution
- command-level validation
- pagination and output shaping
- schedule lifecycle shaping
- runtime instance inspection and log shaping
- stateless risk confirmation for high-impact mutations
- current YAML rendering for workflow reads
- current YAML parsing, optional schedule block handling, and legacy form
  compilation for workflow create

`services/` does not own:

- Typer
- direct generated imports
- stdout rendering

Workflow task identity allocation is split across the service and upstream
layers. The compiler always receives an explicit allocator. Lint and dry-run
use deterministic preview-only codes, while an applied workflow create,
workflow edit, or workflow-instance edit first completes the same local
compile preflight and then obtains persistent task codes from DolphinScheduler
through the bound `TaskOperations` interface. The upstream 3.4.1 adapter backs
that interface with the generated `gen-task-codes` REST operation and batches
requests to the server limit of 100 codes. Existing-task and no-op edits do not
request new codes.

### Commands

`commands/` owns:

- Typer declarations
- option parsing
- calling service functions
- delegating result emission to `cli_runtime.emit_result()`

`commands/` does not own:

- HTTP
- DS DTO details
- name resolution

### Progressive Self-Description

`services.schema` is a deep module over the generated/handwritten command
catalog. Its interface exposes index, group, action, list, and expanded queries;
callers do not need to know how action nodes, DS-version identity, payload
metadata, data shapes, or correction candidates are assembled.

The normal discovery path is deliberately bounded:

```text
schema index -> group action index -> action-local contract
```

An agent that already knows an action may jump directly to the final step.
`--full` selects the expanded projection adapter for catalog audits and
generators. It is not the default representation.

The root help is the routing seam for callers that cannot read repository
documentation. It directs agents to inspect only the immediate command through
leaf help or an action-local schema, using one relevant group only when the
action is unknown and without preloading downstream actions. Successful
lifecycle results then carry complete, output-bounded `next_actions` commands;
callers preserve a goal-aligned, authorized selection unchanged and serialize
mutations before dependent reads.

Interaction is help-first with schema on demand; implementation is
contract-first. The pure `command_contract` deep module owns canonical root
option facts, explicit action routes, ordered input facts, parser versus fixed
default semantics, path rules, value-source resolution, and shell-safe command
rendering. Typer help/parser metadata, bounded schema JSON, and lifecycle
navigation are separate adapters at that seam. Strongly typed command callbacks
remain explicit, and Click-tree parity tests verify that their observable parser
contract matches the canonical facts. Migration is incremental; the four root
options and `workflow.create` form the initial three-adapter tracer bullet.
Commands with hidden compatibility flags or multiple aliases remain
handwritten until the canonical grammar models visibility and aliases; they
must not be forced through a lossy projection merely to increase coverage.

Each modeled command fact has one construction source. The versioned wire
contract may retain a compatibility projection of that fact—for example a
legacy `default` beside the richer `resolution` object—but adapters do not
redefine it. In particular, action-local JSON owns one `command` object and
does not carry duplicate renderer rows. The output adapter derives table, TSV,
and explicit column projection rows from that object. Schema view-aware data
shapes keep index, list, group, and command projection paths local to the
output boundary.

`services.task_authoring` applies the same deep-module rule to task-type
contracts. It builds canonical metadata, fields, and state rules once, then
lazily derives only the bounded field, exact field, JSON Schema, compile
mapping, or explicit legacy-full projection requested. Commands do not assemble
these representations. View-dependent row paths and document restrictions are
registered by action in the shared output data-shape module, so
table/TSV/column adapters do not contain task-type-specific branches.

Static cross-field command constraints live in the schema constraint registry
and mirror service/command validation (`exactly_one_of`, `requires`, mode
alternatives, and related forms). Tests require every referenced flag or
placeholder to exist in the action contract. Dynamic runtime facts, including
confirmation tokens derived from a concrete mutation, stay in service errors
rather than being guessed by the static schema.

Self-description quality is evaluated at the task level rather than by response
size alone: necessary tokens, discovery round trips, retry tokens, and
ambiguity/misoperation risk all count. Byte-budget tests bound common responses
without removing information needed to construct the next correct command.
Layout compaction is secondary: semantic projection and avoiding duplicate
representations normally save more task tokens than removing JSON whitespace.

## Dependency Rules

```text
commands/ -> services/ -> upstream/ -> generated/
foundation supports all layers but does not import upward.
```

Hard rules:

- `commands/` never imports `generated/`
- `services/` never imports `generated/`
- `services/` never imports `commands/`
- `upstream/` never imports `services/`
- generated code stays self-contained
- `JsonValue` and `JsonObject` stay boundary-oriented types
- names are treated as opaque strings; business code must not infer structure
  from delimiters

`import-linter` enforces the layered dependency direction.

## Selection and Naming

Current naming rules:

- governance and definition resources such as environment, datasource, namespace,
  queue, worker-group, tenant, user, project, workflow, and task are
  name-first
- `resource` is path-first and uses the DS `fullName` path selector directly
- lifecycle and runtime resources such as schedule, workflow-instance, and
  task-instance are id-first
- DS codes and ids are resolved underneath
- positional arguments mean raw names unless the command explicitly documents a
  numeric code, id shortcut, or DS-native path selector
- names are opaque strings, not path fragments
- resource paths are DS-owned paths, not CLI-invented handles

The current implementation does not expose handles. If handles or richer
selector forms are added later, they must complement the current name-first,
path-first, and id-first surface instead of reintroducing delimiter-based refs.

## Codegen Toolchain

The codegen stack lives under `tools/ds_codegen/`.

- `tools/generate_ds_contract.py` is the regeneration entrypoint
- `tools/ds_codegen/api.py` is the internal façade
- `tools/ds_codegen/extract/` handles Java parsing and inference
- `tools/ds_codegen/render/` and `render/package/` handle output generation
- `tools/check_generated_freshness.py` verifies committed generated output

Prefer `generate_ds_contract.py` or `ds_codegen.api` over reaching into leaf
modules from outside the codegen package.

## Quality Gate

For full local validation, prefer:

- `python tools/check_quality_gate.py`
- `python tools/check_quality_gate.py --include-live`
  This appends `tests/live` and expects the live-test environment variables to
  already be configured.

Substantial changes should keep these green:

- `ruff check src tests tools`
  This includes `B008` and a `C90` complexity gate.
- `ruff format --check src tests tools`
- `mypy src tests tools`
- `lint-imports`
- `tools/check_generated_freshness.py`
- `tools/check_error_translation_governance.py`
- relevant `pytest` targets

When changing DS-facing failure handling, use these review tools to keep error
translation grounded in upstream behavior:

- `tools/extract_ds_api_error_inventory.py --format summary`
  Builds the upstream error inventory from local DS source.
- `tools/audit_dsctl_error_translation.py --format summary`
  Audits handwritten service translation coverage and pagination hooks.
- `tools/extract_dsctl_error_translation_matrix.py --format summary`
  Shows the current DS-code-to-dsctl-error mapping implemented by services.

For cluster-interacting surfaces, offline tests alone are not sufficient proof
of correctness. Keep the corresponding live coverage defined in
`docs/development/live-testing.md`.
