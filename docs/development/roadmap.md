# Roadmap

This file tracks committed implementation phases.

Use `docs/reference/future-capabilities.md` for loose future ideas,
`docs/reference/domain-model.md` for DS grounding, and
`docs/development/codegen.md` for local upstream source setup.

## Current Foundation (Done)

- [x] Local upstream DolphinScheduler `3.4.1` source available for generator
      development and DS-facing behavior review
- [x] AST extraction and code generation pipeline (`tools/ds_codegen/`)
- [x] Versioned package renderer emitting typed DS-native packages
- [x] Runtime transport (`client.py`), config, error, output infrastructure
- [x] Top-tier lint/type-check standards (ruff 30+ rule sets, mypy strict)
- [x] Design docs: architecture, CLI contract, roadmap, domain reference

---

## Phase 0: Wire the Generated Layer into src/ (Done)

**Goal:** The generated contracts are importable from `src/dsctl/generated/`,
the upstream adapter compiles, and `dsctl version` works end to end.

- [x] Copy generated package from `build/` into
      `src/dsctl/generated/versions/ds_3_4_1/`
- [x] Copy upstream adapter layer (protocol, registry, ds_3_4_1 adapter)
      into `src/dsctl/upstream/`
- [x] Reconcile the generated `_base.py` HTTP layer with existing `client.py`
      through a handwritten upstream transport bridge
- [x] Create `src/dsctl/app.py` (Typer root app, global options, error handler)
- [x] Create `src/dsctl/context.py` (session context read/write)
- [x] Create `src/dsctl/commands/meta.py` with `dsctl version` and
      `dsctl context`
- [x] Wire `__main__.py` to `app.py`
- [x] Add `typer` to project dependencies
- [x] All quality gate checks pass

**Done when:**
```bash
dsctl version
# → {"ok": true, "action": "version", "data": {"cli": "0.1.0", "ds": "3.4.1"}}

dsctl context
# → {"ok": true, "action": "context", "data": {"api_url": "...", "project": null, "workflow": null, ...}}
```

---

## Phase 1: First Vertical Slice — Project CRUD (Done)

**Goal:** One complete resource works end to end through all four layers.
This proves the full architecture (commands → services → upstream → generated).

- [x] `src/dsctl/commands/project.py` — list, get, create, update, delete
- [x] `src/dsctl/services/project.py` — business logic
- [x] `src/dsctl/services/resolver.py` — name → code resolution (project only)
- [x] `src/dsctl/services/pagination.py` — page exhaust helper
- [x] Adapter: add `list_projects`, `get_project`, `create_project`,
      `update_project`, `delete_project` to `DS341Adapter`
- [x] `tests/commands/test_project.py` — CliRunner integration tests
- [x] `tests/services/test_project.py` — unit tests with mock adapter
- [x] `tests/services/test_resolver.py` — resolver tests
- [x] `tests/services/test_pagination.py` — pagination tests

**Done when:**
```bash
dsctl project list
dsctl project get etl-prod
dsctl project create --name test-project --description "test"
dsctl project update test-project --description "updated"
dsctl project delete test-project --force

# All return JSON envelopes. All resolve names to codes.
# All tests pass. Quality gate passes.
```

---

## Phase 2: Context + Workflow Read (Done)

**Goal:** `dsctl use` works, and workflow/task read operations complete the
query side of the two most important resources.

- [x] `src/dsctl/commands/use.py` — `dsctl use project|workflow|--clear`
- [x] Context integration in project/workflow/task commands
- [x] `src/dsctl/commands/workflow.py` — list, get, describe
- [x] `src/dsctl/services/workflow.py` — list, get (with YAML export), describe
- [x] `src/dsctl/commands/task.py` — list, get
- [x] `src/dsctl/services/task.py` — list, get
- [x] Resolver: add workflow and task name resolution
- [x] `src/dsctl/support/yaml_io.py` — YAML rendering
- [x] `dsctl workflow export` returns roundtrip-able workflow YAML
- [x] Tests for all of the above

**Done when:**
```bash
dsctl use project etl-prod
dsctl workflow list                        # uses context
dsctl workflow get daily-etl               # uses context
dsctl workflow export daily-etl         # YAML output
dsctl use workflow daily-etl
dsctl task list                            # uses project + workflow context
dsctl task get extract                     # single task detail
dsctl use --clear
```

---

## Phase 3: YAML Models + Workflow Create (In Progress)

**Goal:** LLM or human can create a complete workflow from a YAML file.

- [x] `src/dsctl/models/common.py` — shared enums, RetryConfig, etc.
- [x] `src/dsctl/models/task_spec.py` — per-task-type field schemas
- [x] `src/dsctl/models/workflow_spec.py` — WorkflowSpec, full YAML schema
- [x] `src/dsctl/commands/workflow.py` — add `create` subcommand
- [x] `src/dsctl/services/workflow.py` — create logic:
      WorkflowSpec → taskDefinitionJson + taskRelationJson
- [x] Adapter: `create_workflow` in DS341Adapter
- [x] DAG validation (acyclic check, unique names, dependency resolution)
- [x] `--dry-run` support
- [x] `tests/models/test_workflow_spec.py` — YAML parsing tests
- [x] `tests/services/test_workflow.py` — create workflow tests
- [x] `dsctl template workflow`, workflow patch templates, `dsctl template environment`,
      `dsctl template cluster`, and `dsctl template task SHELL|SQL|HTTP|...`
- [x] YAML `schedule:` block support during `workflow create`
- [x] extend task-type coverage for DS logical/compound nodes:
      `SUB_WORKFLOW`, `DEPENDENT`, `SWITCH`, `CONDITIONS`

**Done when:**
```bash
# Create from YAML
dsctl workflow create --file workflow.yaml
dsctl workflow create --file workflow.yaml --dry-run
dsctl workflow create --file workflow.yaml --confirm-risk TOKEN

# Template discovery
dsctl template workflow --raw # → full YAML template with comments
dsctl template workflow-patch --raw # → workflow edit patch template
dsctl template workflow-instance-patch --raw # → instance edit patch template
dsctl template task SHELL     # → SHELL task template

# The created workflow appears in DS UI and can be triggered.
# Optional schedule blocks are created after workflow online and may require a
# separate confirmation token for high-frequency cron expressions.
# Basic `depends_on` edges are stable; DS logical nodes expand through
# task-type-specific YAML models rather than through a generic dependency DSL.
# `SWITCH` and `CONDITIONS` branch targets are written as task names in YAML
# and compiled into DS task codes during create.
```

---

## Phase 4: Patch + Edit Workflows (Done)

**Goal:** LLM can edit existing workflows with minimal patch YAML and get a
stable dry-run diff before apply.

- [x] `src/dsctl/models/workflow_patch.py` — WorkflowPatch model + merge logic
- [x] `src/dsctl/commands/workflow.py` — add `edit` subcommand
- [x] `src/dsctl/services/workflow.py` — edit logic:
      fetch current → merge patch → validate → submit
- [x] Edit response includes diff (added/modified/removed tasks and edges)
- [x] `dsctl workflow delete` with offline/schedule/runtime/dependency guardrails
- [x] `tests/models/test_workflow_patch.py` — merge logic tests
- [x] `tests/services/test_workflow.py` — edit tests
- [x] `dsctl task update --set key=value` inline edits

**Done when:**
```bash
# Patch workflow
dsctl workflow edit --patch patch.yaml
dsctl workflow edit --patch patch.yaml --dry-run
dsctl workflow edit WORKFLOW --file workflow.yaml --dry-run
# → {"data": {"diff": {"added_tasks": [...], "removed_edges": [...]}}}

# Inline task update
dsctl task update extract --set command="python v2.py"
dsctl task update extract --set retry.times=5 --set priority=HIGH
```

---

## Phase 5: Execution + Monitoring (Done)

**Goal:** Trigger workflows and inspect runtime state using explicit
instance-oriented resources.

**Contract:**

- current stable slice:
  - `workflow online|offline|run|run-task|backfill WORKFLOW`
  - `workflow-instance list|get|parent|edit|watch|stop|rerun|recover-failed|execute-task`
  - `task-instance list|get|sub-workflow|log|force-success|savepoint|stop`

**Grounding:**

- new execution belongs to the workflow definition surface
- control actions on an existing execution belong to workflow instances
- task-instance is an observation resource, not a launch resource

**Implementation:**

- [x] `src/dsctl/commands/workflow.py` — add `run`
- [x] `src/dsctl/commands/workflow.py` — add `online|offline`
- [x] `src/dsctl/commands/workflow_instance.py`
- [x] `src/dsctl/commands/task_instance.py`
- [x] `src/dsctl/services/workflow.py` — add release lifecycle
- [x] `src/dsctl/services/workflow_instance.py`
- [x] `src/dsctl/services/task_instance.py`
- [x] Adapter support for:
      `online_workflow`, `offline_workflow`, `trigger_workflow`,
      `list_workflow_instances`, `get_workflow_instance`,
      `update_workflow_instance`,
      `stop_workflow_instance`, `rerun_workflow_instance`,
      `recover_failed_workflow_instance`, `execute_task`,
      `list_task_instances`, `get_task_instance`, `get_task_log`,
      `force_task_success`, `task_save_point`, `stop_task`
- [x] Workflow online/offline returns refreshed workflow payloads and keeps the
      attached-schedule lifecycle explicit
- [x] Runtime selector rule: workflow-instance and task-instance are id-first
- [x] `workflow-instance edit` compiles a DAG patch against finished-instance
      `dagData` and can optionally sync the saved DAG back to the current
      workflow definition
- [x] `workflow-instance watch` blocks until DS reaches a final execution state
- [x] `workflow-instance stop` validates stop eligibility against the current DS
      execution state before issuing the stop request
- [x] `execute-task` scope mapping: `self|pre|post` →
      DS `TASK_ONLY|TASK_PRE|TASK_POST`
- [x] `workflow run-task` starts a workflow from one task using
      `startNodeList` and emits the dependent-node caveat
- [x] `workflow backfill` mirrors DS complement-data execution with range or
      explicit-date selection and optional task-scoped starts
- [x] Tests for the delivered success and not-found paths
- [x] Tests for delivered runtime control invalid-state and timeout paths

**Done when:**
```bash
dsctl workflow online daily-etl
dsctl workflow offline daily-etl
dsctl workflow run daily-etl
dsctl workflow run-task daily-etl --task extract_orders
dsctl workflow backfill daily-etl --start "2026-04-01 00:00:00" --end "2026-04-02 00:00:00"
dsctl workflow-instance list --state RUNNING
dsctl workflow-instance get 123
dsctl workflow-instance parent 456
dsctl workflow-instance edit 123 --patch instance-patch.yaml
dsctl workflow-instance edit 123 --patch instance-patch.yaml --sync-definition
dsctl workflow-instance stop 123
dsctl workflow-instance rerun 123
dsctl workflow-instance recover-failed 123
dsctl workflow-instance execute-task 123 --task extract_orders --scope pre
dsctl task-instance list --workflow-instance 123
dsctl task-instance sub-workflow 456 --workflow-instance 123
dsctl task-instance log 456 --tail 200
dsctl task-instance force-success 456 --workflow-instance 123
dsctl task-instance savepoint 456 --workflow-instance 123
dsctl task-instance stop 456 --workflow-instance 123
```

---

## Phase 6: Schedule Management (Done)

**Goal:** Full schedule lifecycle as an independent resource bound to workflow
definitions.

**Contract:**

- `schedule list|get|preview|create|update|delete|online|offline`

**Grounding:**

- in DS 3.4.1 schedule is a persisted trigger resource, not a workflow field
- create/update/get use v2 schedule CRUD APIs
- list currently uses the legacy project-scoped paging endpoint by design:
  the v2 `filterSchedule` API is global and filters by `projectName like`,
  while the CLI contract for `schedule list` is explicitly "inside one
  resolved project"
- preview uses the legacy project-scoped preview endpoint and returns the next
  five fire times
- online/offline is a separate lifecycle that materializes or removes Quartz
  jobs
- workflow offline also forces schedule offline

**Implementation:**

- [x] `src/dsctl/commands/schedule.py`
- [x] `src/dsctl/services/schedule.py`
- [x] Adapter support for schedule CRUD and schedule lifecycle actions
- [x] `schedule preview`, `schedule explain`, and stateless `--confirm-risk`
      safety confirmation
- [x] DS 3.4.1 bridge mixes:
      legacy schedule paging with v2 schedule get/create/update/delete and
      legacy schedule preview/online/offline endpoints
- [x] Tests for preview, create/update/delete, and lifecycle rules
- [x] Schedule section in YAML (`workflow.yaml` → `schedule:` block)
- [x] `dsctl workflow create --file workflow.yaml` auto-creates schedule if
      `schedule:` is present

**Done when:**
```bash
dsctl schedule create --workflow daily-etl --cron "0 0 2 * * ?" \
  --timezone Asia/Shanghai --start 2024-01-01 --end 2025-12-31
dsctl schedule list --workflow daily-etl
dsctl schedule get 1
dsctl schedule preview 1
dsctl schedule online 1
dsctl schedule offline 1
dsctl schedule delete 1 --force
```

---

## Phase 7: Remaining Resources

**Goal:** Complete the governance and self-description resource coverage needed
for production use.

- [x] Codegen follow-up: datasource detail now routes through the generated
      `queryDataSource` client. The shared `BaseDataSourceParamDTO` contract is
      rendered as an open model with a synthetic `type` field so DS-native
      plugin-specific detail keys survive validation and JSON emission. A fully
      typed per-plugin datasource union remains optional future refinement, not
      a blocker for the CLI surface.
- [x] `resource` — list/view/upload/create/mkdir/download/delete
- [x] `user` — CRUD
- [x] `user grant` — project grant/revoke
- [x] `user grant` — datasource grant/revoke
- [x] `user grant` — namespace grant/revoke
- [x] `datasource` — CRUD + connection test
- [x] `namespace` — list/get/available/create/delete
- [x] `queue` — CRUD
- [x] `worker-group` — CRUD
- [x] `task-group` — lifecycle plus task-group queue list/force-start/priority
- [x] `alert-plugin` — list/get/definition list/schema/create/update/delete/test
- [x] `alert-group` — CRUD
- [x] `tenant` — CRUD
- [x] `environment` — environment CRUD
- [x] `cluster` — cluster CRUD
- [x] `monitor` — health, servers, database stats
- [x] `audit` — list, model-types, operation-types
- [x] `project-preference` — get/update/enable/disable singleton project preference
- [x] `capabilities` — version capability discovery
- [x] `schema` — stable self-description for the current surface
- [x] `template` — YAML/template discovery
- [x] Tests for each

**Done when:** Every resource in the architecture doc has a working command
group with tests.

---

## Phase 8: Diagnostics + LLM Integration

**Goal:** Add diagnostics and AI-native ergonomics on top of the stable resource
surface.

- [ ] `dsctl digest` / resource-specific `digest` views where they materially
      reduce context size
  - [x] `dsctl workflow digest` — compact workflow DAG summary
  - [x] `dsctl workflow-instance digest` — compact runtime progress summary
- [x] `schedule explain` for pre-mutation schedule reasoning
- [ ] broader `dsctl explain` for execution-context and parameter reasoning
- [x] `dsctl lint` for local workflow design-time checks
- [x] `dsctl doctor` for runtime and governance diagnostics
- [x] `dsctl schema` — JSON tool definition output for the current stable surface
- [x] `dsctl enum names`, `dsctl enum list <enum>` — enum value discovery
- [x] `dsctl task-type list` — live DS task-type discovery with favourite flags
- [x] `dsctl task-type get|schema` — local task authoring summaries, field
      contracts, state rules, choices, and compile mappings
- [x] audit log inspection and audit filter metadata discovery
- [x] workflow lineage inspection and dependent-task discovery
- [ ] `--non-interactive` mode (never prompt stdin)
- [ ] Error hints with concrete next-step guidance
  - [x] config, resolver, selection, delete-force, confirmation, timeout, and
        common discovery/input errors emit `error.suggestion`
- [ ] Claude Code skill: `/ds` entry point wrapping dsctl
- [ ] Skill knowledge files for complex operations
- [ ] End-to-end LLM agent test: create workflow → trigger → check → report

**Done when:** A Claude Code skill can autonomously:
1. Create a multi-task workflow from natural language description.
2. Trigger it and wait for completion.
3. Check status, retrieve logs on failure, and report results.
4. Edit the workflow to fix issues and re-trigger.

All without human intervention beyond the initial instruction.

---

## Live Validation Track (In Progress)

**Goal:** Prove the current CLI surface against a real DolphinScheduler
cluster, not just mocked transports and fake adapters.

This track runs in parallel with feature development. Any cluster-interacting
surface is only partially validated until the corresponding live coverage
exists.

Current snapshot:

- [x] full tracked live suite passes on the current test cluster
- [x] admin bootstrap now creates a tenant, ETL-style user, and access token
- [x] runtime, schedule, governance, and optional-capability semantics are
      captured in `docs/development/live-testing.md`

### Live Phase A: Harness + Policy

- [x] `docs/development/live-testing.md` defines personas, coverage rules, and failure
      analysis principles
- [x] Explicit rule: every cluster-interacting command needs live coverage
- [x] Explicit matrix for the current surface
- [x] `tests/live/` package scaffold
- [x] pytest markers for:
      `live`, `live_admin`, `live_developer`, `destructive`, `slow`,
      `optional_capability`
- [x] shared live helpers for unique run prefixes, env loading, cleanup, and
      CLI invocation

**Done when:** a contributor can run the documented live suites with a stable
test harness and env-file convention.

### Live Phase B: Preflight + Bootstrap

- [x] preflight checks for `doctor` and `monitor`
- [x] admin bootstrap flow:
      tenant → user → access-token
- [x] generated non-admin token is used by the main suite
- [x] explicit negative check proving the ETL persona is not silently running
      with admin privileges

**Done when:** the suite can prepare or validate the required non-admin test
identity from the provided admin token and can fail early with precise setup
diagnostics.

### Live Phase C: Mandatory Core Contract Coverage

- [x] live contract cases for:
      `doctor`, `task-type`, `monitor`, `project`, `workflow`, `task`,
      `schedule`, `workflow-instance`, `task-instance`
- [x] live contract cases for:
      `project-parameter`, `project-preference`, `project-worker-group`
- [x] live contract cases for:
      `audit`, `resource`, `namespace`
- [x] read surfaces prove payload projection and selector semantics
- [x] mutation surfaces prove write → read-back → cleanup

**Done when:** every non-optional core ETL surface has at least one passing
live contract test.

### Live Phase D: Governance + Admin Coverage

- [x] live contract cases for:
      `tenant`, `user`, `access-token`, `queue`, `worker-group`
- [x] live contract cases for:
      `environment`, `cluster`, `alert-group`
- [x] permission-boundary tests for admin-only mutations
- [x] grant/revoke live tests where cluster policy allows them

**Done when:** all governance surfaces that our current cluster can support
have admin-path live coverage plus at least one denial or boundary case.

### Live Phase E: Optional Capability Coverage

- [x] `datasource` live tests in an environment with real reachable backends
- [x] `alert-plugin` live tests in an environment with installed plugin
      backends
- [x] capability-gated `environment`, `cluster`, `namespace`, and `resource`
      scenarios
      in compatible deployments where needed
- [x] any skipped suite records the missing capability explicitly

**Done when:** every remaining cluster-interacting command either has passing
live coverage or an explicit documented capability block.

### Live Phase F: Runtime Semantics And Recovery

- [x] schedule-created instances observed in real runtime
- [x] `workflow-instance watch` terminal-state validation
- [x] `stop`, `rerun`, `recover-failed`, and `execute-task` scenario coverage
- [x] `task-instance log`, `stop`, `force-success`, `savepoint` scenario
      coverage where the cluster semantics allow them
- [x] sub-workflow and dependency-sensitive runtime scenarios

**Done when:** runtime and schedule correctness is proven by real state
transitions, not just request/response contract checks.

### Live Phase G: Closure

- [x] live failures have matching offline regression tests where possible
- [x] docs updated for cluster constraints and discovered semantics
- [x] command warnings, diagnostics, or suggestions improved from live findings
- [x] release readiness reviewed against `docs/development/live-testing.md`

**Done when:** live validation has become a repeatable maintenance track rather
than a one-time verification spike.

### Stop Conditions

The live-validation track should proceed autonomously unless blocked by one of
these:

- missing credentials or env files
- unsafe destructive behavior on a shared cluster without reliable cleanup
- missing external capability for an optional suite
- a real product decision that cannot be reduced to implementation work

---

## Definition of Done (Overall)

The project is **production-ready** when all of these are true:

### Functional Completeness
- [x] All stable resource groups in the architecture doc have working command groups
- [x] Full YAML create/edit/get roundtrip for workflows
- [x] Patch dialect merges correctly for all task types
- [ ] `dsctl use` context switching works across all commands
- [x] Pagination auto-exhaust works for all paginated list commands
- [x] Name-first resolution works for all supported name-first resources

### LLM Readiness
- [ ] Every command returns structured JSON envelope (no exceptions)
  - [x] all registered command callbacks are structurally required to route
        through `emit_result`
- [x] `dsctl schema` outputs complete tool definition
- [x] `dsctl template` covers all upstream default task types
- [ ] Error responses include machine-actionable `type` and `suggestion`
- [ ] `--dry-run` available on all mutating commands
- [ ] Claude Code `/ds` skill works end-to-end

### Quality
- [x] ruff 30+ rule sets pass with zero violations
- [x] mypy strict passes with zero errors
- [ ] Test coverage ≥ 80% on `services/` and `models/`
- [x] Every cluster-interacting command has at least one live test in a
      compatible environment
- [x] Every error type has at least one test
- [x] Architecture boundary check passes (no upward imports)
- [ ] No hardcoded IPs, tokens, or secrets in source
  - [x] handwritten source is scanned for local/private host literals and
        literal secret assignments

### Documentation
- [ ] `docs/development/architecture.md` matches actual code
- [x] `docs/reference/cli-contract.md` — stable command surface documented
- [ ] `docs/development/roadmap.md` matches implemented and planned phases
- [ ] `docs/reference/domain-model.md` stays grounded in upstream DS semantics
- [x] `dsctl schema` output is tested to match actual commands
