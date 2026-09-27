# Architecture

`dsctl` is a REST-only DolphinScheduler CLI. Generated contracts own the exact
wire; handwritten modules own user intent, domain recipes, transport and output.
A contributor changing one DS release should follow one exact source decision
through one domain binding, without editing parallel CRUD wrappers or command
parameter lists.

The accepted compatibility policy is recorded in
[ADR 0001](decisions/0001-multi-version-compatibility.md) and
[Multi-Version Compatibility Architecture](multi-version-architecture.md).
The [maintenance principles](#maintenance-principles) below guide changes to
those boundaries. Task-specific public behavior belongs in the
[CLI Contract](../reference/cli-contract.md) and
[Workflow Authoring](../user/workflow-authoring.md).

## Current Stable Surface

The installed CLI has 181 actions. All 6,697 action/version coordinates have an
explicit decision: 5,797 supported, 5 limited and 895 upstream-absent. This
coverage is separate from live verification and
[support policy](../user/version-compatibility.md#support-policy-and-verification).
The original `3.4.3` admission added source and contract coverage without
claiming live evidence at admission.
Current artifact-bound coverage is recorded in the
[live evidence matrix](live-testing.md#exact-version-profile-gates).
Its task updates use the whole-workflow transaction, user/token identity
selection uses the native summary VO, instance lists use their native summary,
and schedules expose their native missed-fire policy. None of these recipes
inherit a neighbouring profile.

Use `dsctl --help` to discover a group, `dsctl GROUP ACTION --help` for an
invocation, and `dsctl schema --command GROUP.ACTION` for structured details. Command
syntax lives in the command catalog, rather than another architecture list.

## DS Model Lens

| Plane | Objects |
| --- | --- |
| Governance | User, token, tenant, queue, worker group, environment, cluster, datasource, namespace, resource, alert plugin/group |
| Project | Project, project parameter/preference/worker-group, task group |
| Design | Workflow definition, task definition, relation, lineage, schedule |
| Runtime | Workflow instance, task instance, command, audit, log, health |

The CLI calls a DS process definition a workflow. Native IDs and codes remain
distinct values. Names are opaque strings; numeric-looking names do not become
IDs implicitly. Resources use DS `fullName` paths. Schedules and runtime
instances use their documented IDs. See [Domain Model](../reference/domain-model.md).

## Layer Architecture

```text
command catalog ──> Typer adapter ──> typed command callbacks
                                         │
                                      services
                                         │
                                  bound domain recipes
                                         │
                              exact generated wire programs
                                         │
                              WireExecutor ──> HTTP client
```

| Owner | Responsibility | Does not own |
| --- | --- | --- |
| Foundation | Configuration, HTTP transport, JSON boundary checks, errors, output | DS version recipes |
| Command catalog | Routes, ordered inputs, help, aliases, defaults, resolution and path rules | Service execution |
| Commands | Typed callback arguments, service calls, result emission | HTTP, DS field names or version branches |
| Services | Selection context, authoring intent, confirmation, public errors and output shaping | Generated imports, controller paths or transport retry policy |
| Upstream domains | Native identity resolution, exact recipes, preparation, preservation and readback | Typer, stdout or service imports |
| Generated | Reviewed exact request/response schemas, bindings and provenance | CLI ergonomics or inferred compatibility |

`import-linter` enforces the direction: commands → services → upstream →
generated. Shared foundation modules do not import upward. `JsonValue` and
`JsonObject` are boundary types; domain interfaces use named records and intents.

### Foundation

Configuration resolves one immutable invocation snapshot from an explicit
context/file selector, process selector, present process connection values, or
saved user default, in that order. One user registry stores named env-file
references, their bound API URLs and optional project defaults; credentials
remain in the source files. URL/token/version select identity; retry and timeout
policies resolve independently from process, selected profile, then defaults.
No directory or legacy selection files participate.
Registry writes use a short lock and atomic replacement; network operations use
the resolved snapshot outside that lock. Workflows require explicit selection.
See [Configuration](../user/configuration.md) and the
[acceptance design](persistent-connection-context-design.md).

The shared `DolphinSchedulerClient` owns connection settings, authentication,
timeouts, HTTP errors, result envelopes and transport retry behavior. A bound
wire program supplies its reviewed execution mode so a read can retry while an
uncertain mutation is not silently repeated.

Services translate upstream failures into the stable public error contract.
Generated-response diagnostics contain bounded field/type information without
repeating rejected values. Output rendering owns JSON, table, TSV and column
projection. Services return data and an optional failed outcome; they do not write stdout.
A failed diagnostic result retains its report while the shared renderer chooses
stderr and a nonzero exit status.

Service facts, the selected information view, and format encoding are separate
steps. YAML templates store one body; line views are derived by the renderer.
Dry-run preparation stores one ordered request plan; default rendering retains
semantic changes and prepared stage order, while explicit columns expand wire
details. `json-compact` encodes only business row collections explicitly marked
with `data_shape.compact_rows: true`, at their existing `row_path`, including
explicitly declared nested collections in summary views. Sibling data remains
unchanged. A broken declared path or row shape raises `output_contract_error`.
It retains all selected top-level fields in a shared `columns` array and native
JSON values in `rows`; other views only compact whitespace. The renderer does
not infer eligibility from row count or sparsity, recursively encode nested
values, or apply table/TSV defaults to remove JSON fields. Error details and
changing page coverage remain visible in text formats.

`execution_states.py` owns reviewed state meanings shared by runtime validation
and navigation. Exact action availability and permission decisions remain
separate. Navigation binds concrete targets through the command catalog; replay
baselines, trigger receipts and log continuation come from the responsible
runtime domains rather than being guessed from a final state.

### Target Version Resolution

Pure configuration separates `TargetSettings` and `ConnectionSettings` from
an exact `ClusterProfile`. `services/version_resolution.py` resolves the target
once per command before availability checks and runtime binding. The CLI
composition root injects its invocation scope into the emitter; foundation
modules do not import services. Runtime callers can carry an already resolved
profile through workflow authoring and execution.

`upstream/version_discovery.py` owns bounded REST bootstrap probes. The canonical
runtime compiler derives product metadata and public document facts from all
37 exact source snapshots before slicing. `generated/version_discovery.py`
exports those facts from logical evidence, type, parameter, operation and
profile modules. Equal records can share implementation; source memberships
remain independent. Bootstrap discovery adds no domain operation or public
command, and explicit versions remain authoritative.

Resolution distinguishes exact metadata from `CompatibilityResolution`.
`upstream/api_contract_discovery.py` requires complete public route coverage
within each document group to retain candidates, then matches verifiable
operation parameters across every candidate. This is evidence of a compatible
surface, not exact release identification. Unknown nested request shapes and
mismatches cannot authorize their operations; custom builds remain possible.

`upstream/read_compatibility.py` independently reviews an action's transitive
read closure, codec behavior, selectors, projections, enums and error semantics.
Only observed compatible operations can enter that closure. An admitted read
uses an existing exact compiler profile internally, with a transport policy
that rejects unapproved or mutating requests. Public output keeps the exact
server version unknown and adds `resolved.target` plus a `read_compatibility`
warning. Writes and authoring continue to require exact selection.

A URL- and credential-scoped five-minute cache supports offline discovery;
candidate entries also bind the generated discovery digest. Every remote
invocation and automatic `doctor` refreshes the observation. Successful
unresolved results replace positive entries, and failed probes invalidate old
entries. No stale or default selection rescues a failed remote probe. Cold-cache
schema/capabilities expose only the installed catalog; candidate enum discovery
requires equal complete members across all candidates. Untargeted local
authoring keeps its documented baseline, while help and context stay offline.

[Version and Read Discovery](contract-read-discovery.md) records metadata
rules, probe bounds and the reviewed read scope. Metadata-only observations
do not attest contract-read admission.

### Commands

`command_contract.py` exposes the contract API; `_command_catalog.py` contains
all 181 invocation declarations and `_command_contract_model.py` their immutable
value types. The Typer adapter binds these declarations to ordinary typed
callbacks. It validates parameter names and types before constructing parser
metadata. Callbacks retain explicit service calls and normal Python defaults.

Schema discovery derives its command tree directly from catalog routes.
Every catalog action declares its remote/local effects, including the conditional
dry-run effect where applicable. Schema 3 projects those same declarations.
Group descriptions and domain payload overrides add only facts
that the invocation catalog does not own; there is no parallel action tree.
Selected-version availability enriches the resulting commands. Parse defaults, context resolution and
service requirements are distinct facts: an optional parser argument may still
be required by a service, and an exact version may constrain an input further.
Hidden compatibility options stay hidden in help and discovery. Help, schema and
shell-safe follow-up commands consume one invocation source.

### Generated

Source inventory, reviewed membership, and runtime packaging have separate
owners. `exact_sources.json` can inventory an unreviewed release;
`version_profile_decisions.json` owns reviewed exact profiles;
`runtime_bundles.json` selects which reviewed profiles are installed. The default
files currently name the same 37 releases, but neither source discovery nor a
matching contract adds a runtime profile. A selected profile must have every
required domain, task, and datasource decision before generation succeeds.

Extractor response corrections also separate reusable source rules from exact
review membership. Candidate rules must prove their declared Java structure
against the candidate's own source. Historical exact-only corrections remain
exact-only; sharing an extraction rule does not share a support decision.

`tools/generate_ds_runtime_bundles.py` atomically regenerates the tracked runtime
from the exact sources declared in `tools/ds_codegen/runtime_bundles.json`.
Never hand-edit generated output or refresh one tracked version independently.
Use `tools/generate_ds_contract.py` for a complete single-source audit artifact.
Full audit contracts remain reproducible even when their unused code is absent
from the installed CLI. Exact runtime slices with no remaining source operations
contain only their enums and identity artifacts; their empty client, model and
operation facades are not emitted. The standalone audit generator still emits
the complete contract package.

The shared compiler builds 21 domain plans independently from every original
exact bundle. The `workflow_runtime` plan includes workflow definitions,
schedules, lineage, tasks, runtime instances and logs; the other plans own their
respective DS domains. Only after all plans are compiled does the compiler strip
their local source-operation ownership union and unreferenced type closure from
the legacy slices. Explicit domain-port dependencies preserve cross-domain reads
without duplicating local executable ownership.

| Generated area | Role |
| --- | --- |
| `versions/` | Exact enums, source manifests and artifact identity |
| `wire_programs/` | Exact domain profiles, programs and codec records |
| `wire_programs/_schemas/` | Request roots, response closures and shared closed types |
| `wire_runtime/` | Shared request/response support |
| Profile modules | Reviewed decisions, recipes, task/runtime profiles and conformance catalogs |

There is one executable compiled-domain pipeline. The retired family catalog,
wrapper kernels and broad generated sessions are no longer alternate runtime
paths. Historical artifact schemas keep their empty family-binding identity
fields for audit compatibility.

Mechanical sharing does not create semantic membership. Codec records and
identical closed type definitions may share implementation, while each exact
profile retains its source closure and reviewed bindings. A shared type identity
includes its complete definition; a changed enum member, order or value must
produce a separate node. Root executable digests and the artifact manifest cover
the resulting imports and shared module bytes. Missing or modified dependencies
fail installation validation.

Generated default annotations represent the value actually stored by Pydantic,
including unresolved native symbolic defaults. The validation schema still uses
the original DS input type: a stored omitted default does not make an otherwise
invalid explicit input acceptable. Regression tests cover values, validation,
serialization and schema output together.

### Upstream

A bound domain selects an exact profile once, then exposes caller-oriented
methods. Domain recipes may perform resolution and verification reads around
one explicit mutation. They retain native name/id/code distinctions, field
omission, response normalization and version-specific preservation rules.
No exact profile imports or inherits a neighboring version's package.

Handwritten integration modules use domain or role names such as `projects.py`,
`workflows.py`, `code_native_reads.py` and `id_native_reads.py`. `wire.py` owns
request preparation and execution, `response_projection.py` owns response
decoding helpers, and `mutation_outcomes.py` owns dispatch/readback uncertainty.
The old `generated_*` adapter names are retired. Actual generated code lives
under `generated/`, including the task-profile data.

A compiled program validates and detaches one `WireRequest` during preparation.
`WireExecutor` sends that request directly through the shared client, then gives
the payload to a decoder with no transport capability. Dry-run and apply use the
same prepared request. Fingerprint checks bind preparation to the selected
program and installed generated bytes. File upload and binary download keep
explicit transport methods.

Project definition reads and lifecycle readback share `CompiledProjectReads`.
Its compiled list/get execution and paging have one owner. Each read boundary
retains its identity validation and error vocabulary; legacy owner lookup and
mutation verification remain explicit domain policies.

`TaskLogTail` is the canonical log paging and response-normalization seam.
Task-definition mutation similarly hides whether an exact profile can safely
update one task or needs a whole-workflow operation. Services request intent;
the selected domain owns that DS recipe.

### Composition Policy

Task source facts and reviewed authoring claims have separate owners.
`task_profile_facts.json` contains extracted upstream facts;
`task_profile_reviews.json` contains exact reviewed claims. The task compiler
materializes `generated/task_profiles.py`, both through the canonical atomic
runtime generator and the focused `generate_ds_task_profiles.py` command.
Services consume its selected facts through `upstream/task_profiles.py`.
A source-present plugin is not automatically a typed authoring capability.

The model layer registers each reviewed family's canonical parameter model once.
The catalog reads structural field types, requiredness, choices and defaults
from the selected model where they agree with the public contract; semantic
exceptions and ambiguous union paths stay explicit. Upstream projection pairs
encode/decode functions by family with separate typed signatures for resources,
workflow references and local task references. These registrations choose
implementation; exact review data still decides eligibility. Discovery uses the
same `TaskAuthoringField` value type as reviewed catalog entries, including the
seven explicitly registered model-only families. No dormant generic DEPENDENT
builder competes with the selected-version reviewed facet.

The handwritten task packages organize changes by task family within each
existing layer:

| Package | Family implementation | Shared owner |
| --- | --- | --- |
| `models/task_spec/` | Canonical models, nested values and field validators | Base types, normalization and explicit model registry |
| `upstream/task_authoring_surface/` | Exact support, runtime epochs and preservation boundaries | Surface aggregation and shared records |
| `upstream/task_parameter_projection/` | Paired encoding, decoding and preservation checks | Typed dispatch, graph context and reference interfaces |
| `services/task_authoring_catalog/` | Field descriptions, templates and authoring policy | Catalog lookup, explicit registration and model-derived structure |

For example, K8S changes start in each package's `k8s.py`; Kubeflow retains its
own semantic module. A family may contain several exact wire epochs. These
packages do not merge model availability, authoring authorization or wire
selection into one registry, and do not discover plugins dynamically.

`task_references.py` owns the reviewed local-reference locations used by DAG
construction, patch rename and exact parameter projection. Each consumer keeps
its own validation, runtime-state and preservation policy. `task_settings.py`
owns direct authoring-to-native task-node bindings and their actual encoding and
export normalization. Discovery and authorized overlay derive native field names
from those bindings; timeout coupling and exact overlay permissions stay explicit.

Typed validation, exact projection, explicit opaque authoring and unchanged
preservation are distinct operations. Invalid typed input must not downgrade to
opaque input. Preservation that requires an existing-server baseline cannot be
recreated by exporting YAML and later importing it independently. Runtime holes,
resource/parameter restrictions and worker prerequisites remain exact decisions.
Use the selected task schema for the family-specific contract.

### Services

Services resolve explicit flags and saved context, validate command intent,
prepare canonical authoring plans, apply confirmation policy and shape results.
Paged list services share the result envelope around the existing pagination
engine, while retaining domain serializers and error translators.

The output boundary emits each warning once as a structured object and omits
the warning field when empty. JSON layouts share that same envelope; table/TSV
project business data and send diagnostic messages to stderr. Navigation uses
only already returned identities and state, derives command effects from the
catalog, and performs no I/O. A related instance id does not by itself establish
its project, so cross-object hints require the target's scope as well as its id.

`services/workflow/` exposes the command-facing operations and groups their
implementations into reads, creation, editing, execution and lifecycle. Selected
native identities, schedule lookup, execution options and error translation
have local owners. `services/workflow_instance/` similarly separates reads,
editing, actions and watching. Shared authoring, compilation, patching,
rendering and schedule helpers live in `services/_workflow/`; they do not
import the public service packages. The package entry points export operations
without duplicating their implementations.

Workflow compilation captures an isolated specification, task identities, edges
and deterministic preview once. Apply binds server-allocated identities to that
plan after preflight. Existing-task and no-op edits allocate no new codes.

Native graph encoding and preservation sit below the service seam. Legacy
string task IDs and modern numeric task codes must not be conflated. Read
hydration resolves resource and child-workflow references best effort; unresolved
or richer state keeps its opaque provenance. Authoring resolution verifies
required targets before mutation.

Attached schedules are independently persisted resources. Their authoritative
lookup is separate from workflow detail and lightweight selector resolution.
Export includes that record; full-file edit treats it as a concurrency snapshot
and sends no schedule mutation. A workflow-offline cascade is handled according
to the documented edit contract. Workflow create reuses its schedule mutation's
result, and workflow offline refreshes schedule state after the cascade.

Workflow templates resolve the same local exact target as task templates and
lint. One canonical skeleton demonstrates shared parameters and task dependencies;
task templates own runtime controls and task-specific graph rules. Optional
metadata defaults come from the authoring model, and execution-mode availability
comes from the generated workflow recipe. Schedule templates and parsing share
the schedule recipe's timezone capability. The workflow-create schema derives
file structure from those same models and exact capabilities, separates CLI
arguments from YAML fields, and links to task and execution-context contracts.
Semantic validation remains in the authoring and graph compiler; a bounded task
schema outline is not a substitute for those checks. No separate per-release
templates or parallel field-definition registry is maintained.

## DS Evolution Workflow

1. Add an explicit exact source identity and prepare its read-only checkout.
2. Extract the source contract; inspect structural changes and the source,
   effective-wire, consumed-projection and preservation fingerprint axes.
3. Review changed domain recipes and authoring claims against controller,
   model and executor evidence. Record unsupported or absent coordinates
   explicitly; matching shapes do not approve a new version.
4. Change compiler inputs or generator logic, then regenerate the complete
   runtime atomically. Retain complete reviewed source closures and provenance.
5. Test independent exact request/response expectations, invalid inputs,
   preservation and transport uncertainty. Run the development gate.
6. For release or promotion, build one canonical wheel, run the required exact
   installed-wheel campaigns, track receipts and pass the release gate. Do not
   rebuild an attested wheel or reuse a historical receipt for changed bytes.

See [Codegen](codegen.md), [Tooling](tooling.md) and [Release](release.md) for
commands and detailed obligations. `references/` is optional ignored workspace
state, read-only from this project's perspective.

The [3.4.2 EMR Serverless evolution exercise](task-evolution-exercise.md)
traces a real new task through those owners. Every generated authoring review
or exclusion must register its implementation; model-only families are explicit
registrations. An omitted builder fails instead of silently bypassing facet
policy.

## Maintenance Principles

- Keep one edit path per responsibility: CLI inputs start in the command
  catalog, task structure in the selected model, and exact wire changes in
  compiler inputs or the generator. Remove superseded declarations in the same
  change instead of keeping parallel implementations.
- Judge a refactor by the decisions and edit points a contributor must
  understand. File movement, formatting compression and line-count reductions
  alone do not demonstrate simpler maintenance. Split modules around concrete
  responsibilities; keep their dependency direction explicit.
- Share generated types only when complete dependency identities and
  validation/serialization behavior agree. Keep exact review membership,
  preservation rules and independent expected requests separate even when
  their current values happen to match.
- For a DS upgrade, record changed source decisions, domain recipes, typed
  claims and unsupported coordinates. Compare all four fingerprint axes,
  regenerate atomically and follow the evolution workflow above.
- Validate observable behavior through public seams and isolated installed
  entry points, including completion, pipe closure, signals and error channels.
  Minimum and current dependency environments are separate obligations.
  Historical test counts or receipts never replace checks on changed code or
  a new candidate artifact.

## Quality Gate

`python tools/check_quality_gate.py --mode development` runs lint, formatting,
architecture and type checks, generated freshness, static conformance assessment,
and portable, source-contract and source-rebuild tests. `--portable` is the
clean-checkout lane; additional Python versions use `--portable --tests-only`.
Diagnostic skips report partial checks.

`--mode release` includes all development checks plus the three current-wheel
evidence checks. It rejects partial lanes and skip options. `--include-live`
explicitly appends the destructive source-tree suite and requires configured
live-test flags and credentials; it does not replace installed-wheel receipts.

Tests should cross the public module seam and assert independent observable
behavior. Share expensive exact-source preparation, but retain independent
expected requests, version matrices and negative cases. Fakes should represent
collaborator outcomes without reimplementing DS routes, field projection or the
compiler. Verify a clean built wheel as well as the source tree.

[Dependency compatibility](dependency-compatibility.md) records the executable
dependency floor and its CI matrix. [Release](release.md) owns installed-artifact
validation and current-wheel evidence requirements; local campaign progress
belongs in ignored build output.
