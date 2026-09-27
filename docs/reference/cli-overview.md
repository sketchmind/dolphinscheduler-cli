# CLI overview

This is the entry contract for contributors. Before changing an invocation,
output, error or mutation, read its command section and applicable shared rules
in the [complete CLI Contract](cli-contract.md). That reference retains the full
examples, exact version boundaries and public compatibility guarantees.

## Invocation and discovery

Use the narrowest known scope:

```bash
dsctl --help
dsctl workflow --help
dsctl workflow create --help
dsctl schema --command workflow.create
```

The installed surface has 181 actions. Each selected exact DS profile decides
whether an action is supported, limited or absent upstream before transport. `DS_VERSION`
uses metadata discovery for configured targets when omitted or `auto`; an
explicit exact value overrides discovery. If metadata cannot identify the
release, matching public API contracts can admit reviewed reads. Candidate
versions never become an exact server identity, even when only one remains.
Writes and authoring require an exact version. Untargeted local authoring keeps
the `3.4.1` baseline. Selecting another profile does not promote it.

With an unresolved automatic target, `schema` and `capabilities` still expose
the installed CLI catalog without network requests or invented version-specific
constraints. Cached candidates add `read_compatible` or `requires_exact_version`
status; enum discovery exposes only complete enum contracts equal across all
candidates. See [Contract Read Discovery](../development/contract-read-discovery.md).

One command catalog supplies routes, ordered inputs, aliases, visibility, help,
parser defaults, path constraints and context-resolution metadata. Typed command
callbacks own service calls. Domain schemas enrich invocation facts with exact
availability, payload and effect information.

Root help explains once that catalog global options may appear before or after
the command path, that JSON preserves types and metadata while table/TSV are
field views, and that filters and field projection bound the information
before format encoding. Leaf help then shows native arguments and options, a compact
`Global options` panel projected from the canonical globals, and the short
footer `Schema: dsctl schema --command ACTION`. Applicable list and dry-run
commands retain pagination and prepared-effect hints. Use `list` to select
identities, `get` for the native resource, `describe` for definition authoring
details, `digest` for runtime progress/failures, and `export` as the starting
artifact for an edit. These views answer different questions and need not
repeat every field from the underlying resource.

## Configuration and selection

- Select a complete source: explicit `--context NAME` or `--env-file PATH`,
  then process `DSCTL_CONTEXT` or `DSCTL_ENV_FILE`, then any present process
  `DS_API_URL`, `DS_API_TOKEN` or `DS_VERSION`, then the saved `default-context`. Same-level selectors
  conflict; connection identities never merge or fall back after a selection
  error. Retry/timeout policy resolves separately: process, profile, built-in.
- `context create|update|list|get|delete` manages named references in the user
  registry; `config get|set|unset default-context` manages its only setting.
  Credentials stay in the referenced env file. No directory files are searched.
- Bare `context` reads the effective target locally; `doctor` performs
  connection and identity diagnostics.
- A selected named context supplies an optional project default. Explicit
  project selection wins; workflows remain explicit. There is no `use` command.
- The parser requires positional `WORKFLOW` for workflow `get`, `export`,
  `describe`, `digest`, `edit`, `online`, `offline`, `run`, `run-task`,
  `backfill`, `delete`, `lineage get`, and `lineage dependent-tasks`. It also
  requires `--workflow WORKFLOW` for task `list`, `get`, and `update`, and for
  `schedule create`. Missing selectors fail before configuration resolution or
  network access. `schedule explain` requires `--workflow` only in create form,
  when `SCHEDULE_ID` is absent; list/runtime filters remain optional.
- Definition and governance resources are generally name-first; resource files
  use DS `fullName`; schedule and runtime instances use documented IDs.
  Names remain opaque, including numeric-looking names where the command owns
  a literal-name selector. Only documented shortcuts accept an ID or code.

## Global output options

| Option | Contract |
| --- | --- |
| `--context NAME` | Select a saved named context |
| `--env-file PATH` | Select the isolated target profile |
| `--format FORMAT` | Choose `json`, `json-compact`, `table` or `tsv`; JSON is default |
| `--columns FIELDS` | Select data fields; JSON retains the surrounding envelope |

Global options may appear before or after the command path. Exact syntax,
validation and examples are in [Global options](cli-contract.md#global-option).
Root framework options `--show-completion SHELL` and `--install-completion SHELL`
require an explicit shell and expose static completion without DS requests. They are
separate from the catalog globals and actions.

## Results and errors

Standard JSON results use the envelope containing `ok`, `action`, `resolved`,
`data`. Nonempty structured `warnings` are optional; each item has a code,
message and applicable facts. Applicable results also contain bounded
`next_actions`, `action_index` or pagination metadata. Object member order is not
a contract. Projection changes `data`, preserving envelope metadata.
`json` retains ordinary objects. `json-compact` replaces declared business row
collections at `data_shape.row_path` with `columns` and positional `rows` when
`data_shape.compact_rows` is `true`; empty and single-row collections
use that same container. All returned fields are retained by default. Compact
business-list projection accepts top-level fields and `*`; select a parent or
use `json` for dotted paths. Other views keep their structures with compact
whitespace. See [JSON layout](cli-contract.md#json-layout) for decoding rules.

Successful results and raw artifacts go to stdout. `workflow export` and
`workflow-instance export` always emit only YAML; templates emit YAML only with
`--raw` when that option is available; `task-instance log --raw` emits only log
text. Templates and logs without raw mode return structured results. Structured
command errors go to stderr with exit status 1; parser usage errors use stderr
and status 2.
SIGINT exits with status 130; an early-closed stdout pipe exits with status 1.
Neither prints a traceback.
JSON contains its warnings; table/TSV stdout stays row-only and diagnostics go to
stderr. Raw artifacts retain their exact body and receive no appended navigation.

Navigation is advisory and derived from existing result facts. It creates no
additional request and grants no mutation permission. Follow-up commands preserve
the explicit target/profile and shell-safe quoting. See
[Output Envelope](cli-contract.md#output-envelope) and [Error Model](error-model.md).

An action-index group's `targets: "all"` covers all returned rows. Truncated
indexes or rows with invalid/duplicate identities use explicit target ids.
Index coverage is separate from remote pagination completeness. In compact
lists, `scope` and `target.field` refer to the decoded logical rows and fields;
`"all"` retains the same returned-row scope.

## Authoring and mutation

Templates and task schemas describe the reviewed subset for the exact selected
version. A source-present task or a parameter model alone does not establish
typed support. Invalid typed input fails; it must not silently become opaque
input. Richer state may require an existing-server baseline for preservation.
Standalone YAML does not manufacture that provenance.

Lint and dry-run validate canonical intent before mutation. Preparation detaches
the exact request used by apply; transport preserves the selected retry and
uncertainty policy. High-impact operations retain their documented confirmation
and force requirements. Attached schedules are independent resources: workflow
export can carry a schedule snapshot, while definition edit sends no schedule
mutation.

Read the relevant command contract for reference resolution, no-op behavior,
readback, confirmation and exact-version exclusions. For changes to task
semantics, also load the relevant
[reviewed family boundary](../development/task-authoring-boundaries.md).

Each task has a main template; only distinct operations remain advertised as
separate scenarios. `template workflow --example` supplies complete parameter,
branch, child-workflow and dependency compositions. See
[Task examples](../user/task-examples.md).

Default dry-run output shows changes, constraints and prepared mutation order.
Add `--columns requests` for the ordered native REST audit view, or
`--columns '*'` to retain both. Apply prepares again against current state.

Runtime results distinguish accepted triggers from resolved instance IDs and
new execution rounds from prior terminal states. Time filters and pagination
include their actual selection/coverage semantics; log windows provide source
positions and continuation. Follow [Operational investigation](../user/operational-investigation.md)
for complete reads, failure evidence, recovery and migration.
