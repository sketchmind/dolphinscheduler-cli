# dolphinscheduler-cli

`dolphinscheduler-cli` provides `dsctl`, an independent, REST-only command-line
interface for Apache DolphinScheduler. It supports configuration, workflow
authoring, schedules, runtime inspection, and operational recovery with stable
output and error contracts for humans, scripts, and agents.

Python 3.11 or newer is required. DolphinScheduler `3.4.1` is the current
stable profile and offline default; see [Version Compatibility](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/version-compatibility.md)
for the experimental compatibility tiers.

Schedule environment inheritance varies by version. The selected CLI profile
reports it through `capabilities --section schedule`; see
[Schedule Environments](docs/user/runtime.md#schedule-environments) for older
servers and the task-level alternative.

## Install

From PyPI:

```bash
python -m pip install dolphinscheduler-cli
dsctl version
```

With `pipx`:

```bash
pipx install dolphinscheduler-cli
dsctl version
```

For a source checkout, use the [Development](#development) setup below.

## Configure

Set the target DolphinScheduler API URL and token with environment variables:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
dsctl doctor
```

With URL and token configured, omitting `DS_VERSION` or setting it to `auto`
first checks reviewed product-version metadata. When exact identification is
unavailable, public Swagger/OpenAPI contracts can admit a bounded set of
reviewed reads. Contract candidates, including a single candidate, never
identify the server's exact release. Writes, authoring, templates and lint
still require an exact version for a configured target. Set `DS_VERSION` from
the deployment's actual release when needed; failed probes never reuse a stale
selection. See [Configuration](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/configuration.md#version-selection).
The CLI has explicit compatibility profiles
for all 37 final releases from `1.3.9` through `3.4.3`, covering 181 stable actions.
Each action has an explicit supported, limited or upstream-absent decision for
every release. `3.4.1` remains the stable target; the other
profiles are experimental until their release gates pass. See
[Version Compatibility](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/version-compatibility.md) for exact scope and
[Live Testing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/live-testing.md#exact-version-profile-gates)
for artifact-bound evidence. A source review or generated-code change does not
refresh live evidence or promote a profile. Completed development campaigns
retain their original installation artifacts; publication still requires
[current-wheel release acceptance](docs/development/release.md#closing-development-acceptance).
The stable
profile is a release policy, not a parent profile or a fallback for other releases.
For example, `workflow-instance execute-task` is blocked on `3.3.1`–`3.4.1`:
those releases expose the API but lack its master command handler. Inspect
`dsctl capabilities --action workflow-instance.execute-task` for the selected target.

Save a reusable named context by referencing a complete dotenv file:

```bash
dsctl context create production --file cluster.env --project etl-prod
dsctl config set default-context production
dsctl context
```

The registry stores the file reference and project; tokens stay in the env file.
Use `--context NAME` or `--env-file PATH` to select one invocation's target.
Without explicit or process selectors, any present process `DS_API_URL`,
`DS_API_TOKEN` or `DS_VERSION` selects the process connection before the saved
default. URL, token and version stay together. Retry and timeout settings can
override the selected profile without changing the connection.
See [Configuration](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/configuration.md) for precedence and task isolation.

`context` inspects the effective target locally. Use `doctor` for connection,
identity and version diagnostics.

## Quick Start

First verify the connection and discover existing resources without changing
cluster state:

```bash
dsctl doctor
dsctl project list

# Replace these example values with names returned by the list commands.
project=etl-prod
workflow=daily-etl
dsctl workflow list --project "$project"
dsctl workflow get "$workflow" --project "$project"
```

`project` and `workflow` above are ordinary shell variables used only to keep
the example consistent; they are not required `dsctl` environment variables.
A selected named context can supply the project when `--project` is omitted.
Workflow selectors remain explicit: workflow `get`, `export`, `describe`,
`digest`, `edit`, `online`, `offline`, `run`, `run-task`, `backfill`, `delete`,
`lineage get`, and `lineage dependent-tasks` require the positional `WORKFLOW`;
task `list`, `get`, and `update`, plus `schedule create`, require
`--workflow WORKFLOW`. The parser rejects a missing required selector before
configuration resolution or network access. `schedule explain` keeps its two
forms: `--workflow` is required only when `SCHEDULE_ID` is omitted. List and
runtime filter options remain optional. Running a workflow is covered under
[Runtime Operations](#runtime-operations).

## Discover Commands

Start at the narrowest level you already know. These are alternatives, not a
sequence that must be executed in full:

```bash
# Known command: construct the invocation here.
dsctl workflow edit --help

# Known resource family, unknown action: browse one group.
dsctl workflow --help

# Unknown resource family: browse the root.
dsctl --help
```

Leaf help is the task-oriented first projection for both humans and agents; it
should be enough to construct the common invocation without learning a
project-specific discovery protocol. Root help explains global option placement
and output tradeoffs once. Leaf help lists native arguments and options, shows a
compact `Global options` panel generated from the same command catalog, and ends
with `Schema: dsctl schema --command ACTION`. List and dry-run commands retain
their applicable pagination and prepared-effect hints.

Use `dsctl --show-completion zsh` to preview a completion script, or
`dsctl --install-completion zsh` to install it; specify your shell explicitly.
`-h` aliases `--help`; `--version` prints only the installed CLI version. See
[Shell Completion](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/installation.md#shell-completion).

| Need | Use |
| --- | --- |
| Invoke a known command | Leaf `--help` |
| Obtain its exact machine contract | `schema --command ACTION` |
| Find an action in a known family | Group help or `schema --group GROUP`; schema marks selected-version availability |
| Ask whether a known action is supported | `capabilities --action ACTION` |
| Browse product-level feature/version support | `capabilities` or one `--section` |

`schema` is the structured, versioned reflection of CLI arguments, choices,
constraints, payload hints, and output shapes. It is not a mandatory preflight,
and JSON is not inherently more token-efficient than help text. Choose only the
narrowest query needed for the current decision:

```bash
# Known action.
dsctl schema --command workflow.edit
dsctl schema --command task-type.schema

# Known group, unknown action.
dsctl schema --group workflow

# Unknown group.
dsctl schema
dsctl schema --list-groups
```

`dsctl schema --full` retains the expanded whole-surface contract for audits
and generators; it is not the normal agent discovery path.

Use `capabilities` only for product-level feature and version discovery. It is
neither an argument schema nor a required step before executing a known command:

```bash
dsctl capabilities
dsctl capabilities --action workflow.create
dsctl capabilities --section authoring
```

The default is a bounded summary. Use `dsctl capabilities --full` only when
the complete expanded inventory is required; `--summary` remains available as
an explicit spelling of the default view. Resource and authoring sections
describe the installed CLI surface, which is stated directly in
`data.surface`; use `--action ACTION` for the selected DolphinScheduler
version's executable availability.

Successful JSON may include bounded `next_actions` or a list-level
`action_index`. Follow a suggested command only when it matches the current goal
and has the required mutation authorization; use its `schema_command` only when
exact inputs are still unknown. Server permissions and execution-time state
remain authoritative. A `mutates: true` action must complete before dependent
reads, and table/TSV output remains data-only. See the
[CLI Contract](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md) for the complete navigation
categories and safety contract.

## Workflow Authoring

Create workflow YAML from templates and lint it locally before sending it to
DolphinScheduler. Dry-run shows prepared changes and known blockers; add
`--columns requests` when the native REST payload must be inspected:

```bash
dsctl template workflow --raw > workflow.yaml
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
```

Inspect task fragments and type-specific fields only when the authored workflow
needs them:

```bash
dsctl template task SHELL --raw
dsctl task-type schema SHELL
DS_VERSION=3.4.1 dsctl template task DATAX --raw
DS_VERSION=3.4.1 dsctl template task CHUNJUN --raw
DS_VERSION=3.4.1 dsctl template task MR --raw
DS_VERSION=3.4.1 dsctl template task SQOOP --raw
DS_VERSION=3.3.2 dsctl template task PYTORCH --raw
```

Omit `--variant` for the main template. `dsctl task-type get TYPE` lists
meaningful scenarios, such as resource scripts, output parameters, dependency
targets, or different business operations. A named business operation can also
be the default: DVC retains `upload`, `download`, and `init`, with `upload`
selected when the option is omitted. Ordinary input parameters and optional
fields are explained in the main template; there is no `minimal` selector.

Task templates and schemas describe the reviewed subset for the selected
`DS_VERSION`, including runtime prerequisites and opaque-state restrictions.
Inspect that schema and [Workflow Authoring](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/workflow-authoring.md)
before applying a task. A template does not prove that a worker has its required
plugins, resources or data access.

Export an existing workflow, edit the YAML, and apply the full edited document:

An exported `schedule:` block is verified as a read-only snapshot during edit;
schedule changes remain explicit schedule operations.

```bash
dsctl workflow export daily-etl --project etl-prod > workflow.yaml
dsctl --columns diff,no_change,workflow_state_constraints,schedule_impacts \
  workflow edit daily-etl --project etl-prod --file workflow.yaml --dry-run
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml
```

For small changes, start from a patch template:

```bash
dsctl template workflow-patch --raw > patch.yaml
dsctl workflow edit daily-etl --project etl-prod --patch patch.yaml --dry-run
```

## Schedule Operations

Discover and inspect the attached schedule before changing it. The numeric
value below is an ordinary shell variable for this example, not `dsctl`
configuration:

```bash
project=etl-prod
workflow=daily-etl
dsctl schedule list --project "$project" --workflow "$workflow"

# Replace 26 with the id returned by schedule list.
schedule_id=26
dsctl schedule get "$schedule_id"
dsctl schedule preview "$schedule_id"
dsctl schedule explain "$schedule_id" --cron '0 0 2 * * ?'
```

`schedule explain` reviews the proposed mutation without changing remote state.
When applying a change, follow its confirmation guidance. An online schedule
must be taken offline before `schedule update`, and activation remains an
explicit `schedule online` operation.

## Runtime Operations

Running a workflow changes cluster state. Copy one id from
`data.workflowInstanceIds` in the run response into the ordinary shell variable
shown below:

```bash
project=etl-prod
workflow=daily-etl
dsctl workflow run "$workflow" --project "$project"

# Replace 901 with an id returned by workflow run.
workflow_instance_id=901
dsctl workflow-instance digest "$workflow_instance_id" --project "$project"
dsctl workflow-instance watch "$workflow_instance_id" --project "$project"
dsctl task-instance list --project "$project" --workflow-instance "$workflow_instance_id"
```

Copy a task-instance id from the list response when raw logs are needed. Export
a workflow instance before editing runtime task definitions:

```bash
# Reuse or replace these ids with values from the preceding responses.
workflow_instance_id=901
# Replace 902 with an id returned by task-instance list.
task_instance_id=902
dsctl task-instance log "$task_instance_id" --raw

dsctl workflow-instance export "$workflow_instance_id" --project "$project" > instance.yaml
dsctl workflow-instance edit "$workflow_instance_id" --project "$project" --file instance.yaml --dry-run
```

`task-instance get`, `watch`, `savepoint` and `stop` also accept a task-instance
ID with only `--project`, including standalone STREAM tasks. Supply
`--workflow-instance` when known to narrow the lookup. Log access remains ID-only.

## Output

Structured commands return a stable JSON envelope by default. Raw artifact
operations have explicit success rules: `workflow export` and
`workflow-instance export` always write only native YAML; template commands
that offer `--raw` write a native YAML artifact only when it is selected; and
`task-instance log --raw` writes only the log text. Without those raw modes,
templates and logs use the standard structured result. Global display options
do not change a successful raw artifact; structured failures still use the
stable error contract.

If display formatting fails after an operation returns, the error retains its
`data` and `resolved` identities and marks `phase: output_render`. Inspect that
result; do not repeat a mutation just to change its display columns.

For structured results, global output options may appear before or after the
command path. Use `--format json|json-compact|table|tsv` to select the display format. The
previous `--output-format` name is no longer accepted.
`resource download --output PATH` still sets the download destination. Examples
keep the canonical prefix form:

```bash
dsctl --format json-compact --columns id,name,state workflow-instance list --project etl-prod --page-size 10
dsctl --format table workflow-instance list --project etl-prod
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --format tsv --columns '*' task-instance list --project etl-prod --workflow-instance 901
```

For agents reading structured results, use filters, explicit columns and a small
page size to bound the information. `json` keeps the usual objects;
`json-compact` encodes declared business lists as `{"columns":[...],"rows":[...]}`
at their existing row path, with one value array per line. Match each value to
the column at the same position. The default retains all returned fields;
nested objects, arrays and numeric types remain JSON values. Details, schemas,
templates and diagnostics keep their structures with compact whitespace.
Both formats preserve the envelope, pagination, resolved selections, warnings
and navigation. See [JSON layout](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md#json-layout)
for empty lists, encoding boundaries and decoding.

Successful data and raw artifacts are written to stdout. Structured command
errors are written to stderr with a nonzero exit code. Table and TSV stdout
remain pure row data; partial/non-first-page summaries and warnings are written
to stderr. Raw artifact warnings also use stderr without changing the artifact
body.

`--columns '*'` selects all top-level row fields. Quote `*` so the shell does
not expand it as a filesystem glob. Compact business lists accept top-level
columns only; select a parent field or use `json` for dotted-path projection.

## Project Principles

- REST-only integration with DolphinScheduler APIs.
- Generated-first contracts for DS-facing request and response shapes.
- Stable command names, output envelopes, and error types for scripts and
  agents.

## Documentation

User documentation:

- [Installation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/installation.md)
- [Configuration](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/configuration.md)
- [Commands](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/commands.md)
- [Workflow Authoring](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/workflow-authoring.md)
- [Complete Task Compositions](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/task-examples.md)
- [Operational Investigation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/operational-investigation.md)
- [Runtime Operations](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/runtime.md)
- [Version Compatibility](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/version-compatibility.md)

Development documentation:

- [Architecture](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/architecture.md)
- [Multi-Version Compatibility Decision](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/decisions/0001-multi-version-compatibility.md)
- [Multi-Version Compatibility Architecture](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/multi-version-architecture.md)
- [Codegen](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/codegen.md)
- [Tooling](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/tooling.md)
- [Live Testing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/live-testing.md)
- [Release Process](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/release.md)
- [Roadmap](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/roadmap.md)
- [Contributing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CONTRIBUTING.md)

Reference documentation:

- [CLI Contract](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md)
- [Domain Model](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/domain-model.md)
- [Error Model](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/error-model.md)
- [Future Capabilities](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/future-capabilities.md)

Separately installable agent skill source:

- [DolphinScheduler CLI Skill](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/skills/dsctl/SKILL.md)

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python tools/check_quality_gate.py --mode development
```

See [Contributing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CONTRIBUTING.md) for the development workflow,
[Tooling](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/tooling.md) for code generation and live checks, and
the [Release Process](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/development/release.md) for package verification and
the TestPyPI-first publication flow.
