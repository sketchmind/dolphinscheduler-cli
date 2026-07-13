# dolphinscheduler-cli

`dolphinscheduler-cli` provides `dsctl`, an independent, REST-only command-line
interface for Apache DolphinScheduler. It supports configuration, workflow
authoring, schedules, runtime inspection, and operational recovery with stable
output and error contracts for humans, scripts, and agents.

Python 3.11 or newer is required. DolphinScheduler `3.4.1` is the fully
live-tested target; see [Version Compatibility](docs/user/version-compatibility.md)
for the experimental compatibility tiers.

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
export DS_VERSION="3.4.1"
dsctl doctor
```

`DS_VERSION` defaults to `3.4.1`. Versions `3.4.0` and `3.3.2` are selectable
experimental targets; see
[Version Compatibility](docs/user/version-compatibility.md) for the current
support policy.

You can also load connection settings from a dotenv-style file:

```bash
dsctl --env-file cluster.env context
```

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
Use `dsctl use project NAME` only when you intentionally want later commands to
reuse stored project context. Running a workflow is covered under
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
project-specific discovery protocol.

| Need | Use |
| --- | --- |
| Invoke a known command | Leaf `--help` |
| Obtain its exact machine contract | `schema --command ACTION` |
| Find an action in a known family | Group help or `schema --group GROUP` |
| Ask whether a feature exists or is supported | `capabilities` |

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
dsctl capabilities --section authoring
```

The default is a bounded summary. Use `dsctl capabilities --full` only when
the complete expanded inventory is required; `--summary` remains available as
an explicit spelling of the default view.

Successful JSON may include bounded `next_actions` or a list-level
`action_index`. Follow a suggested command only when it matches the current goal
and has the required mutation authorization; use its `schema_command` only when
exact inputs are still unknown. Server permissions and execution-time state
remain authoritative. A `mutates: true` action must complete before dependent
reads, and table/TSV output remains data-only. See the
[CLI Contract](docs/reference/cli-contract.md) for the complete navigation
categories and safety contract.

## Workflow Authoring

Create workflow YAML from templates and lint it locally before sending it to
DolphinScheduler. Dry-run is the verbose DS request-plan view when that payload
must be inspected:

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
```

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
dsctl workflow-instance digest "$workflow_instance_id"
dsctl workflow-instance watch "$workflow_instance_id"
dsctl task-instance list --workflow-instance "$workflow_instance_id"
```

Copy a task-instance id from the list response when raw logs are needed. Export
a workflow instance before editing runtime task definitions:

```bash
# Reuse or replace these ids with values from the preceding responses.
workflow_instance_id=901
# Replace 902 with an id returned by task-instance list.
task_instance_id=902
dsctl task-instance log "$task_instance_id" --raw

dsctl workflow-instance export "$workflow_instance_id" > instance.yaml
dsctl workflow-instance edit "$workflow_instance_id" --file instance.yaml --dry-run
```

## Output

Structured commands return a stable JSON envelope by default. Raw artifact
operations such as `workflow export`, `workflow-instance export`, templates with
`--raw`, and `task-instance log --raw` instead write their native YAML or text
body on success. Global display options do not change a successful raw artifact;
structured failures still use the stable error contract.

For structured results, global output options may appear before or after the
command path. Examples keep the canonical prefix form:

```bash
dsctl --compact --columns id,name,state workflow-instance list --project etl-prod --page-size 10
dsctl --output-format table workflow-instance list --project etl-prod
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --output-format tsv --columns '*' task-instance list --workflow-instance 901
```

For agents reading structured row or object results, prefer explicit columns and
a small page size, then add compact JSON when useful. This keeps the standard
envelope, types, pagination, resolved selections, warnings, and structured
errors while avoiding wide DS response objects. Standard JSON uses UTF-8
directly instead of escaping ordinary non-ASCII text. Column projection and
page size provide the largest reductions; compact JSON is the final lossless
whitespace optimization.

Successful data and raw artifacts are written to stdout. Structured command
errors are written to stderr with a nonzero exit code. Table and TSV stdout
remain pure row data; partial/non-first-page summaries and warnings are written
to stderr. Raw artifact warnings also use stderr without changing the artifact
body.

`--columns '*'` selects all top-level row fields. Quote `*` so the shell does
not expand it as a filesystem glob.

## Project Principles

- REST-only integration with DolphinScheduler APIs.
- Generated-first contracts for DS-facing request and response shapes.
- Stable command names, output envelopes, and error types for scripts and
  agents.

## Documentation

User documentation:

- [Installation](docs/user/installation.md)
- [Configuration](docs/user/configuration.md)
- [Commands](docs/user/commands.md)
- [Workflow Authoring](docs/user/workflow-authoring.md)
- [Runtime Operations](docs/user/runtime.md)
- [Version Compatibility](docs/user/version-compatibility.md)

Development documentation:

- [Architecture](docs/development/architecture.md)
- [Codegen](docs/development/codegen.md)
- [Tooling](docs/development/tooling.md)
- [Live Testing](docs/development/live-testing.md)
- [Release Process](docs/development/release.md)
- [Roadmap](docs/development/roadmap.md)
- [Contributing](CONTRIBUTING.md)

Reference documentation:

- [CLI Contract](docs/reference/cli-contract.md)
- [Domain Model](docs/reference/domain-model.md)
- [Error Model](docs/reference/error-model.md)
- [Future Capabilities](docs/reference/future-capabilities.md)

Separately installable agent skill source:

- [DolphinScheduler CLI Skill](skills/dsctl/SKILL.md)

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python tools/check_quality_gate.py
```

See [Contributing](CONTRIBUTING.md) for the development workflow,
[Tooling](docs/development/tooling.md) for code generation and live checks, and
the [Release Process](docs/development/release.md) for package verification and
the TestPyPI-first publication flow.
