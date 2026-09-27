# dolphinscheduler-cli

Manage Apache DolphinScheduler from your terminal with `dsctl`. Author workflows
as YAML, preview changes, and inspect schedules, executions and task logs through
DolphinScheduler's REST APIs.

- **Workflow authoring:** templates, validation, export/edit and change previews.
- **Operations:** scheduling, backfills, execution monitoring and recovery.
- **Governance and resources:** projects, users, tenants, datasources and resource
  files.
- **Automation:** structured JSON results, typed errors, and table or TSV views.
- **Compatibility:** exact profiles for 37 DS releases from `1.3.9` to `3.4.3`,
  with documented support for each operation.

This independent CLI requires **Python 3.11+** and access to a DolphinScheduler
API. See [Version compatibility](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/version-compatibility.md)
for supported operations and deployment prerequisites.

[Documentation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/README.md)
· [Agent skill](#use-with-coding-agents)
· [Changelog](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CHANGELOG.md)
· [Contributing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CONTRIBUTING.md)

## Install

Install the released package in an isolated environment with `pipx`:

```bash
pipx install dolphinscheduler-cli
dsctl --version
```

Alternatively, use `python -m pip install dolphinscheduler-cli`.
[Installation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/installation.md)
covers source installs, upgrades and shell completion.

This README follows `main`, including unreleased changes. For an installed
release, use the documentation at the tag matching `dsctl --version`.

## Quick Start

Set your API URL, token and exact server version. Replace these example values
with your deployment's settings:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
export DS_VERSION="3.4.1"

dsctl doctor
dsctl project list
```

`doctor` checks connectivity and authenticated identity. Choose a project from
the results, then list and inspect its workflows:

```bash
dsctl workflow list --project etl-prod
dsctl workflow digest daily-etl --project etl-prod
```

Use your own project and workflow names in place of `etl-prod` and `daily-etl`.
`digest` gives a compact graph overview; use `workflow describe` when you need
task configuration details.
[Configuration](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/configuration.md)
explains saved connections, credential files and switching clusters.

## Workflow Lifecycle

Create a workflow and run it once, then iterate: **inspect → repair the instance
→ rerun or recover → verify**. Keep the definition in sync for future runs.
Once the result meets its goal, enable recurring scheduling. Pause or retire
the workflow when the work is complete.

### 1. Prepare a definition

For a new workflow, generate a template. Set `workflow.name` to `daily-etl`,
`workflow.project` to your chosen project, and keep `workflow.release_state`
`OFFLINE` while editing its tasks:

```bash
dsctl template workflow --raw > workflow.yaml
# Edit workflow.yaml with your names, project and tasks.
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
```

Review the dry-run before applying. Lint checks the local definition; dry-run
resolves references and previews the target changes. Create the definition once,
then use the resulting execution as the starting point for refinement.

For task examples, use `dsctl template task SQL --raw`; `dsctl template task`
lists other families. Inspect `dsctl task-type schema SQL` for parameter
constraints. See [Workflow authoring](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/workflow-authoring.md)
and [Task examples](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/task-examples.md)
for dependencies and reusable compositions.

### 2. Iterate until the result meets your goal

With the definition and worker prerequisites ready, bring the workflow ONLINE
and run it manually. ONLINE makes the definition runnable; schedule activation
is a separate step below:

```bash
dsctl workflow online daily-etl --project etl-prod
dsctl workflow run daily-etl --project etl-prod
```

Use the instance ID from the run result or its suggested discovery command.
To find a run from history, use
`dsctl workflow-instance list --project etl-prod --workflow daily-etl`.
Replace `901` with the selected instance ID and inspect its progress:

```bash
dsctl workflow-instance digest 901 --project etl-prod
```

If it is still active and completion is needed, wait for up to ten minutes.
`--exit-status` makes successful execution the shell success condition:

```bash
dsctl workflow-instance watch 901 --project etl-prod --timeout-seconds 600 --exit-status
```

Compare task results and outputs with the intended outcome. For a failure, use
its task ID from the digest, replacing `902` below, to read the recent log:

```bash
dsctl task-instance log 902
```

Use `dsctl task-instance list --project etl-prod --workflow-instance 901` when
you need to select other tasks in the run. Add `--raw` to the log command for
plain text; structured logs retain line coverage and continuation.

When a finished run fails or its output needs refinement, export that instance
and edit its tasks. The following development loop uses `--sync-definition` to
save the repair to both the instance and the workflow definition immediately,
so future runs use the revised graph:

```bash
dsctl workflow-instance export 901 --project etl-prod > instance.yaml
# Edit instance.yaml, keeping the complete desired task set.
dsctl lint workflow instance.yaml
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml --sync-definition --dry-run
dsctl workflow-instance edit 901 --project etl-prod --file instance.yaml --sync-definition
```

After reviewing and applying the repair, choose one execution action:

| Intent | Command |
| --- | --- |
| Recheck the whole workflow, including tasks that previously succeeded | `dsctl workflow-instance rerun 901 --project etl-prod` |
| Resume a `FAILURE` instance from its failed tasks, retaining successful work | `dsctl workflow-instance recover-failed 901 --project etl-prod` |

Use the result's suggested `watch` command, retaining its `--after-run-times`
marker, to observe the new execution round. Inspect the result and repeat until
the output meets the goal. Choose a full rerun when changes affect previously
successful tasks or their outputs need to be regenerated. Read a fresh digest
after each replay and use its current task IDs for logs.

For a small repair or a task rename, use
`dsctl template workflow-instance-patch --raw` and `workflow-instance edit --patch`.
Instance repair covers tasks, dependencies, global parameters and timeout.
Use `workflow export` and
`workflow edit` with an OFFLINE definition for definition metadata or changes
before the first run. Follow [scheduled-workflow editing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/workflow-authoring.md#edit-a-scheduled-workflow)
when revising a workflow that already has a schedule. The
[runtime guide](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/runtime.md)
explains instance-only edits and exact-version synchronization rules;
[Operational investigation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/operational-investigation.md)
covers recovery choices and deeper log analysis.

### 3. Enable recurring scheduling

After verifying the workflow's result, keep the definition ONLINE and create
its schedule. This example uses a daily 02:00 Quartz cron; replace the dates
with your future scheduling window and choose the intended timezone:

```bash
dsctl schedule create --workflow daily-etl --project etl-prod \
  --cron "0 0 2 * * ?" --timezone Asia/Shanghai \
  --start "2027-01-01 00:00:00" --end "2027-12-31 23:59:59"
```

Use the returned schedule ID in place of `701`. For an existing schedule,
find its ID with `dsctl schedule list --project etl-prod --workflow daily-etl`.
Review the next trigger times before activation:

```bash
dsctl schedule preview 701 --project etl-prod
dsctl schedule online 701 --project etl-prod
```

Check the first scheduled execution with the same instance and task commands
above. For exact-version options, including the server-local timezone on DS
`1.3.9`, see the [schedule reference](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md#dsctl-schedule-create).

### 4. Pause or retire the workflow

Pause future scheduled runs while retaining the definition:

```bash
dsctl schedule offline 701 --project etl-prod
```

For retirement, take the workflow OFFLINE to prevent further scheduled runs.
Let unfinished executions finish and retain any definitions or logs you need
before permanently deleting it:

```bash
dsctl workflow offline daily-etl --project etl-prod
dsctl workflow-instance list --project etl-prod --workflow daily-etl
# Wait for unfinished executions before deleting.
dsctl workflow delete daily-etl --project etl-prod --force
```

Review the returned deletion result. If other workflows still reference this
one, resolve those dependencies before retrying its deletion.
[Runtime operations](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/runtime.md)
covers backfills, execution controls and environment selection.

## Explore Commands

Use `dsctl --help` to find a command group, then choose the lookup that answers
your current question:

| Need | Command |
| --- | --- |
| Syntax and examples for a known action | `dsctl workflow edit --help` |
| Machine-readable inputs and constraints | `dsctl schema --command workflow.edit` |
| Availability on the selected DS version | `dsctl capabilities --action workflow.edit` |

Structured commands return JSON by default. Use `--format table` or `--format tsv`
for interactive views; workflow exports produce YAML:

```bash
dsctl --format table workflow list --project etl-prod
```

See the [CLI reference](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md)
for filtering, output formats and errors.

## Use with Coding Agents

The [dsctl skill](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/skills/dsctl/SKILL.md)
guides agents through command discovery, workflow changes, scheduling, diagnosis
and outcome verification. Install the complete
[`skills/dsctl` directory](https://github.com/sketchmind/dolphinscheduler-cli/tree/main/skills/dsctl),
including its `references/` files, so the agent can load guidance for the task
at hand.

Give your agent this installation request:

```text
Install the dsctl skill from https://github.com/sketchmind/dolphinscheduler-cli,
directory skills/dsctl, into your supported skills location. Use the repository
ref matching my installed CLI: the release tag for a release, or the checkout
revision for a development install. Preserve the complete directory and any
existing local customizations, then verify that the skill is discoverable.
```

The skill uses the `dsctl` executable and connection configured above. Once
installed, ask the agent to use it for a concrete task, for example:

> Use the dsctl skill to investigate the latest failed run of daily-etl in
> project etl-prod and summarize the relevant task logs.

## Documentation and Contributing

Browse the [documentation map](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/README.md)
for user guides, references and developer documentation.
[Contributing](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CONTRIBUTING.md)
covers development setup, checks and pull requests.

Report bugs and feature requests through
[GitHub Issues](https://github.com/sketchmind/dolphinscheduler-cli/issues).
For vulnerabilities, follow the
[security policy](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/SECURITY.md).

Licensed under [Apache 2.0](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/LICENSE).
