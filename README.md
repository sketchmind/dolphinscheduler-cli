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
dsctl workflow get daily-etl --project etl-prod
```

Use your own project and workflow names in place of `etl-prod` and `daily-etl`.
[Configuration](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/configuration.md)
explains saved connections, credential files and switching clusters.

## Author a Workflow

Discover task families and inspect their parameters for your selected DS version:

```bash
dsctl template task
dsctl task-type schema SQL
```

Generate a workflow template, edit it for your tasks, and preview the result:

```bash
dsctl template workflow --raw > workflow.yaml
# Edit workflow.yaml, keeping its OFFLINE release state for this example.
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
```

Lint validates YAML locally. Dry-run resolves references and previews the planned
changes. After reviewing the preview, save the OFFLINE workflow:

```bash
dsctl workflow create --file workflow.yaml --project etl-prod
```

For an existing workflow, export its current definition, edit the file, and
preview the update:

```bash
dsctl workflow export daily-etl --project etl-prod > workflow.yaml
# Edit workflow.yaml.
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml --dry-run
```

Follow [Workflow authoring](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/workflow-authoring.md)
for task parameters, applying edits and publication, or start from the
[task examples](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/task-examples.md).

## Run and Monitor

Start an existing **ONLINE** workflow and find its new execution:

```bash
dsctl workflow run daily-etl --project etl-prod
dsctl workflow-instance list --project etl-prod --workflow daily-etl
```

Once the new instance appears, use its ID in place of `901`:

```bash
dsctl workflow-instance digest 901 --project etl-prod
dsctl workflow-instance watch 901 --project etl-prod
```

To investigate a failed run, list its task instances:

```bash
dsctl task-instance list --project etl-prod --workflow-instance 901
```

Use the failed task instance's ID in place of `902` to read its log:

```bash
dsctl task-instance log 902 --raw
```

[Runtime operations](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/runtime.md)
covers schedules, backfills and execution controls.
[Operational investigation](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/user/operational-investigation.md)
explains task logs, failure diagnosis and recovery.

## Explore Commands

Use help for syntax, `schema` for machine-readable inputs, and `capabilities`
for support on the selected server version:

```bash
dsctl --help
dsctl workflow edit --help
dsctl schema --command workflow.edit
dsctl capabilities --action workflow.edit
```

Structured commands return JSON by default. Use `--format table` or `--format tsv`
for interactive views; workflow exports produce YAML:

```bash
dsctl --format table workflow list --project etl-prod
```

In scripts and CI jobs, use `workflow-instance watch --exit-status` to make the
process exit status reflect the execution outcome. For the instance selected
above:

```bash
dsctl workflow-instance watch 901 --project etl-prod --exit-status
```

See the
[CLI reference](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/docs/reference/cli-contract.md)
for filtering, output formats and errors. An optional
[agent skill](https://github.com/sketchmind/dolphinscheduler-cli/blob/main/skills/dsctl/SKILL.md)
provides guidance for coding agents using the same CLI.

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
