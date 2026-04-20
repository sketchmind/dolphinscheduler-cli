# dolphinscheduler-cli

`dolphinscheduler-cli` provides `dsctl`, a command-line interface for Apache
DolphinScheduler.

Use it for configuration, workflow authoring, runtime inspection, and
operational recovery through DolphinScheduler REST APIs.

This is an independent CLI project for Apache DolphinScheduler.

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

For local development, install from a source checkout:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev]
dsctl version
```

## Configure

Set the target DolphinScheduler API URL and token with environment variables:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
export DS_VERSION="3.4.1"
dsctl doctor
```

`DS_VERSION` defaults to `3.4.1`. It can currently select `3.4.1`, `3.4.0`,
or `3.3.2`; those versions share the generated `3.4.1` contract adapter until
an upstream REST difference requires a separate adapter.

You can also load connection settings from a dotenv-style file:

```bash
dsctl --env-file cluster.env context
```

## Quick Start

```bash
dsctl doctor
dsctl project list
dsctl use project etl-prod
dsctl workflow list
dsctl workflow run daily-etl
dsctl workflow-instance watch <workflow_instance_id>
dsctl workflow-instance digest <workflow_instance_id>
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance log <task_instance_id> --raw
```

## Discover Commands

Start with `--help` for human-readable command entry points:

```bash
dsctl --help
dsctl workflow --help
dsctl workflow edit --help
```

Use `schema` for machine-readable arguments, options, choices, payload hints,
and output shape metadata:

```bash
dsctl schema --list-groups
dsctl schema --list-commands
dsctl schema --command workflow.edit
dsctl schema --command task-type.schema
```

Use `capabilities` for lightweight feature discovery:

```bash
dsctl capabilities --summary
dsctl capabilities --section authoring
```

## Workflow Authoring

Create workflow YAML from templates, lint it locally, then dry-run before
sending it to DolphinScheduler:

```bash
dsctl template task SHELL --raw
dsctl task-type schema SQL
dsctl template workflow --raw > workflow.yaml
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --project etl-prod --dry-run
dsctl workflow create --file workflow.yaml --project etl-prod
```

Export an existing workflow, edit the YAML, and apply the full edited document:

```bash
dsctl workflow export daily-etl --project etl-prod > workflow.yaml
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml --dry-run
dsctl workflow edit daily-etl --project etl-prod --file workflow.yaml
```

For small changes, start from a patch template:

```bash
dsctl template workflow-patch --raw > patch.yaml
dsctl workflow edit daily-etl --project etl-prod --patch patch.yaml --dry-run
```

## Runtime Operations

```bash
dsctl workflow run daily-etl --project etl-prod
dsctl workflow run-task daily-etl --project etl-prod --task load
dsctl workflow-instance list --project etl-prod
dsctl workflow-instance digest <workflow_instance_id>
dsctl workflow-instance watch <workflow_instance_id>
dsctl workflow-instance recover-failed <workflow_instance_id>
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance log <task_instance_id> --raw
```

Export a workflow instance before editing runtime task definitions:

```bash
dsctl workflow-instance export <workflow_instance_id> > instance.yaml
dsctl workflow-instance edit <workflow_instance_id> --file instance.yaml --dry-run
```

## Output

Commands return a stable JSON envelope by default. Use global output options
before the command group when a table or pipeline-oriented view is more useful:

```bash
dsctl --output-format table workflow-instance list --project etl-prod
dsctl --output-format tsv --columns id,name,state task-instance list --workflow-instance <workflow_instance_id>
dsctl --columns id,name,state workflow-instance list --project etl-prod
dsctl --output-format tsv --columns '*' task-instance list --workflow-instance <workflow_instance_id>
```

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

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev]
python tools/check_quality_gate.py
```

Generate the tracked DS contract package after changing the generator:

```bash
python tools/generate_ds_contract.py --package-output build/ds_contract/package_sample
python tools/check_generated_freshness.py
```

For destructive cluster-backed coverage, export the live-test environment
variables and run:

```bash
python tools/check_quality_gate.py --include-live
```

## Packaging

Build and inspect local distributions before publishing:

```bash
python -m build
python -m twine check dist/*
python tools/check_package_contents.py dist/*
python -m pip install dist/dolphinscheduler_cli-*.whl
dsctl version
```

Use TestPyPI before the first public PyPI release. See
[docs/development/release.md](docs/development/release.md).
