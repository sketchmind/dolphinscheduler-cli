# dolphinscheduler-cli

`dolphinscheduler-cli` is a generated-first, REST-only command-line interface
for Apache DolphinScheduler.

The CLI is built around DolphinScheduler-native REST contracts and exposes them
through stable `dsctl` commands for configuration, authoring, runtime
inspection, and operational repair workflows. It does not use Py4J,
PyDolphinScheduler, or the Python gateway.

This is an independent CLI project for Apache DolphinScheduler. It should not
be described as an official Apache DolphinScheduler distribution unless that
status changes explicitly.

## Install

From PyPI:

```bash
python -m pip install dolphinscheduler-cli
dsctl version
```

For isolated CLI usage:

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

## Quick Examples

```bash
dsctl project list
dsctl use project etl-prod
dsctl workflow list
dsctl workflow get daily-etl
dsctl workflow run daily-etl --project etl-prod
dsctl workflow-instance digest <workflow_instance_id>
dsctl task-instance list --workflow-instance <workflow_instance_id>
dsctl task-instance list --project etl-prod --state FAILURE
dsctl task-instance log <task_instance_id>
```

## Command Surface

Stable user-facing commands today:

- `dsctl version`
- `dsctl context`
- `dsctl doctor`
- `dsctl schema`
- `dsctl capabilities`
- `dsctl enum list ENUM`
- `dsctl lint workflow FILE`
- `dsctl task-type list`
- `dsctl env list|get|create|update|delete`
- `dsctl cluster list|get|create|update|delete`
- `dsctl datasource list|get|create|update|delete|test`
- `dsctl namespace list|get|available|create|delete`
- `dsctl resource list|view|upload|create|mkdir|download|delete`
- `dsctl queue list|get|create|update|delete`
- `dsctl worker-group list|get|create|update|delete`
- `dsctl task-group list|get|create|update|close|start`
- `dsctl task-group queue list|force-start|set-priority`
- `dsctl alert-plugin list|get|schema|create|update|delete|test`
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
- `dsctl template workflow|params|task`
- `dsctl workflow list|get|describe|digest|create|edit|online|offline|run|run-task|backfill|delete`
- `dsctl workflow lineage list|get|dependent-tasks`
- `dsctl workflow-instance list|get|parent|digest|update|watch|stop|rerun|recover-failed|execute-task`
- `dsctl task list|get|update`
- `dsctl task-instance list|get|watch|sub-workflow|log|force-success|savepoint|stop`

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
