# Installation

## Supported Python

`dolphinscheduler-cli` currently requires Python 3.11 or newer.

The published package is tested on Python 3.11, 3.12, and 3.13.

## Install From PyPI

Install the latest released package from PyPI:

```bash
python -m pip install dolphinscheduler-cli
dsctl version
```

Package page: <https://pypi.org/project/dolphinscheduler-cli/>

Upgrade an existing install:

```bash
python -m pip install --upgrade dolphinscheduler-cli
dsctl version
```

For isolated CLI usage, `pipx` is usually cleaner than installing into a shared
Python environment:

```bash
pipx install dolphinscheduler-cli
dsctl version
```

Upgrade a `pipx` install:

```bash
pipx upgrade dolphinscheduler-cli
```

## Shell Completion

Choose your shell explicitly and preview its completion script:

```bash
dsctl --show-completion zsh
```

To install it into your shell configuration, run:

```bash
dsctl --install-completion zsh
```

Restart your shell after installation. Supported shell names are `bash`, `zsh`,
`fish`, `powershell` and `pwsh`. Both options require a shell name and work
without parent-shell detection or a DolphinScheduler connection.

Use `-h` or `--help` at any command level. `dsctl -v` and `dsctl --version` print
only the installed CLI version as plain text, independently of connection
configuration and display options. The `version` subcommand returns the detailed
CLI and selected DS profile report and supports `--format`.

## Install From Source

Use source installs for local development or unreleased changes.

From an existing source checkout:

```bash
cd dolphinscheduler-cli
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
dsctl version
```

An editable install follows source changes, but it does not automatically
refresh dependencies or installed package metadata when `pyproject.toml`
changes. After updating the checkout, activate the same virtual environment
and refresh the install:

```bash
python -m pip install -e '.[dev]'
python -m pip check
command -v dsctl
dsctl --version
dsctl --help
```

The `dsctl` path should belong to that environment. An older editable install
in another Python environment can load the current source with outdated
dependencies even when the project's own virtual environment works.

## Configure A Cluster

`dsctl version` does not require a live DolphinScheduler connection. Commands
that talk to DolphinScheduler need a DS API URL and token:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
dsctl doctor
```

You can also use a dotenv-style file:

```bash
cat > dsctl.env <<'EOF'
DS_API_URL=https://dolphinscheduler.example.com/dolphinscheduler
DS_API_TOKEN=...
EOF

dsctl --env-file dsctl.env doctor
```

Set `DS_VERSION` to the exact server version when automatic discovery is
unavailable. See [Configuration](configuration.md) for the full profile format.

## Verify The Install

`dsctl version` does not require a live DolphinScheduler connection. It reports
the CLI version, selected DolphinScheduler version, adapter family, generated
contract version, and selectable server versions.

`dsctl doctor` performs local profile checks and remote health checks. It
requires a configured DolphinScheduler API URL and token.
