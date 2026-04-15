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

## Install From Source

Use source installs for local development or unreleased changes.

From an existing source checkout:

```bash
cd dolphinscheduler-cli
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev]
dsctl version
```

## Configure A Cluster

`dsctl version` does not require a live DolphinScheduler connection. Commands
that talk to DolphinScheduler need a DS API URL and token:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
export DS_VERSION="3.4.1"
dsctl doctor
```

You can also use a dotenv-style file:

```bash
cat > dsctl.env <<'EOF'
DS_API_URL=https://dolphinscheduler.example.com/dolphinscheduler
DS_API_TOKEN=...
DS_VERSION=3.4.1
EOF

dsctl --env-file dsctl.env doctor
```

See [Configuration](configuration.md) for the full profile format.

## Verify The Install

`dsctl version` does not require a live DolphinScheduler connection. It reports
the CLI version, selected DolphinScheduler version, adapter family, generated
contract version, and supported server versions.

`dsctl doctor` performs local profile checks and remote health checks. It
requires a configured DolphinScheduler API URL and token.
