# Installation

## Supported Python

`dolphinscheduler-cli` currently requires Python 3.11 or newer.

The first public release should keep the advertised support range aligned with
CI. Do not widen `requires-python` until the test matrix covers the added
Python versions.

## Install From Source

From an existing source checkout:

```bash
cd dolphinscheduler-cli
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .[dev]
dsctl version
```

## Install From PyPI

After the package is published:

```bash
python -m pip install dolphinscheduler-cli
dsctl version
```

For isolated CLI usage, `pipx` is usually a cleaner installation method:

```bash
pipx install dolphinscheduler-cli
dsctl version
```

## Verify The Install

`dsctl version` does not require a live DolphinScheduler connection. It reports
the CLI version, selected DolphinScheduler version, adapter family, generated
contract version, and supported server versions.

`dsctl doctor` performs local profile checks and remote health checks. It
requires a configured DolphinScheduler API URL and token.
