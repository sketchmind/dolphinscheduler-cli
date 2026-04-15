# Configuration

`dsctl` reads DolphinScheduler connection settings from process environment
variables and optional dotenv-style env files.

## Connection Settings

Supported profile keys:

- `DS_VERSION`
- `DS_API_URL`
- `DS_API_TOKEN`
- `DS_API_RETRY_ATTEMPTS`
- `DS_API_RETRY_BACKOFF_MS`

Example:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
export DS_VERSION="3.4.1"
dsctl doctor
```

## Env Files

Pass an env file with the global `--env-file` option:

```bash
dsctl --env-file cluster.env context
```

Values loaded from the process environment override values loaded from the env
file. This lets CI or shell-local credentials take precedence over a shared
profile file.

## Selection Context

Project and workflow selection belongs to local CLI context, not profile
configuration:

```bash
dsctl use project etl-prod
dsctl use workflow daily-etl
dsctl context
```

Explicit command flags take precedence over saved context. The effective
selection rule is `flag > context`.

## Version Selection

`DS_VERSION` selects the target DolphinScheduler server version. It defaults to
`3.4.1` and accepts common normalized forms such as `v3.4.1` and `ds_3_4_1`.

See [Version Compatibility](version-compatibility.md) for the current support
matrix.

