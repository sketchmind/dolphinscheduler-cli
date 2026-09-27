# Configuration

`dsctl` selects one complete connection source per invocation. Use a named
context for repeated work, an env file for a temporary target, or process
`DS_*` variables for shell-driven configuration.

## Set Up a Named Context

Keep connection settings in a caller-managed dotenv file, for example
`production.env`:

```dotenv
DS_API_URL=https://dolphinscheduler.example.com/dolphinscheduler
DS_API_TOKEN=...
DS_VERSION=3.4.1
```

Register it and choose a default:

```bash
dsctl context create production --file production.env --project etl-prod
dsctl config set default-context production
dsctl context
dsctl workflow list
dsctl workflow get daily-etl
```

Registration saves the absolute file reference, normalized API URL and optional
project in `$XDG_CONFIG_HOME/dsctl/config.yaml`, or
`~/.config/dsctl/config.yaml`. Credentials stay in the env file. Creation checks
that the file contains a valid URL and token; it does not contact the server or
verify the project. Use `dsctl doctor` for remote diagnostics.

`doctor` checks connection settings, the selected version, API health where
available, and authenticated current-user access. It does not validate the
selected project's permissions or execute a workflow. A successful report
does not prove master/worker connectivity or task readiness; inspect the
relevant resource and execution results for those checks.

The default applies in fresh shells and across directories when no higher
priority source is present. `config set` reports the saved value and the
effective selection separately: saving a default may leave an environment
selection effective for the current shell.

## Select a Target for One Command or Task

```bash
dsctl --context production workflow list
dsctl --env-file staging.env workflow list --project etl-staging
DSCTL_CONTEXT=production dsctl context
DSCTL_ENV_FILE=/absolute/path/staging.env dsctl context
```

`--context` and `--env-file` are mutually exclusive. An explicit selector
supersedes process selectors. Without one, `DSCTL_CONTEXT` and `DSCTL_ENV_FILE`
are mutually exclusive. They select a source; they do not change the saved
default. For independent tasks sharing a directory, supply a different
`DSCTL_CONTEXT` to every child process of each task. Use an absolute path when
supplying `DSCTL_ENV_FILE` across directories.

The first applicable source wins:

1. Explicit `--context NAME` or `--env-file PATH`.
2. Process `DSCTL_CONTEXT` or `DSCTL_ENV_FILE`.
3. Process connection settings, when `DS_API_URL`, `DS_API_TOKEN` or
   `DS_VERSION` is present, including an empty value.
4. The saved `default-context`.
5. Unconfigured local operation.

Each connection source supplies its own URL, token and version. Missing
identity values never come from another source. Empty or incomplete environment
values therefore shadow a saved default; remote commands fail instead of
switching to that default. `DS_VERSION=3.4.1` alone supports offline
version-selected work but does not supply a remote connection. A missing
context, broken file or invalid selected source fails without trying another
target.

## Connection Settings

The identity keys are `DS_API_URL`, `DS_API_TOKEN` and `DS_VERSION`. Omitting
`DS_VERSION` requests automatic discovery. Selecting a file or named context
keeps its identity independent of inherited process identity values.

Execution policy resolves separately, per setting: process value, selected
profile value, then built-in default. Policy-only environment variables never
change the selected connection or hide its project:

| Setting | Default | Meaning |
| --- | --- | --- |
| `DS_API_RETRY_ATTEMPTS` | `3` | Maximum attempts for reviewed replay-safe requests |
| `DS_API_RETRY_BACKOFF_MS` | `200` | Initial retry backoff in milliseconds |
| `DS_API_TIMEOUT_SECONDS` | `10` | Positive finite request timeout in seconds |

Version probes share a 45-second overall budget and cap each request by the
remaining budget. Retrying still requires both a replay-safe operation and an
eligible transient failure. A 401 is never retried; mutation uncertainty still
requires authoritative readback before repeating the operation.

Direct environment setup remains supported:

```bash
export DS_API_URL="https://dolphinscheduler.example.com/dolphinscheduler"
export DS_API_TOKEN="..."
dsctl doctor
```

## Inspect and Maintain Contexts

```bash
dsctl context
dsctl context list
dsctl context get production
dsctl context update production --project analytics
dsctl context update production --clear-project
dsctl context update production --file replacement.env --project analytics
dsctl config get default-context
dsctl config unset default-context
dsctl context delete production
```

Bare `context` reports the effective source, requested version, URL and project
without network access. `context list` and `get` inspect saved entries even
when their referenced files are unavailable. All context and config operations
are local. `config` accepts only the key `default-context`.

A named context supplies an optional project default. Explicit `--project`
overwrites that selection for one command; commands with a project in an input
file retain their documented file-selection rules. Workflows always use an
explicit command or file selector. Temporary env-file and process `DS_*`
sources have no saved project default.

Token rotation in the referenced file takes effect on the next invocation and
retains the project. Changing the file's API URL externally invalidates its
binding. Rebind deliberately with `context update NAME --file FILE`; every
explicit file update clears the previous project unless `--project` supplies
its replacement. An in-progress invocation keeps its resolved snapshot.

Deleting the default context is rejected. First run
`config unset default-context`, then delete it. Registry updates are serialized
and atomic. They never modify the caller's credential file.

## Migration from Scoped Selection

`dsctl use` and ambient workflow defaults have been removed. The CLI never
searches the current directory or its parents for configuration and does not
read `.dsctl-context.yaml` or the old user `dsctl/context.yaml`. Register an
existing env file with `context create`, optionally set its project, and choose
`default-context` explicitly. Old files are left untouched.

## Version Selection

With a configured API URL and token, omitting `DS_VERSION` or setting it to
`auto` enables remote version discovery. An explicit exact version takes
precedence and avoids the probe; common forms such as `v3.4.1` and `ds_3_4_1`
are normalized.

Remote commands resolve the target before checking availability or preparing a
request. One invocation shares one fresh observation; `doctor` also probes
afresh in automatic mode. Discovery first checks reviewed product-version
metadata, then fixed, same-origin Swagger/OpenAPI documents when exact metadata
is unavailable. Matching document routes produce candidate versions; matching
operation contracts and a separate semantic review decide which reads can run.
A candidate list, including a single candidate, does not establish the server's
release or rule out a custom build.

The potential read scope is project/workflow list and get, schedule list and
get, workflow-instance list/get/digest, task-instance list/get/log, and the
current-user check in `doctor`. Admission depends on every operation needed by
the action, including reference-resolution reads. A mismatched operation can
disable one action while unrelated reads remain available. Writes, workflow
exports and authoring, templates, lint, and unreviewed reads require an exact
`DS_VERSION` when the target is not identified. Configure that value from the
deployment's actual release, not from the candidate list.

Compatibility execution reports `ds_version: null` under `resolved.target`,
retains `candidate_versions` and any independent `reported_version`, and emits
a `read_compatibility` warning. The internal execution profile does not become
a public selected server version. Run `dsctl capabilities --action project.list`
to inspect cached admission or `dsctl doctor` to refresh it.

Local discovery stays offline. `schema` and `capabilities` remain useful on a
cold or expired cache: they describe installed invocation syntax and mark exact
DS semantics unknown. Cached candidates add read-admission facts without
selecting task models or YAML templates. `enum names` and `enum list` expose
only enums whose full members, values and attributes agree across all cached
candidates. Templates and lint require an explicit version or a fresh cached
exact observation. Help, shell completion and `context` never probe; `context`
reports the requested version (`auto` or the explicit value). Completely
untargeted local authoring retains the `3.4.1` baseline and `version` labels its
source `offline_default`.

The five-minute cache lives under `$XDG_CACHE_HOME/dsctl/version-discovery`, or
`~/.cache/dsctl/version-discovery`. It is scoped to the normalized API URL,
including its context path, and a credential hash; tokens are not stored.
Candidate entries also bind the generated discovery-contract digest. Every new
remote invocation probes again. A successful unresolved observation replaces
an older positive result; probe or network failures clear the old entry.
Neither failure nor expiry falls back to stale evidence or another profile.

Reviewed exact metadata identifies eight releases: `3.2.0` and `3.2.1` through
OpenAPI, and `3.3.1`, `3.3.2`, `3.4.0`, `3.4.1`, `3.4.2` and `3.4.3` through
product information. These fields rely on a completed installation or upgrade.
Stock `3.2.2` records `3.3.0`; the CLI preserves that reported value separately
and never rewrites it to exact `3.2.2`. Contract discovery may still admit reviewed
reads for this anomaly. Other unreviewed release reports and contradictory
metadata do not silently activate a different profile. Earlier releases can use
the same read path when their API documents are available. Hidden or disabled
documentation requires an explicit version for remote operations. API group
labels such as `V1` and `V2` never identify a DolphinScheduler release.

See [Version Compatibility](version-compatibility.md) for the current support
matrix.
