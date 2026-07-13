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
dsctl use workflow daily-etl --project etl-prod
dsctl context
```

Explicit command flags take precedence over saved context. The effective
selection rule is `flag > context`.

`dsctl context` is a local read: it reports the effective project/workflow
tuple, the scope that supplied it, and the stored `set_at` timestamp without
contacting DolphinScheduler. `dsctl use` also changes local state only; supplied
names are not remotely validated. Use `dsctl doctor` for connection,
credentials, version adaptation, and local context-file health. Verify a saved
project or workflow with the corresponding `get` or `list` command.

Project and workflow context are stored as one scoped selection. Set project
context before workflow context, or bind both atomically with `dsctl use
workflow NAME --project PROJECT`. Without `--project`, workflow updates in user
scope use the project stored in that same user layer instead of copying a
shadowing project-layer value. A workflow saved for one project is never reused
when a command explicitly selects another project.

User-scoped context is the base layer and the current directory's
project-scoped context has higher precedence. Updating user scope while a
project-scoped selection exists does not change the effective selection until
that project-scoped selection is cleared; successful output marks the update as
shadowed only when the updated user layer still contains a project selection.
Clearing the user layer leaves nothing to shadow, while `effective_scope` still
reports that project context supplied the result. Context-file replacement is
atomic. In context YAML, `null` means the key is absent and may fall back to the
lower layer; it is not a persistent tombstone.

Place target options after the subcommand, for example `dsctl use project NAME
--scope user`. Group-level `dsctl use --clear --scope SCOPE` remains the form
for clearing a whole layer. A name cannot be combined with a target's
`--clear`, and `dsctl use workflow --clear` cannot be combined with
`--project`.

### Migrating workflow-only context from v0.2

Older v0.2 context files could contain `workflow` without a project binding.
Current versions reject that ambiguous state with `config_error` instead of
guessing which project owns the workflow. Bind a project and deliberately
discard the unbound workflow with the command for the affected scope:

```bash
dsctl use project PROJECT --scope project
dsctl use project PROJECT --scope user
```

Then set workflow context again. To discard the affected layer instead, run
`dsctl use --clear --scope project` or `dsctl use --clear --scope user`.
If both layers contain workflow-only state, repair or clear `project` scope
first, then repair or clear `user` scope.

## Version Selection

`DS_VERSION` selects the target DolphinScheduler server version. It defaults to
`3.4.1` and accepts common normalized forms such as `v3.4.1` and `ds_3_4_1`.

See [Version Compatibility](version-compatibility.md) for the current support
matrix.
