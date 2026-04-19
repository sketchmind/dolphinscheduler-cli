# Workflow Authoring

This guide describes the stable YAML authoring surface used by
`dsctl workflow create`, `dsctl workflow edit`, `dsctl lint workflow`, and
`dsctl template`.

Use the command output as the first source of truth when generating YAML:

```bash
dsctl capabilities
dsctl schema
dsctl task-type list
dsctl template task
dsctl task-type get SQL
dsctl task-type schema SQL
dsctl template params
dsctl template task SHELL --variant resource --raw
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --dry-run
```

## Discovery Flow

Use `dsctl task-type list` when you need the live DS task-type catalog for the
configured cluster and current user. Use `dsctl template task` when you need
the compact local template catalog. Then use `dsctl task-type get TYPE` for the
per-type authoring summary and `dsctl task-type schema TYPE` for full fields,
state rules, choices, discovery commands, related commands, and compile
mappings.

`dsctl template workflow` returns a minimal full workflow inside the standard
JSON envelope. Use `dsctl template workflow --raw > workflow.yaml` when you want
only the YAML file content. The template is intentionally small so generated
files do not include unrelated optional fields.

`dsctl template params` returns a compact parameter-topic index. Expand only the
topic needed for the current authoring task:

```bash
dsctl template params --topic property
dsctl template params --topic built-in
dsctl template params --topic time
dsctl template params --topic context
dsctl template params --topic output
```

`dsctl template task` returns compact machine-readable task-template rows:

- `rows[].task_type`
- `rows[].kind`
- `rows[].category`
- `rows[].default_variant`
- `rows[].variants`
- `rows[].next_command`

`dsctl template task TYPE --variant VARIANT` returns a task-level YAML fragment
for one concrete authoring scenario. Add `--raw` when you want only the YAML
fragment without the JSON envelope. Copy the fragment under `tasks:` in a
workflow YAML file, then run `dsctl lint workflow` and `workflow create
--dry-run`.

## Payload Modes

Script-like tasks can use either a CLI shorthand or the DS-native task params
shape:

```yaml
name: inline-shell
type: SHELL
command: |
  echo "inline command"
```

The `command` shorthand is only accepted for `SHELL`, `PYTHON`, and
`REMOTESHELL`. It compiles to DS `taskParams.rawScript`.

Use `task_params` when a task needs DS-native plugin fields:

```yaml
name: shell-resource-task
type: SHELL
task_params:
  rawScript: |
    bash scripts/job.sh
  resourceList:
    - resourceName: /tenant/resources/scripts/job.sh
  localParams: []
  varPool: []
```

For `SHELL` and `PYTHON`, attached DS resources use
`task_params.resourceList[].resourceName`. Upload or inspect resources with:

```bash
dsctl resource upload --file job.sh --dir /tenant/resources/scripts
dsctl resource list --dir /tenant/resources/scripts
```

## Dynamic Parameters

Workflow-level parameters are written under `workflow.global_params`:

```yaml
workflow:
  name: parameterized-workflow
  global_params:
    bizdate: "${system.biz.date}"
```

Task-level parameters use the DS `Property` shape under
`task_params.localParams`:

```yaml
name: shell-params-task
type: SHELL
task_params:
  rawScript: |
    echo "bizdate=${bizdate}"
    echo '${setValue(row_count=42)}'
  localParams:
    - prop: bizdate
      direct: IN
      type: VARCHAR
      value: ${system.biz.date}
    - prop: row_count
      direct: OUT
      type: INTEGER
      value: "0"
  resourceList: []
  varPool: []
```

`direct: IN` declares input values used by `${name}` placeholders. `direct: OUT`
declares values that can be published into the downstream var pool. Script-like
tasks publish output values by writing `${setValue(name=value)}` or
`#{setValue(name=value)}` to task logs. SQL tasks can publish output values from
result columns whose names match OUT parameter `prop` values.

DS also supports runtime time placeholders using `$[...]`, such as
`$[yyyyMMdd-1]`, `$[add_months(yyyyMMdd,-1)]`, and
`$[month_first_day(yyyy-MM-dd,-1)]`. The CLI preserves these expressions as
strings; DS evaluates them at runtime. Run `dsctl template params --topic time`
for the focused syntax list.

Inside `$[...]`, DS uses Java-style date patterns. Lowercase `yyyy` is calendar
year; uppercase `YYYY` is week-based year. `dsctl lint workflow` and
`workflow create/edit` warn on risky expressions such as `$[YYYYMMdd]` or
`$[yyyyww]` so callers can choose calendar-year, week-year, or
`year_week(...)` deliberately.

For SQL tasks, `sqlType: 0` means query SQL that returns rows; `sqlType: 1`
means non-query SQL such as DDL or DML. Run `dsctl task-type schema SQL` to see
the `sqlType` state rules. SQL task payloads keep `localParams`, `varPool`,
`preStatements`, and `postStatements` as lists so copied templates stay
compatible with DS SQL task execution.

Use task template parameter variants for concrete examples:

```bash
dsctl template params
dsctl template params --topic time
dsctl template task SHELL --variant params
dsctl template task PYTHON --variant params
dsctl template task SQL --variant params
dsctl template task HTTP --variant params
dsctl template task SWITCH --variant params
dsctl task-type schema SQL
```

## Typed Task Templates

Current typed task templates cover:

- `SHELL`
- `PYTHON`
- `REMOTESHELL`
- `SQL`
- `HTTP`
- `SUB_WORKFLOW`
- `DEPENDENT`
- `SWITCH`
- `CONDITIONS`

Useful variants:

```bash
dsctl template task SHELL --variant resource
dsctl template task SHELL --variant params
dsctl template task PYTHON --variant resource
dsctl template task PYTHON --variant params
dsctl template task SQL --variant pre-post-statements
dsctl template task SQL --variant params
dsctl template task HTTP --variant post-json
dsctl template task HTTP --variant params
dsctl template task SWITCH --variant branching
dsctl template task SWITCH --variant params
dsctl template task CONDITIONS --variant condition-routing
dsctl template task CONDITIONS --variant params
dsctl template task DEPENDENT --variant workflow-dependency
dsctl template task DEPENDENT --variant params
dsctl template task SUB_WORKFLOW --variant child-workflow
dsctl template task SUB_WORKFLOW --variant params
dsctl template task REMOTESHELL --variant datasource
dsctl template task REMOTESHELL --variant params
```

Unknown or not-yet-typed DS task types still accept raw `task_params: {}`
templates so exported workflows can round-trip while typed coverage grows.

## Validation

Always validate generated YAML locally before sending it to DolphinScheduler:

```bash
dsctl lint workflow workflow.yaml
dsctl workflow create --file workflow.yaml --dry-run
```

`lint workflow` checks the stable YAML model and local compile path.
`--dry-run` shows the DS legacy request plan, schedule preview when present,
and any risk confirmation token required before mutation.
