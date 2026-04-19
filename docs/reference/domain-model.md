# DolphinScheduler Domain Model Reference

This document is a grounding note, not the CLI contract.

Use it when a design question depends on DolphinScheduler semantics rather than
on the current `dsctl` implementation.

## Grounding

These notes were grounded in a local checkout of upstream Apache
DolphinScheduler source. During development that checkout is usually mounted at
`references/dolphinscheduler`, but `references/` is ignored, not packaged, and
not required for installed CLI usage.

Representative upstream paths:

- `references/dolphinscheduler/docs/docs/en/guide/project/workflow-definition.md`
- `references/dolphinscheduler/docs/docs/en/architecture/design.md`
- `references/dolphinscheduler/dolphinscheduler-dao/src/main/java/...`

## Terminology Map

| `dsctl` term | DS term | Meaning |
| --- | --- | --- |
| `project` | project | collaboration and authorization boundary |
| `workflow` | workflow definition / process definition | design-time DAG definition |
| `task` | task definition | design-time node definition |
| `schedule` | schedule / timing | periodic trigger configuration |
| `workflow-instance` | process instance / workflow instance | one execution of a workflow |
| `task-instance` | task instance | one execution of a task node |

When reading upstream code and API shapes, expect `process definition`,
`process instance`, and `task instance`.

## Object Planes

The most useful mental split is:

| Plane | Core objects | Why it matters |
| --- | --- | --- |
| Governance | user, tenant, queue, worker group, environment, datasource, resource, alert group | execution context, authorization, and cluster-level diagnostics |
| Project | project | default selection boundary and ownership boundary |
| Design | workflow, task, relation, schedule | what should run |
| Runtime | command, workflow-instance, task-instance, logs, health | what actually ran |

This separation matters because DS failures are often governance or runtime
problems, not design problems.

## Core Relationships

```text
project
  └── workflow
        ├── task[*]
        ├── relation[*]
        └── schedule[0..1]

workflow trigger
  └── command
        └── workflow-instance
              └── task-instance[*]

workflow/task runtime resolution
  └── tenant / queue / worker group / environment / datasource / resource / alert group
```

Important consequences:

- `workflow` and `schedule` are separate resources.
- `workflow` and `workflow-instance` are separate resources.
- `task` and `task-instance` are separate resources.
- Diagnostics often need both design data and resolved runtime context.

## Project Preference Facts

Grounded in upstream project-preference controller/service and UI forms:

- DS 3.4.1 stores project preference as one project-scoped singleton row with
  a JSON `preferences` string and integer `state`.
- `state=1` means enabled and `state=0` means disabled.
- DS backend stores, queries, and toggles that row, but it does not
  automatically rewrite workflow definitions, task definitions, or schedules
  from it.
- Official DS clients mainly consume project preference as a default-value
  source when building UI forms such as workflow start, timing, and new-task
  dialogs.

For `dsctl`, this means project preference should be treated as an optional
client-side default source, not as server-side inheritance.

## Task Graph Facts

Grounded in upstream task structure docs:

- Plain predecessor edges and logical task nodes are different concepts.
- A basic DAG edge can be represented as "task B depends on task A".
- DS also has task types whose graph behavior is part of the task definition
  itself, not just the edge list.

Examples:

- `DEPENDENT` carries a `dependence` tree and can express cross-workflow or
  cross-task checks.
- `CONDITIONS` carries `conditionResult.successNode` and
  `conditionResult.failedNode`.
- `SWITCH` stores branch expressions in task params and chooses downstream
  branches at runtime.

For `dsctl`, this means a simple `depends_on` list is only a shorthand for
plain predecessor edges. It should not be stretched into a generic language
for every DS graph feature.

## Task Type Facts

Grounded in upstream `task-type-config.yaml`, task-plugin factories, and UI
task registries:

- DS 3.4.1 has a broader default task-type universe than the current stable
  typed task-spec subset.
- The stable CLI now emits templates for every DS default task type, but only a
  curated subset currently has typed `task_params` models; the remainder use
  generic raw `task_params` placeholders.
- DS upstream groups default task types under these categories:
  - `Universal`
  - `Cloud`
  - `Logic`
  - `DataIntegration`
  - `MachineLearning`
  - `Other`
- Task type names should stay DS-native. For example, upstream uses
  `REMOTESHELL`, not `REMOTE_SHELL`.

For `dsctl`, this means capability discovery should distinguish:

- types that DS 3.4.1 exposes by default
- types that the CLI supports with stable typed templates
- types that the CLI supports only through generic raw templates
- the live DS task-type list returned by `GET /favourite/taskTypes`, which also
  carries the current user's `isCollection` favourite flag

## Workflow Lifecycle Facts

Grounded in upstream workflow docs:

- Only offline workflows can be edited.
- Only online workflows can run.
- Schedules can only be created against online workflows.
- A newly created schedule starts offline and must be brought online
  separately to take effect.

These are DS facts, not CLI inventions, so workflow, schedule, and runtime
verbs must stay separated in future command design.

## Schedule Lifecycle Facts

Grounded in upstream workflow docs, schedule services, and Quartz task logic:

- In DS 3.4.1, one workflow definition has at most one persisted schedule.
- A schedule is not just a cron string. It stores:
  - time window (`startTime`, `endTime`)
  - timezone (`timezoneId`)
  - cron (`crontab`)
  - execution defaults (`failureStrategy`, `warningType`,
    `warningGroupId`, `workflowInstancePriority`, `workerGroup`,
    `tenantCode`, `environmentCode`)
  - lifecycle state (`releaseState`)
- Creating a schedule persists the trigger definition, but it does not make the
  schedule effective until the schedule is brought online.
- Bringing a schedule online requires the bound workflow definition to already
  be online.
- Bringing a schedule online creates or updates the Quartz job for that
  schedule.
- Bringing a schedule offline deletes the Quartz job, but does not delete the
  workflow definition.
- Bringing a workflow offline also forces its schedule offline.
- At trigger time, Quartz re-checks both schedule state and workflow state. If
  either is invalid, the Quartz job is deleted and the schedule may be forced
  back offline.

For CLI design, this means `schedule` should stay a top-level resource even
though it is always bound to a workflow definition.

## Runtime Lifecycle Facts

Grounded in upstream architecture docs:

- A workflow run is represented internally through the command table and then
  becomes a process instance.
- A task instance is created when a specific task node executes inside a run.
- Runtime state, log retrieval, retry, and failover semantics belong to
  `workflow-instance` and `task-instance`, not to `workflow` and `task`.

For `dsctl`, this means future runtime commands should model:

- `workflow run` as a trigger operation
- `workflow-instance *` as workflow-instance inspection/control
- `task-instance *` as task-instance inspection/log access

The backend also distinguishes between:

- creating a new workflow instance from a workflow definition
- executing selected tasks inside an existing workflow instance

That is why future CLI runtime actions should attach:

- new execution to `workflow run`
- control and retry actions to `workflow-instance`
- observation and log retrieval to `task-instance`

## Naming and Identity Rules

Names in DolphinScheduler are business data. They should be treated as opaque
strings.

Practical rules for `dsctl`:

- never infer structure from delimiters inside names
- do not bake `split(':')`, `split('/')`, or similar parsing into business code
- prefer name-first resolution at the CLI boundary
- treat DS codes and ids as the stable server-side identifiers underneath

The current CLI does not expose handles yet. If handles or richer selector
forms are introduced later, they should complement opaque names, not replace
the rule that names themselves are not path syntax.

## Why This Reference Exists

Keep this document factual and small. The authoritative product docs remain:

- `README.md`
- `docs/development/architecture.md`
- `docs/reference/cli-contract.md`
- `docs/development/roadmap.md`

When those docs need domain grounding, link back here instead of duplicating
large concept essays in multiple places.
