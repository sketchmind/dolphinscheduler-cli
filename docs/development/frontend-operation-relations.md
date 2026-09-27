# Frontend operation relations and CLI design

Identity propagation, state conditions, side effects and result observation in
frontend operations provide review cues for CLI design. The
[CLI contract](../reference/cli-contract.md) owns public behavior, and
[Architecture](architecture.md) owns implementation responsibilities. New DS
actions still require exact-version source evidence and release gates; the
presence of a menu, button or enum does not authorize support.

## Relations and responsibilities

| Relation | Frontend example | Appropriate CLI responsibility |
| --- | --- | --- |
| Identity and object relations | Open an instance list from a definition; open a task's containing instance; open a child instance from its parent | Return actual identifiers and scope; generate `next_actions` when sufficient facts are available |
| Input discovery | Select workers, environments and tenants for schedules; select clusters for namespaces | Existing field schema, `discovery_command`/`discovery_command_pattern`, and template guidance |
| State preconditions | Edit after taking a definition offline; execute a node after a terminal state; recover after failure | Service validation and exact recipes; schema describes preconditions, while navigation provides conservative hints |
| Result observation | Observe a new execution round after submitting a control request; observe actual execution after forcing a queue entry | Acceptance receipts, resolved identities, baselines for new execution rounds, `watch`/`digest`/logs |
| Side effects and scope | Taking a definition offline also takes its schedule offline; cascading deletion; replacing an entire grant set | Command summaries, schema, previews, and effect facts in structured results/errors |
| Different goals | Saving a draft, manual validation, scheduled production use and recovery from failure serve different purposes | A small set of explained related commands and scenario documentation/skills as needed, without requiring a linear sequence |

Button `loading` and `disabled` values, countdowns, selected rows and route changes
also contain transient UI state. They cannot directly determine business
eligibility. A clickable button does not prove that permissions, worker
prerequisites or all backend constraints are satisfied.

## Identity, navigation and observation

Navigation uses only identities, scope and state already returned, with no
additional REST requests. Read/write properties come from catalog effects;
services retain responsibility for complete validation and authorization.
Schema provides static command and input discovery, dynamic action_index groups
known row facts, and next_actions offers a small set of follow-up commands with
complete identities. Do not generate executable commands when identities or
target scope are missing.

`targets: "all"` covers only every row returned in this response. Every row must
have a valid, unique identity and be included in the index. Partial coverage
caused by truncation, duplicate identities or invalid identities uses explicit
IDs. Field projection and JSON encoding do not change the logical index scope,
and index coverage does not imply complete remote pagination.

The target ID in a parent/child instance relation does not establish the target
project. Do not copy the source project's `resolved.project` as the child
instance's scope. Retain the relation read and generate navigation only when the
target project has supporting evidence. Objects with the same name, large integer
IDs, legacy string IDs and modern codes retain their exact identity boundaries.

Definitions and schedules have separate states. Taking a definition offline may
also take its schedule offline; explicitly check the schedule when restoring
production use. Saving a draft, running manually and configuring a schedule are
different goals; going online does not require immediate execution.
`accepted: true` in control results such as queue force-start proves only request
acceptance. Actual execution and completion require evidence from instances,
execution-round baselines, watch, digest or logs.

JSON and compact JSON share the same information contract. Compact changes
encoding only for explicitly declared business collections. Warnings use optional
structured `warnings`; unknown facts, supported nulls and query coverage remain
unchanged. Table/TSV project business fields; action indexes do not become repeated
text in every row.

## Exact project authorization boundaries

| Exact release set | Count | Source semantics of `grantProject` |
| --- | ---: | --- |
| 1.3.9, 3.0.0–3.0.6, 3.1.0–3.1.9 | 18 | Delete all relations, then write the supplied set; the recipe uses `replace` |
| 2.0.0–2.0.9 | 10 | Insert the supplied relations directly; the recipe uses `insert` |
| 3.2.0–3.2.2, 3.3.1–3.3.2, 3.4.0–3.4.3 | 9 | Update only the target project relation; the recipe uses `upsert` |

The source method is `UsersService[Impl].grantProject` in each exact API.
`grantProjectWithReadPerm` first appears in 3.2.0. The public REST APIs in the
first two groups only grant write permissions. Reviewed evidence and exact action
selection are maintained by the
[compatibility compiler](../../tools/ds_codegen/compatibility_impact.py) and
[user domain adaptation](../../src/dsctl/upstream/users.py).

Replacing the entire set must preserve other project relations and the upstream
concurrency limitations. Insert semantics must account for idempotency; newer
versions must account separately for upgrading read-only permissions.
Readback with `verification: "membership_only"` proves only that the relation
exists, not its permission level or actual execution by the target user.
An administrator's view cannot replace verification under the target identity.

Future changes must review whether grants replace the entire set, whether
read-only permissions are supported, and whether queries return relation perm.
Unchanged routes or DTOs do not replace service-semantics review.

## Task journeys and acceptance

| User goal | Path using existing commands | Decisions/evidence to retain |
| --- | --- | --- |
| Create a workflow | template → lint → create dry-run → create | Then save only, run manually or configure a schedule; online→run is not mandatory |
| Edit a scheduled workflow | export → modify/preview → explicit offline → edit → online → check schedule | Dry-run can report state blockers; restoring an offline schedule is a separate decision |
| Adjust a schedule | list/get → explain → offline (if needed) → update → preview → online (if needed) | Schedule id, actual timezone/parameters, no implicit activation on update, and current workflow state |
| Investigate failure | instance digest → tasks/logs → child instance or node history → choose repair/recovery → watch | Actual parent/child relations, original version and parameters, current execution-round baseline, and query coverage |
| Investigate queuing | task-group → queue → task/instance → adjust priority or request force-start → observe | Capacity pools differ from queue records; acceptance is not execution; do not guess missing identities |
| Verify authorization | Administrator inspects/changes specific relations and permission levels → readback → user explicitly selects the target identity for verification | Do not switch identities automatically; prove membership, permission level, object visibility and actual execution separately |

Acceptance covers OFFLINE/ONLINE/missing/unknown states, permission levels,
unavailable versions, missing or duplicate identities, insufficient input,
cross-context cases, accepted requests that have not started, and completion of
a new execution round. Navigation commands must be accepted by the real parser
without adding requests; both JSON formats retain the same facts after decoding.

Compare output designs using equivalent tasks and business information. Evaluate
task correctness, identity and state errors, writes without authorization for the
target, CLI/REST call counts, actual usage and elapsed time. Total bytes are a
secondary metric; do not impose an information budget that sacrifices correctness.

## Upstream review cues

The DS 1.3.9
[instance menu](https://github.com/apache/dolphinscheduler/blob/174c78c4a90a53fdfe7131e9b065edaa38b7936f/dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/instance/pages/list/_source/list.vue#L106-L138)
uses `disabled` to express groups eligible for operations, while DS 3.4.1
[instance actions](https://github.com/apache/dolphinscheduler/blob/f19eb8ce7dc4d7c0be9e213610d6812718294333/dolphinscheduler-ui/src/views/projects/workflow/instance/components/table-action.tsx#L119-L261)
use it to disable buttons. Variable names cannot directly define business
eligibility rules.

From DS 3.2.0, task-instance entry points change from processInstanceId/name to
workflowInstanceId/name. Naming changes still require exact wire adaptation;
they do not create new object relations. The DS 3.4.2
[start form](https://github.com/apache/dolphinscheduler/blob/71eb6412f940afa1f171f1097dc0e99ed61d16e2/dolphinscheduler-ui/src/views/projects/workflow/definition/components/use-modal.ts)
corrects parameter field binding and validation, showing that validation can
change even when menu actions remain unchanged.

Differences in UI handlers, forms and menus should trigger source review of the
owning controllers, services and enums. Review the operations that actually
changed. Do not derive generic business rules from arbitrary Vue/TSX conditionals
or extend a sample review into authorization of capabilities across all versions.
