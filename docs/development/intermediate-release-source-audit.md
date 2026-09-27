# Intermediate release source audit

Date: 2026-09-08. Product baseline: `e5eea9c`.

This historical audit filled the source inventory between the 15 exact profiles
selectable at that baseline: `2.0.1`–`2.0.8`, `3.0.1`–`3.0.5`, and
`3.1.1`–`3.1.8`. Its scope, together with those representatives, covered 36 final
upstream releases from `1.3.9` through `3.4.2`; `3.3.0-alpha` is not a final
release. These numbers describe the audit scope, not the current inventory.
The later [final-release admission](stable-release-admission.md) records exact
profiles through `3.4.3`; [architecture](architecture.md#current-stable-surface)
owns current counts. Source analysis alone does not register or promote profiles.

The existing implementation is reusable at operation and task-family boundaries.
An entire intermediate release cannot inherit all behavior and support decisions
from either endpoint. In particular, intermediate worker defects can be absent
from both sampled endpoints. Exact task restrictions therefore remain necessary
even when the REST contract is unchanged.

## Method and evidence boundaries

- Reuse full contract snapshots only after checking their source identity and
  extractor fingerprint. The audit reused the 15 representative snapshots.
- Keep mounted reference sources read-only. Verify each official Git tag,
  commit, tree and root POM version before extraction. All 21 intermediate
  sources had root POM versions matching their tags.
- Extract each exact version with the contract generator and compare it with
  both representatives in its minor line: `2.0.0`/`2.0.9`, `3.0.0`/`3.0.6`,
  `3.1.0`/`3.1.9`.
- Separate full controller/type inventory changes from the CLI's reviewed
  operation roots and their transitive type closure. Also compare normalized
  effective HTTP request and response contracts. Java package moves and unused
  operations are not automatically CLI incompatibilities.
- Inventory API/service and task-plugin changes from Git objects. Read the
  relevant changed methods to distinguish new behavior, reusable existing
  recipes, and actual defects. This is bounded source review, not exhaustive
  proof of every worker, permission, dependency, and deployment configuration.

An equal normalized wire comparison is a reuse candidate, not an action-support
decision. A nonzero source diff is a review input, not proof that a request fails.
No new server instance or worker task was exercised for this audit.

## Exact source inventory

The audit extracted all 21 snapshots and compared each with both same-minor
endpoints. Counts below describe that full extracted controller
inventory, not supported CLI actions.

| Release | Source commit | Controller operations |
| --- | --- | ---: |
| `2.0.1` | [`bf2197995ac8`](https://github.com/apache/dolphinscheduler/tree/bf2197995ac835c66b44874b8658579a3da916b0) | 189 |
| `2.0.2` | [`24ddd07493c6`](https://github.com/apache/dolphinscheduler/tree/24ddd07493c6260a877db8a5c73e654263d356ad) | 195 |
| `2.0.3` | [`f1a3c52a66eb`](https://github.com/apache/dolphinscheduler/tree/f1a3c52a66eb8dec1f853b9c9a02b88cdebbd0dc) | 196 |
| `2.0.4` | [`0ff01e3c68da`](https://github.com/apache/dolphinscheduler/tree/0ff01e3c68da6a174836216f87c05f707233f7f9) | 195 |
| `2.0.5` | [`f31328276c12`](https://github.com/apache/dolphinscheduler/tree/f31328276c12821c21d1ce07e824bc74146c19b2) | 195 |
| `2.0.6` | [`6aaf6e39ed87`](https://github.com/apache/dolphinscheduler/tree/6aaf6e39ed87fae47fbaf276e86c65f7e1f75c65) | 195 |
| `2.0.7` | [`bf5a0f228b78`](https://github.com/apache/dolphinscheduler/tree/bf5a0f228b7858c8a3e682a0caa225e3569db15e) | 195 |
| `2.0.8` | [`2a22b960580c`](https://github.com/apache/dolphinscheduler/tree/2a22b960580c907f67128a294df2b10b4380312a) | 195 |
| `3.0.1` | [`cb03ca8166cb`](https://github.com/apache/dolphinscheduler/tree/cb03ca8166cb9d6d8e63db43664b4a2126ea43b4) | 228 |
| `3.0.2` | [`d2ba3d4cfb3a`](https://github.com/apache/dolphinscheduler/tree/d2ba3d4cfb3ab9f252340aa603ea036baa52f88c) | 229 |
| `3.0.3` | [`a3b882a80a91`](https://github.com/apache/dolphinscheduler/tree/a3b882a80a91d2b8d1335e42b5d19a8ef144fe3b) | 229 |
| `3.0.4` | [`0a4175261e93`](https://github.com/apache/dolphinscheduler/tree/0a4175261e93ebfe5557fec023ac4b1c2ec950ee) | 229 |
| `3.0.5` | [`dc3c96513425`](https://github.com/apache/dolphinscheduler/tree/dc3c9651342529fcfde47dd0949a0ced18ce9e4a) | 229 |
| `3.1.1` | [`efc03679994a`](https://github.com/apache/dolphinscheduler/tree/efc03679994a7522a0adc1e6662285ab8f617f61) | 257 |
| `3.1.2` | [`f1aefae5e25d`](https://github.com/apache/dolphinscheduler/tree/f1aefae5e25daa5beef08accd8fbd26c32fb6b47) | 257 |
| `3.1.3` | [`4a3299ac8cba`](https://github.com/apache/dolphinscheduler/tree/4a3299ac8cba79c8e40ce082b0f2594c369b4073) | 257 |
| `3.1.4` | [`2d6393d722f8`](https://github.com/apache/dolphinscheduler/tree/2d6393d722f8288dfb7ab5eddbbb8319b491824e) | 257 |
| `3.1.5` | [`9e5b0a1cc346`](https://github.com/apache/dolphinscheduler/tree/9e5b0a1cc3469cd7f56f15966f8fe2ebcc1f40b0) | 257 |
| `3.1.6` | [`7780fdc2f537`](https://github.com/apache/dolphinscheduler/tree/7780fdc2f537aa6978d76ee62f2088f730b5969d) | 257 |
| `3.1.7` | [`b570a187f6eb`](https://github.com/apache/dolphinscheduler/tree/b570a187f6eb92b20d6daacae01eeca17c774007) | 257 |
| `3.1.8` | [`df9becb77045`](https://github.com/apache/dolphinscheduler/tree/df9becb770455d9d8aae661a54ce1a2a1b131964) | 257 |

## Extractor coverage gaps

The initial comparison reported a nonzero strict CLI closure diff for every
candidate. Those numbers must not be interpreted as incompatible actions.
Several response corrections in
`tools/ds_codegen/extract/return_type_resolution.py` were scoped to the 15
reviewed versions, so intermediate sources did not receive them in that run.

The `3.0.5`/`3.0.6` pair exposed this directly: its relevant Java source trees
are identical, but the audit's raw extractor reported three changed response
operations:

- resource paging uses a different qualification of `PageInfo<Resource>`;
- schedule creation retains a synthesized service-map model instead of the
  actual `Schedule` payload;
- task deletion lacks the reviewed nullable `ProcessDefinition` correction.

These were analyzer coverage gaps, not three newly discovered REST changes.
The same patterns recurred elsewhere; older schedule-preview response corrections
also required explicit coverage. Keep original snapshots and raw diffs intact.
Any derived correction must validate the candidate's actual source against the
existing guard and retain its exact source identity. A matching version-number
prefix is not sufficient evidence.

The derived audit applied existing source guards to the actual candidate source;
it did not modify snapshots or the generator's version tables. A missing
`Schedule` model was reused only after its complete class AST and imports
matched. Schedule-preview correction additionally required matching controller
and service method ASTs because the correction then had no structural
source guard. Failed guards retained their original difference and reason.

In that audit, **9 of 21 releases matched a representative's selected static
CLI closure**. The other 12 were not proven equal by that comparison; this was
not a count of incompatible releases or their final admission result.

| Candidate releases | Historical comparison after source-guard review |
| --- | --- |
| `2.0.8` | Zero selected-closure differences from `2.0.9`. |
| `3.0.2`–`3.0.5` | Zero selected-closure differences from `3.0.6`. |
| `3.1.1`–`3.1.2` | Zero selected-closure differences from `3.1.0`. |
| `3.1.7`–`3.1.8` | Zero selected-closure differences from `3.1.9`. |
| `2.0.6`–`2.0.7` | No normalized operation differences from `2.0.9`; two explicitly retained SWITCH model fields differ: `nextNode` is `List<String>` instead of `Long`. |
| `3.0.1` | One remaining difference from `3.0.0`: the schedule-update priority default. |
| `3.1.3`–`3.1.6` | One remaining difference from `3.1.9`: the task execution-status enum lacks `STOP`. |
| `2.0.1`–`2.0.5` | Type/resource-response mapping and some operation differences remained; failed source guards and missing type roots were retained for the later exact admission review. `2.0.2` also has an optional instance-update `flag`; `2.0.2`/`2.0.3` task-delete inference was `Void` rather than the later nullable entity. No whole-closure equality was asserted. |

In particular, `3.1.2` appears in the zero-closure group and still has the
OPENMLDB worker defect below. REST closure equality does not establish task
execution support. For early `2.0.x`, package relocation and unresolved
extraction evidence must be reviewed separately from genuine wire changes;
remaining diff counts are not a justified estimate of new implementation work.

Before generating new runtime profiles, extend the reviewed correction coverage
for the verified sources and reproduce the resulting contracts. Do not relabel
an intermediate source as an existing representative to bypass the checks.

## Material findings

### Reuse and existing branch selection

`3.0.5` and `3.0.6` have identical Java source trees in the API, service, DAO,
and common modules. This is stronger reuse evidence than a matching request
signature, while still excluding plugins, SQL resources, dependencies, and
deployment state.

`2.0.8` and `2.0.9` have only one API/controller/service source difference:
the default execution command when `execType` is null. Current CLI execution
requests explicitly send that field, so they do not trigger the difference.

The user domain needs existing branches selected at the actual transition:
`2.0.1` has no revoke-project endpoint and no created-user entity response;
`2.0.2`–`2.0.8` have both. The existing `legacy_200` and `state_entity` user
recipes already express these alternatives. This conclusion is specific to
the user domain, not a whole-version alias. See the
[2.0.2 controller](https://github.com/apache/dolphinscheduler/blob/24ddd07493c6260a877db8a5c73e654263d356ad/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/controller/UsersController.java#L270).

The task-instance page filter also needs exact enum membership:
`TaskExecutionStatus.STOP` is absent in `3.1.3`–`3.1.6` and present from
`3.1.7`. This changes the accepted `stateType` values without changing the
controller's parameter signature. It calls for the correct enum catalog,
not a separate page-list adapter.

`3.0.1`–`3.0.3` combine timezone-aware schedule preview with no rejection of
past schedule start times. Neither endpoint has that combination: timezone
handling arrived in `3.0.1`, and the past-start check in `3.0.4`. The CLI
already sends the relevant fields and uses the server's preview result, so this
requires exact expectations rather than a new scheduling adapter.

### Intermediate task defects

These are source-derived execution findings. They were not reproduced against
newly deployed workers during this audit.

| Task / release boundary | Source finding | Admission consequence |
| --- | --- | --- |
| `FLINK`, `3.1.1` | The SQL main-jar guard is fixed, but SQL deployment selection maps `CLUSTER` to local execution and `LOCAL` to YARN. The condition is corrected in `3.1.2`. | Do not infer correct local SQL execution from the main-jar fix alone. |
| `OPENMLDB`, `3.1.2` | The inherited Python execution method dereferences an uninitialized parent parameter after running the command. `3.1.1` lacks that dereference; `3.1.3` initializes the parent parameter. | Record a `3.1.2` execution hole even though both representative endpoints lack this particular defect. |
| `K8S`, through `3.1.3` | A RUNNING event can release the worker wait prematurely; the reviewed fix first appears in `3.1.4`. | Preserve the runtime restriction until the actual fix. |
| `FLINK_STREAM`, through `3.1.4` | Its own main-jar override remains defective after the ordinary FLINK path changes; the fix appears in `3.1.5`. | Keep stream and batch boundaries independent. |

The first two findings demonstrate why checking only representative endpoints
cannot establish all intermediate task memberships:

- In [3.1.1 FlinkArgsUtils](https://github.com/apache/dolphinscheduler/blob/efc03679994a7522a0adc1e6662285ab8f617f61/dolphinscheduler-task-plugin/dolphinscheduler-task-flink/src/main/java/org/apache/dolphinscheduler/plugin/task/flink/FlinkArgsUtils.java#L127),
  the wrong deployment-mode condition selects the execution target. Compare
  [3.1.2](https://github.com/apache/dolphinscheduler/blob/f1aefae5e25daa5beef08accd8fbd26c32fb6b47/dolphinscheduler-task-plugin/dolphinscheduler-task-flink/src/main/java/org/apache/dolphinscheduler/plugin/task/flink/FlinkArgsUtils.java#L127).
- [3.1.2 OpenmldbTask.init](https://github.com/apache/dolphinscheduler/blob/f1aefae5e25daa5beef08accd8fbd26c32fb6b47/dolphinscheduler-task-plugin/dolphinscheduler-task-openmldb/src/main/java/org/apache/dolphinscheduler/plugin/task/openmldb/OpenmldbTask.java#L65)
  initializes only its own parameter. The inherited
  [PythonTask.handle](https://github.com/apache/dolphinscheduler/blob/f1aefae5e25daa5beef08accd8fbd26c32fb6b47/dolphinscheduler-task-plugin/dolphinscheduler-task-python/src/main/java/org/apache/dolphinscheduler/plugin/task/python/PythonTask.java#L102)
  runs the command, then dereferences the unset parent parameter and marks the
  task failed. If the command has already produced external effects, retrying
  can repeat those effects. This consequence is conditional on reaching that
  execution path; it is not a live-test result.

The audit also located these transition points for the later exact reviews:

| Surface | First changed release | Evidence to retain |
| --- | --- | --- |
| WATERDROP | `2.0.1` | Explicit worker registration as a SHELL alias. |
| PROCEDURE | `2.0.2` | Native OUT-parameter publication path. |
| DEPENDENT | `3.0.2` / `3.1.1` | Month-boundary dependency windows in the respective release lines. |
| DATAX | `3.1.1` | Null/empty prepared-parameter map guard; later argument quoting changes do not establish arbitrary shell-value safety. |
| SAGEMAKER | `3.1.1`, fixed in `3.1.3` | Only `3.1.1`–`3.1.2` discard the refreshed local status and can poll indefinitely after initial EXECUTING. `3.1.0` updates an instance field in `describePipelineExecution` and is unaffected; the earlier audit inference about it was incorrect. |
| SEATUNNEL | `3.1.6`, then `3.1.7` | An intermediate engine-selector/deploy-mode combination precedes the startup-script protocol. Verify polymorphic deserialization and launcher behavior before admitting those facets. |

The later admission review followed the complete SageMaker status data flow:
[`3.1.0` instance-field update](https://github.com/apache/dolphinscheduler/blob/3.1.0/dolphinscheduler-task-plugin/dolphinscheduler-task-sagemaker/src/main/java/org/apache/dolphinscheduler/plugin/task/sagemaker/PipelineUtils.java#L83),
[`3.1.1` stale local variable](https://github.com/apache/dolphinscheduler/blob/3.1.1/dolphinscheduler-task-plugin/dolphinscheduler-task-sagemaker/src/main/java/org/apache/dolphinscheduler/plugin/task/sagemaker/PipelineUtils.java#L69),
and [`3.1.3` returned-value assignment](https://github.com/apache/dolphinscheduler/blob/3.1.3/dolphinscheduler-task-plugin/dolphinscheduler-task-sagemaker/src/main/java/org/apache/dolphinscheduler/plugin/task/sagemaker/PipelineUtils.java#L76).
The final exact task admissions and remaining holes are recorded in
[task authoring boundaries](task-authoring-boundaries.md).

The task scan covered existing review evidence plus task-plugin, worker/master,
parameter, registration and authoring UI surfaces across the 21 candidates and
six endpoints, then read selected material changes. It did not manually attest
every changed method or transitive execution dependency.

### Shared schedule error translation

The audit exposed a shared schedule error-translation gap for
`START_TIME_BEFORE_CURRENT_TIME_ERROR` (`80004`), which appears in `3.0.4` and
remains in representative `3.0.6`. The source is
[3.0.4 SchedulerServiceImpl](https://github.com/apache/dolphinscheduler/blob/0a4175261e93ebfe5557fec023ac4b1c2ec950ee/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/impl/SchedulerServiceImpl.java#L169).

This gap is now resolved in
[_schedule_support.py](../../src/dsctl/services/_schedule_support.py): the code
maps to `UserInputError`, with a suggestion to choose a future `--start`, keep
`--end` later, and update an existing schedule before retrying `schedule online`.
[test_schedule.py](../../tests/services/test_schedule.py) covers the create,
update and online paths. It is shared error handling, not an intermediate-version
adapter.

## Other source surfaces

Datasource parameter DTO inventory covers inheritance, non-static fields,
Java types, field annotations, and initializers, including both `DataSource`
and `Datasource` filename spellings. All 21 inventories match at least one
same-minor representative. This supports DTO reuse; it does not certify JDBC
processor behavior, installed drivers, or connectivity. The `2.0.0` to `2.0.1`
package relocation is retained as a source difference rather than described as
a changed user payload.

None of these 21 sources provides the product-info method or the version-bearing
OpenAPI configuration required for exact metadata discovery. The older Swagger
configuration, where present, does not set a DS release version in `ApiInfo`.
Adding exact profiles does not itself identify those deployments automatically.
The later [contract-read discovery](contract-read-discovery.md) can admit bounded
reads from observed public API contracts without identifying an exact release;
operations outside that policy still require an explicit `DS_VERSION`.

At the audit baseline, the resolver accepted only the 15 registered versions
and rejected these 21 intermediate versions before transport. Their later exact
admission supersedes that selection limit. Selecting a neighboring version
remains an invalid substitute for an exact profile.

## Admission rules

The subsequent admission followed these rules, which also apply to future
release reviews:

1. Record exact source identities and reviewed operation/action decisions for
   each tag. Reuse existing request/response programs wherever their
   contracts match; do not create a handwritten adapter per release.
2. Record task support and preservation boundaries at the actual transition
   versions, including defects present only in an intermediate release. Reuse
   family implementations while retaining exact membership and restrictions.
3. Resolve the concrete shared defects recorded by this audit and test the
   affected error or authoring behavior. A defect shared with an existing
   representative is a common implementation issue, not a reason to invent a
   new version family.
4. Regenerate the complete profile inventory atomically, then run the existing
   development and applicable installed-wheel/live gates. Keep release-specific
   verification separate from mechanical reuse and profile promotion.

## Reproduction

Use the regular [codegen tools](codegen.md#analyze-a-candidate-release) to extract
an exact source and compare it with a reviewed base. Full snapshots and derived
comparison reports belong under ignored `build/`, not in the runtime package.

For CLI-consumed comparisons, `--scope cli` applies the base's reviewed roots
while preserving the target's actual version and source identity. This scope
requires the current extractor fingerprint. Regenerate stale inputs into new
artifacts, retaining historical snapshots and reports under their original
identity. Do not relabel a target snapshot as another supported release.

The original audit changed documentation only. Its source comparisons did not
change generated output, support levels, task reviews, version discovery or
runtime membership; those changes belonged to the later admission.

## Generator Follow-up

The subsequent generator refactor separated source inventory, reviewed profile
membership and packaging selection. Named source correction rules and exact
review membership have separate modules. Candidate-enabled correction rules
require reusable source guards; other historical corrections remain exact-only.
Reviewed releases retain their exact correction membership, including
coordinates deliberately absent from that membership.

Candidate extraction and CLI-closure comparison now use the regular
[codegen tools](codegen.md#analyze-a-candidate-release); the temporary audit
adjudicator is not a production dependency. The original snapshots and reports
remain historical evidence from their original extractor. Static equality
still does not establish task behavior or runtime admission. That refactor
alone did not add the 21 candidates to the selectable inventory; the later
admission did.

The refactor's focused comparisons recorded these outcomes before admission:

| Candidate | Reviewed base | Historical CLI-scope assessment |
| --- | --- | --- |
| `2.0.1` | `2.0.9` | `incomplete_evidence`: missing operation/type roots |
| `2.0.8` | `2.0.9` | `equal` |
| `3.0.5` | `3.0.6` | `equal` |
| `3.1.2` | `3.1.0` | `equal`; the task defect above is still a separate fact |
| `3.1.3` | `3.1.9` | `different`: `TaskExecutionStatus.STOP` is absent |

These are selected historical static comparisons, not current admission
decisions or release receipts. Preserve the distinction between reproducible
source evidence, reviewed runtime membership and current-wheel acceptance.
