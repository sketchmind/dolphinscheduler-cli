# Exact task evolution exercise: EMR Serverless

This case study follows the `3.4.1` → `3.4.2` EMR Serverless change through
source review, authoring ownership and registration checks. Use it as an example
of the [DS evolution workflow](architecture.md#ds-evolution-workflow), not as
current release evidence. Exact memberships and worker prerequisites remain
owned by the [task authoring boundaries](task-authoring-boundaries.md).

## Source and review decisions

The reviewed upstream tags resolve to these commits and trees:

| Tag | Commit | Tree |
| --- | --- | --- |
| 3.4.1 | `f19eb8ce7dc4d7c0be9e213610d6812718294333` | `a70cc32247f82dc5925ae3539ee1e3879e819d61` |
| 3.4.2 | `71eb6412f940afa1f171f1097dc0e99ed61d16e2` | `7e797deff8662d9bf918feb32b551ca865055df4` |

The original review observed 36 source task types and 34 typed memberships on
`3.4.1`, and 37 and 35 on `3.4.2`. These are historical review counts, not the
current authoring inventory. `EMR_SERVERLESS` is the added source type and
reviewed family. Its
source semantic fingerprint is
`sha256:d1770a42bb8a9d784d0f06c2eef3f35acd6a78e270cd5a404ff3beb971733c0b`.

Under the added upstream directory
`dolphinscheduler-task-plugin/dolphinscheduler-task-emr-serverless/`:

- `EmrServerlessTaskChannelFactory.getName()` registers `EMR_SERVERLESS` by SPI.
- `EmrServerlessParameters` owns `applicationId`, `executionRoleArn`, optional
  `jobName`, and `startJobRunRequestJson`; `AbstractParameters` supplies
  inherited parameters. Resource discovery returns an empty list.
- `EmrServerlessTask.buildStartJobRunRequest()` substitutes prepared values in
  the raw JSON, parses the AWS SDK request, then overwrites application, role,
  name and client token. The fallback name comes from the DS task. Submission
  persists `jobRunId` through `appIds`; tracking can recover it. Credentials
  use `aws.emr.*` or the default AWS chain. These are worker behavior and
  prerequisites, not additional CLI fields or live evidence.

The independent review `emr-serverless-3.4.2-raw-start-job-run` narrows inherited
parameters to unique `IN`/`VARCHAR` bindings, rejects placeholders in the three
top-level identity/name fields, and retains the AWS request as raw JSON.
Runtime and future fields keep their opaque preservation rules.

## Actual edit path and ownership

| Entry point | Responsibility for this change |
| --- | --- |
| `tools/ds_codegen/task_profile_facts.json` | Extracted registration/model identity, exact source tree and semantic fingerprint; absent on every earlier profile |
| `tools/ds_codegen/task_profile_reviews.json` | Exact positive membership, reviewed CLI model, evidence paths and independent review identity |
| `tools/generate_ds_task_profiles.py` → `src/dsctl/generated/task_profiles.py` | Compile and validate the complete runtime ledger; the canonical runtime generator includes the same artifact and generated output is never edited by hand |
| `src/dsctl/models/task_spec/emr_serverless.py` and `registry.py` | `EmrServerlessTaskParamsSpec`, five canonical fields and local validation; one model registry entry with the stable-default exclusion |
| `src/dsctl/services/task_authoring_catalog/emr_serverless.py` and `registry.py` | Field descriptions/templates, parameter restrictions, facet policy, raw substituted-JSON validation and one authoring implementation registration |
| `src/dsctl/upstream/task_parameter_projection/engine.py` | Existing identity projection is sufficient: the canonical names and raw values are already the DS wire. No new projection registration is needed |
| `tests/services/test_emr_serverless_authoring.py` | Independent expected native payload, unsupported exact releases, malformed typed input, raw opaque state, export and metadata-only preservation |

Structural field type/requiredness comes from `model_field`; it does not need
another catalog declaration. Semantic restrictions, templates and compile paths
remain explicit. The review and the implementation are separate assertions:
source presence or an installed model must not imply an authoring policy.
Authoring builders and wire projectors select different work and do not need
matching registration sets. `stable_default` remains the historical public
model lookup contract; this exercise does not derive it from exact reviews.

## Missing-registration experiment and narrow repair

Before the registration guard, removing only `EMR_SERVERLESS` from the authoring builder map
still let `_generated_catalog("3.4.2")` succeed. It reported typed support,
had no materialized entry and added the family to `legacy_typed_task_types`.
The model-name check passed, silently bypassing the reviewed facet and its
substituted-JSON validation. A contributor could therefore forget one
registration while apparently retaining typed support.

Catalog assembly now consumes each exact review/exclusion and requires an
explicit implementation selection. The same registration table uses `None`
for the seven existing model-only families: CONDITIONS, HTTP, PYTHON,
REMOTESHELL, SHELL, SUB_WORKFLOW and SWITCH. Missing registrations fail before
catalog construction, and an exclusion requires a materialized policy.
This removes the rule that an absent builder implicitly chooses generic typed
authoring. It adds no second fallback registry, generator, projection binding
or new task framework.

The number of semantic edit owners for a new family is unchanged. The guard
makes an omitted implementation selection fail explicitly instead of silently
selecting a different policy.

## Reproduction and verification

Read-only upstream inspection (the optional checkout needs both existing tags):

```bash
git -C references/dolphinscheduler rev-parse '3.4.1^{commit}' '3.4.2^{commit}'
git -C references/dolphinscheduler diff --name-status 3.4.1 3.4.2 -- dolphinscheduler-task-plugin/dolphinscheduler-task-emr-serverless
git -C references/dolphinscheduler show '3.4.2:dolphinscheduler-task-plugin/dolphinscheduler-task-emr-serverless/src/main/java/org/apache/dolphinscheduler/plugin/task/emrserverless/EmrServerlessTask.java'
```

From a configured development environment, use these focused checks when
changing the corresponding registration or authoring boundaries:

```bash
python -m pytest -q tests/services/test_task_authoring_model_structure.py tests/services/test_emr_serverless_authoring.py tests/services/test_task_authoring_catalog.py tests/upstream/test_task_parameter_projection.py
python -m pytest -q tests/codegen/test_ds_task_profiles.py tests/models/test_task_spec.py -m 'not source_contract and not source_rebuild'
```

Keep independent expectations for wire compilation, invalid typed input and
opaque preservation. Include missing facet, model-only and exclusion
registrations, and compare generated catalog/schema fixtures without refreshing
them merely to make a refactor pass. Compiler negative cases do not replace a
source rebuild. Substantial changes still require the complete development gate;
focused checks alone establish neither release readiness nor live AWS execution.
