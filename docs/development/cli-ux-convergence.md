# CLI Usability and Evaluation

This document records shared CLI usability decisions and their acceptance
criteria. Public behavior is maintained in the
[CLI Contract](../reference/cli-contract.md); DS behavior remains grounded in
exact source and generated contracts.

## Product decisions

- Keep predictable default JSON across terminals and pipelines. Table and TSV
  remain explicit projections. Preserve identity, coverage and uncertainty when
  narrowing output; use byte/token measurements as review signals, not fixed
  correctness gates.
- Keep one identity source per invocation. Resolve retry/timeout policy
  independently so process tuning cannot silently select another cluster.
- Give every command an explicit effect contract. Use schema 3 to remove old
  duplicate pattern aliases and expose remote/local effects from the catalog.
- Distinguish successful observation from successful execution. Failed doctor
  checks and invalid lint return nonzero with their complete report; ordinary
  watch observes terminal states and `--exit-status` requires execution success.
- Report known dry-run blockers before mutation. Preview is still an observation,
  not a lock, a server-side transaction or a guarantee of worker readiness.
- Keep discovery progressive: purpose/examples and grouped help at the root,
  self-contained leaf syntax, machine contracts in schema, and one main task
  template with branch guidance only when needed.

No new init wizard, global project variable, CSV format, quiet envelope mode or
HTTP facade is required by these findings. Scope flags are accepted where they
actually constrain resolution; native ID-only log access remains ID-only.

## Implementation and acceptance

| Area | Required behavior |
| --- | --- |
| CLI entry | `-h`, local eager `--version`, explicit completion shell; structured usage errors retain spelling and missing-parameter guidance |
| Execution effects | All 181 actions declare remote/local effects; 7 preview-capable commands declare the dry-run override; alert testing remains a write |
| Target selection | Policy-only environment values retain the default context/project; URL/token/version stay atomic; timeout reaches normal REST and discovery |
| Diagnostics | Any doctor error fails with all checks; watch success mode retains final state on failure; parser errors exit 2 and domain failures exit 1 |
| Schedules | Null preferences supply no defaults; ID lookups honor explicit/context project and never escape that scope |
| Output | Dotted JSON projections preserve types/metadata; CJK tables align; nested details have sections; coverage stays meaningful; aliases/version inventory are expanded only where needed |
| Secrets | User password file or explicit stdin works without printing contents; conflicting/malformed inputs stop before mutation |
| Authoring | Workflow lint aggregates independent findings; patch lint declares deferred baseline checks; multiline YAML round-trips losslessly |
| Previews | Backfill local input validation precedes version/network work; run prechecks ONLINE state; creation rejects observed same-project name conflicts |
| Portability and edits | Reviewed datasource references resolve exact names or IDs before wire construction; existing-baseline preservation stays explicit; value-level diffs show before/after |

The lint package separates orchestration, diagnostics, task semantics and graph
checks while preserving its public service API. Generated wire contracts keep
numeric datasource IDs; caller-facing name resolution belongs above that layer.
Exact task memberships, opaque preservation boundaries and profile promotion
are unchanged.

## Source grounding

The existing generated definition and datasource domains supply identity reads;
no new handwritten REST shapes are introduced. Name uniqueness is checked by
`WorkflowDefinitionServiceImpl` in 3.4.1 (same-project `verifyByDefineName`,
`WORKFLOW_DEFINITION_NAME_EXIST`) and the legacy definition name verifier.
`ExecutorService`/`ExecutorServiceImpl` in reviewed 1.3.9, 2.0.0, 2.0.9, 3.3.1
and 3.4.2 rejects execution when release state is not ONLINE. Remote error
translation remains authoritative for races after preview.

## Validation requirements

Focused regressions cover strict datasource reference schemas and their
minimum-version Pydantic projections, name/type binding, preservation,
offline lint, error channels, effects, output projection and the
create/online/run process. Substantial implementation changes also require the
development quality gate:

```bash
python tools/check_quality_gate.py --mode development
```

Installed-wheel acceptance starts from a clean source snapshot and verifies
the wheel's contents, metadata and installed files before live testing. Bind
results to the tested artifact and source; retain failed checks and distinguish
combined lane results from a successful complete gate invocation. See
[Release](release.md) and [Live Testing](live-testing.md) for the current
artifact-bound requirements. Historical receipts do not attest a later
candidate, and usability checks do not promote a source profile.

## Output Encoding Acceptance

The [JSON layout contract](../reference/cli-contract.md#json-layout) owns the
encoding rules. The shared output layer must use declared business row paths,
without command-specific branches, tokenizer-dependent layouts or changes to
generated upstream contracts. Explicit filters and columns select information;
format selection only changes its representation.

Regression coverage must prove field-by-field equivalence after decoding
compact JSON: every declared row path, empty/single/multiple rows, supported
nulls, mixed types, nested values, literal-dot keys, large integers, duplicate
identities, index truncation, projections, shape errors and raw bodies.
Preserve envelope metadata, pagination, warnings and navigation over the same
logical rows. Do not invent unsupported fields or trim information merely to
improve size measurements.

## Usability measurement

Compare CLI-only use with CLI plus the public skill using the same model,
configuration, task inputs and replayable synthetic REST data. Isolate inherited
DS/DSCTL selectors and run outside the source checkout. Evaluation permits only
public CLI commands; review traces for source, credential or direct REST access.
Reading source, bypassing the CLI with direct REST, or making unauthorized
changes invalidates the affected result.
This command protocol boundary does not provide OS confidentiality isolation.

Judge task correctness before efficiency: require correct failure identities,
actual parent/child relationships, supporting log evidence and explicit partial
coverage when permissions restrict discovery. Fix command, request and time
limits before the exercise, and report incomplete attempts as such.

Use reviewable fixed tasks, fixtures and scoring criteria. Retain failed and
repeated attempts; do not hide reruns to obtain a successful comparison.
Simulated wrappers and replay alone do not establish production or real-cluster
acceptance. Exclude authentication headers from reports and keep raw evidence
private. Per-command cost claims require per-request usage measurements.

Check actual argv, exit statuses and CLI/REST traces independently. Record
official model usage as separate input, cached-input, output and reasoning
fields, alongside output bytes and wall time. Distinguish cached and uncached
input; reasoning tokens included in
output usage must not be counted twice. Bytes are not tokens, and CLI output is
only part of model context cost. A single paired sample cannot establish a
stable improvement percentage or general performance benefit. Keep raw traces,
usage reports and temporary candidate records in ignored local evidence storage.

Replay comparisons may normalize observation timestamps, request IDs and fixture
ports while retaining return codes, results and errors. Such replay validates
the recorded invocations; it does not substitute for model evaluation of
unexercised actions or current-wheel live acceptance. Real-cluster read results
also remain bounded by their observed action scope; contract candidates do not
identify an exact release or authorize mutation.

Future evaluations should test whether narrower initial discovery guidance
reduces repeated exploration across different operational and authoring tasks.
Retain explicit full contracts for callers that need them; a small sample does
not justify removing information or imposing an arbitrary response-size ceiling.
