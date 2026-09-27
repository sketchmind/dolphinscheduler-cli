# Version and Read Discovery

Discovery first tries to identify the exact server release from reviewed
metadata. If that is unavailable, an observed API contract can admit a bounded
set of reads without identifying the release. Neither path promotes a profile
or replaces an installed-wheel release gate. Public configuration belongs in
[Configuration](../user/configuration.md#version-selection); module ownership
belongs in [Architecture](architecture.md#target-version-resolution).

## Evidence and Selection

Discovery preserves the metadata-first path and eight exact mappings:
OpenAPI identifies `3.2.0` and `3.2.1`; product information identifies `3.3.1`,
`3.3.2` and `3.4.0`–`3.4.3`. Those six later releases also have reviewed
OpenAPI metadata paths. Explicit `DS_VERSION` is authoritative.
The stock `3.2.2` metadata value `3.3.0` remains an independent reported value,
never an alias for an exact release. Missing metadata and this reviewed anomaly
can continue to document discovery; contradictory or other unreviewed release
reports do not silently select a different profile.

Generated bootstrap facts bind product-information paths and response fields
to full controller snapshots. OpenAPI declarations, database initialization
and legacy route collisions retain their exact source membership:

- Older plugin controllers interpret `query-product-info` as a plugin ID.
  Only their reviewed `110003` result with absent/null data permits fallback;
  arbitrary business errors do not.
- Swagger group labels such as `V1` and `V2` do not identify a DS release.
- Stock `3.2.2` initializes `sql/soft_version` and database version metadata
  to `3.3.0`; this is not an alias for exact `3.2.2`.
- Invalid credentials return HTTP 401 in the reviewed interceptors. Report
  token, expiration and account guidance rather than trying another profile.

When metadata cannot identify the release, generated public-document facts
supply these fixed GET probes relative to the configured API context path:

| Source implementation | Document path | Coverage |
| --- | --- | --- |
| Springfox Swagger 2 through `3.0.6` | `v2/api-docs` | Complete public controller surface |
| Springfox OpenAPI 3 in `3.1.x` | `v3/api-docs?group=v1(current)` and `v3/api-docs?group=v2` | Each complete declared group |
| Springdoc from `3.2.0` | `v3/api-docs` | Complete public controller surface |

The compiler reads all 37 full exact source snapshots before runtime slicing.
Class and method visibility annotations determine public operations. Spring MVC
wire declarations remain separate from Swagger documentation annotations:
reviewed query/form differences, annotation-only parameter names and exposed
server-supplied login-user fields do not change runtime request contracts.
Generated files retain readable shared parameter and operation records,
independent exact memberships and source evidence. Their content digest is a
cache-integrity value, not a compressed matching fingerprint.

`upstream/api_contract_discovery.py` first checks complete route coverage within
the relevant document groups. Missing or extra routes cannot provide complete
coverage for that profile. Remaining `candidate_versions` describe structural
matches only. A singleton still does not establish an exact release or rule
out a custom build.

Operation matching then compares verifiable parameters, locations, scalar and
array item types, requiredness, enums and defaults, accounting for source-backed
documentation declarations. Incomplete nested model/body evidence and parameter
mismatches exclude that operation from `compatible_operations`. Unknown fields
are not proof of compatibility. An operation must match every candidate; one
mismatch can block a dependent action without blocking unrelated reads.

## Reviewed Runtime Scope

`upstream/read_compatibility.py` owns the explicit action policy. It compares
complete transitive wire closures and separately reviewed selectors, response
projections, enum contracts and error semantics across every candidate. Every
operation in the closure, including lookup reads, must also be present in the
observed compatible set. Equal codecs or an HTTP GET method alone do not admit
an action.

The current policy contains 12 business read actions and the identity check in
`doctor`. This is a potential scope, not a promise that all actions are admitted
for every candidate set or document:

| Area | Reviewed actions |
| --- | --- |
| Projects | `project.list`, `project.get` |
| Workflows | `workflow.list`, `workflow.get` |
| Schedules | `schedule.list`, `schedule.get` |
| Workflow instances | `workflow-instance.list`, `workflow-instance.get`, `workflow-instance.digest` |
| Task instances and logs | `task-instance.list`, `task-instance.get`, `task-instance.log` |
| Connection identity | `doctor` current-user check |

The policy excludes writes, authoring, templates, lint, workflow exports and
unreviewed reads. Those operations need explicit exact configuration when the
server remains unidentified. Existing permissions, validation and execution-time
state still apply to admitted reads.

An admitted action uses an existing exact generated profile internally.
`ReadExecutionPolicy` confines transport to the reviewed program fingerprints
and rejects unapproved or mutating requests. The internal execution profile is
not exposed as the server version. Structured results retain `ds_version: null`
in `resolved.target`, candidate and reported-version evidence, an `execution`
value of `read_only`, and a warning whose code is `read_compatibility`. Raw output
keeps its native bytes and uses the normal warning channel.

## Local Discovery and Cache

`schema` and `capabilities` are useful without a warm cache. They describe the
installed CLI catalog and mark exact DS semantics unknown. With cached
candidates they add `read_compatible` or `requires_exact_version` facts. Schema
keeps parser invocation syntax; it does not select a representative release's
YAML models, task templates or version-specific constraints. For example:

```bash
dsctl schema --group workflow
dsctl schema --command workflow.create
dsctl capabilities --action project.list
```

Availability distinguishes local commands (`available_local`), the diagnostic
entry point (`available_diagnostic`), candidate-dependent enum lookup
(`conditional_local`), and a missing observation (`requires_discovery`). Calling
`doctor` is possible before discovery; its authenticated read still needs a
reviewed contract. This avoids requiring an exact version just to learn how
to discover one.

Candidate enum discovery emits only complete enum contracts whose member names,
values and attributes agree across every candidate. It does not publish a union
or choose one candidate's differing members. These are explicitly labeled
`candidate_generated_contracts`, with server membership `unverified`; identical
candidate values do not prove that a customized deployment exposes every member.
For example, a datasource document observed during development omitted `H2`
from the generated candidate's enum. Completely untargeted local
commands retain the documented offline baseline.

The five-minute cache is scoped to normalized URL, API context path and
credential hash. Candidate entries must match the current generated discovery
digest; credentials are not persisted. Remote invocations refresh independently
of the local cache. Successful unresolved observations replace old positive
entries, while probe or network errors clear them. Neither stale evidence nor a
default profile is a fallback after failure.

Probes remain on the configured origin and do not follow redirects or URLs
advertised by a document. One discovery attempt has a 45-second total deadline,
5-second connection bounds, up to 30 seconds for an API-document read, and an
8 MiB response limit. Document parsing accepts at most 4,096 operations and 512
parameters per operation. Authentication and malformed-response errors remain
errors rather than evidence for another profile.

## Verification Boundaries

Regression coverage must distinguish exact identification from candidate
matching, including singleton candidates, complete route coverage, enum
mismatches, missing transitive reads and unreviewed actions. It must also prove
that admitted transport remains read-only, unresolved output does not expose the
execution profile as the server version, and failed probes cannot reuse stale
evidence. Local discovery must preserve parser syntax without borrowing one
candidate's authoring models or differing enum members.

Metadata-path regressions separately cover explicit selection, reviewed route
collisions, misleading version values, invalid credentials, cache expiry and
fresh probe failures. Exercise local cache reuse and fresh `doctor` discovery
independently; one does not establish the other's behavior.

Installed-wheel acceptance must bind each observed action and target to the
tested artifact. The potential scope table above is not a passing-run receipt.
Historical metadata-only observations do not attest contract-read admission.
Exact profile release gates and profile
promotion retain their independent requirements.

## Performance Boundaries

Global schedule-ID lookup on older contracts can require enumerating visible
projects and workflows. Scoped schedule listing avoids that global scan;
performance claims must distinguish these query shapes.

Scoped schema and capability queries currently calculate the complete
compatible read inventory through `compatible_read_actions`. A future scoped
query should evaluate only its requested action; local-only commands can avoid
remote contract compilation. Measure this work separately from CLI cold start
and remote discovery before claiming an improvement.
