# Contract Read Discovery

This extension separates an observed API contract from the server's exact
release. It adds bounded read admission when product-version metadata is
unavailable. It does not promote a compatibility profile, authorize authoring,
or replace an exact installed-wheel release gate. DolphinScheduler `3.4.1`
remains the stable target.

## Evidence and Selection

Discovery preserves the existing metadata-first path and seven exact mappings:
OpenAPI identifies `3.2.0` and `3.2.1`; product information identifies `3.3.1`,
`3.3.2`, `3.4.0`, `3.4.1` and `3.4.2`. Explicit `DS_VERSION` is authoritative.
The stock `3.2.2` metadata value `3.3.0` remains an independent reported value,
never an alias for an exact release. Missing metadata and this reviewed anomaly
can continue to document discovery; contradictory or other unreviewed release
reports do not silently select a different profile.

When metadata cannot identify the release, generated public-document facts
supply these fixed GET probes relative to the configured API context path:

| Source implementation | Document path | Coverage |
| --- | --- | --- |
| Springfox Swagger 2 through `3.0.6` | `v2/api-docs` | Complete public controller surface |
| Springfox OpenAPI 3 in `3.1.x` | `v3/api-docs?group=v1(current)` and `v3/api-docs?group=v2` | Each complete declared group |
| Springdoc from `3.2.0` | `v3/api-docs` | Complete public controller surface |

The compiler reads all 36 full exact source snapshots before runtime slicing.
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
For example, the live datasource document omitted `H2` from the generated
candidate's enum. Completely untargeted local
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

## Verification Status

The original [automatic exact version discovery receipt](automatic-version-discovery.md)
records a metadata-only wheel against 15 installed servers on 2026-09-08. Its
seven exact successes and expected older-version rejections remain historical
facts; the receipt does not attest this extension.

During the current development exercise, the real cluster's 133 public routes
retained candidate `1.3.9` without identifying an exact release. Of those
operations, 126 matched verifiable parameter contracts and seven were excluded.
For example, the live datasource enum omitted `H2` present in the clean exact
source. The mismatch was retained rather than weakening enum comparison.
The development campaign passed all 12 business read actions and the identity
check without `DS_VERSION`. Ordinary remote invocations took about 2–3 seconds;
global schedule-ID lookup took about 51 seconds because this older contract
requires enumerating visible projects and workflows. Scoped schedule listing
avoids that global scan.
Action-by-action execution results belong to the campaign evidence, not the
potential scope table above; they remain separate from exact identification.

A clean `0.4.0` wheel was also built and installed into an isolated package
directory on 2026-09-09. Its SHA-256 is
`ee89b47c67da746491e5fb3b928df4c50759eff76280ef422296d02de57eb32a`.
Package-content validation passed. Seven installed-wheel checks passed against
the same target and cache: `doctor`, `project.list`, `version`, local schema,
read and write capability queries, and candidate enum discovery. No explicit
`DS_VERSION` was supplied. Identity stayed unknown, reads were admitted by
contract, and write capability still required an exact version.

One measured performance follow-up remains: scoped schema and capability
queries currently calculate the complete compatible read inventory. In an
isolated local process, the first 13-action calculation took about 760 ms,
compared with 43 ms for `project.list` alone. A future scoped query should
evaluate only its requested action; local-only commands can avoid remote
contract compilation. This is separate from the roughly 600 ms CLI cold start.

The full development gate passed: 17,170 portable tests, 1,482 source-contract
tests and three source-rebuild tests, plus lint, typing, architecture boundaries,
generated freshness and conformance checks. The rebuild lane verifies
byte-identical source and snapshot outputs across all 36 exact releases.

These observations are development and scoped installed-wheel evidence. Exact
profile release gates and any profile promotion retain their independent
requirements; this document makes no new profile promotion claim.
