# Maintainability follow-through

This is a historical implementation and measurement record. Its counts,
command spellings and validation results describe the recorded source
baseline. Use [Architecture](architecture.md) for current ownership, the
[CLI contract](../reference/cli-contract.md) for current behavior and
[Roadmap](roadmap.md) for open work. Local candidate progress is not part
of this record.

This work starts from `dfd6068`, after the accepted
[architecture convergence](architecture-convergence.md). The earlier validation
and wheel remain historical evidence for their own bytes. These changes need a
new development gate and a new installed wheel before live acceptance.

## Acceptance

The goal is to make one task-family or DS-version change easy to locate,
understand and verify. Physical line count is recorded separately; formatting
compression, file movement and speculative abstraction do not count as reduced
maintenance. Keep exact review membership independent of shared wire shapes.
Preserve the 15 profiles, 174 actions, 42 task families and 418 memberships.
DS 3.4.1 remains the stable default.

## Implementation batches

| Batch | Scope | Verification |
| --- | --- | --- |
| Version selection | Preserve the selected version in suggested template commands; remove test-only 3.4.1/3.4.2 wire binders and obsolete catalog property | Conflicting environment regression; independent exact wire expectations |
| Task families | Organize models, authoring catalog, native projection and reviewed surface within their existing layers | Exact schemas, validation, encode/decode, preservation and membership |
| Workflow services | Separate definition reads, creation, editing, execution and lifecycle; group shared compilation helpers; separate runtime-instance responsibilities | Existing intent/result, request, confirmation, no-op and error tests |
| Evidence and release tools | Separate current truth, receipts, assessments and trace validation; use explicit version policies for formerly version-named entry points | Historical schema compatibility, current-wheel binding, negative and cleanup checks |
| Generated readability | Replace unreadable minified/positional profile representation where a clearer representation is practical; keep useful readable sharing | Equivalent semantic records, atomic regeneration, freshness and installed size measurement |
| Names and tests | Improve misleading names alongside ownership changes; split domain fakes and oversized workflow tests; parameterize version-baseline analysis | Dependency reachability, existing assertions and focused checks |
| Integrated acceptance | Update architecture and contributor instructions, complete development gate, build and verify a new canonical wheel, then execute authorized exact live campaigns | Record source identity, wheel digest and fresh receipts separately |

## Design constraints

- Family models stay below services and upstream adaptation. Keep a small,
  explicit registry in each layer; do not introduce dynamic plugin discovery.
- Typed validation, explicit opaque authoring and baseline preservation retain
  their distinct entry conditions. Similar fields do not imply shared semantics.
- Shared workflow helpers have one dependency direction. Public command-facing
  service entry points stay easy to find.
- Keep the ordered command catalog and the `tools/ds_codegen` namespace.
  Generated repeated text is acceptable when it does not duplicate maintained
  facts. Do not add program pools or enum inheritance merely to reduce lines.
- General release machinery must retain every exact policy. An action superset
  alone does not prove equivalent release evidence. Historical receipts remain
  immutable; no refactor refreshes or promotes them.
- Run focused checks per owner and the complete development gate on the
  integrated tree. Re-run expensive lanes only when new changes or findings
  require it.

## Implementation result

The implementation batches are complete. Task-family implementation now lives
in four directional packages; workflow definitions and runtime instances have
separate responsibility modules and shared compilation helpers. Command-facing
service imports remain stable. Handwritten upstream adapters use domain names;
actual task-profile output lives in `src/dsctl/generated/task_profiles.py` and is
produced by both the focused generator and the canonical atomic generator.

Selected-version suggestions retain an explicit `--ds-version`. The obsolete
3.4.1/3.4.2 test binders and catalog passthrough property are removed. Exact gate
entry points accept `--version`; the external-SHELL policy remains explicitly
limited to 3.4.2. The historical `tools/exact_342_evidence.py` schema validator is
retained because its receipt dialect still has a distinct meaning. Historical
adapter-name strings are schema values and remain unchanged after class renames.
Dependency analysis accepts `--baseline-version`; all 15 baselines retain
174/174 action reachability. Missing semantic closures remain explicit,
evidence-backed and non-projectable.

Independent review checked moved function bodies, exact profile equivalence,
historical receipt compatibility and governance coverage. Error-translation
collectors now inspect nested service packages and statically resolve imported
status constants. Their inventory retains all 149 catches, 19 pagination hooks,
57 translators and 170 matrix branches. Five explicit-object debt records moved
with their owning files; the 16 allowed uses and 74 debt entries are unchanged.

## Size and remaining large files

Counts include all Python files under `src`, `tests` and `tools`, compared with
`dfd6068`. They include newly added files and exclude deleted files.

| Former single-file responsibility | Before | Largest module after |
| --- | ---: | ---: |
| Task parameter models | 5,462 | 602 |
| Reviewed task surface | 3,475 | 293 |
| Task encode/decode projection | 11,335 | 735 |
| Task authoring catalog | 9,950 | 752 |
| Workflow definition services | 5,177 | 1,060 |
| Workflow instance services | 3,017 | 1,172 |
| Conformance receipt validation | 4,091 | 1,026 |

These are navigation and ownership measurements, not total-line reductions.

| Category | Before | After | Change |
| --- | ---: | ---: | ---: |
| Handwritten source | 138,775 | 143,873 | +5,098 |
| Generated source | 85,912 | 96,015 | +10,103 |
| Tests | 213,019 | 214,547 | +1,528 |
| Tools | 72,135 | 72,109 | -26 |

This pass improves ownership and readability; it does not reduce physical source
lines. Of the handwritten increase, 4,150 lines are explicit imports, 767 are
blank lines, 12 are docstring lines and 169 are other lines. These categories
describe text, not a measurement of semantic complexity. Readable profile data
adds 40,040 compressed bytes across the two wheel members. Full records for all
15 profiles are equivalent and independently mutable; readable sharing does not
change exact ownership or review membership.

The largest remaining handwritten file is `_command_catalog.py` (4,929 lines),
an ordered declaration retained as one searchable source of command truth.
`services/task_authoring.py` (2,989 lines),
`upstream/legacy_workflow_graph.py` (2,660 lines) and
`upstream/workflows.py` (2,157 lines) remain candidates for a future change that
has a concrete responsibility boundary. Size alone does not justify another
split. `legacy_workflow_graph` describes actual older graph behavior; its name
does not mean the behavior can be deleted.

## Historical acceptance

The implementation was verified at source commit
`97937f5cf61524049e468bc557d5235064248a22`. Its development gate passed
13,434 portable, 1,040 source-contract and three source-rebuild tests, together
with architecture, generated freshness, typing and error-translation checks.
Installed-package validation used isolated minimum dependencies and exercised
all then-current profile/domain bindings, discovery and template-to-lint paths.
These results apply to that baseline and do not establish current release
readiness.

The later [automatic version discovery](automatic-version-discovery.md)
design documents version selection. Current artifact and live acceptance
requirements are owned by [Release](release.md) and
[Live Testing](live-testing.md).
