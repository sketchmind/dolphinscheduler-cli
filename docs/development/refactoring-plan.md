# Maintainability refactor

This is a historical implementation and measurement record. Its counts,
command spellings and validation results describe the recorded source
baseline. Use [Architecture](architecture.md) for current ownership, the
[CLI contract](../reference/cli-contract.md) for current behavior and
[Roadmap](roadmap.md) for open work. Local candidate progress is not part
of this record.

This refactor starts at `4044131c0e8d9d41054f51e441c3ce45056e5cba`.
Its acceptance criterion is less code to understand and change for one DS
release, with the same exact-version compatibility and CLI behavior.
Moving declarations between files does not count as simplification.

## Baseline

Tracked Python physical lines, including blank lines and comments:

| Ownership | Files | Lines |
| --- | ---: | ---: |
| Handwritten product | 224 | 144,524 |
| Generated product, including task profile data | 1,124 | 100,341 |
| Tests | 373 | 212,175 |
| Tools | 144 | 73,806 |
| Total | 1,865 | 530,846 |

The baseline portable suite passes 13,100 tests; exact source contracts pass
1,040 tests; source rebuilds pass three tests. Fresh generated-package mypy
has 86 existing errors. Current-wheel release evidence is a separate obligation
and cannot be refreshed by a code refactor.

## Delivery batches

| Batch | Change and deletion target | Acceptance |
| --- | --- | --- |
| Runtime and type health | Dispatch prepared requests directly; delete obsolete sessions; correct generator annotations and repeated class scopes | Independent request transcripts, retry/error behavior, fresh generated mypy |
| Command contracts | One catalog for all 174 command invocations; derive Typer and discovery metadata; keep ordinary typed callbacks | Frozen help, flags, defaults, aliases, path constraints and schema comparisons |
| Generation pipeline | Retire empty family/kernel compilation; pool identical generated type nodes without merging exact ownership | Full source closure, reproducible rebuild, content identity and negative isolation tests |
| Task authoring | Co-locate each family's reviewed authoring contract and derive redundant field/schema facts | Exact encode/decode, invalid-input rejection and opaque baseline preservation |
| Workflow boundary and tests | Move exact workflow behavior behind the domain boundary; remove duplicated executable fake behavior | Independent transport expectations and service intent/result tests |
| Evolution and gates | Separate development health from release evidence; shorten entry documentation and document one exact-release update path | CI delegates to the development gate; release checks retain every receipt requirement |

Use high effort for bounded implementation and mechanical migrations after
their seam is validated, xhigh for compatibility-sensitive implementation and
review, and ultra for unresolved architecture or semantic decisions. Keep one
writer per file. The architecture owner integrates shared changes and runs the
complete development gate after focused checks pass.

## Invariants

- REST only; `references/` is read-only upstream evidence.
- All 15 exact versions retain explicit ownership and complete reviewed source
  closures. No nearest-version fallback or inferred semantic membership.
- All 174 actions retain 2,355 supported and 255 upstream-absent coordinates.
- Public invocation and output behavior stays stable; discovered schema
  omissions may be corrected with focused regression evidence.
- `JsonValue` and `JsonObject` remain boundary types. A generic execution DSL
  must not replace readable domain behavior.
- Existing opaque state preserves its provenance restrictions; no new task
  authoring claim follows merely from sharing implementation.
- DS 3.4.1 stays stable. No live receipt or profile promotion is claimed here.

## Completion evidence

Record actual net changes separately for handwritten product, generated
product, tools and tests. Review the removed concepts as well as the line
counts. Run focused tests during independent work, then the complete development
gate on the integrated tree, build a clean wheel, check its contents and import
every exact profile/domain from that wheel. Run release evidence checks and
report their status separately without altering historical receipts.

The refactor is complete when the batches above are integrated and validated,
entry documentation reflects the final architecture, and no obsolete parallel
implementation remains for the migrated responsibilities.

## Delivered structure and measurements

The six batches are integrated. Physical Python lines use the same baseline
scope, including comments and blank lines; new files count against the savings.
Documentation and JSON declarations are excluded from these Python totals.

| Ownership | Before | After | Net deletion |
| --- | ---: | ---: | ---: |
| Handwritten product | 144,524 | 139,402 | 5,122 |
| Generated product | 100,341 | 86,917 | 13,424 |
| Tests | 212,175 | 211,974 | 201 |
| Tools | 73,806 | 72,162 | 1,644 |
| Total | 530,846 | 510,455 | 20,391 |

This is a 3.8% total reduction, including 3.5% of handwritten product and 13.4%
of generated product. Python file count rises from 1,865 to 1,927, primarily
because shared generated enum nodes have independent content-addressed files.
File movement is not counted as a benefit.

- One command catalog replaces parallel Typer/schema invocation declarations
  for 174 actions and 564 inputs. Service calls stay ordinary typed callbacks.
- Direct prepared-request execution replaces generated invocation capture and
  the two old session implementations. The decoder has no transport capability.
- One compiled-domain pipeline replaces the unused wrapper-family/kernel path
  and its obsolete analyzer branches.
- 855 repeated enums share 60 closed definitions. Model classes remain separate
  where identical-looking source has different dependency closures.
- One 42-family model registry and paired typed/native projection bindings
  replace repeated family maps. Model-derived structure removes 351 redundant
  declarations at 194 field sites. Seven ambiguous paths and all semantic
  exceptions remain explicit. This batch's main benefit is fewer edit points;
  concrete callback signatures deliberately limit its line savings.
- Workflow export/describe/digest share an ordered read seam. Domain preparation
  owns native create/update forms; test fakes delegate that preparation instead
  of maintaining their own protocol implementation.

The contributor entry read set falls from 12,297 to 686 lines. The full CLI
contract remains available before changing public behavior, and exact task
rules moved to a dedicated reference without changing their reviewed scope.
These documentation savings are separate from code savings above.

## Final validation

All checks that compose the development gate passed on the integrated tree:
lint, formatting, layout, release-version consistency, explicit-object audit,
seven import-boundary contracts, generated freshness, static conformance,
error-translation governance and type checks. Handwritten/generated-inclusive
mypy checks 1,927 files; the separate fresh generated-package check passes all
1,172 files, clearing the 86 baseline errors without type-check exclusions.

| Validation lane | Result |
| --- | ---: |
| Portable tests | 13,351 passed |
| Exact source contracts | 1,040 passed |
| Complete source rebuilds | 3 passed |
| Minimum-dependency contracts | 233 passed |

The final source-contract and source-rebuild lanes were run independently after
updating obsolete inline-enum layout assertions; actual request validation,
serialization and source-closure assertions remain covered.

All 908 codec request/response validation and serialization schemas, plus their
wire fields, match the frozen baseline. Complete authoring catalog, surface and
model-schema snapshots match all 15 profiles under both Pydantic 2.8 and the
current development dependency set. The 174 parser contracts retain their
independent baseline; nine discovery value labels omitted during migration were
restored with regression coverage.

## Next maintenance goals

The historical [maintainability follow-through](maintainability-follow-through.md)
records the subsequent task-family, workflow, evidence and readability work
from `dfd6068`. The accepted measurements below remain historical snapshots.

The follow-up [maintainability validation](maintainability-validation.md)
records the task evolution exercise, CLI process acceptance, dependency repair
and installed-package validation. The measurements above remain the `94d7a75` snapshot.

1. **Preserve one edit path per responsibility.** A new CLI input starts in the
   catalog; task structural fields start in the selected model; exact wire
   changes start in compiler inputs or the generator. Do not recreate a second
   declaration or handwritten CRUD facade for convenience.
2. **Measure each DS upgrade.** Record changed source decisions, domain recipes,
   typed claims and unsupported coordinates. Regenerate atomically, compare the
   four fingerprint axes and run all development lanes before live campaigns.
3. **Keep further sharing evidence-driven.** Pool model closures only with a
   complete dependency identity and unchanged validation/serialization schemas.
   Do not deduplicate independent expected requests or exact review evidence
   merely to reduce the total line count.
4. **Complete release evidence against one new canonical wheel.** Historical
   conformance fingerprints and exact-read receipt schemas are stale; exact
   3.4.2 lacks a current schema-7 promotion receipt. The three release checks
   continue to reject this state. This refactor neither refreshes receipts nor
   promotes a profile; 3.4.1 remains stable.
5. **Dependency repair completed in the follow-up stage.** The Typer floor is
   now 0.26, whose vendored Click excludes the reproduced import/help failures
   in older Typer with external Click 8.5. Minimum and current CI rows both
   include external Click and preserve independent parser/schema baselines.
   See [Dependency compatibility](dependency-compatibility.md).

## Follow-up architecture convergence

The next audit found residual parallel ownership despite the six delivered
batches: dormant task authoring, repeated model-only field structure, task
reference traversal and task-node mappings, project reads, schema group trees,
empty generated facades, paged service envelopes and receipt value validators.
[Architecture convergence](architecture-convergence.md) records their removal,
the unchanged behavior evidence and measurements against `b58c2e5`. The earlier
completion figures remain historical snapshots, not a claim that all later
maintenance opportunities had been exhausted.
