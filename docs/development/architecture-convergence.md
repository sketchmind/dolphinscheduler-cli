# Architecture convergence

This is a historical implementation and measurement record. Its counts,
command spellings and validation results describe the recorded source
baseline. Use [Architecture](architecture.md) for current ownership, the
[CLI contract](../reference/cli-contract.md) for current behavior and
[Roadmap](roadmap.md) for open work. Local candidate progress is not part
of this record.

This pass starts at `b58c2e5`, following the maintainability acceptance stage.
It closes the residual duplicate implementations identified by the next audit.
Compatibility remains 15 exact releases, 174 actions, 2,355 supported and 255
upstream-absent coordinates, with 42 reviewed families and 418 memberships.
DS 3.4.1 remains stable.

## Responsibilities consolidated

| Responsibility | Previous maintenance burden | Current owner |
| --- | --- | --- |
| Authoring structure | Dormant DEPENDENT builder, a second field descriptor, seven parallel model-only field declarations | Reviewed catalog and model-derived `TaskAuthoringField` structure |
| Local task references | Separate graph and rename walkers plus repeated projection member names | `upstream/task_references.py`; exact validation and native value encoding remain in projectors |
| Task-node settings | Native names repeated across encoding, export, discovery and preservation | `upstream/task_settings.py`; independent preservation allowlist and timeout coupling |
| Project reads | Separate compiled list/get execution and identity projection in read and lifecycle adapters | `CompiledProjectReads`, with explicit validation/error boundaries and shared generated-domain paging helpers |
| Schema hierarchy | Five modules manually enumerate 29 command groups | Catalog routes plus group descriptions and domain overrides |
| Paged results | Repeated pagination calls and resolved-context envelopes | `services/_page_result.py`; serializers and error policies stay domain-owned |
| Generated runtime | Empty client/model/operation facades in every exact slice | Generator emits only retained enums and identity artifacts for empty slices |
| Receipt values | Repeated primitive validators in independent evidence gates | Shared value parsing; each gate retains its receipt schema and acceptance rules |

The old implementations are deleted. This pass introduces neither a general
CRUD framework nor a new request execution path. Complete standalone source
contracts remain reproducible audit artifacts; their full package generation is
separate from the installed runtime slices. The retired internal `DSxxxClient`
imports have no product call sites; consumers of standalone audit packages still
receive their complete generated clients.

A task-node evolution test changes only the `delay` binding and observes the
change in real encoding, export, public discovery and authorized overlay.
Encoding support does not grant overlay permission: `cpu_quota` remains outside
the exact 2.0.0 overlay allowlist. Model-only field evolution similarly follows
the selected model without another structural declaration.

Reference sharing preserves consumer differences. Graph construction excludes
runtime `nextBranch`, while the existing SWITCH patch policy can rename it.
Decimal-looking canonical task names remain names. Malformed opaque rows keep
their existing per-family handling. Reference discovery does not authorize typed
input, and the exact projectors retain validation order and wire epochs.

## Measurement

Physical Python lines include comments, blank lines and new files. Generated
profile data is counted as generated product; documentation and JSON are outside
these totals. This pass is measured against `b58c2e5`; cumulative savings use the
original `4044131` baseline.

| Ownership | Before | After | This pass | Since original baseline |
| --- | ---: | ---: | ---: | ---: |
| Handwritten product | 139,421 | 138,775 | -646 | -5,749 |
| Generated product | 86,917 | 85,912 | -1,005 | -14,429 |
| Tests | 212,714 | 213,019 | +305 | +844 |
| Tools | 72,162 | 72,135 | -27 | -1,671 |
| Total | 511,214 | 509,841 | -1,373 | -21,005 |

Python file count falls from 1,929 to 1,871. New helpers and regression tests
count against the savings. Task-node bindings add code locally to remove four
independent edit points; Project read validation retains two explicit policies
because their validation order and error contracts differ. Neither is described
as a local line-count reduction. The totals reflect the complete integrated pass.
The existing error-translation audit now recognizes both paging entry points;
missing or explicitly null translators remain governed by the same allowlist.
Stable-action dependency extraction uses the exact manifest to recognize empty
runtime slices, while nonempty slices and standalone source audits retain their
strict client/operation checks.

## Validation

- Complete task schema and catalog snapshots remain equal for all 15 versions
  under both current Pydantic and the declared 2.8 floor.
- 769 frozen task-node cases preserve raw wire JSON order, YAML bytes and overlay
  results under both dependency sets.
- 720 task-reference projection outcomes, including failures, and 48 rename
  outcomes match the frozen pre-change implementation across all 15 releases.
- 3,320 Project record/get/list outcomes preserve requests, validation order,
  nullable metadata and complete diagnostics, including exception causes.
- Generated manifests, exact artifact identities and enum definitions remain
  unchanged; generation retires 60 unused facade files and empties 15 package
  initializers, deleting 1,005 generated Python lines.

The complete development gate passes: 13,380 portable tests, 1,040 upstream
source-contract tests and three complete source-rebuild tests (14,423 total).
All seven architecture contracts, generated freshness, static conformance,
error-translation governance, formatting and spelling checks pass. Type checking
covers 1,112 generated files and 1,871 source files. The clean installed-wheel
checks below also pass.

Live campaigns remain separate: changed runtime bytes supersede the previous
wheel and cannot reuse its receipts. No receipt, compatibility promotion or live
result is produced by this convergence work.
