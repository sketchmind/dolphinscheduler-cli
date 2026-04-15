# Future Capability Notes

This document is a compact holding area for useful ideas that are not part of
the current CLI contract.

Nothing here is committed product behavior until it appears in
`docs/reference/cli-contract.md` and lands in code.

## Why Keep This File

Several discarded design drafts contained good future-facing ideas, but they
were too large, duplicated current docs, or described unimplemented behavior
as if it already existed.

This file keeps the useful direction without letting it compete with the
current contract.

## Candidate Future Areas

### Workflow authoring

- full workflow YAML create
- patch/edit/apply flows
- dry-run and diff for structural changes
- roundtrip-safe YAML export
- workflow mutation planning should account for current schedule state instead
  of treating schedule as an inline workflow field

### Runtime follow-ups

- richer watch modes (`--until`, event streaming, summarized progress views)
- runtime explain/digest surfaces for large instance payloads
- stronger runtime safety guidance around stop/rerun/recover operations

### Schedule integration

- schedule reconciliation from workflow YAML
- schedule impact analysis before workflow mutation
- additional schedule explain guidance beyond the current pre-mutation
  confirmation analysis

### Diagnostics and AI support

- additional digest views beyond `workflow digest` and `workflow-instance digest`
- broader `explain` surfaces for parameter and execution-context reasoning
- `lint` for design-time checks
- `doctor` for runtime and governance diagnostics
- richer machine-actionable errors and hints

### Self-description


## Design Inputs Worth Preserving

These ideas remain useful even though they are not current contract:

- keep DS-native naming inside projected DS payloads
- keep CLI metadata and DS payload fields conceptually separate
- keep error types machine-classifiable
- prefer dry-run before mutating complex workflow state
- model workflow, schedule, workflow-instance, and task-instance as separate resources
- treat workflow edits as potentially schedule-impacting operations that may
  require offline/online coordination

## Review Rule

Before implementing any future capability:

1. ground the behavior in upstream DS docs or source
2. decide whether it belongs in the stable contract
3. move the final design into `docs/reference/cli-contract.md` and `docs/development/architecture.md`
4. remove or shrink the corresponding note here
