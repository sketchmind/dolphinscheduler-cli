# Version Compatibility

`DS_VERSION` selects the target DolphinScheduler server version. It defaults to
`3.4.1` and is normalized from common spellings such as `v3.4.1` and
`ds_3_4_1`.

## Current Support Matrix

| Server version | Contract version | Family | Support level | Tested |
| --- | --- | --- | --- | --- |
| `3.4.1` | `3.4.1` | `workflow-3.3-plus` | `full` | yes |
| `3.4.0` | `3.4.1` | `workflow-3.3-plus` | `full` | no |
| `3.3.2` | `3.4.1` | `workflow-3.3-plus` | `full` | no |

`3.3.2` and `3.4.0` currently reuse the `3.4.1` generated contract because
static upstream controller analysis shows no drift in the stable CLI operation
surface. If later work needs endpoints that differ between these versions, the
runtime registry should add an exact generated contract package and adapter.

## Planned Tiers

| Tier | Versions | Policy |
| --- | --- | --- |
| Full support | `3.4.1`, `3.4.0`, `3.3.2`; `3.3.1` planned after runtime enum normalization | Stable CLI surface should work without command-level version branches. |
| Legacy core | `3.2.2` | Adapter translates old `process-*` APIs into the CLI's `workflow` terminology; only core project/workflow/runtime/schedule commands are promised. |
| Unsupported / future analysis | `<=3.2.1`, `3.1.x`, `3.0.x`, `2.x` | Requires separate scope and live validation before support metadata is added. |

## Selection

```bash
export DS_VERSION="3.4.1"
dsctl version
```

The `version` command reports:

- selected server version
- generated contract version
- compatibility family
- support level
- supported server versions

## Compatibility Policy

Services keep stable CLI terms such as `workflow` and `workflow-instance`.
Version-specific names such as `processDefinitionCode` belong in upstream
adapters, not commands or services.

Static contract analysis is not enough to claim support by itself. A version is
considered supported only after its generated package, handwritten adapter,
service-level error translations, and live smoke tests pass for that
DolphinScheduler release.

For developer workflow around contract diffs, see
[Codegen](../development/codegen.md).
