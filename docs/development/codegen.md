# Codegen

`src/dsctl/generated/` contains tracked runtime code produced from upstream
Apache DolphinScheduler source.

## Rules

- Do not hand-edit generated packages.
- Treat `references/` as an optional, ignored local workspace for upstream
  source checkouts.
- Treat upstream source mounted under `references/` as read-only from this
  project's perspective.
- Prefer generator fixes over handwritten DS-facing shapes.
- Keep generated imports inside `dsctl.upstream`.
- Keep `JsonValue` and `JsonObject` at transport and boundary layers.

## Prepare Upstream Source

The installed CLI does not need `references/`. The directory is only needed
when developing the generator, auditing DS-facing behavior, or comparing
upstream DolphinScheduler versions.

Use a local checkout for the default DS source:

```bash
mkdir -p references
git clone https://github.com/apache/dolphinscheduler.git references/dolphinscheduler
git -C references/dolphinscheduler checkout 3.4.1
```

`references/dolphinscheduler` is ignored by git, excluded from distributions,
and checked by package-content gates so it cannot be published accidentally.

For cross-version analysis, keep temporary worktrees under `build/`:

```bash
mkdir -p build/upstream
git -C references/dolphinscheduler fetch --tags origin
git -C references/dolphinscheduler worktree add ../../build/upstream/ds-3.3.2 3.3.2
```

## Generate

```bash
python tools/generate_ds_contract.py --package-output build/ds_contract/package_sample
```

The generator reads the upstream source version from the DolphinScheduler Maven
POM and renders a versioned package under `generated/versions/...`.

## Freshness

Run the freshness check after changing generator logic or tracked generated
runtime code:

```bash
python tools/check_generated_freshness.py
```

## Version Diff

Use the version diff analyzer before adding support for another
DolphinScheduler release:

```bash
python tools/analyze_ds_version_diff.py \
  --ds-source 3.4.1=references/dolphinscheduler \
  --ds-source 3.3.2=build/upstream/ds-3.3.2 \
  --base 3.4.1 \
  --target 3.3.2 \
  --format markdown \
  --output build/ds_contract/diff-3.4.1-to-3.3.2.md
```

Generated reports and snapshots belong under `build/`, not `docs/`, unless a
specific reference document is intentionally promoted and reviewed.

See [Version Compatibility](../user/version-compatibility.md) for the support
policy.
