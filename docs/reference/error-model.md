# DolphinScheduler API Error Model Reference

This document is a grounding note, not the CLI contract.

Use it when `dsctl` error translation depends on how DolphinScheduler 3.4.1
actually reports failures.

## Grounding

These notes were grounded in a local checkout of upstream Apache
DolphinScheduler source. During development that checkout is usually mounted at
`references/dolphinscheduler`, but `references/` is ignored, not packaged, and
not required for installed CLI usage.

Representative upstream paths:

- `references/dolphinscheduler/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/enums/Status.java`
- `references/dolphinscheduler/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/utils/Result.java`
- `references/dolphinscheduler/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/exceptions/ServiceException.java`
- `references/dolphinscheduler/dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/exceptions/ApiExceptionHandler.java`

## Upstream Failure Layers

DS API failures do not live in one place. The relevant layers are:

1. HTTP transport status
   Examples include login interception and rate limiting.
2. `Result<T>` envelope
   Most API responses carry `code`, `msg`, and `data`.
3. `Status` enum
   This is the main catalog of DS API result codes.
4. Bare `ServiceException("...")` sites
   Some upstream failures bypass named `Status` constants.

For `dsctl`, this means upstream error translation cannot rely on status codes
alone. HTTP-only failures and string-only failures also exist.

## Main Upstream Catalog

The primary upstream catalog is `Status.java`.

Current source inventory facts:

- it contains hundreds of named API result codes
- many business failures raised through `ServiceException(Status.X)` become
  `Result.code/msg`
- it is the right baseline for translation coverage planning

But it is not a perfect public contract:

- some numeric codes are reused by multiple names
- some failures use bare `ServiceException("...")`
- some failures bypass `Result` and use direct HTTP status control

So the correct design stance is:

- treat `Status.java` as the main inventory
- do not treat it as the only truth

## What `dsctl` Should Translate

`dsctl` should translate at the service boundary, not in transport or output.

The intended split is:

- client/generated/upstream preserve upstream facts
- services map known upstream failures into stable CLI error types
- output serializes the stable CLI envelope

When a service translates one remote error into a stable CLI error type, the
CLI should still preserve the original remote facts separately. The current
error envelope uses `error.source` for that role instead of overloading
`error.details`.

Translation should be keyed by service operation, not by raw status code alone.

Why:

- the same DS code may appear in multiple resource contexts
- duplicate numeric codes exist upstream
- actionable suggestions depend on the attempted operation

In practice, the useful unit is:

- `project.get`
- `user.list`
- `schedule.create`
- `workflow-instance.execute-task`

not just:

- `30001`
- `50003`

## Local Output Contract Failures

`output_contract_error` is a stable local rendering error when an explicitly
declared compact business collection has a broken row path or row shape.
Its suggestion directs the caller to `--format json` to inspect original
fields. It does not represent an upstream DS status or invalid user input.
Dotted `--columns` selectors on compact business lists remain
`user_input_error` and are rejected before service execution. The public
encoding rules are in [JSON layout](cli-contract.md#json-layout).

## Unknown Mutation Outcomes

`mutation_outcome_unknown` is a stable service error for a dispatched write
whose returned upstream result cannot distinguish an earlier rejection from an
already applied mutation. It is narrower than `api_result_error` and does not
assert a client transport failure. The error preserves `error.source`, carries
the known resource scope, and sets `mutation_may_have_applied: true` plus
`request_replay_safe: false`. Its suggestion must name a bounded read or watch
that reconciles the resource and must prohibit blind replay. Structured
`next_actions`, when available, retain the selected context or environment
file. Services may use this type only for an exact operation, release, and
upstream control path supported by source evidence; a generic result code alone
is insufficient.

## Error Envelope Source Facts

The CLI error envelope separates stable CLI semantics from preserved remote
facts.

The intended split is:

- `error.type`, `error.message`, and `error.suggestion` express stable CLI
  meaning
- `error.details` carries command or resource context
- `error.source` carries machine-readable origin facts from the underlying
  remote failure when they are available

For DS remote failures, `error.source` currently uses:

- `kind = "remote"`
- `system = "dolphinscheduler"`
- `layer = "result"` plus `result_code` and `result_message`, or
- `layer = "http"` plus `status_code`

This lets `dsctl` translate known failures into stable CLI categories without
discarding the original DS result code or message.

Implementation rule:

- when translating one caught remote exception into a new CLI error, raise the
  translated error with `from error`
- this preserves the exception chain so `error.source` can still expose the
  original remote facts
- if a remote error is intentionally left raw, return or raise the original
  `ApiResultError` or `ApiHttpError` directly

## Inventory Script

Use the checked-in inventory script to rebuild an upstream baseline. It reads
an exact DolphinScheduler source checkout, which is optional local material and
is not included in this CLI repository or its packages. Prepare the intended
release with the [source workflow](../development/codegen.md), then replace the
path below with that checkout. The script only reads the upstream tree:

```bash
python tools/extract_ds_api_error_inventory.py --ds-source /path/to/exact-ds-checkout --format summary
python tools/extract_ds_api_error_inventory.py --ds-source /path/to/exact-ds-checkout --format json --output build/ds-api-error-inventory.json
python tools/extract_ds_api_error_inventory.py --ds-source /path/to/exact-ds-checkout --format markdown --output build/ds-api-error-inventory.md
```

The script extracts:

- named `Status` entries
- duplicate numeric codes
- bare `ServiceException("...")` sites
- direct `response.setStatus(...)` sites

This is an inventory tool, not a translation matrix. The translation matrix
still belongs in handwritten `dsctl` service logic.

## dsctl Translation Coverage Audit

Use the `dsctl` audit script to inspect current handwritten translation
coverage. This audit reads the CLI source and does not require an upstream
checkout:

```bash
python tools/audit_dsctl_error_translation.py --format summary
python tools/audit_dsctl_error_translation.py --format markdown --output build/dsctl-error-translation-audit.md
```

The audit reports:

- which service modules define translator functions
- which DS code constants those translators handle
- which `except ApiResultError` sites call a translator
- which paginated list surfaces wire `translate_error=...`

This lets us answer a more useful question than "what codes exist upstream":

- which stable `dsctl` operations already normalize upstream failures
- which surfaces still leak raw `ApiResultError`

For reviewed exceptions to the current governance rule, use:

```bash
python tools/check_error_translation_governance.py
```

The current allowlist is intentionally small. New raw `ApiResultError` sites or
new paginated list surfaces without `translate_error` should be treated as
review items, not as silent defaults.

The same governance flow also reviews translated `ApiResultError` sites that
drop the exception chain, because that would discard `error.source` on the
final CLI error payload.

The same governance check also flags matrix branches that recognize a concrete
DS code but still intentionally leave the outcome as raw `ApiResultError`.

Those reviewed raw branches currently fall into two buckets:

- controller-level fallback codes for unexpected upstream failures
- generic operation-failed codes that do not carry a stable domain meaning such
  as not-found, conflict, invalid-state, or permission-denied

## Translation Matrix

Use the matrix extractor when you need a code-level view of current handwritten
translations:

```bash
python tools/extract_dsctl_error_translation_matrix.py --format summary
python tools/extract_dsctl_error_translation_matrix.py --format markdown --output build/dsctl-error-translation-matrix.md
```

The matrix is derived from the actual translator/helper functions in
`src/dsctl/services/`. It is intentionally implementation-oriented:

- module and helper name
- DS code constants or numeric codes used in branch conditions
- returned or raised `dsctl` exception constructors

This is the fastest way to review whether a concrete DS result code already has
stable CLI semantics.

## Review Rule

Before adding or changing `dsctl` error translation:

1. identify the relevant upstream controller/service path
2. confirm whether the failure is HTTP-only, `Result`-wrapped, or string-only
3. translate only known, stable upstream failure patterns
4. leave unknown failures as raw API/HTTP errors until understood
