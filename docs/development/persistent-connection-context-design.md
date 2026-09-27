# Named Connection Contexts: Implementation and Acceptance

This document records the implemented local configuration contract and its
acceptance criteria. Public syntax and output fields are maintained in the
[CLI Contract](../reference/cli-contract.md#dsctl-context); setup instructions
are in [Configuration](../user/configuration.md).

## Product Contract

A user registers a complete caller-managed env file under a context name,
optionally binds a project, and chooses a saved default. A fresh shell can reuse
that choice. Independent tasks sharing a working directory choose different
names through `DSCTL_CONTEXT`; temporary invocations use `--context` or
`--env-file`. A command selects one whole source and retains one snapshot.

Connection precedence is explicit selector, process selector, any present
process `DS_API_URL`/`DS_API_TOKEN`/`DS_VERSION`, saved user default, then
unconfigured local operation. Execution policies resolve independently per key:
process, selected profile, then built-in default. Same-level
selectors conflict. Empty or incomplete selected inputs remain selected and
cannot trigger a fallback to another target. Version-only input supports
offline work while remote work requires a URL and token.

One registry at `$XDG_CONFIG_HOME/dsctl/config.yaml`, otherwise
`~/.config/dsctl/config.yaml`, contains named entries and `default_context`.
Each entry stores an absolute logical env-file path, normalized bound API URL
and optional project. No token is copied. Project defaults belong only to a
selected named context. Workflows require explicit command or file selectors.
No directory search or legacy `.dsctl-context.yaml` / `dsctl/context.yaml`
fallback remains. The obsolete `use` interface is removed.

Token rotation at the bound URL retains project selection. An external URL
change fails selection until `context update NAME --file FILE` explicitly
rebinds the entry. Every file update clears the previous project unless the
same update supplies a new project. Registry read-modify-write operations are
serialized and atomic; locks are released before any remote operation.

Local registry commands remain usable independently of effective connection
readiness. List/get inspect saved records without opening referenced files.
Changing the saved default reports both saved and effective choices, including
a structured warning when a successful write has failed effective readback.
Deleting the default context requires unsetting that default first.

## Ownership

Foundation modules own typed registry records, dotenv parsing and persistence.
The runtime resolves one target snapshot used by services, version discovery,
navigation and output. The command catalog owns syntax, help, schema, local
mutation classification and constraints. Registry operations require no DS
controller or wire changes; compatibility inventory changes cover local actions
only and do not change profile support policy.

## Acceptance Journeys

Start from an empty isolated user configuration. Exercise the real CLI through
separate processes and inspect its public JSON, exit status, stderr and stored
files. Judge correct target/project selection before command count or output
size. Preconfigured wrappers alone do not prove setup usability.

| Journey | Required observation |
| --- | --- |
| Register A with project, choose default, start a fresh process | A and its project are effective without repeating the env-file path |
| Two tasks at the same directory supply different `DSCTL_CONTEXT` values | Each process resolves its own target/project; neither rewrites the saved default |
| Process connection values exist while setting the default | Saved value changes; effective target remains the process target with no saved project |
| Only `DS_VERSION`, or an empty identity key, exists beside a saved default | Offline context reflects process selection; remote work fails for missing connection rather than using the default |
| Only retry/timeout policy is set beside a saved default | The default connection and project remain selected; the process policy applies |
| Explicit selector is supplied above process selectors | Explicit whole source wins; no inherited project/version/token is merged |
| Both selectors are supplied at one level | Usage/configuration error, with no implicit ranking |
| Current directory and old user path contain stale scoped files | Named/default selection is unchanged and legacy files remain untouched |
| Selected context or env file is missing, malformed or incomplete | Stable error and actionable repair; no other target is selected |
| Token rotates at the same URL | Next invocation uses the source; project remains and output/registry contain no credential |
| File URL changes externally, then update explicitly rebinds | Selection first fails; successful rebind clears old project unless replaced |
| Default entry is deleted | Rejected until explicit default unset; other entries remain |
| Local create/update/list/get/delete/config/context operations | No DS request; saved and effective state are distinguishable |

`tests/commands/test_context_journeys.py` covers fresh-process setup, task
isolation, overrides, incomplete inputs, stale files, endpoint rebinding and
credential exclusion. Focused foundation and command tests cover file errors,
locking, schema validation, conflicting inputs and zero-network behavior.
Runtime tests cover shared snapshot and navigation behavior. Test existence is
separate from passing-run evidence. A remote read smoke, if
performed, is separate evidence and requires an explicitly selected authorized
target.

## Delivery Criteria

Help, schema, navigation, user docs and the distributed skill must teach the
same selection model. Search current docs and code for retired workflow-default
and `use` references, retaining historical release notes where appropriate.
Regenerate catalog-derived artifacts atomically; keep historical receipts
unchanged. Pass focused behavioral tests and the development quality gate.

## Validation Boundaries

Registry acceptance includes concurrent writers and consistent read snapshots,
fresh shells, independent task selectors, token rotation, endpoint rebinding,
malformed paths, and successful default writes with failed effective readback.
Transport acceptance verifies that pagination retains the original URL and
token even if the default, connection file or symlink changes during execution.
Templates and result navigation retain named contexts or explicit file paths.

Process tests use synthetic credentials and loopback REST servers. Loopback
coverage establishes process and transport behavior; it does not establish
Windows file-lock behavior unless run on Windows. Development tests also do
not replace installed-wheel or real-cluster acceptance. Record those results
against their actual platform, target and artifact under the
[release process](release.md).
