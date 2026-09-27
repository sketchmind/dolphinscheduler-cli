# Maintainability validation and release preparation

This is a historical stage report. See [Release](release.md) for the durable
candidate validation and current-wheel evidence requirements. Historical stage
results do not establish that a current candidate is releasable.

This stage starts from `94d7a75`, after the six-batch refactor. Its goal is to
verify the cost of a real DS change and the behavior of the installed CLI, then
prepare one immutable wheel for the required exact-version campaigns.

## Workstreams and acceptance

| Workstream | Acceptance |
| --- | --- |
| Dependency compatibility | Declared dependency ranges exclude known broken combinations; minimum/current installations retain all 174 invocation contracts and the selected task schemas |
| DS evolution | Trace a real release change through source evidence, review, authoring and projection; fail on missing implementation ownership; retain all exact capabilities and schema baselines |
| CLI user paths | Exercise configuration, discovery, authoring, preview, execution and errors through real CLI processes against a controlled local REST fixture |
| Terminal behavior | Verify early pipe closure, interrupt exit behavior and noninteractive mutation refusal; provide static shell completion and measure cold-process startup |
| Installed artifact | Run the complete development gate, build in a clean snapshot, compare wheel bytes/metadata to source and exercise the installed entry point |
| Live evidence | Run required exact-version campaigns with that same wheel, validate their receipts and pass the release gate |

DS 3.4.1 remains the stable target. Source-reviewed compatibility, offline
process tests, installed-wheel checks and live receipts retain their separate
meanings. A local HTTP fixture exercises process integration without claiming
real-cluster compatibility.

## Implemented changes

- The Typer floor is `0.26`; its vendored Click excludes the reproduced import
  and argument-help failures in older Typer with external Click 8.5. Minimum
  and known-current dependency rows both include external Click 8.5. See
  [Dependency compatibility](dependency-compatibility.md).
- The real `3.4.1` to `3.4.2` EMR Serverless exercise found that a missing
  authoring builder silently selected generic model-only authoring while
  declaring typed support. Exact reviews and exclusions now require explicit
  registration, including the seven model-only families. Existing 15 schema
  fingerprints and 418 memberships are retained. See
  [Task evolution exercise](task-evolution-exercise.md).
- Root framework options expose static shell completion. The public
  `dsctl --show-completion` command was verified from a persistent interactive
  zsh terminal; automated tests use isolated shell-generation and candidate
  requests. The four catalog globals and 174 actions retain their contracts.
- Real-process tests cover pipe closure and SIGINT using the existing framework
  handling, without adding application-level exception wrappers. A local REST
  journey exposed project read permission errors escaping as `api_result_error`;
  list and get now reuse the existing service translator for `permission_denied`.

## Startup measurements

On macOS 15.7.1 arm64, Python 3.12.4, each command ran in seven fresh processes
with isolated local configuration. Filesystem caches were shared; these are
local observations, not a portable performance threshold.

| Command | Median seconds |
| --- | ---: |
| `python -m dsctl version` | 0.4051 |
| `python -m dsctl --help` | 0.4226 |
| `python -m dsctl schema --command workflow.create` | 0.3995 |

## Code size

The scope matches the original `4044131` baseline: Python files including
comments and blank lines, with `task_profiles_data.py` counted as generated.
Pending new files are included; documentation and JSON declarations are not.

| Ownership | Baseline lines | Current lines | Change |
| --- | ---: | ---: | ---: |
| Handwritten product | 144,524 | 139,421 | -5,103 |
| Generated product | 100,341 | 86,917 | -13,424 |
| Tests | 212,175 | 212,714 | +539 |
| Tools | 73,806 | 72,162 | -1,644 |
| Total | 530,846 | 511,214 | -19,632 |

This acceptance stage adds 19 handwritten product lines and 740 test lines
relative to `94d7a75`; it changes no generated code. The additional tests cover
real process boundaries and the two reproduced correctness gaps.

## Reusable acceptance lessons

- Verify the installed entry point in an isolated environment as well as source
  imports. Exercise completion, process exit channels, pipe closure and signal
  handling where they affect user workflows.
- Treat minimum and current dependency installations as separate evidence;
  keep parser, authoring and request expectations independent of their
  implementations. See [Dependency compatibility](dependency-compatibility.md).
- Compare an artifact with its exact source before live testing. A later code
  or metadata change invalidates that candidate and its evidence. Current
  requirements are maintained in [Release](release.md) and
  [Live Testing](live-testing.md).

The subsequent [architecture convergence](architecture-convergence.md) records
additional ownership changes and measurements. Open work belongs in the
[Roadmap](roadmap.md); local candidate paths, private campaign state and session
authorization are retained only in ignored workspace reports.
