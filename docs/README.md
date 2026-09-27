# Documentation

Choose the guide for the task at hand. Usage guides explain how to work with
`dsctl`; references define public behavior; developer guides explain ownership,
source review and verification. Examples require the selected DS version and
any stated runtime prerequisites.

## Use the CLI

| Goal | Start here |
| --- | --- |
| Install, upgrade or enable completion | [Installation](user/installation.md) |
| Select a server, credentials, version or named context | [Configuration](user/configuration.md) |
| Find a command and its discovery path | [Commands](user/commands.md) |
| Write or edit workflow YAML and task parameters | [Workflow authoring](user/workflow-authoring.md) |
| Compose parameter, branch, child and dependency examples | [Task examples](user/task-examples.md) |
| Schedule, run and observe workflows | [Runtime operations](user/runtime.md) |
| Investigate failures, recover or compare executions | [Operational investigation](user/operational-investigation.md) |
| Check exact-version scope and support limits | [Version compatibility](user/version-compatibility.md) |

For command syntax, begin with the relevant leaf `--help`. Use
`dsctl schema --command ACTION` for its machine-readable contract and
`dsctl capabilities --action ACTION` for selected-version availability.

## Contribute

Start with [Contributing](../CONTRIBUTING.md), [Architecture](development/architecture.md)
and the [CLI overview](reference/cli-overview.md). Read additional documents when
the change reaches their boundary.

| Change or review | Guide |
| --- | --- |
| Layer ownership and shared maintenance rules | [Architecture](development/architecture.md) |
| Command arguments, output, errors or mutations | [CLI contract](reference/cli-contract.md), [error model](reference/error-model.md), [domain model](reference/domain-model.md) |
| Exact DS contracts and generated runtime | [Codegen](development/codegen.md), [compatibility architecture](development/multi-version-architecture.md) |
| Task authoring, preservation or worker prerequisites | [Reviewed task boundaries](development/task-authoring-boundaries.md), [task evolution example](development/task-evolution-exercise.md) |
| Local checks, helper scripts or dependency floors | [Tooling](development/tooling.md), [dependency compatibility](development/dependency-compatibility.md) |
| Live scenarios, fixtures and cleanup evidence | [Live testing](development/live-testing.md) |
| Candidate validation, packaging and publication | [Release process](development/release.md) |
| Open work and possible future features | [Roadmap](development/roadmap.md) |

## Design and source decisions

These notes explain why boundaries exist. Consult the linked current contracts
for behavior and the roadmap for pending work; a historical source observation
or validation result does not establish acceptance of current code.

| Topic | Decision or evidence |
| --- | --- |
| Exact profiles and compatibility policy | [ADR 0001](development/decisions/0001-multi-version-compatibility.md) |
| Metadata discovery and bounded compatible reads | [Version and read discovery](development/contract-read-discovery.md) |
| Named connection selection and persistence | [Connection context design](development/persistent-connection-context-design.md) |
| CLI usability, output encoding and evaluation | [CLI usability and evaluation](development/cli-ux-convergence.md) |
| Runtime identities, partial writes and uncertain outcomes | [Execution outcomes](development/execution-outcomes.md) |
| Navigation and authorization | [Operation relations](development/frontend-operation-relations.md) |
| Intermediate source differences and release admission | [Source audit](development/intermediate-release-source-audit.md), [admission decisions](development/stable-release-admission.md) |

## Evidence and local material

The governed JSON under `development/live-evidence/` is a versioned input to
receipt checkers, tests and source packages. Each receipt retains its own wheel,
source, scenario and cleanup bindings. Release readiness follows the candidate
validation process, and profile support follows the recorded support policy.

Evidence is organized by scenario, then exact server version:

| Directory | Scope |
| --- | --- |
| `exact-read/<version>/` | Four bounded read actions |
| `conformance-bundles/<version>/` | Named core lifecycle bundles |
| `external-shell/<version>/` | Mutation and restoration of a pre-existing SHELL task; currently reviewed for `3.4.2` |
| `history/` | Superseded or historical admission evidence |

The SHELL scenario uses `3.4.2` as its reviewed test target. Read the
[support policy and verification](user/version-compatibility.md#support-policy-and-verification)
separately from the receipt inventory. Older receipt schemas can remain beside
newer ones for audit; the checker determines which schema can support a current
claim.

Store local campaign progress, raw traces, credentials, private fixtures and
machine settings in ignored workspace directories.
See [documentation contribution rules](../CONTRIBUTING.md#documentation) and
[acceptance closeout](development/release.md#closing-development-acceptance).
