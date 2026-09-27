# Dependency compatibility

Runtime dependency lower bounds are executable compatibility claims. The
`dependency-compatibility` CI lane installs the lower bounds together and runs
the CLI parser/help, request-default and task-authoring contract tests. The
normal Python matrix resolves current allowed dependencies, while the explicit
current row in the dependency lane preserves a reproducible known-modern
comparison.

## Typer and Click boundary

`dolphinscheduler-cli` requires `typer>=0.26,<1` and does not directly depend on
Click. Typer 0.26.0 is the first release that vendors Click; its
[release notes](https://github.com/fastapi/typer/releases/tag/0.26.0) state that
external Click is no longer a dependency and that the earlier temporary Click
upper bound was superseded by vendoring. The tagged
[0.26.0 package metadata](https://github.com/fastapi/typer/blob/0.26.0/pyproject.toml)
also contains no Click requirement.

The lower bound excludes Typer 0.24.1 and 0.25.0. Both import
`get_binary_stream` from `click.utils`; the
[Typer 0.25.0 source](https://github.com/fastapi/typer/blob/0.25.0/typer/__init__.py)
shows that re-export. Click 8.5.0 moved that name behind a deprecated module
re-export in its
[tagged source](https://github.com/pallets/click/blob/8.5.0/src/click/utils.py),
so importing either older Typer release emits `DeprecationWarning`. This project
treats warnings as errors, and the affected Typer/Click combination fails before
the CLI command tree can be built.

The dependency CI lane installs external Click 8.5.0 beside Typer 0.26.0 and
0.27.2 deliberately. Passing both rows proves that the CLI uses Typer's vendored
implementation and that an unrelated package may install modern external Click
without changing the 181-command parser/help contract. The project does not use
external Click classes, plug-ins or custom parameter types; adding one requires
a new compatibility decision because Typer's vendored Click types have separate
class identities.

Structured parser errors use the remaining exception classes from Typer's
private `_click` module. Exit and abort handling uses the public `typer.Exit`
and `typer.Abort` classes. The [0.26.0 exports](https://github.com/fastapi/typer/blob/0.26.0/typer/__init__.py)
alias the original private classes; the [0.27.2 exports](https://github.com/fastapi/typer/blob/0.27.2/typer/__init__.py)
alias the classes moved to `typer.exceptions`. The
[0.27.2 exception refactor](https://github.com/fastapi/typer/releases/tag/0.27.2)
removed the old private `Exit` and `Abort` attributes, so looking them up during
exception handling fails at runtime as well as type checking.

Dependency regressions remove those private aliases and verify standalone exit
codes, abort diagnostics and embedded exception propagation. The process tests
also exercise help, invalid input, interrupts and closed stdout pipes.
Dependency updates must retain these checks; external Click exceptions are not
interchangeable with the vendored classes.
