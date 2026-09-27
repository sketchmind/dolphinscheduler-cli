from __future__ import annotations

import argparse
import importlib.util
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Literal

from check_generated_freshness import CACHE_GENERATED_ROOT
from live_gate.exact_profile_policy import CURRENT_RELEASE_EXACT_GATE_POLICY

if TYPE_CHECKING:
    from collections.abc import Mapping

ROOT = Path(__file__).resolve().parents[1]
TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
FRESH_GENERATED_ROOT = CACHE_GENERATED_ROOT.relative_to(ROOT).as_posix()


@dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...]
    unavailable_reason: str | None = None


def python_cmd(python: str, *args: str) -> tuple[str, ...]:
    return (python, *args)


def truthy_env(name: str, env: Mapping[str, str]) -> bool:
    return env.get(name, "").strip().lower() in TRUTHY_ENV_VALUES


def has_module(module: str) -> bool:
    return importlib.util.find_spec(module) is not None


def validate_live_preconditions(env: Mapping[str, str]) -> list[str]:
    errors: list[str] = []
    if not truthy_env("DSCTL_RUN_LIVE_TESTS", env):
        errors.append("set DSCTL_RUN_LIVE_TESTS=1 before using --include-live")
    if not truthy_env("DSCTL_RUN_LIVE_ADMIN_TESTS", env):
        errors.append("set DSCTL_RUN_LIVE_ADMIN_TESTS=1 before using --include-live")

    admin_env_file = env.get("DS_LIVE_ADMIN_ENV_FILE", "").strip()
    api_url = env.get("DS_LIVE_API_URL", "").strip()
    admin_token = env.get("DS_LIVE_ADMIN_TOKEN", "").strip()
    if admin_env_file != "":
        if not Path(admin_env_file).is_file():
            errors.append("DS_LIVE_ADMIN_ENV_FILE points to a file that does not exist")
    elif api_url == "" or admin_token == "":
        errors.append(
            "configure DS_LIVE_ADMIN_ENV_FILE or both DS_LIVE_API_URL and "
            "DS_LIVE_ADMIN_TOKEN before using --include-live"
        )

    etl_env_file = env.get("DS_LIVE_ETL_ENV_FILE", "").strip()
    if etl_env_file != "" and not Path(etl_env_file).is_file():
        errors.append("DS_LIVE_ETL_ENV_FILE points to a file that does not exist")

    return errors


def lint_imports_cmd(python: str) -> tuple[str, ...]:
    lint_imports = shutil.which("lint-imports")
    if lint_imports is not None:
        return (lint_imports,)
    return python_cmd(python, "-m", "importlinter.cli")


def codespell_step(python: str) -> Step:
    codespell = shutil.which("codespell")
    if codespell is not None:
        return Step(
            "Codespell",
            (codespell, "--toml", "pyproject.toml"),
        )
    if has_module("codespell"):
        return Step(
            "Codespell",
            python_cmd(python, "-m", "codespell", "--toml", "pyproject.toml"),
        )
    return Step(
        "Codespell",
        (),
        unavailable_reason=(
            "codespell is not installed in the active environment; "
            "install dev dependencies or use --skip-codespell"
        ),
    )


def _portable_test_step(python: str) -> Step:
    return Step(
        "Portable Tests",
        python_cmd(
            python,
            "-m",
            "pytest",
            "-m",
            "not live and not source_contract and not source_rebuild",
            "-q",
            "--durations=20",
        ),
    )


def _require_tests_only_options(
    *,
    tests_only: bool,
    portable: bool,
    include_pytest: bool,
    include_live: bool,
) -> None:
    if not tests_only:
        return
    if not portable:
        message = "--tests-only requires --portable"
        raise ValueError(message)
    if not include_pytest:
        message = "--tests-only requires pytest to be enabled"
        raise ValueError(message)
    if include_live:
        message = "--tests-only cannot include live tests"
        raise ValueError(message)


def _test_steps(
    python: str,
    *,
    portable: bool,
    include_pytest: bool,
    include_live: bool,
) -> list[Step]:
    steps: list[Step] = []
    if include_pytest:
        steps.append(_portable_test_step(python))
        if not portable:
            steps.extend(
                [
                    Step(
                        "Source Contract Tests",
                        python_cmd(
                            python,
                            "-m",
                            "pytest",
                            "-m",
                            "source_contract and not live",
                            "-q",
                            "--durations=20",
                        ),
                    ),
                    Step(
                        "Source Rebuild Tests",
                        python_cmd(
                            python,
                            "-m",
                            "pytest",
                            "-m",
                            "source_rebuild and not live",
                            "-q",
                            "--durations=20",
                        ),
                    ),
                ]
            )
    if include_live:
        steps.append(
            Step(
                "Run live tests",
                python_cmd(
                    python, "-m", "pytest", "-q", "--durations=20", "tests/live"
                ),
            )
        )
    return steps


def build_steps(
    python: str,
    *,
    mode: Literal["development", "release"] = "development",
    portable: bool = False,
    tests_only: bool = False,
    include_codespell: bool = True,
    include_pytest: bool = True,
    include_generated_type_check: bool = True,
    include_live: bool = False,
) -> list[Step]:
    if mode not in {"development", "release"}:
        message = f"Unknown quality gate mode: {mode}"
        raise ValueError(message)
    if mode == "release" and (
        portable
        or tests_only
        or not all((include_codespell, include_pytest, include_generated_type_check))
    ):
        message = (
            "--mode release requires every development check; "
            "partial lanes and skip options are not allowed"
        )
        raise ValueError(message)
    _require_tests_only_options(
        tests_only=tests_only,
        portable=portable,
        include_pytest=include_pytest,
        include_live=include_live,
    )
    if tests_only:
        return [_portable_test_step(python)]

    steps = [
        Step(
            "Lint",
            python_cmd(python, "-m", "ruff", "check", "src", "tests", "tools"),
        ),
        Step(
            "Format Check",
            python_cmd(
                python,
                "-m",
                "ruff",
                "format",
                "--check",
                "src",
                "tests",
                "tools",
            ),
        ),
        Step(
            "Project Layout Check",
            python_cmd(python, "tools/check_project_layout.py"),
        ),
        Step(
            "Release Version Consistency",
            python_cmd(python, "tools/check_release_version.py"),
        ),
        Step(
            "Explicit Object Audit",
            python_cmd(python, "tools/check_explicit_object.py"),
        ),
        Step("Architecture Boundary Check", lint_imports_cmd(python)),
    ]
    if not portable:
        steps.append(
            Step(
                "Generated Code Freshness",
                python_cmd(python, "tools/check_generated_freshness.py"),
            )
        )
        if include_generated_type_check:
            steps.append(
                Step(
                    "Generated Package Type Check",
                    python_cmd(
                        python,
                        "-m",
                        "mypy",
                        f"{FRESH_GENERATED_ROOT}/versions",
                        f"{FRESH_GENERATED_ROOT}/wire_programs",
                        f"{FRESH_GENERATED_ROOT}/wire_runtime",
                        "--follow-imports=silent",
                    ),
                )
            )
        steps.append(
            Step(
                "Static Conformance Bundle Assessment",
                python_cmd(
                    python,
                    "tools/analyze_ds_conformance_bundles.py",
                    "--check-generated",
                    "--require-assessed",
                    "--output",
                    "build/ds_contract/conformance-assessment.json",
                ),
            )
        )
    steps.extend(
        [
            Step(
                "Error Translation Governance",
                python_cmd(python, "tools/check_error_translation_governance.py"),
            ),
            Step(
                "Type Check",
                python_cmd(python, "-m", "mypy", "src", "tests", "tools"),
            ),
        ]
    )
    if include_codespell:
        steps.append(codespell_step(python))
    steps.extend(
        _test_steps(
            python,
            portable=portable,
            include_pytest=include_pytest,
            include_live=False,
        )
    )
    if mode == "release":
        steps.extend(
            [
                Step(
                    "Conformance Bundle Evidence",
                    python_cmd(python, "tools/check_conformance_bundle_evidence.py"),
                ),
                Step(
                    "Exact-Profile Read Evidence",
                    python_cmd(python, "tools/check_exact_profile_read_evidence.py"),
                ),
                Step(
                    f"Exact {CURRENT_RELEASE_EXACT_GATE_POLICY.ds_version} "
                    "Promotion Evidence",
                    python_cmd(
                        python,
                        "tools/check_exact_profile_promotion_evidence.py",
                        "--version",
                        CURRENT_RELEASE_EXACT_GATE_POLICY.ds_version,
                    ),
                ),
            ]
        )
    if include_live:
        steps.extend(
            _test_steps(
                python, portable=portable, include_pytest=False, include_live=True
            )
        )
    return steps


def run_step(step: Step) -> int:
    started = perf_counter()
    print(f"[quality] {step.name}", flush=True)
    try:
        if step.unavailable_reason is not None:
            print(f"[quality] unavailable: {step.unavailable_reason}")
            return 2
        print(f"[quality] $ {shlex.join(step.command)}", flush=True)
        # Commands come from the static quality-gate step table, not user input.
        completed = subprocess.run(step.command, cwd=ROOT, check=False)  # noqa: S603
        return completed.returncode
    finally:
        elapsed = perf_counter() - started
        print(f"[quality] elapsed: {step.name}: {elapsed:.2f}s", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check development health or release readiness."
    )
    parser.add_argument(
        "--mode",
        choices=("development", "release"),
        default="development",
        help=(
            "development (default) checks code and exact source contracts; release "
            "additionally requires all current-wheel receipts and forbids "
            "partial checks"
        ),
    )
    parser.add_argument(
        "--portable",
        action="store_true",
        help=(
            "run the clean-checkout, cross-Python compatibility lane; omit exact "
            "source/evidence audits, source-contract/rebuild tests, and generated "
            "runtime freshness/type checks"
        ),
    )
    parser.add_argument(
        "--tests-only",
        action="store_true",
        help=(
            "with --portable, run only the cross-Python pytest lane; intended "
            "for additional CI interpreter versions after the full gate passes"
        ),
    )
    parser.add_argument(
        "--skip-codespell",
        action="store_true",
        help="skip the codespell pass",
    )
    parser.add_argument(
        "--skip-pytest",
        action="store_true",
        help="skip the repository pytest suite",
    )
    parser.add_argument(
        "--skip-generated-type-check",
        "--skip-generated-sample",
        dest="skip_generated_type_check",
        action="store_true",
        help=(
            "skip the generated-package type check; --skip-generated-sample is "
            "retained as a compatibility alias"
        ),
    )
    parser.add_argument(
        "--include-live",
        action="store_true",
        help=(
            "append the destructive real-cluster live suite; requires the "
            "normal live-test environment variables"
        ),
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        steps = build_steps(
            sys.executable,
            mode=args.mode,
            portable=args.portable,
            tests_only=args.tests_only,
            include_codespell=not args.skip_codespell,
            include_pytest=not args.skip_pytest,
            include_generated_type_check=not args.skip_generated_type_check,
            include_live=args.include_live,
        )
    except ValueError as option_error:
        parser.error(str(option_error))
    if args.include_live:
        live_errors = validate_live_preconditions(os.environ)
        if live_errors:
            for error in live_errors:
                print(f"[quality] live precondition failed: {error}")
            return 2

    for step in steps:
        returncode = run_step(step)
        if returncode != 0:
            print(f"[quality] failed: {step.name}")
            return returncode
    if args.portable:
        result = "portable tests" if args.tests_only else "portable checks"
    elif args.skip_codespell or args.skip_pytest or args.skip_generated_type_check:
        result = "partial development checks"
    else:
        result = f"{args.mode} gate"
    print(f"[quality] {result} passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
