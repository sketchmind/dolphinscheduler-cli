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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

ROOT = Path(__file__).resolve().parents[1]
TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...]
    skip_reason: str | None = None


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
        skip_reason=(
            "codespell is not installed in the active environment; "
            "install dev dependencies or use --skip-codespell"
        ),
    )


def build_steps(
    python: str,
    *,
    include_codespell: bool = True,
    include_pytest: bool = True,
    include_generated_sample: bool = True,
    include_live: bool = False,
) -> list[Step]:
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
            "Explicit Object Audit",
            python_cmd(python, "tools/check_explicit_object.py"),
        ),
        Step("Architecture Boundary Check", lint_imports_cmd(python)),
        Step(
            "Generated Code Freshness",
            python_cmd(python, "tools/check_generated_freshness.py"),
        ),
        Step(
            "Error Translation Governance",
            python_cmd(python, "tools/check_error_translation_governance.py"),
        ),
        Step("Type Check", python_cmd(python, "-m", "mypy", "src", "tests", "tools")),
    ]
    if include_codespell:
        steps.append(codespell_step(python))
    if include_pytest:
        steps.append(
            Step(
                "Run tests",
                python_cmd(python, "-m", "pytest", "-q", "--ignore", "tests/live"),
            )
        )
    if include_live:
        steps.append(
            Step(
                "Run live tests",
                python_cmd(python, "-m", "pytest", "-q", "tests/live"),
            )
        )
    if include_generated_sample:
        steps.extend(
            [
                Step(
                    "Generate Package Sample",
                    python_cmd(
                        python,
                        "tools/generate_ds_contract.py",
                        "--package-output",
                        "build/ds_contract/package_sample",
                    ),
                ),
                Step(
                    "Generated Package Type Check",
                    python_cmd(
                        python,
                        "-m",
                        "mypy",
                        "build/ds_contract/package_sample/generated/versions/ds_3_4_1",
                        "--follow-imports=silent",
                    ),
                ),
            ]
        )
    return steps


def run_step(step: Step) -> int:
    print(f"[quality] {step.name}")
    if step.skip_reason is not None:
        print(f"[quality] skipped: {step.skip_reason}")
        return 0
    print(f"[quality] $ {shlex.join(step.command)}")
    # Commands come from the static quality-gate step table, not user input.
    completed = subprocess.run(step.command, cwd=ROOT, check=False)  # noqa: S603
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
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
        "--skip-generated-sample",
        action="store_true",
        help="skip package sample generation and generated-package type check",
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
    if args.include_live:
        live_errors = validate_live_preconditions(os.environ)
        if live_errors:
            for error in live_errors:
                print(f"[quality] live precondition failed: {error}")
            return 2

    steps = build_steps(
        sys.executable,
        include_codespell=not args.skip_codespell,
        include_pytest=not args.skip_pytest,
        include_generated_sample=not args.skip_generated_sample,
        include_live=args.include_live,
    )

    for step in steps:
        returncode = run_step(step)
        if returncode != 0:
            print(f"[quality] failed: {step.name}")
            return returncode
    print("[quality] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
