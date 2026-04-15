from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("check_quality_gate")


def test_build_steps_matches_ci_shape() -> None:
    quality = _load_module()

    steps = quality.build_steps("python")

    assert [step.name for step in steps] == [
        "Lint",
        "Format Check",
        "Project Layout Check",
        "Explicit Object Audit",
        "Architecture Boundary Check",
        "Generated Code Freshness",
        "Error Translation Governance",
        "Type Check",
        "Codespell",
        "Run tests",
        "Generate Package Sample",
        "Generated Package Type Check",
    ]
    run_tests_step = next(step for step in steps if step.name == "Run tests")
    assert run_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-q",
        "--ignore",
        "tests/live",
    )


def test_build_steps_honors_skip_flags() -> None:
    quality = _load_module()

    steps = quality.build_steps(
        "python",
        include_codespell=False,
        include_pytest=False,
        include_generated_sample=False,
    )

    assert [step.name for step in steps] == [
        "Lint",
        "Format Check",
        "Project Layout Check",
        "Explicit Object Audit",
        "Architecture Boundary Check",
        "Generated Code Freshness",
        "Error Translation Governance",
        "Type Check",
    ]


def test_build_steps_can_append_live_suite() -> None:
    quality = _load_module()

    steps = quality.build_steps("python", include_live=True)

    assert [step.name for step in steps] == [
        "Lint",
        "Format Check",
        "Project Layout Check",
        "Explicit Object Audit",
        "Architecture Boundary Check",
        "Generated Code Freshness",
        "Error Translation Governance",
        "Type Check",
        "Codespell",
        "Run tests",
        "Run live tests",
        "Generate Package Sample",
        "Generated Package Type Check",
    ]
    run_tests_step = next(step for step in steps if step.name == "Run tests")
    live_tests_step = next(step for step in steps if step.name == "Run live tests")
    assert run_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-q",
        "--ignore",
        "tests/live",
    )
    assert live_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-q",
        "tests/live",
    )


def test_validate_live_preconditions_requires_flags_and_admin_profile() -> None:
    quality = _load_module()

    errors = quality.validate_live_preconditions({})

    assert errors == [
        "set DSCTL_RUN_LIVE_TESTS=1 before using --include-live",
        "set DSCTL_RUN_LIVE_ADMIN_TESTS=1 before using --include-live",
        (
            "configure DS_LIVE_ADMIN_ENV_FILE or both DS_LIVE_API_URL and "
            "DS_LIVE_ADMIN_TOKEN before using --include-live"
        ),
    ]


def test_validate_live_preconditions_accepts_direct_admin_env() -> None:
    quality = _load_module()

    errors = quality.validate_live_preconditions(
        {
            "DSCTL_RUN_LIVE_TESTS": "1",
            "DSCTL_RUN_LIVE_ADMIN_TESTS": "true",
            "DS_LIVE_API_URL": "http://example.test/dolphinscheduler",
            "DS_LIVE_ADMIN_TOKEN": "secret",
        }
    )

    assert errors == []


def test_validate_live_preconditions_accepts_existing_env_files(
    tmp_path: Path,
) -> None:
    quality = _load_module()
    admin_env_file = tmp_path / "admin.env"
    etl_env_file = tmp_path / "etl.env"
    admin_env_file.write_text("DS_API_URL=http://example\n", encoding="utf-8")
    etl_env_file.write_text("DS_API_URL=http://example\n", encoding="utf-8")

    errors = quality.validate_live_preconditions(
        {
            "DSCTL_RUN_LIVE_TESTS": "1",
            "DSCTL_RUN_LIVE_ADMIN_TESTS": "1",
            "DS_LIVE_ADMIN_ENV_FILE": str(admin_env_file),
            "DS_LIVE_ETL_ENV_FILE": str(etl_env_file),
        }
    )

    assert errors == []


def test_validate_live_preconditions_rejects_missing_env_files() -> None:
    quality = _load_module()
    missing_admin_env = str(Path("/missing-admin.env"))
    missing_etl_env = str(Path("/missing-etl.env"))

    errors = quality.validate_live_preconditions(
        {
            "DSCTL_RUN_LIVE_TESTS": "1",
            "DSCTL_RUN_LIVE_ADMIN_TESTS": "1",
            "DS_LIVE_ADMIN_ENV_FILE": missing_admin_env,
            "DS_LIVE_ETL_ENV_FILE": missing_etl_env,
        }
    )

    assert errors == [
        "DS_LIVE_ADMIN_ENV_FILE points to a file that does not exist",
        "DS_LIVE_ETL_ENV_FILE points to a file that does not exist",
    ]
