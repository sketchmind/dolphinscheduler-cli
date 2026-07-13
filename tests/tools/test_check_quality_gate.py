from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Protocol

    import pytest

    class _NamedStep(Protocol):
        name: str


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
        "Release Version Consistency",
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
        "-m",
        "not live",
        "-q",
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
        "Release Version Consistency",
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
        "Release Version Consistency",
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
        "-m",
        "not live",
        "-q",
    )
    assert live_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-q",
        "tests/live",
    )


def test_main_fails_when_codespell_is_unavailable_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    original_run_step = quality.run_step

    monkeypatch.setattr(quality.shutil, "which", lambda _command: None)
    monkeypatch.setattr(
        quality,
        "has_module",
        lambda module: module != "codespell",
    )

    def run_without_external_commands(step: _NamedStep) -> int:
        if step.name == "Codespell":
            return int(original_run_step(step))
        return 0

    monkeypatch.setattr(quality, "run_step", run_without_external_commands)

    returncode = quality.main(["--skip-pytest", "--skip-generated-sample"])

    assert returncode == 2
    output = capsys.readouterr().out
    assert (
        "codespell is not installed in the active environment; "
        "install dev dependencies or use --skip-codespell"
    ) in output
    assert "[quality] failed: Codespell" in output
    assert "[quality] all checks passed" not in output


def test_main_allows_codespell_to_be_skipped_explicitly(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    executed_steps: list[str] = []

    def pass_external_steps(step: _NamedStep) -> int:
        executed_steps.append(step.name)
        return 0

    monkeypatch.setattr(quality, "run_step", pass_external_steps)

    returncode = quality.main(
        ["--skip-codespell", "--skip-pytest", "--skip-generated-sample"]
    )

    assert returncode == 0
    assert "Codespell" not in executed_steps
    assert "[quality] all checks passed" in capsys.readouterr().out


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
