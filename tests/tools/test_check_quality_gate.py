from __future__ import annotations

import importlib
import sys
from pathlib import Path
from subprocess import CompletedProcess
from typing import TYPE_CHECKING

import pytest
import yaml

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Protocol

    class _NamedStep(Protocol):
        name: str
        command: tuple[str, ...]


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module() -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module("check_quality_gate")


def test_release_gate_adds_all_receipt_checks_after_complete_development_gate() -> None:
    quality = _load_module()
    development = quality.build_steps("python")
    release = quality.build_steps("python", mode="release")

    assert release[: len(development)] == development
    assert [(step.name, step.command) for step in release[len(development) :]] == [
        (
            "Conformance Bundle Evidence",
            ("python", "tools/check_conformance_bundle_evidence.py"),
        ),
        (
            "Exact-Profile Read Evidence",
            ("python", "tools/check_exact_profile_read_evidence.py"),
        ),
        (
            "Exact 3.4.2 Promotion Evidence",
            (
                "python",
                "tools/check_exact_profile_promotion_evidence.py",
                "--version",
                "3.4.2",
            ),
        ),
    ]


@pytest.mark.parametrize(
    ("flag", "build_option"),
    [
        ("--portable", {"portable": True}),
        ("--tests-only", {"tests_only": True}),
        ("--skip-codespell", {"include_codespell": False}),
        ("--skip-pytest", {"include_pytest": False}),
        ("--skip-generated-type-check", {"include_generated_type_check": False}),
        ("--skip-generated-sample", {"include_generated_type_check": False}),
    ],
)
def test_release_gate_rejects_partial_checks_before_running_commands(
    flag: str,
    build_option: dict[str, bool],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality = _load_module()

    def unexpected_step(_step: _NamedStep) -> int:
        pytest.fail("release gate executed an incomplete check sequence")

    monkeypatch.setattr(quality, "run_step", unexpected_step)
    with pytest.raises(ValueError, match="requires every development check"):
        quality.build_steps("python", mode="release", **build_option)
    with pytest.raises(SystemExit, match="2"):
        quality.main(["--mode", "release", flag])


@pytest.mark.parametrize(
    "flag",
    [
        "--allow-missing-conformance-bundle-evidence",
        "--skip-exact-profile-read-evidence",
        "--allow-missing-exact-342-promotion-receipt",
    ],
)
def test_retired_evidence_allowances_cannot_bypass_release_gate(flag: str) -> None:
    quality = _load_module()
    with pytest.raises(SystemExit, match="2"):
        quality.main(["--mode", "release", flag])


@pytest.mark.parametrize("mode", ["development", "release"])
def test_gate_result_names_the_completed_obligation(
    mode: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    monkeypatch.setattr(quality, "run_step", lambda _step: 0)
    assert quality.main(["--mode", mode]) == 0
    assert capsys.readouterr().out == f"[quality] {mode} gate passed\n"


@pytest.mark.parametrize(
    "failing_step",
    [
        "Conformance Bundle Evidence",
        "Exact-Profile Read Evidence",
        "Exact 3.4.2 Promotion Evidence",
    ],
)
def test_any_receipt_failure_prevents_release_success(
    failing_step: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    monkeypatch.setattr(
        quality, "run_step", lambda step: 1 if step.name == failing_step else 0
    )
    assert quality.main(["--mode", "release"]) == 1
    output = capsys.readouterr().out
    assert f"[quality] failed: {failing_step}" in output
    assert "gate passed" not in output


@pytest.mark.parametrize("returncode", [0, 7])
def test_run_step_reports_elapsed_time_and_preserves_exit_status(
    returncode: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    step = quality.Step("Example check", ("python", "check.py"))
    times = iter((10.0, 12.375))
    monkeypatch.setattr(quality, "perf_counter", lambda: next(times))

    def run(
        command: tuple[str, ...], *, cwd: Path, check: bool
    ) -> CompletedProcess[str]:
        assert command == step.command
        assert cwd == quality.ROOT
        assert check is False
        return CompletedProcess(command, returncode)

    monkeypatch.setattr(quality.subprocess, "run", run)

    assert quality.run_step(step) == returncode
    assert capsys.readouterr().out == (
        "[quality] Example check\n"
        "[quality] $ python check.py\n"
        "[quality] elapsed: Example check: 2.38s\n"
    )


def test_run_step_reports_elapsed_time_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()
    times = iter((10.0, 10.25))
    monkeypatch.setattr(quality, "perf_counter", lambda: next(times))
    step = quality.Step("Missing check", (), unavailable_reason="tool is missing")

    assert quality.run_step(step) == 2
    assert capsys.readouterr().out == (
        "[quality] Missing check\n"
        "[quality] unavailable: tool is missing\n"
        "[quality] elapsed: Missing check: 0.25s\n"
    )


def test_build_steps_defines_development_gate() -> None:
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
        "Generated Package Type Check",
        "Static Conformance Bundle Assessment",
        "Error Translation Governance",
        "Type Check",
        "Codespell",
        "Portable Tests",
        "Source Contract Tests",
        "Source Rebuild Tests",
    ]
    portable_tests_step = next(step for step in steps if step.name == "Portable Tests")
    assert portable_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "not live and not source_contract and not source_rebuild",
        "-q",
        "--durations=20",
    )
    source_contract_tests_step = next(
        step for step in steps if step.name == "Source Contract Tests"
    )
    assert source_contract_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "source_contract and not live",
        "-q",
        "--durations=20",
    )
    source_rebuild_tests_step = next(
        step for step in steps if step.name == "Source Rebuild Tests"
    )
    assert source_rebuild_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "source_rebuild and not live",
        "-q",
        "--durations=20",
    )
    conformance_step = next(
        step for step in steps if step.name == "Static Conformance Bundle Assessment"
    )
    assert conformance_step.command == (
        "python",
        "tools/analyze_ds_conformance_bundles.py",
        "--check-generated",
        "--require-assessed",
        "--output",
        "build/ds_contract/conformance-assessment.json",
    )
    generated_type_step = next(
        step for step in steps if step.name == "Generated Package Type Check"
    )
    assert generated_type_step.command == (
        "python",
        "-m",
        "mypy",
        "build/ds_contract/.freshness_cache/fresh/generated/versions",
        "build/ds_contract/.freshness_cache/fresh/generated/wire_programs",
        "build/ds_contract/.freshness_cache/fresh/generated/wire_runtime",
        "--follow-imports=silent",
    )
    freshness_index = next(
        index
        for index, step in enumerate(steps)
        if step.name == "Generated Code Freshness"
    )
    assert steps[freshness_index + 1] is generated_type_step
    assert all(
        "tools/generate_ds_runtime_bundles.py" not in step.command for step in steps
    )


def test_build_steps_defines_clean_checkout_portable_gate() -> None:
    quality = _load_module()

    steps = quality.build_steps("python", portable=True)

    assert [step.name for step in steps] == [
        "Lint",
        "Format Check",
        "Project Layout Check",
        "Release Version Consistency",
        "Explicit Object Audit",
        "Architecture Boundary Check",
        "Error Translation Governance",
        "Type Check",
        "Codespell",
        "Portable Tests",
    ]
    assert next(step for step in steps if step.name == "Portable Tests").command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "not live and not source_contract and not source_rebuild",
        "-q",
        "--durations=20",
    )
    assert "Generated Package Type Check" not in {step.name for step in steps}


def test_build_steps_cannot_enable_generated_type_check_in_portable_mode() -> None:
    quality = _load_module()

    steps = quality.build_steps(
        "python",
        portable=True,
        include_generated_type_check=True,
    )

    assert "Generated Code Freshness" not in {step.name for step in steps}
    assert "Generated Package Type Check" not in {step.name for step in steps}


def test_build_steps_defines_cross_python_portable_tests_only_lane() -> None:
    quality = _load_module()

    steps = quality.build_steps("python", portable=True, tests_only=True)

    assert [(step.name, step.command) for step in steps] == [
        (
            "Portable Tests",
            (
                "python",
                "-m",
                "pytest",
                "-m",
                "not live and not source_contract and not source_rebuild",
                "-q",
                "--durations=20",
            ),
        )
    ]


def test_build_steps_rejects_tests_only_outside_the_portable_pytest_lane() -> None:
    quality = _load_module()

    with pytest.raises(ValueError, match="requires --portable"):
        quality.build_steps("python", tests_only=True)
    with pytest.raises(ValueError, match="requires pytest to be enabled"):
        quality.build_steps(
            "python",
            portable=True,
            tests_only=True,
            include_pytest=False,
        )
    with pytest.raises(ValueError, match="cannot include live tests"):
        quality.build_steps(
            "python",
            portable=True,
            tests_only=True,
            include_live=True,
        )


def test_ci_test_job_delegates_to_the_quality_gate_definition() -> None:
    ci_path = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(ci_path.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["test"]["steps"]
    run_steps = [step for step in steps if "run" in step]
    quality_steps = [
        step
        for step in run_steps
        if "python tools/check_quality_gate.py" in step["run"]
    ]

    assert [(step["run"], step["if"]) for step in quality_steps] == [
        (
            "python tools/check_quality_gate.py --mode development",
            "matrix.python-version == '3.11'",
        ),
        (
            "python tools/check_quality_gate.py --portable --tests-only",
            "matrix.python-version != '3.11'",
        ),
    ]
    for step in run_steps:
        assert "python -m pytest" not in step["run"]
        assert "python -m ruff" not in step["run"]
        assert "python -m mypy" not in step["run"]


@pytest.mark.parametrize("lane", ["source_contract", "source_rebuild"])
def test_exact_source_corpus_collection_requires_one_source_lane(
    request: pytest.FixtureRequest,
    lane: str,
) -> None:
    collection = importlib.import_module("tests.conftest")
    module = pytest.Module.from_parent(request.session, path=Path(__file__))
    item = pytest.Function.from_parent(
        module, name="source_consumer", callobj=lambda: None
    )
    item.fixturenames.append("exact_contract_corpus")

    with pytest.raises(pytest.UsageError, match="exact contract corpus tests require"):
        collection.pytest_collection_modifyitems([item])

    item.add_marker(lane)
    collection.pytest_collection_modifyitems([item])

    item.add_marker(
        "source_rebuild" if lane == "source_contract" else "source_contract"
    )
    with pytest.raises(pytest.UsageError, match="lanes must be disjoint"):
        collection.pytest_collection_modifyitems([item])


@pytest.mark.parametrize(
    "args",
    [
        ["--tests-only"],
        ["--portable", "--tests-only", "--skip-pytest"],
        ["--portable", "--tests-only", "--include-live"],
    ],
)
def test_main_rejects_invalid_tests_only_combinations(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    quality = _load_module()

    with pytest.raises(SystemExit, match="2"):
        quality.main(args)

    assert "tests-only" in capsys.readouterr().err


def test_build_steps_honors_skip_flags() -> None:
    quality = _load_module()

    steps = quality.build_steps(
        "python",
        include_codespell=False,
        include_pytest=False,
        include_generated_type_check=False,
    )

    assert [step.name for step in steps] == [
        "Lint",
        "Format Check",
        "Project Layout Check",
        "Release Version Consistency",
        "Explicit Object Audit",
        "Architecture Boundary Check",
        "Generated Code Freshness",
        "Static Conformance Bundle Assessment",
        "Error Translation Governance",
        "Type Check",
    ]


@pytest.mark.parametrize(
    "skip_flag",
    ["--skip-generated-type-check", "--skip-generated-sample"],
)
def test_main_honors_generated_type_check_skip_aliases(
    skip_flag: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quality = _load_module()
    executed_steps: list[str] = []

    def pass_external_steps(step: _NamedStep) -> int:
        executed_steps.append(step.name)
        return 0

    monkeypatch.setattr(quality, "run_step", pass_external_steps)

    returncode = quality.main(["--skip-codespell", "--skip-pytest", skip_flag])

    assert returncode == 0
    assert "Generated Code Freshness" in executed_steps
    assert "Generated Package Type Check" not in executed_steps


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
        "Generated Package Type Check",
        "Static Conformance Bundle Assessment",
        "Error Translation Governance",
        "Type Check",
        "Codespell",
        "Portable Tests",
        "Source Contract Tests",
        "Source Rebuild Tests",
        "Run live tests",
    ]
    portable_tests_step = next(step for step in steps if step.name == "Portable Tests")
    source_contract_tests_step = next(
        step for step in steps if step.name == "Source Contract Tests"
    )
    live_tests_step = next(step for step in steps if step.name == "Run live tests")
    assert portable_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "not live and not source_contract and not source_rebuild",
        "-q",
        "--durations=20",
    )
    assert source_contract_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-m",
        "source_contract and not live",
        "-q",
        "--durations=20",
    )
    assert live_tests_step.command == (
        "python",
        "-m",
        "pytest",
        "-q",
        "--durations=20",
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

    returncode = quality.main(["--skip-pytest", "--skip-generated-type-check"])

    assert returncode == 2
    output = capsys.readouterr().out
    assert (
        "codespell is not installed in the active environment; "
        "install dev dependencies or use --skip-codespell"
    ) in output
    assert "[quality] failed: Codespell" in output
    assert "[quality] partial development checks passed" not in output


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
        ["--skip-codespell", "--skip-pytest", "--skip-generated-type-check"]
    )

    assert returncode == 0
    assert "Codespell" not in executed_steps
    assert "[quality] partial development checks passed" in capsys.readouterr().out


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
