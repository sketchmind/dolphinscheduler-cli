from __future__ import annotations

import pytest

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface


def test_every_exact_version_has_one_parameter_semantics_profile() -> None:
    profiles = [get_parameter_semantics(version) for version in TARGET_DS_VERSIONS]

    assert [profile.version for profile in profiles] == list(TARGET_DS_VERSIONS)
    assert len({profile.version for profile in profiles}) == 37


def test_task_authoring_surface_caches_by_normalized_exact_version() -> None:
    canonical = get_task_authoring_surface("3.4.1")

    assert get_task_authoring_surface(" v3_4_1 ") is canonical
    assert get_task_authoring_surface("ds_3_4_1") is canonical


@pytest.mark.parametrize(
    ("version", "wire", "key_scope"),
    [
        ("1.3.9", "absent", "absent"),
        ("2.0.0", "startParams", "declared-workflow-globals"),
        ("2.0.9", "startParams", "any-key-as-varchar"),
        ("3.0.0", "startParams", "any-key-as-varchar"),
        ("3.4.2", "startParams", "any-key-as-varchar"),
    ],
)
def test_startup_parameter_epochs_are_exact(
    version: str,
    wire: str,
    key_scope: str,
) -> None:
    startup = get_parameter_semantics(version).startup

    assert startup.wire == wire
    assert startup.key_scope == key_scope


@pytest.mark.parametrize(
    ("version", "parser", "binding", "winner"),
    [
        ("1.3.9", "absent", "absent", "n/a"),
        ("2.0.0", "dollar-line-start", "implicit", "earliest-nonempty"),
        (
            "3.0.0",
            "dollar-or-hash-line-start",
            "implicit",
            "earliest-nonempty",
        ),
        (
            "3.2.1",
            "dollar-or-hash-stream",
            "implicit",
            "earliest-nonempty",
        ),
        (
            "3.3.1",
            "dollar-or-hash-stream",
            "declared-in-only",
            "latest-nonempty",
        ),
        (
            "3.4.2",
            "dollar-or-hash-stream",
            "declared-in-only",
            "latest-nonempty",
        ),
    ],
)
def test_output_parameter_epochs_are_exact(
    version: str,
    parser: str,
    binding: str,
    winner: str,
) -> None:
    output = get_parameter_semantics(version).output

    assert output.set_value_parser == parser
    assert output.downstream_binding == binding
    assert output.same_name_upstream_winner == winner


@pytest.mark.parametrize(
    ("version", "precedence"),
    [
        ("1.3.9", ("local", "global", "builtin")),
        (
            "2.0.0",
            ("local", "startup", "global", "context", "builtin"),
        ),
        (
            "3.0.0",
            ("local", "context", "startup", "global", "builtin"),
        ),
        (
            "3.2.0",
            ("local", "context", "startup", "global", "project", "builtin"),
        ),
        (
            "3.2.1",
            ("startup", "local", "context", "global", "project", "builtin"),
        ),
        (
            "3.3.1",
            ("context", "startup", "local", "global", "project", "builtin"),
        ),
    ],
)
def test_parameter_resolution_precedence_epochs_are_exact(
    version: str,
    precedence: tuple[str, ...],
) -> None:
    assert get_parameter_semantics(version).resolution.effective_precedence == (
        precedence
    )


@pytest.mark.parametrize(
    ("version", "task_type", "local_role", "sources"),
    [
        ("1.3.9", "SUB_PROCESS", "ignored", ("parent-global",)),
        ("2.0.0", "SUB_PROCESS", "select-parent-global", ("parent-global",)),
        (
            "3.0.0",
            "SUB_PROCESS",
            "select-parent-task-varpool",
            ("parent-global", "parent-varpool"),
        ),
        ("3.3.1", "SUB_WORKFLOW", "ignored", ("parent-startup",)),
        (
            "3.4.0",
            "SUB_WORKFLOW",
            "ignored",
            ("parent-global", "parent-startup", "parent-varpool"),
        ),
    ],
)
def test_nested_workflow_parameter_epochs_are_exact(
    version: str,
    task_type: str,
    local_role: str,
    sources: tuple[str, ...],
) -> None:
    nested = get_parameter_semantics(version).nested_workflow

    assert nested.task_type == task_type
    assert nested.task_local_params_role == local_role
    assert nested.child_input_sources == sources


@pytest.mark.parametrize(
    ("version", "native_task_type", "native_code_field"),
    [
        ("1.3.9", "SUB_PROCESS", "processDefinitionId"),
        ("3.2.2", "SUB_PROCESS", "processDefinitionCode"),
        ("3.3.1", "SUB_WORKFLOW", "workflowDefinitionCode"),
        ("3.4.2", "SUB_WORKFLOW", "workflowDefinitionCode"),
    ],
)
def test_nested_authoring_identity_follows_parameter_semantics(
    version: str,
    native_task_type: str,
    native_code_field: str,
) -> None:
    nested_semantics = get_parameter_semantics(version).nested_workflow
    nested_surface = get_task_authoring_surface(version).nested_workflow

    assert nested_semantics.task_type == native_task_type
    assert nested_surface.native_task_type == nested_semantics.task_type
    assert nested_surface.native_code_field == native_code_field


@pytest.mark.parametrize(
    (
        "version",
        "available",
        "program_types",
        "native_program_type",
        "parameter_substitution",
        "credential_source",
        "credential_keys",
    ),
    [
        ("1.3.9", False, (), False, False, None, ()),
        ("2.0.9", False, (), False, False, None, ()),
        (
            "3.0.0",
            True,
            ("RUN_JOB_FLOW",),
            False,
            False,
            "worker-properties",
            ("aws.access.key.id", "aws.secret.access.key", "aws.region"),
        ),
        (
            "3.0.6",
            True,
            ("RUN_JOB_FLOW",),
            False,
            False,
            "worker-resource-properties",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
        ),
        (
            "3.1.0",
            True,
            ("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
            True,
            False,
            "worker-resource-properties",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
        ),
        (
            "3.2.1",
            True,
            ("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
            True,
            False,
            "worker-resource-properties",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
        ),
        (
            "3.2.2",
            True,
            ("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
            True,
            True,
            "worker-resource-properties",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
        ),
        (
            "3.3.1",
            True,
            ("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
            True,
            True,
            "aws-authentication",
            (
                "aws.emr.credentials.provider.type",
                "aws.emr.region",
                "aws.emr.access.key.id",
                "aws.emr.access.key.secret",
            ),
        ),
        (
            "3.4.2",
            True,
            ("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
            True,
            True,
            "aws-authentication",
            (
                "aws.emr.credentials.provider.type",
                "aws.emr.region",
                "aws.emr.access.key.id",
                "aws.emr.access.key.secret",
            ),
        ),
    ],
)
def test_emr_authoring_surface_tracks_exact_execution_epochs(
    version: str,
    *,
    available: bool,
    program_types: tuple[str, ...],
    native_program_type: bool,
    parameter_substitution: bool,
    credential_source: str | None,
    credential_keys: tuple[str, ...],
) -> None:
    emr = get_task_authoring_surface(version).emr

    assert emr.available is available
    assert emr.program_types == program_types
    assert emr.native_program_type is native_program_type
    assert emr.parameter_substitution is parameter_substitution
    assert emr.credential_source == credential_source
    assert emr.credential_keys == credential_keys
    assert emr.failover_supported is False


@pytest.mark.parametrize(
    ("version", "available", "script_execution"),
    [
        ("1.3.9", False, None),
        ("2.0.0", False, None),
        ("2.0.9", False, None),
        ("3.0.0", False, None),
        ("3.0.6", False, None),
        ("3.1.0", True, "hive-e-whole-command"),
        ("3.1.9", True, "hive-e-whole-command"),
        ("3.2.0", True, "generated-file-sql-only"),
        ("3.2.1", True, "generated-file-sql-only"),
        ("3.2.2", True, "generated-file-sql-only"),
        ("3.3.1", True, "generated-file-sql-only"),
        ("3.3.2", True, "generated-file-sql-only"),
        ("3.4.0", True, "generated-file-sql-only"),
        ("3.4.1", True, "generated-file-sql-only"),
        ("3.4.2", True, "generated-file-sql-only"),
    ],
)
def test_hive_cli_authoring_surface_tracks_exact_execution_epoch(
    version: str,
    *,
    available: bool,
    script_execution: str | None,
) -> None:
    hive_cli = get_task_authoring_surface(version).hive_cli

    assert hive_cli.available is available
    assert hive_cli.script_execution == script_execution


def test_property_type_and_project_parameter_boundaries_share_exact_facts() -> None:
    legacy = get_parameter_semantics("1.3.9")
    listed = get_parameter_semantics("2.0.0")
    file_capable = get_parameter_semantics("3.2.0")

    assert "LIST" not in legacy.allowed_property_types
    assert "LIST" in listed.allowed_property_types
    assert "FILE" not in listed.allowed_property_types
    assert "FILE" in file_capable.allowed_property_types
    assert legacy.resolution.project_parameters is False
    assert get_parameter_semantics("3.1.9").resolution.project_parameters is False
    assert file_capable.resolution.project_parameters is True
