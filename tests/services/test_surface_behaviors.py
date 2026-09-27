"""Selection and remediation contracts without requiring particular call syntax."""

import pytest
from tests.bound_domain_fakes import patch_bound_domain_service_runtime
from tests.fakes import (
    FakeEnvironment,
    FakeEnvironmentAdapter,
    FakeProject,
    FakeProjectAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    fake_read_service_runtime,
)
from tests.support import make_profile, normalize_cli_help
from tests.value_shape_assertions import assert_mapping, assert_sequence
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import NotFoundError, ResolutionError, UserInputError
from dsctl.services import env as env_service
from dsctl.services import project as project_service
from dsctl.services import runtime as runtime_service
from dsctl.services.schema import get_schema_result
from dsctl.services.selection import (
    require_workflow_selection,
)
from dsctl.upstream import resolver
from dsctl.upstream.environments import ENVIRONMENT_DOMAIN, EnvironmentDomain


@pytest.fixture
def environments(monkeypatch: pytest.MonkeyPatch) -> FakeEnvironmentAdapter:
    adapter = FakeEnvironmentAdapter(
        environments=[
            FakeEnvironment(code=7, name="prod blue; literal", config="export A=1"),
            FakeEnvironment(code=8, name="prod blue; literal backup"),
        ]
    )
    patch_bound_domain_service_runtime(
        monkeypatch,
        env_service,
        expected_domain=ENVIRONMENT_DOMAIN,
        runtime_domain=EnvironmentDomain(environments=adapter),
        profile_factory=make_profile,
    )
    return adapter


@pytest.mark.parametrize("selector", ["prod blue; literal", "7"])
def test_environment_service_resolves_exact_opaque_name_or_code(
    environments: FakeEnvironmentAdapter, selector: str
) -> None:
    result = env_service.get_environment_result(selector)
    assert assert_mapping(result.data)["code"] == environments.environments[0].code


def test_missing_and_ambiguous_names_offer_discovery_and_id_recovery(
    environments: FakeEnvironmentAdapter,
) -> None:
    with pytest.raises(NotFoundError) as missing:
        env_service.get_environment_result("absent")
    assert missing.value.suggestion
    assert "dsctl environment list" in missing.value.suggestion
    discovered = assert_sequence(
        assert_mapping(env_service.list_environments_result().data)["totalList"]
    )
    assert len(discovered) == 2
    environments.environments.append(FakeEnvironment(code=9, name="prod blue; literal"))
    with pytest.raises(ResolutionError) as ambiguous:
        env_service.get_environment_result("prod blue; literal")
    assert ambiguous.value.error_type == "resolution_error"
    assert ambiguous.value.suggestion
    assert "code" in ambiguous.value.suggestion or "id" in ambiguous.value.suggestion
    assert assert_mapping(env_service.get_environment_result("9").data)["code"] == 9


def test_delete_confirmation_suggestion_is_accepted_and_preserves_target(
    environments: FakeEnvironmentAdapter,
) -> None:
    with pytest.raises(UserInputError) as error:
        env_service.delete_environment_result("prod blue; literal", force=False)
    assert "--force" in (error.value.suggestion or "")
    assert len(environments.environments) == 2
    parsed = CliRunner().invoke(
        app, ["environment", "delete", "prod blue; literal", "--force", "--help"]
    )
    assert parsed.exit_code == 0
    env_service.delete_environment_result("prod blue; literal", force=True)
    assert [item.code for item in environments.environments] == [8]


@pytest.mark.parametrize("selector", ["project with spaces", "7"])
def test_compiled_project_service_resolves_name_or_code(
    monkeypatch: pytest.MonkeyPatch, selector: str
) -> None:
    adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="project with spaces"),
            FakeProject(code=8, name="project with spaces backup"),
        ]
    )
    monkeypatch.setattr(
        runtime_service,
        "open_read_service_runtime",
        lambda env_file=None: fake_read_service_runtime(
            adapter, profile=make_profile()
        ),
    )
    assert (
        assert_mapping(project_service.get_project_result(selector).data)["code"] == 7
    )


def test_explicit_project_requires_new_workflow_selection_and_resolves_in_scope() -> (
    None
):
    with pytest.raises(UserInputError) as error:
        require_workflow_selection(
            None,
            input_form="argument",
        )
    assert "WORKFLOW" in (error.value.suggestion or "")
    adapter = FakeWorkflowAdapter(
        dags={},
        workflows=[
            FakeWorkflow(code=71, name="daily", project_code_value=7),
            FakeWorkflow(code=81, name="daily", project_code_value=8),
        ],
    )
    corrected = require_workflow_selection(
        "daily",
        input_form="argument",
    )
    assert (
        resolver.workflow(corrected.value, adapter=adapter, project_code=8).code == 81
    )


def test_workflow_example_schema_choices_are_public_parser_choices() -> None:
    data = assert_mapping(get_schema_result(command_action="template.workflow").data)
    options = assert_sequence(assert_mapping(data["command"])["options"])
    example = next(
        assert_mapping(item)
        for item in options
        if assert_mapping(item)["name"] == "example"
    )
    assert example.get("default") is None
    choices = assert_sequence(example["choices"])
    assert list(choices) == ["basic", "output", "branch", "child", "dependent"]
    runner = CliRunner()
    help_result = runner.invoke(app, ["template", "workflow", "--help"])
    assert help_result.exit_code == 0
    assert "--example" in normalize_cli_help(help_result.stdout)
    for choice in choices:
        assert isinstance(choice, str)
        result = runner.invoke(
            app, ["template", "workflow", "--example", choice, "--raw"]
        )
        assert result.exit_code == 0, result.stdout
        assert "workflow:" in result.stdout
        assert "tasks:" in result.stdout
