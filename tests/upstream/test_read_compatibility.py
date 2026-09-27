from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient, ReadExecutionPolicy
from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS, VERSION_PROFILES
from dsctl.upstream import read_compatibility
from dsctl.upstream._compiled_project import PROJECT_PROGRAMS
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.read_compatibility import (
    READ_COMPATIBILITY_ACTIONS,
    available_read_actions,
)
from dsctl.upstream.read_compatibility import (
    build_read_compatibility_plan as build_observed_plan,
)
from dsctl.upstream.wire import WireExecutionMode, WireExecutor
from tests.support import make_profile

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.read_compatibility import ReadCompatibilityPlan, _ReadReview
    from dsctl.upstream.wire import CompiledWireProgram


def _observed_operations(versions: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        {
            program.source_operation
            for version in versions
            if version in TARGET_DS_VERSIONS
            for review in read_compatibility._REVIEWS.values()
            for program in read_compatibility._programs(version, review)
        }
    )


def build_read_compatibility_plan(
    versions: tuple[str, ...], *, action: str
) -> ReadCompatibilityPlan:
    return build_observed_plan(
        versions, action=action, compatible_operations=_observed_operations(versions)
    )


def test_observation_must_cover_every_transitive_operation() -> None:
    versions = ("1.3.9",)
    observed = _observed_operations(versions)
    missing = PROJECT_PROGRAMS.profile("1.3.9").program("get").source_operation
    filtered = tuple(operation for operation in observed if operation != missing)
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_observed_plan(
            versions, action="workflow-instance.get", compatible_operations=filtered
        )
    assert caught.value.details["reason"] == "read_operation_not_observed"
    assert caught.value.details["missing_operations"] == [missing]
    # A missing operation elsewhere does not disable an unrelated bounded read.
    assert "task-instance.log" in available_read_actions(
        versions, compatible_operations=filtered
    )
    assert available_read_actions(versions, compatible_operations=()) == frozenset()


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_each_exact_candidate_can_execute_only_its_reviewed_read_subset(
    version: str,
) -> None:
    for action in READ_COMPATIBILITY_ACTIONS:
        plan = build_read_compatibility_plan((version,), action=action)
        assert plan.execution_version == version
        assert plan.candidate_versions == (version,)
        assert plan.policy.program_fingerprints
    assert "workflow.create" not in READ_COMPATIBILITY_ACTIONS
    assert "workflow.export" not in READ_COMPATIBILITY_ACTIONS
    assert "workflow-instance.export" not in READ_COMPATIBILITY_ACTIONS


@pytest.mark.parametrize(
    "versions",
    [
        ("2.0.4", "2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9"),
        ("3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"),
    ],
)
def test_equal_reviewed_runtime_closures_admit_legacy_candidate_groups(
    versions: tuple[str, ...],
) -> None:
    for action in READ_COMPATIBILITY_ACTIONS:
        plan = build_read_compatibility_plan(tuple(reversed(versions)), action=action)
        assert plan.candidate_versions == versions
        assert plan.execution_version == versions[0]


@pytest.mark.parametrize(("forwards", "inherits"), [(True, False), (False, True)])
def test_execution_environment_policy_does_not_change_read_admission(
    monkeypatch: pytest.MonkeyPatch, *, forwards: bool, inherits: bool
) -> None:
    profiles = RUNTIME_INSTANCE_PROFILES
    monkeypatch.setitem(
        profiles,
        "2.0.5",
        replace(
            profiles["2.0.5"],
            schedule_forwards_environment=forwards,
            task_inherits_workflow_environment=inherits,
        ),
    )
    for action in READ_COMPATIBILITY_ACTIONS:
        plan = build_read_compatibility_plan(("2.0.4", "2.0.5"), action=action)
        assert plan.execution_version == "2.0.4"


@pytest.mark.parametrize(
    "action",
    [
        "project.delete",
        "task.create",
        "workflow.create",
        "workflow.edit",
        "workflow.run",
        "workflow.export",
        "workflow-instance.export",
        "task-instance.force-success",
        "schedule.preview",
        "schedule.explain",
    ],
)
def test_mutations_authoring_exports_and_unreviewed_reads_require_exactness(
    action: str,
) -> None:
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(("1.3.9",), action=action)
    assert caught.value.details["reason"] == "action_not_reviewed_for_compatibility"


@pytest.mark.parametrize("versions", [(), ("3.3.0",), ("3.5.0",), ("3.4.1-custom",)])
def test_unknown_candidates_cannot_become_execution_profiles(
    versions: tuple[str, ...],
) -> None:
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(versions, action="project.list")
    assert caught.value.details["reason"] == "candidate_not_reviewed_for_compatibility"


def test_same_routes_with_different_runtime_response_closures_are_rejected() -> None:
    # The later patch adds real runtime fields. Equal route names do not prove
    # that its full response/state closure is interchangeable.
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(
            ("3.3.1", "3.3.2"), action="workflow-instance.digest"
        )
    assert caught.value.details["reason"] == "read_contracts_differ"
    build_read_compatibility_plan(("3.3.1", "3.3.2"), action="task-instance.log")


def test_source_admission_is_required_even_when_all_codecs_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = VERSION_PROFILES["2.0.9"]
    original = profile["actions"]["project.list"]
    monkeypatch.setitem(
        profile["actions"], "project.list", {**original, "availability": "unsupported"}
    )
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(("2.0.4", "2.0.9"), action="project.list")
    assert caught.value.details["reason"] == "candidate_action_unsupported"


def test_selector_semantics_must_agree_even_when_wire_codecs_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decision = VERSION_PROFILES["2.0.9"]["build_decisions"]["project.page"]
    monkeypatch.setitem(decision, "selector_semantics", [{"identity": "changed"}])
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(("2.0.4", "2.0.9"), action="project.list")
    assert caught.value.details["reason"] == "read_contracts_differ"


def test_executable_request_schema_must_agree(monkeypatch: pytest.MonkeyPatch) -> None:
    original = read_compatibility._programs

    def changed_programs(
        version: str, review: _ReadReview
    ) -> tuple[CompiledWireProgram, ...]:
        programs = original(version, review)
        if version != "2.0.9":
            return programs
        program = programs[0]
        codec = replace(program.codec, request_schema_fingerprint="sha256:" + "f" * 64)
        return (replace(program, codec=codec), *programs[1:])

    monkeypatch.setattr(read_compatibility, "_programs", changed_programs)
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(("2.0.4", "2.0.9"), action="project.list")
    assert caught.value.details["reason"] == "read_contracts_differ"


def test_reviewed_error_semantics_must_agree_even_with_equal_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    codes = read_compatibility._ERROR_CODE_CLOSURES["2.0.9"]
    monkeypatch.setitem(
        read_compatibility._ERROR_CODE_CLOSURES, "2.0.9", (*codes, 99999)
    )
    with pytest.raises(UnsupportedFeatureError) as caught:
        build_read_compatibility_plan(("2.0.4", "2.0.9"), action="project.list")
    assert caught.value.details["reason"] == "read_contracts_differ"


@pytest.mark.parametrize(
    ("version", "primitive", "args", "execution_mode"),
    [
        (
            "1.3.9",
            "delete_legacy",
            {"projectId": 1},
            WireExecutionMode.MUTATION_ONCE,
        ),
        (
            "3.4.1",
            "task_code_allocate",
            {"projectCode": 1, "genNum": 1},
            WireExecutionMode.READ_RETRY_SAFE,
        ),
    ],
)
def test_get_mutations_cannot_escape_through_read_execution_modes(
    version: str,
    primitive: str,
    args: JsonObject,
    execution_mode: WireExecutionMode,
) -> None:
    domain = PROJECT_PROGRAMS if primitive == "delete_legacy" else WORKFLOW_PROGRAMS
    compiled = domain.profile(version)
    program = compiled.program(primitive)
    assert program.codec.method == "GET"
    assert program.execution_mode is execution_mode
    plan = build_read_compatibility_plan((version,), action="project.list")
    requests = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 0, "data": {}})

    with DolphinSchedulerClient(
        make_profile(ds_version=version),
        transport=httpx.MockTransport(send),
        read_policy=plan.policy,
    ) as client:
        executor = WireExecutor(
            client, source_contract_digest=compiled.source_contract_digest
        )
        with pytest.raises(
            UnsupportedFeatureError, match="admitted read compatibility closure"
        ) as error:
            executor.execute(program, program.prepare(args))
        assert error.value.details == {
            "reason": "read_operation_not_admitted",
            "action": "project.list",
            "source_operation": program.source_operation,
        }
        assert error.value.suggestion is not None
        assert "doctor" in error.value.suggestion
    assert requests == []


def test_even_a_fingerprint_in_the_policy_cannot_admit_a_mutation() -> None:
    compiled = WORKFLOW_PROGRAMS.profile("3.4.1")
    program = compiled.program("schedule_preview")
    policy = ReadExecutionPolicy("schedule.preview", frozenset({program.fingerprint}))
    with DolphinSchedulerClient(make_profile(), read_policy=policy) as client:
        executor = WireExecutor(
            client, source_contract_digest=compiled.source_contract_digest
        )
        with pytest.raises(
            UnsupportedFeatureError, match="admitted read compatibility closure"
        ):
            executor.execute(
                program, program.prepare({"projectCode": 1, "schedule": "{}"})
            )


def test_admitted_read_uses_unchanged_generated_encoder_and_decoder() -> None:
    plan = build_read_compatibility_plan(("1.3.9",), action="project.list")
    compiled = PROJECT_PROGRAMS.profile(plan.execution_version)
    program = compiled.program("page")
    requests = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"code": 0, "data": {"totalList": [], "total": 0}}
        )

    with DolphinSchedulerClient(
        make_profile(ds_version=plan.execution_version),
        transport=httpx.MockTransport(send),
        read_policy=plan.policy,
    ) as client:
        executor = WireExecutor(
            client, source_contract_digest=compiled.source_contract_digest
        )
        result = executor.execute(
            program, program.prepare({"pageNo": 1, "pageSize": 10})
        )
    assert result.raw_payload == {"code": 0, "data": {"totalList": [], "total": 0}}
    assert len(requests) == 1
    assert requests[0].url.params["pageSize"] == "10"
