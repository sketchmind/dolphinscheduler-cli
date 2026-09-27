from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from tests.fakes import FakeDag, FakeTaskDefinition

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    InvalidStateError,
    PermissionDeniedError,
)
from dsctl.services._task_resource_refs import (
    resolve_dag_read_task_resource_refs,
    resolve_read_task_resource_refs,
    resolve_task_resource_refs,
)

_WATERDROP_SCRIPT = (
    'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
    "--deploy-mode client --queue default "
    "--config waterdrop/orders.conf\n"
)


@dataclass(frozen=True)
class _Resolution:
    resource_id: int | None
    wire_full_name: str


class _ConflictingResolver:
    def resolve_task_file(self, full_name: str) -> _Resolution:
        return _Resolution(self.resolve_id(full_name), full_name)

    def resolve_id(self, _full_name: str) -> int:
        return 7

    def resolve_full_name(self, resource_id: int) -> str:
        return f"/jobs/{resource_id}.jar"


class _MalformedReadResolver:
    def resolve_task_file(self, full_name: str) -> _Resolution:
        return _Resolution(self.resolve_id(full_name), full_name)

    def resolve_id(self, full_name: str) -> int:
        return len(full_name)

    def resolve_full_name(self, resource_id: int) -> str:
        return " " if resource_id == 7 else "/jobs/good.jar"


class _DeniedResolver:
    def __init__(
        self,
        result_code: int = 30001,
        *,
        result_message: str = "no resource permission",
    ) -> None:
        self.result_code = result_code
        self.result_message = result_message

    def resolve_task_file(self, full_name: str) -> _Resolution:
        return _Resolution(self.resolve_id(full_name), full_name)

    def resolve_id(self, _full_name: str) -> int:
        raise ApiResultError(
            result_code=self.result_code,
            result_message=self.result_message,
        )

    def resolve_full_name(self, resource_id: int) -> str:
        return f"/jobs/{resource_id}.jar"


class _NameOnlyResolver:
    def resolve_task_file(self, full_name: str) -> _Resolution:
        return _Resolution(
            resource_id=None,
            wire_full_name=f"/tenant/resources{full_name}",
        )

    def resolve_id(self, _full_name: str) -> int:
        message = "name-backed validation must not invent a resource id"
        raise AssertionError(message)

    def resolve_full_name(self, resource_id: int) -> str:
        return f"/jobs/{resource_id}.jar"


class _RecordingReadResolver(_NameOnlyResolver):
    def __init__(self) -> None:
        self.requests: list[tuple[str, int | str]] = []

    def resolve_task_file(self, full_name: str) -> _Resolution:
        self.requests.append(("file", full_name))
        return super().resolve_task_file(full_name)

    def resolve_full_name(self, resource_id: int) -> str:
        self.requests.append(("id", resource_id))
        return super().resolve_full_name(resource_id)


def test_forward_resolution_translates_non_bijective_resource_ids() -> None:
    with pytest.raises(
        ApiTransportError,
        match="conflicting task resource",
    ) as captured:
        resolve_task_resource_refs(
            _ConflictingResolver(),
            ["/jobs/a.jar", "/jobs/b.jar"],
            boundary_resource="workflow",
            action="create",
        )

    assert captured.value.details == {
        "resource": "workflow",
        "action": "create",
        "resource_full_names": ["/jobs/a.jar", "/jobs/b.jar"],
        "resource_ids": [7, 7],
    }
    assert captured.value.suggestion is not None
    assert "task resource FILE" in captured.value.suggestion


def test_forward_resolution_explains_disabled_server_storage() -> None:
    with pytest.raises(InvalidStateError) as captured:
        resolve_task_resource_refs(
            _DeniedResolver(60002, result_message="存储未启用"),
            ["/jobs/a.jar"],
            boundary_resource="workflow",
            action="create",
        )

    assert captured.value.source is not None
    assert captured.value.source["result_code"] == 60002
    assert captured.value.details["resource_full_name"] == "/jobs/a.jar"
    assert captured.value.details["action"] == "create"
    assert captured.value.suggestion is not None
    assert "administrator" in captured.value.suggestion


def test_reverse_resolution_skips_malformed_full_names_for_opaque_fallback() -> None:
    refs = resolve_read_task_resource_refs(_MalformedReadResolver(), [7, 8, 8])

    assert dict(refs.id_by_full_name) == {"/jobs/good.jar": 8}
    assert dict(refs.full_name_by_id) == {8: "/jobs/good.jar"}


@pytest.mark.parametrize("result_code", [30001, 1400001])
def test_permission_failure_includes_actionable_resource_suggestion(
    result_code: int,
) -> None:
    with pytest.raises(PermissionDeniedError) as captured:
        resolve_task_resource_refs(
            _DeniedResolver(result_code),
            ["/jobs/private.jar"],
            boundary_resource="workflow",
            action="create",
        )

    assert captured.value.suggestion is not None
    assert "dsctl resource list" in captured.value.suggestion
    assert "request FILE resource access" in captured.value.suggestion


def test_33_string_only_resource_permission_failure_is_translated() -> None:
    resolver = _DeniedResolver(
        10000,
        result_message=(
            "The user's tenant is analytics have no permission to access the "
            "resource: /tenant/resources/ml/train.py"
        ),
    )

    with pytest.raises(PermissionDeniedError) as captured:
        resolve_task_resource_refs(
            resolver,
            ["/ml/train.py"],
            boundary_resource="workflow",
            action="create",
        )

    assert captured.value.details["result_code"] == 10000
    assert "request FILE resource access" in (captured.value.suggestion or "")


def test_33_other_internal_resource_failure_is_not_mislabeled_as_permission() -> None:
    resolver = _DeniedResolver(
        10000,
        result_message="Invalidated resource path: /wrong/root/ml/train.py",
    )

    with pytest.raises(ApiTransportError, match="could not resolve"):
        resolve_task_resource_refs(
            resolver,
            ["/ml/train.py"],
            boundary_resource="workflow",
            action="create",
        )


def test_forward_resolution_records_name_only_file_verification() -> None:
    refs = resolve_task_resource_refs(
        _NameOnlyResolver(),
        ["/ml/train.py"],
        boundary_resource="workflow",
        action="create",
    )

    assert refs.verified_full_names == frozenset({"/ml/train.py"})
    assert dict(refs.id_by_full_name) == {}
    assert dict(refs.wire_full_name_by_full_name) == {
        "/ml/train.py": "/tenant/resources/ml/train.py"
    }


def _waterdrop_task(
    *,
    code: int,
    task_params: object,
) -> FakeTaskDefinition:
    return FakeTaskDefinition(
        code=code,
        name=f"waterdrop-{code}",
        task_type_value="WATERDROP",
        task_params_value=(
            json.dumps(task_params, separators=(",", ":"))
            if not isinstance(task_params, str)
            else task_params
        ),
    )


def _dag_with_tasks(*tasks: FakeTaskDefinition) -> FakeDag:
    return FakeDag(
        workflow_definition_value=None,
        task_definition_list_value=list(tasks),
        workflow_task_relation_list_value=[],
    )


def test_waterdrop_read_resource_ids_only_include_exact_typed_candidates() -> None:
    exact = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": _WATERDROP_SCRIPT,
    }
    dag = _dag_with_tasks(
        _waterdrop_task(code=1, task_params=exact),
        _waterdrop_task(code=2, task_params=exact),
    )

    resolver = _RecordingReadResolver()
    refs = resolve_dag_read_task_resource_refs(resolver, dag, profile_version="2.0.9")

    assert resolver.requests == [("id", 811)]
    assert dict(refs.full_name_by_id) == {811: "/jobs/811.jar"}

    absent_resolver = _RecordingReadResolver()
    absent_refs = resolve_dag_read_task_resource_refs(
        absent_resolver, dag, profile_version="2.0.0"
    )

    assert absent_resolver.requests == []
    assert absent_refs.verified_full_names == frozenset()


@pytest.mark.parametrize(
    "task_params",
    [
        "not-json",
        {
            "localParams": [],
            "resourceList": [{"id": 811}, {"id": 812}],
            "rawScript": _WATERDROP_SCRIPT,
        },
        {
            "localParams": [{"prop": "day"}],
            "resourceList": [{"id": 811}],
            "rawScript": _WATERDROP_SCRIPT,
        },
        {
            "localParams": [],
            "resourceList": [{"id": True}],
            "rawScript": _WATERDROP_SCRIPT,
        },
        {
            "localParams": [],
            "resourceList": [{"id": 811, "name": "/waterdrop/orders.conf"}],
            "rawScript": _WATERDROP_SCRIPT,
        },
        {
            "localParams": [],
            "resourceList": [{"id": 811}],
            "rawScript": (
                'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
                "--deploy-mode client --queue priority "
                "--config waterdrop/orders.conf\n"
            ),
        },
        {
            "localParams": [],
            "resourceList": [{"id": 811}],
            "rawScript": _WATERDROP_SCRIPT,
            "future": True,
        },
    ],
)
def test_waterdrop_read_resource_ids_skip_opaque_shapes(task_params: object) -> None:
    dag = _dag_with_tasks(_waterdrop_task(code=1, task_params=task_params))
    resolver = _RecordingReadResolver()

    refs = resolve_dag_read_task_resource_refs(resolver, dag, profile_version="2.0.9")

    assert resolver.requests == []
    assert refs.verified_full_names == frozenset()


def _pytorch_task(
    *,
    code: int,
    task_params: object,
) -> FakeTaskDefinition:
    return FakeTaskDefinition(
        code=code,
        name=f"pytorch-{code}",
        task_type_value="PYTORCH",
        task_params_value=json.dumps(task_params, separators=(",", ":")),
    )


def test_pytorch_read_resource_ids_only_include_exact_31x_typed_candidates() -> None:
    exact = {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": "ml/train.py",
        "scriptParams": "--epochs 10",
        "pythonCommand": "/opt/python/bin/python3",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
        "resourceList": [{"id": 71}],
    }
    richer = {**exact, "futureField": True}
    dag = _dag_with_tasks(
        _pytorch_task(code=1, task_params=exact),
        _pytorch_task(code=2, task_params=exact),
        _pytorch_task(code=3, task_params=richer),
    )

    resolver = _RecordingReadResolver()
    refs = resolve_dag_read_task_resource_refs(resolver, dag, profile_version="3.1.9")

    assert resolver.requests == [("id", 71)]
    assert dict(refs.full_name_by_id) == {71: "/jobs/71.jar"}

    modern_resolver = _RecordingReadResolver()
    modern_refs = resolve_dag_read_task_resource_refs(
        modern_resolver, dag, profile_version="3.2.0"
    )

    assert modern_resolver.requests == []
    assert modern_refs.verified_full_names == frozenset()


def test_pytorch_modern_read_refs_require_the_observed_storage_identity() -> None:
    exact = {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": "ml/train.py",
        "scriptParams": "--epochs 10",
        "pythonLauncher": "/opt/python/bin/python3",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
        "resourceList": [{"resourceName": "/tenant/resources/ml/train.py"}],
    }
    dag = _dag_with_tasks(_pytorch_task(code=1, task_params=exact))

    refs = resolve_dag_read_task_resource_refs(
        _NameOnlyResolver(),
        dag,
        profile_version="3.2.0",
    )

    assert refs.verified_full_names == frozenset({"/ml/train.py"})
    assert dict(refs.id_by_full_name) == {}
    assert dict(refs.wire_full_name_by_full_name) == {
        "/ml/train.py": "/tenant/resources/ml/train.py"
    }


def test_pytorch_modern_read_refs_reject_an_unverified_storage_identity() -> None:
    exact = {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": "ml/train.py",
        "scriptParams": "",
        "pythonLauncher": "/opt/python/bin/python3",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
        "resourceList": [{"resourceName": "/foreign/resources/ml/train.py"}],
    }
    dag = _dag_with_tasks(_pytorch_task(code=1, task_params=exact))

    refs = resolve_dag_read_task_resource_refs(
        _NameOnlyResolver(),
        dag,
        profile_version="3.2.0",
    )

    assert refs.verified_full_names == frozenset()
    assert dict(refs.wire_full_name_by_full_name) == {}


def test_mixed_legacy_dag_reads_strict_ids_in_first_seen_order() -> None:
    waterdrop = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": _WATERDROP_SCRIPT,
    }
    rows: list[tuple[str, str | bool | None]] = [
        ("MR", '{"mainJar":{"id":9}}'),
        ("WATERDROP", json.dumps(waterdrop)),
        ("MR", '{"mainJar":{"id":9}}'),
        ("MR", '{"mainJar":{"id":8}}'),
        ("MR", '{"mainJar":{"id":true}}'),
        ("MR", '{"mainJar":{"id":0}}'),
        ("MR", '{"mainJar":{"id":-1}}'),
        ("MR", '{"mainJar":{"id":"7"}}'),
        ("MR", '{"mainJar":[]}'),
        ("MR", "not-json"),
        ("MR", "[]"),
        ("MR", None),
        ("MR", True),
        ("SHELL", '{"mainJar":{"id":10}}'),
    ]
    dag = _dag_with_tasks(
        *(
            FakeTaskDefinition(
                code=code,
                name=f"task-{code}",
                task_type_value=task_type,
                task_params_value=params,
            )
            for code, (task_type, params) in enumerate(rows, start=1)
        )
    )
    resolver = _RecordingReadResolver()

    refs = resolve_dag_read_task_resource_refs(resolver, dag, profile_version="2.0.9")

    assert resolver.requests == [("id", 9), ("id", 811), ("id", 8)]
    assert dict(refs.full_name_by_id) == {
        9: "/jobs/9.jar",
        811: "/jobs/811.jar",
        8: "/jobs/8.jar",
    }


def test_mixed_modern_dag_decodes_once_and_preserves_distinct_file_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exact = {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": "ml/train.py",
        "scriptParams": "--epochs 10",
        "pythonLauncher": "/opt/python/bin/python3",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
        "resourceList": [{"resourceName": "/tenant/resources/ml/train.py"}],
    }
    tasks = [
        _pytorch_task(code=1, task_params=exact),
        _pytorch_task(
            code=2,
            task_params={
                **exact,
                "script": "ml/predict.py",
                "resourceList": [{"resourceName": "/tenant/resources/ml/predict.py"}],
            },
        ),
        _pytorch_task(code=3, task_params=exact),
        _pytorch_task(
            code=4,
            task_params={
                **exact,
                "resourceList": [{"resourceName": "/foreign/resources/ml/train.py"}],
            },
        ),
        _pytorch_task(code=5, task_params={**exact, "futureField": True}),
        _pytorch_task(code=6, task_params={**exact, "isCreateEnvironment": 0}),
        FakeTaskDefinition(
            code=7,
            name="broken",
            task_type_value="PYTORCH",
            task_params_value="not-json",
        ),
        _pytorch_task(code=8, task_params=[]),
        FakeTaskDefinition(
            code=9,
            name="old-mr",
            task_type_value="MR",
            task_params_value='{"mainJar":{"id":71}}',
        ),
        FakeTaskDefinition(
            code=10,
            name="unrelated",
            task_type_value="HTTP",
            task_params_value="do not decode this task",
        ),
    ]
    decoded_inputs: list[str] = []
    original_loads = json.loads

    def recording_loads(raw: str) -> object:
        decoded_inputs.append(raw)
        return original_loads(raw)

    monkeypatch.setattr(json, "loads", recording_loads)
    resolver = _RecordingReadResolver()

    refs = resolve_dag_read_task_resource_refs(
        resolver, _dag_with_tasks(*tasks), profile_version="3.2.0"
    )

    assert decoded_inputs == [task.taskParams for task in tasks[:-1]]
    assert resolver.requests == [
        ("file", "/ml/train.py"),
        ("file", "/ml/predict.py"),
        ("file", "/ml/train.py"),
    ]
    assert refs.verified_full_names == frozenset({"/ml/train.py", "/ml/predict.py"})
    assert dict(refs.id_by_full_name) == {}
    assert dict(refs.wire_full_name_by_full_name) == {
        "/ml/train.py": "/tenant/resources/ml/train.py",
        "/ml/predict.py": "/tenant/resources/ml/predict.py",
    }


def test_read_resolution_finishes_ids_before_files_and_skips_failed_candidates() -> (
    None
):
    class FailingResolver(_RecordingReadResolver):
        def resolve_full_name(self, resource_id: int) -> str:
            full_name = super().resolve_full_name(resource_id)
            if resource_id == 7:
                raise ApiResultError(result_code=20004, result_message="not found")
            return full_name

        def resolve_task_file(self, full_name: str) -> _Resolution:
            resolution = super().resolve_task_file(full_name)
            if full_name == "/ml/missing.py":
                message = "Resource lookup failed"
                raise ApiTransportError(message)
            return resolution

    resolver = FailingResolver()
    refs = resolve_read_task_resource_refs(
        resolver,
        [7, True, 8, 7, 0, -1],
        file_candidates=[
            ("/ml/missing.py", "/tenant/resources/ml/missing.py"),
            ("/ml/train.py", "/tenant/resources/ml/train.py"),
        ],
    )

    assert resolver.requests == [
        ("id", 7),
        ("id", 8),
        ("file", "/ml/missing.py"),
        ("file", "/ml/train.py"),
    ]
    assert dict(refs.full_name_by_id) == {8: "/jobs/8.jar"}
    assert refs.verified_full_names == frozenset({"/jobs/8.jar", "/ml/train.py"})
    assert dict(refs.wire_full_name_by_full_name) == {
        "/jobs/8.jar": "/jobs/8.jar",
        "/ml/train.py": "/tenant/resources/ml/train.py",
    }


@pytest.mark.parametrize("version", ["3.1.9", "3.2.0", "3.4.1"])
def test_script_java_mr_read_binds_only_the_exact_observed_file_identity(
    version: str,
) -> None:
    script_info = (
        {"id": 71}
        if version == "3.1.9"
        else {"resourceName": "/tenant/resources/scripts/job.sh"}
    )
    jar_info = (
        {"id": 72}
        if version == "3.1.9"
        else {"resourceName": "/tenant/resources/jobs/app.jar"}
    )
    tasks = [
        FakeTaskDefinition(
            code=1,
            name="shell",
            task_type_value="SHELL",
            task_params_value=json.dumps({"resourceList": [script_info, script_info]}),
        ),
        FakeTaskDefinition(
            code=2,
            name="python",
            task_type_value="PYTHON",
            task_params_value=json.dumps({"resourceList": [script_info]}),
        ),
        FakeTaskDefinition(
            code=3,
            name="jar",
            task_type_value="MR",
            task_params_value=json.dumps({"mainJar": jar_info}),
        ),
        FakeTaskDefinition(
            code=4,
            name="foreign",
            task_type_value="SHELL",
            task_params_value=json.dumps(
                {
                    "resourceList": [
                        {"id": True},
                        {"id": 0},
                        {"id": 71, "res": "/scripts/job.sh"},
                        {"resourceName": "/foreign/resources/scripts/job.sh"},
                        {"resourceName": "/scripts/job.sh"},
                    ]
                }
            ),
        ),
    ]
    if version != "3.1.9":
        tasks.append(
            FakeTaskDefinition(
                code=5,
                name="java",
                task_type_value="JAVA",
                task_params_value=json.dumps({"mainJar": jar_info}),
            )
        )
    resolver = _RecordingReadResolver()
    refs = resolve_dag_read_task_resource_refs(
        resolver, _dag_with_tasks(*tasks), profile_version=version
    )
    if version == "3.1.9":
        assert resolver.requests == [("id", 71), ("id", 72)]
        assert dict(refs.full_name_by_id) == {71: "/jobs/71.jar", 72: "/jobs/72.jar"}
    else:
        assert resolver.requests == [
            ("file", "/scripts/job.sh"),
            ("file", "/jobs/app.jar"),
            ("file", "/scripts/job.sh"),
        ]
        assert refs.verified_full_names == frozenset(
            {"/scripts/job.sh", "/jobs/app.jar"}
        )
        assert dict(refs.wire_full_name_by_full_name) == {
            "/scripts/job.sh": "/tenant/resources/scripts/job.sh",
            "/jobs/app.jar": "/tenant/resources/jobs/app.jar",
        }
