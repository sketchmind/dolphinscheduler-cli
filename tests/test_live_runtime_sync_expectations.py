"""Exercise the live runtime scenario through its public CLI result seam."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.live import test_workflow_runtime_surfaces as runtime
from tests.live.support import DsctlCommandResult


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
def test_early_2_0_task_update_preserves_workflow_version(version: str) -> None:
    runtime._assert_workflow_version_after_update(
        ds_version=version, before=4, after=4, change="task_update"
    )
    for invalid in (3, 5):
        with pytest.raises(AssertionError):
            runtime._assert_workflow_version_after_update(
                ds_version=version,
                before=4,
                after=invalid,
                change="task_update",
            )


@pytest.mark.parametrize("version", ["2.0.0", "2.0.1", "2.0.2"])
def test_early_2_0_instance_sync_preserves_workflow_version(version: str) -> None:
    runtime._assert_workflow_version_after_update(
        ds_version=version, before=4, after=4, change="instance_sync"
    )
    with pytest.raises(AssertionError):
        runtime._assert_workflow_version_after_update(
            ds_version=version, before=4, after=5, change="instance_sync"
        )
    runtime._assert_workflow_version_after_update(
        ds_version=version, before=4, after=5, change="workflow_edit"
    )


@pytest.mark.parametrize("version", ["2.0.3", "3.0.0", "3.4.3"])
def test_later_instance_sync_requires_workflow_version_advance(version: str) -> None:
    runtime._assert_workflow_version_after_update(
        ds_version=version, before=4, after=5, change="instance_sync"
    )
    for invalid in (3, 4):
        with pytest.raises(AssertionError):
            runtime._assert_workflow_version_after_update(
                ds_version=version,
                before=4,
                after=invalid,
                change="instance_sync",
            )
    runtime._assert_workflow_version_after_update(
        ds_version=version, before=4, after=5, change="task_update"
    )
    with pytest.raises(AssertionError):
        runtime._assert_workflow_version_after_update(
            ds_version=version, before=4, after=4, change="task_update"
        )


def _apply_rename(
    instance_tasks: set[str],
    definition_tasks: set[str],
    rename: dict[str, str],
    *,
    sync: bool,
) -> None:
    assert rename["from"] in instance_tasks
    assert (rename["from"], rename["to"]) in {
        ("extract", "extract-instance-only"),
        ("extract", "extract-synced"),
        ("extract-instance-only", "extract-synced"),
    }
    instance_tasks.remove(rename["from"])
    instance_tasks.add(rename["to"])
    if sync:
        definition_tasks.clear()
        definition_tasks.update(instance_tasks)


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", "2.0.0", "2.0.1", "2.0.2", "2.0.3", "3.4.3"],
)
def test_instance_sync_scenario_retains_exact_cli_expectations(
    ds_version: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    requires_sync = ds_version in {"2.0.0", "2.0.1", "2.0.2"}
    instance_tasks = {"extract", "load"}
    definition_tasks = {"extract", "load"}
    calls: list[tuple[str, ...]] = []

    def result(
        args: list[str],
        *,
        data: object,
        error: object = None,
        resolved: object = None,
    ) -> DsctlCommandResult:
        action = ".".join(args[:2])
        failed = error is not None
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=1 if failed else 0,
            stdout="",
            stderr="",
            payload={
                "ok": not failed,
                "action": action,
                "data": data,
                "error": error,
                "resolved": resolved,
            },
        )

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        assert repo_root == tmp_path
        assert env_file == tmp_path / "etl.env"
        calls.append(tuple(args))
        action = tuple(args[:2])
        if action == ("workflow", "run"):
            return result(
                args,
                data={
                    "accepted": True,
                    "workflowInstanceIds": [71],
                    "instanceResolution": "resolved",
                },
            )
        if action == ("task", "list"):
            return result(args, data=[{"name": name} for name in definition_tasks])
        read_payloads = {
            ("workflow", "get"): {"version": 1},
            ("workflow-instance", "get"): {"workflowDefinitionVersion": 1},
        }
        if action in read_payloads:
            return result(args, data=read_payloads[action])
        if action == ("workflow-instance", "edit"):
            patch_path = Path(args[args.index("--patch") + 1])
            patch = yaml.safe_load(patch_path.read_text())
            rename = patch["patch"]["tasks"]["rename"][0]
            sync = "--sync-definition" in args
            if not sync and requires_sync:
                assert rename == {"from": "extract", "to": "extract-instance-only"}
                return result(
                    args,
                    data=None,
                    error={
                        "type": "user_input_error",
                        "details": {
                            "required_option": "--sync-definition",
                            "ds_version": ds_version,
                            "workflow_instance_id": 71,
                        },
                    },
                )
            _apply_rename(instance_tasks, definition_tasks, rename, sync=sync)
            return result(
                args,
                data={
                    "id": 71,
                    "workflowDefinitionVersion": (
                        1
                        if ds_version == "1.3.9" or requires_sync
                        else 3
                        if sync
                        else 2
                    ),
                },
                resolved={"syncDefine": sync},
            )
        assert action in {
            ("project", "create"),
            ("workflow", "create"),
            ("workflow", "online"),
            ("workflow-instance", "watch"),
        }
        return result(args, data={})

    def run_dsctl_raw(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        assert repo_root == tmp_path
        assert env_file == tmp_path / "etl.env"
        assert args[:2] == ["workflow-instance", "export"]
        calls.append(tuple(args))
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout=yaml.safe_dump(
                {"workflow": {}, "tasks": [{"name": name} for name in instance_tasks]}
            ),
            stderr="",
            payload={},
        )

    def cleanup(*args: object, **kwargs: object) -> None:
        del args, kwargs

    monkeypatch.setattr(runtime, "run_dsctl", run_dsctl)
    monkeypatch.setattr(runtime, "run_dsctl_raw", run_dsctl_raw)
    monkeypatch.setattr(runtime, "delete_workflow_eventually", cleanup)
    monkeypatch.setattr(runtime, "delete_project_eventually", cleanup)

    runtime.test_workflow_instance_edit_respects_sync_definition_flag(
        live_repo_root=tmp_path,
        live_etl_env_file=tmp_path / "etl.env",
        live_etl_ds_version=ds_version,
        live_name_factory=lambda stem: stem,
        tmp_path=tmp_path,
    )

    edits = [args for args in calls if args[:2] == ("workflow-instance", "edit")]
    assert len(edits) == 2
    assert "--sync-definition" not in edits[0]
    assert "--sync-definition" in edits[1]
    assert instance_tasks == definition_tasks == {"extract-synced", "load"}
    assert sum(args[:2] == ("workflow-instance", "get") for args in calls) == (
        2 if requires_sync else 0
    )
    assert sum(args[:2] == ("workflow", "get") for args in calls) == (
        2 if requires_sync else 0
    )
