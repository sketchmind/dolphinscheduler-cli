from pathlib import Path

import pytest

from dsctl.models.workflow_patch import load_workflow_patch


def test_load_workflow_patch_accepts_name_based_workflow_and_task_ops(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL pipeline v2
      release_state: ONLINE
  tasks:
    rename:
      - from: extract
        to: extract-v2
    update:
      - match:
          name: load
        set:
          command: echo load v2
          depends_on: [extract-v2]
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
    delete:
      - obsolete
""".strip(),
        encoding="utf-8",
    )

    patch = load_workflow_patch(patch_path)

    assert patch.workflow is not None
    assert patch.workflow.set.description == "Daily ETL pipeline v2"
    assert patch.workflow.set.release_state is not None
    assert patch.workflow.set.release_state.value == "ONLINE"
    assert patch.tasks is not None
    assert patch.tasks.rename[0].from_name == "extract"
    assert patch.tasks.rename[0].to_name == "extract-v2"
    assert patch.tasks.update[0].match.name == "load"
    assert patch.tasks.create[0].name == "verify"
    assert patch.tasks.delete == ["obsolete"]


def test_load_workflow_patch_rejects_empty_task_set(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set: {}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"tasks\.update\[\]\.set"):
        load_workflow_patch(patch_path)
