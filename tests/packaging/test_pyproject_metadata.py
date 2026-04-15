from __future__ import annotations

import tomllib
from pathlib import Path
from typing import cast

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_pyproject_exposes_dsctl_console_script() -> None:
    project = _load_project_metadata()

    assert project["scripts"] == {"dsctl": "dsctl.app:main"}


def test_pyproject_has_public_package_metadata() -> None:
    project = _load_project_metadata()

    assert project["name"] == "dolphinscheduler-cli"
    assert project["readme"] == "README.md"
    assert project["requires-python"] == ">=3.11"
    keywords = project["keywords"]
    assert isinstance(keywords, list)
    assert project["license"] == "Apache-2.0"
    assert project["license-files"] == ["LICENSE"]
    assert "apache-dolphinscheduler" in keywords
    assert project["urls"] == {
        "Homepage": "https://github.com/sketchmind/dolphinscheduler-cli",
        "Repository": "https://github.com/sketchmind/dolphinscheduler-cli",
        "Issues": "https://github.com/sketchmind/dolphinscheduler-cli/issues",
        "Changelog": (
            "https://github.com/sketchmind/dolphinscheduler-cli/blob/main/CHANGELOG.md"
        ),
    }


def _load_project_metadata() -> dict[str, object]:
    data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    assert isinstance(project, dict)
    return cast("dict[str, object]", project)
