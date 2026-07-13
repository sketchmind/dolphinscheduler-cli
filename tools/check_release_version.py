from __future__ import annotations

import argparse
import ast
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_project_version(root: Path) -> str:
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    project = data.get("project")
    if not isinstance(project, dict):
        message = "pyproject.toml is missing [project]"
        raise TypeError(message)
    version = project.get("version")
    if not isinstance(version, str) or version == "":
        message = "pyproject.toml has no non-empty project.version"
        raise ValueError(message)
    return version


def load_runtime_version(root: Path) -> str:
    version_file = root / "src" / "dsctl" / "__init__.py"
    module = ast.parse(
        version_file.read_text(encoding="utf-8"), filename=str(version_file)
    )
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in statement.targets
        ):
            continue
        value = statement.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        break
    message = "src/dsctl/__init__.py has no literal __version__ assignment"
    raise ValueError(message)


def check_release_version(root: Path, *, tag: str | None = None) -> str:
    project_version = load_project_version(root)
    runtime_version = load_runtime_version(root)
    if runtime_version != project_version:
        message = (
            "version mismatch: "
            f"pyproject.toml={project_version!r}, dsctl.__version__={runtime_version!r}"
        )
        raise ValueError(message)

    release_tag = tag.strip() if tag is not None else ""
    expected_tag = f"v{project_version}"
    if release_tag not in ("", expected_tag):
        message = (
            f"release tag mismatch: expected {expected_tag!r}, got {release_tag!r}"
        )
        raise ValueError(message)
    return project_version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify package, runtime, and optional release-tag versions."
    )
    parser.add_argument(
        "--tag",
        help="release tag to verify; an empty value skips tag validation",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        version = check_release_version(args.root, tag=args.tag)
    except (
        OSError,
        SyntaxError,
        TypeError,
        ValueError,
        tomllib.TOMLDecodeError,
    ) as error:
        print(f"release version check failed: {error}", file=sys.stderr)
        return 1

    print(f"release version check passed: {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
