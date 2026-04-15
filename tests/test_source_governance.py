from __future__ import annotations

import ast
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src" / "dsctl"
PRIVATE_HOST_PATTERN = re.compile(
    r"\blocalhost\b|"
    r"\b127(?:\.\d{1,3}){3}\b|"
    r"\b0\.0\.0\.0\b|"
    r"\b10(?:\.\d{1,3}){3}\b|"
    r"\b192\.168(?:\.\d{1,3}){2}\b|"
    r"\b172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2}\b"
)
SENSITIVE_NAME_SUFFIXES = (
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "access_key",
    "private_key",
)


def test_handwritten_source_has_no_private_or_localhost_literals() -> None:
    matches: list[str] = []
    for path in _handwritten_python_files():
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            literal = node.value.strip()
            if not literal or not PRIVATE_HOST_PATTERN.search(literal):
                continue
            matches.append(
                f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}: {literal!r}"
            )

    assert matches == []


def test_handwritten_source_has_no_literal_secret_assignments() -> None:
    matches: list[str] = []
    for path in _handwritten_python_files():
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            target_name = _sensitive_target_name(node)
            if target_name is None:
                continue
            literal = _string_literal_value(_assigned_value(node))
            if literal in (None, ""):
                continue
            matches.append(
                f"{path.relative_to(PROJECT_ROOT)}:{node.lineno}: "
                f"{target_name}={literal!r}"
            )

    assert matches == []


def _handwritten_python_files() -> list[Path]:
    return sorted(
        path for path in SOURCE_ROOT.rglob("*.py") if "generated" not in path.parts
    )


def _sensitive_target_name(node: ast.AST) -> str | None:
    targets: list[ast.expr]
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]
    else:
        return None

    for target in targets:
        if not isinstance(target, ast.Name):
            continue
        normalized = target.id.lower()
        if normalized.endswith(SENSITIVE_NAME_SUFFIXES):
            return target.id
    return None


def _assigned_value(node: ast.Assign | ast.AnnAssign) -> ast.expr | None:
    return node.value


def _string_literal_value(node: ast.expr | None) -> str | None:
    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
        return None
    return node.value
