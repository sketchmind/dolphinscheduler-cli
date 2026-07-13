from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = REPO_ROOT / ".agents" / "skills" / "use-dsctl"
MARKDOWN_POINTER = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+)\)")


def test_skill_markdown_pointers_are_self_contained() -> None:
    skill_root = SKILL_ROOT.resolve()

    for document in SKILL_ROOT.rglob("*.md"):
        text = document.read_text(encoding="utf-8")
        for match in MARKDOWN_POINTER.finditer(text):
            target = match.group(1).split("#", maxsplit=1)[0]
            if not target or "://" in target:
                continue

            resolved = (document.parent / target).resolve()
            assert resolved.is_relative_to(skill_root), (
                f"{document.relative_to(REPO_ROOT)} points outside the skill: {target}"
            )
            assert resolved.is_file(), (
                f"{document.relative_to(REPO_ROOT)} has a missing pointer: {target}"
            )
