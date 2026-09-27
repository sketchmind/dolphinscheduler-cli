from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def set_generated_operation_action(
    source_root: Path,
    *,
    operation: str,
    action: str,
) -> None:
    """Change one named generated-profile operation binding in a test tree."""
    path = source_root / "src/dsctl/generated/version_profiles.py"
    source = path.read_text(encoding="utf-8")
    prefix = "_PROFILE_JSON = r'''\n"
    before, payload_and_suffix = source.split(prefix, maxsplit=1)
    payload_source, suffix = payload_and_suffix.split(
        "\n'''\n_SHARED_DATA",
        maxsplit=1,
    )
    payload = json.loads(payload_source)
    for record in payload["build_records"].values():
        if record["semantic_operation"] == operation:
            record["stable_action"] = action
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    path.write_text(
        before + prefix + rendered + "\n'''\n_SHARED_DATA" + suffix,
        encoding="utf-8",
    )


__all__ = ["set_generated_operation_action"]
