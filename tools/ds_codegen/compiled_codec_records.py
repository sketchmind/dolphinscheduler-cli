"""Render exact codec identities over a pool of complete, equal JSON records."""

from __future__ import annotations

import json
from pprint import pformat
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


CODECS_EXPRESSION = (
    "{name: deepcopy(CODEC_RECORDS[record]) for name, record in CODEC_BINDINGS.items()}"
)


def render_codec_records(codecs: tuple[tuple[str, JsonObject], ...]) -> str:
    """Keep every exact key while storing each complete JSON value only once."""
    records: dict[str, JsonObject] = {}
    bindings: dict[str, str] = {}
    record_names: dict[str, str] = {}
    for name, record in codecs:
        if name in bindings:
            message = f"compiled codec identity {name!r} is duplicated"
            raise ValueError(message)
        identity = json.dumps(
            record,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        selected = record_names.setdefault(identity, name)
        if selected == name:
            records[name] = record
        bindings[name] = selected
    return "\n".join(
        (
            "CODEC_RECORDS: dict[str, JsonObject] = "
            + pformat(records, sort_dicts=False, width=88),
            "CODEC_BINDINGS: dict[str, str] = "
            + pformat(bindings, sort_dicts=False, width=88),
            "CODECS: dict[str, JsonObject] = " + CODECS_EXPRESSION,
        )
    )
