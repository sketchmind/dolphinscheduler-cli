from __future__ import annotations

import pytest
import yaml

from dsctl.support.yaml_io import dump_yaml_document


@pytest.mark.parametrize(
    ("value", "indicator"),
    [
        ("select 1\nfrom source", "sql: |-"),
        ("select 1\nfrom source\n", "sql: |"),
        ("select 1\nfrom source\n\n", "sql: |+"),
    ],
)
def test_dump_yaml_document_uses_lossless_literal_blocks(
    value: str,
    indicator: str,
) -> None:
    rendered = dump_yaml_document({"sql": value})

    assert rendered.startswith(indicator)
    assert yaml.safe_load(rendered) == {"sql": value}


def test_dump_yaml_document_keeps_single_line_scalars_plain() -> None:
    rendered = dump_yaml_document({"command": "echo hello"})

    assert rendered == "command: echo hello\n"
    assert yaml.safe_load(rendered) == {"command": "echo hello"}


def test_dump_yaml_document_remains_safe_for_unsupported_objects() -> None:
    with pytest.raises(yaml.representer.RepresenterError):
        dump_yaml_document({"unsafe": object()})  # type: ignore[dict-item]
