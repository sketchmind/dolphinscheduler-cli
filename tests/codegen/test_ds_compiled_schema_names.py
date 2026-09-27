from __future__ import annotations

from typing import Literal

import pytest

from ds_codegen.compiled_schema_names import schema_module_name, schema_symbol_name

_HEX_DIGEST = "0123456789abcdef" * 4
_DIGEST = f"sha256:{_HEX_DIGEST}"


@pytest.mark.parametrize("schema", ["definition_get", f"definition_get_{_HEX_DIGEST}"])
def test_schema_symbols_keep_the_role_and_only_shorten_the_physical_digest(
    schema: str,
) -> None:
    assert schema_symbol_name(schema, _DIGEST) == "definition_get_0123456789ab"


@pytest.mark.parametrize("role", ["request", "response"])
def test_support_modules_live_under_a_domain_with_short_leaf_names(
    role: Literal["request", "response"],
) -> None:
    module = schema_module_name(
        "workflow_runtime", role, f"definition_get_{_HEX_DIGEST}", _DIGEST
    )
    assert module == (f"_schemas.workflow_runtime.{role}_definition_get_0123456789ab")
    assert len(module.rsplit(".", 1)[1]) < 64


def test_short_name_prefix_is_not_a_replacement_for_complete_digest_identity() -> None:
    other_digest = "sha256:" + _HEX_DIGEST[:12] + "f" * 52
    assert other_digest != _DIGEST
    assert schema_module_name(
        "queue", "response", "get", _DIGEST
    ) == schema_module_name("queue", "response", "get", other_digest)


@pytest.mark.parametrize(
    "digest", ["sha256:0123456789ab", _HEX_DIGEST, "sha256:" + "z" * 64]
)
def test_physical_name_requires_a_complete_valid_digest(digest: str) -> None:
    with pytest.raises(ValueError, match="full SHA-256 digest"):
        schema_symbol_name("get", digest)
