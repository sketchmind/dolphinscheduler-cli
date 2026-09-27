from __future__ import annotations

import importlib
from copy import deepcopy

import pytest

from dsctl.upstream.environments import _ENVIRONMENT_PROGRAMS
from dsctl.upstream.wire import WireContractError


def test_compiled_loader_binds_request_schema_ids_independent_of_primitives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.environment")
    assert module.CODECS["get_modern"]["params"] == "code"
    assert module.CODECS["delete_void"]["params"] == "code"
    codecs = deepcopy(module.CODECS)
    codecs["get_modern"]["params"] = "create"
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError, match="get_modern request schema is invalid"):
        _ENVIRONMENT_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_loader_rejects_an_unreachable_request_schema_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.environment")
    schemas = dict(module.REQUEST_SCHEMAS)
    schemas["unused"] = schemas["code"]
    monkeypatch.setattr(module, "REQUEST_SCHEMAS", schemas)

    with pytest.raises(WireContractError, match="request-schema inventory"):
        _ENVIRONMENT_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize("params", [None, "missing"])
def test_compiled_loader_rejects_missing_or_unknown_request_schema_zero_io(
    params: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.environment")
    codecs = deepcopy(module.CODECS)
    if params is None:
        del codecs["get_modern"]["params"]
        message = "unsupported fields"
    else:
        codecs["get_modern"]["params"] = params
        message = "parameters are invalid|request schema is invalid"
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError, match=message):
        _ENVIRONMENT_PROGRAMS.fresh_profile("3.4.1")
