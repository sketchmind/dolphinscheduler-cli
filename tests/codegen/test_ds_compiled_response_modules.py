from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import replace

import pytest

from ds_codegen.compiled_domains import (
    CompiledDomainPlan,
    CompiledDomainSet,
    CompiledProfile,
    CompiledProgram,
    CompiledRequest,
    CompiledResponseModule,
    _compile_domain_modules,
    _render_domain_module,
)
from ds_codegen.compiled_queues import QUEUE_COMPILED_DOMAIN

_DIGEST = "sha256:" + "1" * 64
_READ_MODULE = "_schemas.queue.response_read_111111111111"
_WRITE_MODULE = "_schemas.queue.response_write_111111111111"
_CONTENT = (
    "from pydantic import BaseModel\n"
    "class Payload(BaseModel):\n"
    "    value: int\n"
    "RESPONSE_TYPE = Payload\n"
    f"EXECUTABLE_SCHEMA_DIGEST = {_DIGEST!r}\n"
)


def _plan(*responses: CompiledResponseModule) -> CompiledDomainPlan:
    program = CompiledProgram(
        source_operation="ProbeController.read",
        codec="read_codec",
        codec_digest=_DIGEST,
        request_schema_digest=_DIGEST,
        response_digest=_DIGEST,
        response_schema_digest=_DIGEST,
        result_envelope="optional",
        program_digest=_DIGEST,
    )
    return CompiledDomainPlan(
        definition=QUEUE_COMPILED_DOMAIN,
        profiles=(
            CompiledProfile(
                version="3.4.1",
                status="supported",
                source_tag="3.4.1",
                source_commit="2" * 40,
                source_tree="3" * 40,
                source_contract_digest=_DIGEST,
                recipe_id="probe",
                programs=(("read", program),),
            ),
        ),
        requests=(),
        responses=responses,
        scalar_responses=(),
        codecs=(("read_codec", {"response": "read_schema"}),),
    )


def _response(schema: str, module: str) -> CompiledResponseModule:
    return CompiledResponseModule(
        schema=schema,
        module_name=module,
        root_annotation="Payload",
        executable_digest=_DIGEST,
        content=_CONTENT,
    )


def _assignments(source: str) -> dict[str, ast.expr]:
    return {
        node.targets[0].id: node.value
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
    }


def test_identical_responses_share_physical_modules_without_mutating_plan() -> None:
    first = _response("read_schema", _READ_MODULE)
    second = _response("write_schema", _WRITE_MODULE)
    plan = _plan(second, first)
    original = deepcopy(plan)
    before = _assignments(_render_domain_module(plan))

    domain, support = _compile_domain_modules(plan)

    assert domain.name == "queue"
    assert support.name == _READ_MODULE
    assert support.content == _CONTENT
    assert domain.content.count(f"from .{_READ_MODULE} import (") == 2
    assert f"from .{_WRITE_MODULE} import (" not in domain.content
    after = _assignments(domain.content)
    assert set(after) == set(before)
    assert all(
        ast.dump(after[name]) == ast.dump(value) for name, value in before.items()
    )
    assert plan == original
    assert plan.responses == (second, first)
    assert plan.responses[0] is second
    assert plan.responses[1] is first


def test_different_complete_content_keeps_unique_module_bytes_unchanged() -> None:
    first = _response("read_schema", _READ_MODULE)
    second = replace(
        _response("write_schema", _WRITE_MODULE),
        content=_CONTENT + "# Different complete module content.\n",
    )
    plan = _plan(first, second)

    domain, *support = _compile_domain_modules(plan)

    assert domain.content == _render_domain_module(plan)
    assert [(item.name, item.content) for item in support] == [
        (first.module_name, first.content),
        (second.module_name, second.content),
    ]


def test_conflicting_response_module_names_cannot_overwrite_content() -> None:
    first = _response("read_schema", _READ_MODULE)
    second = replace(first, schema="write_schema", content=_CONTENT + "# changed\n")

    with pytest.raises(
        ValueError, match=r"module .*response_read_111111111111 has conflicting"
    ):
        _compile_domain_modules(_plan(first, second))


def test_same_short_name_cannot_hide_a_different_full_schema_digest() -> None:
    first = _response(f"read_{_DIGEST.removeprefix('sha256:')}", _READ_MODULE)
    different_digest = "sha256:" + "1" * 12 + "2" * 52
    second = replace(
        first,
        schema=f"read_{different_digest.removeprefix('sha256:')}",
        executable_digest=different_digest,
        content=_CONTENT.replace(_DIGEST, different_digest),
    )

    with pytest.raises(ValueError, match="short-name collision"):
        _compile_domain_modules(_plan(first, second))


def test_identical_response_content_is_not_shared_across_domains() -> None:
    first = _plan(_response("read_schema", _READ_MODULE))
    other_module = "_schemas.other.response_read_111111111111"
    second = replace(
        _plan(_response("read_schema", other_module)),
        definition=replace(QUEUE_COMPILED_DOMAIN, name="other"),
    )

    modules = CompiledDomainSet(plans=(first, second), legacy_bundles=()).modules()

    assert [item.name for item in modules if item.kind == "support"] == [
        _READ_MODULE,
        other_module,
    ]


def test_inline_request_epochs_keep_distinct_static_models_and_exact_defaults() -> None:
    first = CompiledRequest(
        schema="create_first",
        class_name="CreateParams",
        source="class CreateParams(BaseParamsModel):\n    value: int = 1\n",
        executable_digest=_DIGEST,
        content_addressed=True,
    )
    second = replace(
        first, schema="create_second", source=first.source.replace("= 1", "= 2")
    )
    original = replace(_plan(), requests=(first, second))
    source = _render_domain_module(original)
    classes = [
        node.name for node in ast.parse(source).body if isinstance(node, ast.ClassDef)
    ]
    assert classes == [
        "_create_first_111111111111_request_types",
        "_create_second_111111111111_request_types",
    ]
    namespace: dict[str, object] = {}
    exec(compile(source, "<inline-request-epochs>", "exec"), namespace)  # noqa: S102
    schemas = namespace["REQUEST_SCHEMAS"]
    assert isinstance(schemas, dict)
    assert schemas["create_first"].model().model_dump() == {"value": 1}
    assert schemas["create_second"].model().model_dump() == {"value": 2}
    assert schemas["create_first"].model is not schemas["create_second"].model
    assert schemas["create_first"].model.__name__ == "CreateParams"
    assert schemas["create_second"].model.model_json_schema()["title"] == "CreateParams"
    assert original.requests == (first, second)
