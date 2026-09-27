from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_literals import read_compiled_literals
from ds_codegen.compiled_projects import PROJECT_COMPILED_DOMAIN
from live_gate import runtime_ownership

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledPrimitive
    from ds_codegen.ir import OperationSpec


@pytest.mark.parametrize(
    ("version", "name", "method", "path"),
    [
        ("1.3.9", "delete_legacy", "GET", "projects/delete"),
        ("2.0.0", "delete", "DELETE", "projects/{code}"),
    ],
)
def test_static_classification_uses_the_reviewed_delete_transport(
    version: str, name: str, method: str, path: str
) -> None:
    primitive, program, codecs = _project_program(version, name)

    def classify(operation: OperationSpec) -> str | None:
        assert (operation.http_method, operation.path) == (method, path)
        return PROJECT_COMPILED_DOMAIN.classify_operation(operation)

    assert runtime_ownership._program(
        replace(PROJECT_COMPILED_DOMAIN, classify_operation=classify),
        primitive,
        program,
        codecs,
        label=f"project {version}",
    ) == ("ProjectController.deleteProject", name)


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0"])
@pytest.mark.parametrize(
    ("drift", "message"),
    [
        ("method_digest", "program digest"),
        ("method_epoch", "request epoch is not uniquely reviewed"),
        ("source_primitive", "source primitive"),
    ],
)
def test_static_classification_rejects_method_and_source_primitive_tampering(
    version: str, drift: str, message: str
) -> None:
    name = "delete_legacy" if version == "1.3.9" else "delete"
    primitive, program, codecs = _project_program(version, name)
    if drift == "source_primitive":
        program["source_operation"] = "ProjectController.createProject"
    else:
        codecs[name]["method"] = "DELETE" if version == "1.3.9" else "GET"
    if drift != "method_digest":
        payload = {
            "schema_version": 4,
            "codec_record": codecs[name],
            **{key: value for key, value in program.items() if key != "program_digest"},
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
        program["program_digest"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    with pytest.raises(ValueError, match=message):
        runtime_ownership._program(
            PROJECT_COMPILED_DOMAIN,
            primitive,
            program,
            codecs,
            label=f"project {version}",
        )


def _project_program(
    version: str, name: str
) -> tuple[CompiledPrimitive, dict[str, object], dict[str, dict[str, object]]]:
    path = (
        Path(__file__).resolve().parents[2]
        / "src/dsctl/generated/wire_programs/project.py"
    )
    data = read_compiled_literals(path, {"CODECS", "PROFILES"})
    profiles = runtime_ownership._mapping(data["PROFILES"])
    profile = runtime_ownership._mapping(profiles[version])
    programs = runtime_ownership._mapping(profile["programs"])
    program = dict(runtime_ownership._mapping(programs[name]))
    codecs = {
        codec_name: dict(runtime_ownership._mapping(codec))
        for codec_name, codec in runtime_ownership._mapping(data["CODECS"]).items()
    }
    primitive = next(
        item for item in PROJECT_COMPILED_DOMAIN.primitives if item.name == name
    )
    return primitive, program, codecs
