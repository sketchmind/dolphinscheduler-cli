from __future__ import annotations

import hashlib
import json
import runpy
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from ds_codegen.compiled_schema_pool import pool_schema_enums
from ds_codegen.compiled_wire_artifacts import (
    CompiledWireModule,
    render_compiled_wire_artifact,
)
from dsctl.generated import wire_runtime

_LEAF = "_schemas.example.response_get_7192d1014419"
_MODULES = (
    CompiledWireModule(
        "example",
        f"from .{_LEAF} import VALUE\n",
        "domain",
    ),
    CompiledWireModule(_LEAF, "VALUE = 42\n", "support"),
    CompiledWireModule(
        "_schemas.example.request_get_123456789abc", "NAME = 'get'\n", "support"
    ),
)
_EXPECTED_SUPPORT = (
    "_schemas/__init__.py",
    "_schemas/example/__init__.py",
    "_schemas/example/request_get_123456789abc.py",
    "_schemas/example/response_get_7192d1014419.py",
)
_EXPECTED_MODULES = ("__init__.py", *_EXPECTED_SUPPORT, "example.py")
_PROBE = """
import json
import sys
import dsctl.generated
from dsctl.upstream.wire import validate_compiled_wire_installation
assert not any(name.startswith("dsctl.generated.wire_programs") for name in sys.modules)
for name in tuple(sys.modules):
    if name.startswith("dsctl.generated.wire_runtime"):
        del sys.modules[name]
dsctl.generated.__path__ = [sys.argv[1]]
installation = validate_compiled_wire_installation()
module = installation.load_module("example")
print(json.dumps({"domains": installation.domain_modules, "value": module.VALUE}))
"""


def _output_root(tmp_path: Path) -> Path:
    assert wire_runtime.__file__ is not None
    output = tmp_path / "dsctl"
    shutil.copytree(
        Path(wire_runtime.__file__).parent,
        output / "generated" / "wire_runtime",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    return output


def _render(tmp_path: Path) -> Path:
    output = _output_root(tmp_path)
    render_compiled_wire_artifact(_MODULES, output)
    return output


def _probe(generated_root: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter and owned probe source
        [sys.executable, "-B", "-c", _PROBE, generated_root],
        check=False,
        capture_output=True,
        text=True,
    )


def test_nested_schema_packages_are_complete_sorted_and_content_bound(
    tmp_path: Path,
) -> None:
    output = _render(tmp_path)
    package = output / "generated" / "wire_programs"
    manifest = runpy.run_path(str(package / "_manifest.py"))
    assert manifest["COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION"] == 5
    assert manifest["RENDERER_ABI"] == 15
    assert manifest["MODULES"] == _EXPECTED_MODULES
    assert manifest["SUPPORT_MODULES"] == _EXPECTED_SUPPORT
    assert manifest["DOMAIN_MODULES"] == ("example.py",)
    for name in ("__init__.py", *_EXPECTED_SUPPORT[:2]):
        assert (package / name).read_text() == "from __future__ import annotations\n"
    for module in _MODULES:
        assert (package / f"{module.name.replace('.', '/')}.py").read_text() == (
            module.content
        )
    digest = hashlib.sha256(b"dsctl-compiled-wire-artifact-v5\0")
    for name in _EXPECTED_MODULES:
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update((package / name).read_bytes())
        digest.update(b"\0")
    assert manifest["CONTENT_DIGEST"] == f"sha256:{digest.hexdigest()}"
    before = (package / "_manifest.py").read_bytes()
    render_compiled_wire_artifact(tuple(reversed(_MODULES)), output)
    assert (package / "_manifest.py").read_bytes() == before


@pytest.mark.parametrize("installation", ["directory", "wheel"])
def test_runtime_loads_nested_schema_packages_from_isolated_installation(
    tmp_path: Path, installation: str
) -> None:
    output = _render(tmp_path)
    location = str(output / "generated")
    if installation == "wheel":
        wheel = tmp_path / "fixture.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            for path in sorted((output / "generated").rglob("*.py")):
                archive.write(path, path.relative_to(tmp_path).as_posix())
        location = f"{wheel}/dsctl/generated"
    result = _probe(location)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"domains": ["example.py"], "value": 42}


@pytest.mark.parametrize(
    ("relative_path", "drift", "message"),
    [
        ("_schemas/__init__.py", "missing", "inventory does not match"),
        ("_schemas/example/__init__.py", "missing", "inventory does not match"),
        (_EXPECTED_SUPPORT[-1], "missing", "inventory does not match"),
        ("example_old_response.py", "extra", "inventory does not match"),
        ("_schemas/example/__init__.py", "changed", "content digest does not match"),
        (_EXPECTED_SUPPORT[-1], "changed", "content digest does not match"),
    ],
)
def test_nested_installation_rejects_missing_extra_or_changed_module_bytes(
    tmp_path: Path, relative_path: str, drift: str, message: str
) -> None:
    output = _render(tmp_path)
    path = output / "generated" / "wire_programs" / relative_path
    if drift == "missing":
        path.unlink()
    else:
        path.write_text("VALUE = 99\n", encoding="utf-8")
    result = _probe(str(output / "generated"))
    assert result.returncode != 0
    assert message in result.stderr


@pytest.mark.parametrize("drift", ["missing", "changed"])
def test_enum_pool_is_manifest_owned_and_tampering_prevents_execution(
    tmp_path: Path, drift: str
) -> None:
    pooled = pool_schema_enums(
        "from __future__ import annotations\n"
        "from enum import StrEnum\n"
        "class State(StrEnum):\n    FIRST = 'first'\n"
        "VALUE = State.FIRST\n",
        module_parts=("wire_programs", "_schemas", "example", "response"),
    )
    assert len(pooled.modules) == 1
    output = _output_root(tmp_path)
    root = CompiledWireModule(_LEAF, pooled.source, "support")
    domain = CompiledWireModule("example", f"from .{_LEAF} import VALUE\n", "domain")
    render_compiled_wire_artifact((domain, root, *pooled.modules), output)
    generated = output / "generated"
    result = _probe(str(generated))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["value"] == "first"
    pool_path = pooled.modules[0].name.replace(".", "/") + ".py"
    manifest = runpy.run_path(str(generated / "wire_programs" / "_manifest.py"))
    assert pool_path in manifest["SUPPORT_MODULES"]
    assert "_schemas/_enums/__init__.py" in manifest["SUPPORT_MODULES"]
    path = generated / "wire_programs" / pool_path
    if drift == "missing":
        path.unlink()
    else:
        path.write_text("raise RuntimeError('must not execute candidate')\n")
    result = _probe(str(generated))
    assert result.returncode != 0
    expected = (
        "inventory does not match"
        if drift == "missing"
        else "content digest does not match"
    )
    assert expected in result.stderr
    assert "must not execute candidate" not in result.stderr


@pytest.mark.parametrize(
    "name",
    [
        "../escape",
        "/absolute",
        "_schemas/example/model",
        "_schemas\\example\\model",
        "_schemas..model",
        ".model",
        "model.",
        "_schemas.Example.model",
        "_schemas.exämple.model",
        "_schemas.class.model",
        "__init__",
        "_manifest",
        "_schemas.__init__",
        "_schemas.__pycache__.model",
        "_schemas._manifest.model",
    ],
)
def test_unsafe_module_names_fail_before_replacing_existing_output(
    tmp_path: Path, name: str
) -> None:
    root = tmp_path / "generated" / "wire_programs"
    root.mkdir(parents=True)
    marker = root / "unchanged.txt"
    marker.write_text("keep", encoding="utf-8")
    modules = (
        CompiledWireModule("example", "VALUE = 42\n", "domain"),
        CompiledWireModule(name, "VALUE = 42\n", "support"),
    )
    with pytest.raises(ValueError, match="compiled wire module is invalid"):
        render_compiled_wire_artifact(modules, tmp_path)
    assert marker.read_text() == "keep"
    assert tuple(root.iterdir()) == (marker,)


@pytest.mark.parametrize(
    ("modules", "message"),
    [
        (
            (CompiledWireModule("example.domain", "VALUE = 42\n", "domain"),),
            "module is invalid",
        ),
        ((_MODULES[0], _MODULES[0]), "uniquely named"),
        (
            (
                _MODULES[0],
                CompiledWireModule("example.schema", "VALUE = 42\n", "support"),
            ),
            "conflict with support packages",
        ),
        (
            (
                *_MODULES,
                CompiledWireModule("_schemas.example", "VALUE = 42\n", "support"),
            ),
            "conflict with support packages",
        ),
    ],
)
def test_module_package_collisions_and_nested_domains_are_rejected(
    tmp_path: Path, modules: tuple[CompiledWireModule, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        render_compiled_wire_artifact(modules, tmp_path)
    assert not (tmp_path / "generated").exists()
