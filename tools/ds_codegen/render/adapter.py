from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot


def write_upstream_adapter_package(
    snapshot: ContractSnapshot,
    output_root: Path,
) -> None:
    upstream_root = output_root / "upstream"
    if upstream_root.exists():
        shutil.rmtree(upstream_root)
    upstream_root.mkdir(parents=True, exist_ok=True)

    version_slug = _version_slug(snapshot.ds_version)
    client_class_name = _client_class_name(snapshot.ds_version)
    adapter_class_name = _adapter_class_name(snapshot.ds_version)

    _write_file(
        upstream_root / "__init__.py",
        "\n".join(
            [
                "from .protocol import UpstreamAdapter",
                (
                    "from .registry import "
                    "SUPPORTED_VERSIONS, get_adapter, normalize_version"
                ),
                "",
                (
                    '__all__ = ["UpstreamAdapter", "SUPPORTED_VERSIONS", '
                    '"get_adapter", "normalize_version"]'
                ),
                "",
            ]
        ),
    )
    _write_file(
        upstream_root / "protocol.py",
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "from typing import Protocol, TypeVar",
                "",
                "import requests",
                "",
                'ClientT = TypeVar("ClientT")',
                "",
                "class UpstreamAdapter(Protocol[ClientT]):",
                "    ds_version: str",
                "    version_slug: str",
                "    client_class: type[ClientT]",
                "",
                "    def create_client(",
                "        self,",
                "        base_url: str,",
                "        token: str,",
                "        *,",
                "        session: requests.Session | None = None,",
                "    ) -> ClientT: ...",
                "",
                '__all__ = ["UpstreamAdapter"]',
                "",
            ]
        ),
    )
    _write_file(
        upstream_root / "adapters" / "__init__.py",
        "\n".join(
            [
                f"from .{version_slug} import {adapter_class_name}",
                "",
                f'__all__ = ["{adapter_class_name}"]',
                "",
            ]
        ),
    )
    _write_file(
        upstream_root / "adapters" / f"{version_slug}.py",
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "from dataclasses import dataclass",
                "",
                "import requests",
                "",
                f"from generated.versions.{version_slug} import {client_class_name}",
                "from upstream.protocol import UpstreamAdapter",
                "",
                "@dataclass(frozen=True)",
                f"class {adapter_class_name}(UpstreamAdapter[{client_class_name}]):",
                f'    ds_version: str = "{snapshot.ds_version}"',
                f'    version_slug: str = "{version_slug}"',
                f"    client_class: type[{client_class_name}] = {client_class_name}",
                "",
                "    def create_client(",
                "        self,",
                "        base_url: str,",
                "        token: str,",
                "        *,",
                "        session: requests.Session | None = None,",
                f"    ) -> {client_class_name}:",
                "        return self.client_class(",
                "            base_url,",
                "            token,",
                "            session=session,",
                "        )",
                "",
                f'__all__ = ["{adapter_class_name}"]',
                "",
            ]
        ),
    )
    _write_file(
        upstream_root / "registry.py",
        _render_registry_module(snapshot.ds_version, version_slug, adapter_class_name),
    )


def _version_slug(ds_version: str) -> str:
    return f"ds_{ds_version.replace('.', '_')}"


def _client_class_name(ds_version: str) -> str:
    digits = "".join(ch for ch in ds_version if ch.isdigit())
    return f"DS{digits}Client"


def _adapter_class_name(ds_version: str) -> str:
    digits = "".join(ch for ch in ds_version if ch.isdigit())
    return f"DS{digits}Adapter"


def _write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def _render_registry_module(
    ds_version: str,
    version_slug: str,
    adapter_class_name: str,
) -> str:
    version_constant = f"_DS_{ds_version.replace('.', '_')}"
    adapter_mapping_line = f'    "{ds_version}": {version_constant},'
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            f"from .adapters.{version_slug} import {adapter_class_name}",
            "from .protocol import UpstreamAdapter",
            "",
            f"{version_constant} = {adapter_class_name}()",
            "",
            "_ADAPTERS_BY_VERSION: dict[str, UpstreamAdapter[object]] = {",
            adapter_mapping_line,
            "}",
            "",
            "SUPPORTED_VERSIONS = tuple(sorted(_ADAPTERS_BY_VERSION))",
            "",
            "def normalize_version(version: str) -> str:",
            "    normalized = version.strip().lower()",
            '    if normalized.startswith("v"):',
            "        normalized = normalized[1:]",
            '    if normalized.startswith("ds_"):',
            "        normalized = normalized[3:]",
            '    normalized = normalized.replace("_", ".")',
            "    return normalized",
            "",
            "def get_adapter(version: str) -> UpstreamAdapter[object]:",
            "    normalized = normalize_version(version)",
            "    try:",
            "        return _ADAPTERS_BY_VERSION[normalized]",
            "    except KeyError as exc:",
            '        supported = ", ".join(SUPPORTED_VERSIONS)',
            "        raise KeyError(",
            (
                '            f"Unsupported DS version {version!r}. '
                'Supported versions: {supported}"'
            ),
            "        ) from exc",
            "",
            '__all__ = ["SUPPORTED_VERSIONS", "get_adapter", "normalize_version"]',
            "",
        ]
    )
