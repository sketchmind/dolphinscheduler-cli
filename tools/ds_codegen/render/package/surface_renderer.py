"""Render generated package entrypoints and namespace exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ds_codegen.render.requests_example import _snake_case

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.render.package.planner import PackageRenderContext


def write_init_files(package_root: Path) -> None:
    client_name = client_class_name_from_package_root(package_root)
    init_paths = {
        package_root.parent.parent / "__init__.py": "",
        package_root.parent / "__init__.py": "",
        package_root / "__init__.py": (
            f'from .client import {client_name}\n\n__all__ = ["{client_name}"]\n'
        ),
    }
    for path, content in init_paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


def write_recursive_package_inits(
    package_root: Path,
    package_exports: dict[tuple[str, ...], dict[str, list[str]]],
) -> None:
    for directory in sorted(path for path in package_root.rglob("*") if path.is_dir()):
        init_path = directory / "__init__.py"
        if init_path.exists():
            continue
        relative_parts = directory.relative_to(package_root).parts
        exports_by_module = package_exports.get(relative_parts)
        if exports_by_module:
            init_path.write_text(render_package_init(exports_by_module))
            continue
        init_path.write_text("")


def render_package_init(exports_by_module: dict[str, list[str]]) -> str:
    lines = ["from __future__ import annotations", ""]
    exported_names: list[str] = []
    for module_name, names in sorted(exports_by_module.items()):
        if not names:
            continue
        import_names = ", ".join(names)
        lines.append(f"from .{module_name} import {import_names}")
        exported_names.extend(names)
    if lines[-1] != "":
        lines.append("")
    lines.append(
        "__all__ = [" + ", ".join(f'"{name}"' for name in exported_names) + "]"
    )
    lines.append("")
    return "\n".join(lines)


def write_client_module(
    package_root: Path,
    version_package_parts: tuple[str, ...],
    context: PackageRenderContext,
) -> None:
    operations_by_controller = sorted(
        {operation.controller for operation in context.snapshot.operations}
    )
    client_path = package_root / "client.py"
    client_path.write_text(
        render_client_module(
            version_package_parts=version_package_parts,
            controller_names=operations_by_controller,
        )
    )


def render_client_module(
    *,
    version_package_parts: tuple[str, ...],
    controller_names: list[str],
) -> str:
    client_name = client_class_name(version_package_parts)
    import_lines: list[str] = []
    namespace_lines: list[str] = []
    for controller_name in controller_names:
        module_name = controller_module_name(controller_name)
        mixin_name = controller_operations_class_name(controller_name)
        import_lines.append(f"from .api.operations.{module_name} import {mixin_name}")
        namespace_lines.append(f"        self.{module_name} = {mixin_name}(")
        namespace_lines.append("            self.base_url,")
        namespace_lines.append("            self.token,")
        namespace_lines.append("            session=self._session,")
        namespace_lines.append("        )")
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            "from .api.operations._base import SessionLike",
            "",
            *import_lines,
            "",
            f"class {client_name}:",
            "    def __init__(",
            "        self,",
            "        base_url: str,",
            "        token: str,",
            "        *,",
            "        session: SessionLike | None = None,",
            "    ) -> None:",
            '        self.base_url = base_url.rstrip("/")',
            "        self.token = token",
            "        self._session = session",
            *namespace_lines,
            "",
            f'__all__ = ["{client_name}"]',
            "",
        ]
    )


def client_class_name(version_package_parts: tuple[str, ...]) -> str:
    version_slug = version_package_parts[-1]
    digits = "".join(ch for ch in version_slug if ch.isdigit())
    return f"DS{digits}Client"


def client_class_name_from_package_root(package_root: Path) -> str:
    return client_class_name(tuple(package_root.parts[-3:]))


def controller_module_name(controller_name: str) -> str:
    name = controller_name.removesuffix("Controller")
    return _snake_case(name)


def controller_operations_class_name(controller_name: str) -> str:
    base_name = controller_name.removesuffix("Controller")
    return f"{base_name}Operations"
