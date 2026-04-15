"""DolphinScheduler source-tree helpers for code generation tools."""

from __future__ import annotations

import shutil
import tempfile
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def default_ds_source_root(repo_root: Path) -> Path:
    """Return the default checked-in upstream source tree location."""
    return repo_root / "references" / "dolphinscheduler"


def read_ds_source_version(ds_source_root: Path) -> str:
    """Read the DolphinScheduler project version from the root Maven POM.

    The root POM contains both a parent version and the DS project version.  We
    intentionally read only the direct child ``project/version`` element so the
    reported snapshot version is the DS release tag, not the ASF parent POM.
    """
    pom_path = ds_source_root / "pom.xml"
    if not pom_path.is_file():
        message = f"DolphinScheduler source root is missing pom.xml: {ds_source_root}"
        raise FileNotFoundError(message)

    # The analyzer reads local git worktrees or checked-out release sources.
    # Network XML is never accepted here, so stdlib ElementTree is sufficient.
    root = ET.parse(pom_path).getroot()  # noqa: S314
    namespace = _xml_namespace(root.tag)
    version = root.findtext(_qualified_name("version", namespace))
    if version is None or not version.strip():
        message = f"Unable to read DolphinScheduler version from {pom_path}"
        raise ValueError(message)
    return version.strip()


@contextmanager
def codegen_repo_root_for_ds_source(ds_source_root: Path) -> Iterator[Path]:
    """Yield a temporary repo root that exposes one DS source as references/.

    The existing extraction pipeline is intentionally repo-root based because it
    also runs in the normal checked-out project.  Version analysis often needs
    to point the same extractor at several git worktrees or cloned tags.  This
    context manager keeps that input adaptation outside the extractor itself:
    it creates a small temporary repo root whose ``references/dolphinscheduler``
    path points at the requested source tree.
    """
    resolved_source = ds_source_root.resolve()
    if not resolved_source.is_dir():
        message = f"DolphinScheduler source root does not exist: {ds_source_root}"
        raise FileNotFoundError(message)

    with tempfile.TemporaryDirectory(prefix="dsctl_codegen_source_") as tmp:
        repo_root = Path(tmp)
        references_root = repo_root / "references"
        references_root.mkdir()
        mounted_source = references_root / "dolphinscheduler"
        try:
            mounted_source.symlink_to(resolved_source, target_is_directory=True)
        except OSError:
            shutil.copytree(
                resolved_source,
                mounted_source,
                ignore=shutil.ignore_patterns(
                    ".git",
                    "target",
                    "node_modules",
                    "dist",
                    "build",
                ),
            )
        yield repo_root


def _xml_namespace(tag: str) -> str | None:
    if not tag.startswith("{"):
        return None
    return tag[1:].split("}", 1)[0]


def _qualified_name(name: str, namespace: str | None) -> str:
    if namespace is None:
        return name
    return f"{{{namespace}}}{name}"
