"""Shared safety mechanics for installed-wheel live gates."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import tempfile
import venv
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Iterator

PROFILE_ENV_NAMES = frozenset(
    {
        "DS_API_URL",
        "DS_API_TOKEN",
        "DS_API_RETRY_ATTEMPTS",
        "DS_API_RETRY_BACKOFF_MS",
        "DS_VERSION",
    }
)


@dataclass(frozen=True)
class GateFiles:
    """Mutable inputs snapshotted by an installed-wheel live gate."""

    wheel: Path
    env_file: Path
    attestation_key_file: Path
    cluster_manifest: Path
    fixture_manifest: Path
    evidence: Path


@dataclass(frozen=True)
class GateValidationMessages:
    """Runner-specific wording for common installed-wheel input checks."""

    missing_template: str
    wheel: str
    profile_private: str
    attestation_private: str
    overwrite_template: str


@dataclass(frozen=True)
class InstalledWheel:
    """Binaries from one wheel installed into a private virtual environment."""

    python: Path
    executable: Path


@contextmanager
def private_workspace(*, prefix: str) -> Iterator[Path]:
    """Yield a mode-0700 temporary workspace and remove it on exit."""
    with tempfile.TemporaryDirectory(prefix=prefix) as raw_workspace:
        workspace = Path(raw_workspace)
        workspace.chmod(0o700)
        yield workspace


def validate_gate_files(
    files: GateFiles,
    *,
    messages: GateValidationMessages,
) -> None:
    """Apply the shared file, mode, artifact, and no-overwrite checks."""
    for label, path in (
        ("wheel", files.wheel),
        ("env file", files.env_file),
        ("attestation key file", files.attestation_key_file),
        ("cluster manifest", files.cluster_manifest),
        ("fixture manifest", files.fixture_manifest),
    ):
        if not path.is_file():
            message = messages.missing_template.format(label=label, path=path)
            raise FileNotFoundError(message)
    if files.wheel.suffix != ".whl":
        raise ValueError(messages.wheel)
    for path, message in (
        (files.env_file, messages.profile_private),
        (files.attestation_key_file, messages.attestation_private),
    ):
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise PermissionError(message)
    if files.evidence.exists():
        message = messages.overwrite_template.format(path=files.evidence)
        raise FileExistsError(message)


def snapshot_gate_files(
    files: GateFiles,
    destination: Path,
    *,
    changed_template: str,
) -> GateFiles:
    """Copy mutable inputs once into a private, stable directory tree."""
    artifact_dir = destination / "artifact"
    manifest_dir = destination / "manifests"
    artifact_dir.mkdir(parents=True, mode=0o700)
    manifest_dir.mkdir(parents=True, mode=0o700)
    snapshot = GateFiles(
        wheel=artifact_dir / files.wheel.name,
        env_file=destination / "profile.env",
        attestation_key_file=destination / "attestation.key",
        cluster_manifest=manifest_dir / "cluster.json",
        fixture_manifest=manifest_dir / "fixture.json",
        evidence=files.evidence,
    )
    for source, target in (
        (files.wheel, snapshot.wheel),
        (files.env_file, snapshot.env_file),
        (files.attestation_key_file, snapshot.attestation_key_file),
        (files.cluster_manifest, snapshot.cluster_manifest),
        (files.fixture_manifest, snapshot.fixture_manifest),
    ):
        _copy_stable(source, target, changed_template=changed_template)
        target.chmod(0o600)
    return snapshot


def install_wheel(
    wheel: Path,
    workspace: Path,
    *,
    missing_executable_message: str,
    executable_name: str = "dsctl",
) -> InstalledWheel:
    """Install a wheel into an isolated venv and locate its executable."""
    venv_dir = workspace / "venv"
    venv.EnvBuilder(
        with_pip=True,
        system_site_packages=False,
        clear=True,
    ).create(venv_dir)
    python = venv_binary(venv_dir, "python")
    executable = venv_binary(venv_dir, executable_name)
    subprocess.run(  # noqa: S603 -- executable is the private gate venv
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--force-reinstall",
            str(wheel),
        ],
        check=True,
        cwd=workspace,
        env=isolated_environment(),
    )
    if not executable.is_file():
        raise FileNotFoundError(missing_executable_message)
    return InstalledWheel(python=python, executable=executable)


def isolated_environment() -> dict[str, str]:
    """Remove ambient source, target selection, policy, and live-gate controls."""
    return {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONHOME", "PYTHONPATH"}
        and not key.startswith(("DS_", "DSCTL_"))
    }


def bind_direct_api_target(
    environment: dict[str, str],
    env_file: Path,
    *,
    invalid_url_message: str,
) -> None:
    """Add the selected profile API host to both no-proxy spellings."""
    host = profile_api_host(env_file, invalid_url_message=invalid_url_message)
    entries: list[str] = []
    for name in ("NO_PROXY", "no_proxy"):
        for entry in environment.get(name, "").split(","):
            normalized = entry.strip()
            if normalized and normalized not in entries:
                entries.append(normalized)
    if host not in entries:
        entries.append(host)
    value = ",".join(entries)
    environment["NO_PROXY"] = value
    environment["no_proxy"] = value


def profile_api_host(env_file: Path, *, invalid_url_message: str) -> str:
    """Return the hostname from one paired-quote-aware profile API URL."""
    api_url = read_env_file(env_file).get("DS_API_URL", "")
    host = urlsplit(api_url).hostname
    if host:
        return host
    raise ValueError(invalid_url_message)


def read_env_file(path: Path) -> dict[str, str]:
    """Read the small dotenv subset used by gate connection profiles."""
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", maxsplit=1)
        values[key.strip()] = _strip_optional_quotes(value.strip())
    return values


def publish_passing_evidence(
    candidate: Path,
    destination: Path,
    *,
    invalid_candidate_message: str,
) -> None:
    """Publish a passing receipt atomically without replacing an existing one."""
    payload = json.loads(candidate.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("status") != "passed":
        raise ValueError(invalid_candidate_message)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
        stream.write(candidate.read_bytes())
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def venv_binary(venv_dir: Path, name: str) -> Path:
    """Resolve one cross-platform virtual-environment binary path."""
    scripts = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return venv_dir / scripts / f"{name}{suffix}"


def _copy_stable(
    source: Path,
    destination: Path,
    *,
    changed_template: str,
) -> None:
    digest_before = _file_sha256(source)
    shutil.copyfile(source, destination)
    digest_after = _file_sha256(source)
    digest_copy = _file_sha256(destination)
    if digest_before != digest_after or digest_after != digest_copy:
        destination.unlink(missing_ok=True)
        message = changed_template.format(path=source)
        raise RuntimeError(message)


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value
