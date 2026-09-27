from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.wire import (
    CompiledWireProfile,
    ValidatedCompiledWireInstallation,
    WireContractError,
    WireExecutionMode,
    WireExecutor,
    WireResultEnvelope,
    installed_compiled_wire_installation,
    load_compiled_wire_profiles,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.client import BinaryResponse, DolphinSchedulerClient, MultipartFiles
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.wire import PreparedCompiledWireCall, WireExecution

PrimitiveT = TypeVar("PrimitiveT", bound=str)


@dataclass(frozen=True)
class CompiledProgramExpectation:
    """Handwritten execution policy for one stable domain primitive."""

    mode: WireExecutionMode
    envelope: WireResultEnvelope
    absent_versions: frozenset[str] = frozenset()
    file_fields: tuple[str, ...] = ()
    response_transport: Literal["json", "binary"] = "json"


READ_RETRY_OPTIONAL = CompiledProgramExpectation(
    mode=WireExecutionMode.READ_RETRY_SAFE,
    envelope=WireResultEnvelope.OPTIONAL,
)
MUTATION_ONCE_REQUIRED = CompiledProgramExpectation(
    mode=WireExecutionMode.MUTATION_ONCE,
    envelope=WireResultEnvelope.REQUIRED,
)


@dataclass(frozen=True)
class CompiledDomainPrograms(Generic[PrimitiveT]):
    """Load and bind one exact compiled-domain program inventory."""

    name: str
    schema_constant: str
    schema_version: int
    expectations: Mapping[PrimitiveT, CompiledProgramExpectation]

    def __post_init__(self) -> None:
        """Freeze the ordered primitive inventory supplied by the domain."""
        if not self.expectations:
            message = "Compiled domain program expectations must be nonempty"
            raise ValueError(message)
        object.__setattr__(
            self,
            "expectations",
            MappingProxyType(dict(self.expectations)),
        )

    def profile(self, ds_version: str) -> CompiledWireProfile:
        """Select an exact profile from this domain's immutable installed inventory."""
        return self._select_profile(self._profiles, ds_version)

    def fresh_profile(self, ds_version: str) -> CompiledWireProfile:
        """Revalidate all artifact evidence without reading or changing caches."""
        return self._select_profile(self._load_profiles(), ds_version)

    @cached_property
    def _profiles(self) -> Mapping[str, CompiledWireProfile]:
        return self._load_profiles(installation=installed_compiled_wire_installation())

    def _select_profile(
        self,
        profiles: Mapping[str, CompiledWireProfile],
        ds_version: str,
    ) -> CompiledWireProfile:
        try:
            return profiles[ds_version]
        except KeyError as exc:
            message = f"DS {ds_version} has no reviewed {self.name} capability decision"
            raise WireContractError(message) from exc

    def _load_profiles(
        self,
        *,
        installation: ValidatedCompiledWireInstallation | None = None,
    ) -> Mapping[str, CompiledWireProfile]:
        primitives: tuple[str, ...] = tuple(self.expectations)
        execution_modes: dict[str, WireExecutionMode] = {
            primitive: expectation.mode
            for primitive, expectation in self.expectations.items()
        }
        result_envelopes: dict[str, WireResultEnvelope] = {
            primitive: expectation.envelope
            for primitive, expectation in self.expectations.items()
        }
        primitive_absent_versions: dict[str, frozenset[str]] = {
            primitive: expectation.absent_versions
            for primitive, expectation in self.expectations.items()
        }
        profiles = load_compiled_wire_profiles(
            self.name,
            schema_constant=self.schema_constant,
            schema_version=self.schema_version,
            target_versions=TARGET_DS_VERSIONS,
            primitives=primitives,
            execution_modes=execution_modes,
            result_envelopes=result_envelopes,
            primitive_absent_versions=primitive_absent_versions,
            installation=installation,
        )
        for profile in profiles.values():
            self._validate_transports(profile)
        return profiles

    def _validate_transports(self, profile: CompiledWireProfile) -> None:
        for primitive, expectation in self.expectations.items():
            program = profile.programs.get(primitive)
            if program is not None and (
                program.codec.file_fields != expectation.file_fields
                or program.codec.response_transport != expectation.response_transport
            ):
                message = (
                    f"Compiled {self.name} {primitive} transport "
                    "does not match its reviewed policy"
                )
                raise WireContractError(message)

    def bind(
        self,
        compiled_profile: CompiledWireProfile,
        selected_profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> BoundCompiledPrograms[PrimitiveT]:
        """Validate version identity and bind an executor to supported programs."""
        self._validate_transports(compiled_profile)
        if compiled_profile.status != "supported":
            message = (
                f"DS {compiled_profile.ds_version} has no compiled {self.name} recipe"
            )
            raise WireContractError(message)
        versions = (
            compiled_profile.ds_version,
            selected_profile.ds_version,
            http_client.profile.ds_version,
        )
        if len(set(versions)) != 1:
            message = (
                f"Compiled {self.name} adapter does not match "
                "the selected client profile"
            )
            raise WireContractError(message)
        return BoundCompiledPrograms(
            ds_version=compiled_profile.ds_version,
            _profile=compiled_profile,
            _executor=WireExecutor(
                http_client,
                source_contract_digest=compiled_profile.source_contract_digest,
            ),
        )


@dataclass(frozen=True)
class BoundCompiledPrograms(Generic[PrimitiveT]):
    """Supported compiled programs bound to one exact client and executor."""

    ds_version: str
    _profile: CompiledWireProfile = field(repr=False)
    _executor: WireExecutor = field(repr=False)

    def prepare(
        self,
        primitive: PrimitiveT,
        args: JsonObject,
    ) -> PreparedCompiledWireCall:
        """Validate parameters before a caller enters its mutation dispatch scope."""
        return self._profile.program(primitive).prepare(args)

    def execute(
        self,
        primitive: PrimitiveT,
        prepared: PreparedCompiledWireCall,
    ) -> WireExecution[OpaqueGeneratedValue]:
        """Execute an already prepared primitive and retain transport evidence."""
        return self._executor.execute(self._profile.program(primitive), prepared)

    def call(
        self,
        primitive: PrimitiveT,
        args: JsonObject,
    ) -> OpaqueGeneratedValue:
        """Prepare and execute one named primitive through its validated program."""
        return self.execute(primitive, self.prepare(primitive, args)).payload

    def upload(
        self,
        primitive: PrimitiveT,
        args: JsonObject,
        *,
        files: MultipartFiles,
    ) -> WireExecution[OpaqueGeneratedValue]:
        """Validate the form and dispatch caller-owned file handles separately."""
        return self._executor.upload(
            self._profile.program(primitive), self.prepare(primitive, args), files=files
        )

    def download(self, primitive: PrimitiveT, args: JsonObject) -> BinaryResponse:
        """Validate the request and retain the raw binary response."""
        return self._executor.download(
            self._profile.program(primitive), self.prepare(primitive, args)
        )


__all__ = [
    "MUTATION_ONCE_REQUIRED",
    "READ_RETRY_OPTIONAL",
    "BoundCompiledPrograms",
    "CompiledDomainPrograms",
    "CompiledProgramExpectation",
]
