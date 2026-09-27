from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

from dsctl.models.task_spec import (
    TaskParamsSpec,
    task_family_model,
)
from dsctl.services._task_field_structure import field_structure

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.models.common import YamlObject, YamlValue


def _family_model(task_type: str) -> type[TaskParamsSpec]:
    family = task_family_model(task_type)
    if family is None:
        message = f"Unregistered task family {task_type!r}"
        raise ValueError(message)
    return family.params_model


class TaskAuthoringIntent(StrEnum):
    """Ways callers may consume one task authoring contract."""

    TYPED_CREATE = "typed_create"
    TYPED_EDIT = "typed_edit"
    OPAQUE_CREATE = "opaque_create"
    OPAQUE_EDIT = "opaque_edit"
    OPAQUE_PRESERVE = "opaque_preserve"


class _MissingDefault:
    """Sentinel for catalog fields without a default."""


_MISSING = _MissingDefault()


@dataclass(frozen=True, slots=True)
class TaskAuthoringField:
    """One stable authoring field projected into task-type schema views."""

    path: str
    value_type: str
    required: bool = False
    default: YamlValue | _MissingDefault = _MISSING
    choices: tuple[str, ...] = ()
    active_when: str | None = None
    choice_source: str | None = None
    related_commands: tuple[str, ...] = ()
    compile_path: str | None = None
    description: str = ""
    _model_fields: frozenset[str] = frozenset()

    def bind_model(self, model: type[TaskParamsSpec] | None) -> TaskAuthoringField:
        """Resolve requested structural facts from the selected facet model."""
        if not self._model_fields:
            return self
        if model is None:
            message = f"{self.path} requires an explicit typed parameter model"
            raise ValueError(message)
        shape = field_structure(model, self.path)
        if "default" in self._model_fields and not shape.has_default:
            message = f"{self.path} has no model default to project"
            raise ValueError(message)
        return replace(
            self,
            value_type=shape.value_type
            if "type" in self._model_fields
            else self.value_type,
            required=shape.required
            if "required" in self._model_fields
            else self.required,
            choices=shape.choices if "choices" in self._model_fields else self.choices,
            default=deepcopy(shape.default)
            if "default" in self._model_fields
            else self.default,
            _model_fields=frozenset(),
        )

    def to_data(self) -> YamlObject:
        """Return one JSON-safe field projection."""
        data: YamlObject = {
            "path": self.path,
            "type": self.value_type,
            "required": self.required,
            "description": self.description,
        }
        if self.default is not _MISSING:
            data["default"] = deepcopy(cast("YamlValue", self.default))
        if self.choices:
            data["choices"] = list(self.choices)
        if self.active_when is not None:
            data["active_when"] = self.active_when
        if self.choice_source is not None:
            data["choice_source"] = self.choice_source
        if self.related_commands:
            data["related_commands"] = list(self.related_commands)
        if self.compile_path is not None:
            data["compile_path"] = self.compile_path
        return data


def model_field(
    path: str,
    value_type: str | None = None,
    *,
    required: bool | None = None,
    choices: tuple[str, ...] | None = None,
    default: YamlValue | _MissingDefault = _MISSING,
    model_default: bool = False,
    active_when: str | None = None,
    choice_source: str | None = None,
    related_commands: tuple[str, ...] = (),
    compile_path: str | None = None,
    description: str = "",
) -> TaskAuthoringField:
    """Describe executor semantics while the facet model owns shared structure.

    Explicit type/required/choice overrides retain reviewed semantics that differ
    from the Pydantic field. Default visibility remains an explicit discovery choice.
    """
    derived = {
        name
        for name, value in (
            ("type", value_type),
            ("required", required),
            ("choices", choices),
        )
        if value is None
    }
    if model_default:
        if default is not _MISSING:
            message = "A model default cannot coexist with a discovery default"
            raise ValueError(message)
        derived.add("default")
    return TaskAuthoringField(
        path,
        value_type or "",
        required=False if required is None else required,
        choices=choices or (),
        default=default,
        active_when=active_when,
        choice_source=choice_source,
        related_commands=related_commands,
        compile_path=compile_path,
        description=description,
        _model_fields=frozenset(derived),
    )


@dataclass(frozen=True, slots=True)
class TaskAuthoringStateRule:
    """One task-facet state rule projected into discovery output."""

    when: str
    condition_paths: tuple[str, ...]
    active_paths: tuple[str, ...]
    inactive_paths: tuple[str, ...] = ()
    compile_policy: tuple[tuple[str, str], ...] = ()
    description: str = ""

    def to_data(self) -> YamlObject:
        """Return one JSON-safe state-rule projection."""
        return {
            "when": self.when,
            "condition_paths": list(self.condition_paths),
            "active_paths": list(self.active_paths),
            "inactive_paths": list(self.inactive_paths),
            "compile_policy": dict(self.compile_policy),
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class TaskAuthoringTemplate:
    """One scenario or optional field example owned by an authoring facet."""

    name: str
    summary: str
    yaml: str
    payload_modes: tuple[str, ...]
    parameter_fields: tuple[str, ...] = ()
    resource_fields: tuple[str, ...] = ()
    purpose: Literal["scenario", "option"] = "scenario"

    def render(self) -> str:
        """Return the stable YAML fragment for this template variant."""
        return self.yaml


@dataclass(frozen=True, slots=True)
class TaskAuthoringFacetContract:
    """Reviewed, family-neutral semantics for one meaningful task capability."""

    facet_id: str
    family: str
    review: str
    params_model: type[TaskParamsSpec] | None
    fields: tuple[TaskAuthoringField, ...]
    state_rules: tuple[TaskAuthoringStateRule, ...]
    templates: tuple[TaskAuthoringTemplate, ...]
    parameter_data_types: tuple[str, ...] | None = None
    parameter_directions: tuple[str, ...] | None = None
    allow_local_out_without_var_pool: bool = False
    runtime_only_fields: tuple[str, ...] = ()
    opaque_authoring_selector: Callable[[Mapping[str, YamlValue]], bool] | None = None
    restrict_opaque_authoring_to_selector: bool = False

    def __post_init__(self) -> None:
        """Materialize structural facts once using this exact facet's model."""
        object.__setattr__(
            self,
            "fields",
            tuple(item.bind_model(self.params_model) for item in self.fields),
        )


@dataclass(frozen=True, slots=True)
class TaskAuthoringFacetMembership:
    """One exact profile's explicit membership in a reviewed task facet."""

    profile_version: str
    contract: TaskAuthoringFacetContract
    typed_create: bool
    typed_edit: bool
    opaque_create: bool
    opaque_edit: bool
    opaque_preserve: bool
    constraint: str | None = None
    runtime_only_fields: tuple[str, ...] = ()

    def supports(self, intent: TaskAuthoringIntent) -> bool:
        """Return whether this exact membership serves one authoring intent."""
        if intent is TaskAuthoringIntent.TYPED_CREATE:
            return self.typed_create
        if intent is TaskAuthoringIntent.TYPED_EDIT:
            return self.typed_edit
        if intent is TaskAuthoringIntent.OPAQUE_CREATE:
            return self.opaque_create
        if intent is TaskAuthoringIntent.OPAQUE_EDIT:
            return self.opaque_edit
        return self.opaque_preserve


@dataclass(frozen=True, slots=True)
class TaskTypeAuthoringProfile:
    """Exact-profile authoring composition for one task type."""

    task_type: str
    category: str
    kind: str
    default_facet: str
    facets: Mapping[str, TaskAuthoringFacetMembership]

    def __post_init__(self) -> None:
        """Freeze facet membership and require an explicit default."""
        frozen = MappingProxyType(dict(self.facets))
        if self.default_facet not in frozen:
            message = f"Default facet {self.default_facet!r} is not a member"
            raise ValueError(message)
        object.__setattr__(self, "facets", frozen)

    @property
    def default(self) -> TaskAuthoringFacetMembership:
        """Return the default typed-authoring facet."""
        return self.facets[self.default_facet]


@dataclass(frozen=True, slots=True)
class TaskTypedAuthoringReview:
    """One explicit source-locked decision to enable a CLI typed model."""

    source_task_type: str
    cli_task_type: str
    review: str
    cli_model: str
    semantic_fingerprint: str


@dataclass(frozen=True, slots=True)
class TaskTypeProfileFact:
    """One exact upstream task type discovered from its tagged source tree."""

    task_type: str
    parameter_model_import: str
    semantic_fingerprint: str
    registration_kind: str
    typed_authoring_review: TaskTypedAuthoringReview | None = None


def _validate_membership_contract(
    membership: TaskAuthoringFacetMembership,
) -> None:
    if not set(membership.runtime_only_fields).issubset(
        membership.contract.runtime_only_fields
    ):
        message = (
            "Exact runtime-only fields must be declared by their authoring contract"
        )
        raise ValueError(message)
    directions = membership.contract.parameter_directions
    if directions is not None and (
        not directions or not set(directions).issubset({"IN", "OUT"})
    ):
        message = "Task parameter directions must be a non-empty IN/OUT set"
        raise ValueError(message)
