from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.task_spec import (
    canonical_task_type,
    normalize_typed_task_params,
    task_params_model_for_type,
    validate_k8s_task_name,
)
from dsctl.services.task_authoring_catalog.semantics import validate_task_semantics

if TYPE_CHECKING:
    from dsctl.models.common import GlobalParamSpec, YamlObject, YamlValue
    from dsctl.upstream.parameter_semantics import ParameterSemanticsProfile
    from dsctl.upstream.task_authoring_surface import (
        TaskAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.conditions import (
    _validate_legacy_conditions_139_authored_params,
)
from dsctl.services.task_authoring_catalog.script import (
    _validate_legacy_script_139_authored_params,
)
from dsctl.services.task_authoring_catalog.sql import (
    SQL_RESOURCE_FILE_FACET,
    _uses_sql_resource_fields,
)
from dsctl.services.task_authoring_catalog.sub_workflow import (
    _validate_sub_workflow_identity_fields,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetMembership,
    TaskAuthoringIntent,
    TaskTypeAuthoringProfile,
    TaskTypeProfileFact,
    _validate_membership_contract,
)


def _validated_task_type_facts(
    raw_facts: Mapping[str, TaskTypeProfileFact],
) -> Mapping[str, TaskTypeProfileFact]:
    facts = MappingProxyType(dict(raw_facts))
    for task_type, fact in facts.items():
        if task_type != fact.task_type or task_type != canonical_task_type(task_type):
            message = "Task profile fact keys must match canonical task types"
            raise ValueError(message)
        review = fact.typed_authoring_review
        if review is None:
            continue
        if review.source_task_type != task_type:
            message = (
                f"Typed authoring review source type does not match fact {task_type}"
            )
            raise ValueError(message)
        if review.cli_task_type != canonical_task_type(review.cli_task_type):
            message = (
                f"Typed authoring review CLI type is not canonical for {task_type}"
            )
            raise ValueError(message)
        if review.semantic_fingerprint != fact.semantic_fingerprint:
            message = f"Typed authoring review fingerprint is stale for {task_type}"
            raise ValueError(message)
    return facts


def _validate_catalog_entries(
    entries: Mapping[str, TaskTypeAuthoringProfile],
    *,
    facts: Mapping[str, TaskTypeProfileFact],
) -> None:
    if facts:
        missing_entry_facts = sorted(set(entries) - set(facts))
        if missing_entry_facts:
            missing = ", ".join(missing_entry_facts)
            message = f"Materialized task entries require upstream facts: {missing}"
            raise ValueError(message)
    reviewed_cli_types = {
        review.cli_task_type
        for fact in facts.values()
        if (review := fact.typed_authoring_review) is not None
    }
    unreviewed_typed_entries = sorted(
        entry.task_type
        for entry in entries.values()
        if any(
            membership.typed_create or membership.typed_edit
            for membership in entry.facets.values()
        )
        and entry.task_type not in reviewed_cli_types
    )
    if unreviewed_typed_entries:
        task_types = ", ".join(unreviewed_typed_entries)
        message = f"Typed task entries require source-locked reviews: {task_types}"
        raise ValueError(message)
    for fact in facts.values():
        review = fact.typed_authoring_review
        if review is None:
            continue
        entry = entries.get(review.cli_task_type)
        installed_model = task_params_model_for_type(review.cli_task_type)
        if entry is None:
            model_matches = (
                installed_model is not None
                and installed_model.__name__ == review.cli_model
            )
        else:
            contract_models = tuple(
                membership.contract.params_model
                for membership in entry.facets.values()
                if (membership.typed_create or membership.typed_edit)
                and membership.contract.params_model is not None
            )
            model_matches = any(
                model.__name__ == review.cli_model
                or (
                    installed_model is not None
                    and installed_model.__name__ == review.cli_model
                    and issubclass(model, installed_model)
                )
                for model in contract_models
            )
        if not model_matches:
            message = (
                f"Typed authoring review for {review.cli_task_type} does not match an "
                "installed CLI task_params model"
            )
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class TaskAuthoringCatalog:
    """Exact upstream facts plus independently reviewed authoring policy."""

    profile_version: str
    entries: Mapping[str, TaskTypeAuthoringProfile]
    parameter_semantics: ParameterSemanticsProfile
    authoring_surface: TaskAuthoringSurface
    task_type_facts: Mapping[str, TaskTypeProfileFact] = field(default_factory=dict)
    source_contract_fingerprint: str | None = None
    source_provenance: Mapping[str, YamlValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze the profile and reject unreviewed typed authorization."""
        frozen = MappingProxyType(dict(self.entries))
        for entry in frozen.values():
            for membership in entry.facets.values():
                if membership.profile_version != self.profile_version:
                    message = "Task authoring membership version must match its catalog"
                    raise ValueError(message)
                _validate_membership_contract(membership)
        facts = _validated_task_type_facts(self.task_type_facts)
        _validate_catalog_entries(frozen, facts=facts)
        object.__setattr__(self, "entries", frozen)
        object.__setattr__(self, "task_type_facts", facts)
        if self.parameter_semantics.version != self.profile_version:
            message = "Parameter semantics version must match its authoring catalog"
            raise ValueError(message)
        if self.authoring_surface.version != self.profile_version:
            message = "Task authoring surface version must match its catalog"
            raise ValueError(message)
        if (
            self.authoring_surface.nested_workflow.native_task_type
            != self.parameter_semantics.nested_workflow.task_type
        ):
            message = "Nested workflow authoring surface must match parameter semantics"
            raise ValueError(message)
        nested_native_type = self.parameter_semantics.nested_workflow.task_type
        for fact in facts.values():
            review = fact.typed_authoring_review
            if (
                review is not None
                and review.cli_task_type == "SUB_WORKFLOW"
                and review.source_task_type != nested_native_type
            ):
                message = (
                    "SUB_WORKFLOW review source type must match nested workflow "
                    "semantics"
                )
                raise ValueError(message)
        object.__setattr__(
            self,
            "source_provenance",
            MappingProxyType(dict(self.source_provenance)),
        )

    @property
    def task_types(self) -> tuple[str, ...]:
        """Return task types materialized in this first exact-profile slice."""
        return tuple(sorted(self.entries))

    @property
    def upstream_task_types(self) -> tuple[str, ...]:
        """Return every task type proved present by this exact source profile."""
        return tuple(sorted(self.task_type_facts))

    @property
    def authoring_task_types(self) -> tuple[str, ...]:
        """Return the user-facing task types with create/edit authorization."""
        return self.authorable_task_types

    @property
    def authorable_task_types(self) -> tuple[str, ...]:
        """Return source-present CLI types with typed or raw create/edit policy."""
        return tuple(
            sorted(
                {
                    cli_task_type
                    for source_task_type in self.task_type_facts
                    if (
                        self.supports_typed_authoring(
                            cli_task_type := (
                                self.cli_task_type_for_source(source_task_type)
                                or source_task_type
                            )
                        )
                        or self.supports_opaque_authoring(source_task_type)
                    )
                }
            )
        )

    @property
    def reviewed_typed_task_types(self) -> frozenset[str]:
        """Return task types whose CLI typed semantics passed explicit review."""
        return frozenset(
            review.cli_task_type
            for fact in self.task_type_facts.values()
            if (review := fact.typed_authoring_review) is not None
        )

    @property
    def legacy_typed_task_types(self) -> frozenset[str]:
        """Return reviewed fallback models outside explicit facet entries."""
        return self.reviewed_typed_task_types - self.entries.keys()

    @property
    def opaque_authoring_task_types(self) -> frozenset[str]:
        """Return exact native types eligible for unvalidated create/edit."""
        return frozenset(
            task_type
            for task_type in self.task_type_facts
            if self.supports_opaque_authoring(task_type)
        )

    @property
    def parameter_data_types(self) -> frozenset[str]:
        """Return exact Property data types accepted by this profile."""
        return self.parameter_semantics.allowed_property_types

    def task_type_fact(self, task_type: str) -> TaskTypeProfileFact | None:
        """Return exact upstream evidence for one task type when present."""
        return self.task_type_facts.get(canonical_task_type(task_type))

    def cli_task_type_for_source(self, task_type: str) -> str | None:
        """Map one native source type to its reviewed CLI alias when present."""
        normalized = canonical_task_type(task_type)
        fact = self.task_type_facts.get(normalized)
        if fact is None:
            return None
        review = fact.typed_authoring_review
        return normalized if review is None else review.cli_task_type

    def source_task_type_for_cli(self, task_type: str) -> str | None:
        """Map one user-facing authoring type back to its exact native source type."""
        normalized = canonical_task_type(task_type)
        for source_task_type, fact in self.task_type_facts.items():
            review = fact.typed_authoring_review
            cli_task_type = source_task_type if review is None else review.cli_task_type
            if cli_task_type == normalized:
                return source_task_type
        return None

    def supports_typed_authoring(self, task_type: str) -> bool:
        """Return whether one exact task/model pair passed authoring review."""
        normalized = canonical_task_type(task_type)
        return any(
            review.cli_task_type == normalized
            for fact in self.task_type_facts.values()
            if (review := fact.typed_authoring_review) is not None
        )

    def supports_opaque_authoring(self, task_type: str) -> bool:
        """Return whether exact policy permits raw native create/edit."""
        normalized = canonical_task_type(task_type)
        if self.task_type_fact(normalized) is None:
            return False
        if self.profile_version == "1.3.9" and normalized in {
            "CONDITIONS",
            "SUB_PROCESS",
        }:
            # CONDITIONS is split across outer TaskNode fields. SUB_PROCESS owns
            # a server database id that canonical authoring must resolve from a
            # same-project name. Both are server-origin preservation only.
            return False
        entry = self.entries.get(normalized)
        if entry is None:
            # Upstream-native types without a materialized facet retain the
            # historical lossless generic task_params authoring path.
            return True
        return any(
            membership.opaque_create or membership.opaque_edit
            for membership in entry.facets.values()
        )

    def effective_authoring_intent(
        self,
        task_type: str,
        *,
        requested: TaskAuthoringIntent,
        task_params: Mapping[str, YamlValue] | None = None,
    ) -> TaskAuthoringIntent:
        """Select a reviewed typed facet or an explicit native opaque mode."""
        if requested not in {
            TaskAuthoringIntent.TYPED_CREATE,
            TaskAuthoringIntent.TYPED_EDIT,
        }:
            return requested
        normalized = canonical_task_type(task_type)
        entry = self.entries.get(normalized)
        selector = (
            None if entry is None else entry.default.contract.opaque_authoring_selector
        )
        if task_params is not None and selector is not None and selector(task_params):
            return (
                TaskAuthoringIntent.OPAQUE_CREATE
                if requested is TaskAuthoringIntent.TYPED_CREATE
                else TaskAuthoringIntent.OPAQUE_EDIT
            )
        if self.supports_typed_authoring(normalized):
            return requested
        return (
            TaskAuthoringIntent.OPAQUE_CREATE
            if requested is TaskAuthoringIntent.TYPED_CREATE
            else TaskAuthoringIntent.OPAQUE_EDIT
        )

    def validate_global_params(self, params: list[GlobalParamSpec]) -> None:
        """Reject authored workflow parameter semantics absent from this profile."""
        for param in params:
            data_type = param.type.value
            if data_type not in self.parameter_data_types:
                raise self._unsupported_parameter_data_type(
                    field="workflow.global_params[].type",
                    value=data_type,
                )
            if (
                param.direct.value == "OUT"
                and not self.parameter_semantics.output.var_pool_transport
            ):
                raise self._unsupported_parameter_field(
                    field="workflow.global_params[].direct",
                    value="OUT",
                )

    def validate_task_identity(self, task_type: str, task_name: str) -> None:
        """Validate exact runtime identities derived from one authored task name."""
        normalized_type = canonical_task_type(task_type)
        if normalized_type != "K8S":
            return
        validate_k8s_task_name(task_name)

    def validate_authored_task_params(
        self,
        task_type: str,
        task_params: Mapping[str, YamlValue],
    ) -> None:
        """Apply exact parameter gates only to newly authored typed payloads."""
        normalized_type = canonical_task_type(task_type)
        entry = self.entries.get(normalized_type)
        allow_local_out_without_var_pool = bool(
            entry is not None
            and entry.default.contract.allow_local_out_without_var_pool
        )
        if (
            "varPool" in task_params
            and not self.parameter_semantics.output.var_pool_transport
        ):
            raise self._unsupported_parameter_field(
                field="tasks[].task_params.varPool",
                value="present",
                task_type=normalized_type,
            )
        for collection_name in ("localParams", "varPool"):
            raw_params = task_params.get(collection_name)
            if not isinstance(raw_params, Sequence) or isinstance(
                raw_params,
                (bytes, bytearray, str),
            ):
                continue
            for raw_param in raw_params:
                if not isinstance(raw_param, Mapping):
                    continue
                raw_type = raw_param.get("type")
                if (
                    isinstance(raw_type, str)
                    and raw_type not in self.parameter_data_types
                ):
                    raise self._unsupported_parameter_data_type(
                        field=(f"tasks[].task_params.{collection_name}[].type"),
                        value=raw_type,
                        task_type=normalized_type,
                    )
                if (
                    raw_param.get("direct") == "OUT"
                    and not self.parameter_semantics.output.var_pool_transport
                    and not allow_local_out_without_var_pool
                ):
                    raise self._unsupported_parameter_field(
                        field=(f"tasks[].task_params.{collection_name}[].direct"),
                        value="OUT",
                        task_type=normalized_type,
                    )

    def require_task_type(self, task_type: str) -> TaskTypeAuthoringProfile:
        """Return one exact task-type composition or fail closed."""
        normalized = canonical_task_type(task_type)
        entry = self.entries.get(normalized)
        if entry is not None:
            return entry
        message = (
            f"{normalized} authoring is not materialized for DolphinScheduler "
            f"{self.profile_version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": self.profile_version,
                "task_type": normalized,
            },
        )

    def require_facet(
        self,
        task_type: str,
        facet_id: str,
    ) -> TaskAuthoringFacetMembership:
        """Return one explicit exact-profile facet membership."""
        entry = self.require_task_type(task_type)
        membership = entry.facets.get(facet_id)
        if membership is not None:
            return membership
        constraint = (
            f"{facet_id} is not available in the exact {self.profile_version} profile."
        )
        raise self._unsupported_facet_error(
            task_type=entry.task_type,
            facet_id=facet_id,
            intent=None,
            constraint=constraint,
        )

    def require_authoring_task_type(
        self,
        task_type: str,
        *,
        intent: TaskAuthoringIntent,
    ) -> str:
        """Authorize only source-reviewed typed work or opaque preservation."""
        normalized = canonical_task_type(task_type)
        if intent is TaskAuthoringIntent.OPAQUE_PRESERVE:
            return normalized
        if intent in {
            TaskAuthoringIntent.OPAQUE_CREATE,
            TaskAuthoringIntent.OPAQUE_EDIT,
        }:
            if self.supports_opaque_authoring(normalized):
                return normalized
            constraint = (
                f"Exact DolphinScheduler {self.profile_version} policy permits only "
                f"opaque preservation for {normalized}."
                if self.task_type_fact(normalized) is not None
                else (
                    f"Exact DolphinScheduler {self.profile_version} source does not "
                    f"expose native task type {normalized}."
                )
            )
            message = (
                f"{normalized} opaque authoring is unsupported for "
                f"DolphinScheduler {self.profile_version}."
            )
            raise UnsupportedFeatureError(
                message,
                details={
                    "selected_version": self.profile_version,
                    "task_type": normalized,
                    "intent": intent.value,
                    "constraint": constraint,
                },
            )
        if self.supports_typed_authoring(normalized):
            return normalized
        constraint = (
            "CLI typed create/edit has not passed source-structure and semantic "
            f"review for {normalized} on DolphinScheduler {self.profile_version}."
        )
        message = (
            f"{normalized} typed authoring is unsupported for DolphinScheduler "
            f"{self.profile_version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": self.profile_version,
                "task_type": normalized,
                "intent": intent.value,
                "constraint": constraint,
            },
        )

    def normalize_task_params(
        self,
        task_type: str,
        task_params: YamlObject,
        *,
        intent: TaskAuthoringIntent,
    ) -> YamlObject:
        """Validate typed intent or preserve one native task payload losslessly."""
        normalized_type = self.require_authoring_task_type(task_type, intent=intent)
        entry = self.entries.get(normalized_type)
        if entry is None:
            return self._normalize_legacy_task_params(
                normalized_type,
                task_params,
                intent=intent,
            )
        return self._normalize_materialized_task_params(
            entry,
            task_params,
            intent=intent,
        )

    def _normalize_legacy_task_params(
        self,
        task_type: str,
        task_params: YamlObject,
        *,
        intent: TaskAuthoringIntent,
    ) -> YamlObject:
        """Serve reviewed models that have not yet moved into facet contracts."""
        if intent in {
            TaskAuthoringIntent.OPAQUE_CREATE,
            TaskAuthoringIntent.OPAQUE_EDIT,
            TaskAuthoringIntent.OPAQUE_PRESERVE,
        }:
            return deepcopy(task_params)
        model = task_params_model_for_type(task_type)
        if model is None:
            message = f"{task_type} has no reviewed typed task-parameter model"
            raise RuntimeError(message)
        if (
            task_type == "HTTP"
            and not self.authoring_surface.http.request_body
            and "httpBody" in task_params
        ):
            message = (
                "task_params.httpBody is unsupported for typed HTTP authoring on "
                f"DolphinScheduler {self.profile_version}"
            )
            raise ValueError(message)
        if self.profile_version == "1.3.9" and task_type == "CONDITIONS":
            unsupported = sorted({"localParams", "varPool"}.intersection(task_params))
            if unsupported:
                names = ", ".join(unsupported)
                message = (
                    "DolphinScheduler 1.3.9 CONDITIONS task_params does not own "
                    f"these fields: {names}"
                )
                raise ValueError(message)
        if task_type == "SUB_WORKFLOW":
            _validate_sub_workflow_identity_fields(
                task_params,
                profile_version=self.profile_version,
            )
        normalized_params = normalize_typed_task_params(model, task_params)
        self.validate_authored_task_params(task_type, normalized_params)
        if self.profile_version == "1.3.9" and task_type in {"PYTHON", "SHELL"}:
            _validate_legacy_script_139_authored_params(
                normalized_params,
                task_type=task_type,
            )
        if self.profile_version == "1.3.9" and task_type == "CONDITIONS":
            _validate_legacy_conditions_139_authored_params(normalized_params)
        return normalized_params

    def _normalize_materialized_task_params(
        self,
        entry: TaskTypeAuthoringProfile,
        task_params: YamlObject,
        *,
        intent: TaskAuthoringIntent,
    ) -> YamlObject:
        """Select, authorize, and normalize one materialized facet contract."""
        facet_id = (
            SQL_RESOURCE_FILE_FACET
            if _uses_sql_resource_fields(task_params)
            else entry.default_facet
        )
        membership = entry.facets.get(facet_id)
        if membership is None:
            if intent is TaskAuthoringIntent.OPAQUE_PRESERVE:
                return deepcopy(task_params)
            constraint = (
                f"{facet_id} is not available in the exact "
                f"{self.profile_version} profile."
            )
            raise self._unsupported_facet_error(
                task_type=entry.task_type,
                facet_id=facet_id,
                intent=intent,
                constraint=constraint,
            )
        if not membership.supports(intent):
            constraint = membership.constraint or (
                f"{facet_id} does not support {intent.value}."
            )
            raise self._unsupported_facet_error(
                task_type=entry.task_type,
                facet_id=facet_id,
                intent=intent,
                constraint=constraint,
            )

        if intent is TaskAuthoringIntent.OPAQUE_PRESERVE:
            return deepcopy(task_params)

        if intent in {
            TaskAuthoringIntent.OPAQUE_CREATE,
            TaskAuthoringIntent.OPAQUE_EDIT,
        }:
            contract = membership.contract
            selector = contract.opaque_authoring_selector
            if contract.restrict_opaque_authoring_to_selector and (
                selector is None or not selector(task_params)
            ):
                constraint = (
                    f"{contract.facet_id} does not recognize this "
                    "payload as an explicit opaque authoring mode."
                )
                raise self._unsupported_facet_error(
                    task_type=entry.task_type,
                    facet_id=contract.facet_id,
                    intent=intent,
                    constraint=constraint,
                )
            return deepcopy(task_params)

        model = membership.contract.params_model
        if model is None:
            message = f"{facet_id} has no typed task-parameter model"
            raise RuntimeError(message)
        typed_params = deepcopy(task_params)
        if intent in {
            TaskAuthoringIntent.TYPED_CREATE,
            TaskAuthoringIntent.TYPED_EDIT,
        }:
            for field_name in membership.runtime_only_fields:
                typed_params.pop(field_name, None)
        normalized_params = normalize_typed_task_params(model, typed_params)
        validate_task_semantics(
            entry.task_type,
            normalized_params,
            version=self.profile_version,
            surface=self.authoring_surface,
        )
        self.validate_authored_task_params(entry.task_type, normalized_params)
        return normalized_params

    def _unsupported_facet_error(
        self,
        *,
        task_type: str,
        facet_id: str,
        intent: TaskAuthoringIntent | None,
        constraint: str,
    ) -> UnsupportedFeatureError:
        details: YamlObject = {
            "selected_version": self.profile_version,
            "task_type": task_type,
            "facet": facet_id,
        }
        if intent is not None:
            details["intent"] = intent.value
        details["constraint"] = constraint
        message = (
            f"{facet_id} is unsupported for DolphinScheduler {self.profile_version}."
        )
        return UnsupportedFeatureError(message, details=details)

    def _unsupported_parameter_data_type(
        self,
        *,
        field: str,
        value: str,
        task_type: str | None = None,
    ) -> UnsupportedFeatureError:
        message = (
            f"Parameter data type {value} is unsupported for DolphinScheduler "
            f"{self.profile_version}."
        )
        allowed_values = cast("list[YamlValue]", sorted(self.parameter_data_types))
        details: YamlObject = {
            "selected_version": self.profile_version,
            "field": field,
            "value": value,
            "allowed_values": allowed_values,
            "reason": "upstream_capability_absent",
        }
        if task_type is not None:
            details["task_type"] = task_type
        return UnsupportedFeatureError(
            message,
            details=details,
            suggestion=(
                "Run `dsctl enum list data-type` for the selected profile and "
                "choose one of its reported values."
            ),
        )

    def _unsupported_parameter_field(
        self,
        *,
        field: str,
        value: str,
        task_type: str | None = None,
    ) -> UnsupportedFeatureError:
        details: YamlObject = {
            "selected_version": self.profile_version,
            "field": field,
            "value": value,
            "reason": "upstream_capability_absent",
        }
        if task_type is not None:
            details["task_type"] = task_type
        return UnsupportedFeatureError(
            (
                f"Parameter field {field} is unsupported for "
                f"DolphinScheduler {self.profile_version}."
            ),
            details=details,
            suggestion=(
                "Remove the unsupported authored field for this exact DS profile, "
                "or preserve an exported native payload without editing it."
            ),
        )
