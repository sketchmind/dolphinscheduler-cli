"""Progress facts for the existing workflow create/edit sequences."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.errors import DsctlError
    from dsctl.output import JsonObject

_PARTIAL_MUTATION_SUGGESTION = (
    "Inspect known_resources and completed_stages before retrying. "
    "Continue only the unfinished stage; "
    "do not repeat the entire workflow mutation."
)


@dataclass
class WorkflowMutationProgress:
    """Remember completed stages and identities; never replay or roll back writes."""

    known_resources: JsonObject
    completed_stages: list[str] = field(default_factory=list)
    mutation_applied: bool = False

    def complete(self, stage: str, *, mutation: bool = False) -> None:
        self.completed_stages.append(stage)
        self.mutation_applied |= mutation

    def to_data(self) -> JsonObject:
        return {
            "completed_stages": list(self.completed_stages),
            "known_resources": self.known_resources,
            "mutation_applied": self.mutation_applied,
        }

    def annotate(self, error: DsctlError, *, failed_stage: str) -> None:
        applied = self.mutation_applied or error.details.get("mutation_applied") is True
        data = self.to_data()
        data.pop("mutation_applied")
        error.details.update(data)
        if applied or not error.details.get("mutation_may_have_applied"):
            error.details["mutation_applied"] = applied
        error.details.setdefault("failed_stage", failed_stage)
        if error.details.get("mutation_may_have_applied") or error.details.get(
            "phase"
        ) in {"readback", "mutation_response", "instance_resolution"}:
            error.details["unconfirmed_stages"] = [error.details["failed_stage"]]
        if self.mutation_applied:
            suggestion = error.suggestion or ""
            if not suggestion.startswith(_PARTIAL_MUTATION_SUGGESTION):
                error.suggestion = (
                    f"{_PARTIAL_MUTATION_SUGGESTION} {suggestion}".rstrip()
                )
