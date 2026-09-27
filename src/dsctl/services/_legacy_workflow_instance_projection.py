from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.support.yaml_io import dump_yaml_document

if TYPE_CHECKING:
    from dsctl.upstream.legacy_workflow_graph import DecodedLegacyWorkflowGraph


@dataclass(frozen=True, slots=True)
class LegacyWorkflowInstanceReadProjection:
    """Pure canonical YAML projection of one DS 1.3.9 instance graph."""

    graph: DecodedLegacyWorkflowGraph
    workflow_name: str
    project_name: str

    def yaml_text(self) -> str:
        """Render the exact instance graph as a reusable authoring document."""
        return dump_yaml_document(
            self.graph.workflow_document(
                name=self.workflow_name,
                project=self.project_name,
                description=None,
                release_state="OFFLINE",
            )
        )


__all__ = ["LegacyWorkflowInstanceReadProjection"]
