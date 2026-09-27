"""Exact task parameter projection and preservation."""

from dsctl.upstream.task_parameter_projection.engine import (
    decode_task_parameters,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)
from dsctl.upstream.task_parameter_projection.pytorch import (
    pytorch_typed_resource_file_candidate,
    pytorch_typed_resource_id_candidate,
)
from dsctl.upstream.task_parameter_projection.sql import (
    is_sql_139_complete_native_package,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskGraphContext,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskResourceRefIndex,
    TaskWorkflowRefIndex,
)
from dsctl.upstream.task_parameter_projection.waterdrop import (
    waterdrop_typed_resource_id_candidate,
)

__all__ = [
    "DecodedTaskParameters",
    "ProjectedTask",
    "ProjectionDirection",
    "ProjectionSource",
    "TaskGraphContext",
    "TaskParameterProjectionError",
    "TaskRefIndex",
    "TaskResourceRefIndex",
    "TaskWorkflowRefIndex",
    "decode_task_parameters",
    "decode_task_parameters_with_provenance",
    "encode_task_parameters",
    "is_sql_139_complete_native_package",
    "pytorch_typed_resource_file_candidate",
    "pytorch_typed_resource_id_candidate",
    "waterdrop_typed_resource_id_candidate",
]
