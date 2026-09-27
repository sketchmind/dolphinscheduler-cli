from __future__ import annotations

from ds_codegen.render.package.operations_renderer import OperationRenderDeps
from ds_codegen.render.package.planner import python_class_name
from ds_codegen.render.package.render_support import (
    display_doc_text,
    pydantic_field_name,
    render_docstring_lines,
    render_parameter_field_config,
)
from ds_codegen.render.package.type_renderer import TypeRenderDeps
from ds_codegen.render.package.type_support import (
    collect_annotation_import_targets,
    field_annotation_type,
    relative_import_statement,
    render_annotation_type,
)
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

REQUESTS_BASE_CLASS_NAME = "BaseRequestsClient"
ROOT_MODEL_MODULE_PARTS = ("_models",)
BASE_CONTRACT_MODEL_NAME = "BaseContractModel"
BASE_VIEW_MODEL_NAME = "BaseViewModel"
BASE_ENTITY_MODEL_NAME = "BaseEntityModel"
BASE_PARAMS_MODEL_NAME = "BaseParamsModel"


def operation_render_deps() -> OperationRenderDeps:
    """Return the shared dependency set for operation rendering."""
    return OperationRenderDeps(
        requests_base_class_name=REQUESTS_BASE_CLASS_NAME,
        base_params_model_name=BASE_PARAMS_MODEL_NAME,
        collect_annotation_import_targets=collect_annotation_import_targets,
        display_doc_text=display_doc_text,
        field_annotation_type=field_annotation_type,
        pydantic_field_name=pydantic_field_name,
        python_class_name=python_class_name,
        relative_import_statement=relative_import_statement,
        render_annotation_type=render_annotation_type,
        render_docstring_lines=render_docstring_lines,
        render_parameter_field_config=render_parameter_field_config,
    )


def type_render_deps() -> TypeRenderDeps:
    """Return the shared dependency set for exact type rendering."""
    return TypeRenderDeps(
        base_contract_model_name=BASE_CONTRACT_MODEL_NAME,
        base_view_model_name=BASE_VIEW_MODEL_NAME,
        base_entity_model_name=BASE_ENTITY_MODEL_NAME,
        root_model_module_parts=ROOT_MODEL_MODULE_PARTS,
        strict_integer_fields=cleanup_strict_integer_fields,
        model_role_module_parts={},
    )


__all__ = [
    "BASE_CONTRACT_MODEL_NAME",
    "BASE_ENTITY_MODEL_NAME",
    "BASE_PARAMS_MODEL_NAME",
    "BASE_VIEW_MODEL_NAME",
    "REQUESTS_BASE_CLASS_NAME",
    "ROOT_MODEL_MODULE_PARTS",
    "operation_render_deps",
    "type_render_deps",
]
