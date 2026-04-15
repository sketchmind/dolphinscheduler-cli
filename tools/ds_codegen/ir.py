from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

HttpMethod = Literal["DELETE", "GET", "PATCH", "POST", "PUT"]
ParamBinding = Literal[
    "model_attribute",
    "path_variable",
    "request_attribute",
    "request_body",
    "request_param",
]
ScalarValue = bool | int | float | str
AnnotationValue = ScalarValue | list[ScalarValue]
ModelKind = Literal[
    "api_dto",
    "api_util",
    "dao_entity",
    "generated_view",
    "other_class",
]
ResponseProjection = Literal[
    "direct",
    "single_data",
    "single_data_list",
    "status_data",
]


@dataclass(frozen=True)
class ParameterSpec:
    name: str
    java_type: str
    binding: ParamBinding | None
    wire_name: str | None
    required: bool | None
    default_value: str | None
    hidden: bool
    description: str | None
    example: str | None
    allowable_values: str | None
    schema_type: str | None


@dataclass(frozen=True)
class OperationSpec:
    operation_id: str
    controller: str
    method_name: str
    api_group: str
    http_method: HttpMethod
    path: str
    summary: str | None
    description: str | None
    documentation: str | None
    parameter_docs: dict[str, str]
    returns_doc: str | None
    consumes: list[str]
    return_type: str
    inferred_return_type: str | None
    logical_return_type: str
    response_projection: ResponseProjection
    parameters: list[ParameterSpec]


@dataclass(frozen=True)
class EnumValueSpec:
    name: str
    arguments: list[str]
    documentation: str | None


@dataclass(frozen=True)
class EnumFieldSpec:
    name: str
    java_type: str
    annotations: list[str]


@dataclass(frozen=True)
class EnumSpec:
    name: str
    import_path: str
    documentation: str | None
    fields: list[EnumFieldSpec]
    json_value_field: str | None
    values: list[EnumValueSpec]


@dataclass(frozen=True)
class DtoFieldSpec:
    name: str
    java_type: str
    wire_name: str
    required: bool | None
    default_value: str | None
    nullable: bool
    default_factory: str | None
    description: str | None
    example: str | None
    allowable_values: str | None
    documentation: str | None


@dataclass(frozen=True)
class DtoSpec:
    name: str
    import_path: str
    documentation: str | None
    extends: str | None
    fields: list[DtoFieldSpec]


@dataclass(frozen=True)
class ModelSpec:
    name: str
    import_path: str
    kind: ModelKind
    documentation: str | None
    extends: str | None
    fields: list[DtoFieldSpec]


@dataclass(frozen=True)
class ContractSnapshot:
    ds_version: str
    operation_count: int
    enum_count: int
    dto_count: int
    model_count: int
    operations: list[OperationSpec]
    enums: list[EnumSpec]
    dtos: list[DtoSpec]
    models: list[ModelSpec]

    def to_json_dict(self) -> dict[str, object]:
        return asdict(self)
