from __future__ import annotations

from pprint import pformat
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot


def render_python_registry(snapshot: ContractSnapshot) -> str:
    operations_literal = _render_literal(snapshot.to_json_dict()["operations"])
    dtos_literal = _render_literal(snapshot.to_json_dict()["dtos"])
    models_literal = _render_literal(snapshot.to_json_dict()["models"])
    enums_literal = _render_literal(snapshot.to_json_dict()["enums"])

    return (
        "from __future__ import annotations\n\n"
        f'DS_VERSION = "{snapshot.ds_version}"\n'
        f"OPERATION_COUNT = {snapshot.operation_count}\n"
        f"DTO_COUNT = {snapshot.dto_count}\n"
        f"MODEL_COUNT = {snapshot.model_count}\n"
        f"ENUM_COUNT = {snapshot.enum_count}\n\n"
        f"OPERATIONS = {operations_literal}\n\n"
        f"DTOS = {dtos_literal}\n\n"
        f"MODELS = {models_literal}\n\n"
        f"ENUMS = {enums_literal}\n\n"
        'OPERATIONS_BY_ID = {item["operation_id"]: item for item in OPERATIONS}\n'
        'DTOS_BY_NAME = {item["name"]: item for item in DTOS}\n'
        'MODELS_BY_NAME = {item["name"]: item for item in MODELS}\n'
        'ENUMS_BY_NAME = {item["name"]: item for item in ENUMS}\n'
    )


def write_python_registry(snapshot: ContractSnapshot, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_python_registry(snapshot))


def _render_literal(value: object) -> str:
    return pformat(value, width=88, sort_dicts=False)
