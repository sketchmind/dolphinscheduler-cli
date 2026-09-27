"""Read structural field facts from the selected Pydantic parameter model."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING

from dsctl.models.common import YamlObject, YamlValue, is_yaml_object

if TYPE_CHECKING:
    from pydantic import BaseModel


class UnknownFieldStructureError(ValueError):
    """A discovery path has no unambiguous structural model counterpart."""


@dataclass(frozen=True, slots=True)
class FieldStructure:
    """Structural facts with explicit absence of a model default."""

    value_type: str
    required: bool
    choices: tuple[str, ...]
    has_default: bool
    default: YamlValue = None


def field_structure(model: type[BaseModel], path: str) -> FieldStructure:
    """Resolve an aliased discovery path against its selected parameter model."""
    return structure_from_schema(_model_schema(model), path)


@cache
def _model_schema(model: type[BaseModel]) -> YamlObject:
    schema = model.model_json_schema(by_alias=True)
    if not is_yaml_object(schema):
        message = f"{model.__name__} did not produce an object schema"
        raise UnknownFieldStructureError(message)
    return schema


def structure_from_schema(schema: YamlObject, path: str) -> FieldStructure:
    """Read one property or array-item path without guessing across model unions."""
    prefix = "task_params."
    if not path.startswith(prefix):
        message = f"{path!r} is not a parameter-model path"
        raise UnknownFieldStructureError(message)
    node = schema
    required = False
    parent_default: YamlValue = None
    parent_has_default = False
    for segment in path.removeprefix(prefix).split("."):
        node = _concrete_node(schema, node)
        name = segment.removesuffix("[]")
        properties = node.get("properties")
        if not isinstance(properties, dict) or not isinstance(
            properties.get(name), dict
        ):
            message = f"{path!r} is not an unambiguous model property"
            raise UnknownFieldStructureError(message)
        required_names = node.get("required", [])
        required = isinstance(required_names, list) and name in required_names
        property_node = properties[name]
        if not isinstance(property_node, dict):
            message = f"{path!r} has no object field schema"
            raise UnknownFieldStructureError(message)
        node = _concrete_node(schema, property_node)
        parent_default = node.get("default")
        parent_has_default = "default" in node
        if segment.endswith("[]"):
            items = node.get("items")
            if not isinstance(items, dict):
                message = f"{path!r} is not a homogeneous model array"
                raise UnknownFieldStructureError(message)
            node = _concrete_node(schema, items)
    raw_type = node.get("type")
    if not isinstance(raw_type, str):
        message = f"{path!r} has no unambiguous model type"
        raise UnknownFieldStructureError(message)
    # Pydantic 2.8 redundantly emits enum=[const]; newer releases omit it.
    # A fixed value is not a discovery choice set.
    raw_choices = [] if "const" in node else node.get("enum", [])
    choices = (
        tuple(str(choice) for choice in raw_choices)
        if isinstance(raw_choices, list)
        else ()
    )
    return FieldStructure(
        value_type="enum" if raw_type == "string" and choices else raw_type,
        required=required,
        choices=choices,
        has_default="default" in node or parent_has_default,
        default=node.get("default", parent_default),
    )


def _concrete_node(root: YamlObject, node: YamlObject) -> YamlObject:
    reference = node.get("$ref")
    if isinstance(reference, str):
        current: YamlValue = root
        if not reference.startswith("#/"):
            message = "Only local Pydantic schema references are structural facts"
            raise UnknownFieldStructureError(message)
        for segment in reference[2:].split("/"):
            key = segment.replace("~1", "/").replace("~0", "~")
            if not isinstance(current, dict) or key not in current:
                message = f"Unresolved model schema reference {reference!r}"
                raise UnknownFieldStructureError(message)
            current = current[key]
        if not isinstance(current, dict):
            message = f"Model schema reference {reference!r} is not an object"
            raise UnknownFieldStructureError(message)
        node = {
            **current,
            **{key: value for key, value in node.items() if key != "$ref"},
        }
    conjunction = node.get("allOf")
    if conjunction is not None:
        if (
            not isinstance(conjunction, list)
            or len(conjunction) != 1
            or not isinstance(conjunction[0], dict)
        ):
            message = "Combined model constraints need explicit reviewed structure"
            raise UnknownFieldStructureError(message)
        # Pydantic 2.8 wraps a default-bearing reference in a single allOf.
        # Newer releases place the same reference beside the default directly.
        node = {
            **_concrete_node(root, conjunction[0]),
            **{key: value for key, value in node.items() if key != "allOf"},
        }
    alternatives = node.get("anyOf", node.get("oneOf"))
    if isinstance(alternatives, list):
        concrete = [
            item
            for item in alternatives
            if isinstance(item, dict) and item.get("type") != "null"
        ]
        if len(concrete) != 1:
            message = "Variant-dependent model fields need explicit reviewed structure"
            raise UnknownFieldStructureError(message)
        selected = _concrete_node(root, concrete[0])
        return {
            **selected,
            **{
                key: value
                for key, value in node.items()
                if key not in {"anyOf", "oneOf"}
            },
        }
    return node
