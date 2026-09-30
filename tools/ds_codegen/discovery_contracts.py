"""Compile public API-document facts separately from effective runtime contracts.

Documentation identifies compatible API surfaces, never exact server releases.
Springfox annotations can disagree with Spring MVC binding declarations; retain
both facts rather than turning documentation quirks into runtime permissions.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import javalang

from ds_codegen.extract.metadata import _annotation_values
from ds_codegen.java_source import parse_java_compilation_unit

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import (
        AnnotationValue,
        ContractSnapshot,
        DtoSpec,
        ModelSpec,
        OperationSpec,
        ParameterSpec,
    )
    from ds_codegen.version_discovery import DiscoveryProfile

_API = "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"


@dataclass(frozen=True)
class ApiParameterContract:
    name: str
    location: str
    schema_type: str | None
    required: bool
    document_required: tuple[bool, ...]
    enum_values: tuple[str, ...]
    default_value: str | None
    item_schema_type: str | None = None
    document_schema_types: tuple[str, ...] = ()


@dataclass(frozen=True)
class ApiOperationContract:
    operation_id: str
    document_group: str | None
    method: str
    path: str
    parameters: tuple[ApiParameterContract, ...]
    ignored_document_parameters: tuple[str, ...]


@dataclass(frozen=True)
class DocumentSource:
    source: str
    path: str
    document_group: str | None


def compile_document_sources(source_root: Path) -> tuple[DocumentSource, ...]:
    """Select the documentation implementation declared by this exact source."""
    configuration = source_root / _API / "configuration"
    swagger = configuration / "SwaggerConfig.java"
    springfox = configuration / "OpenAPIConfiguration.java"
    springdoc = configuration / "SwaggerConfiguration.java"
    if swagger.is_file():
        source = swagger.read_text(encoding="utf-8")
        _require(source, "DocumentationType.SWAGGER_2", swagger)
        _require(source, "PathSelectors.any()", swagger)
        return (DocumentSource("swagger2", "v2/api-docs", None),)
    if springfox.is_file():
        source = springfox.read_text(encoding="utf-8")
        _review_grouped_dockets(source, springfox)
        return (
            DocumentSource("openapi3", "v3/api-docs?group=v1(current)", "v1"),
            DocumentSource("openapi3", "v3/api-docs?group=v2", "v2"),
        )
    if springdoc.is_file():
        _require(springdoc.read_text(encoding="utf-8"), "new OpenAPI()", springdoc)
        return (DocumentSource("openapi3", "v3/api-docs", None),)
    message = f"API documentation configuration missing: {source_root}"
    raise ValueError(message)


def _require(source: str, snippet: str, path: Path) -> None:
    if snippet not in source:
        message = f"API documentation source seam changed: {path}: {snippet}"
        raise ValueError(message)


def _java_structure(value: object) -> object:
    """Compare Java syntax without source positions, comments or whitespace."""
    if isinstance(value, javalang.ast.Node):
        return type(value).__name__, tuple(
            (name, _java_structure(getattr(value, name))) for name in value.attrs
        )
    if isinstance(value, (list, tuple)):
        return tuple(_java_structure(item) for item in value)
    return value


def _review_grouped_dockets(source: str, path: Path) -> None:
    """Bind each reviewed group to its own package and native path selector."""
    tree = parse_java_compilation_unit(source)
    methods = [
        method
        for _, method in tree.filter(javalang.tree.MethodDeclaration)
        if method.return_type is not None and method.return_type.name == "Docket"
    ]
    if len(methods) != 2 or {method.name for method in methods} != {
        "createV1RestApi",
        "createV2RestApi",
    }:
        message = f"API documentation source seam changed: {path}: Docket methods"
        raise ValueError(message)
    for method in methods:
        v1 = method.name == "createV1RestApi"
        group = "v1(current)" if v1 else "v2"
        info = "apiV1Info" if v1 else "apiV2Info"
        negate = ".negate()" if v1 else ""
        reviewed = parse_java_compilation_unit(
            "class Reviewed { Docket source() { return "
            'new Docket(DocumentationType.OAS_30).groupName("'
            + group
            + '").apiInfo('
            + info
            + "()).select().apis(RequestHandlerSelectors.basePackage("
            '"org.apache.dolphinscheduler.api.controller"))'
            '.paths(PathSelectors.any().and(PathSelectors.ant("/v2/**")'
            + negate
            + ")).build(); } }"
        )
        expected = reviewed.types[0].methods[0].body
        if _java_structure(method.body) != _java_structure(expected) or not any(
            annotation.name == "Bean" for annotation in method.annotations
        ):
            message = f"API documentation source seam changed: {path}: {method.name}"
            raise ValueError(message)


def compile_public_operations(
    snapshot: ContractSnapshot,
    source_root: Path,
    documents: tuple[DocumentSource, ...],
) -> tuple[tuple[ApiOperationContract, ...], tuple[tuple[str, str], ...]]:
    """Keep public source routes and meaningful declared wire parameters."""
    declarations: dict[str, tuple[javalang.tree.ClassDeclaration, str]] = {}
    evidence: list[tuple[str, str]] = []
    for path in sorted((source_root / _API / "controller").rglob("*.java")):
        source = path.read_text(encoding="utf-8")
        tree = parse_java_compilation_unit(source)
        relative = path.relative_to(source_root).as_posix()
        for declaration in tree.types:
            if not isinstance(declaration, javalang.tree.ClassDeclaration):
                continue
            # v2 declarations have the same simple names as their v1 peers.
            group = "v2" if "/controller/v2/" in relative else "v1"
            declarations[f"{group}:{declaration.name}"] = (declaration, relative)
        evidence.append((relative, hashlib.sha256(source.encode()).hexdigest()))
    operations: list[ApiOperationContract] = []
    for operation in snapshot.operations:
        declaration, _ = declarations[f"{operation.api_group}:{operation.controller}"]
        methods = [
            m
            for m in declaration.methods
            if m.name == operation.method_name
            and tuple(p.name for p in m.parameters)
            == tuple(p.name for p in operation.parameters)
        ]
        if len(methods) != 1:
            message = f"ambiguous documentation method: {operation.operation_id}"
            raise ValueError(message)
        method = methods[0]
        if _route_hidden(declaration.annotations) or _route_hidden(method.annotations):
            continue
        # A source namespace identifies Java declarations, not a Docket group.
        # Reviewed grouped documents split routes using PathSelectors /v2/**.
        document_group = None
        if any(document.document_group is not None for document in documents):
            document_group = (
                "v2" if operation.path.strip("/").split("/")[0] == "v2" else "v1"
            )
        operations.append(
            _public_operation(snapshot, operation, method, document_group)
        )
    # Config bytes explain document endpoint and group membership independently.
    for filename in (
        "SwaggerConfig.java",
        "OpenAPIConfiguration.java",
        "SwaggerConfiguration.java",
    ):
        path = source_root / _API / "configuration" / filename
        if path.is_file():
            evidence.append(
                (
                    path.relative_to(source_root).as_posix(),
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                )
            )
    return tuple(
        sorted(operations, key=lambda op: (op.path, op.method, op.operation_id))
    ), tuple(evidence)


def _route_hidden(annotations: list[javalang.tree.Annotation]) -> bool:
    """Operation visibility is independent of parameter annotation placement."""
    return any(
        annotation.name.rsplit(".", 1)[-1] in {"ApiIgnore", "Hidden"}
        or (
            annotation.name.rsplit(".", 1)[-1] in {"ApiOperation", "Operation"}
            and _annotation_values(annotation).get("hidden") is True
        )
        for annotation in annotations
    )


def _parameter_hidden(annotations: list[javalang.tree.Annotation]) -> bool:
    return any(
        annotation.name.rsplit(".", 1)[-1] == "ApiIgnore"
        or (
            annotation.name.rsplit(".", 1)[-1] in {"ApiParam", "Parameter"}
            and _annotation_values(annotation).get("hidden") is True
        )
        for annotation in annotations
    )


def _public_operation(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    method: javalang.tree.MethodDeclaration,
    document_group: str | None,
) -> ApiOperationContract:
    implicit = {
        str(values["name"]): values
        for _, annotation in method.filter(javalang.tree.Annotation)
        if annotation.name == "ApiImplicitParam"
        and "name" in (values := _annotation_values(annotation))
    }
    ignored = set(implicit) - {p.wire_name or p.name for p in operation.parameters}
    parameters: list[ApiParameterContract] = []
    source_parameters = {p.name: p for p in method.parameters}
    for parameter in operation.parameters:
        source_parameter = source_parameters[parameter.name]
        if parameter.binding == "request_attribute":
            # Springfox expands a non-hidden login User into bean fields, though
            # Spring MVC supplies it server-side. These are document-only fields.
            if not _parameter_hidden(source_parameter.annotations):
                models: list[ModelSpec | DtoSpec] = [*snapshot.models, *snapshot.dtos]
                for model in models:
                    if model.import_path == parameter.java_type:
                        ignored.update(field.wire_name for field in model.fields)
            continue
        if _parameter_hidden(source_parameter.annotations) or parameter.hidden:
            continue
        location = {
            "path_variable": "path",
            "request_param": "request",
            "request_body": "body",
            "model_attribute": "model",
        }.get(parameter.binding or "")
        if location is None:
            # Servlet request/response/context arguments are not public inputs.
            continue
        name = parameter.wire_name or parameter.name
        required = (
            True
            if location == "path"
            else False
            if parameter.default_value is not None
            else parameter.required is not False
        )
        documented_required = {required}
        if name in implicit:
            documented_required.add(implicit[name].get("required", False) is True)
        for annotation in source_parameter.annotations:
            if annotation.name in {"ApiParam", "Parameter"}:
                values = _annotation_values(annotation)
                if "required" in values:
                    documented_required.add(values["required"] is True)
        parameters.append(
            ApiParameterContract(
                name=name,
                location=location,
                schema_type=_schema_type(snapshot, parameter),
                required=required,
                document_required=tuple(sorted(documented_required)),
                enum_values=_enum_values(snapshot, parameter),
                default_value=parameter.default_value,
                item_schema_type=_item_schema_type(snapshot, parameter),
                document_schema_types=_document_schema_types(
                    snapshot, parameter, implicit.get(name, {})
                ),
            )
        )
    return ApiOperationContract(
        operation_id=operation.operation_id,
        document_group=document_group,
        method=operation.http_method,
        path=operation.path.strip("/"),
        parameters=tuple(sorted(parameters, key=lambda p: (p.location, p.name))),
        ignored_document_parameters=tuple(sorted(ignored)),
    )


def _schema_type(snapshot: ContractSnapshot, parameter: ParameterSpec) -> str | None:
    java_type = parameter.java_type.rsplit(".", 1)[-1]
    if java_type in {
        "int",
        "Integer",
        "long",
        "Long",
        "short",
        "Short",
        "byte",
        "Byte",
    }:
        return "integer"
    if java_type in {"float", "Float", "double", "Double", "BigDecimal"}:
        return "number"
    if java_type in {"boolean", "Boolean"}:
        return "boolean"
    if java_type in {"String", "Date", "LocalDateTime"} or _enum_values(
        snapshot, parameter
    ):
        return "string"
    if "<" in parameter.java_type or "[]" in parameter.java_type:
        return (
            "array"
            if parameter.java_type.startswith(("List<", "Set<"))
            or "[]" in parameter.java_type
            else None
        )
    return None


def _document_schema_types(
    snapshot: ContractSnapshot,
    parameter: ParameterSpec,
    declaration: dict[str, AnnotationValue],
) -> tuple[str, ...]:
    types = {_schema_type(snapshot, parameter)}
    declared = (
        declaration.get("dataTypeClass", declaration.get("dataType", "String"))
        if declaration
        else None
    )
    if isinstance(declared, str):
        # Old annotations use Int/int/integer interchangeably. The annotation
        # remains documentation evidence only; it never changes wire encoding.
        normalized = {
            "int": "Integer",
            "integer": "Integer",
            "string": "String",
            "boolean": "Boolean",
            "long": "Long",
        }.get(declared.lower(), declared)
        types.add(_schema_type(snapshot, replace(parameter, java_type=normalized)))
    return tuple(sorted(value for value in types if value is not None))


def _item_schema_type(
    snapshot: ContractSnapshot, parameter: ParameterSpec
) -> str | None:
    java_type = parameter.java_type
    if java_type.endswith("[]"):
        item = java_type[:-2]
    elif java_type.startswith(("List<", "Set<")) and java_type.endswith(">"):
        item = java_type.split("<", 1)[1][:-1]
    else:
        return None
    return _schema_type(snapshot, replace(parameter, java_type=item))


def _enum_values(
    snapshot: ContractSnapshot, parameter: ParameterSpec
) -> tuple[str, ...]:
    enum = next(
        (enum for enum in snapshot.enums if enum.import_path == parameter.java_type),
        None,
    )
    return tuple(value.name for value in enum.values) if enum is not None else ()


def render_document_contracts(profiles: tuple[DiscoveryProfile, ...]) -> list[str]:
    """Render readable shared operation records with exact independent membership."""
    lines = [
        "",
        "",
        "@dataclass(frozen=True)",
        "class DocumentProbe:",
        '    """Public API metadata; matching a document never proves a release."""',
        "    source: str",
        "    path: str",
        "    exact_versions: tuple[str, ...]",
        "    document_group: str | None",
        "",
        "",
        "@dataclass(frozen=True)",
        "class ApiParameterContract:",
        "    name: str",
        "    location: str",
        "    schema_type: str | None",
        "    required: bool",
        "    document_required: tuple[bool, ...]",
        "    enum_values: tuple[str, ...]",
        "    default_value: str | None",
        "    item_schema_type: str | None",
        "    document_schema_types: tuple[str, ...]",
        "",
        "",
        "@dataclass(frozen=True)",
        "class ApiOperationContract:",
        "    operation_id: str",
        "    document_group: str | None",
        "    method: str",
        "    path: str",
        "    parameters: tuple[ApiParameterContract, ...]",
        "    ignored_document_parameters: tuple[str, ...]",
        "",
        "",
        "@dataclass(frozen=True)",
        "class ApiContractProfile:",
        "    document_paths: tuple[str, ...]",
        "    operations: tuple[str, ...]",
        "",
        "",
        "DOCUMENT_PROBES: tuple[DocumentProbe, ...] = (",
    ]
    documents = tuple(
        dict.fromkeys(doc for profile in profiles for doc in profile.documents)
    )
    for document in documents:
        versions = tuple(
            profile.version for profile in profiles if document in profile.documents
        )
        lines.extend(
            [
                "    DocumentProbe(",
                f"        source={document.source!r},",
                f"        path={document.path!r},",
                f"        exact_versions={versions!r},",
                f"        document_group={document.document_group!r},",
                "    ),",
            ]
        )
    lines.extend(
        [
            ")",
            "",
            "# Equal records share implementation, never exact source membership.",
            "PARAMETER_CONTRACTS: dict[str, ApiParameterContract] = {",
        ]
    )
    parameter_keys: dict[ApiParameterContract, str] = {}
    for profile in profiles:
        for operation in profile.public_operations:
            for parameter in operation.parameters:
                if parameter in parameter_keys:
                    continue
                key = (
                    f"{parameter.location}:{parameter.name}@"
                    f"{operation.operation_id}@{profile.version}"
                )
                parameter_keys[parameter] = key
                lines.extend(
                    [
                        f"    {key!r}: ApiParameterContract(",
                        f"        name={parameter.name!r},",
                        f"        location={parameter.location!r},",
                        f"        schema_type={parameter.schema_type!r},",
                        f"        required={parameter.required!r},",
                        f"        document_required={parameter.document_required!r},",
                        f"        enum_values={parameter.enum_values!r},",
                        f"        default_value={parameter.default_value!r},",
                        f"        item_schema_type={parameter.item_schema_type!r},",
                        "        document_schema_types="
                        f"{parameter.document_schema_types!r},",
                        "    ),",
                    ]
                )
    lines.extend(["}", "", "OPERATION_CONTRACTS: dict[str, ApiOperationContract] = {"])
    identities: dict[ApiOperationContract, str] = {}
    for profile in profiles:
        for operation in profile.public_operations:
            if operation in identities:
                continue
            key = f"{operation.operation_id}@{profile.version}"
            identities[operation] = key
            lines.extend(
                [
                    f"    {key!r}: ApiOperationContract(",
                    f"        operation_id={operation.operation_id!r},",
                    f"        document_group={operation.document_group!r},",
                    f"        method={operation.method!r},",
                    f"        path={operation.path!r},",
                    "        parameters=(",
                    *(
                        "            PARAMETER_CONTRACTS["
                        f"{parameter_keys[parameter]!r}],"
                        for parameter in operation.parameters
                    ),
                    "        ),",
                    "        ignored_document_parameters="
                    f"{operation.ignored_document_parameters!r},",
                    "    ),",
                ]
            )
    lines.extend(["}", "", "PROFILE_OPERATION_SETS: dict[str, tuple[str, ...]] = {"])
    memberships: dict[tuple[str, ...], str] = {}
    for profile in profiles:
        membership = tuple(
            identities[operation] for operation in profile.public_operations
        )
        if membership in memberships:
            continue
        memberships[membership] = profile.version
        lines.extend(
            [
                f"    {profile.version!r}: (",
                *(f"        {identity!r}," for identity in membership),
                "    ),",
            ]
        )
    lines.extend(["}", "", "CONTRACT_PROFILES: dict[str, ApiContractProfile] = {"])
    for profile in profiles:
        membership = tuple(
            identities[operation] for operation in profile.public_operations
        )
        lines.extend(
            [
                f"    {profile.version!r}: ApiContractProfile(",
                "        document_paths="
                f"{tuple(doc.path for doc in profile.documents)!r},",
                "        operations=PROFILE_OPERATION_SETS["
                f"{memberships[membership]!r}],",
                "    ),",
            ]
        )
    lines.extend(["}", ""])
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    lines.extend([f"DISCOVERY_CONTRACT_DIGEST = {digest!r}", ""])
    return lines


def write_document_contracts(
    output_root: Path, profiles: tuple[DiscoveryProfile, ...]
) -> None:
    """Keep each generated data concern in a directly importable logical module."""
    lines = render_document_contracts(profiles)
    documents = lines.index("DOCUMENT_PROBES: tuple[DocumentProbe, ...] = (")
    parameters = lines.index("PARAMETER_CONTRACTS: dict[str, ApiParameterContract] = {")
    operations = lines.index("OPERATION_CONTRACTS: dict[str, ApiOperationContract] = {")
    memberships = lines.index("PROFILE_OPERATION_SETS: dict[str, tuple[str, ...]] = {")
    header = [
        '"""Generated API-document discovery facts; do not edit by hand."""',
        "",
        "from __future__ import annotations",
        "",
    ]
    modules = {
        "_discovery_types": [
            *header,
            "from dataclasses import dataclass",
            *lines[:documents],
        ],
        "_discovery_parameters": [
            *header,
            "from ._discovery_types import ApiParameterContract",
            "",
            *lines[parameters:operations],
        ],
        "_discovery_operations": [
            *header,
            "from ._discovery_parameters import PARAMETER_CONTRACTS",
            "from ._discovery_types import ApiOperationContract",
            "",
            *lines[operations:memberships],
        ],
        "_discovery_profiles": [
            *header,
            "from ._discovery_types import ApiContractProfile, DocumentProbe",
            "",
            *lines[documents:parameters],
            *lines[memberships:],
        ],
    }
    for name, content in modules.items():
        (output_root / f"{name}.py").write_text("\n".join(content), encoding="utf-8")
