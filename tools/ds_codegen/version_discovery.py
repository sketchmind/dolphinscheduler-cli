"""Compile the small, version-independent connection discovery contract."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import javalang

from ds_codegen.discovery_contracts import (
    ApiOperationContract,
    DocumentSource,
    compile_document_sources,
    compile_public_operations,
    write_document_contracts,
)
from ds_codegen.java_source import parse_java_compilation_unit

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import ContractSnapshot, DtoSpec, ModelSpec

_API = "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
_SWAGGER = f"{_API}configuration/SwaggerConfiguration.java"
_PRODUCT_OPERATION = "UiPluginController.queryProductInfo"


@dataclass(frozen=True)
class DiscoveryProfile:
    """Exact facts retained before the full source contract is sliced."""

    version: str
    product_path: str | None
    product_version_field: str | None
    openapi: bool
    evidence: tuple[tuple[str, str], ...]
    route_miss_code: int | None = None
    database_product_version: str | None = None
    documents: tuple[DocumentSource, ...] = ()
    public_operations: tuple[ApiOperationContract, ...] = ()


def compile_discovery_profile(
    snapshot: ContractSnapshot, source_root: Path
) -> DiscoveryProfile:
    """Derive product-info from the IR and verify the narrow OpenAPI source seam."""
    evidence: list[tuple[str, str]] = []
    product_path: str | None = None
    product_version_field: str | None = None
    operation = next(
        (
            item
            for item in snapshot.operations
            if item.operation_id == _PRODUCT_OPERATION
        ),
        None,
    )
    if operation is not None:
        candidates: list[ModelSpec | DtoSpec] = [*snapshot.models, *snapshot.dtos]
        models = [
            model
            for model in candidates
            if model.import_path == operation.logical_return_type
        ]
        if (
            operation.http_method != "GET"
            or operation.parameters
            or operation.return_type != "Result<ProductInfoDto>"
            or len(models) != 1
        ):
            message = "product-info source contract changed"
            raise ValueError(message)
        fields = models[0].fields
        if (
            len(fields) != 1
            or fields[0].name != "version"
            or fields[0].java_type != "String"
        ):
            message = "product-info version field changed"
            raise ValueError(message)
        product_path = operation.path
        product_version_field = fields[0].wire_name
        for relative, snippets in (
            (
                f"{_API}controller/UiPluginController.java",
                ("return Result.success(result);",),
            ),
            (
                f"{_API}utils/Result.java",
                (
                    "private Integer code;",
                    "private T data;",
                    "Status.SUCCESS.getCode()",
                ),
            ),
            (f"{_API}enums/Status.java", ("SUCCESS(0,",)),
            (
                f"{_API}service/impl/UiPluginServiceImpl.java",
                ("dsVersionDao.selectVersion()", "result.setVersion(dsVersion);"),
            ),
        ):
            evidence.append(_review_source(source_root, relative, snippets))
    swagger_path = source_root / _SWAGGER
    openapi = swagger_path.is_file()
    if openapi:
        evidence.append(
            _review_source(
                source_root,
                _SWAGGER,
                (
                    ".version(getDsVersion())",
                    "new OpenAPI().info(info)",
                    "dsVersionDao.selectVersion()",
                ),
            )
        )
        evidence.append(
            _review_source(
                source_root,
                "dolphinscheduler-api/src/main/resources/swagger.properties",
                ("springdoc.api-docs.enabled=true",),
            )
        )
        evidence.append(
            _review_source(
                source_root,
                f"{_API}configuration/AppConfiguration.java",
                ('"/v3/api-docs/**"', ".excludePathPatterns("),
            )
        )
    route_miss_code = None
    legacy_detail = next(
        (
            operation
            for operation in snapshot.operations
            if operation.operation_id == "UiPluginController.queryUiPluginDetailById"
        ),
        None,
    )
    if product_path is None and legacy_detail is not None:
        if (
            legacy_detail.path != "ui-plugins/{id}"
            or legacy_detail.http_method != "GET"
        ):
            message = "legacy product probe collision route changed"
            raise ValueError(message)
        controller_path = f"{_API}controller/UiPluginController.java"
        evidence.append(
            _review_source(
                source_root, controller_path, ("@ApiException(QUERY_PLUGINS_ERROR)",)
            )
        )
        status_path = f"{_API}enums/Status.java"
        evidence.append(
            _review_source(source_root, status_path, ("QUERY_PLUGINS_ERROR(",))
        )
        route_miss_code = _query_plugins_error_code(source_root / status_path)
    database_version_path = "dolphinscheduler-dao/src/main/resources/sql/soft_version"
    database_file = source_root / database_version_path
    database_version = None
    if database_file.is_file():
        database_version = database_file.read_text(encoding="utf-8").strip()
        evidence.append(
            _review_source(source_root, database_version_path, (database_version,))
        )
    documents = compile_document_sources(source_root)
    public_operations, public_evidence = compile_public_operations(
        snapshot, source_root
    )
    evidence.extend(item for item in public_evidence if item not in evidence)
    return DiscoveryProfile(
        version=snapshot.ds_version,
        product_path=product_path,
        product_version_field=product_version_field,
        openapi=openapi,
        evidence=tuple(evidence),
        route_miss_code=route_miss_code,
        database_product_version=database_version,
        documents=documents,
        public_operations=public_operations,
    )


def _query_plugins_error_code(path: Path) -> int:
    tree = parse_java_compilation_unit(path.read_text(encoding="utf-8"))
    for _, declaration in tree.filter(javalang.tree.EnumDeclaration):
        if declaration.name != "Status":
            continue
        for constant in declaration.body.constants:
            if constant.name == "QUERY_PLUGINS_ERROR" and constant.arguments:
                value = constant.arguments[0]
                if isinstance(value, javalang.tree.Literal) and value.value.isdecimal():
                    return int(value.value)
    message = "legacy product probe collision error code changed"
    raise ValueError(message)


def _review_source(
    source_root: Path, relative: str, snippets: tuple[str, ...]
) -> tuple[str, str]:
    source = (source_root / relative).read_text(encoding="utf-8")
    if not all(snippet in source for snippet in snippets):
        message = f"version discovery source seam changed: {relative}"
        raise ValueError(message)
    return relative, hashlib.sha256(source.encode()).hexdigest()


def write_version_discovery(
    output_root: Path, profiles: tuple[DiscoveryProfile, ...]
) -> None:
    """Render immutable bootstrap records without adding a stable CLI operation."""
    product_profiles = tuple(profile for profile in profiles if profile.product_path)
    product_shapes = {
        (profile.product_path, profile.product_version_field)
        for profile in product_profiles
    }
    if len(product_shapes) > 1:
        message = "product-info requires separately reviewed discovery shapes"
        raise ValueError(message)
    lines = [
        '"""Generated connection discovery facts; do not edit by hand."""',
        "",
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
        "",
        "",
        "@dataclass(frozen=True)",
        "class ProbeContract:",
        '    """A reviewed GET probe for database-recorded product versions."""',
        "",
        "    source: str",
        "    path: str",
        "    version_fields: tuple[str, ...]",
        "    exact_versions: tuple[str, ...]",
        "    success_code: int | None",
        "    route_miss_codes: tuple[int, ...]",
        "    route_miss_versions: tuple[str, ...]",
        "",
        "",
        "PROBES: tuple[ProbeContract, ...] = (",
    ]
    if product_profiles:
        path, field = next(iter(product_shapes))
        lines.extend(
            _render_probe(
                "product_info",
                str(path),
                ("data", str(field)),
                tuple(
                    p.version
                    for p in product_profiles
                    if p.database_product_version == p.version
                ),
                0,
                tuple(
                    sorted(
                        {
                            p.route_miss_code
                            for p in profiles
                            if p.route_miss_code is not None
                        }
                    )
                ),
                tuple(p.version for p in profiles if p.route_miss_code is not None),
            )
        )
    openapi_versions = tuple(
        profile.version
        for profile in profiles
        if profile.openapi and profile.database_product_version == profile.version
    )
    if openapi_versions:
        # Springdoc's JSON route is corroborated by the reviewed DS exclusion.
        # Legacy Springfox API labels V1/V2 are deliberately outside this record.
        lines.extend(
            _render_probe(
                "openapi",
                "v3/api-docs",
                ("info", "version"),
                openapi_versions,
                None,
                (),
                (),
            )
        )
    lines.extend(
        [
            ")",
            "",
            "# Exact source memberships keep their own file digests.",
            "SOURCE_EVIDENCE: dict[str, tuple[tuple[str, str], ...]] = {",
        ]
    )
    for profile in profiles:
        lines.append(f"    {profile.version!r}: (")
        for path, digest in profile.evidence:
            lines.extend(
                [
                    "        (",
                    f"            {path!r},",
                    f"            {digest!r},",
                    "        ),",
                ]
            )
        lines.append("    ),")
    lines.extend(
        [
            "}",
            "",
            "# Database product version can differ from the exact release tag.",
            "DATABASE_PRODUCT_VERSIONS: dict[str, str] = {",
        ]
    )
    lines.extend(
        f"    {profile.version!r}: {profile.database_product_version!r},"
        for profile in profiles
        if profile.database_product_version is not None
    )
    lines.extend(["}", ""])
    target = output_root / "generated" / "version_discovery.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    evidence_start = lines.index(
        "# Exact source memberships keep their own file digests."
    )
    evidence_end = lines.index(
        "# Database product version can differ from the exact release tag."
    )
    evidence_lines = lines[evidence_start:evidence_end]
    (target.parent / "_discovery_evidence.py").write_text(
        '"""Generated exact discovery source evidence; do not edit by hand."""\n\n'
        + "\n".join(evidence_lines),
        encoding="utf-8",
    )
    del lines[evidence_start:evidence_end]
    imports = [
        "from ._discovery_evidence import SOURCE_EVIDENCE as SOURCE_EVIDENCE",
        "from ._discovery_operations import OPERATION_CONTRACTS as OPERATION_CONTRACTS",
        "from ._discovery_parameters import PARAMETER_CONTRACTS as PARAMETER_CONTRACTS",
        "from ._discovery_profiles import (",
        "    CONTRACT_PROFILES as CONTRACT_PROFILES,",
        "    DISCOVERY_CONTRACT_DIGEST as DISCOVERY_CONTRACT_DIGEST,",
        "    DOCUMENT_PROBES as DOCUMENT_PROBES,",
        ")",
        "from ._discovery_types import (",
        "    ApiContractProfile as ApiContractProfile,",
        "    ApiOperationContract as ApiOperationContract,",
        "    ApiParameterContract as ApiParameterContract,",
        "    DocumentProbe as DocumentProbe,",
        ")",
        "",
    ]
    lines[6:6] = imports
    write_document_contracts(target.parent, profiles)
    target.write_text("\n".join(lines), encoding="utf-8")


def _render_probe(
    source: str,
    path: str,
    fields: tuple[str, ...],
    versions: tuple[str, ...],
    success_code: int | None,
    route_miss_codes: tuple[int, ...],
    route_miss_versions: tuple[str, ...],
) -> list[str]:
    return [
        "    ProbeContract(",
        f"        source={source!r},",
        f"        path={path!r},",
        f"        version_fields={fields!r},",
        "        exact_versions=(",
        *(f"            {version!r}," for version in versions),
        "        ),",
        f"        success_code={success_code!r},",
        f"        route_miss_codes={route_miss_codes!r},",
        "        route_miss_versions=(",
        *(f"            {version!r}," for version in route_miss_versions),
        "        ),",
        "    ),",
    ]
