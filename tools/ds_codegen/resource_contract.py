"""Reviewed exact-version recipes for DolphinScheduler file resources.

The resource surface crosses three unusually important wire boundaries:

* releases through 3.1.9 address resources by database id even though the CLI
  exposes the DS ``fullName`` path;
* 3.2.x moves the public controller to storage paths, but still carries the
  transitional tenant and parent-id parameters;
* 3.3.1 replaces the storage DTOs and introduces a controller bug in
  ``viewResource`` that passes ``skipLineNum`` as the requested line limit.

Binary downloads and multipart uploads are deliberately recorded here as
source operations even though their runtime adapter uses the shared raw HTTP
transport.  The generated contract remains the source of endpoint and
parameter evidence; the narrow resource domain owns the non-JSON transport.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

TARGET_RESOURCE_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "2.0.9",
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
    "3.1.0",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

RESOURCE_SEMANTIC_OPERATIONS = (
    "resource.page",
    "resource.view",
    "resource.upload",
    "resource.create",
    "resource.mkdir",
    "resource.download",
    "resource.delete",
)

IdentityWire = Literal["id", "full-name"]
BaseDirectoryWire = Literal["root", "endpoint"]
ViewWire = Literal[
    "generated-id", "generated-path-tenant", "generated-path", "binary-window"
]
ParentDirectoryWire = Literal["id-and-path", "storage-path", "absolute-path"]
UploadRoute = Literal["legacy-create", "root-create"]
EvidenceKind = Literal["controller", "ui"]


@dataclass(frozen=True)
class Evidence:
    """One exact source coordinate used by a resource decision."""

    version: str
    kind: EvidenceKind
    source: str
    symbol: str
    conclusion: str

    @property
    def reference(self) -> str:
        """Return the conventional source reference consumed by profiles."""
        return f"{self.source}#{self.symbol}" if self.symbol else self.source


@dataclass(frozen=True)
class ResourceRecipe:
    """Wire and projection decisions for one exact resource controller."""

    identity_wire: IdentityWire
    base_directory_wire: BaseDirectoryWire
    parent_directory_wire: ParentDirectoryWire
    page_operation: str
    lookup_operation: str | None
    upload_operation: str
    upload_route: UploadRoute
    create_operation: str
    mkdir_operation: str
    view_evidence_operation: str | None
    view_operation: str
    view_wire: ViewWire
    download_operation: str
    delete_operation: str
    tenant_code_on_reads: bool
    page_model: str
    item_model: str
    view_model: str | None


@dataclass(frozen=True)
class ResourceVersionContract:
    """Reviewed resource-domain contract for one exact DS version."""

    version: str
    resource: ResourceRecipe


_CONTROLLER = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
    "api/controller/ResourcesController.java"
)
_LEGACY_UI = "dolphinscheduler-ui/src/js/conf/home/store/resource/actions.js"
_UI = "dolphinscheduler-ui/src/service/modules/resources/index.ts"

_RESULT_MODEL = "org.apache.dolphinscheduler.api.utils.Result"
_PAGE_MODEL = "org.apache.dolphinscheduler.api.utils.PageInfo"
_RESOURCE_139_MODEL = "org.apache.dolphinscheduler.dao.entity.Resource"
# The controller imports Spring ``Resource`` for downloads, while its service
# populates paging data with the DS entity.  Extraction carries a source-proven,
# fail-closed operation-scoped import override for this collision.
_RESOURCE_20_MODEL = "org.apache.dolphinscheduler.dao.entity.Resource"
_STORAGE_ENTITY_MODEL = "org.apache.dolphinscheduler.plugin.storage.api.StorageEntity"
_RESOURCE_ITEM_MODEL = "org.apache.dolphinscheduler.api.vo.ResourceItemVO"
_RESOURCE_CONTENT_MODEL = "generated.view.ResourcesServiceImpl_readResource_map"
_MODERN_RESOURCE_CONTENT_MODEL = (
    "org.apache.dolphinscheduler.api.vo.resources.FetchFileContentResponse"
)

_LEGACY_139 = ResourceRecipe(
    identity_wire="id",
    base_directory_wire="root",
    parent_directory_wire="id-and-path",
    page_operation="ResourcesController.queryResourceListPaging",
    lookup_operation="ResourcesController.queryResource",
    upload_operation="ResourcesController.createResource",
    upload_route="legacy-create",
    create_operation="ResourcesController.onlineCreateResource",
    mkdir_operation="ResourcesController.createDirectory",
    view_evidence_operation=None,
    view_operation="ResourcesController.viewResource",
    view_wire="generated-id",
    download_operation="ResourcesController.downloadResource",
    delete_operation="ResourcesController.deleteResource",
    tenant_code_on_reads=False,
    page_model=_PAGE_MODEL,
    item_model=_RESOURCE_139_MODEL,
    view_model=None,
)
_ID_REST = ResourceRecipe(
    identity_wire="id",
    base_directory_wire="root",
    parent_directory_wire="id-and-path",
    page_operation="ResourcesController.queryResourceListPaging",
    lookup_operation="ResourcesController.queryResource",
    upload_operation="ResourcesController.createResource",
    upload_route="root-create",
    create_operation="ResourcesController.onlineCreateResource",
    mkdir_operation="ResourcesController.createDirectory",
    view_evidence_operation=None,
    view_operation="ResourcesController.viewResource",
    view_wire="generated-id",
    download_operation="ResourcesController.downloadResource",
    delete_operation="ResourcesController.deleteResource",
    tenant_code_on_reads=False,
    page_model=_PAGE_MODEL,
    item_model=_RESOURCE_20_MODEL,
    view_model=_RESOURCE_CONTENT_MODEL,
)
_STORAGE_PATH = ResourceRecipe(
    identity_wire="full-name",
    base_directory_wire="endpoint",
    parent_directory_wire="storage-path",
    page_operation="ResourcesController.queryResourceListPaging",
    lookup_operation=None,
    upload_operation="ResourcesController.createResource",
    upload_route="root-create",
    create_operation="ResourcesController.onlineCreateResource",
    mkdir_operation="ResourcesController.createDirectory",
    view_evidence_operation=None,
    view_operation="ResourcesController.viewResource",
    view_wire="generated-path-tenant",
    download_operation="ResourcesController.downloadResource",
    delete_operation="ResourcesController.deleteResource",
    tenant_code_on_reads=True,
    page_model=_PAGE_MODEL,
    item_model=_STORAGE_ENTITY_MODEL,
    view_model=_RESOURCE_CONTENT_MODEL,
)
_STORAGE_PATH_322 = replace(
    _STORAGE_PATH,
    create_operation="ResourcesController.createResourceFile",
)
_ABSOLUTE_PATH = ResourceRecipe(
    identity_wire="full-name",
    base_directory_wire="endpoint",
    parent_directory_wire="absolute-path",
    page_operation="ResourcesController.pagingResourceItemRequest",
    lookup_operation=None,
    upload_operation="ResourcesController.createFile",
    upload_route="root-create",
    create_operation="ResourcesController.createFileFromContent",
    mkdir_operation="ResourcesController.createDirectory",
    # The generated view operation is intentionally not executed.  Exact
    # source shows ``limit == -1 ? MAX_VALUE : skipLineNum`` in every target
    # from 3.3.1 through 3.4.2, so the stable view reads the binary download
    # and slices lines locally.
    view_evidence_operation="ResourcesController.viewResource",
    view_operation="ResourcesController.downloadResource",
    view_wire="binary-window",
    download_operation="ResourcesController.downloadResource",
    delete_operation="ResourcesController.deleteResource",
    tenant_code_on_reads=False,
    page_model=_PAGE_MODEL,
    item_model=_RESOURCE_ITEM_MODEL,
    view_model=_MODERN_RESOURCE_CONTENT_MODEL,
)


# Exact 2.0.1–2.0.5 services return a typed content map, not the later
# synthesized service-map model. Both use the same id-based REST operations.
_ID_REST_CONTENT_MAP = replace(_ID_REST, view_model=None)


_ABSOLUTE_PATH_NATIVE_VIEW = replace(
    _ABSOLUTE_PATH,
    view_evidence_operation=None,
    view_operation="ResourcesController.viewResource",
    view_wire="generated-path",
)

RESOURCE_CONTRACTS: dict[str, ResourceVersionContract] = {
    "1.3.9": ResourceVersionContract("1.3.9", _LEGACY_139),
    "2.0.0": ResourceVersionContract("2.0.0", _ID_REST),
    "2.0.1": ResourceVersionContract("2.0.1", _ID_REST_CONTENT_MAP),
    "2.0.2": ResourceVersionContract("2.0.2", _ID_REST_CONTENT_MAP),
    "2.0.3": ResourceVersionContract("2.0.3", _ID_REST_CONTENT_MAP),
    "2.0.4": ResourceVersionContract("2.0.4", _ID_REST_CONTENT_MAP),
    "2.0.5": ResourceVersionContract("2.0.5", _ID_REST_CONTENT_MAP),
    "2.0.6": ResourceVersionContract("2.0.6", _ID_REST),
    "2.0.7": ResourceVersionContract("2.0.7", _ID_REST),
    "2.0.8": ResourceVersionContract("2.0.8", _ID_REST),
    "2.0.9": ResourceVersionContract("2.0.9", _ID_REST),
    "3.0.0": ResourceVersionContract("3.0.0", _ID_REST),
    "3.0.1": ResourceVersionContract("3.0.1", _ID_REST),
    "3.0.2": ResourceVersionContract("3.0.2", _ID_REST),
    "3.0.3": ResourceVersionContract("3.0.3", _ID_REST),
    "3.0.4": ResourceVersionContract("3.0.4", _ID_REST),
    "3.0.5": ResourceVersionContract("3.0.5", _ID_REST),
    "3.0.6": ResourceVersionContract("3.0.6", _ID_REST),
    "3.1.0": ResourceVersionContract("3.1.0", _ID_REST),
    "3.1.1": ResourceVersionContract("3.1.1", _ID_REST),
    "3.1.2": ResourceVersionContract("3.1.2", _ID_REST),
    "3.1.3": ResourceVersionContract("3.1.3", _ID_REST),
    "3.1.4": ResourceVersionContract("3.1.4", _ID_REST),
    "3.1.5": ResourceVersionContract("3.1.5", _ID_REST),
    "3.1.6": ResourceVersionContract("3.1.6", _ID_REST),
    "3.1.7": ResourceVersionContract("3.1.7", _ID_REST),
    "3.1.8": ResourceVersionContract("3.1.8", _ID_REST),
    "3.1.9": ResourceVersionContract("3.1.9", _ID_REST),
    "3.2.0": ResourceVersionContract("3.2.0", _STORAGE_PATH),
    "3.2.1": ResourceVersionContract("3.2.1", _STORAGE_PATH),
    "3.2.2": ResourceVersionContract("3.2.2", _STORAGE_PATH_322),
    "3.3.1": ResourceVersionContract("3.3.1", _ABSOLUTE_PATH),
    "3.3.2": ResourceVersionContract("3.3.2", _ABSOLUTE_PATH),
    "3.4.0": ResourceVersionContract("3.4.0", _ABSOLUTE_PATH),
    "3.4.1": ResourceVersionContract("3.4.1", _ABSOLUTE_PATH),
    "3.4.2": ResourceVersionContract("3.4.2", _ABSOLUTE_PATH),
    "3.4.3": ResourceVersionContract("3.4.3", _ABSOLUTE_PATH_NATIVE_VIEW),
}


def resource_contract(version: str) -> ResourceVersionContract:
    """Return one reviewed contract without neighbouring-version inference."""
    try:
        return RESOURCE_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed resource-domain contract"
        raise ValueError(message) from exc


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return the exact source closure used by each stable resource action."""
    recipe = resource_contract(version).resource
    base = (
        ("ResourcesController.queryResourceBaseDir",)
        if recipe.base_directory_wire == "endpoint"
        else ()
    )
    lookup = (recipe.lookup_operation,) if recipe.lookup_operation else ()
    parent = (*base, *lookup)
    target = lookup
    return {
        "resource.page": (*parent, recipe.page_operation),
        "resource.view": (
            *target,
            *(
                (recipe.view_evidence_operation,)
                if recipe.view_evidence_operation
                else ()
            ),
            recipe.view_operation,
        ),
        "resource.upload": (*parent, recipe.upload_operation),
        "resource.create": (*parent, recipe.create_operation),
        "resource.mkdir": (*parent, recipe.mkdir_operation),
        "resource.download": (*target, recipe.download_operation),
        "resource.delete": (*target, recipe.delete_operation),
    }


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit response roots needed by the generated runtime slice."""
    recipe = resource_contract(version).resource
    sources = semantic_operation_sources(version)
    roots: dict[str, tuple[str, ...]] = {}
    for semantic_operation, source_operations in sources.items():
        values = [_RESULT_MODEL]
        if semantic_operation == "resource.page":
            values.extend((recipe.page_model, recipe.item_model))
        elif (
            recipe.lookup_operation is not None
            and recipe.lookup_operation in source_operations
        ):
            values.append(recipe.item_model)
        if semantic_operation == "resource.view" and recipe.view_model is not None:
            values.append(recipe.view_model)
        roots[semantic_operation] = tuple(dict.fromkeys(values))
    return roots


def semantic_operation_evidence(version: str) -> dict[str, tuple[Evidence, ...]]:
    """Return controller and UI evidence for each exact stable action."""
    recipe = resource_contract(version).resource
    ui = (
        _LEGACY_UI
        if version
        in TARGET_RESOURCE_VERSIONS[: TARGET_RESOURCE_VERSIONS.index("3.0.0")]
        else _UI
    )
    evidence: dict[str, tuple[Evidence, ...]] = {}
    for semantic_operation, source_operations in semantic_operation_sources(
        version
    ).items():
        conclusion = "exact controller and UI invocation agree"
        if (
            semantic_operation == "resource.view"
            and recipe.view_wire == "binary-window"
        ):
            conclusion = (
                "viewResource misroutes limit; binary download plus local line "
                "window preserves the stable view contract"
            )
        elif semantic_operation == "resource.upload":
            conclusion = "controller requires multipart upload transport"
        elif semantic_operation == "resource.download":
            conclusion = "controller returns a binary response outside Result JSON"
        evidence[semantic_operation] = (
            Evidence(
                version,
                "controller",
                _CONTROLLER,
                ";".join(
                    operation.partition(".")[2] for operation in source_operations
                ),
                conclusion,
            ),
            Evidence(
                version,
                "ui",
                ui,
                semantic_operation.partition(".")[2],
                conclusion,
            ),
        )
    return evidence


def semantic_operation_facets(version: str) -> dict[str, tuple[str, ...]]:
    """Return compact reviewed facets for profile and audit output."""
    recipe = resource_contract(version).resource
    common = (
        f"identity:{recipe.identity_wire}",
        f"base-directory:{recipe.base_directory_wire}",
    )
    facets = (
        *common,
        f"parent:{recipe.parent_directory_wire}",
        f"view:{recipe.view_wire}",
        "upload:multipart",
        "download:binary",
    )
    return dict.fromkeys(RESOURCE_SEMANTIC_OPERATIONS, facets)


__all__ = [
    "RESOURCE_CONTRACTS",
    "RESOURCE_SEMANTIC_OPERATIONS",
    "TARGET_RESOURCE_VERSIONS",
    "Evidence",
    "ResourceRecipe",
    "ResourceVersionContract",
    "resource_contract",
    "semantic_operation_evidence",
    "semantic_operation_facets",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
]
