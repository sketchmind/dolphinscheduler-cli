"""Analyze generated operations actually consumed by one version adapter."""

from __future__ import annotations

import ast
import json
import re
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.contract_inputs import load_contract_input
from ds_codegen.contract_type_refs import collect_type_reference_names
from ds_codegen.contract_visibility import (
    has_executable_response_type,
    is_client_supplied_parameter,
)
from ds_codegen.extract.type_lookup import normalize_java_type_for_matching
from ds_codegen.snapshot_resolution import (
    ResolutionScope,
    ResolvedTypeGraph,
    ScopedTypeUse,
    SnapshotTypeResolver,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from ds_codegen.contract_inputs import ContractInput, LoadedContract
    from ds_codegen.ir import (
        ContractSnapshot,
        DtoSpec,
        EnumSpec,
        ModelSpec,
        OperationSpec,
        ParameterSpec,
    )

JsonObject = dict[str, Any]
_GENERATED_OPERATION = re.compile(
    r"^\s*DS operation:\s*(?P<source>[^|]+?)\s*\|\s*"
    r"(?P<method>[A-Z]+)\s+/(?P<path>\S+)\s*$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class AdapterGeneratedOperation:
    """One generated client method reached by the adapter source."""

    client_operation: str
    call_count: int
    source_method: str
    http_method: str
    path: str


def analyze_adapter_wire_candidates(
    *,
    adapter_source: Path,
    generated_client_source: Path,
    generated_operations_root: Path,
    inputs: Sequence[ContractInput],
    baseline_version: str = "3.4.1",
) -> JsonObject:
    """Build an exact all-version wire-family candidate report."""
    contracts = _load_exact_target_contracts(inputs)
    baseline = contracts[baseline_version].snapshot
    resolvers = {
        version: SnapshotTypeResolver.compile(contract.snapshot)
        for version, contract in contracts.items()
    }
    generated = discover_adapter_generated_operations(
        adapter_source=adapter_source,
        generated_client_source=generated_client_source,
        generated_operations_root=generated_operations_root,
    )
    operations = [
        _build_operation_report(
            generated_operation=item,
            baseline=baseline,
            contracts=contracts,
            resolvers=resolvers,
        )
        for item in generated
    ]
    return {
        "schema_version": 2,
        "kind": "dolphinscheduler-adapter-wire-family-candidates",
        "complete": True,
        "claim": "wire-family-candidates-only",
        "automatic_support": False,
        "comparison_unit": "unique-generated-client-operation",
        "candidate_scope": {
            "source_id": "exact baseline source operation id",
            "route": "HTTP method and path for the same source operation id",
            "effective_request": (
                "normalized source-declared request and request-type closure"
            ),
            "effective_response": (
                "normalized logical response, exact response-type closure, and "
                "resolved reference edges"
            ),
        },
        "baseline": {
            "version": baseline_version,
            "adapter_call_count": sum(item.call_count for item in generated),
            "generated_operation_count": len(generated),
        },
        "sources": [
            _source_summary(contracts[version]) for version in REVIEWED_DS_VERSIONS
        ],
        "summary": _summarize_versions(operations),
        "operations": operations,
    }


def discover_adapter_generated_operations(
    *,
    adapter_source: Path,
    generated_client_source: Path,
    generated_operations_root: Path,
) -> list[AdapterGeneratedOperation]:
    """Map every ``self.client.<group>.<method>()`` call to generated metadata."""
    client_classes, client_bindings = _generated_client_bindings(
        generated_client_source
    )
    calls = _adapter_client_calls(
        adapter_source,
        generated_client_classes=client_classes,
    )
    generated: list[AdapterGeneratedOperation] = []
    for (client_attribute, method_name), call_count in sorted(calls.items()):
        binding = client_bindings.get(client_attribute)
        if binding is None:
            message = (
                f"adapter calls unknown generated client attribute {client_attribute!r}"
            )
            raise ValueError(message)
        operations_class, module_name = binding
        source_method, http_method, path = _generated_method_metadata(
            generated_operations_root / f"{module_name.rsplit('.', 1)[-1]}.py",
            operations_class=operations_class,
            method_name=method_name,
        )
        generated.append(
            AdapterGeneratedOperation(
                client_operation=f"{client_attribute}.{method_name}",
                call_count=call_count,
                source_method=source_method,
                http_method=http_method,
                path=path,
            )
        )
    return generated


def render_adapter_wire_candidate_report(report: Mapping[str, object]) -> str:
    """Serialize one report deterministically for machine review."""
    return json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def load_exact_target_contracts(
    inputs: Sequence[ContractInput],
) -> dict[str, LoadedContract]:
    """Load the complete exact target matrix for candidate-only analyzers."""
    return _load_exact_target_contracts(inputs)


def source_contract_summary(contract: LoadedContract) -> JsonObject:
    """Return the compact exact-source identity shared by matrix reports."""
    return _source_summary(contract)


def resolve_operation_anchor(
    snapshot: ContractSnapshot,
    *,
    source_operation: str,
    http_method: str,
    path: str,
) -> OperationSpec:
    """Resolve one generated operation's exact baseline snapshot anchor."""
    generated = AdapterGeneratedOperation(
        client_operation=source_operation,
        call_count=1,
        source_method=source_operation,
        http_method=http_method,
        path=path.strip("/"),
    )
    return _resolve_baseline_operation(generated, snapshot)


def effective_request_candidate(
    operation: OperationSpec,
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver | None = None,
) -> JsonObject:
    """Normalize one operation request using the adapter-candidate rules."""
    return _effective_request_candidate(operation, snapshot, resolver=resolver)


def operation_by_source_id(
    snapshot: ContractSnapshot,
    source_operation: str,
) -> OperationSpec | None:
    """Resolve an exact source operation id, rejecting duplicate evidence."""
    return _operation_by_id(snapshot, source_operation)


def operations_by_exact_route(
    snapshot: ContractSnapshot,
    *,
    http_method: str,
    path: str,
) -> tuple[OperationSpec, ...]:
    """Return deterministic exact-route matches for rename discovery."""
    normalized_path = path.strip("/")
    return tuple(
        sorted(
            (
                operation
                for operation in snapshot.operations
                if operation.http_method == http_method
                and operation.path == normalized_path
            ),
            key=lambda operation: operation.operation_id,
        )
    )


def logical_return_candidate(operation: OperationSpec) -> str:
    """Normalize one logical response type using the shared matching rules."""
    return normalize_java_type_for_matching(operation.logical_return_type)


def effective_response_candidate(
    operation: OperationSpec,
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver | None = None,
) -> JsonObject:
    """Normalize one logical response and its transitive wire-type closure."""
    active_resolver = resolver or SnapshotTypeResolver.compile(snapshot)
    logical_return = logical_return_candidate(operation)
    if has_executable_response_type(operation):
        type_closure, reference_edges = _referenced_type_closure(
            snapshot,
            reference_names=collect_type_reference_names(operation.logical_return_type),
            scope=ResolutionScope(
                "operation_response",
                operation.operation_id,
            ),
            resolver=active_resolver,
        )
    else:
        type_closure, reference_edges = [], []
    candidate: JsonObject = {
        "logical_return": logical_return,
        "type_closure": type_closure,
    }
    if reference_edges:
        candidate["reference_edges"] = reference_edges
    return candidate


def _load_exact_target_contracts(
    inputs: Sequence[ContractInput],
) -> dict[str, LoadedContract]:
    labels = tuple(item.label for item in inputs)
    duplicates = sorted({label for label in labels if labels.count(label) > 1})
    if duplicates:
        message = f"duplicate adapter-analysis versions: {', '.join(duplicates)}"
        raise ValueError(message)
    missing = [version for version in REVIEWED_DS_VERSIONS if version not in labels]
    unexpected = [version for version in labels if version not in REVIEWED_DS_VERSIONS]
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing target versions: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected target versions: {', '.join(unexpected)}")
        raise ValueError("; ".join(details))
    contracts: dict[str, LoadedContract] = {}
    for item in inputs:
        contract = load_contract_input(item, require_exact=True)
        if contract.snapshot.ds_version != item.label:
            message = (
                f"adapter-analysis input {item.label!r} contains DS "
                f"{contract.snapshot.ds_version!r}"
            )
            raise ValueError(message)
        contracts[item.label] = contract
    return contracts


def _adapter_client_calls(
    adapter_source: Path,
    *,
    generated_client_classes: frozenset[str],
) -> Counter[tuple[str, str]]:
    tree = ast.parse(
        adapter_source.read_text(encoding="utf-8"),
        filename=str(adapter_source),
    )
    calls: Counter[tuple[str, str]] = Counter()
    for function in _defined_functions(tree):
        calls.update(
            _function_client_calls(
                function,
                generated_client_classes=generated_client_classes,
            )
        )
    if not calls:
        message = f"adapter contains no generated client calls: {adapter_source}"
        raise ValueError(message)
    return calls


def _function_client_calls(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    generated_client_classes: frozenset[str],
) -> Counter[tuple[str, str]]:
    arguments = (
        *function.args.posonlyargs,
        *function.args.args,
        *function.args.kwonlyargs,
    )
    client_variables = {
        argument.arg
        for argument in arguments
        if _annotation_name(argument.annotation) in generated_client_classes
    }
    if function.args.vararg is not None and (
        _annotation_name(function.args.vararg.annotation) in generated_client_classes
    ):
        client_variables.add(function.args.vararg.arg)
    if function.args.kwarg is not None and (
        _annotation_name(function.args.kwarg.annotation) in generated_client_classes
    ):
        client_variables.add(function.args.kwarg.arg)

    nodes = list(_walk_function_scope(function))
    group_aliases: dict[str, str] = {}
    for node in sorted(nodes, key=lambda item: getattr(item, "lineno", -1)):
        if (
            not isinstance(node, ast.Assign)
            or len(node.targets) != 1
            or not isinstance(node.targets[0], ast.Name)
        ):
            continue
        target_name = node.targets[0].id
        if _is_generated_client_root(node.value, client_variables):
            client_variables.add(target_name)
            continue
        group = _generated_group_access(node.value, client_variables)
        if group is None:
            continue
        previous = group_aliases.get(target_name)
        if previous is not None and previous != group:
            message = (
                f"generated operation alias {target_name!r} is ambiguous: "
                f"{previous!r} and {group!r}"
            )
            raise ValueError(message)
        group_aliases[target_name] = group

    calls: Counter[tuple[str, str]] = Counter()
    for node in nodes:
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        group_access = node.func.value
        if isinstance(group_access, ast.Name):
            group = group_aliases.get(group_access.id)
            if group is not None:
                calls[(group, node.func.attr)] += 1
            continue
        group = _generated_group_access(group_access, client_variables)
        if group is not None:
            calls[(group, node.func.attr)] += 1
    return calls


def _defined_functions(
    tree: ast.AST,
) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _walk_function_scope(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[ast.AST]:
    nodes: list[ast.AST] = []
    pending: list[ast.AST] = list(reversed(function.body))
    while pending:
        node = pending.pop()
        nodes.append(node)
        children = list(ast.iter_child_nodes(node))
        pending.extend(
            reversed(
                [
                    child
                    for child in children
                    if not isinstance(
                        child,
                        (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
                    )
                ]
            )
        )
    return nodes


def _annotation_name(annotation: ast.expr | None) -> str | None:
    if annotation is None:
        return None
    rendered = ast.unparse(annotation).strip("'\"")
    return rendered.rsplit(".", 1)[-1]


def _is_generated_client_root(
    node: ast.AST,
    client_variables: set[str],
) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "client"
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    ) or (isinstance(node, ast.Name) and node.id in client_variables)


def _generated_group_access(
    node: ast.AST,
    client_variables: set[str],
) -> str | None:
    if not isinstance(node, ast.Attribute):
        return None
    if _is_generated_client_root(node.value, client_variables):
        return node.attr
    return None


def _generated_client_bindings(
    generated_client_source: Path,
) -> tuple[frozenset[str], dict[str, tuple[str, str]]]:
    tree = ast.parse(
        generated_client_source.read_text(encoding="utf-8"),
        filename=str(generated_client_source),
    )
    imports: dict[str, str] = {}
    for import_node in tree.body:
        if not isinstance(import_node, ast.ImportFrom) or import_node.module is None:
            continue
        for imported in import_node.names:
            imports[imported.asname or imported.name] = import_node.module
    bindings: dict[str, tuple[str, str]] = {}
    client_classes: set[str] = set()
    for class_node in tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        class_has_binding = False
        for walked_node in ast.walk(class_node):
            if (
                not isinstance(walked_node, ast.Assign)
                or len(walked_node.targets) != 1
                or not isinstance(walked_node.targets[0], ast.Attribute)
                or not isinstance(walked_node.targets[0].value, ast.Name)
                or walked_node.targets[0].value.id != "self"
                or not isinstance(walked_node.value, ast.Call)
                or not isinstance(walked_node.value.func, ast.Name)
            ):
                continue
            operations_class = walked_node.value.func.id
            module_name = imports.get(operations_class)
            if module_name is None:
                continue
            bindings[walked_node.targets[0].attr] = (
                operations_class,
                module_name,
            )
            class_has_binding = True
        if class_has_binding:
            client_classes.add(class_node.name)
    return frozenset(client_classes), bindings


def _generated_method_metadata(
    operation_source: Path,
    *,
    operations_class: str,
    method_name: str,
) -> tuple[str, str, str]:
    if not operation_source.is_file():
        message = f"generated operation module does not exist: {operation_source}"
        raise ValueError(message)
    tree = ast.parse(
        operation_source.read_text(encoding="utf-8"),
        filename=str(operation_source),
    )
    class_node = next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == operations_class
        ),
        None,
    )
    method_node = (
        next(
            (
                node
                for node in class_node.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == method_name
            ),
            None,
        )
        if class_node is not None
        else None
    )
    if method_node is None:
        message = (
            f"generated operation {operations_class}.{method_name} does not exist "
            f"in {operation_source}"
        )
        raise ValueError(message)
    documentation = ast.get_docstring(method_node, clean=False) or ""
    match = _GENERATED_OPERATION.search(documentation)
    if match is None:
        message = (
            f"generated operation {operations_class}.{method_name} has no "
            "DS operation metadata"
        )
        raise ValueError(message)
    return (
        match.group("source").strip(),
        match.group("method"),
        match.group("path").strip("/"),
    )


def _build_operation_report(
    *,
    generated_operation: AdapterGeneratedOperation,
    baseline: ContractSnapshot,
    contracts: Mapping[str, LoadedContract],
    resolvers: Mapping[str, SnapshotTypeResolver],
) -> JsonObject:
    baseline_operation = _resolve_baseline_operation(generated_operation, baseline)
    baseline_request = _effective_request_candidate(
        baseline_operation,
        baseline,
        resolver=resolvers[baseline.ds_version],
    )
    baseline_response = effective_response_candidate(
        baseline_operation,
        baseline,
        resolver=resolvers[baseline.ds_version],
    )
    versions = []
    for version in REVIEWED_DS_VERSIONS:
        target = _operation_by_id(
            contracts[version].snapshot,
            baseline_operation.operation_id,
        )
        if target is None:
            same_source_id = False
            same_route = False
            same_request = False
            same_response = False
        else:
            same_source_id = True
            same_route = (
                target.http_method == baseline_operation.http_method
                and target.path == baseline_operation.path
            )
            same_request = (
                _effective_request_candidate(
                    target,
                    contracts[version].snapshot,
                    resolver=resolvers[version],
                )
                == baseline_request
            )
            same_response = (
                effective_response_candidate(
                    target,
                    contracts[version].snapshot,
                    resolver=resolvers[version],
                )
                == baseline_response
            )
        versions.append(
            {
                "version": version,
                "same_source_id": same_source_id,
                "same_route": same_route,
                "same_effective_request_candidate": same_request,
                "same_effective_response_candidate": same_response,
                "same_effective_request_and_response_candidate": (
                    same_request and same_response
                ),
            }
        )
    return {
        "client_operation": generated_operation.client_operation,
        "call_count": generated_operation.call_count,
        "source_operation": baseline_operation.operation_id,
        "baseline": {
            "http_method": baseline_operation.http_method,
            "path": baseline_operation.path,
            "effective_request_candidate": baseline_request,
            "effective_response_candidate": baseline_response,
        },
        "versions": versions,
    }


def _resolve_baseline_operation(
    generated: AdapterGeneratedOperation,
    baseline: ContractSnapshot,
) -> OperationSpec:
    route_matches = [
        operation
        for operation in baseline.operations
        if operation.http_method == generated.http_method
        and operation.path == generated.path
    ]
    exact_matches = [
        operation
        for operation in route_matches
        if operation.operation_id == generated.source_method
    ]
    matches = exact_matches or [
        operation
        for operation in route_matches
        if f"{operation.controller}.{operation.method_name}" == generated.source_method
    ]
    if len(matches) != 1:
        message = (
            f"generated call {generated.client_operation!r} maps to "
            f"{generated.source_method} {generated.http_method} /{generated.path}, "
            f"but the baseline contains {len(matches)} matching operations"
        )
        raise ValueError(message)
    return matches[0]


def _operation_by_id(
    snapshot: ContractSnapshot,
    operation_id: str,
) -> OperationSpec | None:
    matches = [
        operation
        for operation in snapshot.operations
        if operation.operation_id == operation_id
    ]
    if len(matches) > 1:
        message = (
            f"DS {snapshot.ds_version} contains duplicate operation id {operation_id!r}"
        )
        raise ValueError(message)
    return matches[0] if matches else None


def _effective_request_candidate(
    operation: OperationSpec,
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver | None = None,
) -> JsonObject:
    active_resolver = resolver or SnapshotTypeResolver.compile(snapshot)
    parameters = [
        _effective_parameter_candidate(parameter)
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    ]
    parameters.sort(
        key=lambda item: (
            str(item["binding"]),
            str(item["wire_name"]),
            str(item["java_type"]),
        )
    )
    type_closure, reference_edges = _request_type_closure(
        operation,
        snapshot,
        resolver=active_resolver,
    )
    candidate: JsonObject = {
        "http_method": operation.http_method,
        "path": operation.path,
        "consumes": sorted(operation.consumes),
        "parameters": parameters,
        "type_closure": type_closure,
    }
    if reference_edges:
        candidate["reference_edges"] = reference_edges
    return candidate


def _effective_parameter_candidate(parameter: ParameterSpec) -> JsonObject:
    required = parameter.required
    if required is None and parameter.binding in {
        "path_variable",
        "request_body",
        "request_param",
    }:
        required = parameter.default_value is None
    return {
        "binding": parameter.binding,
        "wire_name": parameter.wire_name or parameter.name,
        "java_type": normalize_java_type_for_matching(parameter.java_type),
        "required": required,
        "default_value": parameter.default_value,
    }


def _request_type_closure(
    operation: OperationSpec,
    snapshot: ContractSnapshot,
    *,
    resolver: SnapshotTypeResolver,
) -> tuple[list[JsonObject], list[JsonObject]]:
    reference_names = {
        name
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
        for name in collect_type_reference_names(parameter.java_type)
    }
    return _referenced_type_closure(
        snapshot,
        reference_names=reference_names,
        scope=ResolutionScope("operation_request", operation.operation_id),
        resolver=resolver,
    )


def _referenced_type_closure(
    snapshot: ContractSnapshot,
    *,
    reference_names: set[str],
    scope: ResolutionScope,
    resolver: SnapshotTypeResolver,
) -> tuple[list[JsonObject], list[JsonObject]]:
    specs_by_ref: dict[tuple[str, str], DtoSpec | ModelSpec | EnumSpec] = {}
    refs_by_import_path: dict[str, tuple[str, str]] = {}
    for surface, specs in (
        ("dtos", snapshot.dtos),
        ("models", snapshot.models),
        ("enums", snapshot.enums),
    ):
        for spec in specs:
            ref = (surface, spec.import_path)
            specs_by_ref[ref] = spec
            refs_by_import_path[spec.import_path] = ref

    graph = resolver.resolve_type_graph(
        ScopedTypeUse(reference_name, scope) for reference_name in reference_names
    )
    closure = {refs_by_import_path[import_path] for import_path in graph.import_paths}
    return (
        [
            _type_candidate(surface, specs_by_ref[(surface, import_path)])
            for surface, import_path in sorted(closure)
        ],
        _reference_edge_candidates(graph),
    )


def _reference_edge_candidates(graph: ResolvedTypeGraph) -> list[JsonObject]:
    return [
        {
            "owner_kind": edge.owner_kind,
            "owner_ref": (
                "$operation"
                if edge.owner_kind in {"operation_request", "operation_response"}
                else edge.owner_ref
            ),
            "reference_name": edge.reference_name,
            "import_path": edge.import_path,
        }
        for edge in sorted(
            graph.edges,
            key=lambda item: (
                item.owner_kind,
                item.owner_ref,
                item.reference_name,
                item.import_path,
            ),
        )
    ]


def _type_candidate(
    surface: str,
    spec: DtoSpec | ModelSpec | EnumSpec,
) -> JsonObject:
    if surface == "enums":
        enum_spec = cast("EnumSpec", spec)
        return {
            "surface": surface,
            "import_path": enum_spec.import_path,
            "json_value_field": enum_spec.json_value_field,
            "values": [
                {"name": value.name, "arguments": value.arguments}
                for value in enum_spec.values
            ],
        }
    structured_spec = cast("DtoSpec | ModelSpec", spec)
    return {
        "surface": surface,
        "import_path": structured_spec.import_path,
        "extends": _normalize_optional_java_type(structured_spec.extends),
        "fields": [
            {
                "wire_name": field.wire_name,
                "java_type": normalize_java_type_for_matching(field.java_type),
                "required": field.required,
                "default_value": field.default_value,
                "nullable": field.nullable,
                "default_factory": field.default_factory,
                "allowable_values": field.allowable_values,
            }
            for field in sorted(
                structured_spec.fields,
                key=lambda item: item.wire_name,
            )
        ],
    }


def _normalize_optional_java_type(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        message = f"structured type extends must be a string or null: {value!r}"
        raise TypeError(message)
    return normalize_java_type_for_matching(value)


def _summarize_versions(operations: list[JsonObject]) -> list[JsonObject]:
    summary = []
    for index, version in enumerate(REVIEWED_DS_VERSIONS):
        rows = [operation["versions"][index] for operation in operations]
        summary.append(
            {
                "version": version,
                "same_source_id": _count_true(rows, "same_source_id"),
                "same_route": _count_true(rows, "same_route"),
                "same_effective_request_candidate": _count_true(
                    rows,
                    "same_effective_request_candidate",
                ),
                "same_effective_response_candidate": _count_true(
                    rows,
                    "same_effective_response_candidate",
                ),
                "same_effective_request_and_response_candidate": _count_true(
                    rows,
                    "same_effective_request_and_response_candidate",
                ),
            }
        )
    return summary


def _count_true(rows: list[JsonObject], key: str) -> int:
    return sum(row.get(key) is True for row in rows)


def _source_summary(contract: LoadedContract) -> JsonObject:
    origin = contract.provenance["origin"]
    git = origin["git"]
    return {
        "version": contract.label,
        "contract_digest": contract.provenance["contract_digest"],
        "tag": git["tag"],
        "commit": git["commit"],
        "tree": git["tree"],
    }


__all__ = [
    "AdapterGeneratedOperation",
    "analyze_adapter_wire_candidates",
    "discover_adapter_generated_operations",
    "effective_request_candidate",
    "effective_response_candidate",
    "load_exact_target_contracts",
    "logical_return_candidate",
    "operation_by_source_id",
    "operations_by_exact_route",
    "render_adapter_wire_candidate_report",
    "resolve_operation_anchor",
    "source_contract_summary",
]
