"""Resolve operation-level logical payload types from raw controller signatures."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import TYPE_CHECKING, NoReturn, assert_never

import javalang

from ds_codegen.contract_type_refs import (
    canonicalize_builtin_type_expression,
    collect_type_reference_names,
    is_fully_qualified_reference_name,
)
from ds_codegen.extract.inference_support import _is_weak_inferred_type
from ds_codegen.extract.return_type_reviews import (
    REVIEWED_RETURN_TYPE_RULES,
    REVIEWED_SOURCE_VERSIONS,
    REVIEWED_TYPE_IMPORTS,
)
from ds_codegen.extract.return_type_rules import (
    DAO_RESOURCE_IMPORT,
    RETURN_TYPE_RULES,
    ReturnTypeRule,
    SourceEvidence,
)
from ds_codegen.extract.type_lookup import _render_reference_name, _render_type
from ds_codegen.java_source import (
    SourceResolutionScope,
    load_type_declaration,
    logical_type_name,
    resolve_referenced_import_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection
    from pathlib import Path

    from ds_codegen.ir import ResponseProjection


@dataclass(frozen=True)
class _ReviewedOverride:
    rule: ReturnTypeRule
    expected_raw_type: str


def _reviewed_overrides() -> dict[tuple[str, str], _ReviewedOverride]:
    rules = {rule.name: rule for rule in RETURN_TYPE_RULES}
    if len(rules) != len(RETURN_TYPE_RULES):
        message = "duplicate return-type rule name"
        raise RuntimeError(message)
    overrides: dict[tuple[str, str], _ReviewedOverride] = {}
    for review in REVIEWED_RETURN_TYPE_RULES:
        rule = rules[review.rule_name]
        if review.raw_return_type not in rule.raw_return_types:
            message = f"invalid reviewed raw shape for {rule.name!r}"
            raise RuntimeError(message)
        for version in review.versions:
            key = (version, rule.operation_id)
            if key in overrides:
                message = f"duplicate reviewed return-type override {key!r}"
                raise RuntimeError(message)
            overrides[key] = _ReviewedOverride(rule, review.raw_return_type)
    return overrides


@dataclass
class LogicalReturnTypeOverrideScope:
    """Track source-scoped logical return overrides for one extraction."""

    source_version: str
    _match_counts: Counter[str] = dataclass_field(default_factory=Counter)
    _type_import_match_counts: Counter[tuple[str, str]] = dataclass_field(
        default_factory=Counter
    )

    _matched_rules: dict[str, ReturnTypeRule] = dataclass_field(default_factory=dict)

    def resolve(
        self,
        *,
        operation_id: str,
        repo_root: Path,
        raw_return_type: str,
        inferred_return_type: str | None,
        import_map: dict[str, str],
    ) -> str | None:
        """Return one applicable override while validating its source shape."""

        reviewed = _reviewed_overrides().get((self.source_version, operation_id))
        inferred_evidence_shape = _logical_type_evidence_shape(inferred_return_type)
        if reviewed is not None:
            rule = reviewed.rule
            if (raw_return_type, inferred_evidence_shape) != (
                reviewed.expected_raw_type,
                rule.inferred_return_type,
            ):
                message = (
                    f"{operation_id} return-type override no longer matches the "
                    "expected upstream controller/service shape for DolphinScheduler "
                    f"{self.source_version}; got raw={raw_return_type!r}, "
                    f"inferred={inferred_return_type!r}"
                )
                raise RuntimeError(message)
        else:
            if self.source_version in REVIEWED_SOURCE_VERSIONS:
                return None
            candidates = [
                candidate
                for candidate in RETURN_TYPE_RULES
                if candidate.operation_id == operation_id
                and candidate.candidate_enabled
                and candidate.source_evidence is not None
                and raw_return_type in candidate.raw_return_types
                and inferred_evidence_shape == candidate.inferred_return_type
            ]
            if not candidates:
                # Preserve the original inference when no proven rule describes it.
                return None
            if len(candidates) != 1:
                names = ", ".join(candidate.name for candidate in candidates)
                message = (
                    f"{operation_id} has ambiguous return-type rules for "
                    f"DolphinScheduler {self.source_version}: {names}"
                )
                raise RuntimeError(message)
            rule = candidates[0]
        if rule.source_evidence is not None:
            _validate_source_evidence(
                evidence=rule.source_evidence,
                operation_id=operation_id,
                source_version=self.source_version,
                repo_root=repo_root,
                import_map=import_map,
            )
        self._match_counts[operation_id] += 1
        self._matched_rules[operation_id] = rule
        return rule.logical_return_type

    def resolve_type_import_path(
        self,
        repo_root: Path,
        *,
        operation_id: str,
        type_name: str,
        import_map: dict[str, str],
        package_name: str | None,
        owner_import_path: str | None,
        default_resolver: Callable[..., str | None],
    ) -> str | None:
        """Resolve source-proven logical types before controller imports win."""
        overrides = {
            override_type_name: import_path
            for (
                version,
                override_operation,
                override_type_name,
            ), import_path in _all_type_import_overrides().items()
            if version == self.source_version and override_operation == operation_id
        }
        matched_rule = self._matched_rules.get(operation_id)
        if matched_rule is not None:
            overrides.update(matched_rule.type_import_overrides)
        for override_type_name, import_path in overrides.items():
            if type_name in {override_type_name, import_path}:
                self._type_import_match_counts[(operation_id, override_type_name)] = 1
                return import_path
        return default_resolver(
            repo_root,
            type_name,
            SourceResolutionScope(import_map, package_name, owner_import_path),
        )

    def validate(self, *, controller_names: Collection[str]) -> None:
        """Require each override for an extracted controller to match exactly once."""

        in_scope_controllers = frozenset(controller_names)
        expected_operation_ids = {
            operation_id
            for source_version, operation_id in _reviewed_overrides()
            if source_version == self.source_version
            and operation_id.partition(".")[0] in in_scope_controllers
        }
        expected_operation_ids.update(self._matched_rules)
        for operation_id in sorted(expected_operation_ids):
            match_count = self._match_counts[operation_id]
            if match_count == 1:
                continue
            message = (
                f"{operation_id} return-type override expected one match for "
                f"DolphinScheduler {self.source_version}, found {match_count}"
            )
            raise RuntimeError(message)

        expected_type_import_overrides = {
            (operation_id, type_name)
            for source_version, operation_id, type_name in _all_type_import_overrides()
            if source_version == self.source_version
            and operation_id.partition(".")[0] in in_scope_controllers
        }
        expected_type_import_overrides.update(
            (operation_id, type_name)
            for operation_id, rule in self._matched_rules.items()
            for type_name, _ in rule.type_import_overrides
        )
        for operation_id, type_name in sorted(expected_type_import_overrides):
            match_count = self._type_import_match_counts[(operation_id, type_name)]
            if match_count == 1:
                continue
            message = (
                f"{operation_id} logical type-import override expected one "
                f"match for {type_name!r} in DolphinScheduler "
                f"{self.source_version}, found {match_count}"
            )
            raise RuntimeError(message)

    def response_model_projections(self) -> dict[str, str]:
        """Return source-validated operation projections for this extraction."""
        return {
            operation_id: rule.response_model_projection
            for operation_id, rule in self._matched_rules.items()
            if rule.response_model_projection is not None
        }


def operation_type_import_override(
    source_version: str,
    operation_id: str,
    type_name: str,
) -> str | None:
    """Return one reviewed operation-scoped type identity without source lookup."""
    return _all_type_import_overrides().get((source_version, operation_id, type_name))


def _all_type_import_overrides() -> dict[tuple[str, str, str], str]:
    overrides = dict(REVIEWED_TYPE_IMPORTS)
    for (version, operation_id), reviewed in _reviewed_overrides().items():
        for type_name, import_path in reviewed.rule.type_import_overrides:
            key = (version, operation_id, type_name)
            existing = overrides.setdefault(key, import_path)
            if existing != import_path:
                message = f"conflicting type-import override for {key!r}"
                raise RuntimeError(message)

    return overrides


def _logical_type_evidence_shape(java_type: str | None) -> str | None:
    """Compare reviewed source shapes independently of exact import spelling."""

    if java_type is None:
        return None
    normalized = canonicalize_builtin_type_expression(java_type)
    replacements = {
        reference_name: logical_type_name(reference_name)
        for reference_name in collect_type_reference_names(normalized)
        if "." in reference_name
    }
    for reference_name, logical_name in sorted(
        replacements.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        normalized = re.sub(
            rf"(?<![A-Za-z0-9_$.]){re.escape(reference_name)}"
            r"(?![A-Za-z0-9_$.])",
            logical_name,
            normalized,
        )
    return normalized


def unwrap_result_like_type(java_type: str) -> str | None:
    """Return the payload type for direct result-like wrappers."""

    if java_type == "Result":
        return "Object"
    if java_type.startswith("Result<") and java_type.endswith(">"):
        return java_type[7:-1]
    if java_type == "TaskInstanceSuccessResponse":
        return "Void"
    return None


def resolve_operation_logical_return_type(
    *,
    override_scope: LogicalReturnTypeOverrideScope,
    operation_id: str,
    repo_root: Path,
    raw_return_type: str,
    inferred_return_type: str | None,
    import_map: dict[str, str],
    package_name: str | None,
    owner_import_path: str | None = None,
) -> str:
    """Resolve the logical payload returned by an operation after DS envelope unwrap."""

    overridden_return_type = override_scope.resolve(
        operation_id=operation_id,
        repo_root=repo_root,
        raw_return_type=raw_return_type,
        inferred_return_type=inferred_return_type,
        import_map=import_map,
    )
    if overridden_return_type is not None:
        return overridden_return_type

    direct_payload_type = unwrap_result_like_type(raw_return_type)
    # Body inference may stop at an intermediate stream before its map/collect
    # chain. Its element type is not the serialized result. A concrete list
    # signature recovers that result without losing inferred nullability or
    # structural projections for other response shapes.
    if (
        direct_payload_type is not None
        and direct_payload_type.startswith("List<")
        and not _is_weak_logical_type_candidate(direct_payload_type)
        and inferred_return_type is not None
        and inferred_return_type.startswith("Stream<")
    ):
        return direct_payload_type
    if inferred_return_type is not None and not _is_weak_logical_type_candidate(
        inferred_return_type
    ):
        return inferred_return_type
    if direct_payload_type is not None:
        if direct_payload_type != "Object":
            return direct_payload_type
        if inferred_return_type is not None:
            return inferred_return_type
        return direct_payload_type
    specialized_payload_type = _resolve_result_wrapper_payload_type(
        repo_root=repo_root,
        return_type=raw_return_type,
        import_map=import_map,
        package_name=package_name,
        owner_import_path=owner_import_path,
        active_return_types=(),
    )
    if specialized_payload_type is not None:
        if specialized_payload_type != "Object":
            return specialized_payload_type
        if inferred_return_type is not None:
            return inferred_return_type
        return specialized_payload_type
    if inferred_return_type is not None:
        return inferred_return_type
    return raw_return_type


def _validate_source_evidence(
    *,
    evidence: SourceEvidence,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    if evidence == "alert_plugin_page_nullable_total_list":
        _validate_alert_plugin_page_nullable_total_list(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
        )
        return
    if evidence == "schedule_preview_string_stream":
        _validate_schedule_preview_string_stream(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "legacy_current_user_data_list":
        _validate_legacy_current_user_data_list(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "legacy_datasource_page_records":
        _validate_legacy_datasource_page_records(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "legacy_process_definition_page_records":
        _validate_legacy_process_definition_page_records(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "legacy_schedule_create_missing_data_list":
        _validate_legacy_schedule_create_missing_data_list(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "schedule_create_data_list":
        _validate_schedule_create_data_list(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "resource_page_records":
        _validate_resource_page_records(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "resource_detail_record":
        _validate_resource_detail_record(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
            import_map=import_map,
        )
        return
    if evidence == "task_delete_nullable_payload":
        _validate_task_delete_nullable_payload(
            operation_id=operation_id,
            source_version=source_version,
            repo_root=repo_root,
        )
        return
    assert_never(evidence)


def _validate_alert_plugin_page_nullable_total_list(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
) -> None:
    """Prove this page setter can overwrite PageInfo's default with null."""

    service_import = (
        "org.apache.dolphinscheduler.api.service.impl.AlertPluginInstanceServiceImpl"
    )
    loaded = load_type_declaration(repo_root, service_import, {})
    if loaded is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="alert-plugin service implementation cannot be loaded",
        )
    declaration = loaded[1]
    page_method = _require_exact_class_method(
        declaration,
        method_name="listPaging",
        operation_id=operation_id,
        source_version=source_version,
        owner_label=service_import,
    )
    helper_method = _require_exact_class_method(
        declaration,
        method_name="buildPluginInstanceVOList",
        operation_id=operation_id,
        source_version=source_version,
        owner_label=service_import,
    )

    page_variables = {
        declarator.name
        for _, node in page_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "PageInfo<AlertPluginInstanceVO>"
        for declarator in node.declarators
        if isinstance(declarator.initializer, javalang.tree.ClassCreator)
        and _render_type(declarator.initializer.type) == "PageInfo"
    }
    nullable_writes = [
        node
        for _, node in page_method
        if isinstance(node, javalang.tree.MethodInvocation)
        and node.qualifier in page_variables
        and node.member == "setTotalList"
        and len(node.arguments) == 1
        and isinstance(node.arguments[0], javalang.tree.MethodInvocation)
        and node.arguments[0].member == "buildPluginInstanceVOList"
        and not node.arguments[0].qualifier
    ]
    if len(page_variables) != 1 or len(nullable_writes) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "listPaging no longer writes the reviewed helper result to exactly "
                "one PageInfo.totalList"
            ),
        )

    empty_inputs: set[str] = set()
    for statement in helper_method.body or ():
        if not isinstance(statement, javalang.tree.IfStatement):
            continue
        condition = statement.condition
        then_statement = statement.then_statement
        if not (
            isinstance(condition, javalang.tree.MethodInvocation)
            and condition.qualifier == "CollectionUtils"
            and condition.member == "isEmpty"
            and len(condition.arguments) == 1
            and isinstance(condition.arguments[0], javalang.tree.MemberReference)
            and not condition.arguments[0].qualifier
            and isinstance(then_statement, javalang.tree.BlockStatement)
            and len(then_statement.statements) == 1
            and isinstance(then_statement.statements[0], javalang.tree.ReturnStatement)
            and isinstance(
                then_statement.statements[0].expression, javalang.tree.Literal
            )
            and then_statement.statements[0].expression.value == "null"
        ):
            continue
        empty_inputs.add(condition.arguments[0].member)
    if empty_inputs != {"alertPluginInstances", "pluginDefineList"}:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "buildPluginInstanceVOList no longer has the reviewed null returns "
                "for empty instances and plugin definitions"
            ),
        )


def _validate_schedule_preview_string_stream(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove the returned DATA_LIST is a stream mapped through a String method."""

    def fail(detail: str) -> NoReturn:
        _raise_source_evidence_error(
            operation_id=operation_id, source_version=source_version, detail=detail
        )

    def method_at(
        import_path: str, method_name: str
    ) -> javalang.tree.MethodDeclaration:
        loaded = load_type_declaration(repo_root, import_path, {})
        if loaded is None:
            fail(f"preview evidence cannot load {import_path}")
        return _require_exact_class_method(
            loaded[1],
            method_name=method_name,
            operation_id=operation_id,
            source_version=source_version,
            owner_label=import_path,
        )

    service_import = "org.apache.dolphinscheduler.api.service.SchedulerService"
    if import_map.get("SchedulerService") != service_import:
        fail("preview controller no longer imports SchedulerService")
    controller = method_at(
        "org.apache.dolphinscheduler.api.controller.SchedulerController",
        "previewSchedule",
    )
    result_variables = [
        declarator.name
        for _, node in controller
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer,
            qualifier="schedulerService",
            member="previewSchedule",
        )
    ]
    returns = [
        node.expression
        for _, node in controller
        if isinstance(node, javalang.tree.ReturnStatement)
    ]
    if (
        len(result_variables) != 1
        or len(returns) != 1
        or not (
            _is_method_invocation(
                returns[0], qualifier="", member="returnDataList", argument_count=1
            )
            and _is_variable_reference(returns[0].arguments[0], result_variables[0])
        )
    ):
        fail("preview controller no longer returns exactly the service DATA_LIST map")

    loaded_service = load_type_declaration(repo_root, service_import, {})
    if loaded_service is None:
        fail("preview service declaration cannot be loaded")
    if isinstance(loaded_service[1], javalang.tree.InterfaceDeclaration):
        service_import = (
            "org.apache.dolphinscheduler.api.service.impl.SchedulerServiceImpl"
        )
    service = method_at(service_import, "previewSchedule")
    loaded_implementation = load_type_declaration(repo_root, service_import, {})
    if loaded_implementation is None:
        fail("preview service implementation cannot be loaded")
    date_utils_import = "org.apache.dolphinscheduler.common.utils.DateUtils"
    if loaded_implementation[2].get("DateUtils") != date_utils_import:
        fail("preview service no longer imports the reviewed DateUtils")
    date_formatter = method_at(date_utils_import, "dateToString")
    if _render_type(date_formatter.return_type) != "String" or [
        _render_type(parameter.type) for parameter in date_formatter.parameters
    ] != ["Date"]:
        fail("preview date formatter no longer maps Date to String")

    service_maps = {
        declarator.name
        for _, node in service
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
    }
    date_lists = {
        declarator.name
        for _, node in service
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "List<Date>"
        for declarator in node.declarators
    }
    writes = [
        node
        for _, node in service
        if isinstance(node, javalang.tree.MethodInvocation)
        and node.qualifier in service_maps
        and node.member == "put"
        and len(node.arguments) == 2
        and _is_qualified_member_reference(
            node.arguments[0], qualifier="Constants", member="DATA_LIST"
        )
    ]
    if len(writes) != 1:
        fail("preview service no longer writes exactly one DATA_LIST")
    returned_maps = [
        node.expression
        for _, node in service
        if isinstance(node, javalang.tree.ReturnStatement)
    ]
    if not returned_maps or any(
        not _is_variable_reference(expression, writes[0].qualifier)
        for expression in returned_maps
    ):
        fail("preview service no longer returns the DATA_LIST map on every path")
    payload = writes[0].arguments[1]
    if not (
        isinstance(payload, javalang.tree.MethodInvocation)
        and payload.qualifier in date_lists
        and payload.member == "stream"
        and not payload.arguments
        and len(payload.selectors or ()) == 1
    ):
        fail("preview DATA_LIST is no longer a directly mapped date stream")
    mapping = payload.selectors[0]
    if not (
        isinstance(mapping, javalang.tree.MethodInvocation)
        and mapping.member == "map"
        and len(mapping.arguments) == 1
        and not mapping.selectors
        and _is_date_string_mapping(mapping.arguments[0])
    ):
        fail("preview date stream no longer maps exactly DateUtils.dateToString")

    base = method_at(
        "org.apache.dolphinscheduler.api.controller.BaseController", "returnDataList"
    )
    if len(base.parameters) != 1:
        fail("returnDataList no longer receives exactly one map")
    map_name = base.parameters[0].name
    payload_variables = [
        declarator.name
        for _, node in base
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer, qualifier=map_name, member="get", argument_count=1
        )
        and _is_qualified_member_reference(
            declarator.initializer.arguments[0],
            qualifier="Constants",
            member="DATA_LIST",
        )
    ]
    successes = [
        node.expression
        for _, node in base
        if isinstance(node, javalang.tree.ReturnStatement)
        and _is_method_invocation(
            node.expression, qualifier="", member="success", argument_count=2
        )
    ]
    if (
        len(payload_variables) != 1
        or len(successes) != 1
        or not (_is_variable_reference(successes[0].arguments[1], payload_variables[0]))
    ):
        fail("returnDataList no longer forwards DATA_LIST as the success payload")


def _is_date_string_mapping(node: object) -> bool:
    if isinstance(node, javalang.tree.MethodReference):
        return _is_variable_reference(node.expression, "DateUtils") and (
            _is_variable_reference(node.method, "dateToString")
        )
    if isinstance(node, javalang.tree.LambdaExpression) and len(node.parameters) == 1:
        parameter = node.parameters[0]
        return isinstance(parameter, javalang.tree.MemberReference) and (
            _is_method_invocation(
                node.body,
                qualifier="DateUtils",
                member="dateToString",
                argument_count=1,
            )
            and not node.body.selectors
            and _is_variable_reference(node.body.arguments[0], parameter.member)
        )
    return False


def _validate_task_delete_nullable_payload(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
) -> None:
    """Prove delete has both a ProcessDefinition and a null success path."""
    service_import = (
        "org.apache.dolphinscheduler.api.service.impl.TaskDefinitionServiceImpl"
    )
    loaded = load_type_declaration(repo_root, service_import, {})
    if loaded is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed task-definition service cannot be loaded",
        )
    _, service, _, _ = loaded
    method = _require_exact_class_method(
        service,
        method_name="deleteTaskDefinitionByCode",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="task-definition service",
    )
    if _render_type(method.return_type) != "Map<String, Object>" or tuple(
        _render_type(parameter.type) for parameter in method.parameters
    ) != ("User", "long", "long"):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed task-definition delete signature changed",
        )

    result_names = {
        declarator.name
        for _, node in method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
    }
    upstream_relation_names = {
        declarator.name
        for _, node in method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "List<ProcessTaskRelation>"
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer,
            qualifier="processTaskRelationMapper",
            member="queryUpstreamByCode",
            argument_count=2,
        )
    }
    if len(result_names) != 1 or len(upstream_relation_names) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=("expected one result map and one reviewed upstream-relation list"),
        )
    result_name = next(iter(result_names))
    relation_name = next(iter(upstream_relation_names))

    matching_branches = []
    for _, node in method:
        if not isinstance(node, javalang.tree.IfStatement):
            continue
        condition = node.condition
        if not (
            _is_method_invocation(
                condition,
                qualifier=relation_name,
                member="isEmpty",
                argument_count=0,
            )
            and condition.prefix_operators == ["!"]
            and node.else_statement is not None
        ):
            continue
        update_calls = [
            child
            for _, child in node.then_statement
            if _is_method_invocation(
                child,
                qualifier="",
                member="updateDag",
            )
            and len(child.arguments) >= 2
            and _is_variable_reference(child.arguments[1], result_name)
        ]
        null_success_calls = [
            child
            for _, child in node.else_statement
            if _is_method_invocation(
                child,
                qualifier="",
                member="putMsg",
                argument_count=2,
            )
            and _is_variable_reference(child.arguments[0], result_name)
            and _is_qualified_member_reference(
                child.arguments[1],
                qualifier="Status",
                member="SUCCESS",
            )
        ]
        payload_writes = [
            child
            for _, child in node.else_statement
            if _is_method_invocation(
                child,
                qualifier=result_name,
                member="put",
                argument_count=2,
            )
            and _is_qualified_member_reference(
                child.arguments[0],
                qualifier="Constants",
                member="DATA_LIST",
            )
        ]
        if (
            len(update_calls) == 1
            and len(null_success_calls) == 1
            and not payload_writes
        ):
            matching_branches.append(node)
    if len(matching_branches) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "the upstream-relation branch no longer pairs updateDag payload "
                "success with status-only null success"
            ),
        )


def _validate_resource_page_records(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove paging emits DS entity rows, not Spring download resources."""
    if import_map.get("Resource") != "org.springframework.core.io.Resource":
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "the controller no longer has the reviewed Spring Resource collision"
            ),
        )
    service_import = "org.apache.dolphinscheduler.api.service.impl.ResourcesServiceImpl"
    loaded = load_type_declaration(repo_root, service_import, {})
    if loaded is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed resources service implementation cannot be loaded",
        )
    _, service, service_imports, service_package = loaded
    if (
        resolve_referenced_import_path(
            repo_root,
            "Resource",
            SourceResolutionScope(service_imports, service_package, service_import),
        )
        != DAO_RESOURCE_IMPORT
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the resources service no longer imports the DS Resource entity",
        )
    method = _require_exact_class_method(
        service,
        method_name="queryResourceListPaging",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="resources service implementation",
    )
    local_types: dict[str, str] = {}
    for _, node in method:
        if not isinstance(node, javalang.tree.LocalVariableDeclaration):
            continue
        declared_type = _render_type(node.type)
        for declarator in node.declarators:
            local_types[declarator.name] = declared_type
    pages = [name for name, kind in local_types.items() if kind == "IPage<Resource>"]
    targets = [
        name for name, kind in local_types.items() if kind == "PageInfo<Resource>"
    ]
    results = [
        name
        for name, kind in local_types.items()
        if kind in {"Result", "Result<Object>"}
    ]
    if len(pages) != 1 or len(targets) != 1 or len(results) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "expected one IPage<Resource>, PageInfo<Resource>, and Result "
                "local in the reviewed paging service"
            ),
        )
    page_name, target_name, result_name = pages[0], targets[0], results[0]
    set_rows = [
        node
        for _, node in method
        if _is_method_invocation(
            node,
            qualifier=target_name,
            member="setTotalList",
            argument_count=1,
        )
        and _is_method_invocation(
            node.arguments[0],
            qualifier=page_name,
            member="getRecords",
            argument_count=0,
        )
    ]
    set_payload = [
        node
        for _, node in method
        if _is_method_invocation(
            node,
            qualifier=result_name,
            member="setData",
            argument_count=1,
        )
        and _is_variable_reference(node.arguments[0], target_name)
    ]
    if len(set_rows) != 1 or not set_payload:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "PageInfo<Resource> is no longer populated from the DS entity "
                "page and assigned to the result payload"
            ),
        )


def _validate_resource_detail_record(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove full-name lookup returns a DS Resource entity payload."""
    if import_map.get("Resource") != "org.springframework.core.io.Resource":
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "the controller no longer has the reviewed Spring Resource collision"
            ),
        )
    service_import = "org.apache.dolphinscheduler.api.service.impl.ResourcesServiceImpl"
    loaded = load_type_declaration(repo_root, service_import, {})
    if loaded is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed resources service implementation cannot be loaded",
        )
    _, service, service_imports, service_package = loaded
    if (
        resolve_referenced_import_path(
            repo_root,
            "Resource",
            SourceResolutionScope(service_imports, service_package, service_import),
        )
        != DAO_RESOURCE_IMPORT
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the resources service no longer imports the DS Resource entity",
        )
    method = _require_exact_class_method(
        service,
        method_name="queryResource",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="resources service implementation",
    )
    local_types: dict[str, str] = {}
    for _, node in method:
        if not isinstance(node, javalang.tree.LocalVariableDeclaration):
            continue
        declared_type = _render_type(node.type)
        for declarator in node.declarators:
            local_types[declarator.name] = declared_type
    result_names = {
        name for name, kind in local_types.items() if kind == "Result<Object>"
    }
    resource_names = {name for name, kind in local_types.items() if kind == "Resource"}
    resource_lists = {
        name for name, kind in local_types.items() if kind == "List<Resource>"
    }
    payload_writes = [
        node
        for _, node in method
        if isinstance(node, javalang.tree.MethodInvocation)
        and node.qualifier in result_names
        and node.member == "setData"
        and len(node.arguments) == 1
        and (
            (
                isinstance(node.arguments[0], javalang.tree.MemberReference)
                and node.arguments[0].member in resource_names
            )
            or (
                isinstance(node.arguments[0], javalang.tree.MethodInvocation)
                and node.arguments[0].qualifier in resource_lists
                and node.arguments[0].member == "get"
            )
        )
    ]
    if len(result_names) != 1 or not resource_lists or not payload_writes:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "queryResource no longer assigns a DS Resource selected from "
                "List<Resource> to its result payload"
            ),
        )


def _validate_legacy_schedule_create_missing_data_list(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove that 1.3 create succeeds with HTTP data=null."""
    expected_service_import = "org.apache.dolphinscheduler.api.service.SchedulerService"
    if import_map.get("SchedulerService") != expected_service_import:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the controller no longer imports the reviewed schedule service",
        )

    controller_import = "org.apache.dolphinscheduler.api.controller.SchedulerController"
    loaded_controller = load_type_declaration(repo_root, controller_import, {})
    if loaded_controller is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed schedule controller cannot be loaded",
        )
    _, controller, _, _ = loaded_controller
    controller_method = _require_exact_class_method(
        controller,
        method_name="createSchedule",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="controller",
    )
    result_variables = [
        declarator.name
        for _, node in controller_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer,
            qualifier="schedulerService",
            member="insertSchedule",
        )
    ]
    if len(result_variables) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="createSchedule no longer captures one insertSchedule result map",
        )
    result_variable = result_variables[0]
    controller_returns = [
        node
        for _, node in controller_method
        if isinstance(node, javalang.tree.ReturnStatement)
        and _is_method_invocation(
            node.expression,
            qualifier="",
            member="returnDataList",
            argument_count=1,
        )
        and _is_variable_reference(node.expression.arguments[0], result_variable)
    ]
    if len(controller_returns) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the insertSchedule map is no longer passed to returnDataList",
        )

    loaded_service = load_type_declaration(repo_root, expected_service_import, {})
    if loaded_service is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed schedule service cannot be loaded",
        )
    _, service, _, _ = loaded_service
    service_method = _require_exact_class_method(
        service,
        method_name="insertSchedule",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="service",
    )
    service_results = [
        declarator.name
        for _, node in service_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
    ]
    if len(service_results) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="insertSchedule no longer owns one result map",
        )
    service_result = service_results[0]
    data_list_writes = [
        node
        for _, node in service_method
        if _is_method_invocation(
            node,
            qualifier=service_result,
            member="put",
            argument_count=2,
        )
        and _is_qualified_member_reference(
            node.arguments[0],
            qualifier="Constants",
            member="DATA_LIST",
        )
    ]
    schedule_id_writes = [
        node
        for _, node in service_method
        if _is_method_invocation(
            node,
            qualifier=service_result,
            member="put",
            argument_count=2,
        )
        and isinstance(node.arguments[0], javalang.tree.Literal)
        and node.arguments[0].value == '"scheduleId"'
    ]
    if data_list_writes or len(schedule_id_writes) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "insertSchedule no longer writes only the reviewed literal "
                "scheduleId instead of Constants.DATA_LIST"
            ),
        )

    base_controller_import = "org.apache.dolphinscheduler.api.controller.BaseController"
    loaded_base = load_type_declaration(repo_root, base_controller_import, {})
    if loaded_base is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed base controller cannot be loaded",
        )
    _, base_controller, _, _ = loaded_base
    return_data_list = _require_exact_class_method(
        base_controller,
        method_name="returnDataList",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="base-controller",
    )
    data_list_reads = [
        declarator.name
        for _, node in return_data_list
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer,
            qualifier="result",
            member="get",
            argument_count=1,
        )
        and _is_qualified_member_reference(
            declarator.initializer.arguments[0],
            qualifier="Constants",
            member="DATA_LIST",
        )
    ]
    if len(data_list_reads) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="returnDataList no longer reads exactly Constants.DATA_LIST",
        )


def _validate_schedule_create_data_list(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove that main-route schedule create serializes one Schedule entity."""
    expected_service_import = "org.apache.dolphinscheduler.api.service.SchedulerService"
    if import_map.get("SchedulerService") != expected_service_import:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the controller no longer imports the reviewed schedule service",
        )

    controller_import = "org.apache.dolphinscheduler.api.controller.SchedulerController"
    loaded_controller = load_type_declaration(repo_root, controller_import, {})
    if loaded_controller is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed schedule controller cannot be loaded",
        )
    _, controller, _, _ = loaded_controller
    controller_method = _require_exact_class_method(
        controller,
        method_name="createSchedule",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="controller",
    )
    result_variables = [
        declarator.name
        for _, node in controller_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
        if _is_method_invocation(
            declarator.initializer,
            qualifier="schedulerService",
            member="insertSchedule",
        )
    ]
    if len(result_variables) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="createSchedule no longer captures one insertSchedule result map",
        )
    controller_result = result_variables[0]
    controller_returns = [
        node
        for _, node in controller_method
        if isinstance(node, javalang.tree.ReturnStatement)
        and _is_method_invocation(
            node.expression,
            qualifier="",
            member="returnDataList",
            argument_count=1,
        )
        and _is_variable_reference(node.expression.arguments[0], controller_result)
    ]
    if len(controller_returns) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the insertSchedule map is no longer passed to returnDataList",
        )

    service_import = "org.apache.dolphinscheduler.api.service.impl.SchedulerServiceImpl"
    loaded_service = load_type_declaration(repo_root, service_import, {})
    if loaded_service is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed schedule service implementation cannot be loaded",
        )
    _, service, _, _ = loaded_service
    service_method = _require_exact_class_method(
        service,
        method_name="insertSchedule",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="service implementation",
    )
    schedule_variables = {
        declarator.name
        for _, node in service_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Schedule"
        for declarator in node.declarators
    }
    service_result_variables = {
        declarator.name
        for _, node in service_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
    }
    data_list_writes = [
        node
        for _, node in service_method
        if isinstance(node, javalang.tree.MethodInvocation)
        and node.qualifier in service_result_variables
        and node.member == "put"
        and len(node.arguments) == 2
        and _is_qualified_member_reference(
            node.arguments[0],
            qualifier="Constants",
            member="DATA_LIST",
        )
    ]
    if len(data_list_writes) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="insertSchedule no longer writes exactly one DATA_LIST payload",
        )
    payload = data_list_writes[0].arguments[1]
    if not _is_method_invocation(
        payload,
        qualifier="scheduleMapper",
        member="selectById",
        argument_count=1,
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="schedule create DATA_LIST is no longer a mapper entity readback",
        )
    schedule_id = payload.arguments[0]
    if not isinstance(schedule_id, javalang.tree.MethodInvocation) or (
        schedule_id.qualifier not in schedule_variables
        or schedule_id.member != "getId"
        or schedule_id.arguments
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "schedule create readback is no longer keyed by the created Schedule"
            ),
        )


def _validate_legacy_current_user_data_list(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    expected_service_import = "org.apache.dolphinscheduler.api.service.UsersService"
    if import_map.get("UsersService") != expected_service_import:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the controller no longer imports the reviewed user service",
        )

    controller_import = "org.apache.dolphinscheduler.api.controller.UsersController"
    loaded_controller = load_type_declaration(repo_root, controller_import, {})
    if loaded_controller is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed user controller cannot be loaded",
        )
    _, controller, _, _ = loaded_controller
    controller_method = _require_exact_class_method(
        controller,
        method_name="getUserInfo",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="controller",
    )
    if _render_type(controller_method.return_type) != "Result" or tuple(
        _render_type(parameter.type) for parameter in controller_method.parameters
    ) != ("User",):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed controller method signature changed",
        )

    controller_results: list[str] = []
    for _, node in controller_method:
        if not isinstance(node, javalang.tree.LocalVariableDeclaration):
            continue
        if _render_type(node.type) != "Map<String, Object>":
            continue
        for declarator in node.declarators:
            initializer = declarator.initializer
            if not _is_method_invocation(
                initializer,
                qualifier="usersService",
                member="getUserInfo",
                argument_count=1,
            ):
                continue
            if not _is_variable_reference(initializer.arguments[0], "loginUser"):
                continue
            controller_results.append(declarator.name)
    if len(controller_results) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "expected one Map result from UsersService.getUserInfo, found "
                f"{len(controller_results)}"
            ),
        )
    result_variable = controller_results[0]
    controller_returns = [
        node
        for _, node in controller_method
        if isinstance(node, javalang.tree.ReturnStatement)
        and _is_method_invocation(
            node.expression,
            qualifier="",
            member="returnDataList",
            argument_count=1,
        )
        and _is_variable_reference(node.expression.arguments[0], result_variable)
    ]
    if len(controller_returns) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "the reviewed service result is no longer unwrapped by returnDataList"
            ),
        )

    loaded_service = load_type_declaration(repo_root, expected_service_import, {})
    if loaded_service is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed user service cannot be loaded",
        )
    _, service, _, _ = loaded_service
    service_method = _require_exact_class_method(
        service,
        method_name="getUserInfo",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="service",
    )
    if _render_type(service_method.return_type) != "Map<String, Object>" or tuple(
        _render_type(parameter.type) for parameter in service_method.parameters
    ) != ("User",):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed service method signature changed",
        )

    user_variables = [
        declarator.name
        for _, node in service_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "User"
        for declarator in node.declarators
    ]
    if len(user_variables) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=f"expected one local User payload, found {len(user_variables)}",
        )
    user_variable = user_variables[0]
    service_results = [
        declarator.name
        for _, node in service_method
        if isinstance(node, javalang.tree.LocalVariableDeclaration)
        and _render_type(node.type) == "Map<String, Object>"
        for declarator in node.declarators
    ]
    if len(service_results) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                f"expected one local service result map, found {len(service_results)}"
            ),
        )
    service_result = service_results[0]
    data_list_writes = [
        node
        for _, node in service_method
        if _is_method_invocation(
            node,
            qualifier=service_result,
            member="put",
            argument_count=2,
        )
        and _is_qualified_member_reference(
            node.arguments[0],
            qualifier="Constants",
            member="DATA_LIST",
        )
        and _is_variable_reference(node.arguments[1], user_variable)
    ]
    if len(data_list_writes) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="DATA_LIST is no longer populated from the reviewed User payload",
        )
    service_returns = [
        node
        for _, node in service_method
        if isinstance(node, javalang.tree.ReturnStatement)
        and _is_variable_reference(node.expression, service_result)
    ]
    if len(service_returns) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed DATA_LIST map is no longer returned by the service",
        )


def _require_exact_class_method(
    declaration: javalang.tree.TypeDeclaration,
    *,
    method_name: str,
    operation_id: str,
    source_version: str,
    owner_label: str,
) -> javalang.tree.MethodDeclaration:
    if not isinstance(declaration, javalang.tree.ClassDeclaration):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=f"the reviewed {owner_label} is no longer a class",
        )
    methods = [method for method in declaration.methods if method.name == method_name]
    if len(methods) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(f"expected exactly one {owner_label} method, found {len(methods)}"),
        )
    return methods[0]


def _is_variable_reference(node: object, variable_name: str) -> bool:
    return (
        isinstance(node, javalang.tree.MemberReference)
        and not node.qualifier
        and node.member == variable_name
    )


def _is_qualified_member_reference(
    node: object,
    *,
    qualifier: str,
    member: str,
) -> bool:
    return (
        isinstance(node, javalang.tree.MemberReference)
        and node.qualifier == qualifier
        and node.member == member
    )


def _validate_legacy_process_definition_page_records(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    expected_import_path = (
        "org.apache.dolphinscheduler.api.service.ProcessDefinitionService"
    )
    if import_map.get("ProcessDefinitionService") != expected_import_path:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the controller no longer imports the reviewed service type",
        )

    loaded_service = load_type_declaration(repo_root, expected_import_path, {})
    if loaded_service is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed service type cannot be loaded",
        )
    _, service, _, _ = loaded_service
    if not isinstance(service, javalang.tree.ClassDeclaration):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed service type is no longer a class",
        )

    methods = [
        method
        for method in service.methods
        if method.name == "queryProcessDefinitionListPaging"
    ]
    if len(methods) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=f"expected exactly one service method, found {len(methods)}",
        )
    method = methods[0]
    expected_parameter_types = (
        "User",
        "String",
        "String",
        "Integer",
        "Integer",
        "Integer",
    )
    actual_parameter_types = tuple(
        _render_type(parameter.type) for parameter in method.parameters
    )
    if (
        _render_type(method.return_type) != "Map<String, Object>"
        or actual_parameter_types != expected_parameter_types
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed service method signature changed",
        )

    record_variables: list[str] = []
    page_variables: list[str] = []
    for _, node in method:
        if not isinstance(node, javalang.tree.LocalVariableDeclaration):
            continue
        declared_type = _render_type(node.type)
        for declarator in node.declarators:
            initializer = declarator.initializer
            if declared_type == "IPage<ProcessDefinition>" and _is_method_invocation(
                initializer,
                qualifier="processDefineMapper",
                member="queryDefineListPaging",
            ):
                record_variables.append(declarator.name)
            if (
                declared_type == "PageInfo"
                and isinstance(initializer, javalang.tree.ClassCreator)
                and _render_type(initializer.type) == "PageInfo<ProcessData>"
            ):
                page_variables.append(declarator.name)

    if len(record_variables) != 1 or len(page_variables) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "expected one IPage<ProcessDefinition> record source and one "
                "raw PageInfo created as PageInfo<ProcessData>"
            ),
        )

    record_variable = record_variables[0]
    page_variable = page_variables[0]
    set_lists_calls = [
        node
        for _, node in method
        if _is_method_invocation(
            node,
            qualifier=page_variable,
            member="setLists",
        )
    ]
    if len(set_lists_calls) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "expected exactly one PageInfo.setLists call, found "
                f"{len(set_lists_calls)}"
            ),
        )
    set_lists_call = set_lists_calls[0]
    if len(set_lists_call.arguments) != 1 or not _is_method_invocation(
        set_lists_call.arguments[0],
        qualifier=record_variable,
        member="getRecords",
        argument_count=0,
    ):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "PageInfo.setLists is no longer populated from the reviewed "
                "IPage<ProcessDefinition>.getRecords call"
            ),
        )


def _validate_legacy_datasource_page_records(
    *,
    operation_id: str,
    source_version: str,
    repo_root: Path,
    import_map: dict[str, str],
) -> None:
    """Prove that the legacy page serializes DataSource, not Resource, rows."""
    expected_import_path = "org.apache.dolphinscheduler.api.service.DataSourceService"
    if import_map.get("DataSourceService") != expected_import_path:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the controller no longer imports the reviewed datasource service",
        )
    loaded_service = load_type_declaration(repo_root, expected_import_path, {})
    if loaded_service is None:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed datasource service cannot be loaded",
        )
    _, service, _, _ = loaded_service
    method = _require_exact_class_method(
        service,
        method_name="queryDataSourceListPaging",
        operation_id=operation_id,
        source_version=source_version,
        owner_label="datasource service",
    )
    if _render_type(method.return_type) != "Map<String, Object>" or tuple(
        _render_type(parameter.type) for parameter in method.parameters
    ) != ("User", "String", "Integer", "Integer"):
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail="the reviewed datasource paging service signature changed",
        )

    page_sources: list[str] = []
    record_lists: list[str] = []
    page_targets: list[str] = []
    for _, node in method:
        if not isinstance(node, javalang.tree.LocalVariableDeclaration):
            continue
        declared_type = _render_type(node.type)
        for declarator in node.declarators:
            initializer = declarator.initializer
            if declared_type == "IPage<DataSource>":
                page_sources.append(declarator.name)
            if (
                declared_type == "List<DataSource>"
                and isinstance(initializer, javalang.tree.MethodInvocation)
                and initializer.member == "getRecords"
                and initializer.qualifier in page_sources
            ):
                record_lists.append(declarator.name)
            if (
                declared_type == "PageInfo"
                and isinstance(initializer, javalang.tree.ClassCreator)
                and _render_type(initializer.type) == "PageInfo<Resource>"
            ):
                page_targets.append(declarator.name)
    if len(page_sources) != 1 or len(record_lists) != 1 or len(page_targets) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "expected one IPage<DataSource>, one List<DataSource> record "
                "list, and one raw PageInfo<Resource> initializer"
            ),
        )
    set_lists_calls = [
        node
        for _, node in method
        if _is_method_invocation(
            node,
            qualifier=page_targets[0],
            member="setLists",
            argument_count=1,
        )
        and _is_variable_reference(node.arguments[0], record_lists[0])
    ]
    if len(set_lists_calls) != 1:
        _raise_source_evidence_error(
            operation_id=operation_id,
            source_version=source_version,
            detail=(
                "PageInfo<Resource>.setLists is no longer populated by the "
                "reviewed List<DataSource>"
            ),
        )


def _is_method_invocation(
    node: object,
    *,
    qualifier: str,
    member: str,
    argument_count: int | None = None,
) -> bool:
    if not isinstance(node, javalang.tree.MethodInvocation):
        return False
    if node.qualifier != qualifier or node.member != member:
        return False
    return argument_count is None or len(node.arguments) == argument_count


def _raise_source_evidence_error(
    *,
    operation_id: str,
    source_version: str,
    detail: str,
) -> NoReturn:
    message = (
        f"{operation_id} effective response override source evidence no longer "
        f"matches DolphinScheduler {source_version}: {detail}"
    )
    raise RuntimeError(message)


def resolve_operation_response_projection(
    *,
    raw_return_type: str,
    logical_return_type: str,
) -> ResponseProjection:
    """Return the generated-client projection needed after transport unwrap."""
    if (
        raw_return_type
        in {
            "Map<String, Object>",
            "Map<String, Map<String, Object>>",
        }
        and logical_return_type != raw_return_type
    ):
        return "status_data"
    return "direct"


def _is_weak_logical_type_candidate(java_type: str) -> bool:
    return _is_weak_inferred_type(java_type) or java_type.endswith("_PageInfo_created")


def _resolve_result_wrapper_payload_type(
    *,
    repo_root: Path,
    return_type: str,
    import_map: dict[str, str],
    package_name: str | None,
    owner_import_path: str | None,
    active_return_types: tuple[str, ...],
) -> str | None:
    base_return_type = _base_java_type(return_type)
    if base_return_type in active_return_types:
        return None
    return_import_path = resolve_referenced_import_path(
        repo_root,
        base_return_type,
        SourceResolutionScope(import_map, package_name, owner_import_path),
    )
    if return_import_path is None:
        return None
    loaded_declaration = load_type_declaration(repo_root, return_import_path, {})
    if loaded_declaration is None:
        return None
    _, type_declaration, type_import_map, type_package_name = loaded_declaration
    if not isinstance(type_declaration, javalang.tree.ClassDeclaration):
        return None

    direct_data_type = _concrete_data_field_type(type_declaration)
    if direct_data_type is not None:
        return direct_data_type

    extends = type_declaration.extends
    if extends is None:
        return None
    extends_name = _render_reference_name(extends)
    direct_payload_type = unwrap_result_like_type(extends_name)
    if direct_payload_type is not None:
        return direct_payload_type
    return _resolve_result_wrapper_payload_type(
        repo_root=repo_root,
        return_type=extends_name,
        import_map=type_import_map,
        package_name=type_package_name,
        owner_import_path=return_import_path,
        active_return_types=(*active_return_types, base_return_type),
    )


def _concrete_data_field_type(
    type_declaration: javalang.tree.ClassDeclaration,
) -> str | None:
    for field in type_declaration.fields:
        declarator_names = [declarator.name for declarator in field.declarators]
        if "data" not in declarator_names:
            continue
        java_type = _render_type(field.type)
        if java_type not in {"Object", "T"}:
            return java_type
    return None


def _base_java_type(java_type: str) -> str:
    base_type = java_type.split("<", 1)[0]
    if "." in base_type and not is_fully_qualified_reference_name(base_type):
        return base_type.rsplit(".", 1)[-1]
    return base_type
