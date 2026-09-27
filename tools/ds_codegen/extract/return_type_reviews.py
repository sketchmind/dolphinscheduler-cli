"""Exact response/type reviews, separate from reusable source-proof rules."""

from __future__ import annotations

from dataclasses import dataclass

from ds_codegen.extract.return_type_rules import DAO_RESOURCE_IMPORT


@dataclass(frozen=True)
class ReviewedReturnTypeRule:
    """Releases whose exact raw shape and rule membership must not drift."""

    rule_name: str
    raw_return_type: str
    versions: tuple[str, ...]


# This is an extraction review inventory, not a runtime support registry.
# Candidate source matches never add entries to this inventory.
REVIEWED_RETURN_TYPE_RULES = (
    ReviewedReturnTypeRule(
        rule_name="alert_plugin_page_nullable_total_list",
        raw_return_type="Result",
        versions=(
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="instance_update_nullable_without_sync",
        raw_return_type="Result",
        versions=("2.0.0", "2.0.1", "2.0.2"),
    ),
    ReviewedReturnTypeRule(
        rule_name="legacy_datasource_page_records",
        raw_return_type="Result",
        versions=("1.3.9",),
    ),
    ReviewedReturnTypeRule(
        rule_name="legacy_process_definition_page_records",
        raw_return_type="Result",
        versions=("1.3.9",),
    ),
    ReviewedReturnTypeRule(
        rule_name="legacy_schedule_create_missing_data_list",
        raw_return_type="Result",
        versions=("1.3.9",),
    ),
    ReviewedReturnTypeRule(
        rule_name="schedule_preview_string_stream",
        raw_return_type="Result",
        versions=("1.3.9",),
    ),
    ReviewedReturnTypeRule(
        rule_name="schedule_preview_method_reference_stream",
        raw_return_type="Result",
        versions=(
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
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="legacy_current_user_data_list",
        raw_return_type="Result",
        versions=("1.3.9",),
    ),
    ReviewedReturnTypeRule(
        rule_name="resource_detail_record",
        raw_return_type="Result",
        versions=(
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
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="resource_detail_record",
        raw_return_type="Result<Object>",
        versions=("3.0.0", "3.0.6", "3.1.0", "3.1.9"),
    ),
    ReviewedReturnTypeRule(
        rule_name="resource_page_records",
        raw_return_type="Result",
        versions=(
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
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="resource_page_records",
        raw_return_type="Result<Object>",
        versions=(
            "3.0.0",
            "3.0.6",
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.9",
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="schedule_create_data_list",
        raw_return_type="Result",
        versions=(
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
            "3.0.6",
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
            "3.1.9",
            "3.2.0",
            "3.2.1",
            "3.2.2",
            "3.3.1",
            "3.3.2",
            "3.4.0",
            "3.4.1",
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="task_delete_nullable_payload",
        raw_return_type="Result",
        versions=(
            "2.0.4",
            "2.0.5",
            "2.0.6",
            "2.0.7",
            "2.0.8",
            "2.0.9",
            "3.0.0",
            "3.0.6",
            "3.1.0",
            "3.1.1",
            "3.1.2",
            "3.1.3",
            "3.1.4",
            "3.1.5",
            "3.1.6",
            "3.1.7",
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="project_preference_nullable_entity",
        raw_return_type="Result",
        versions=(
            "3.2.0",
            "3.2.1",
            "3.2.2",
            "3.3.1",
            "3.3.2",
            "3.4.0",
            "3.4.1",
            "3.4.2",
            "3.4.3",
        ),
    ),
    ReviewedReturnTypeRule(
        rule_name="task_update_nullable_success",
        raw_return_type="Result",
        versions=("3.4.1",),
    ),
    ReviewedReturnTypeRule(
        rule_name="assigned_worker_group_list",
        raw_return_type="Map<String, Object>",
        versions=("3.4.2", "3.4.3"),
    ),
)


# Additional type identities lack a reusable structural proof; retain exact scope.
REVIEWED_TYPE_IMPORTS = {
    (
        "1.3.9",
        "ResourcesController.queryResource",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "1.3.9",
        "ResourcesController.queryResourceListPaging",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "1.3.9",
        "ResourcesController.queryUdfFuncListPaging",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "3.0.0",
        "ResourcesController.queryResourceById",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "3.0.6",
        "ResourcesController.queryResourceById",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "3.1.0",
        "ResourcesController.queryResourceById",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
    (
        "3.1.9",
        "ResourcesController.queryResourceById",
        "Resource",
    ): DAO_RESOURCE_IMPORT,
}

# Absence of a rule is also part of each reviewed release's source contract.
# Reusable candidate rules must not silently fill these reviewed holes.
REVIEWED_SOURCE_VERSIONS = frozenset(
    version for review in REVIEWED_RETURN_TYPE_RULES for version in review.versions
)
