from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        OpenmldbAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

OPENMLDB_LITERAL_SINGLE_STATEMENT_FACET = "OPENMLDB/literal_single_statement"


def _openmldb_runtime_guidance(surface: OpenmldbAuthoringSurface) -> str:
    """Describe the exact Python bridge and its disclosure/recovery hazards."""
    if surface.exclusion_reason is not None:
        return f"OPENMLDB typed authoring is disabled: {surface.exclusion_reason}."
    if not surface.available:
        return "OPENMLDB is absent from this exact DolphinScheduler profile."
    if surface.python_launcher is None:
        message = "Available OPENMLDB surface lacks one exact Python launcher"
        raise ValueError(message)
    logging_facts = (
        surface.task_params_logged,
        surface.sql_logged,
        surface.rendered_script_logged,
        surface.generated_script_logged,
    )
    logging = (
        "Complete task params, raw SQL, the rendered Python, and the final "
        "generated Python file are logged at INFO; dsctl does not redact them. "
        if all(logging_facts)
        else ""
    )
    normalization = (
        "Typed validation rejects CR and preserves canonical and wire spelling "
        "for every accepted value; upstream normalizes CRLF before generating "
        "Python. "
        if surface.line_endings_normalized
        else ""
    )
    output = (
        "Statement results can be published as DolphinScheduler output values. "
        if surface.result_output_supported
        else (
            "The statement result returned by each SQLAlchemy execute call is "
            "discarded and is not a DolphinScheduler output parameter. "
        )
    )
    failover = (
        "The OpenMLDB operation supports durable worker-failover resume. "
        if surface.failover_supported
        else (
            "The synchronous local Python3 process exposes no durable application "
            "id and has no worker-failover resume. "
        )
    )
    retry = (
        "A retry can reexecute the whole SQL statement and may repeat side effects. "
        if surface.retry_reexecutes
        else ""
    )
    return (
        "Route to a worker with Python3, the OpenMLDB SDK, SQLAlchemy and its "
        "OpenMLDB driver, ZooKeeper reachability, and target-data permissions. "
        f"The executor reads {surface.python_launcher}; when unset it invokes "
        "python3, and a configured /bin/python-style path is forced to "
        f"/bin/python3. {logging}{normalization}{output}{failover}{retry}"
        "Offline mode also enables synchronous jobs with a fixed 1800000 ms "
        "job timeout. Keep secrets out of every task field."
    )


def _openmldb_fields(
    surface: OpenmldbAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    runtime = _openmldb_runtime_guidance(surface)
    return (
        model_field(
            "task_params.zk",
            compile_path="taskDefinitionJson[].taskParams.zk",
            description=(
                "Comma-separated DNS/IPv4 ZooKeeper host:port ensemble with "
                "unambiguous decimal ports from 1 through 65535. "
                f"{runtime}"
            ),
        ),
        model_field(
            "task_params.zkPath",
            compile_path="taskDefinitionJson[].taskParams.zkPath",
            description=(
                "Absolute conservative ZooKeeper znode path; root slash is valid. "
                "Empty segments, leading-dot, traversal, delimiter, and placeholder "
                "forms are rejected."
            ),
        ),
        model_field(
            "task_params.executeMode",
            compile_path="taskDefinitionJson[].taskParams.executeMode",
            description=(
                "Exact lowercase OpenMLDB execution mode. Offline mode adds "
                "set @@sync_job=true and set @@job_timeout=1800000; online mode "
                "does not add those two statements."
            ),
        ),
        model_field(
            "task_params.sql",
            compile_path="taskDefinitionJson[].taskParams.sql",
            description=(
                "One nonblank literal single statement inserted into "
                "upstream-generated Python with canonical and wire spelling "
                "preserved. Semicolons, double quotes, backslashes, CR, unsafe "
                "controls, and DS placeholders are rejected; tabs, line feeds, "
                "single quotes, and ordinary Unicode keep their spelling."
            ),
        ),
    )


def _openmldb_template_body(
    surface: OpenmldbAuthoringSurface,
    body: str,
) -> str:
    comments = (
        f"# Runtime prerequisite: {_openmldb_runtime_guidance(surface)}\n"
        "# Typed scope: exactly zk, zkPath, executeMode, and one literal SQL "
        "statement. Inherited, resource, parameter, and future fields remain "
        "unchanged/export preservation only.\n"
        "# Logging warning: task parameters and generated Python are visible at "
        "INFO; do not place secrets in task fields.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _openmldb_templates(
    surface: OpenmldbAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Execute one literal OpenMLDB statement through Python 3.",
            payload_modes=("task_params",),
            yaml=_openmldb_template_body(
                surface,
                """# Task template for one literal OpenMLDB statement
name: openmldb-single-statement
type: OPENMLDB
description: Execute one literal OpenMLDB statement
task_params:
  zk: zk-1.example.com:2181,zk-2.example.com:2181
  zkPath: /openmldb/production
  executeMode: online
  sql: SELECT 1 AS answer
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
""",
            ),
        ),
    )


def _openmldb_authoring_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).openmldb
    if surface.exclusion_reason is not None:
        if (
            profile_version != "3.1.2"
            or surface.available
            or surface.exclusion_reason
            != "python-parent-parameters-uninitialized-after-execution"
        ):
            message = f"OPENMLDB {profile_version} is not the reviewed runtime hole"
            raise ValueError(message)
        facet = "OPENMLDB/runtime_exclusion"
        membership = TaskAuthoringFacetMembership(
            profile_version=profile_version,
            contract=TaskAuthoringFacetContract(
                facet_id=facet,
                family="openmldb-runtime-exclusion-v1",
                review="openmldb-uninitialized-python-parent-runtime-exclusion",
                params_model=None,
                fields=(),
                state_rules=(),
                templates=(),
            ),
            typed_create=False,
            typed_edit=False,
            opaque_create=False,
            opaque_edit=False,
            opaque_preserve=True,
            constraint=(
                f"Exact {profile_version} OPENMLDB runs SQL then dereferences "
                "uninitialized Python parent parameters and reports failure; "
                "retry can repeat effects. Existing state is unchanged/export "
                "preservation only."
            ),
        )
        return TaskTypeAuthoringProfile(
            task_type="OPENMLDB",
            category="MachineLearning",
            kind="generic",
            default_facet=facet,
            facets={facet: membership},
        )
    if not surface.available:
        message = f"OPENMLDB is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=OPENMLDB_LITERAL_SINGLE_STATEMENT_FACET,
        family="openmldb-literal-single-statement-v1",
        review="openmldb-literal-single-statement-python-safe-subset",
        params_model=_family_model("OPENMLDB"),
        fields=_openmldb_fields(surface),
        state_rules=(),
        templates=_openmldb_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="OPENMLDB",
        category="MachineLearning",
        kind="typed",
        default_facet=OPENMLDB_LITERAL_SINGLE_STATEMENT_FACET,
        facets={OPENMLDB_LITERAL_SINGLE_STATEMENT_FACET: membership},
    )
