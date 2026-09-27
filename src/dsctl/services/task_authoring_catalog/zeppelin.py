from __future__ import annotations

from collections.abc import Mapping
from functools import partial
from typing import TYPE_CHECKING

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.task_authoring_surface import (
    ZeppelinAuthoringSurface,
    get_task_authoring_surface,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue

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

ZEPPELIN_PARAGRAPH_FACET = "ZEPPELIN/paragraph"


def _zeppelin_runtime_guidance(surface: ZeppelinAuthoringSurface) -> str:
    if surface.connection_mode == "WORKER_CONFIG":
        connection = "Configure worker zeppelin.rest.url before execution."
    elif surface.connection_mode == "REST_ENDPOINT":
        connection = (
            "The typed subset connects anonymously to the authored REST endpoint; "
            "inline username/password remains opaque-only."
        )
    else:
        connection = (
            "Choose a ZEPPELIN datasource id. Pure authoring does not inspect its "
            "credentials; use an anonymous datasource."
        )
    credential_logging = {
        "none": "",
        "inline-task-params": (
            " Exact DolphinScheduler 3.2.0 logs raw task parameters, including "
            "any native inline username/password."
        ),
        "resolved-username": (
            " Exact DolphinScheduler 3.2.1 logs the resolved datasource username "
            "after successful login."
        ),
        "resolved-credentials": (
            " Exact DolphinScheduler 3.2.2 and newer log resolved datasource "
            "credentials after injection."
        ),
    }[surface.credential_logging]
    output = (
        " DolphinScheduler 3.4.1 creates task-local result evidence without reliable "
        "downstream transport."
        if surface.result_output == "task-params-only"
        else (
            " DolphinScheduler 3.4.2 can publish taskName.result at runtime, but that "
            "output remains runtime-only."
            if surface.result_output == "var-pool"
            else ""
        )
    )
    return (
        f"{connection} Upstream performs one synchronous paragraph call, exposes no "
        "remote application id or failover resume, and a retry may execute the "
        f"paragraph again.{credential_logging}{output}"
    )


def _zeppelin_fields(
    surface: ZeppelinAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    if surface.connection_mode is None:
        return ()
    fields: list[TaskAuthoringField] = [
        model_field(
            "task_params.noteId",
            compile_path="taskDefinitionJson[].taskParams.noteId",
            description=(
                "Literal Zeppelin note id as one URL-safe path segment. "
                f"{_zeppelin_runtime_guidance(surface)}"
            ),
        ),
        model_field(
            "task_params.paragraphId",
            compile_path="taskDefinitionJson[].taskParams.paragraphId",
            description=(
                "Literal paragraph id; whole note and production clone execution "
                "remain opaque-only."
            ),
        ),
        model_field(
            "task_params.connectionMode",
            choices=(surface.connection_mode,),
            description=(
                "Canonical connection intent selected by the exact profile; this "
                "discriminator is projected away rather than sent to DolphinScheduler."
            ),
        ),
    ]
    if surface.connection_mode == "REST_ENDPOINT":
        fields.append(
            model_field(
                "task_params.restEndpoint",
                required=True,
                active_when="task_params.connectionMode == REST_ENDPOINT",
                compile_path="taskDefinitionJson[].taskParams.restEndpoint",
                description=(
                    "Literal anonymous absolute HTTP(S) endpoint without credentials, "
                    "query, fragment, or DS placeholder syntax."
                ),
            )
        )
    elif surface.connection_mode == "DATASOURCE":
        fields.append(
            model_field(
                "task_params.datasource",
                "integer|string",
                required=True,
                choices=(),
                active_when="task_params.connectionMode == DATASOURCE",
                choice_source="dsctl datasource list",
                related_commands=(
                    "dsctl datasource list",
                    "dsctl datasource get DATASOURCE",
                ),
                compile_path="taskDefinitionJson[].taskParams.datasource",
                description=(
                    "Positive ZEPPELIN datasource id or exact name. Use an "
                    "anonymous datasource; credential-bearing datasource execution "
                    "remains an explicit opaque/risk-managed path."
                ),
            )
        )
    fields.append(
        model_field(
            "task_params.parameters",
            model_default=True,
            compile_path=(
                "taskDefinitionJson[].taskParams.parameters"
                if surface.literal_parameters
                else None
            ),
            description=(
                "Literal string-to-string paragraph parameters serialized to the "
                "native JSON string from DolphinScheduler 3.1.0; this map must stay "
                "empty on 3.0.x. DS placeholders and control text are rejected. "
                "This is not secret storage: dsctl does not detect or redact "
                "secret-like values, and upstream may log task parameters."
            ),
        )
    )
    return tuple(fields)


def _zeppelin_template_body(
    surface: ZeppelinAuthoringSurface,
    body: str,
) -> str:
    comments = (
        f"# Runtime prerequisite: {_zeppelin_runtime_guidance(surface)}\n"
        "# Typed scope: one literal paragraph call. This map is not secret storage; "
        "dsctl does not detect or redact arbitrary values, and upstream may log "
        "them. Whole note, clone, credential, and placeholder modes remain "
        "opaque-only.\n"
    )
    return f"{comments}{task_template_with_runtime_controls(body)}"


def _zeppelin_templates(
    surface: ZeppelinAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    mode_lines = {
        "WORKER_CONFIG": "  connectionMode: WORKER_CONFIG\n",
        "REST_ENDPOINT": (
            "  connectionMode: REST_ENDPOINT\n"
            "  restEndpoint: https://zeppelin.example.com\n"
        ),
        "DATASOURCE": "  connectionMode: DATASOURCE\n  datasource: 17\n",
    }
    mode = surface.connection_mode
    if mode is None:
        return ()
    minimal_body = (
        "# Task template for one Zeppelin paragraph\n"
        "name: zeppelin-paragraph\n"
        "type: ZEPPELIN\n"
        "description: Execute one Zeppelin paragraph\n"
        "task_params:\n"
        "  noteId: 2FZ4VC2MX\n"
        "  paragraphId: paragraph-1\n"
        f"{mode_lines[mode]}"
        "  parameters: {}\n"
        "worker_group: default\n"
        "priority: MEDIUM\n"
        "retry:\n"
        "  times: 0\n"
        "  interval: 0\n"
        "timeout: 0\n"
    )
    templates = [
        TaskAuthoringTemplate(
            name="minimal",
            summary="Execute one literal Zeppelin paragraph.",
            payload_modes=("task_params",),
            yaml=_zeppelin_template_body(surface, minimal_body),
        )
    ]
    if surface.literal_parameters:
        templates.append(
            TaskAuthoringTemplate(
                name="params",
                purpose="option",
                summary="Execute one paragraph with a literal string parameter map.",
                payload_modes=("task_params",),
                yaml="""task_params:
  parameters:
    business_date: '2026-08-20'
""",
            )
        )
    return tuple(templates)


def _is_reviewed_zeppelin_opaque_mode(
    surface: ZeppelinAuthoringSurface,
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize explicit native Zeppelin modes outside paragraph typed intent."""
    if "connectionMode" in task_params:
        return False
    if {
        "localParams",
        "varPool",
        "resourceList",
        "appIds",
    }.intersection(task_params):
        return True
    if surface.connection_mode == "WORKER_CONFIG":
        return False
    if "productionNoteDirectory" in task_params:
        return True
    paragraph_id = task_params.get("paragraphId")
    if "noteId" in task_params and (
        not isinstance(paragraph_id, str) or not paragraph_id.strip()
    ):
        return True
    if isinstance(task_params.get("parameters"), str):
        return True
    if surface.credential_source == "task-inline" and {
        "username",
        "password",
    }.intersection(task_params):
        return True
    return (
        surface.connection_mode == "DATASOURCE"
        and task_params.get("type") == "ZEPPELIN"
    )


def _zeppelin_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).zeppelin
    if not surface.available:
        message = f"ZEPPELIN is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=ZEPPELIN_PARAGRAPH_FACET,
        family="zeppelin-paragraph-v1",
        review="zeppelin-paragraph-literal-params-exact-projection",
        params_model=_family_model("ZEPPELIN"),
        fields=_zeppelin_fields(surface),
        state_rules=(),
        templates=_zeppelin_templates(surface),
        opaque_authoring_selector=partial(
            _is_reviewed_zeppelin_opaque_mode,
            surface,
        ),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="ZEPPELIN",
        category="Other",
        kind="typed",
        default_facet=ZEPPELIN_PARAGRAPH_FACET,
        facets={ZEPPELIN_PARAGRAPH_FACET: membership},
    )


def validate_semantics(
    task_params: YamlObject,
    *,
    version: str,
    surface: ZeppelinAuthoringSurface,
) -> None:
    """Bind one canonical paragraph intent to the selected connection epoch."""
    mode = task_params.get("connectionMode")
    if mode != surface.connection_mode:
        message = (
            f"ZEPPELIN connectionMode {mode!r} is unsupported for "
            f"DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "ZEPPELIN",
                "field": "tasks[].task_params.connectionMode",
                "value": mode,
                "supported": surface.connection_mode,
                "reason": "upstream_capability_absent",
            },
            suggestion=(
                "Use the connectionMode shown by `dsctl task-type schema "
                "ZEPPELIN` for the selected version."
            ),
        )
    parameters = task_params.get("parameters")
    if (
        not surface.literal_parameters
        and isinstance(parameters, Mapping)
        and parameters
    ):
        message = (
            "ZEPPELIN paragraph parameters are unavailable for "
            f"DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "ZEPPELIN",
                "field": "tasks[].task_params.parameters",
                "reason": "upstream_capability_absent",
            },
            suggestion=("Remove parameters or select DolphinScheduler 3.1.0 or newer."),
        )
