from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_matrix_fails_closed_and_keeps_route_renames_candidate_only(
    tmp_path: Path,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    matrix = _module("ds_codegen.compatibility_impact")
    analyzer = _module("ds_codegen.stable_action_version_matrix")
    inputs = _contract_inputs(
        tmp_path,
        api=api,
        ir=ir,
        versions=matrix.REVIEWED_DS_VERSIONS,
    )

    report = analyzer.analyze_stable_action_version_matrix(
        dependency_report=_dependency_report(),
        inputs=inputs,
    )

    assert report["complete"] is True
    assert report["automatic_support"] is False
    assert report["semantic_support_claimed"] is False
    assert report["upstream_absence_claimed"] is False
    assert report["claim"] == "mechanical-candidates-only"
    assert len(report["sources"]) == len(inputs)
    assert all(source["tag"] == source["version"] for source in report["sources"])

    operation = report["operation_candidates"][0]
    assert operation["baseline"]["source_operation_id"] == (
        "WidgetController.getWidget"
    )
    candidates = {item["version"]: item for item in operation["versions"]}
    renamed = candidates["2.0.0"]
    assert renamed["selection"] == "unique-route-candidate"
    assert renamed["status"] == "unique-route-candidate"
    assert renamed["selected_source_operation"] == "WidgetController.readWidget"
    assert renamed["candidate_only"] is True
    assert renamed["same_source_id"] is False
    assert renamed["same_route"] is True
    assert renamed["same_effective_request_candidate"] is True
    assert renamed["same_logical_response_candidate"] is True
    assert renamed["review_reasons"] == ["source-operation-rename-candidate"]

    assert candidates["2.0.9"]["status"] == "candidate-missing"
    assert candidates["2.0.9"]["upstream_absence"] == "not-established"
    assert candidates["2.0.9"]["review_reasons"] == ["candidate-missing-needs-review"]
    assert candidates["3.0.0"]["status"] == "ambiguous-route-candidate"
    assert candidates["3.0.0"]["candidate_source_operations"] == [
        "WidgetController.fetchWidget",
        "WidgetV2Controller.fetchWidget",
    ]
    assert candidates["3.0.6"]["status"] == "request-drift"
    assert candidates["3.1.0"]["status"] == "response-drift"
    assert candidates["3.1.9"]["status"] == "response-drift"

    actions = {item["action"]: item for item in report["actions"]}
    assert actions["version"]["classification"] == "local"
    assert {row["status"] for row in actions["version"]["versions"]} == {
        "not-applicable-local"
    }
    diagnostic = actions["doctor"]
    assert diagnostic["classification"] == "diagnostic"
    assert diagnostic["dependency_kinds"] == ["diagnostic", "direct-transport"]
    assert diagnostic["versions"][0]["status"] == "direct-transport-seam"
    assert diagnostic["versions"][0]["mechanical_blocker"] is True
    non_wire = actions["widget.plan"]
    assert non_wire["dependency_kinds"] == ["non-wire-seam"]
    assert non_wire["versions"][0]["status"] == "non-wire-seam"

    get_action = actions["widget.get"]
    baseline = next(
        item for item in get_action["versions"] if item["version"] == "3.4.1"
    )
    assert baseline["status"] == "exact-source-id-candidate"
    assert baseline["requires_manual_review"] is False
    renamed_action = next(
        item for item in get_action["versions"] if item["version"] == "2.0.0"
    )
    assert renamed_action["requires_manual_review"] is True
    assert renamed_action["mechanical_blocker"] is True
    assert renamed_action["review_reasons"] == [
        "semantic-support-not-reviewed",
        "source-operation-rename-candidate",
    ]
    missing_action = next(
        item for item in get_action["versions"] if item["version"] == "2.0.9"
    )
    assert missing_action["status"] == "candidate-missing"
    assert missing_action["review_reasons"] == [
        "alternatives-unresolved",
        "candidate-missing-needs-review",
        "semantic-support-not-reviewed",
    ]
    queued = next(
        item for item in report["review_queue"] if item["action"] == "widget.get"
    )
    assert queued["priority"] == "mechanical-blocker"
    assert {reason["reason"] for reason in queued["reasons"]} >= {
        "multiple-route-candidates",
        "request-drift",
        "response-drift",
        "candidate-missing-needs-review",
        "source-operation-rename-candidate",
    }


@pytest.mark.source_contract
def test_real_matrix_covers_every_stable_action_and_exact_target(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    analyzer = _module("ds_codegen.stable_action_version_matrix")
    source_matrix = _module("ds_codegen.compatibility_impact")
    inputs = [
        exact_contract_corpus.contract_input(version)
        for version in source_matrix.REVIEWED_DS_VERSIONS
    ]

    report = analyzer.analyze_repository_stable_action_version_matrix(
        exact_contract_corpus.repo_root,
        inputs=inputs,
    )

    assert report["complete"] is True
    assert report["summary"]["action_count"] == 181
    assert report["summary"]["action_version_count"] == 181 * len(
        source_matrix.REVIEWED_DS_VERSIONS
    )
    assert len(report["operation_candidates"]) >= 130
    baseline = next(
        item for item in report["version_summary"] if item["version"] == "3.4.1"
    )
    assert baseline["candidate_statuses"] == {
        "direct-transport-seam": 5,
        "exact-source-id-candidate": 147,
        "no-wire-dependency": 2,
        "not-applicable-local": 27,
    }
    schedule_delete = next(
        item for item in report["actions"] if item["action"] == "schedule.delete"
    )
    assert schedule_delete["dependency_relationship"] == "reviewed-composition"
    target = next(
        item for item in schedule_delete["versions"] if item["version"] == "3.4.2"
    )
    assert target["status"] == "exact-source-id-candidate"
    assert target["upstream_absence"] == "not-established"
    assert target["review_reasons"] == ["semantic-support-not-reviewed"]

    workflow_get = next(
        item for item in report["actions"] if item["action"] == "workflow.get"
    )
    assert workflow_get["dependency_relationship"] == "reviewed-composition"
    reviewed_baseline = next(
        item for item in workflow_get["versions"] if item["version"] == "3.4.1"
    )
    assert reviewed_baseline["status"] == "exact-source-id-candidate"
    assert reviewed_baseline["mechanical_blocker"] is False

    environment_create = next(
        item for item in report["actions"] if item["action"] == "environment.create"
    )
    legacy_environment = next(
        item for item in environment_create["versions"] if item["version"] == "1.3.9"
    )
    assert legacy_environment["status"] == "candidate-missing"
    assert {
        operation["status"] for operation in legacy_environment["operation_statuses"]
    } == {"candidate-missing"}
    assert legacy_environment["upstream_absence"] == "not-established"
    assert report["mechanical_gate_passed"] is False
    assert report["review_queue"][0]["priority"] == "mechanical-blocker"


def test_cli_uses_default_exact_snapshot_directory(
    tmp_path: Path,
    capsys: Any,
) -> None:
    api = _module("ds_codegen.api")
    ir = _module("ds_codegen.ir")
    matrix = _module("ds_codegen.compatibility_impact")
    cli = _module("analyze_ds_stable_action_version_matrix")
    snapshot_dir = tmp_path / "snapshots"
    _contract_inputs(
        snapshot_dir,
        api=api,
        ir=ir,
        versions=matrix.REVIEWED_DS_VERSIONS,
    )
    dependency_path = tmp_path / "dependencies.json"
    dependency_path.write_text(
        json.dumps(_dependency_report()),
        encoding="utf-8",
    )
    output = tmp_path / "report.json"

    status = cli.main(
        [
            "--repo-root",
            str(tmp_path),
            "--dependency-report",
            str(dependency_path),
            "--snapshot-dir",
            str(snapshot_dir),
            "--output",
            str(output),
        ]
    )

    assert status == 0
    assert json.loads(output.read_text(encoding="utf-8"))["summary"][
        "action_version_count"
    ] == 4 * len(matrix.REVIEWED_DS_VERSIONS)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def _dependency_report() -> dict[str, object]:
    generated = {
        "client_operation": "widget.get_widget",
        "source_operation": "WidgetController.getWidget",
        "http_method": "GET",
        "path": "widgets/{id}",
    }
    return {
        "schema_version": 1,
        "kind": "dolphinscheduler-stable-action-dependencies",
        "claim": "static-reachability-evidence-only",
        "baseline": {
            "ds_version": "3.4.1",
            "generated_package": "ds_3_4_1",
        },
        "complete": False,
        "actions": [
            {
                "action": "version",
                "classification": "local",
                "generated_operations": [],
                "direct_transport_calls": [],
                "diagnostics": [],
            },
            {
                "action": "doctor",
                "classification": "diagnostic",
                "generated_operations": [],
                "direct_transport_calls": ["DolphinSchedulerClient.healthcheck"],
                "diagnostics": [
                    {
                        "code": "adapter-direct-transport",
                        "message": "healthcheck bypasses generated operations",
                    }
                ],
            },
            {
                "action": "widget.get",
                "classification": "remote",
                "generated_operations": [generated],
                "direct_transport_calls": [],
                "diagnostics": [],
                "dependency_relationship": "alternatives-or-composition-unresolved",
                "wire_dependencies": [
                    {
                        "group": "widgets",
                        "method": "get",
                        "protocol": "WidgetOperations",
                        "operation_relationship": (
                            "alternatives-or-composition-unresolved"
                        ),
                        "generated_operations": [generated],
                        "direct_transport_calls": [],
                    }
                ],
            },
            {
                "action": "widget.plan",
                "classification": "remote",
                "generated_operations": [],
                "direct_transport_calls": [],
                "diagnostics": [
                    {
                        "code": "adapter-non-wire-method",
                        "message": "widget.plan is a pure plan seam",
                    }
                ],
            },
        ],
    }


def _contract_inputs(
    root: Path,
    *,
    api: Any,
    ir: Any,
    versions: tuple[str, ...],
) -> list[Any]:
    root.mkdir(parents=True, exist_ok=True)
    inputs = []
    for version in versions:
        models = [
            _widget_model(
                ir,
                field_type="Long" if version == "3.1.9" else "Integer",
            )
        ]
        if version == "2.0.0":
            operations = [_operation(ir, method_name="readWidget")]
        elif version == "2.0.9":
            operations = []
        elif version == "3.0.0":
            operations = [
                _operation(
                    ir,
                    controller="WidgetController",
                    method_name="fetchWidget",
                ),
                _operation(
                    ir,
                    controller="WidgetV2Controller",
                    method_name="fetchWidget",
                ),
            ]
        elif version == "3.0.6":
            operations = [_operation(ir, parameter_type="Long")]
        elif version == "3.1.0":
            operations = [_operation(ir, logical_return_type="String")]
        else:
            operations = [_operation(ir)]
        snapshot = ir.ContractSnapshot(
            ds_version=version,
            operation_count=len(operations),
            enum_count=0,
            dto_count=0,
            model_count=len(models),
            operations=operations,
            enums=[],
            dtos=[],
            models=models,
        )
        path = root / f"ds-{version}-contract.json"
        api.write_contract_snapshot(
            snapshot,
            path,
            provenance=_exact_provenance(api, snapshot, version),
        )
        inputs.append(api.ContractInput(version, path, "snapshot"))
    return inputs


def _operation(
    api: Any,
    *,
    controller: str = "WidgetController",
    method_name: str = "getWidget",
    parameter_type: str = "Integer",
    logical_return_type: str = "Widget",
) -> Any:
    return api.OperationSpec(
        operation_id=f"{controller}.{method_name}",
        controller=controller,
        method_name=method_name,
        api_group="v1",
        http_method="GET",
        path="widgets/{id}",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="Result",
        inferred_return_type=logical_return_type,
        logical_return_type=logical_return_type,
        response_projection="direct",
        parameters=[
            api.ParameterSpec(
                name="id",
                java_type=parameter_type,
                binding="path_variable",
                wire_name="id",
                required=True,
                default_value=None,
                hidden=False,
                description=None,
                example=None,
                allowable_values=None,
                schema_type=None,
            )
        ],
    )


def _widget_model(ir: Any, *, field_type: str) -> Any:
    return ir.ModelSpec(
        name="Widget",
        import_path="example.Widget",
        kind="other_class",
        documentation=None,
        extends=None,
        fields=[
            ir.DtoFieldSpec(
                name="value",
                java_type=field_type,
                wire_name="value",
                required=True,
                default_value=None,
                nullable=False,
                default_factory=None,
                description=None,
                example=None,
                allowable_values=None,
                documentation=None,
            )
        ],
    )


def _exact_provenance(api: Any, snapshot: Any, version: str) -> dict[str, object]:
    tree = f"{int(version.replace('.', '')):040x}"
    content_digest = f"git-tree:{tree}"
    return {
        "schema_version": 1,
        "exact": True,
        "input_kind": "ds-source",
        "content_digest": content_digest,
        "contract_digest": api.contract_snapshot_digest(snapshot),
        "origin": {
            "kind": "ds-source",
            "content_digest": content_digest,
            "git": {
                "commit": "a" * 40,
                "tree": tree,
                "ref": f"refs/tags/{version}",
                "tag": version,
                "dirty": False,
            },
        },
    }


def test_matrix_uses_the_validated_report_baseline(tmp_path: Path) -> None:
    analyzer = _module("ds_codegen.stable_action_version_matrix")
    source_matrix = _module("ds_codegen.compatibility_impact")
    inputs = _contract_inputs(
        tmp_path,
        api=_module("ds_codegen.api"),
        ir=_module("ds_codegen.ir"),
        versions=source_matrix.REVIEWED_DS_VERSIONS,
    )
    dependencies = _dependency_report()
    dependencies["baseline"] = {"ds_version": "3.4.2", "generated_package": "ds_3_4_2"}
    report = analyzer.analyze_stable_action_version_matrix(
        dependency_report=dependencies, inputs=inputs
    )
    assert report["baseline"]["version"] == "3.4.2"
    widget = next(
        action for action in report["actions"] if action["action"] == "widget.get"
    )
    candidates = {candidate["version"]: candidate for candidate in widget["versions"]}
    assert candidates["3.4.2"]["requires_manual_review"] is False
    assert candidates["3.4.1"]["review_reasons"] == ["semantic-support-not-reviewed"]


@pytest.mark.parametrize(
    "drift", [None, "evidence", "wire", "baseline", "reason", "seam"]
)
def test_unavailable_baseline_is_explicit_and_cannot_imply_other_versions(
    tmp_path: Path, drift: str | None
) -> None:
    analyzer = _module("ds_codegen.stable_action_version_matrix")
    inputs = _contract_inputs(
        tmp_path,
        api=_module("ds_codegen.api"),
        ir=_module("ds_codegen.ir"),
        versions=_module("ds_codegen.compatibility_impact").REVIEWED_DS_VERSIONS,
    )
    absence = {
        "availability": "unsupported",
        "reason": "upstream_capability_absent",
        "constraint": "The selected baseline source has no widget controller.",
        "evidence_sources": ["apache/dolphinscheduler@1.3.9:controller-tree"],
    }
    dependency = {
        "baseline_version": "1.3.9",
        "semantic_operation": "widget.read",
        "source": "operation-contract-ledger-and-reviewed-unavailable-decision",
        "source_operations": [],
        "unavailable": absence,
    }
    action = {
        "action": "widget.get",
        "classification": "remote",
        "generated_operations": [],
        "direct_transport_calls": [],
        "diagnostics": [],
        "complete": True,
        "reviewed_semantic_dependency": dependency,
    }
    if drift == "evidence":
        absence["evidence_sources"] = []
    elif drift == "wire":
        dependency["source_operations"] = ["WidgetController.queryWidget"]
    elif drift == "baseline":
        dependency["baseline_version"] = "3.4.1"
    elif drift == "reason":
        absence["reason"] = "unreviewed"
    elif drift == "seam":
        action["complete"] = False
    dependencies = {
        "kind": "dolphinscheduler-stable-action-dependencies",
        "baseline": {"ds_version": "1.3.9", "generated_package": "ds_1_3_9"},
        "complete": True,
        "actions": [action],
    }
    if drift is not None:
        with pytest.raises(ValueError, match=r"baseline|unavailable"):
            analyzer.analyze_stable_action_version_matrix(
                dependency_report=dependencies, inputs=inputs
            )
        return
    report = analyzer.analyze_stable_action_version_matrix(
        dependency_report=dependencies, inputs=inputs
    )
    assert report["complete"] is True
    assert report["mechanical_gate_passed"] is False
    assert report["upstream_absence_claimed"] is False
    projected = report["actions"][0]
    assert projected["baseline_unavailable"]["reason"] == "upstream_capability_absent"
    assert len(projected["versions"]) == len(inputs)
    assert {candidate["status"] for candidate in projected["versions"]} == {
        "baseline-unavailable-not-projectable"
    }
    assert all(candidate["mechanical_blocker"] for candidate in projected["versions"])


def test_matrix_cli_rejects_a_conflicting_explicit_baseline(
    tmp_path: Path, capsys: Any
) -> None:
    cli = _module("analyze_ds_stable_action_version_matrix")
    dependencies = _dependency_report()
    dependencies["baseline"] = {"ds_version": "3.4.2", "generated_package": "ds_3_4_2"}
    path = tmp_path / "dependencies.json"
    path.write_text(json.dumps(dependencies), encoding="utf-8")
    status = cli.main(
        [
            "--dependency-report",
            str(path),
            "--baseline-version",
            "3.4.1",
        ]
    )
    assert status == 2
    assert "selected baseline differs" in capsys.readouterr().err
