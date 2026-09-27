from __future__ import annotations

import importlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


def _api() -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module("ds_codegen.api")


def test_task_plugin_snapshot_resolves_spi_binding_and_parameter_contract(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=True)

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["SQL"],
    )

    assert snapshot.ds_version == "3.4.2"
    assert snapshot.source_complete is True
    assert snapshot.diagnostics == []
    assert len(snapshot.plugins) == 1
    plugin = snapshot.plugins[0]
    assert plugin.task_type == "SQL"
    assert plugin.registration_kind == "spi_factory"
    assert plugin.parameter_model_import == "org.example.task.SqlParameters"
    assert plugin.model_imports == [
        "org.example.task.AbstractParameters",
        "org.example.task.SqlParameters",
    ]
    assert plugin.enum_imports == ["org.example.task.enums.SqlSourceType"]
    assert plugin.binding_evidence == [
        "org.example.sql.SqlTaskChannelFactory.getName() -> SQL",
        (
            "org.example.sql.SqlTaskChannel.parseParameters() -> "
            "org.example.task.SqlParameters"
        ),
    ]

    model = next(
        model
        for model in snapshot.models
        if model.import_path == plugin.parameter_model_import
    )
    assert [(field.wire_name, field.java_type) for field in model.fields] == [
        ("localParams", "List<String>"),
        ("type", "String"),
        ("datasource", "int"),
        ("sql", "String"),
        ("sqlSource", "org.example.task.enums.SqlSourceType"),
        ("sqlResource", "String"),
    ]
    assert [
        (enum.name, [value.name for value in enum.values]) for enum in snapshot.enums
    ] == [
        ("SqlSourceType", ["SCRIPT", "FILE"]),
    ]
    assert plugin.validation is not None
    assert plugin.validation.method_name == "checkParameters"
    assert plugin.validation.referenced_fields == [
        "datasource",
        "sql",
        "sqlResource",
        "type",
    ]
    assert plugin.resource_references is not None
    assert plugin.resource_references.method_name == "getResourceFilesList"
    assert plugin.resource_references.referenced_fields == ["sqlResource"]
    assert plugin.semantic_fingerprint.startswith("sha256:")


def test_task_plugin_snapshot_projects_json_property_field_wire_names(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    parameter_path = source_root / "src/main/java/org/example/task/SqlParameters.java"
    _write(
        parameter_path,
        parameter_path.read_text(encoding="utf-8")
        .replace(
            "    private String type;",
            '    @JsonProperty("taskType")\n    private String type;',
        )
        .replace(
            "    private int datasource;",
            ('    @JsonProperty(value = "dataSourceId")\n    private int datasource;'),
        )
        .replace(
            "    private String sql;",
            (
                '    @Schema(name = "schemaSql")\n'
                '    @JsonProperty("query")\n'
                "    private String sql;"
            ),
        ),
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["SQL"],
    )

    model = next(
        model
        for model in snapshot.models
        if model.import_path == "org.example.task.SqlParameters"
    )
    assert [(field.name, field.wire_name) for field in model.fields] == [
        ("localParams", "localParams"),
        ("type", "taskType"),
        ("datasource", "dataSourceId"),
        ("sql", "query"),
    ]


def test_task_plugin_snapshot_ignores_test_only_factories(tmp_path: Path) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    _write(
        source_root / "src/test/java/org/example/fake/FakeTaskChannelFactory.java",
        """
package org.example.fake;

public class FakeTaskChannelFactory implements TaskChannelFactory {
    public String getName() {
        return "TEST_ONLY";
    }

    public TaskChannel create() {
        return new FakeTaskChannel();
    }
}
""",
    )
    _write(
        source_root / "src/test/java/org/example/fake/FakeTaskChannel.java",
        """
package org.example.fake;

public class FakeTaskChannel implements TaskChannel {
    public FakeParameters parseParameters(String taskParams) {
        return JSONUtils.parseObject(taskParams, FakeParameters.class);
    }
}
""",
    )
    _write(
        source_root / "src/test/java/org/example/fake/FakeParameters.java",
        """
package org.example.fake;

public class FakeParameters {
    private String fake;
}
""",
    )
    _write(
        source_root
        / "src/main/java/org/example/visiblefake/VisibleFakeTaskChannelFactory.java",
        """
package org.example.visiblefake;

@VisibleForTesting
public class VisibleFakeTaskChannelFactory implements TaskChannelFactory {
    public String getName() {
        return "VISIBLE_TEST_ONLY";
    }

    public TaskChannel create() {
        return new VisibleFakeTaskChannel();
    }
}
""",
    )
    _write(
        source_root
        / "src/main/java/org/example/visiblefake/VisibleFakeTaskChannel.java",
        """
package org.example.visiblefake;

public class VisibleFakeTaskChannel implements TaskChannel {
    public VisibleFakeParameters parseParameters(String taskParams) {
        return JSONUtils.parseObject(taskParams, VisibleFakeParameters.class);
    }
}
""",
    )
    _write(
        source_root
        / "src/main/java/org/example/visiblefake/VisibleFakeParameters.java",
        """
package org.example.visiblefake;

public class VisibleFakeParameters {
    private String fake;
}
""",
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["SQL"],
    )

    assert [plugin.task_type for plugin in snapshot.plugins] == ["SQL"]


def test_task_plugin_snapshot_fails_closed_on_unresolved_production_factory(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    _write(
        source_root / "src/main/java/org/example/future/FutureTaskChannelFactory.java",
        """
package org.example.future;

public class FutureTaskChannelFactory implements TaskChannelFactory {
    public String getName() {
        return discoverNameAtRuntime();
    }

    public TaskChannel create() {
        return new FutureTaskChannel();
    }
}
""",
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["SQL"],
    )

    assert snapshot.source_complete is False
    assert [diagnostic.code for diagnostic in snapshot.diagnostics] == [
        "factory_task_type_not_resolved"
    ]
    assert snapshot.diagnostics[0].path is not None
    assert snapshot.diagnostics[0].path.endswith("FutureTaskChannelFactory.java")


def test_task_plugin_snapshot_fails_closed_on_incomplete_model_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _api()
    module = importlib.import_module("ds_codegen.task_plugins")
    real_extract = module.extract_model_specs

    def incomplete_extract(*args: Any, **kwargs: Any) -> Any:
        models, enum_imports, model_imports = real_extract(*args, **kwargs)
        return models, enum_imports, {*model_imports, "org.example.task.MissingModel"}

    monkeypatch.setattr(module, "extract_model_specs", incomplete_extract)
    snapshot = api.build_task_plugin_snapshot_from_source(
        _modern_sql_source(tmp_path, version="3.4.2", file_mode=False),
        task_types=["SQL"],
    )

    assert snapshot.source_complete is False
    assert snapshot.plugins == []
    assert [diagnostic.code for diagnostic in snapshot.diagnostics] == [
        "parameter_contract_not_resolved"
    ]
    assert "MissingModel" in snapshot.diagnostics[0].message


def test_task_plugin_diff_reports_sql_3_4_2_authoring_drift_without_guessing_policy(
    tmp_path: Path,
) -> None:
    api = _api()
    base = api.build_task_plugin_snapshot_from_source(
        _modern_sql_source(tmp_path, version="3.4.1", file_mode=False),
        task_types=["SQL"],
    )
    target = api.build_task_plugin_snapshot_from_source(
        _modern_sql_source(tmp_path, version="3.4.2", file_mode=True),
        task_types=["SQL"],
    )

    report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=base,
        target_label="3.4.2",
        target=target,
    )

    assert report["kind"] == "dolphinscheduler-task-plugin-contract-diff"
    assert report["source_complete"] is True
    assert report["review_complete"] is False
    assert report["summary"]["task_types"] == {
        "added": 0,
        "removed": 0,
        "changed": 1,
    }
    sql = report["task_types"]["changed"][0]
    assert sql["key"] == "SQL"
    assert sql["fields"]["added"] == [
        {
            "key": "sqlResource",
            "java_type": "String",
            "wire_name": "sqlResource",
            "default_value": None,
        },
        {
            "key": "sqlSource",
            "java_type": "org.example.task.enums.SqlSourceType",
            "wire_name": "sqlSource",
            "default_value": None,
        },
    ]
    assert sql["enums"]["added"] == [
        {
            "key": "org.example.task.enums.SqlSourceType",
            "name": "SqlSourceType",
            "values": ["SCRIPT", "FILE"],
        }
    ]
    assert sql["behavior"]["changed"] == [
        {
            "key": "checkParameters",
            "before_referenced_fields": ["datasource", "sql", "type"],
            "after_referenced_fields": [
                "datasource",
                "sql",
                "sqlResource",
                "type",
            ],
        },
        {
            "key": "getResourceFilesList",
            "before_referenced_fields": [],
            "after_referenced_fields": ["sqlResource"],
        },
    ]
    assert [item["id"] for item in report["unmapped_changes"]] == [
        "SQL:behavior:checkParameters:changed",
        "SQL:behavior:getResourceFilesList:changed",
        "SQL:enum:org.example.task.enums.SqlSourceType:added",
        "SQL:field:sqlResource:added",
        "SQL:field:sqlSource:added",
    ]
    assert "required" not in sql["fields"]["added"][0]
    assert "affected_actions" not in report


def test_task_plugin_diff_maps_requiredness_changes_to_review_items(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    target_parameters = (
        target_root / "src/main/java/org/example/task/SqlParameters.java"
    )
    _write(
        target_parameters,
        target_parameters.read_text(encoding="utf-8").replace(
            "    private String sql;",
            "    @Schema(required = true)\n    private String sql;",
        ),
    )

    report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=api.build_task_plugin_snapshot_from_source(
            base_root,
            task_types=["SQL"],
        ),
        target_label="3.4.2",
        target=api.build_task_plugin_snapshot_from_source(
            target_root,
            task_types=["SQL"],
        ),
    )

    assert [item["id"] for item in report["unmapped_changes"]] == [
        "SQL:field:sql:changed"
    ]
    changes = report["task_types"]["changed"][0]["fields"]["changed"][0]["changes"]
    assert changes == [{"field": "required", "before": None, "after": True}]


def test_task_plugin_diff_maps_nested_model_changes_to_review_items(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    for source_root, extra_field in (
        (base_root, ""),
        (target_root, "    private int timeout;\n"),
    ):
        parameter_path = (
            source_root / "src/main/java/org/example/task/SqlParameters.java"
        )
        _write(
            parameter_path,
            parameter_path.read_text(encoding="utf-8").replace(
                "    private String sql;",
                "    private String sql;\n    private ExtraOptions options;",
            ),
        )
        _write(
            source_root / "src/main/java/org/example/task/ExtraOptions.java",
            f"""
package org.example.task;

public class ExtraOptions {{
    private int retries;
{extra_field}}}
""",
        )

    report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=api.build_task_plugin_snapshot_from_source(
            base_root,
            task_types=["SQL"],
        ),
        target_label="3.4.2",
        target=api.build_task_plugin_snapshot_from_source(
            target_root,
            task_types=["SQL"],
        ),
    )

    assert [item["id"] for item in report["unmapped_changes"]] == [
        "SQL:field:org.example.task.ExtraOptions#timeout:added"
    ]


def test_task_plugin_diff_maps_inheritance_changes_to_review_items(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    target_parameters = (
        target_root / "src/main/java/org/example/task/SqlParameters.java"
    )
    _write(
        target_parameters,
        target_parameters.read_text(encoding="utf-8").replace(
            "extends AbstractParameters",
            "extends AlternateParameters",
        ),
    )
    _write(
        target_root / "src/main/java/org/example/task/AlternateParameters.java",
        """
package org.example.task;

import java.util.List;

public abstract class AlternateParameters {
    private List<String> localParams;
}
""",
    )

    report = api.compare_task_plugin_snapshots(
        base_label="3.4.1",
        base=api.build_task_plugin_snapshot_from_source(
            base_root,
            task_types=["SQL"],
        ),
        target_label="3.4.2",
        target=api.build_task_plugin_snapshot_from_source(
            target_root,
            task_types=["SQL"],
        ),
    )

    assert [item["id"] for item in report["unmapped_changes"]] == [
        "SQL:model:org.example.task.AbstractParameters:removed",
        "SQL:model:org.example.task.AlternateParameters:added",
        "SQL:model:org.example.task.SqlParameters:changed",
    ]


def test_hybrid_generation_prefers_worker_plugin_binding_and_keeps_legacy_only_types(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="2.0.0", file_mode=False)
    _write(
        source_root / "src/main/java/org/example/common/SqlParameters.java",
        """
package org.example.common;

public class SqlParameters {
    private String staleCommonShape;
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/common/SwitchParameters.java",
        """
package org.example.common;

public class SwitchParameters {
    private String resultConditionLocation;
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/common/TaskParametersUtils.java",
        """
package org.example.common;

public class TaskParametersUtils {
    public static Object getParameters(String taskType, String parameter) {
        switch (taskType) {
            case "SQL":
                return JSONUtils.parseObject(parameter, SqlParameters.class);
            case "SWITCH":
                return JSONUtils.parseObject(parameter, SwitchParameters.class);
            default:
                return null;
        }
    }
}
""",
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["SQL", "SWITCH"],
    )

    assert snapshot.source_complete is True
    assert [
        (plugin.task_type, plugin.parameter_model_import) for plugin in snapshot.plugins
    ] == [
        ("SQL", "org.example.task.SqlParameters"),
        ("SWITCH", "org.example.common.SwitchParameters"),
    ]


def test_task_plugin_snapshot_round_trips_without_controller_contract_shape(
    tmp_path: Path,
) -> None:
    api = _api()
    snapshot = api.build_task_plugin_snapshot_from_source(
        _modern_sql_source(tmp_path, version="3.4.2", file_mode=True),
        task_types=["SQL"],
    )
    output = tmp_path / "task-plugins.json"

    api.write_task_plugin_snapshot(snapshot, output)
    loaded = api.load_task_plugin_snapshot(output)

    assert loaded == snapshot
    payload = output.read_text(encoding="utf-8")
    assert '"kind": "dolphinscheduler-task-plugin-contract"' in payload
    assert '"operations"' not in payload


def test_getter_and_comment_churn_changes_source_evidence_but_not_semantic_diff(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    base = api.build_task_plugin_snapshot_from_source(
        base_root,
        task_types=["SQL"],
    )
    parameter_path = base_root / "src/main/java/org/example/task/SqlParameters.java"
    source = parameter_path.read_text(encoding="utf-8")
    parameter_path.write_text(
        source.replace(
            "public class SqlParameters",
            "/** Refactored accessors. */\npublic class SqlParameters",
        ).replace(
            "\n}",
            "\n    public String getSql() { return sql; }\n}\n",
        ),
        encoding="utf-8",
    )
    target = api.build_task_plugin_snapshot_from_source(
        base_root,
        task_types=["SQL"],
    )

    report = api.compare_task_plugin_snapshots(
        base_label="before",
        base=base,
        target_label="after",
        target=target,
    )

    assert base.plugins[0].source_fingerprint != target.plugins[0].source_fingerprint
    assert (
        base.plugins[0].semantic_fingerprint == target.plugins[0].semantic_fingerprint
    )
    assert report["summary"]["task_types"]["changed"] == 0
    assert report["review_complete"] is True


def test_spi_binding_resolves_task_type_from_imported_static_constant(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="3.0.0", file_mode=False)
    _write(
        source_root / "src/main/java/org/example/task/TaskConstants.java",
        """
package org.example.task;

public final class TaskConstants {
    public static final String TASK_TYPE_CONDITIONS = "CONDITIONS";
}
""",
    )
    _write(
        source_root
        / "src/main/java/org/example/conditions/ConditionsTaskChannelFactory.java",
        """
package org.example.conditions;

import static org.example.task.TaskConstants.TASK_TYPE_CONDITIONS;

public class ConditionsTaskChannelFactory implements TaskChannelFactory {
    public String getName() {
        return TASK_TYPE_CONDITIONS;
    }

    public TaskChannel create() {
        return new ConditionsTaskChannel();
    }
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/conditions/ConditionsTaskChannel.java",
        """
package org.example.conditions;

import org.example.task.ConditionsParameters;

public class ConditionsTaskChannel implements TaskChannel {
    public AbstractParameters parseParameters(String taskParams) {
        return JSONUtils.parseObject(taskParams, ConditionsParameters.class);
    }
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/task/ConditionsParameters.java",
        """
package org.example.task;

public class ConditionsParameters extends AbstractParameters {
    private String dependence;
}
""",
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["CONDITIONS"],
    )

    assert snapshot.source_complete is True
    assert snapshot.plugins[0].task_type == "CONDITIONS"
    assert snapshot.plugins[0].parameter_model_import == (
        "org.example.task.ConditionsParameters"
    )


def test_task_plugin_inventory_keeps_exact_provenance_and_cached_snapshots(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = _modern_sql_source(tmp_path, version="9.9.9", file_mode=True)
    _tag_source(source_root, "9.9.9")
    snapshot_dir = tmp_path / "snapshots"

    report = api.generate_task_plugin_inventory(
        [api.ContractInput("9.9.9", source_root, "ds-source")],
        task_types=["SQL"],
        snapshot_dir=snapshot_dir,
    )

    assert report["complete"] is True
    assert report["source_complete"] is True
    assert report["diagnostics"] == []
    assert report["targets"][0]["counts"] == {
        "task_types": 1,
        "models": 2,
        "enums": 1,
    }
    assert report["targets"][0]["task_types"][0]["key"] == "SQL"
    assert report["targets"][0]["provenance"]["exact"] is True
    cached = snapshot_dir / "ds-9.9.9-task-plugins.json"
    payload = json.loads(cached.read_text(encoding="utf-8"))
    assert (
        payload["provenance"]["contract_digest"]
        == (report["targets"][0]["provenance"]["contract_digest"])
    )

    cached_report = api.generate_task_plugin_inventory(
        [api.ContractInput("9.9.9", cached, "snapshot")],
        task_types=["SQL"],
    )
    cached_target = cached_report["targets"][0]
    source_target = report["targets"][0]
    assert (
        cached_target["contract_fingerprint"] == source_target["contract_fingerprint"]
    )
    assert cached_target["task_types"] == source_target["task_types"]
    assert cached_target["provenance"]["input_kind"] == "snapshot"
    assert (
        cached_target["provenance"]["origin"] == (source_target["provenance"]["origin"])
    )


def test_task_plugin_diff_orchestration_accepts_cached_or_source_inputs(
    tmp_path: Path,
) -> None:
    api = _api()
    base = api.build_task_plugin_snapshot_from_source(
        _modern_sql_source(tmp_path, version="3.4.1", file_mode=False),
        task_types=["SQL"],
    )
    base_path = tmp_path / "base.json"
    api.write_task_plugin_snapshot(base, base_path)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=True)
    inputs = api.parse_contract_inputs(
        snapshot_values=[f"3.4.1={base_path}"],
        source_values=[f"3.4.2={target_root}"],
        minimum_count=2,
        require_version_labels=False,
    )

    reports = api.analyze_task_plugin_versions(
        inputs,
        base_label="3.4.1",
        target_labels=[],
        task_types=["SQL"],
    )
    markdown = api.render_task_plugin_diff_reports(
        reports,
        output_format="markdown",
        max_items=20,
    )
    json_text = api.render_task_plugin_diff_reports(
        reports,
        output_format="json",
        max_items=20,
    )

    assert len(reports) == 1
    assert reports[0]["target"]["label"] == "3.4.2"
    assert "# DolphinScheduler Task-Plugin Diff: 3.4.1 -> 3.4.2" in markdown
    assert "`SQL:field:sqlResource:added`" in markdown
    assert "review required" in markdown.lower()
    assert json.loads(json_text)["review_complete"] is False


def test_reviewed_task_plugin_impact_is_source_guarded_and_maps_every_change(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=True)
    _tag_source(base_root, "3.4.1")
    _tag_source(target_root, "3.4.2")
    inputs = [
        api.ContractInput("3.4.1", base_root, "ds-source"),
        api.ContractInput("3.4.2", target_root, "ds-source"),
    ]
    source_roots = {"3.4.1": base_root, "3.4.2": target_root}
    report = api.analyze_task_plugin_versions(
        inputs,
        base_label="3.4.1",
        target_labels=["3.4.2"],
        task_types=["SQL"],
    )[0]
    change_ids = [item["id"] for item in report["unmapped_changes"]]
    review = {
        "schema_version": 1,
        "kind": "dolphinscheduler-task-plugin-impact-review",
        "base": {
            "label": "3.4.1",
            "contract_fingerprint": report["base"]["contract_fingerprint"],
            "source_identity_fingerprint": report["base"][
                "source_identity_fingerprint"
            ],
        },
        "target": {
            "label": "3.4.2",
            "contract_fingerprint": report["target"]["contract_fingerprint"],
            "source_identity_fingerprint": report["target"][
                "source_identity_fingerprint"
            ],
        },
        "evidence": {
            "server_parameter_model": [
                "src/main/java/org/example/task/SqlParameters.java"
            ]
        },
        "facets": [
            {
                "id": "SQL/inline_script",
                "compatibility": "equivalent",
                "implementation_status": "existing",
            },
            {
                "id": "SQL/resource_file",
                "compatibility": "added",
                "implementation_status": "not_implemented",
            },
        ],
        "decisions": [
            {
                "id": "sql-resource-file",
                "change_ids": change_ids,
                "cli_impact": "affected",
                "evidence_planes": ["server_parameter_model"],
                "facets": ["SQL/inline_script", "SQL/resource_file"],
                "implementation_status": "not_implemented",
                "affected_actions": [
                    {"action": "task-type.schema", "role": "discover"},
                    {"action": "template.task", "role": "render"},
                    {"action": "lint.workflow", "role": "validate"},
                    {"action": "workflow.create", "role": "compile"},
                    {"action": "workflow.edit", "role": "preserve"},
                    {"action": "task.update", "role": "preserve"},
                ],
                "rationale": "3.4.2 adds a second reviewed authoring mode.",
            }
        ],
    }

    materialized = api.apply_task_plugin_impact_review(
        report,
        review,
        source_roots=source_roots,
    )

    assert materialized["review_complete"] is True
    assert materialized["unmapped_changes"] == []
    assert materialized["review"]["decisions"][0]["id"] == "sql-resource-file"
    assert materialized["review"]["mapped_change_ids"] == change_ids

    decisions = review["decisions"]
    assert isinstance(decisions, list)
    decision = decisions[0]
    assert isinstance(decision, dict)
    missing_cli_impact = {
        **review,
        "decisions": [
            {key: value for key, value in decision.items() if key != "cli_impact"}
        ],
    }
    with pytest.raises(ValueError, match="requires cli_impact"):
        api.apply_task_plugin_impact_review(
            report,
            missing_cli_impact,
            source_roots=source_roots,
        )
    missing_evidence_plane = {
        **review,
        "decisions": [{**decision, "evidence_planes": ["ui_serialization"]}],
    }
    with pytest.raises(ValueError, match="unknown evidence plane"):
        api.apply_task_plugin_impact_review(
            report,
            missing_evidence_plane,
            source_roots=source_roots,
        )

    unguarded_base = review["base"]
    unguarded_target = review["target"]
    assert isinstance(unguarded_base, dict)
    assert isinstance(unguarded_target, dict)
    unguarded = {
        **review,
        "base": {
            key: value
            for key, value in unguarded_base.items()
            if key != "source_identity_fingerprint"
        },
        "target": {
            key: value
            for key, value in unguarded_target.items()
            if key != "source_identity_fingerprint"
        },
    }
    with pytest.raises(ValueError, match="requires both endpoint identities"):
        api.apply_task_plugin_impact_review(
            report,
            unguarded,
            source_roots=source_roots,
        )
    null_identity = {
        **review,
        "base": {**unguarded_base, "source_identity_fingerprint": None},
        "target": {**unguarded_target, "source_identity_fingerprint": None},
    }
    with pytest.raises(ValueError, match="must be a SHA-256 digest"):
        api.apply_task_plugin_impact_review(
            report,
            null_identity,
            source_roots=source_roots,
        )

    review_target = review["target"]
    assert isinstance(review_target, dict)
    stale = {
        **review,
        "target": {
            **review_target,
            "contract_fingerprint": "sha256:" + "0" * 64,
        },
    }
    with pytest.raises(ValueError, match="target contract fingerprint is stale"):
        api.apply_task_plugin_impact_review(
            report,
            stale,
            source_roots=source_roots,
        )


def test_review_source_identity_guard_covers_ui_and_execution_evidence(
    tmp_path: Path,
) -> None:
    api = _api()
    base_root = _modern_sql_source(tmp_path, version="3.4.1", file_mode=False)
    target_root = _modern_sql_source(tmp_path, version="3.4.2", file_mode=False)
    evidence_path = Path("ui/src/sql-authoring.ts")
    for source_root in (base_root, target_root):
        _write(source_root / evidence_path, "export const sqlSource = 'SCRIPT'\n")
    _tag_source(base_root, "3.4.1")
    _tag_source(target_root, "3.4.2")
    inputs = [
        api.ContractInput("3.4.1", base_root, "ds-source"),
        api.ContractInput("3.4.2", target_root, "ds-source"),
    ]
    report = api.analyze_task_plugin_versions(
        inputs,
        base_label="3.4.1",
        target_labels=["3.4.2"],
        task_types=["SQL"],
    )[0]
    review = {
        "schema_version": 1,
        "kind": "dolphinscheduler-task-plugin-impact-review",
        "base": {
            "label": "3.4.1",
            "contract_fingerprint": report["base"]["contract_fingerprint"],
            "source_identity_fingerprint": report["base"][
                "source_identity_fingerprint"
            ],
        },
        "target": {
            "label": "3.4.2",
            "contract_fingerprint": report["target"]["contract_fingerprint"],
            "source_identity_fingerprint": report["target"][
                "source_identity_fingerprint"
            ],
        },
        "evidence": {"ui_authoring": [evidence_path.as_posix()]},
        "decisions": [],
    }

    source_roots = {"3.4.1": base_root, "3.4.2": target_root}
    materialized = api.apply_task_plugin_impact_review(
        report,
        review,
        source_roots=source_roots,
    )

    assert materialized["review_complete"] is True
    without_evidence = {
        key: value for key, value in review.items() if key != "evidence"
    }
    with pytest.raises(ValueError, match="requires explicit evidence paths"):
        api.apply_task_plugin_impact_review(
            report,
            without_evidence,
            source_roots=source_roots,
        )
    _write(target_root / evidence_path, "export const sqlSource = 'FILE'\n")
    changed_report = api.analyze_task_plugin_versions(
        inputs,
        base_label="3.4.1",
        target_labels=["3.4.2"],
        task_types=["SQL"],
    )[0]
    assert changed_report["target"]["source_exact"] is False
    with pytest.raises(ValueError, match="target source identity is not exact"):
        api.apply_task_plugin_impact_review(
            changed_report,
            review,
            source_roots=source_roots,
        )


def test_transitional_task_plugin_manager_switch_discovers_control_tasks(
    tmp_path: Path,
) -> None:
    api = _api()
    source_root = tmp_path / "ds-3.2.2"
    _write(
        source_root / "pom.xml",
        (
            "<project><modelVersion>4.0.0</modelVersion>"
            "<groupId>org.example</groupId><artifactId>ds</artifactId>"
            "<version>3.2.2</version></project>"
        ),
    )
    _write(
        source_root / "src/main/java/org/example/task/TaskConstants.java",
        """
package org.example.task;

public final class TaskConstants {
    public static final String TASK_TYPE_CONDITIONS = "CONDITIONS";
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/task/TaskPluginManager.java",
        """
package org.example.task;

public final class TaskPluginManager {
    public AbstractParameters getParameters(ParametersNode node) {
        switch (node.getTaskType()) {
            case TaskConstants.TASK_TYPE_CONDITIONS:
                return JSONUtils.parseObject(
                    node.getTaskParams(), ConditionsParameters.class
                );
            default:
                return null;
        }
    }
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/task/AbstractParameters.java",
        """
package org.example.task;

public abstract class AbstractParameters {}
""",
    )
    _write(
        source_root / "src/main/java/org/example/task/ConditionsParameters.java",
        """
package org.example.task;

public class ConditionsParameters extends AbstractParameters {
    private String dependence;
}
""",
    )

    snapshot = api.build_task_plugin_snapshot_from_source(
        source_root,
        task_types=["CONDITIONS"],
    )

    assert snapshot.source_complete is True
    assert snapshot.diagnostics == []
    assert len(snapshot.plugins) == 1
    plugin = snapshot.plugins[0]
    assert plugin.task_type == "CONDITIONS"
    assert plugin.parameter_model_import == "org.example.task.ConditionsParameters"
    assert plugin.execution_kind == "logic"
    assert plugin.registration_kind == "logic_switch"


def _modern_sql_source(
    tmp_path: Path,
    *,
    version: str,
    file_mode: bool,
) -> Path:
    source_root = tmp_path / f"ds-{version}"
    _write(
        source_root / "pom.xml",
        (
            "<project><modelVersion>4.0.0</modelVersion>"
            "<groupId>org.example</groupId><artifactId>ds</artifactId>"
            f"<version>{version}</version></project>"
        ),
    )
    _write(
        source_root / "src/main/java/org/example/sql/SqlTaskChannelFactory.java",
        """
package org.example.sql;

public class SqlTaskChannelFactory implements TaskChannelFactory {
    public static final String NAME = "SQL";

    public String getName() {
        return NAME;
    }

    public TaskChannel create() {
        return new SqlTaskChannel();
    }
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/sql/SqlTaskChannel.java",
        """
package org.example.sql;

import org.example.task.SqlParameters;

public class SqlTaskChannel implements TaskChannel {
    public AbstractParameters parseParameters(String taskParams) {
        return JSONUtils.parseObject(taskParams, SqlParameters.class);
    }
}
""",
    )
    _write(
        source_root / "src/main/java/org/example/task/AbstractParameters.java",
        """
package org.example.task;

import java.util.List;

public abstract class AbstractParameters {
    private List<String> localParams;
}
""",
    )
    parameter_source = """
package org.example.task;

public class SqlParameters extends AbstractParameters {
    private String type;
    private int datasource;
    private String sql;

    public boolean checkParameters() {
        return datasource != 0
            && StringUtils.isNotEmpty(type)
            && StringUtils.isNotEmpty(sql);
    }

    public List<ResourceInfo> getResourceFilesList() {
        return Collections.emptyList();
    }
}
"""
    if file_mode:
        parameter_source = parameter_source.replace(
            "package org.example.task;",
            (
                "package org.example.task;\n\n"
                "import org.example.task.enums.SqlSourceType;"
            ),
        )
        parameter_source = parameter_source.replace(
            "    private String sql;",
            (
                "    private String sql;\n"
                "    private SqlSourceType sqlSource;\n"
                "    private String sqlResource;"
            ),
        )
        parameter_source = parameter_source.replace(
            """        return datasource != 0
            && StringUtils.isNotEmpty(type)
            && StringUtils.isNotEmpty(sql);""",
            """        if (datasource == 0 || StringUtils.isEmpty(type)) {
            return false;
        }
        if (StringUtils.isNotEmpty(sql)) {
            return true;
        }
        return StringUtils.isNotEmpty(sqlResource);""",
        )
        parameter_source = parameter_source.replace(
            "        return Collections.emptyList();",
            """        List<ResourceInfo> resources = new ArrayList<>();
        if (StringUtils.isNotEmpty(sqlResource)) {
            resources.add(new ResourceInfo(sqlResource));
        }
        return resources;""",
        )
    _write(
        source_root / "src/main/java/org/example/task/SqlParameters.java",
        parameter_source,
    )
    if file_mode:
        _write(
            source_root / "src/main/java/org/example/task/enums/SqlSourceType.java",
            """
package org.example.task.enums;

public enum SqlSourceType {
    SCRIPT, FILE
}
""",
        )
    return source_root


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _tag_source(source_root: Path, version: str) -> None:
    git = shutil.which("git")
    assert git is not None
    commands = (
        ("init", "-q"),
        ("config", "user.name", "Task Plugin Test"),
        ("config", "user.email", "task-plugin@example.invalid"),
        ("add", "."),
        ("commit", "-qm", "fixture"),
        ("tag", version),
    )
    for args in commands:
        subprocess.run(  # noqa: S603
            [git, "-C", str(source_root), *args],
            check=True,
            capture_output=True,
            text=True,
        )
