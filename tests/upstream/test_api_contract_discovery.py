from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from dsctl.generated import version_discovery as facts
from dsctl.upstream import api_contract_discovery as matcher

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue


def _reviewed_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    parameter = SimpleNamespace(
        name="state",
        location="request",
        schema_type="string",
        document_schema_types=("string",),
        required=True,
        document_required=(True,),
        enum_values=("ONLINE", "OFFLINE"),
        default_value=None,
        item_schema_type=None,
    )
    operations = {
        "list": SimpleNamespace(
            operation_id="list",
            api_group="v1",
            method="GET",
            path="projects",
            parameters=(),
            ignored_document_parameters=(),
        ),
        "create": SimpleNamespace(
            operation_id="create",
            api_group="v1",
            method="POST",
            path="projects",
            parameters=(parameter,),
            ignored_document_parameters=("loginUser",),
        ),
    }
    monkeypatch.setattr(facts, "OPERATION_CONTRACTS", operations, raising=False)
    monkeypatch.setattr(
        facts,
        "CONTRACT_PROFILES",
        {
            version: SimpleNamespace(
                document_paths=("v2/api-docs",), operations=("list", "create")
            )
            for version in ("1.3.9", "2.0.0")
        },
        raising=False,
    )
    monkeypatch.setattr(
        facts,
        "DOCUMENT_PROBES",
        (
            SimpleNamespace(
                source="swagger2",
                path="v2/api-docs",
                exact_versions=("1.3.9", "2.0.0"),
                api_group=None,
            ),
        ),
        raising=False,
    )


def _document() -> JsonObject:
    return {
        "swagger": "2.0",
        "info": {"title": "Private title"},
        "paths": {
            "/projects": {
                "get": {"parameters": [], "description": "private prose"},
                "post": {
                    "parameters": [
                        {
                            "name": "state",
                            "in": "query",
                            "type": "string",
                            "required": True,
                            "enum": ["ONLINE", "OFFLINE"],
                        }
                    ]
                },
            }
        },
    }


def test_full_document_keeps_all_matching_versions_and_safe_summaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    result = matcher.match_documents({"v2/api-docs": _document()})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("create", "list")
    assert "Private" not in repr(result)
    assert "private prose" not in repr(result)
    assert "exact release" in result.evidence_summary


@pytest.mark.parametrize(
    "mutation",
    [
        "partial",
        "extension",
        "parameter_type",
        "required",
        "enum",
        "new_parameter",
        "array_items",
        "default",
    ],
)
def test_route_or_request_shape_changes_limit_compatible_operations(
    monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    _reviewed_contracts(monkeypatch)
    document = _document()
    paths = document["paths"]
    assert isinstance(paths, dict)
    project = paths["/projects"]
    assert isinstance(project, dict)
    post = project["post"]
    assert isinstance(post, dict)
    parameters = post["parameters"]
    assert isinstance(parameters, list)
    parameter = parameters[0]
    assert isinstance(parameter, dict)
    if mutation == "partial":
        del project["get"]
    elif mutation == "extension":
        paths["/custom-admin"] = {"get": {}}
    elif mutation == "new_parameter":
        parameters.append({"name": "custom", "in": "query", "type": "string"})
    else:
        changes: dict[str, JsonObject] = {
            "parameter_type": {"type": "integer"},
            "required": {"required": False},
            "enum": {"enum": ["ONLINE", "OFFLINE", "CUSTOM"]},
            "array_items": {"type": "array", "items": {"type": "integer"}},
            "default": {"default": "ONLINE"},
        }
        parameter.update(changes[mutation])
    result = matcher.match_documents({"v2/api-docs": document})
    if mutation in {"partial", "extension"}:
        assert not result.candidate_versions
        assert not result.compatible_operations
    else:
        assert result.candidate_versions == ("1.3.9", "2.0.0")
        assert result.compatible_operations == ("list",)


def test_documentation_text_order_and_known_ignored_parameter_do_not_change_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    document = _document()
    document["paths"] = {
        "/projects": {
            "post": {
                "summary": "new text",
                "parameters": [
                    {"name": "loginUser", "in": "query", "type": "string"},
                    {
                        "name": "state",
                        "in": "formData",
                        "type": "ref",
                        "required": True,
                        "enum": ["OFFLINE", "ONLINE"],
                        "x-example": "private example",
                    },
                ],
            },
            "get": {},
        }
    }
    assert matcher.match_documents({"v2/api-docs": document}).candidate_versions == (
        "1.3.9",
        "2.0.0",
    )


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"swagger": "2.0", "paths": {}},
        {"openapi": "2.0", "paths": {}},
        {"swagger": "2.0", "paths": {"projects": {"get": {}}}},
    ],
)
def test_malformed_documents_are_not_candidates(
    monkeypatch: pytest.MonkeyPatch, payload: JsonValue
) -> None:
    _reviewed_contracts(monkeypatch)
    result = matcher.match_documents({"v2/api-docs": payload})
    assert not result.candidate_versions


@pytest.mark.parametrize(
    "reference",
    [
        "https://private.example/parameters.json",
        "file:///private/secret",
        "#/parameters/loop",
        "#/parameters/missing",
    ],
)
def test_remote_missing_and_cyclic_references_are_rejected_without_fetching(
    monkeypatch: pytest.MonkeyPatch, reference: str
) -> None:
    _reviewed_contracts(monkeypatch)
    document = _document()
    document["parameters"] = {"loop": {"$ref": "#/parameters/loop"}}
    document["paths"] = {
        "/projects": {"get": {}, "post": {"parameters": [{"$ref": reference}]}}
    }
    result = matcher.match_documents({"v2/api-docs": document})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("list",)


def test_openapi3_shared_local_reference_matches_and_local_parameter_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    document: JsonObject = {
        "openapi": "3.0.1",
        "components": {
            "parameters": {
                "state": {
                    "name": "state",
                    "in": "query",
                    "required": True,
                    "schema": {"type": "string", "enum": ["ONLINE", "OFFLINE"]},
                }
            }
        },
        "paths": {
            "/projects": {
                "get": {},
                "post": {"parameters": [{"$ref": "#/components/parameters/state"}]},
            }
        },
    }
    assert matcher.match_documents({"v2/api-docs": document}).candidate_versions == (
        "1.3.9",
        "2.0.0",
    )


def test_grouped_profiles_require_both_complete_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    first = "v3/api-docs?group=v1(current)"
    second = "v3/api-docs?group=v2"
    monkeypatch.setattr(
        facts,
        "DOCUMENT_PROBES",
        tuple(
            SimpleNamespace(
                source="openapi3", path=path, exact_versions=("3.1.0",), api_group=group
            )
            for path, group in ((first, "v1"), (second, "v2"))
        ),
    )
    monkeypatch.setattr(
        facts,
        "CONTRACT_PROFILES",
        {
            "3.1.0": SimpleNamespace(
                document_paths=(first, second), operations=("list", "create")
            )
        },
    )
    monkeypatch.setattr(facts.OPERATION_CONTRACTS["create"], "api_group", "v2")
    one: JsonObject = {"openapi": "3.0.1", "paths": {"/projects": {"get": {}}}}
    two = _document()
    paths = two["paths"]
    assert isinstance(paths, dict)
    project = paths["/projects"]
    assert isinstance(project, dict)
    del project["get"]
    assert not matcher.match_documents({first: one}).candidate_versions
    assert matcher.match_documents({first: one, second: two}).candidate_versions == (
        "3.1.0",
    )
    # A correct union is insufficient when routes appear in the wrong group.
    assert not matcher.match_documents({first: two, second: one}).candidate_versions
    # A second group's duplicate cannot override contradictory first-group facts.
    project["get"] = {
        "parameters": [{"name": "custom", "in": "query", "type": "string"}]
    }
    assert not matcher.match_documents({first: one, second: two}).candidate_versions


def test_operation_must_match_every_route_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    different_parameter = SimpleNamespace(
        name="state",
        location="request",
        schema_type="string",
        document_schema_types=("string",),
        required=True,
        document_required=(True,),
        enum_values=("ONLINE", "OFFLINE", "CUSTOM"),
        default_value=None,
        item_schema_type=None,
    )
    second = SimpleNamespace(
        operation_id="create",
        api_group="v1",
        method="POST",
        path="projects",
        parameters=(different_parameter,),
        ignored_document_parameters=(),
    )
    monkeypatch.setitem(facts.OPERATION_CONTRACTS, "create2", second)
    monkeypatch.setitem(
        facts.CONTRACT_PROFILES,
        "2.0.0",
        SimpleNamespace(
            document_paths=("v2/api-docs",), operations=("list", "create2")
        ),
    )
    result = matcher.match_documents({"v2/api-docs": _document()})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("list",)
    assert any(probe.get("operation_id") == "create" for probe in result.probes)


def test_nested_model_evidence_does_not_authorize_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    body = SimpleNamespace(name="dto", location="body", schema_type=None)
    operation = SimpleNamespace(
        operation_id="create",
        api_group="v1",
        method="POST",
        path="projects",
        parameters=(body,),
        ignored_document_parameters=(),
    )
    monkeypatch.setitem(facts.OPERATION_CONTRACTS, "create", operation)
    document: JsonObject = {
        "openapi": "3.0.1",
        "paths": {
            "/projects": {
                "get": {},
                "post": {
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}},
                                }
                            }
                        }
                    }
                },
            }
        },
    }
    result = matcher.match_documents({"v2/api-docs": document})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("list",)


def test_missing_enum_evidence_does_not_authorize_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reviewed_contracts(monkeypatch)
    document: JsonObject = {
        "swagger": "2.0",
        "paths": {
            "/projects": {
                "get": {},
                "post": {
                    "parameters": [
                        {
                            "name": "state",
                            "in": "query",
                            "type": "string",
                            "required": True,
                        }
                    ]
                },
            }
        },
    }
    result = matcher.match_documents({"v2/api-docs": document})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("list",)


def test_reduced_public_legacy_document_limits_compatibility_per_operation() -> None:
    # Public wire evidence, stripped of host, prose, tags, examples and responses.
    # Its full 133-route inventory matches 1.3.9, but its missing H2 enum does not.
    fixture = (
        Path(__file__).parents[1] / "fixtures/version_discovery/legacy-swagger.json"
    )
    payload: JsonValue = json.loads(fixture.read_text(encoding="utf-8"))
    result = matcher.match_documents({"v2/api-docs": payload})
    assert result.candidate_versions == ("1.3.9",)
    assert "ProjectController.queryProjectListPaging" in result.compatible_operations
    assert (
        "ProcessInstanceController.queryProcessInstanceList"
        in result.compatible_operations
    )
    assert "TaskInstanceController.queryTaskListPaging" in result.compatible_operations
    assert "LoggerController.queryLog" in result.compatible_operations
    assert (
        "DataSourceController.queryDataSourceList" not in result.compatible_operations
    )
    assert (
        "ProjectController.importProcessDefinition" not in result.compatible_operations
    )
    assert any(probe.get("operation_count") == 133 for probe in result.probes)


@pytest.mark.parametrize("item_type", [None, "object", "array"])
def test_unverified_array_items_never_authorize_operation(
    monkeypatch: pytest.MonkeyPatch, item_type: str | None
) -> None:
    _reviewed_contracts(monkeypatch)
    body = SimpleNamespace(
        name="dto", location="body", schema_type="array", item_schema_type=item_type
    )
    operation = SimpleNamespace(
        operation_id="create",
        api_group="v1",
        method="POST",
        path="projects",
        parameters=(body,),
        ignored_document_parameters=(),
    )
    monkeypatch.setitem(facts.OPERATION_CONTRACTS, "create", operation)
    document: JsonObject = {
        "openapi": "3.0.1",
        "paths": {
            "/projects": {
                "get": {},
                "post": {
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"name": {"type": "string"}},
                                    },
                                }
                            }
                        }
                    }
                },
            }
        },
    }
    result = matcher.match_documents({"v2/api-docs": document})
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert result.compatible_operations == ("list",)
