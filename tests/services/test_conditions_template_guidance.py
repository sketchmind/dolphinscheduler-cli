import pytest
import yaml
from tests.value_shape_assertions import assert_mapping

from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.services.template import task_template_result


@pytest.mark.parametrize("version", ["1.3.9", "3.4.1"])
def test_conditions_default_template_explains_automatic_graph_edges(
    version: str,
) -> None:
    result = task_template_result(
        "CONDITIONS", catalog=get_task_authoring_catalog(version)
    )
    data = assert_mapping(result.data)
    assert assert_mapping(data["template"])["variants"] == []
    text = data["yaml"]
    assert isinstance(text, str)
    assert text.count("same workflow tasks[] list") == 1
    assert "incoming edges from predicate tasks" in text
    assert "outgoing\n# edges to successNode/failedNode targets" in text
    assert text.count("no duplicate depends_on is needed") == 1
    assert yaml.safe_load(text)["depends_on"] == []
    if version == "1.3.9":
        assert "each name exactly one different direct successor" in text


@pytest.mark.parametrize("version", ["1.3.9", "3.4.1"])
@pytest.mark.parametrize(
    ("field", "edge_description"),
    [
        (
            "task_params.dependence.dependTaskList[].dependItemList[].task",
            "adds its edge into this CONDITIONS node",
        ),
        (
            "task_params.conditionResult.successNode[]",
            "adds edges from this CONDITIONS node to these targets",
        ),
        (
            "task_params.conditionResult.failedNode[]",
            "adds edges from this CONDITIONS node to these targets",
        ),
    ],
)
def test_conditions_bounded_schema_explains_reference_edges(
    version: str, field: str, edge_description: str
) -> None:
    result = task_type_schema_result(
        "CONDITIONS", catalog=get_task_authoring_catalog(version)
    )
    data = assert_mapping(result.data)
    fields = data["fields"]
    assert isinstance(fields, list)
    matching = [
        assert_mapping(item) for item in fields if assert_mapping(item)["path"] == field
    ]
    assert len(matching) == 1
    description = matching[0]["description"]
    assert isinstance(description, str)
    assert "same workflow tasks[] list" in description
    assert edge_description in description
    assert "no duplicate depends_on is needed" in description
