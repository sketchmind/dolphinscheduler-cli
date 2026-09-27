"""Constructor field positions are observable enum wire-contract facts."""

from __future__ import annotations

from dataclasses import replace

from ds_codegen.ir import ContractSnapshot, EnumFieldSpec, EnumSpec, EnumValueSpec
from ds_codegen.render.package.type_renderer import _enum_wire_literal
from ds_codegen.version_diff import compare_contract_snapshots


def test_enum_constructor_order_change_reports_different_wire_values() -> None:
    base = _snapshot()
    original = base.enums[0]
    reordered = replace(original, fields=list(reversed(original.fields)))
    candidate = replace(base, ds_version="3.4.2", enums=[reordered])

    assert _enum_wire_literal(original, original.values[0], "int") == "1"
    assert _enum_wire_literal(reordered, reordered.values[0], "int") == "2"
    report = compare_contract_snapshots(
        base_label="3.4.1", base=base, target_label="3.4.2", target=candidate
    )

    assert report["summary"]["enums"] == {"added": 0, "removed": 0, "changed": 1}
    assert report["enums"]["changed"] == [
        {
            "key": original.import_path,
            "changes": [
                {
                    "field": "field_order",
                    "before": ["code", "alternate"],
                    "after": ["alternate", "code"],
                }
            ],
        }
    ]


def test_unchanged_enum_constructor_order_has_no_difference() -> None:
    base = _snapshot()

    report = compare_contract_snapshots(
        base_label="3.4.1",
        base=base,
        target_label="3.4.2",
        target=replace(base, ds_version="3.4.2"),
    )

    assert report["summary"]["enums"] == {"added": 0, "removed": 0, "changed": 0}
    assert report["enums"]["changed"] == []


def _snapshot() -> ContractSnapshot:
    enum = EnumSpec(
        name="WireCode",
        import_path="org.apache.dolphinscheduler.common.enums.WireCode",
        documentation=None,
        fields=[
            EnumFieldSpec(name="code", java_type="int", annotations=[]),
            EnumFieldSpec(name="alternate", java_type="int", annotations=[]),
        ],
        json_value_field="code",
        values=[EnumValueSpec(name="VALUE", arguments=["1", "2"], documentation=None)],
    )
    return ContractSnapshot(
        ds_version="3.4.1",
        operation_count=0,
        enum_count=1,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[enum],
        dtos=[],
        models=[],
    )
