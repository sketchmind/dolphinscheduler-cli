from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import javalang

if TYPE_CHECKING:
    from types import ModuleType


def _ensure_tools_on_path() -> None:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))


def _load_module(name: str) -> ModuleType:
    _ensure_tools_on_path()
    return importlib.import_module(name)


def test_resolve_inferred_return_type_prefers_specific_types() -> None:
    inference_support = _load_module("ds_codegen.extract.inference_support")

    inferred = inference_support._resolve_inferred_return_type(
        ["Object", "Long"],
        saw_success_like_void=True,
    )

    assert inferred == "Optional<Long>"


def test_extract_method_invocation_with_qualifier_handles_this_selector_chain() -> None:
    inference_support = _load_module("ds_codegen.extract.inference_support")

    source = """
class Sample {
    Object test() {
        return this.result.success(data);
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    method = compilation_unit.types[0].methods[0]
    return_statement = method.body[0]

    extracted = inference_support._extract_method_invocation_with_qualifier(
        return_statement.expression,
    )

    assert extracted is not None
    invocation, qualifier = extracted
    assert invocation.member == "success"
    assert qualifier == "result"


def test_infer_expression_data_type_handles_collections_empty_list() -> None:
    expression_inference = _load_module("ds_codegen.extract.expression_inference")

    source = """
import java.util.Collections;

class Sample {
    Object test() {
        return Collections.emptyList();
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    method = compilation_unit.types[0].methods[0]
    return_statement = method.body[0]

    deps = expression_inference.ExpressionInferenceDeps(
        infer_argument_types=lambda **_: [],
        infer_service_invocation_payload_type=lambda **_: None,
        infer_same_class_method_payload_type=lambda **_: None,
        infer_success_like_invocation_payload_type=lambda **_: None,
        infer_return_data_list_payload_type=lambda **_: None,
        infer_class_creator_return_type=lambda **_: None,
        infer_selector_chain_data_type=lambda **_: None,
        unwrap_result_like_type=lambda value: value,
    )

    inferred = expression_inference.infer_expression_data_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("Sample.java"),
        expression=return_statement.expression,
        controller_field_types={},
        variable_types={},
        variable_initializers={},
        import_map={"Collections": "java.util.Collections"},
        package_name=None,
        deps=deps,
    )

    assert inferred == "List<Object>"


def test_infer_local_return_statement_payload_type_tracks_result_set_data() -> None:
    inference_support = _load_module("ds_codegen.extract.inference_support")
    local_inference = _load_module("ds_codegen.extract.local_inference")

    source = """
class Sample {
    Result<Object> test(Long code) {
        Result<Object> result = new Result<>();
        result.setData(code);
        return result;
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    method = compilation_unit.types[0].methods[0]
    return_statement = method.body[-1]
    result_initializer = method.body[0].declarators[0].initializer

    deps = local_inference.LocalInferenceDeps(
        infer_expression_return_type=lambda **_: None,
        infer_expression_data_type=lambda **kwargs: (
            "Long"
            if isinstance(kwargs["expression"], javalang.tree.MemberReference)
            and kwargs["expression"].member == "code"
            else None
        ),
        infer_local_data_structure_type=lambda **_: None,
        collect_method_variable_types=lambda _: {},
        collect_method_variable_initializers=lambda _: {},
        infer_argument_types=lambda **_: [],
        find_method_declaration=lambda *args, **kwargs: None,
        method_signature_key=lambda method: (method.name, len(method.parameters)),
        resolve_inferred_return_type=inference_support._resolve_inferred_return_type,
        is_weak_inferred_type=lambda _: False,
        is_collection_like_java_type=lambda _: False,
        is_data_list_expression=lambda _: False,
        unwrap_generated_view_data_list_type=lambda value: value,
        render_reference_name=lambda type_node: str(type_node.name),
        resolve_referenced_import_path=lambda *args, **kwargs: None,
        type_extends_result=lambda *args, **kwargs: False,
    )

    inferred = local_inference.infer_local_return_statement_payload_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("Sample.java"),
        method=method,
        return_statement=return_statement,
        controller_field_types={},
        variable_types={"code": "Long", "result": "Result<Object>"},
        variable_initializers={"result": result_initializer},
        import_map={},
        package_name=None,
        deps=deps,
    )

    assert inferred == "Long"


def test_infer_local_data_structure_type_builds_generated_view() -> None:
    structure_inference = _load_module("ds_codegen.extract.structure_inference")

    source = """
class Sample {
    void test(Long code) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("code", code);
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    method = compilation_unit.types[0].methods[0]
    captured: dict[str, object] = {}

    def _register_generated_view_model(**kwargs: object) -> str:
        captured["registered"] = kwargs
        return "SampleView"

    deps = structure_inference.StructureInferenceDeps(
        infer_local_variable_payload_type=lambda **_: None,
        infer_expression_data_type=lambda **kwargs: (
            "Long"
            if isinstance(kwargs["expression"], javalang.tree.MemberReference)
            and kwargs["expression"].member == "code"
            else None
        ),
        resolve_string_constant_value=lambda **kwargs: (
            str(kwargs["expression"].value).strip('"')
            if isinstance(kwargs["expression"], javalang.tree.Literal)
            else None
        ),
        register_generated_view_model=_register_generated_view_model,
        get_generated_view_model_fields=lambda _: None,
        is_weak_inferred_type=lambda _: False,
    )

    inferred = structure_inference.infer_local_data_structure_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("Sample.java"),
        method=method,
        variable_name="payload",
        view_name_hint="SampleView",
        variable_types={"payload": "Map<String, Object>", "code": "Long"},
        variable_initializers={},
        controller_field_types={},
        import_map={},
        package_name=None,
        deps=deps,
    )

    assert inferred == "SampleView"
    assert captured["registered"] == {
        "base_name": "SampleView",
        "fields": [("code", "Long")],
    }


def test_infer_class_creator_structured_type_builds_generated_view() -> None:
    class_creator_inference = _load_module("ds_codegen.extract.class_creator_inference")

    source = """
class Payload {
    private Long code;

    Payload(Long code) {
        this.code = code;
    }
}

class Sample {
    Object test(Long code) {
        return new Payload(code);
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    payload_class = compilation_unit.types[0]
    sample_method = compilation_unit.types[1].methods[0]
    class_creator = sample_method.body[0].expression
    captured: dict[str, object] = {}

    def _register_generated_view_model(**kwargs: object) -> str:
        captured["registered"] = kwargs
        return "Sample_Payload_created"

    deps = class_creator_inference.ClassCreatorInferenceDeps(
        render_reference_name=lambda type_node: str(type_node.name),
        infer_expression_data_type=lambda **kwargs: (
            "Long"
            if isinstance(kwargs["expression"], javalang.tree.MemberReference)
            and kwargs["expression"].member == "code"
            else None
        ),
        infer_expression_return_type=lambda **_: None,
        infer_argument_types=lambda **_: ["Long"],
        resolve_referenced_import_path=lambda *args, **kwargs: None,
        type_extends_result=lambda *args, **kwargs: False,
        load_java_type_context=lambda **kwargs: (
            (None, payload_class, {}, None)
            if kwargs["java_type"] == "Payload"
            else None
        ),
        find_constructor_declaration=class_creator_inference.find_constructor_declaration,
        find_field_type_declaration_in_hierarchy=lambda **kwargs: (
            "Long" if kwargs["field_name"] == "code" else None
        ),
        register_generated_view_model=_register_generated_view_model,
    )

    inferred = class_creator_inference.infer_class_creator_structured_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("Sample.java"),
        class_creator=class_creator,
        view_name_hint="Sample_Payload_created",
        controller_field_types={},
        variable_types={"code": "Long"},
        variable_initializers={},
        import_map={},
        package_name=None,
        deps=deps,
    )

    assert inferred == "Sample_Payload_created"
    assert captured["registered"] == {
        "base_name": "Sample_Payload_created",
        "fields": [("code", "Long")],
    }


def test_infer_operation_return_type_wraps_void_and_specific_types() -> None:
    operation_inference = _load_module("ds_codegen.extract.operation_inference")

    source = """
class Sample {
    Object test(boolean cond, Long first, Long second) {
        if (cond) {
            return first;
        }
        return second;
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    method = compilation_unit.types[0].methods[0]

    deps = operation_inference.OperationInferenceDeps(
        infer_local_data_structure_type=lambda **_: None,
        infer_local_variable_payload_type=lambda **_: None,
        infer_return_statement_type=lambda **kwargs: (
            "Long"
            if kwargs["return_statement"].expression.member == "first"
            else "Void"
        ),
        infer_local_return_statement_payload_type=lambda **_: None,
    )

    inferred = operation_inference.infer_operation_return_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("Sample.java"),
        method=method,
        controller_field_types={},
        import_map={},
        package_name=None,
        deps=deps,
    )

    assert inferred == "Optional<Long>"


def test_infer_service_method_payload_type_follows_same_class_delegation() -> None:
    service_inference = _load_module("ds_codegen.extract.service_inference")

    source = """
class SampleService {
    Result<Object> current() {
        return helper();
    }

    Result<Long> helper() {
        return null;
    }
}
"""
    compilation_unit = javalang.parse.parse(source)
    service_methods = compilation_unit.types[0].methods

    deps = service_inference.ServiceInferenceDeps(
        infer_argument_types=lambda **_: [],
        infer_expression_data_type=lambda **_: None,
        infer_local_data_structure_type=lambda **_: None,
        infer_operation_return_type=lambda **_: None,
        unwrap_generated_view_data_list_type=lambda value: value,
        is_data_list_expression=lambda _: False,
    )

    inferred = service_inference.infer_service_method_payload_type(
        repo_root=Path(__file__).resolve().parents[2],
        controller_path=Path("SampleService.java"),
        service_method=service_methods[0],
        service_owner_methods=service_methods,
        service_field_types={},
        service_import_map={},
        service_package_name=None,
        deps=deps,
        active_service_methods=(),
    )

    assert inferred == "Long"
