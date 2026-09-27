from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

PAGE_INFO_IMPORT = "org.apache.dolphinscheduler.api.utils.PageInfo"


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def test_139_page_info_models_the_controller_wire_payload(tmp_path: Path) -> None:
    type_extraction = _load_module("ds_codegen.extract.type_extraction")
    repo_root = _write_legacy_page_sources(tmp_path)

    models, _, _ = type_extraction.extract_model_specs(
        repo_root,
        {PAGE_INFO_IMPORT},
        {},
        {},
    )

    page_info = next(model for model in models if model.import_path == PAGE_INFO_IMPORT)
    assert [field.wire_name for field in page_info.fields] == [
        "totalList",
        "total",
        "totalPage",
        "currentPage",
    ]
    assert {field.wire_name: field.java_type for field in page_info.fields} == {
        "totalList": "List<T>",
        "total": "Integer",
        "totalPage": "Integer",
        "currentPage": "Integer",
    }


def test_139_page_info_projection_fails_closed_when_reassembly_drifts(
    tmp_path: Path,
) -> None:
    type_extraction = _load_module("ds_codegen.extract.type_extraction")
    repo_root = _write_legacy_page_sources(
        tmp_path,
        page_total_getter="getPageSize",
    )

    with pytest.raises(
        RuntimeError,
        match=r"PageInfo wire projection source evidence no longer matches",
    ):
        type_extraction.extract_model_specs(
            repo_root,
            {PAGE_INFO_IMPORT},
            {},
            {},
        )


def _write_legacy_page_sources(
    root: Path,
    *,
    page_total_getter: str = "getTotalCount",
) -> Path:
    repo_root = root / "repo"
    source_root = repo_root / "references/dolphinscheduler"
    source_root.mkdir(parents=True)
    (source_root / "pom.xml").write_text(
        "<project><version>1.3.9</version></project>\n",
        encoding="utf-8",
    )
    _write_java(
        source_root,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "utils/PageInfo.java",
        """
package org.apache.dolphinscheduler.api.utils;

import java.util.List;

public class PageInfo<T> {
    private List<T> lists;
    private Integer totalCount = 0;
    private Integer pageSize = 20;
    private Integer currentPage = 0;
    private Integer pageNo;
}
""",
    )
    _write_java(
        source_root,
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/BaseController.java",
        f"""
package org.apache.dolphinscheduler.api.controller;

import java.util.HashMap;
import java.util.Map;

public class BaseController {{
    public Result returnDataListPaging(Map<String, Object> result) {{
        PageInfo pageInfo = (PageInfo) result.get(Constants.DATA_LIST);
        return success(
                pageInfo.getLists(),
                pageInfo.getCurrentPage(),
                pageInfo.{page_total_getter}(),
                pageInfo.getTotalPage());
    }}

    public Result success(
            Object totalList,
            Integer currentPage,
            Integer total,
            Integer totalPage) {{
        Result result = new Result();
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.TOTAL_LIST, totalList);
        map.put(Constants.CURRENT_PAGE, currentPage);
        map.put(Constants.TOTAL_PAGE, totalPage);
        map.put(Constants.TOTAL, total);
        result.setData(map);
        return result;
    }}
}}
""",
    )
    _write_java(
        source_root,
        "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
        "common/Constants.java",
        """
package org.apache.dolphinscheduler.common;

public class Constants {
    public static final String TOTAL_LIST = "totalList";
    public static final String CURRENT_PAGE = "currentPage";
    public static final String TOTAL_PAGE = "totalPage";
    public static final String TOTAL = "total";
}
""",
    )
    return repo_root


def _write_java(root: Path, relative_path: str, source: str) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
