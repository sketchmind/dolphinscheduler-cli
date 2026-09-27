from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

PAGE_INFO_IMPORT = "org.apache.dolphinscheduler.api.utils.PageInfo"
PROJECT_IMPORT = "org.apache.dolphinscheduler.dao.entity.Project"
PROJECT_PAGE_TYPE = f"{PAGE_INFO_IMPORT}<{PROJECT_IMPORT}>"


def _load_module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_legacy_project_page_has_complete_impact_and_runtime_type_closure(
    tmp_path: Path,
    version: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    impact = _load_module("ds_codegen.compatibility_impact")
    runtime_contract = _load_module("ds_codegen.runtime_contract")
    source_root = tmp_path / f"dolphinscheduler-{version}"
    _write_legacy_project_page_source(source_root, version=version)

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ProjectController.queryProjectListPaging"
    )
    if version != "1.3.9":
        direct_operation = next(
            item
            for item in snapshot.operations
            if item.operation_id == "ProjectController.queryDirectProjectListPaging"
        )
        assert direct_operation.logical_return_type == PROJECT_PAGE_TYPE
    binding = impact.ReviewedBinding(
        source_operations=(operation.operation_id,),
        type_closure=(
            impact.WireTypeRef("models", PAGE_INFO_IMPORT),
            impact.WireTypeRef("models", PROJECT_IMPORT),
        ),
        selector_semantics=(
            impact.SelectorSemantics(
                resource="project",
                consumed_selectors=(),
                exposed_identities=("name", "id"),
                native_identity="id",
                resolution="paged-search-discovery",
            ),
        ),
        evidence_sources=(
            impact.EvidenceSource(
                "controller",
                "ProjectController.java#queryProjectListPaging",
            ),
            impact.EvidenceSource("ui", "projects/list.ts"),
        ),
    )

    report = impact.analyze_semantic_bindings(
        _inventory_from_snapshot(snapshot),
        bindings={version: {"project.page": binding}},
    )
    sliced = runtime_contract.slice_contract_for_bindings(
        snapshot,
        {"project.page": binding},
    )

    assert operation.logical_return_type == PROJECT_PAGE_TYPE
    assert report["complete"] is True
    assert report["diagnostics"] == []
    assert {item.import_path for item in sliced.models} >= {
        PAGE_INFO_IMPORT,
        PROJECT_IMPORT,
    }


def _inventory_from_snapshot(snapshot: Any) -> dict[str, object]:
    return {
        "schema_version": 2,
        "kind": "dolphinscheduler-source-contract-inventory",
        "complete": True,
        "diagnostics": [],
        "targets": [
            {
                "label": snapshot.ds_version,
                "ds_version": snapshot.ds_version,
                "surfaces": {
                    "operations": [
                        {
                            "key": item.operation_id,
                            "fingerprint": f"operation:{item.operation_id}",
                            "http_method": item.http_method,
                            "path": item.path,
                        }
                        for item in snapshot.operations
                    ],
                    "dtos": _type_surface(snapshot.dtos),
                    "models": _type_surface(snapshot.models),
                    "enums": _type_surface(snapshot.enums),
                },
            }
        ],
    }


def _type_surface(items: list[Any]) -> list[dict[str, str]]:
    return [
        {
            "key": item.import_path,
            "fingerprint": f"type:{item.import_path}",
        }
        for item in items
    ]


def _write_legacy_project_page_source(root: Path, *, version: str) -> None:
    root.mkdir(parents=True)
    (root / "pom.xml").write_text(
        f"<project><version>{version}</version></project>\n",
        encoding="utf-8",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/ProjectController.java",
        _controller_source(legacy=version == "1.3.9"),
    )
    if version == "1.3.9":
        _write_java(
            root,
            "org/apache/dolphinscheduler/api/service/ProjectService.java",
            _legacy_concrete_service_source(),
        )
        _write_legacy_process_definition_page_source(root)
        _write_legacy_current_user_source(root)
    else:
        _write_java(
            root,
            "org/apache/dolphinscheduler/api/service/ProjectService.java",
            _project_service_interface_source(),
        )
        _write_java(
            root,
            "org/apache/dolphinscheduler/api/service/impl/ProjectServiceImpl.java",
            _result_service_source(),
        )
    if version == "1.3.9":
        _write_legacy_page_reassembly_source(root)
    else:
        _write_java(
            root,
            "org/apache/dolphinscheduler/api/utils/PageInfo.java",
            """
package org.apache.dolphinscheduler.api.utils;

import java.util.List;

public class PageInfo<T> {
    private List<T> totalList;
    private List<T> lists;
    private int total;

    public PageInfo(int pageNo, int pageSize) {}
    public void setLists(List<T> lists) { this.lists = lists; }
}
""",
        )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/utils/Result.java",
        """
package org.apache.dolphinscheduler.api.utils;

public class Result {
    private Object data;
    public void setData(Object data) {}
    public boolean checkResult() { return true; }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/dao/entity/Project.java",
        """
package org.apache.dolphinscheduler.dao.entity;

public class Project {
    private int id;
    private String name;
}
""",
    )
    if version != "1.3.9":
        _write_java(
            root,
            "org/apache/dolphinscheduler/dao/entity/Schedule.java",
            """
package org.apache.dolphinscheduler.dao.entity;

public class Schedule {}
""",
        )
    _write_schedule_override_sources(root, version=version)
    if version == "1.3.9":
        _write_legacy_datasource_page_source(root)


def _controller_source(*, legacy: bool) -> str:
    if legacy:
        method_body = """
        Map<String, Object> result = projectService.queryProjectListPaging();
        return returnDataListPaging(result);
"""
    else:
        method_body = """
        Result result = checkPageParams();
        if (!result.checkResult()) {
            return result;
        }
        result = projectService.queryProjectListPaging();
        return result;
"""
    return f"""
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.Result;

@RequestMapping("/projects")
public class ProjectController {{
    private ProjectService projectService;

    @GetMapping()
    public Result queryProjectListPaging() {{
        {method_body}
    }}

    @GetMapping("/direct")
    public Result queryDirectProjectListPaging() {{
        Result result = checkPageParams();
        if (!result.checkResult()) {{
            return result;
        }}
        return projectService.queryProjectListPaging();
    }}
}}
"""


def _legacy_concrete_service_source() -> str:
    return """
package org.apache.dolphinscheduler.api.service;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.Project;

public class ProjectService {
    public Map<String, Object> queryProjectListPaging() {
        Map<String, Object> result = new HashMap<>();
        PageInfo pageInfo = new PageInfo<Project>(1, 10);
        result.put(Constants.DATA_LIST, pageInfo);
        return result;
    }
}
"""


def _write_legacy_process_definition_page_source(root: Path) -> None:
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/ProcessDefinitionController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

@RequestMapping("projects/{projectName}/process")
public class ProcessDefinitionController {
    private ProcessDefinitionService processDefinitionService;

    @GetMapping("/list-paging")
    public Result queryProcessDefinitionListPaging() {
        Map<String, Object> result =
                processDefinitionService.queryProcessDefinitionListPaging(
                        null, null, null, null, null, null);
        return returnDataListPaging(result);
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/ProcessDefinitionService.java",
        """
package org.apache.dolphinscheduler.api.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.ProcessData;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

public class ProcessDefinitionService {
    public Map<String, Object> queryProcessDefinitionListPaging(
            User loginUser,
            String projectName,
            String searchVal,
            Integer pageNo,
            Integer pageSize,
            Integer userId) {
        IPage<ProcessDefinition> processDefinitionIPage =
                processDefineMapper.queryDefineListPaging(null, null, 0, 0, false);
        PageInfo pageInfo = new PageInfo<ProcessData>(pageNo, pageSize);
        pageInfo.setLists(processDefinitionIPage.getRecords());
        Map<String, Object> result = null;
        result.put(Constants.DATA_LIST, pageInfo);
        return result;
    }
}
""",
    )
    for model_name in ("ProcessData", "ProcessDefinition"):
        _write_java(
            root,
            f"org/apache/dolphinscheduler/dao/entity/{model_name}.java",
            f"""
package org.apache.dolphinscheduler.dao.entity;

public class {model_name} {{}}
""",
        )


def _write_legacy_page_reassembly_source(root: Path) -> None:
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/utils/PageInfo.java",
        """
package org.apache.dolphinscheduler.api.utils;

import java.util.List;

public class PageInfo<T> {
    private List<T> lists;
    private Integer totalCount = 0;
    private Integer pageSize = 20;
    private Integer currentPage = 0;
    private Integer pageNo;

    public PageInfo(int pageNo, int pageSize) {}
    public void setLists(List<T> lists) { this.lists = lists; }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/BaseController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;

public class BaseController {
    public Result returnDataList(Map<String, Object> result) {
        Object datalist = result.get(Constants.DATA_LIST);
        return success(null, datalist);
    }

    public Result success(String message, Object data) {
        Result result = new Result();
        result.setData(data);
        return result;
    }

    public Result returnDataListPaging(Map<String, Object> result) {
        PageInfo pageInfo = (PageInfo) result.get(Constants.DATA_LIST);
        return success(
                pageInfo.getLists(),
                pageInfo.getCurrentPage(),
                pageInfo.getTotalCount(),
                pageInfo.getTotalPage());
    }

    public Result success(
            Object totalList,
            Integer currentPage,
            Integer total,
            Integer totalPage) {
        Result result = new Result();
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.TOTAL_LIST, totalList);
        map.put(Constants.CURRENT_PAGE, currentPage);
        map.put(Constants.TOTAL_PAGE, totalPage);
        map.put(Constants.TOTAL, total);
        result.setData(map);
        return result;
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/common/Constants.java",
        """
package org.apache.dolphinscheduler.common;

public class Constants {
    public static final String DATA_LIST = "dataList";
    public static final String TOTAL_LIST = "totalList";
    public static final String CURRENT_PAGE = "currentPage";
    public static final String TOTAL_PAGE = "totalPage";
    public static final String TOTAL = "total";
}
""",
    )


def _write_legacy_current_user_source(root: Path) -> None:
    """Satisfy the independent exact-source override active for DS 1.3.9."""
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/UsersController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.User;

@RequestMapping("/users")
public class UsersController {
    private UsersService usersService;

    @GetMapping("/get-user-info")
    public Result getUserInfo(User loginUser) {
        Map<String, Object> result = usersService.getUserInfo(loginUser);
        return returnDataList(result);
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/UsersService.java",
        """
package org.apache.dolphinscheduler.api.service;

import java.util.Map;
import org.apache.dolphinscheduler.dao.entity.User;

public class UsersService {
    public Map<String, Object> getUserInfo(User loginUser) {
        Map<String, Object> result = null;
        User user = loginUser;
        result.put(Constants.DATA_LIST, user);
        return result;
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/dao/entity/User.java",
        """
package org.apache.dolphinscheduler.dao.entity;

public class User {}
""",
    )


def _project_service_interface_source() -> str:
    return """
package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.Result;

public interface ProjectService {
    Result queryProjectListPaging();
}
"""


def _result_service_source() -> str:
    return """
package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.Project;

public class ProjectServiceImpl implements ProjectService {
    public Result queryProjectListPaging() {
        Result result = new Result();
        PageInfo<Project> pageInfo = new PageInfo<>(1, 10);
        result.setData(pageInfo);
        return result;
    }
}
"""


def _write_schedule_override_sources(root: Path, *, version: str) -> None:
    """Materialize the independent exact schedule overrides for partial fixtures."""
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/SchedulerController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.apache.dolphinscheduler.api.utils.Result;

@RequestMapping("/schedules")
public class SchedulerController {
    private SchedulerService schedulerService;

    @PostMapping()
    public Result createSchedule() {
        Map<String, Object> result = schedulerService.insertSchedule();
        return returnDataList(result);
    }

    @PostMapping("/preview")
    public Result previewSchedule() {
        Map<String, Object> result = schedulerService.previewSchedule();
        return returnDataList(result);
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/common/utils/DateUtils.java",
        """
package org.apache.dolphinscheduler.common.utils;

import java.util.Date;

public class DateUtils {
    public static String dateToString(Date date) { return "formatted"; }
}
""",
    )
    if version == "1.3.9":
        _write_java(
            root,
            "org/apache/dolphinscheduler/api/service/SchedulerService.java",
            """
package org.apache.dolphinscheduler.api.service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.dolphinscheduler.common.utils.DateUtils;

public class SchedulerService {
    public Map<String, Object> insertSchedule() {
        Map<String, Object> result = null;
        result.put("scheduleId", 1);
        return result;
    }

    public Map<String, Object> previewSchedule() {
        Map<String, Object> result = new HashMap<>();
        List<Date> dates = null;
        result.put(Constants.DATA_LIST,
                dates.stream().map(date -> DateUtils.dateToString(date)));
        return result;
    }
}
""",
        )
        return
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/SchedulerService.java",
        """
package org.apache.dolphinscheduler.api.service;

import java.util.Map;

public interface SchedulerService {
    Map<String, Object> insertSchedule();
    Map<String, Object> previewSchedule();
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/impl/SchedulerServiceImpl.java",
        """
package org.apache.dolphinscheduler.api.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.apache.dolphinscheduler.common.utils.DateUtils;

public class SchedulerServiceImpl implements SchedulerService {
    private ScheduleMapper scheduleMapper;

    public Map<String, Object> insertSchedule() {
        Map<String, Object> result = new HashMap<>();
        Schedule scheduleObj = null;
        result.put(
                Constants.DATA_LIST,
                scheduleMapper.selectById(scheduleObj.getId()));
        result.put("scheduleId", scheduleObj.getId());
        return result;
    }

    public Map<String, Object> previewSchedule() {
        Map<String, Object> result = new HashMap<>();
        List<Date> dates = null;
        result.put(Constants.DATA_LIST, dates.stream().map(DateUtils::dateToString));
        return result;
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/impl/ScheduleMapper.java",
        """
package org.apache.dolphinscheduler.api.service.impl;

public interface ScheduleMapper {
    Schedule selectById(int id);
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/impl/Schedule.java",
        """
package org.apache.dolphinscheduler.api.service.impl;

public class Schedule {
    public int getId() { return 1; }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/BaseController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.utils.Result;

public class BaseController {
    public Result returnDataList(Map<String, Object> result) {
        Object datalist = result.get(Constants.DATA_LIST);
        return success(null, datalist);
    }

    public Result success(String message, Object data) {
        Result result = new Result();
        result.setData(data);
        return result;
    }
}
""",
    )


def _write_legacy_datasource_page_source(root: Path) -> None:
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/controller/DataSourceController.java",
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.DataSourceService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.DataSource;

@RequestMapping("/datasources")
public class DataSourceController {
    private DataSourceService dataSourceService;

    @GetMapping("/list-paging")
    public Result queryDataSourceListPaging() {
        Map<String, Object> result =
                dataSourceService.queryDataSourceListPaging(null, null, 1, 10);
        return returnDataListPaging(result);
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/api/service/DataSourceService.java",
        """
package org.apache.dolphinscheduler.api.service;

import java.util.List;
import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.DataSource;

public class DataSourceService {
    public Map<String, Object> queryDataSourceListPaging(
            User loginUser,
            String searchVal,
            Integer pageNo,
            Integer pageSize) {
        IPage<DataSource> dataSourceList = null;
        List<DataSource> dataSources = dataSourceList.getRecords();
        PageInfo pageInfo = new PageInfo<Resource>(pageNo, pageSize);
        pageInfo.setLists(dataSources);
        Map<String, Object> result = null;
        result.put(Constants.DATA_LIST, pageInfo);
        return result;
    }
}
""",
    )
    _write_java(
        root,
        "org/apache/dolphinscheduler/dao/entity/DataSource.java",
        """
package org.apache.dolphinscheduler.dao.entity;

public class DataSource {}
""",
    )


def _write_java(root: Path, relative_path: str, source: str) -> None:
    path = root / "dolphinscheduler-api/src/main/java" / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
