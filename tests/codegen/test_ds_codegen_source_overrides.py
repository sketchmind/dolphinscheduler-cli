from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from types import ModuleType


_MODERN_SCHEDULE_CREATE_BODY = (
    "Map<String, Object> result = schedulerService.insertSchedule(); "
    "return returnDataList(result);"
)


def _load_module(name: str) -> ModuleType:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def _write_alert_plugin_nullable_page_source(
    root: Path,
    *,
    empty_result: str = "null",
) -> None:
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/impl/AlertPluginInstanceServiceImpl.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.service.impl;

public class AlertPluginInstanceServiceImpl {{
    public Result listPaging(
            User loginUser, String searchVal, int pageNo, int pageSize) {{
        IPage<AlertPluginInstance> records = mapper.query(pageNo, pageSize);
        PageInfo<AlertPluginInstanceVO> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotalList(buildPluginInstanceVOList(records.getRecords()));
        return Result.success(pageInfo);
    }}

    private List<AlertPluginInstanceVO> buildPluginInstanceVOList(
            List<AlertPluginInstance> alertPluginInstances) {{
        if (CollectionUtils.isEmpty(alertPluginInstances)) {{
            return {empty_result};
        }}
        List<PluginDefine> pluginDefineList = mapper.queryDefinitions();
        if (CollectionUtils.isEmpty(pluginDefineList)) {{
            return null;
        }}
        return new ArrayList<>();
    }}
}}
""",
        encoding="utf-8",
    )


def test_alert_plugin_page_nullable_list_override_has_source_proof(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("3.1.0")
    _write_alert_plugin_nullable_page_source(tmp_path / "references/dolphinscheduler")
    page_type = (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>"
    )

    assert (
        scope.resolve(
            operation_id="AlertPluginInstanceController.listPaging",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type=page_type,
            import_map={},
        )
        == page_type
    )
    scope.validate(controller_names={"AlertPluginInstanceController"})


def test_alert_plugin_page_nullable_list_override_rejects_missing_null_branch(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("3.1.0")
    _write_alert_plugin_nullable_page_source(
        tmp_path / "references/dolphinscheduler",
        empty_result="new ArrayList<>()",
    )
    page_type = (
        "org.apache.dolphinscheduler.api.utils.PageInfo<"
        "org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>"
    )

    with pytest.raises(
        RuntimeError,
        match=r"effective response override source evidence no longer matches",
    ):
        scope.resolve(
            operation_id="AlertPluginInstanceController.listPaging",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type=page_type,
            import_map={},
        )


def test_alert_plugin_reviewed_versions_preserve_existing_candidate_corrections() -> (
    None
):
    reviews = _load_module("ds_codegen.extract.return_type_reviews")
    versions_by_rule: dict[str, set[str]] = {}
    for review in reviews.REVIEWED_RETURN_TYPE_RULES:
        versions_by_rule.setdefault(review.rule_name, set()).update(review.versions)
    alert_versions = versions_by_rule["alert_plugin_page_nullable_total_list"]
    for rule_name in (
        "resource_page_records",
        "schedule_create_data_list",
        "task_delete_nullable_payload",
    ):
        assert alert_versions <= versions_by_rule[rule_name]


def _write_ds_source(
    root: Path,
    *,
    version: str,
    raw_return_type: str,
    method_body: str,
    method_name: str = "createSchedule",
    task_raw_return_type: str = "Result",
    task_method_name: str = "updateTaskWithUpstream",
    task_method_body: str = "return Result.success(code);",
) -> None:
    root.mkdir(parents=True)
    (root / "pom.xml").write_text(
        f"<project><version>{version}</version></project>\n",
        encoding="utf-8",
    )
    controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/SchedulerController.java"
    )
    controller_path.parent.mkdir(parents=True, exist_ok=True)
    extra_methods = ""
    if (version == "1.3.9" or version.startswith("2.0.")) and method_name != (
        "previewSchedule"
    ):
        extra_methods += """
    @PostMapping("/preview")
    public Result previewSchedule() {
        Map<String, Object> result = schedulerService.previewSchedule();
        return returnDataList(result);
    }
"""
    if version == "3.4.2" and method_name != "previewSchedule":
        extra_methods += """
    @PostMapping("/preview")
    public Result<List<String>> previewSchedule() {
        List<Date> dates = null;
        return Result.success(dates.stream().map(date -> "formatted")
                .collect(Collectors.toList()));
}
"""
    if method_name == "previewSchedule" and version != "3.4.2":
        extra_methods += """
    @PostMapping()
    public Result createSchedule() {
        Map<String, Object> result = schedulerService.insertSchedule();
        return returnDataList(result);
    }
"""
    controller_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.controller;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.apache.dolphinscheduler.dao.entity.Schedule;

@RequestMapping("/schedules")
public class SchedulerController {{
    private SchedulerService schedulerService;

    @PostMapping()
    public {raw_return_type} {method_name}() {{
        {method_body}
    }}
{extra_methods}
}}
""",
        encoding="utf-8",
    )
    _write_entity_source(root, "Schedule")
    if version == "1.3.9" and method_name in {"createSchedule", "previewSchedule"}:
        _write_legacy_schedule_create_evidence(root)
    if version not in {"1.3.9", "3.4.2"} and method_name in {
        "createSchedule",
        "previewSchedule",
    }:
        _write_modern_schedule_create_evidence(root)
    if (
        version == "1.3.9"
        or version.startswith("2.0.")
        or (method_name == "previewSchedule" and version != "3.4.2")
    ):
        _write_schedule_preview_evidence(root, legacy=version == "1.3.9")
    if version == "3.4.1":
        _write_task_update_source(
            root,
            raw_return_type=task_raw_return_type,
            method_name=task_method_name,
            method_body=task_method_body,
        )


def _write_schedule_preview_evidence(root: Path, *, legacy: bool) -> None:
    api = root / ("dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api")
    service = api / "service/SchedulerService.java"
    implementation = (
        service if legacy else api / "service/impl/SchedulerServiceImpl.java"
    )
    mapping = (
        "date -> DateUtils.dateToString(date)" if legacy else "DateUtils::dateToString"
    )
    body = f"""
    public Map<String, Object> previewSchedule() {{
        Map<String, Object> result = new HashMap<>();
        List<Date> dates = null;
        result.put(Constants.DATA_LIST, dates.stream().map({mapping}));
        return result;
    }}
"""
    source = implementation.read_text(encoding="utf-8")
    source = source.replace(
        "import java.util.Map;",
        "import java.util.Map;\nimport java.util.Date;\nimport java.util.List;\n"
        "import org.apache.dolphinscheduler.common.utils.DateUtils;",
    )
    head, _, tail = source.rpartition("}")
    implementation.write_text(head + body + "}" + tail, encoding="utf-8")
    if not legacy:
        source = service.read_text(encoding="utf-8").replace(
            "Map<String, Object> insertSchedule();",
            "Map<String, Object> insertSchedule();\n"
            "    Map<String, Object> previewSchedule();",
        )
        service.write_text(source, encoding="utf-8")
    date_utils = root / (
        "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
        "common/utils/DateUtils.java"
    )
    date_utils.parent.mkdir(parents=True, exist_ok=True)
    date_utils.write_text(
        """
package org.apache.dolphinscheduler.common.utils;
import java.util.Date;
public class DateUtils {
    public static String dateToString(Date date) { return "formatted"; }
}
""",
        encoding="utf-8",
    )


def _write_legacy_schedule_create_evidence(root: Path) -> None:
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/SchedulerService.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
        """
package org.apache.dolphinscheduler.api.service;

import java.util.Map;

public class SchedulerService {
    public Map<String, Object> insertSchedule() {
        Map<String, Object> result = null;
        result.put("scheduleId", 1);
        return result;
    }
}
""",
        encoding="utf-8",
    )
    base_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/BaseController.java"
    )
    base_path.write_text(
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;

public class BaseController {
    public Result returnDataList(Map<String, Object> result) {
        Object datalist = result.get(Constants.DATA_LIST);
        return success(null, datalist);
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
        encoding="utf-8",
    )


def _write_modern_schedule_create_evidence(root: Path) -> None:
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/SchedulerService.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
        """
package org.apache.dolphinscheduler.api.service;

import java.util.Map;

public interface SchedulerService {
    Map<String, Object> insertSchedule();
}
""",
        encoding="utf-8",
    )
    implementation_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/impl/SchedulerServiceImpl.java"
    )
    implementation_path.parent.mkdir(parents=True, exist_ok=True)
    implementation_path.write_text(
        """
package org.apache.dolphinscheduler.api.service.impl;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.api.service.SchedulerService;

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
}
""",
        encoding="utf-8",
    )
    result_type_path = implementation_path.with_name(
        "SchedulerServiceImpl_insertSchedule_result.java"
    )
    result_type_path.write_text(
        """
package org.apache.dolphinscheduler.api.service.impl;

public class SchedulerServiceImpl_insertSchedule_result {
}
""",
        encoding="utf-8",
    )
    mapper_path = implementation_path.with_name("ScheduleMapper.java")
    mapper_path.write_text(
        """
package org.apache.dolphinscheduler.api.service.impl;

public interface ScheduleMapper {
    SchedulerServiceImpl_insertSchedule_result selectById(int id);
}
""",
        encoding="utf-8",
    )
    schedule_path = implementation_path.with_name("Schedule.java")
    schedule_path.write_text(
        """
package org.apache.dolphinscheduler.api.service.impl;

public class Schedule {
    public int getId() {
        return 1;
    }
}
""",
        encoding="utf-8",
    )
    base_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/BaseController.java"
    )
    base_path.write_text(
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;

public class BaseController {
    public Result returnDataList(Map<String, Object> result) {
        Object datalist = result.get(Constants.DATA_LIST);
        return success(null, datalist);
    }
}
""",
        encoding="utf-8",
    )


def _write_task_update_source(
    root: Path,
    *,
    raw_return_type: str,
    method_name: str = "updateTaskWithUpstream",
    method_body: str = "return Result.success(code);",
) -> None:
    controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/TaskDefinitionController.java"
    )
    controller_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.controller;

@RequestMapping("/projects/{{projectCode}}/task-definition")
public class TaskDefinitionController {{
    @PutMapping("/{{code}}/with-upstream")
    public {raw_return_type} {method_name}(long code) {{
        {method_body}
    }}
}}
""",
        encoding="utf-8",
    )


def _write_task_delete_service_source(
    root: Path,
    *,
    null_success_branch: bool = True,
    null_success_writes_payload: bool = False,
) -> None:
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/impl/TaskDefinitionServiceImpl.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    success_statements = []
    if null_success_branch:
        success_statements.append("putMsg(result, Status.SUCCESS);")
    if null_success_writes_payload or not null_success_branch:
        success_statements.append("result.put(Constants.DATA_LIST, processDefinition);")
    success_statement = "\n            ".join(success_statements)
    service_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.service.impl;

public class TaskDefinitionServiceImpl {{
    public Map<String, Object> deleteTaskDefinitionByCode(
            User loginUser, long projectCode, long taskCode) {{
        Map<String, Object> result = new HashMap<>();
        List<ProcessTaskRelation> taskRelationList =
                processTaskRelationMapper.queryUpstreamByCode(projectCode, taskCode);
        if (!taskRelationList.isEmpty()) {{
            long processDefinitionCode =
                    taskRelationList.get(0).getProcessDefinitionCode();
            updateDag(
                    loginUser, result, processDefinitionCode,
                    relationList, taskDefinitions);
        }} else {{
            {success_statement}
        }}
        return result;
    }}
}}
""",
        encoding="utf-8",
    )


def _write_legacy_process_definition_source(
    root: Path,
    *,
    controller_return_type: str = "Result",
    records_type: str = "IPage<ProcessDefinition>",
    duplicate_service_method: bool = False,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "pom.xml").write_text(
        "<project><version>1.3.9</version></project>\n",
        encoding="utf-8",
    )
    schedule_controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/SchedulerController.java"
    )
    if not schedule_controller_path.exists():
        schedule_controller_path.parent.mkdir(parents=True, exist_ok=True)
        schedule_controller_path.write_text(
            """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.SchedulerService;

@RequestMapping("/schedule")
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
            encoding="utf-8",
        )
    _write_legacy_schedule_create_evidence(root)
    _write_schedule_preview_evidence(root, legacy=True)
    controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/ProcessDefinitionController.java"
    )
    controller_path.parent.mkdir(parents=True, exist_ok=True)
    controller_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

@RequestMapping("projects/{{projectName}}/process")
public class ProcessDefinitionController {{
    private ProcessDefinitionService processDefinitionService;

    @GetMapping("/list-paging")
    public {controller_return_type} queryProcessDefinitionListPaging() {{
        Map<String, Object> result =
                processDefinitionService.queryProcessDefinitionListPaging(
                        null, null, null, null, null, null);
        return returnDataListPaging(result);
    }}
}}
""",
        encoding="utf-8",
    )
    duplicate = ""
    if duplicate_service_method:
        duplicate = """
    public Map<String, Object> queryProcessDefinitionListPaging() {
        return null;
    }
"""
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/ProcessDefinitionService.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.service;

import java.util.Map;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

public class ProcessDefinitionService {{
    public Map<String, Object> queryProcessDefinitionListPaging(
            User loginUser,
            String projectName,
            String searchVal,
            Integer pageNo,
            Integer pageSize,
            Integer userId) {{
        {records_type} processDefinitionIPage =
                processDefineMapper.queryDefineListPaging(null, null, 0, 0, false);
        PageInfo pageInfo = new PageInfo<ProcessData>(pageNo, pageSize);
        pageInfo.setLists(processDefinitionIPage.getRecords());
        Map<String, Object> result = null;
        result.put(Constants.DATA_LIST, pageInfo);
        return result;
    }}
{duplicate}
}}
""",
        encoding="utf-8",
    )
    _write_legacy_contract_type_sources(root)
    _write_legacy_current_user_source(root)
    _write_legacy_datasource_page_source(root)


def _write_legacy_datasource_page_source(root: Path) -> None:
    controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/DataSourceController.java"
    )
    controller_path.parent.mkdir(parents=True, exist_ok=True)
    controller_path.write_text(
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.DataSourceService;
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
        encoding="utf-8",
    )
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/DataSourceService.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
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
        encoding="utf-8",
    )


def _write_legacy_current_user_source(
    root: Path,
    *,
    data_list_value: str = "user",
) -> None:
    controller_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller/UsersController.java"
    )
    controller_path.parent.mkdir(parents=True, exist_ok=True)
    controller_path.write_text(
        """
package org.apache.dolphinscheduler.api.controller;

import java.util.Map;
import org.apache.dolphinscheduler.api.service.UsersService;
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
        encoding="utf-8",
    )
    service_path = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/UsersService.java"
    )
    service_path.parent.mkdir(parents=True, exist_ok=True)
    service_path.write_text(
        f"""
package org.apache.dolphinscheduler.api.service;

import java.util.Map;
import org.apache.dolphinscheduler.dao.entity.User;

public class UsersService {{
    public Map<String, Object> getUserInfo(User loginUser) {{
        Map<String, Object> result = null;
        User user = loginUser;
        result.put(Constants.DATA_LIST, {data_list_value});
        return result;
    }}
}}
""",
        encoding="utf-8",
    )


def _write_entity_source(root: Path, name: str) -> None:
    path = (
        root / "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/dao/"
        f"entity/{name}.java"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""
package org.apache.dolphinscheduler.dao.entity;

public class {name} {{}}
""",
        encoding="utf-8",
    )


def _write_legacy_contract_type_sources(root: Path) -> None:
    page_info = (
        root / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "utils/PageInfo.java"
    )
    page_info.parent.mkdir(parents=True, exist_ok=True)
    page_info.write_text(
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
        encoding="utf-8",
    )
    constants = (
        root / "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
        "common/Constants.java"
    )
    constants.parent.mkdir(parents=True, exist_ok=True)
    constants.write_text(
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
        encoding="utf-8",
    )
    for name in ("DataSource", "ProcessDefinition", "User"):
        _write_entity_source(root, name)


@pytest.mark.parametrize(
    ("version", "raw_return_type", "method_body", "expected_logical_type"),
    [
        (
            "1.3.9",
            "Result",
            (
                "Map<String, Object> result = schedulerService.insertSchedule(); "
                "return returnDataList(result);"
            ),
            "Void",
        ),
        (
            "3.4.1",
            "Result",
            _MODERN_SCHEDULE_CREATE_BODY,
            "org.apache.dolphinscheduler.dao.entity.Schedule",
        ),
        (
            "3.4.2",
            "Result<Schedule>",
            "Schedule created = null; return Result.success(created);",
            "org.apache.dolphinscheduler.dao.entity.Schedule",
        ),
    ],
)
def test_source_snapshot_applies_schedule_override_only_to_matching_release(
    tmp_path: Path,
    version: str,
    raw_return_type: str,
    method_body: str,
    expected_logical_type: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / f"dolphinscheduler-{version}"
    _write_ds_source(
        source_root,
        version=version,
        raw_return_type=raw_return_type,
        method_body=method_body,
    )

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    create_schedule = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.createSchedule"
    )

    assert create_schedule.logical_return_type == expected_logical_type


@pytest.mark.parametrize(
    ("version", "raw_return_type", "inferred_type"),
    [
        ("1.3.9", "Result", "Stream<String>"),
        ("2.0.0", "Result", "Void"),
        ("2.0.9", "Result", "Void"),
        ("2.0.4", "Result", "Void"),
        ("3.4.2", "Result<List<String>>", "List<String>"),
    ],
)
def test_schedule_preview_projects_mapped_dates_as_string_list(
    tmp_path: Path,
    version: str,
    raw_return_type: str,
    inferred_type: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / f"dolphinscheduler-{version}"
    _write_ds_source(
        source_root,
        version=version,
        raw_return_type=raw_return_type,
        method_name="previewSchedule",
        method_body=(
            "List<Date> dates = null; return Result.success(dates.stream()"
            '.map(date -> "formatted").collect(Collectors.toList()));'
            if version == "3.4.2"
            else "Map<String, Object> result = schedulerService.previewSchedule(); "
            "return returnDataList(result);"
        ),
    )

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    preview_schedule = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.previewSchedule"
    )

    assert preview_schedule.inferred_return_type == inferred_type
    assert preview_schedule.logical_return_type == "List<String>"


def test_source_snapshot_models_341_task_update_result_payload_as_nullable(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-3.4.1"
    _write_ds_source(
        source_root,
        version="3.4.1",
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
    )

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    task_update = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskDefinitionController.updateTaskWithUpstream"
    )

    assert task_update.inferred_return_type == "long"
    assert task_update.logical_return_type == "Optional<long>"


@pytest.mark.parametrize("version", ["2.0.9", "3.0.0", "3.0.6", "3.1.0"])
def test_task_delete_result_payload_override_is_nullable(
    tmp_path: Path,
    version: str,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope(version)
    _write_task_delete_service_source(tmp_path / "references/dolphinscheduler")

    logical_type = scope.resolve(
        operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
        repo_root=tmp_path,
        raw_return_type="Result",
        inferred_return_type="ProcessDefinition",
        import_map={},
    )
    scope.validate(controller_names={"TaskDefinitionController"})

    assert logical_type == "Optional<ProcessDefinition>"


@pytest.mark.parametrize("version", ["2.0.9", "2.0.8"])
def test_task_delete_result_payload_override_rejects_missing_null_branch(
    tmp_path: Path,
    version: str,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope(version)
    _write_task_delete_service_source(
        tmp_path / "references/dolphinscheduler",
        null_success_branch=False,
    )

    with pytest.raises(
        RuntimeError,
        match=r"effective response override source evidence no longer matches",
    ):
        scope.resolve(
            operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="ProcessDefinition",
            import_map={},
        )


def test_task_delete_result_payload_override_rejects_status_plus_payload_branch(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.9")
    _write_task_delete_service_source(
        tmp_path / "references/dolphinscheduler",
        null_success_writes_payload=True,
    )

    with pytest.raises(
        RuntimeError,
        match=r"effective response override source evidence no longer matches",
    ):
        scope.resolve(
            operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="ProcessDefinition",
            import_map={},
        )


def test_task_delete_result_payload_override_fails_closed_on_inference_drift(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.9")

    with pytest.raises(
        RuntimeError,
        match=r"deleteTaskDefinitionByCode return-type override no longer matches",
    ):
        scope.resolve(
            operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
            repo_root=tmp_path,
            raw_return_type="Result<Void>",
            inferred_return_type=None,
            import_map={},
        )


def test_source_snapshot_keeps_342_task_update_result_payload_required(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-3.4.2"
    _write_ds_source(
        source_root,
        version="3.4.2",
        raw_return_type="Result<Schedule>",
        method_body="Schedule created = null; return Result.success(created);",
    )
    _write_task_update_source(
        source_root,
        raw_return_type="Result<Long>",
        method_body=(
            "Long updatedTaskCode = code; return Result.success(updatedTaskCode);"
        ),
    )

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    task_update = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "TaskDefinitionController.updateTaskWithUpstream"
    )

    assert task_update.logical_return_type == "Long"


def test_139_process_definition_page_uses_effective_record_type(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(source_root)

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id
        == "ProcessDefinitionController.queryProcessDefinitionListPaging"
    )

    assert operation.return_type == "Result"
    assert operation.inferred_return_type == (
        "org.apache.dolphinscheduler.api.utils.PageInfo<ProcessData>"
    )
    assert operation.logical_return_type == (
        "PageInfo<org.apache.dolphinscheduler.dao.entity.ProcessDefinition>"
    )


def test_139_current_user_uses_effective_data_list_type(tmp_path: Path) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(source_root)

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    operation = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "UsersController.getUserInfo"
    )

    assert operation.return_type == "Result"
    assert operation.inferred_return_type == (
        "org.apache.dolphinscheduler.dao.entity.User"
    )
    assert operation.logical_return_type == (
        "org.apache.dolphinscheduler.dao.entity.User"
    )


def test_139_current_user_override_rejects_data_list_drift(tmp_path: Path) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(source_root)
    _write_legacy_current_user_source(source_root, data_list_value="loginUser")

    with pytest.raises(
        RuntimeError,
        match=r"effective response override source evidence no longer matches",
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


def test_139_process_definition_page_override_rejects_record_type_drift(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(
        source_root,
        records_type="IPage<ProcessData>",
    )

    with pytest.raises(
        RuntimeError,
        match=r"effective response override source evidence no longer matches",
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


def test_139_process_definition_page_override_rejects_raw_return_type_drift(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(
        source_root,
        controller_return_type="Result<PageInfo<ProcessData>>",
    )

    with pytest.raises(
        RuntimeError,
        match=r"return-type override no longer matches",
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


def test_139_process_definition_page_override_requires_unique_service_method(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-1.3.9"
    _write_legacy_process_definition_source(
        source_root,
        duplicate_service_method=True,
    )

    with pytest.raises(
        RuntimeError,
        match=r"expected exactly one service method, found 2",
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


def test_341_task_update_override_fails_closed_when_source_shape_drifts(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler-3.4.1"
    _write_ds_source(
        source_root,
        version="3.4.1",
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
        task_raw_return_type="Result<Long>",
    )

    with pytest.raises(
        RuntimeError,
        match=r"updateTaskWithUpstream return-type override no longer matches",
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


def test_contract_snapshot_fails_closed_when_scoped_override_shape_drifts(
    tmp_path: Path,
) -> None:
    pipeline = _load_module("ds_codegen.extract.pipeline")
    source_root = tmp_path / "repo/references/dolphinscheduler"
    _write_ds_source(
        source_root,
        version="3.4.1",
        raw_return_type="Result<Schedule>",
        method_body="Schedule created = null; return Result.success(created);",
    )

    with pytest.raises(RuntimeError, match="return-type override no longer matches"):
        pipeline.build_contract_snapshot(tmp_path / "repo")


def test_contract_snapshot_fails_closed_when_scoped_override_operation_is_missing(
    tmp_path: Path,
) -> None:
    pipeline = _load_module("ds_codegen.extract.pipeline")
    source_root = tmp_path / "repo/references/dolphinscheduler"
    _write_ds_source(
        source_root,
        version="3.4.1",
        raw_return_type="Result",
        method_name="renamedCreateSchedule",
        method_body=(
            "SchedulerServiceImpl_insertSchedule_result created = null; "
            "return Result.success(created);"
        ),
    )

    with pytest.raises(
        RuntimeError,
        match=(
            r"SchedulerController\.createSchedule return-type override expected "
            r"one match.*found 0"
        ),
    ):
        pipeline.build_contract_snapshot(tmp_path / "repo")


def test_contract_snapshot_fails_closed_when_two_controller_fqns_match_override(
    tmp_path: Path,
) -> None:
    pipeline = _load_module("ds_codegen.extract.pipeline")
    source_root = tmp_path / "repo/references/dolphinscheduler"
    _write_ds_source(
        source_root,
        version="3.4.1",
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
    )
    controller_dir = (
        source_root
        / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "controller"
    )
    controller_source = (controller_dir / "SchedulerController.java").read_text(
        encoding="utf-8"
    )
    duplicate_controller = controller_dir / "v2/SchedulerController.java"
    duplicate_controller.parent.mkdir()
    duplicate_controller.write_text(
        controller_source.replace(
            "package org.apache.dolphinscheduler.api.controller;",
            "package org.apache.dolphinscheduler.api.controller.v2;",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        RuntimeError,
        match=r"return-type override expected one match.*found 2",
    ):
        pipeline.build_contract_snapshot(tmp_path / "repo")


@pytest.mark.parametrize("version", ["2.0.10", "3.0.3", "3.1.10", "99.0.0"])
def test_candidate_schedule_correction_preserves_evidence_and_own_model(
    tmp_path: Path,
    version: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    reviews = _load_module("ds_codegen.extract.return_type_reviews")
    source_root = tmp_path / "candidate"
    _write_ds_source(
        source_root,
        version=version,
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
    )
    schedule = source_root / (
        "dolphinscheduler-dao/src/main/java/org/apache/dolphinscheduler/dao/"
        "entity/Schedule.java"
    )
    schedule.write_text(
        "package org.apache.dolphinscheduler.dao.entity; "
        "public class Schedule { private String candidateField; }",
        encoding="utf-8",
    )
    reviewed_before = reviews.REVIEWED_RETURN_TYPE_RULES

    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    operation = next(
        op for op in snapshot.operations if op.method_name == "createSchedule"
    )
    model = next(model for model in snapshot.models if model.name == "Schedule")

    assert snapshot.ds_version == version
    assert operation.return_type == "Result"
    assert operation.inferred_return_type == (
        "org.apache.dolphinscheduler.api.service.impl."
        "SchedulerServiceImpl_insertSchedule_result"
    )
    assert (
        operation.logical_return_type
        == "org.apache.dolphinscheduler.dao.entity.Schedule"
    )
    assert [field.name for field in model.fields] == ["candidateField"]
    assert reviewed_before == reviews.REVIEWED_RETURN_TYPE_RULES
    assert not any(version in review.versions for review in reviewed_before)


@pytest.mark.parametrize(
    "version",
    [
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
    ],
)
def test_reviewed_20x_schedule_create_data_list_resolves_schedule(
    tmp_path: Path,
    version: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "dolphinscheduler"
    _write_ds_source(
        source_root,
        version=version,
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
    )
    snapshot = version_diff.build_snapshot_from_ds_source(source_root)
    create_schedule = next(
        operation
        for operation in snapshot.operations
        if operation.operation_id == "SchedulerController.createSchedule"
    )

    assert create_schedule.inferred_return_type.endswith(
        "SchedulerServiceImpl_insertSchedule_result"
    )
    assert create_schedule.logical_return_type.endswith("Schedule")


def test_candidate_schedule_correction_requires_actual_mapper_readback(
    tmp_path: Path,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "candidate"
    _write_ds_source(
        source_root,
        version="3.1.4",
        raw_return_type="Result",
        method_body=_MODERN_SCHEDULE_CREATE_BODY,
    )
    implementation = source_root / (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/impl/SchedulerServiceImpl.java"
    )
    implementation.write_text(
        implementation.read_text(encoding="utf-8").replace(
            "selectById(scheduleObj.getId())", "selectById(1)"
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        RuntimeError, match="readback is no longer keyed by the created Schedule"
    ):
        version_diff.build_snapshot_from_ds_source(source_root)


@pytest.mark.parametrize("version", ["1.3.9", "2.0.9", "2.0.4"])
@pytest.mark.parametrize("drift", ["mapping", "formatter", "envelope"])
def test_schedule_preview_requires_actual_string_payload_source(
    tmp_path: Path,
    version: str,
    drift: str,
) -> None:
    version_diff = _load_module("ds_codegen.version_diff")
    source_root = tmp_path / "candidate"
    _write_ds_source(
        source_root,
        version=version,
        raw_return_type="Result",
        method_name="previewSchedule",
        method_body=(
            "Map<String, Object> result = schedulerService.previewSchedule(); "
            "return returnDataList(result);"
        ),
    )
    if drift == "formatter":
        path = source_root / (
            "dolphinscheduler-common/src/main/java/org/apache/dolphinscheduler/"
            "common/utils/DateUtils.java"
        )
        source = path.read_text(encoding="utf-8").replace(
            'String dateToString(Date date) { return "formatted"; }',
            "Date dateToString(Date date) { return date; }",
        )
    elif drift == "envelope":
        path = source_root / (
            "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
            "controller/BaseController.java"
        )
        source = path.read_text(encoding="utf-8").replace(
            "return success(null, datalist)", "return success(null, result)"
        )
    else:
        implementation = (
            "SchedulerService.java"
            if version == "1.3.9"
            else "impl/SchedulerServiceImpl.java"
        )
        path = source_root / (
            "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
            f"service/{implementation}"
        )
        source = (
            path.read_text(encoding="utf-8")
            .replace(".map(DateUtils::dateToString)", "")
            .replace(".map(date -> DateUtils.dateToString(date))", "")
        )
    path.write_text(source, encoding="utf-8")

    # Exercise the independent structural proof even if improved inference
    # rejects the drift before reaching that proof during full extraction.
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    with pytest.raises(RuntimeError, match="source evidence no longer matches"):
        resolution.LogicalReturnTypeOverrideScope(version).resolve(
            operation_id="SchedulerController.previewSchedule",
            repo_root=source_root,
            raw_return_type="Result",
            inferred_return_type="Stream<String>" if version == "1.3.9" else "Void",
            import_map={
                "SchedulerService": (
                    "org.apache.dolphinscheduler.api.service.SchedulerService"
                )
            },
        )

    expected_error = (
        "return-type override no longer matches"
        if drift == "mapping" or (version == "1.3.9" and drift == "formatter")
        else "source evidence no longer matches"
    )
    with pytest.raises(RuntimeError, match=expected_error):
        version_diff.build_snapshot_from_ds_source(source_root)


@pytest.mark.parametrize("version", ["2.0.10", "3.0.3", "3.1.1"])
def test_candidate_task_delete_reuses_nullable_payload_proof(
    tmp_path: Path,
    version: str,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope(version)
    _write_task_delete_service_source(tmp_path / "references/dolphinscheduler")

    assert (
        scope.resolve(
            operation_id="TaskDefinitionController.deleteTaskDefinitionByCode",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="ProcessDefinition",
            import_map={},
        )
        == "Optional<ProcessDefinition>"
    )
    scope.validate(controller_names={"TaskDefinitionController"})


def test_candidate_correction_rejects_ambiguous_named_rules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    rule = next(
        rule
        for rule in resolution.RETURN_TYPE_RULES
        if rule.name == "schedule_create_data_list"
    )
    monkeypatch.setattr(
        resolution,
        "RETURN_TYPE_RULES",
        (*resolution.RETURN_TYPE_RULES, replace(rule, name="ambiguous_create")),
    )
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.10")
    with pytest.raises(RuntimeError, match=r"ambiguous return-type rules.*2\.0\.10"):
        scope.resolve(
            operation_id="SchedulerController.createSchedule",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="SchedulerServiceImpl_insertSchedule_result",
            import_map={},
        )


@pytest.mark.parametrize(
    ("operation_id", "raw", "inferred"),
    [
        ("TaskDefinitionController.updateTaskWithUpstream", "Result", "long"),
        (
            "ProjectPreferenceController.queryProjectPreferenceByProjectCode",
            "Result",
            "ProjectPreference",
        ),
        (
            "ProjectWorkerGroupController.queryAssignedWorkerGroups",
            "Map<String, Object>",
            "ProjectWorkerGroupController_queryAssignedWorkerGroups_result",
        ),
        ("SchedulerController.createSchedule", "Result", "UnreviewedPayload"),
        ("UsersController.getUserInfo", "Result", "User"),
    ],
)
def test_unproven_candidate_shape_keeps_original_inference(
    tmp_path: Path,
    operation_id: str,
    raw: str,
    inferred: str,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("99.0.0")
    logical = resolution.resolve_operation_logical_return_type(
        override_scope=scope,
        operation_id=operation_id,
        repo_root=tmp_path,
        raw_return_type=raw,
        inferred_return_type=inferred,
        import_map={},
        package_name=None,
    )
    assert logical == inferred
    scope.validate(controller_names={operation_id.partition(".")[0]})


@pytest.mark.parametrize(
    ("raw", "inferred", "expected"),
    [
        ("Result<List<String>>", "Stream<ZonedDateTime>", "List<String>"),
        (
            "Result<List<DataSourceSimpleInfoVO>>",
            "Stream<DataSource>",
            "List<DataSourceSimpleInfoVO>",
        ),
        ("Result<List<Object>>", "List<User>", "List<User>"),
        ("Result", "User", "User"),
    ],
)
def test_concrete_controller_payload_precedes_partial_body_inference(
    tmp_path: Path, raw: str, inferred: str, expected: str
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    logical = resolution.resolve_operation_logical_return_type(
        override_scope=resolution.LogicalReturnTypeOverrideScope("99.0.0"),
        operation_id="ExampleController.list",
        repo_root=tmp_path,
        raw_return_type=raw,
        inferred_return_type=inferred,
        import_map={},
        package_name=None,
    )
    assert logical == expected


def test_reviewed_resource_raw_shape_cannot_use_other_rule_input_epoch(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.0")
    with pytest.raises(RuntimeError, match="return-type override no longer matches"):
        scope.resolve(
            operation_id="ResourcesController.queryResource",
            repo_root=tmp_path,
            raw_return_type="Result<Object>",
            inferred_return_type="Resource",
            import_map={},
        )


def _write_candidate_resource_page_source(
    root: Path, *, ambiguous: bool = False
) -> None:
    _write_entity_source(root, "Resource")
    service = root / (
        "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
        "service/impl/ResourcesServiceImpl.java"
    )
    service.parent.mkdir(parents=True, exist_ok=True)
    service.write_text(
        """
package org.apache.dolphinscheduler.api.service.impl;
import org.apache.dolphinscheduler.dao.entity.*;
import candidate.other.*;
public class ResourcesServiceImpl {
    public Result queryResourceListPaging() {
        IPage<Resource> page = null;
        PageInfo<Resource> info = null;
        Result result = null;
        info.setTotalList(page.getRecords());
        result.setData(info);
        return result;
    }
}
""",
        encoding="utf-8",
    )
    if ambiguous:
        other = (
            root / "dolphinscheduler-api/src/main/java/candidate/other/Resource.java"
        )
        other.parent.mkdir(parents=True)
        other.write_text(
            "package candidate.other; public class Resource {}", encoding="utf-8"
        )


def test_candidate_resource_identity_resolves_real_wildcard_source(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    java_source = _load_module("ds_codegen.java_source")
    _write_candidate_resource_page_source(tmp_path / "references/dolphinscheduler")
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.10")
    operation = "ResourcesController.queryResourceListPaging"
    imports = {"Resource": "org.springframework.core.io.Resource"}
    assert (
        scope.resolve(
            operation_id=operation,
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="PageInfo<org.springframework.core.io.Resource>",
            import_map=imports,
        )
        == "PageInfo<Resource>"
    )
    assert (
        scope.resolve_type_import_path(
            tmp_path,
            operation_id=operation,
            type_name="Resource",
            import_map=imports,
            package_name="org.apache.dolphinscheduler.api.controller",
            owner_import_path="org.apache.dolphinscheduler.api.controller.ResourcesController",
            default_resolver=java_source.resolve_referenced_import_path,
        )
        == "org.apache.dolphinscheduler.dao.entity.Resource"
    )
    scope.validate(controller_names={"ResourcesController"})
    # A mechanically resolved candidate is not added to the exact import review.
    assert (
        resolution.operation_type_import_override("2.0.10", operation, "Resource")
        is None
    )


def test_candidate_resource_identity_rejects_ambiguous_wildcard_source(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    _write_candidate_resource_page_source(
        tmp_path / "references/dolphinscheduler", ambiguous=True
    )
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.10")
    with pytest.raises(ValueError, match="ambiguous on-demand imports"):
        scope.resolve(
            operation_id="ResourcesController.queryResourceListPaging",
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type="PageInfo<Resource>",
            import_map={"Resource": "org.springframework.core.io.Resource"},
        )


def test_candidate_resource_import_is_required_after_source_correction(
    tmp_path: Path,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    _write_candidate_resource_page_source(tmp_path / "references/dolphinscheduler")
    scope = resolution.LogicalReturnTypeOverrideScope("2.0.10")
    scope.resolve(
        operation_id="ResourcesController.queryResourceListPaging",
        repo_root=tmp_path,
        raw_return_type="Result",
        inferred_return_type="PageInfo<Resource>",
        import_map={"Resource": "org.springframework.core.io.Resource"},
    )
    with pytest.raises(
        RuntimeError, match=r"type-import override expected one.*found 0"
    ):
        scope.validate(controller_names={"ResourcesController"})


@pytest.mark.parametrize(
    ("version", "operation_id", "inferred"),
    [
        ("1.3.9", "ResourcesController.queryResource", "Resource"),
        ("1.3.9", "ResourcesController.queryResourceListPaging", "PageInfo<Resource>"),
        (
            "3.1.9",
            "TaskDefinitionController.deleteTaskDefinitionByCode",
            "ProcessDefinition",
        ),
    ],
)
def test_reviewed_release_cannot_acquire_rule_outside_exact_membership(
    tmp_path: Path,
    version: str,
    operation_id: str,
    inferred: str,
) -> None:
    resolution = _load_module("ds_codegen.extract.return_type_resolution")
    scope = resolution.LogicalReturnTypeOverrideScope(version)
    assert (
        scope.resolve(
            operation_id=operation_id,
            repo_root=tmp_path,
            raw_return_type="Result",
            inferred_return_type=inferred,
            import_map={},
        )
        is None
    )
