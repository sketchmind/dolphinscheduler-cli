import json

import pytest
import yaml

from dsctl.errors import UserInputError
from dsctl.models import WorkflowSpec
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.services._workflow import compile as workflow_compile_service
from dsctl.services._workflow.patch import apply_workflow_patch
from dsctl.services.template import supported_task_template_types, task_template_result
from dsctl.upstream.task_parameter_projection import TaskResourceRefIndex

_PATCH_FILE_REFS = TaskResourceRefIndex.from_resolved_files(
    ["/jobs/patched-orders.jar", "/jobs/patched-mapreduce.jar"],
    id_by_full_name={},
    wire_full_name_by_full_name={
        "/jobs/patched-orders.jar": "/tenant/resources/jobs/patched-orders.jar",
        "/jobs/patched-mapreduce.jar": "/tenant/resources/jobs/patched-mapreduce.jar",
    },
)


def _compilable_workflow_document(
    task_document: dict[str, object],
) -> dict[str, object]:
    task_type = task_document["type"]
    tasks: list[dict[str, object]] = [task_document]
    if task_type == "SWITCH":
        tasks.extend(
            [
                {
                    "name": "task-a",
                    "type": "SHELL",
                    "command": "echo A",
                },
                {
                    "name": "task-b",
                    "type": "SHELL",
                    "command": "echo B",
                },
                {
                    "name": "task-default",
                    "type": "SHELL",
                    "command": "echo default",
                },
            ]
        )
    if task_type == "CONDITIONS":
        tasks.extend(
            [
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
                {
                    "name": "upstream-task",
                    "type": "SHELL",
                    "command": "echo upstream",
                },
            ]
        )
    return {
        "workflow": {"name": "patched-workflow"},
        "tasks": tasks,
    }


def _task_patch_set(
    task_type: str,
) -> dict[str, object]:
    if task_type in {"SHELL", "PYTHON"}:
        commands = {
            "SHELL": "echo patched shell",
            "PYTHON": 'print("patched python")',
        }
        return {"command": commands[task_type]}
    if task_type == "REMOTESHELL":
        return {
            "task_params": {
                "rawScript": "echo patched remote",
                "type": "SSH",
                "datasource": 2,
            }
        }
    if task_type == "PROCEDURE":
        return {
            "task_params": {
                "type": "MYSQL",
                "datasource": 2,
                "method": "{call reporting.refresh_daily(?)}",
                "localParams": [
                    {
                        "prop": "bizdate",
                        "direct": "IN",
                        "type": "VARCHAR",
                        "value": "${system.biz.date}",
                    }
                ],
                "varPool": [],
            }
        }
    if task_type == "SQL":
        return {
            "task_params": {
                "type": "MYSQL",
                "datasource": 2,
                "sql": "select 2;",
                "sqlType": 0,
                "sendEmail": False,
                "displayRows": 20,
                "showType": "TABLE",
                "connParams": "",
                "preStatements": [],
                "postStatements": [],
                "groupId": 0,
                "title": "",
                "limit": 0,
                "localParams": [],
                "varPool": [],
            }
        }
    if task_type == "HTTP":
        return {
            "task_params": {
                "url": "https://example.test/patched",
                "httpMethod": "GET",
                "httpParams": [],
                "httpBody": "",
                "httpCheckCondition": "STATUS_CODE_DEFAULT",
                "condition": "",
                "connectTimeout": 5000,
            }
        }
    if task_type == "SUB_WORKFLOW":
        return {
            "task_params": {
                "workflowDefinitionCode": 1000000000002,
            }
        }
    if task_type == "DEPENDENT":
        return {
            "task_params": {
                "dependence": {
                    "relation": "AND",
                    "checkInterval": 20,
                    "failurePolicy": "DEPENDENT_FAILURE_FAILURE",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 1,
                                    "definitionCode": 1000000000002,
                                    "depTaskCode": 0,
                                    "cycle": "day",
                                    "dateValue": "last7Days",
                                    "parameterPassing": False,
                                }
                            ],
                        }
                    ],
                }
            }
        }
    if task_type == "SWITCH":
        return {
            "task_params": {
                "switchResult": {
                    "dependTaskList": [
                        {
                            "condition": '${route} == "patched-A"',
                            "nextNode": "task-a",
                        },
                        {
                            "condition": '${route} == "patched-B"',
                            "nextNode": "task-b",
                        },
                    ],
                    "nextNode": "task-default",
                }
            }
        }
    if task_type == "CONDITIONS":
        return {
            "task_params": {
                "dependence": {
                    "relation": "OR",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "task": "upstream-task",
                                    "status": "FAILURE",
                                }
                            ],
                        }
                    ],
                },
                "conditionResult": {
                    "successNode": ["on-success"],
                    "failedNode": ["on-failed"],
                },
            }
        }
    emr_params = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": (
            '{"Name":"patched-emr-job","ReleaseLabel":"emr-6.15.0",'
            '"Instances":{"KeepJobFlowAliveWhenNoSteps":true}}'
        ),
        "localParams": [],
    }
    hive_cli_params = {
        "hiveCliTaskExecutionType": "SCRIPT",
        "hiveSqlScript": "SELECT 'patched';",
        "hiveCliOptions": "--silent",
        "localParams": [],
    }
    dvc_params = {
        "dvcTaskType": "Upload",
        "dvcRepository": "git@github.com:example-org/dvc-data.git",
        "dvcDataLocation": "datasets/orders-v2",
        "dvcLoadSaveDataPath": "~/datasets/orders-v2",
        "dvcVersion": "orders_v2.0.0",
        "dvcMessage": "Publish patched orders dataset v2.0.0",
    }
    mlflow_params = {
        "mlflowTaskType": "MLflow Models",
        "deployType": "MLFLOW",
        "mlflowTrackingUri": "https://mlflow.example.com/api",
        "deployModelKey": "models:/patched-orders/2",
        "deployPort": "7001",
    }
    openmldb_params = {
        "zk": "zk-patched-1.example.com:2181,zk-patched-2.example.com:2181",
        "zkPath": "/patched/openmldb",
        "executeMode": "online",
        "sql": "SELECT 2 AS patched",
    }
    jupyter_params = {
        "condaEnvName": "analytics-py310",
        "inputNotePath": "/opt/notebooks/patched-input.ipynb",
        "outputNotePath": "/opt/notebooks/patched-output.ipynb",
        "parameters": {"business_date": "2026-08-20"},
        "kernel": "python3",
        "engine": "nbclient",
        "executionTimeout": 900,
        "startTimeout": 60,
    }
    java_params = {
        "mainJar": "/jobs/patched-orders.jar",
        "mainArgs": ["--date", "2026-08-21"],
    }
    mr_params = {
        "mainJar": "/jobs/patched-mapreduce.jar",
        "mainClass": "com.example.PatchedMapReduce",
        "mainArgs": ["--date", "2026-08-21"],
    }
    sqoop_params = {
        "subcommand": "export",
        "args": [
            "--connect",
            "jdbc:mysql://db.example.invalid:3306/warehouse",
            "--table",
            "patched_orders",
        ],
    }
    dinky_params = {
        "address": "https://dinky.example.com/api",
        "taskId": "patched-job-1842",
        "online": False,
    }
    dms_params = {
        "isRestartTask": True,
        "isJsonFormat": False,
        "migrationType": "full-load",
        "startReplicationTaskType": "resume-processing",
        "replicationTaskArn": (
            "arn:aws:dms:us-east-1:123456789012:task:PATCHED-DMS-TASK"
        ),
    }
    data_factory_params = {
        "factoryName": "patched-analytics-factory",
        "resourceGroupName": "patched-analytics-rg",
        "pipelineName": "patched-daily-copy",
    }
    datax_params = {
        "json": '{"job":{"content":[],"setting":{"speed":{"channel":2}}}}',
    }
    chunjun_params = {
        "json": '{"job":{"content":[],"setting":{"speed":{"channel":3}}}}',
    }
    datasync_params = {
        "jsonFormat": False,
        "name": "nightly-transfer",
        "sourceLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/patched-source"
        ),
        "destinationLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/patched-destination"
        ),
        "cloudWatchLogGroupArn": (
            "arn:aws:logs:cn-north-1:123456789012:log-group:patched-datasync"
        ),
    }
    sagemaker_params = {
        "sagemakerRequestJson": (
            '{"PipelineName":"patched-training",'
            '"ClientRequestToken":"patched-training-20260824"}'
        ),
        "datasource": 17,
        "localParams": [],
    }
    aliyun_serverless_spark_params = {
        "datasource": 17,
        "workspaceId": "w-patched-analytics",
        "resourceQueueId": "root.patched",
        "jobName": "patched-orders",
        "entryPoint": "oss://analytics-jobs/releases/patched-orders",
        "entryPointArguments": ["--date", "2026-08-21"],
        "sparkSubmitParameters": "--class com.example.PatchedOrders",
        "isProduction": False,
    }
    spark_params = {
        "rawScript": "SELECT 'patched spark sql';",
    }
    flink_params = {
        "rawScript": "SELECT 'patched flink sql';",
    }
    grpc_params = {
        "url": "grpc.example.internal:7443",
        "channelCredentialType": "TLS_DEFAULT",
        "serviceName": "EchoService",
        "methodName": "Echo",
        "requestFields": [
            {"name": "account", "number": 1},
            {"name": "region", "number": 2},
        ],
        "responseFields": [{"name": "result", "number": 1}],
        "message": {"account": "patched", "region": "cn"},
        "grpcConnectTimeoutMs": 10_000,
    }
    k8s_params = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:patched",
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "environment": [],
        "command": [],
        "args": [],
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    zeppelin_params = {
        "noteId": "2FZ4VC2MX",
        "paragraphId": "paragraph-1",
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "parameters": {"run_mode": "patched"},
    }
    seatunnel_params = {
        "rawScript": "env { execution.parallelism = 2 }\n",
    }
    generic_params = {"patched": True, "taskType": task_type}
    typed_params = {
        "ALIYUN_SERVERLESS_SPARK": aliyun_serverless_spark_params,
        "CHUNJUN": chunjun_params,
        "DATASYNC": datasync_params,
        "DATAX": datax_params,
        "DATA_FACTORY": data_factory_params,
        "DINKY": dinky_params,
        "DMS": dms_params,
        "DVC": dvc_params,
        "EMR": emr_params,
        "FLINK": flink_params,
        "GRPC": grpc_params,
        "HIVECLI": hive_cli_params,
        "JAVA": java_params,
        "JUPYTER": jupyter_params,
        "K8S": k8s_params,
        "MLFLOW": mlflow_params,
        "MR": mr_params,
        "OPENMLDB": openmldb_params,
        "SAGEMAKER": sagemaker_params,
        "SEATUNNEL": seatunnel_params,
        "SPARK": spark_params,
        "SQOOP": sqoop_params,
        "ZEPPELIN": zeppelin_params,
    }.get(task_type, generic_params)
    return {"task_params": typed_params}


def _expected_compiled_task_params(
    task_type: str,
    patch_set: dict[str, object],
) -> dict[str, object]:
    if "task_params" not in patch_set:
        command = patch_set["command"]
        assert isinstance(command, str)
        return {
            "rawScript": command,
            "localParams": [],
            "resourceList": [],
        }
    task_params = patch_set["task_params"]
    assert isinstance(task_params, dict)
    if task_type == "ALIYUN_SERVERLESS_SPARK":
        return {
            **task_params,
            "entryPointArguments": "--date#2026-08-21",
            "type": "ALIYUN_SERVERLESS_SPARK",
            "codeType": "JAR",
        }
    if task_type == "DATAX":
        return {
            "customConfig": 1,
            **task_params,
            "xms": 1,
            "xmx": 1,
        }
    if task_type == "PROCEDURE":
        return {
            **task_params,
            "method": "{call reporting.refresh_daily(${bizdate})}",
        }
    if task_type == "ZEPPELIN":
        return {
            "noteId": "2FZ4VC2MX",
            "paragraphId": "paragraph-1",
            "datasource": 17,
            "type": "ZEPPELIN",
            "parameters": '{"run_mode":"patched"}',
        }
    if task_type == "JUPYTER":
        return {
            "condaEnvName": "analytics-py310",
            "inputNotePath": "/opt/notebooks/patched-input.ipynb",
            "outputNotePath": "/opt/notebooks/patched-output.ipynb",
            "parameters": '{"business_date":"2026-08-20"}',
            "kernel": "python3",
            "engine": "nbclient",
            "executionTimeout": "900",
            "startTimeout": "60",
        }
    projected_params: dict[str, dict[str, object]] = {
        "DATASYNC": {
            "jsonFormat": False,
            "name": "nightly-transfer",
            "sourceLocationArn": (
                "arn:aws:datasync:cn-north-1:123456789012:location/patched-source"
            ),
            "destinationLocationArn": (
                "arn:aws:datasync:cn-north-1:123456789012:location/patched-destination"
            ),
            "cloudWatchLogGroupArn": (
                "arn:aws:logs:cn-north-1:123456789012:log-group:patched-datasync"
            ),
        },
        "CHUNJUN": {
            "customConfig": 1,
            "json": '{"job":{"content":[],"setting":{"speed":{"channel":3}}}}',
            "deployMode": "local",
        },
        "SPARK": {
            "programType": "SQL",
            "rawScript": "SELECT 'patched spark sql';",
            "master": "local",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
        },
        "FLINK": {
            "programType": "SQL",
            "deployMode": "local",
            "initScript": "",
            "rawScript": "SELECT 'patched flink sql';",
        },
        "GRPC": {
            "url": "grpc.example.internal:7443",
            "channelCredentialType": "TLS_DEFAULT",
            "grpcServiceDefinition": (
                'syntax = "proto3";\n\n'
                "service EchoService {\n"
                "  rpc Echo (Request) returns (Response);\n"
                "}\n\n"
                "message Request {\n"
                "  string account = 1;\n"
                "  string region = 2;\n"
                "}\n\n"
                "message Response {\n"
                "  string result = 1;\n"
                "}\n"
            ),
            "grpcServiceDefinitionJSON": (
                '{"nested":{"EchoService":{"methods":{"Echo":'
                '{"requestType":"Request","responseType":"Response"}}},'
                '"Request":{"fields":{"account":{"type":"string","id":1},'
                '"region":{"type":"string","id":2}}},"Response":{"fields":'
                '{"result":{"type":"string","id":1}}}}}'
            ),
            "methodName": "EchoService/Echo",
            "message": '{"account":"patched","region":"cn"}',
            "grpcCheckCondition": "STATUS_CODE_DEFAULT",
            "condition": "",
            "grpcConnectTimeoutMs": 10_000,
        },
        "K8S": {
            "datasource": 17,
            "type": "K8S",
            "namespace": "",
            "kubeConfig": "",
            "image": "registry.example/worker:patched",
            "minCpuCores": 0.0,
            "minMemorySpace": 0.0,
            "localParams": [],
            "command": "[]",
            "args": "[]",
            "imagePullPolicy": "IfNotPresent",
            "customizedLabels": [],
            "nodeSelectors": [],
        },
        "JAVA": {
            "localParams": [],
            "mainJar": {"resourceName": "/tenant/resources/jobs/patched-orders.jar"},
            "runType": "FAT_JAR",
            "mainArgs": "--date 2026-08-21",
            "jvmArgs": "",
            "isModulePath": False,
            "resourceList": [
                {"resourceName": "/tenant/resources/jobs/patched-orders.jar"},
            ],
            "mainClass": "",
        },
        "MR": {
            "localParams": [],
            "mainJar": {"resourceName": "/tenant/resources/jobs/patched-mapreduce.jar"},
            "mainClass": "com.example.PatchedMapReduce",
            "mainArgs": "--date 2026-08-21",
            "others": "",
            "appName": "",
            "yarnQueue": "",
            "resourceList": [],
            "programType": "JAVA",
        },
        "SAGEMAKER": {
            "sagemakerRequestJson": (
                '{"PipelineName":"patched-training",'
                '"ClientRequestToken":"patched-training-20260824"}'
            ),
            "localParams": [],
            "datasource": 17,
            "resourceList": [],
            "type": "SAGEMAKER",
        },
        "SQOOP": {
            "jobType": "CUSTOM",
            "localParams": [],
            "customShell": (
                "sqoop export --connect "
                "jdbc:mysql://db.example.invalid:3306/warehouse "
                "--table patched_orders"
            ),
        },
        "SEATUNNEL": {
            "localParams": [],
            "startupScript": "seatunnel.sh",
            "useCustom": True,
            "rawScript": "env { execution.parallelism = 2 }\n",
            "resourceList": [],
            "deployMode": "local",
            "others": "",
        },
    }
    if task_type in projected_params:
        return projected_params[task_type]
    if task_type == "SWITCH":
        return {
            "switchResult": {
                "dependTaskList": [
                    {
                        "condition": '${route} == "patched-A"',
                        "nextNode": 8002,
                    },
                    {
                        "condition": '${route} == "patched-B"',
                        "nextNode": 8003,
                    },
                ],
                "nextNode": 8004,
            }
        }
    if task_type == "CONDITIONS":
        return {
            "dependence": {
                "relation": "OR",
                "dependTaskList": [
                    {
                        "relation": "AND",
                        "dependItemList": [
                            {
                                "depTaskCode": 8004,
                                "status": "FAILURE",
                            }
                        ],
                    }
                ],
            },
            "conditionResult": {
                "successNode": [8002],
                "failedNode": [8003],
            },
        }
    return dict(task_params)


_DEFAULT_PATCH_TEMPLATE_TASK_TYPES = tuple(
    task_type
    for task_type in supported_task_template_types()
    # FLINK_STREAM is generic on stable 3.4.1; its exact 3.2.2 typed patch and
    # file-edit paths are locked by test_flink_stream_inline_sql_authoring.py.
    # KUBEFLOW is a closed three-field model; arbitrary taskParams mutation is
    # locked by test_kubeflow_authoring.py instead of this open-map smoke test.
    if task_type not in {"FLINK_STREAM", "KUBEFLOW"}
)


@pytest.mark.parametrize(
    "task_type",
    _DEFAULT_PATCH_TEMPLATE_TASK_TYPES,
)
def test_apply_workflow_patch_updates_template_task_payloads(
    task_type: str,
) -> None:
    codes = iter(range(8001, 8100))
    template = task_template_result(task_type)
    data = template.data
    assert isinstance(data, dict)
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    task_document = yaml.safe_load(yaml_text)
    assert isinstance(task_document, dict)
    baseline = WorkflowSpec.model_validate(_compilable_workflow_document(task_document))
    primary_task = baseline.tasks[0]
    patch_set = _task_patch_set(task_type)
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": primary_task.name},
                            "set": patch_set,
                        }
                    ]
                }
            }
        }
    ).patch

    merged, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )
    compilation = workflow_compile_service.prepare_workflow_create_compilation(merged)
    payload = compilation.materialize(
        [next(codes) for _ in range(compilation.required_task_code_count)],
        resource_refs=_PATCH_FILE_REFS,
    )
    task_definitions = json.loads(payload["taskDefinitionJson"])
    compiled_primary = task_definitions[0]

    assert diff["updated_tasks"] == [primary_task.name]
    assert compiled_primary["taskType"] == task_type
    assert json.loads(compiled_primary["taskParams"]) == _expected_compiled_task_params(
        task_type,
        patch_set,
    )
    if "command" in patch_set:
        assert merged.tasks[0].command == patch_set["command"]
        assert merged.tasks[0].task_params is None
    else:
        assert merged.tasks[0].command is None
        assert merged.tasks[0].task_params == patch_set["task_params"]


def test_apply_workflow_patch_translates_invalid_java_main_args() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-java-workflow"},
            "tasks": [
                {
                    "name": "run-java-fat-jar",
                    "type": "JAVA",
                    "task_params": {
                        "mainJar": "/jobs/daily-orders.jar",
                        "mainArgs": [],
                    },
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "run-java-fat-jar"},
                            "set": {
                                "task_params": {
                                    "mainJar": "/jobs/daily-orders.jar",
                                    "mainArgs": "--date",
                                }
                            },
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="mainArgs must be one strict list"):
        apply_workflow_patch(
            baseline,
            patch,
            edge_builder=workflow_compile_service.workflow_edges,
        )


def test_apply_workflow_patch_updates_extended_task_execution_fields() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {
                                "flag": "NO",
                                "environment_code": 42,
                                "task_group_id": 21,
                                "task_group_priority": 7,
                                "timeout": 15,
                                "timeout_notify_strategy": "FAILED",
                                "cpu_quota": 50,
                                "memory_max": 1024,
                            },
                        }
                    ]
                }
            }
        }
    ).patch

    merged, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )
    compilation = workflow_compile_service.prepare_workflow_create_compilation(merged)
    payload = compilation.materialize([8101] * compilation.required_task_code_count)
    task_definitions = json.loads(payload["taskDefinitionJson"])

    assert diff["updated_tasks"] == ["extract"]
    assert task_definitions == [
        {
            "code": 8101,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {
                    "rawScript": "echo extract",
                    "localParams": [],
                    "resourceList": [],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "NO",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": 42,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "OPEN",
            "timeoutNotifyStrategy": "FAILED",
            "timeout": 15,
            "delayTime": 0,
            "resourceIds": "",
            "taskExecuteType": "BATCH",
            "taskGroupId": 21,
            "taskGroupPriority": 7,
            "cpuQuota": 50,
            "memoryMax": 1024,
        }
    ]


def test_apply_workflow_patch_treats_semantic_defaults_as_no_change() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                    "timeout": 15,
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {
                                "worker_group": None,
                                "timeout_notify_strategy": "WARN",
                                "cpu_quota": -1,
                                "memory_max": -1,
                            },
                        }
                    ]
                }
            }
        }
    ).patch

    _, diff = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )

    assert diff["updated_tasks"] == []


def test_apply_workflow_patch_rewrites_conditions_predicate_after_task_rename() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "conditions-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                },
                {
                    "name": "route",
                    "type": "CONDITIONS",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "AND",
                                    "dependItemList": [
                                        {"task": "extract", "status": "SUCCESS"}
                                    ],
                                }
                            ],
                        },
                        "conditionResult": {
                            "successNode": ["on-success"],
                            "failedNode": ["on-failed"],
                        },
                    },
                },
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "rename": [{"from": "extract", "to": "extract-v2"}],
                }
            }
        }
    ).patch

    merged, _ = apply_workflow_patch(
        baseline,
        patch,
        edge_builder=workflow_compile_service.workflow_edges,
    )

    conditions_params = merged.tasks[1].task_params
    assert conditions_params is not None
    assert conditions_params["dependence"] == {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "AND",
                "dependItemList": [{"task": "extract-v2", "status": "SUCCESS"}],
            }
        ],
    }


def test_apply_workflow_patch_rejects_invalid_task_execution_combination() -> None:
    baseline = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "patched-workflow"},
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo extract",
                }
            ],
        }
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "extract"},
                            "set": {"timeout_notify_strategy": "FAILED"},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="requires timeout > 0"):
        apply_workflow_patch(
            baseline,
            patch,
            edge_builder=workflow_compile_service.workflow_edges,
        )
