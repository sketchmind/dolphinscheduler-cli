# 从主模板组成工作流

先明确目标的 `DS_VERSION`，再运行 `dsctl template task TYPE --raw`。每类任务的默认模板给出一个活动配置，普通 IN 参数或可选字段只有短注释提示；替换原字段，不追加同名 YAML key。SHELL/PYTHON 添加 `localParams` 时，把 `command` 移到 `task_params.rawScript`。

只有需要完整联动配置的场景才进入 `data.template.variants`：例如 `output` 展示生产者代码与 OUT 声明，`resource` 展示附件名与脚本调用，DEPENDENT 的 `task-dependency` 改变引用目标。DVC 的 upload/download/init、EMR 的 add-steps、HTTP 的 post-json 也表达明确业务操作。默认无需 `--variant`；`minimal`、`params` 和纯别名选择器已删除。SQL pre/post 使用默认模板的短字段提示，不增加场景选择。DATASYNC `raw-json` 是受 selector 限制的 opaque 输入，不是 normal typed 字段校验，也不是任意 AWS 请求透传。

下面通过已安装 CLI 获取**按所选 DS_VERSION 投影的完整工作流**，不需要复制仓库内的另一份 YAML。`basic` 与省略 `--example` 完全相同；四种组合均可加 `--with-schedule`，附加调度仍默认不激活。1.3.9 的 output/branch 会明确拒绝。这些示例经过本地解析、模型和图编译检查，不证明 worker、插件、凭据、资源或被引用工作流已存在。替换示例身份，用所选版本的 schema 核查字段。调用时显式选择的 `--env-file FILE` 也应带到发现、lint 和 dry-run 命令中；模板给出的导航会保留它。

## OUT 生产者与下游 IN

保存为 `output-flow.yaml`：

```bash
dsctl template workflow --example output --raw > output-flow.yaml
```

3.3.1+ 下游必须声明 IN；只在后继脚本写 `${row_count}` 不建立同样的输入契约。1.3.9 不支持这一输出组合，旧版日志输出标记与参数覆盖规则也不同。用 `dsctl template params --topic output` 查当前版本标记，用 `--topic context` 查优先级。共享值放 `workflow.global_params`；只供一个任务使用的值放其 `localParams`，不要假定两处同名值在所有版本以同样顺序覆盖。

## 分支与合流

保存为 `branch-flow.yaml`：

```bash
dsctl template workflow --example branch --raw > branch-flow.yaml
```

SWITCH 自动生成到命中/default 目标的边；目标必须在同一个 `tasks[]` 中，合流节点显式列出前驱。2.0.0–3.2.1 的 SWITCH 只读取 workflow globals 和传入 runtime varPool，**不读取该节点的 localParams**；CLI 分支例刻意使用 global，因此不会误教旧版局部参数路由。3.2.2+ 才使用包含 localParams 的 prepared map。上游动态输出作为条件输入时，还须用 `depends_on` 建立到 SWITCH 的前驱边。1.3.9 没有 SWITCH。

若分支取决于同一实例中某任务的 SUCCESS/FAILURE，改用 `dsctl template task CONDITIONS --raw`。其模板已说明自动添加谓词任务入边与 successNode/failedNode 出边；不要重复添加同样的 `depends_on`。模板的可选兼容 `bizdate` 字段不参与状态谓词求值。

## 调用同项目子工作流

先准备独立的 `child.yaml`，改成独立 child 名称并按需要调整其任务，再创建和上线：

```bash
dsctl template workflow --example basic --raw > child.yaml
```

父工作流 `parent.yaml`：

```bash
dsctl template workflow --example child --raw > parent.yaml
```

父文件中的 child 名称或数字是占位符。运行 `dsctl workflow list --project PROJECT` / `dsctl workflow get WORKFLOW --project PROJECT`，取真实 child code 并核对子流程、引用闭包和上线状态。1.3.9 改用同项目 `childWorkflowName`，由 CLI 解析 id；不要把 code 填进 name。当前 authoring 的存在性/循环检查不能保证之后修改的子图仍就绪。

SUB_WORKFLOW 的局部参数不是跨版本统一的 child input map。3.3.x 只转发 parent startup；3.4.x 转发 parent globals/startup/varPool，localParams 不作为 child 输入。2.0.x 还有同名覆盖与正常完成/取消输出的差异。执行 `dsctl template params --topic context` 与子任务 schema 读取精确规则，不在父模板机械复制所有 IN/OUT。tenant、worker group、environment 属于运行/调度或任务层配置，应在对应 schema 中选择；子工作流存在不证明其 worker 环境已安装。

## 等待另一个项目的工作流或任务

保存为 `dependency-flow.yaml`：

```bash
dsctl template workflow --example dependent --raw > dependency-flow.yaml
```

依次运行 `dsctl project list`、`dsctl workflow list --project PROJECT`、`dsctl task list --project PROJECT --workflow WORKFLOW`。现代模板取 **定义的 code**，不是实例 id；整工作流用 `depTaskCode: 0`。指定任务时改为其 task code 和 `DEPENDENT_ON_TASK`。1.3.9 使用 `projectName` / `workflowName` / `taskName`，整流程由 compiler 转换为 ALL；主模板同时给出两种范围的替换例。

`cycle` / `dateValue` 必须成对选择 schema 列出的 exact 枚举，值不做参数替换。日期窗口按 DS 调度/运行上下文解释，并不等于“总是查当前此刻”。跨工作流引用不会自动生成本文件的 DAG 边；目标身份存在、目标项目权限、历史实例及预期状态仍需确认。3.2.0+ 的 failure controls 和 3.2.1+ 的 parameterPassing 只按版本模板使用。

## 检查、依赖与变更

```bash
dsctl lint workflow output-flow.yaml
dsctl workflow create --file output-flow.yaml --project PROJECT --dry-run
```

对其余文件同样先 lint，再针对目标 dry-run。资源先用不带 --dir 的 `resource list` 取 FILE root，再按 schema 将文件 fullName 转成保留前导 / 的 root-relative 名，并核对 worker 下载目录中的脚本路径；datasource 取正 id 和匹配类型，连接测试不替代业务表/权限校验。ARN、Pigeon job、Zeppelin paragraph 等没有 CLI 发现命令的外部对象，须从所属系统取得；PIGEON 的 p_host 放在父级 workflow.global_params。

外层 task `timeout` 单位是分钟，0 关闭；开启后默认 WARN 只告警，FAILED/WARNFAILED 的中止及远端取消效果仍取决于任务。HTTP connectTimeout、gRPC deadline 的毫秒，以及其他插件自己的秒数，不随此外层单位改变。KUBEFLOW/PYTORCH 等特殊约束继续以 exact schema 为准。

已有工作流先读取/export 基线，再用 `dsctl template workflow-patch --raw` 编辑明确字段。默认只改 description；instance patch 默认只改指定任务脚本，不活动设置 workflow timeout/global_params。删除不需要的操作块，执行对应 edit `--dry-run` 后再应用。预览与应用之间对象可能变化；定义回改不回滚已发生的外部副作用。
