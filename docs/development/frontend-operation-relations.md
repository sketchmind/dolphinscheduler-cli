# 前端操作关系与 CLI 设计

前端操作的身份传递、状态条件、副作用和结果观察是 CLI 设计的审阅线索。
公共行为以 [CLI 契约](../reference/cli-contract.md) 为准，
实现所有权以 [架构](architecture.md) 为准。新增 DS action 仍需逐版本源码证据
和发布门槛；菜单、按钮或 enum 的存在不能批准支持。

## 关系与承载位置

| 关系 | 前端中的例子 | CLI 的合适承载位置 |
| --- | --- | --- |
| 身份与对象关联 | 定义进入实例列表；任务进入所属实例；父实例进入子实例 | 返回真实标识与作用域；有足够事实时生成 `next_actions` |
| 输入发现 | 调度选择 worker、环境、租户；namespace 选择集群 | 现有字段 schema、`discovery_command`/`discovery_command_pattern`、模板说明 |
| 状态前置 | 下线后编辑；终态后执行节点；失败后恢复 | 服务校验及 exact recipe；schema 描述前置条件，导航只作保守提示 |
| 结果观察 | 控制请求提交后查看新一轮状态；强制入队后观察实际执行 | 接受回执、已解析身份、新轮次基线、`watch`/`digest`/日志 |
| 副作用与范围 | 定义下线连带调度下线；删除级联；授权整集合替换 | 命令摘要、schema、预览及结构化结果/错误中的影响事实 |
| 目标分支 | 保存草稿、手动验证、定时投产、恢复失败是不同目的 | 少量有说明的关联命令与按需场景文档/skill，不强制线性执行 |

按钮的 `loading`、`disabled`、倒计时、选中行和路由切换，还包含界面临时状态。
它们不能直接进入业务资格判断。按钮可点也不证明权限、worker 条件和全部后端约束满足。

## 身份、导航与观察

导航只使用已经返回的身份、作用域与状态，不发起额外 REST 请求。
操作的读取或写入属性从 catalog 的 effects 派生；服务仍负责完整验证与授权。
schema 提供静态命令和输入发现，动态 action_index 按已知行事实分组，
next_actions 提供少量身份完整的后续命令。缺失身份或目标作用域时不生成可执行命令。

`targets: "all"` 仅表示本次返回的全部行，要求所有行身份有效、唯一且全部纳入索引。
截断、重复或非法身份造成的部分覆盖使用明确 ID。字段投影和 JSON 编码不改变逻辑索引范围，
索引覆盖也不等于远端分页完整性。

父子实例关系中的目标 ID 不证明目标项目。源项目的 `resolved.project`
不能复制为子实例作用域；应保留关系读取，在目标项目有证据后才能生成导航。
同名对象、长整数 ID、旧版字符串 ID 与现代 code 均保留 exact 身份边界。

定义与调度分别持有状态；定义下线可能连带调度下线，后续恢复投产应明确检查调度。
保存草稿、手动运行和配置定时调度是不同目标，不强制上线后立即执行。
队列 force-start 等控制结果中的 `accepted: true` 只证明请求接受；
实际执行与完成需要实例、轮次基线、watch、digest 或日志证据。

JSON 与紧凑 JSON 共享信息契约，compact 只在显式声明的业务集合改变编码。
警告使用可选的结构化 `warnings`；未知事实、受支持的空值和查询覆盖保持原样。
table/TSV 投影业务字段，操作索引不成为每行重复文本。

## Exact 项目授权边界

| Exact 发布集合 | 数量 | `grantProject` 的源码语义 |
| --- | ---: | --- |
| 1.3.9、3.0.0–3.0.6、3.1.0–3.1.9 | 18 | 删除全部关系后写入传入集合；recipe 使用 `replace` |
| 2.0.0–2.0.9 | 10 | 直接插入传入关系；recipe 使用 `insert` |
| 3.2.0–3.2.2、3.3.1–3.3.2、3.4.0–3.4.3 | 9 | 只更新目标项目关系；recipe 使用 `upsert` |

源方法位于各 exact API 的 `UsersService[Impl].grantProject`；
`grantProjectWithReadPerm` 从 3.2.0 出现。前两组公共 REST 仅产生写权限。
reviewed evidence 与 exact action 选择由
[兼容性编译器](../../tools/ds_codegen/compatibility_impact.py) 和
[用户领域适配](../../src/dsctl/upstream/users.py) 维护。

替换整集合时需保留其他项目关系，同时保留上游并发限制；插入语义需考虑幂等性，
新版本需区分只读升级。回读的 `verification: "membership_only"`
只证明关系存在，不证明权限等级或目标用户实际执行。管理员视角不能替代目标身份验证。

未来演进需复核是否替换全集合、是否支持只读、查询是否返回 relation perm。
路由或 DTO 未变不能替代服务语义审阅。

## 任务旅程与验收

| 用户目标 | 现有命令构成的路径 | 必须保留的决定/证据 |
| --- | --- | --- |
| 新建任务流 | template → lint → create dry-run → create | 之后可以仅保存、手动运行或配置调度；不强制 online→run |
| 修改定时工作流 | export → 修改/预览 → 显式 offline → edit → online → 检查调度 | dry-run 可报告状态阻断；下线的调度是否恢复是独立决定 |
| 调整调度 | list/get → explain → offline（如需）→ update → preview → online（如需） | schedule id、实际时区/参数、更新未隐式激活、当前 workflow 状态 |
| 排查失败 | 实例 digest → 任务/日志 → 子实例或节点历史 → 选择修复/恢复 → watch | 真实父子关系、原始版本和参数、本轮基线、查询覆盖范围 |
| 排查排队 | task-group → queue → task/instance → 调整优先级或请求 force-start → 观察 | 容量池与队列记录不同；接受不等于执行；不存在身份不猜测 |
| 验证授权 | 管理员查看/修改具体关系与等级 → 回读 → 用户明确选择目标身份验证 | 不自动切身份；成员关系、权限等级、对象可见、实际执行分别证明 |

规则验收覆盖 OFFLINE/ONLINE/缺失/未知状态、权限等级、不可用版本、
缺失或重复身份、输入不足、跨 context、接受后未启动和新轮次完成。
导航命令应被真实 parser 接受，且不增加请求；两种 JSON 解码后保持相同事实。

输出方案的比较使用等价任务与业务信息，评估任务正确率、身份和状态错误、
未经目标授权的写入、CLI/REST 次数、实际用量和用时。
总字节是辅助指标，不设置牺牲正确性的信息预算。

## 上游审阅线索

DS 1.3.9 的
[实例菜单](https://github.com/apache/dolphinscheduler/blob/174c78c4a90a53fdfe7131e9b065edaa38b7936f/dolphinscheduler-ui/src/js/conf/home/pages/projects/pages/instance/pages/list/_source/list.vue#L106-L138)
用 `disabled` 表达可操作组，而 DS 3.4.1 的
[实例动作](https://github.com/apache/dolphinscheduler/blob/f19eb8ce7dc4d7c0be9e213610d6812718294333/dolphinscheduler-ui/src/views/projects/workflow/instance/components/table-action.tsx#L119-L261)
用它禁用按钮。不能直接将变量名称作为业务资格规则。

DS 3.2.0 起任务实例入口由 processInstanceId/name 转为 workflowInstanceId/name；
命名变化仍需 exact wire 适配，不能创造新的对象关系。
DS 3.4.2 的
[启动表单](https://github.com/apache/dolphinscheduler/blob/71eb6412f940afa1f171f1097dc0e99ed61d16e2/dolphinscheduler-ui/src/views/projects/workflow/definition/components/use-modal.ts)
修正参数字段绑定及验证，说明菜单动作未变也可能伴随校验变化。

UI handler、表单和菜单差异应触发所属 controller、service、enum 的源码复核。
按实际变化的操作重审，不能从任意 Vue/TSX 条件表达式生成通用业务规则，
也不能把样本审阅推广为全部版本的能力授权。
