# 推荐版本化、定向回填与回退（Recommendation Versioning）

本能力把"生成版本、新鲜度、来源解释、定向回填、回到上一版"做成贯穿离线写入与在线读取的同一条链路。代码主要位于 `versioning/`，并在 `worker/`、`logics/`、`server/`、`master/`、`config/` 中接线。

## 1. 设计原则与硬约束落点

### 单一事实来源
版本指纹与新鲜度判定只有一份实现，离线和在线共用：

- `versioning.Components.Fingerprint()` 是唯一的版本标识算法。
- `versioning.Judge.Evaluate()` 是唯一的新鲜度判定。离线 pipeline 的 `Pipeline.checkVersionOutOfDate` 与在线 `versioning.Resolver.Resolve` 都调用它，杜绝"写新读旧、两边各算一份"。

### 原子性（缓存接口无事务）
现有 `cache.Database` 没有事务、没有 TTL。原子性通过"先写数据、后翻转单键指针"实现：

1. 结果写入版本化子集 `recommend_versioned/<user>#<seq>`（对读取不可见，直到指针指向它）。
2. 最后用一次 `Set` 写入用户指针键 `versioning_user_pointer/<user>`。

读取方只通过指针定位子集，因此永远看不到"半个版本"。回退同样是单键翻转（`versioning_rollback` / `versioning_user_rollback/<user>`）。

### 兼容
- 现有 `GET /api/recommend/<user>` 的路径、body、顺序、数量、分页、状态码完全不变；只**附加** `X-Gorse-Version-*` 响应头。
- 新版本化写入同时镜像 legacy `recommend` 集合与 `recommend_digest`/`recommend_update_time` 键，未升级的读取方照常工作。
- 新解释能力放在新端点 `GET /api/recommend/<user>/explain`，老客户端无需改动。
- 周期性任务的不活跃跳过、过期判定、冷启动跳过、兜底顺序、反馈写回去重语义全部保留；版本判断只叠加在既有判定之后。

### 资源隔离
- 回填并发由 `versioning.backfill_concurrency` 上限约束，复用项目自带的 `common/parallel` 有界 worker pool。
- 回填/回退与在线读取共用同一缓存，但回填失败只记录，不删除任何可用结果。

### 安全
- 版本指纹只含摘要、模型编号、时间戳、链路名；ranker 的 API token 不进入指纹（`rankerFingerprint` 只取 URL/Model 等非密字段）。
- 解释与观测输出只含 item id、来源标签、计数与时间，不含反馈原文或物品标签原文。
- 未引入任何新的第三方依赖，仅用标准库与现有 `testify`。

## 2. 版本标识

`versioning.Components`：

| 字段 | 含义 | 变化来源 |
|---|---|---|
| `RecallDigest` | 召回集合与参数 | `RecommendConfig.Hash()` |
| `RankerType` | 排序方式 | none/fm/reranker/decision |
| `RankerDigest` | 排序参数（去密） | ranker 配置非密字段 |
| `ModelID` | 模型编号 | MF 模型毫秒 ID |
| `SampleCutoff` | 样本截止时刻 | 本轮生成开始时间（UTC） |
| `FallbackChain` | 兜底链路（有序） | `Fallback.Recommenders` |

任一字段变化 => 新指纹；等价重跑 => 同指纹。注册表 `versioning_registry` 按指纹去重，等价重跑复用同一版本序号；真正变化时分配单调递增序号。当前版本指针为 `versioning_current`。

## 3. 读取判定（四态）

`Judge` 输出 `fresh / stale / missing / rollback`：

- **fresh**：指针指纹+序号等于当前版本，且数据年龄未超 `Recommend.CacheExpire`。
- **stale**：版本落后（给出版本落后数 `VersionsBehind`），或超数据年龄上限；元数据损坏也按最保守 stale 处理并给可读原因。
- **missing**：用户无指针或无任何版本。
- **rollback**：回退生效中，读取回退目标子集并带标记。

数据年龄 = 当前时间 − 指针 `UpdatedAt`；年龄上限即 `AgeLimit`。

## 4. 缺失/不足与来源解释

- 离线为空或不足时，在线 `Recommender` 走既有兜底链，顺序、去重、类别过滤语义不变。
- `logics.Recommender` 新增 opt-in 解释模式（`EnableExplain`），用与结果并行的 provenance map 记录每个 item 的来源，不改变内容/顺序/数量。
- 解释体 `versioning.Explain` 报告：
  - `fallback.used / chains / original_version_existed`：是否兜底、哪条链路、原版本是否存在。
  - `online.boundary`：在线边界时刻（请求时看到的最新反馈时间）。
  - `online.sample_cutoff`：离线样本截止时刻。
  - `online.filtered_count`：截止后反馈导致的在线过滤条数。
  - `online.supplement_count`：请求时在线补充条数。
  - `sources`：每个返回 item 的来源（`offline` / `fallback/<name>` / `online/latest`）。

## 5. 定向回填

三种选人范围 `versioning.Selector.Scope`：

- `users`：显式用户清单。
- `modified_window`：按 `LastModifyUserTime` 落在修改时间窗口。
- `behind_version`：指针序号低于指定版本（含无指针/损坏指针）。

干跑（`dry_run`）只返回候选数、版本分布 `VersionDistribution` 与预计条数，不写任何结果。

执行保证（`versioning.Manager`）：

- 有界并发（`parallel.ForEach` + 配置上限）、可取消（`Cancel`，context 驱动）。
- 逐用户失败上报 `UserFailure{user, stage, reason}`，失败不删除可用结果。
- 同版本重复执行幂等：任务持久化 `DoneUsers`，`Resume` 只补未完成用户，不从头开始。
- 同范围并发：相同 scope 合并进在跑任务；重叠 scope 明确拒绝（`ErrConflictingBackfill`），不并行写同一用户。
- 未选中用户完全不动。
- 任务状态持久化在缓存（`versioning_backfill_task/<id>` + 索引），中断后可观测、可续跑。

真实生成器 `worker.BackfillGenerator` 复用 pipeline 的召回/替换/排序子步骤，通过 `versioning.Generator` 接口接入，保证回填结果与周期任务一致，并按显式目标版本提交。

## 6. 回到上一版

- `Manager.ActivateRollback` 先校验范围内**每个**用户都保留目标版本子集；任一缺失即拒绝（`ErrRollbackTargetMissing` + 缺失清单），拒绝时不写任何回退键，状态与切换前完全一致。
- 激活后在线 `Resolver` 整体切到目标版本，返回带 rollback 标记的旧结果；`DeactivateRollback` 切回当前版本。
- 回退窗口内 `Pipeline.checkVersionOutOfDate` 对在范围用户返回"不需重算"，回退期间不触发重算。
- 每用户保留版本数由 `versioning.retained_versions` 限定（默认 3）。

## 7. 观测

`Manager.Observe` 单次遍历用户、复用同一 `Judge`，输出 `ObservabilitySnapshot`：

- 按版本分组的 fresh/stale/missing/rollback 用户数与总数。
- 最老/最新数据年龄。
- 当前版本序号、回退是否激活。
- 最近回填任务状态与失败数（`TaskSummary`）。

## 8. HTTP 接口

在线（server，沿用既有 API key 鉴权）：

- `GET /api/recommend/<user>`：不变，附加版本响应头。
- `GET /api/recommend/<user>/explain`：新增，返回 items + explanation。

管理（master，dashboard 鉴权）：

- `GET  /api/dashboard/versions`：版本注册表。
- `GET  /api/dashboard/versions/observe`：分组观测。
- `POST /api/dashboard/backfills`：启动/干跑回填。
- `GET  /api/dashboard/backfills`、`GET /api/dashboard/backfills/{id}`：任务列表/详情。
- `POST /api/dashboard/backfills/{id}/cancel`、`/resume`：取消/续跑。
- `POST /api/dashboard/rollback`、`DELETE /api/dashboard/rollback`：激活/解除回退。

## 9. 配置

```toml
[versioning]
retained_versions = 3
backfill_concurrency = 4
max_backfill_failures = 1000
```

## 10. 缓存键一览

| 键 | 作用 |
|---|---|
| `versioning_registry` | 版本注册表（有序数组 JSON） |
| `versioning_current` | 当前版本指针 |
| `versioning_rollback` | 全局回退状态 |
| `versioning_user_rollback/<user>` | 单用户回退覆盖 |
| `versioning_user_pointer/<user>` | 用户版本指针（含 bounded history） |
| `recommend_versioned/<user>#<seq>` | 版本化结果子集（score 集合） |
| `versioning_backfill_task/<id>` | 回填任务记录 |
| `versioning_backfill_index` | 回填任务 id 索引 |

## 11. 测试

全部使用仓库现有风格（`testify` suite/assert + `apitest`），数据与缓存落在 `t.TempDir()` 的本地 sqlite/xvec，不依赖外部服务：

- `versioning/*_test.go`：指纹等价/变更、注册表、四态判定、回退、损坏降级、干跑、取消、幂等续跑、冲突合并/拒绝、观测分组。
- `server/versioning_test.go`：四态可观察输出、旧接口逐字段一致、损坏指针不失败请求。
- `master/versioning_test.go`：干跑/观测/回退拒绝与状态不变/回填任务生命周期。
- `worker/backfill_test.go`：真实 pipeline 生成器按目标版本提交且不影响未选中用户。
