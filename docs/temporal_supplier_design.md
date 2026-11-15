# 供应商时态维度（SCD2）设计

## 1. 问题与目标

上游会**回填或更正**供应商的名称、区域、分类等属性。现有 `dwd/dim_supplier.sql`
按 `updated_at` 只保留每个 `supplier_id` 的最新一行（SCD1），后果是：

- 历史采购单在重跑后被**按最新属性重新解释**（区域、名称口径漂移）；
- 迟到的更正、历史回补到达后，旧事实无法还原当时关联到的属性；
- 源端给出的 `valid_from/valid_to` 可能重叠、乱序，直接信任会产生同一时点多版本，
  事实 join 出现一对多放大。

目标：把供应商维度改造成**带有效期的 SCD2 时态模型**，保证

1. 任一事实在其业务发生时刻，**恰好关联到一个**供应商版本；
2. 变更乱序到达、同一时点冲突、历史回补、整维重建都给出确定性结果；
3. 不依赖人工清理旧分区；迁移期下游既有读取仍得到明确、一致的结果；
4. 版本表不随回补次数无限膨胀；事实关联成本可控；
5. 无法判定先后的源数据有明确的拒绝规则，而不是静默选一条。

## 2. 整体结构

```
ods_supplier（源，按批次覆盖）
      │  ingest：只追加 + 内容寻址去重
      ▼
dwd_supplier_changelog          原始变更日志（append-only，事实表）
      │  build_supplier_dim：全量、确定性重建
      ├──────────────► dwd_dim_supplier          服务层 SCD2 版本表（每供应商连续、不重叠区间）
      └──────────────► dwd_dim_supplier_reject   被拒绝记录及原因（隔离区）

dwd_dim_supplier（视图兼容层，每供应商一行 = 当前版本）
dwd_fact_purchase_order         事实在构建时一次性解析出 supplier_sk（点时态 join）
dws/ads                         只对 supplier_sk 做等值 join，不再做区间 join
```

- **changelog 是事实，dim 是派生物。** 所有重建都从同一份 changelog 出发，
  纯函数式地重算版本表，不依赖"这次到了哪些文件"，因此天然支持乱序与回补。
- 版本表用**整表原子重写**（写临时目录 → rename 替换），不删旧分区、不留孤儿文件，
  在只读挂载/对象存储语义下同样成立。

## 3. 表结构

### 3.1 `dwd_supplier_changelog`（变更日志）

| 列 | 说明 |
| --- | --- |
| supplier_id | 业务自然键 |
| supplier_name / region / credit_level | 属性快照 |
| valid_from / valid_to | 源端声明的生效区间（本地时间，naive timestamp） |
| updated_at | 源端记录时间（到达顺序的判据之一） |
| record_hash | 内容哈希 `md5(业务键+属性+valid_from+valid_to)`，同内容只存一份 |
| ingested_at | 入湖时间（批次逻辑日期，回放时确定） |

`record_hash` 即去重键：重复推送、整批重放都不会让日志膨胀。

### 3.2 `dwd_dim_supplier`（SCD2 版本表）

| 列 | 说明 |
| --- | --- |
| supplier_sk | **确定性代理键** = `md5(supplier_id ‖ valid_from)`，同输入重算不变 |
| supplier_id | 自然键 |
| supplier_name / region / credit_level | 版本属性 |
| valid_from / valid_to | 重算后的半开区间 `[valid_from, valid_to)`；当前版本 `valid_to` 为 NULL |
| is_current | 仅该供应商最后一个版本为 true |
| version_seq | 该供应商内从 1 开始的版本序号 |
| source_valid_to | 源端原始 `valid_to`，保留审计痕迹 |
| interval_adjusted | 源区间与重算区间不一致时为 true（重叠被裁剪/相邻被拼接） |
| record_hash / updated_at / ingested_at | 血缘与回放依据 |

不变式（每次构建后校验，见 `temporal.py::temporal_invariants`）：

- **I1 不重叠**：同一 `supplier_id` 任意两个版本的 `[valid_from, valid_to)` 不相交；
- **I2 连续**：同一供应商除当前版本外，每个版本的 `valid_to` 等于下一版本的 `valid_from`；
- **I3 点时态唯一**：对任意时刻 t 与供应商，至多一个版本满足
  `valid_from <= t < coalesce(valid_to, +∞)`；
- **I4 键稳定**：`supplier_sk` 只依赖 `(supplier_id, valid_from)`，回补不改变既有行。

### 3.3 `dwd_dim_supplier_reject`（隔离区）

保留被拒绝原始记录全部列 + `reject_reason` + `rejected_at`。软处理（区间调整）
**不**进隔离区，只在版本表上打 `interval_adjusted` 标记。

## 4. 变更处理规则（核心语义）

对 changelog 中每个 `supplier_id` 独立处理。

### 4.1 排序与时间线重算

通过校验的记录按确定性键排序：

```
ORDER BY valid_from ASC, updated_at ASC, record_hash ASC
```

- `valid_from` 决定业务生效顺序；
- `updated_at` 表达源端记录先后，乱序到达（晚到一条 `valid_from` 更早的记录）
  在全量重建时自然归位，不依赖到达顺序；
- `record_hash` 是最终的确定性平局键，保证任意重放结果字节级一致。

排序后取相邻版本生成半开区间：`valid_to = 下一个版本的 valid_from`，
末版本 `valid_to = NULL`。源端 `valid_to` 只作审计与重叠检测，不被信任为边界。

### 4.2 拒绝规则（何时拒绝无法判定先后的数据）

| 场景 | 处理 | reject_reason |
| --- | --- | --- |
| `valid_from` 为空或无法解析为时间戳 | 拒绝 | `invalid_valid_from` |
| `valid_to` 非空且 `valid_to <= valid_from` | 拒绝 | `invalid_interval` |
| 同供应商、同 `valid_from`、同 `updated_at` 但**属性不同**（含不同 valid_to） | **整组拒绝**，任何一条都不进版本表 | `ambiguous_ordering` |
| 同供应商、同 `valid_from`、同 `updated_at` 且内容完全相同 | 视为重复，由 record_hash 去重折叠 | —（不拒绝） |
| 同 `valid_from` 但 `updated_at` 不同 | 不拒绝：取 `updated_at` 大者为该时点版本，其余被覆盖 | —（软处理） |
| 源区间互相重叠 | 不拒绝：按 4.1 重算边界，标记 `interval_adjusted=true` | —（软处理） |

歧义组整组拒绝的理由：在没有任何可区分先后的信号（生效时刻、记录时刻都相同，
内容又不同）时，任选其一都是在编造事实。隔离区保留证据，待上游补 `updated_at`
或更正后，下一次重建自动纳入。

默认策略是**隔离并继续**（其余供应商与该供应商的其余版本正常产出），
`build_supplier_dim(strict=True)` 时存在被拒绝记录则抛错，让调度作业显式失败。

### 4.3 同时点去重（point dedup）

歧义过滤之后，同一 `(supplier_id, valid_from)` 仍可能存在多条**可排序**记录
（`updated_at` 不同）。此时按 `updated_at_ts DESC, record_hash` 只保留最新一条，
保证时间线上每个生效时刻只对应一个版本。被覆盖的旧记录不进隔离区——
存在可判定先后的信号，属于正常的同点更正。

### 4.4 无变化折叠（no-op collapse）

相邻排序版本若三个业务属性（name/region/credit_level）完全相同，只改了边界，
则折叠为一个版本（保留最早的 `valid_from`）。这是版本表膨胀的第一道闸：
上游反复"触碰"但属性未变的记录不产生新版本。

**右边界的 NULL 陷阱（实现要点）**：折叠组的 `valid_to` 不能用 `max()` 或
`arg_min()` 取，因为这两个聚合都忽略 NULL，而组内末版本为当前版本时其边界恰为
NULL，会错误地把开放时间线收窄成断档，使之后的事实全部无法关联。正确规则：

```sql
CASE WHEN bool_or(valid_to_calc IS NULL) THEN NULL
     ELSE arg_min(valid_to_calc, -ord) END
```

即"组内只要含开放区间，整组就是当前版本；否则取最后一个版本的边界"。
该缺陷由 `test_noop_collapse_keeps_current_state_when_last_version_open_ended` 锁定。

### 4.5 历史回补与维度重建

回补 = 一条 `valid_from` 落在既有时间线中间或之前的新记录。因为每次都从
changelog 全量重算：

- 新版本按 `valid_from` 插入正确位置，只影响相邻版本的 `valid_to`；
- 代理键由内容决定（I4），未受影响的历史版本 `supplier_sk` 不变，
  已解析过的事实不需要重写；
- 被插入点切开的旧版本区间收窄，`supplier_sk` 保持不变（其 `valid_from` 未变）。

### 4.6 事实点时态解析

事实在 DWD 构建时做**一次**区间 join，把代理键固化到事实行：

```sql
LEFT JOIN dwd_dim_supplier d
  ON po.supplier_id = d.supplier_id
 AND po.created_at >= d.valid_from
 AND po.created_at <  COALESCE(d.valid_to, TIMESTAMP '9999-12-31')
```

- I1/I3 保证该 join 对每条事实至多命中一行，不存在一对多放大；
- 命中 0 行（供应商整段缺失或事实早于首个版本）时 `supplier_sk` 为 NULL，
  计入构建报告的 `unresolved_facts`，不丢事实、不静默补值；
- 下游 DWS/ADS 只对 `supplier_sk` 做等值 join。非等值区间比较只在 DWD
  这一次构建中支付；维度体量小，可 broadcast（Spark 侧）/ 全内存（DuckDB 侧）。

## 5. 迁移期兼容

- 新时态表名为 `dwd_dim_supplier` 的**多版本形态**；同时注册同名逻辑视图
  `dwd_dim_supplier_current`（沿用旧表名 `dwd_dim_supplier` 的当前值视图，
  列与旧 SCD1 表完全一致：`supplier_id, supplier_name, region, credit_level,
  dwd_updated_at`），老 SQL 不改一个字即可继续读取，结果等于"当前版本"。
- 视图定义随湖目录里的 `_meta/views.json` 持久化，新进程打开湖时自动重建视图，
  不依赖内存状态。
- 新口径（按业务时刻的历史属性）只被显式改造过的事实/DWS 使用；
  两套读法在同一版本表上共存，直到下游全部切换。

## 6. 膨胀与成本控制

| 手段 | 作用层 |
| --- | --- |
| changelog 按 record_hash 内容寻址去重，重复/重放零增长 | 日志 |
| no-op 折叠（4.3），无属性变化不产版本 | 版本表 |
| 全量原子重写，每次构建物理上只有一份文件，无历史碎片/孤儿分区 | 版本表 |
| 代理键确定性生成，回补不重写未受影响版本、不重写事实 | 全链路 |
| 事实构建时一次解析 supplier_sk，下游全部等值 join | 关联成本 |
| 报告输出 `versions_per_supplier` 等膨胀指标，超阈值可告警 | 可观测 |

归档策略（保留策略，不在本次代码内强制）：changelog 可按 `ingested_at`
分区保留 N 天后只留快照；版本表当前版本与事实仍引用的历史版本不得删除。

## 7. 构建报告与可观测性

`build_supplier_dim` 返回：

- `changelog_rows / version_rows / rejected_rows`
- `ambiguous_groups / invalid_rows`（按原因计数）
- `interval_adjusted_rows`（软处理计数）
- `unresolved_facts`（事实解析缺失数，事实构建时统计）
- `versions_per_supplier_max`（膨胀指标）

对账恒等式：`去重后源记录数 = version_rows（折叠前口径可由 report 解释）
+ rejected_rows`；changelog 行数 ≥ 源去重记录数，差值即被去重的重复推送数。
实现里以更严格的"去向唯一"断言落地：每条 `record_hash` 要么出现在版本表、
要么出现在隔离区，不存在静默丢失（见 `test_accounting_identity_versions_plus_rejected`）。

## 8. 引擎层加固（本次顺带修复的真实缺陷）

时态链路依赖引擎的 upsert 与原子写，实现过程中暴露并修复了两个既有缺陷：

1. **`merge` 会静默删除未匹配的目标行。** 原实现用 INNER JOIN 拼接目标与 staging，
   当某目标行没有任何 staging 行匹配时它不会出现在结果里，整表覆盖后即丢失
   （changelog 增量摄入时会把历史变更清空）。改为 LEFT JOIN + `COALESCE` 回退，
   并对空 staging 短路，保证 upsert 只增改、不删除。
2. **重跑残留与目录可见性。** 新增 `rewrite()`：写 `.staging` 隐藏目录 → rename
   替换 → 清 `.backup`，物理上始终一份文件，不依赖人工清理旧分区；
   `refresh_views` 只跳过点号开头的隐藏目录与 `_meta`，下划线开头的合法表正常注册。

## 9. 交付物清单

| 文件 | 角色 |
| --- | --- |
| `sql/dwd/supplier_changelog_ingest.sql` | 变更日志摄入，record_hash 内容寻址去重 |
| `sql/dwd/dim_supplier_scd2.sql` | SCD2 版本表全量重建（校验/歧义/排序/取界/折叠） |
| `sql/dwd/dim_supplier_reject.sql` | 隔离区，硬无效与同点歧义，带 reject_reason |
| `sql/dwd/dim_supplier_current_view.sql` | SCD1 兼容视图（旧表名/旧列，值为当前版本） |
| `sql/dwd/fact_purchase_order.sql` | 事实构建时一次点时态解析，固化 supplier_sk |
| `lakeside/temporal.py` | 摄入/重建/报告/不变式编排与 `TemporalConflictError` |
| `lakeside/engine.py` | `rewrite` 原子重写、持久化逻辑视图、`merge` 修复、`ensure_empty` |
| `lakeside/pipeline.py` | DWD 构建接入时态流程，报告挂到 run_all 输出 |
| `tests/test_temporal_supplier.py` | 13 个专项测试覆盖全部要求场景 |
| `docs/temporal_supplier_design.md` | 本文档 |

## 10. 测试矩阵（tests/test_temporal_supplier.py）

| 要求 | 对应测试 |
| --- | --- |
| 点时态唯一关联 | `test_scd2_builds_non_overlapping_timeline`、`test_point_in_time_resolution_picks_version_at_business_time` |
| 乱序到达 | `test_out_of_order_arrival_reorders_by_valid_from` |
| 同一时点冲突 → 拒绝 | `test_ambiguous_same_timestamp_conflict_is_quarantined`、`test_same_timestamp_different_updated_at_is_resolvable` |
| 硬无效数据拒绝 | `test_invalid_records_are_rejected_with_reason` |
| 历史回补、代理键稳定 | `test_historical_backfill_preserves_existing_surrogate_keys` |
| 维度重建、无孤儿文件 | `test_rebuild_is_idempotent_and_leaves_no_orphan_files` |
| 迁移期兼容视图 | `test_reopen_lake_restores_compatibility_view` |
| 膨胀控制 | `test_changelog_dedup_and_noop_collapse`、`test_noop_collapse_keeps_current_state_when_last_version_open_ended` |
| 重叠区间软处理 | `test_overlapping_source_intervals_are_adjusted_not_rejected` |
| 对账恒等式 | `test_accounting_identity_versions_plus_rejected` |
