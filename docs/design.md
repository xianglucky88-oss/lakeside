# 湖仓设计说明

## 目标

为对账、结算月报与供应商绩效提供单一数据源，覆盖采购单、入库、结算三条链路。

## 数据流

```
          ┌──────────────────────── ODS（原始）────────────────────────┐
          │ supplier        purchase_order      goods_receipt   settlement │
          └───────┬───────────────────┬─────────────────┬───────────────┘
                  │ 清洗/去重/时区     │                 │
                  ▼                   ▼                 ▼
          ┌──────────────────────── DWD（明细）────────────────────────┐
          │ dim_supplier   fact_purchase_order  fact_goods_receipt  fact_settlement │
          └───────┬───────────────────┬─────────────────┬───────────────┘
                  │     聚合到 po_id 粒度               │
                  ▼                   ▼                 ▼
                    dws_po_summary（宽表，po_id 一行）
                                  │
                                  ▼
                    ads_supplier_monthly（供应商月报）
```

## 分层职责

| 层 | 职责 | 当前状态 |
|---|---|---|
| ODS | 原样落地，不做任何清洗 | 可用（Parquet，无分区） |
| DWD | 去重、时区归正、维表 SCD2 | 可用 |
| DWS | 聚合到 po_id 粒度 | 草稿 |
| ADS | 面向财务的月报 | 空模板 |

## 事实与维度的关系

严格来说只有 3 张事实表，1 张维度表：

- `fact_purchase_order` 的粒度是 **po_id**（理论上应唯一）
- `fact_goods_receipt` 的粒度是 **gr_id**，与 po_id 是 **一对多**
- `fact_settlement` 的粒度是 **settlement_id**，与 po_id 也是 **一对多**
- `dim_supplier` 是供应商维度，理论上是 SCD2

三张事实表的粒度不同：采购单按 po_id、入库按 gr_id、结算按 settlement_id。
跨表汇总时需要先各自聚合到同一粒度。

## 粒度与可加性

| 指标 | 可加性 |
|---|---|
| 采购金额 | 按 po_id 可加，按供应商可加，**不可跨时间窗口随意相加**（去重后） |
| 入库金额 | 同上 |
| 结算金额 | 同上，且必须按**本地业务日**归属 |
| 入库批次数 | 不可相加，必须 `count(DISTINCT gr_id)` |

## 幂等性

| 表 | 写入方式 | 重跑是否幂等 | 备注 |
|---|---|---|---|
| ODS | 全量重写 | 是 | 由 seed 决定，内容完全一致 |
| dim_supplier | 整表覆盖 | 是 | 保留每个供应商的当前版本 |
| fact_* | 整表覆盖 | 是 | 换成增量分区时需注意分区边界 |
| dws_po_summary | 整表覆盖 | 是 | 草稿口径 |
| ads_supplier_monthly | 整表覆盖 | 是（空） | — |

## 时区

| 字段 | 语义 | 处理方式 |
|---|---|---|
| `ods_supplier.valid_from` | 本地时间 | 直接用 |
| `ods_purchase_order.biz_date` | 本地业务日 | 直接用 |
| `ods_settlement.settled_at_utc` | UTC | 见 `biz_date_local` |
| `ods_settlement.biz_date_local` | 本地业务日 | 与 UTC 时间差 8 小时 |

`month_boundary_check()` 会比对 DWD 的 `settle_date` 与 `biz_date_local`。

## 观察点

| 指标 | 位置 | 正常范围 |
|---|---|---|
| DWD 采购单行数 vs 去重 po_id 数 | `verify` → `dwd_po_dedup` | — |
| DWD 采购金额 = ODS 去重金额 | `dwd.po_amount_conserved` | 比值 1.00 |
| DWS 采购金额 = ODS 去重金额 | `dws.po_amount_conserved` | 比值 1.00 |
| DWS 入库金额 = ODS 入库金额 | `dws.gr_amount_conserved` | 比值 1.00 |
| ADS 行数 | `ads.has_output` | > 0 |
| 月末归属差异 | `month_boundary_check()` | — |

## 待决策

1. SCD2 的生效区间端点：左闭右开还是左闭右闭——跨系统对齐前必须先定。
2. 仓库时区是固定 +8 还是会随政策变化；是否需要引入时区数据库。
3. DWS 是否需要保留"未到货"的采购单（当前 LEFT JOIN 会保留，但金额为 null 还是 0 需明确）。
4. 迟到事实（结算晚于采购单所属月份）是否回追历史月报。
