# lakeside

内部湖仓的批式 ELT 管线。分层 `ODS → DWD → DWS → ADS`，
覆盖供应商、采购单、入库单、结算单四类数据，服务于对账与结算月报。

**当前状态：只做到 DWD。DWS 是草稿、ADS 是空模板，金额口径全部未验证。**

## 快速开始

```bash
pip install -r requirements.txt

python -m lakeside build          # 全量构建（~1 秒，60 供应商 / 400 采购单）
python -m lakeside verify         # 打印守恒指标
python3 scripts/verify_lake.py    # 带断言的检查
make demo                         # 固定演示序列
```

默认湖目录是 `./lake`，可用 `LAKE_ROOT` 改。

```bash
LAKE_ROOT=/tmp/lake1 python -m lakeside build
LAKE_ROOT=/tmp/lake1 python scripts/verify_lake.py
```

HTTP 模式（浏览器里看指标）：

```bash
uvicorn lakeside.api:app --port 9300
open http://localhost:9300
```

## 为什么用 DuckDB 而不是直接上 Spark

这个仓库要验证的是**分层逻辑、幂等语义与金额守恒**，不是分布式执行。
DuckDB 单机能秒级跑完，把"改一行 SQL → 看断言变红变绿"的循环压到 1 秒内，
这对出题与录屏都更友好。

目录即表、Parquet 即数据、catalog 完全内存化——湖目录里没有引擎私有状态。
生产实现见 `spark_engine.py`，`configs/pipeline.spark.yaml` 里有等价的 Spark + Iceberg 配置，
两者共享同一批 SQL。

## 结构

```
lakeside/
    config.py      路径与运行参数
    generator.py   四张 ODS 表的确定性生成器
    engine.py      DuckDB 湖仓引擎（写 / merge / 分区 / 扫描量统计）
    pipeline.py    分层编排 + 守恒指标计算
    checks.py      守恒断言（库函数，供 CLI 与单测共用）
    api.py         FastAPI：/build /invariants /month-boundary /catalog
    spark_engine.py  生产实现（PySpark + Iceberg）
    __main__.py    CLI
sql/
    dwd/   dim_supplier, fact_purchase_order, fact_goods_receipt, fact_settlement
    dws/   dws_po_summary          （草稿）
    ads/   ads_supplier_monthly    （空模板）
tests/
scripts/
    verify_lake.py   守恒断言，输出具体数字
    demo.sh          固定演示
```

## 数据规模

默认参数生成：60 个供应商（含历史版本）、约 400 张采购单（含约 12% 重复推送）、
约 600 条入库、约 260 条结算。总耗时约 0.3 秒。
`lakeside/generator.py` 的 `generate()` 可调参。

## 关键指标

`python -m lakeside verify` 输出：

| 指标 | 含义 |
|---|---|
| `ods_po_amount_cents` | ODS 按 po_id 去重后的采购金额 |
| `dwd_po_amount_cents` | DWD 采购金额 |
| `dws_po_amount_cents` | DWS 采购金额 |
| `dws_received_cents` | DWS 入库金额 |
| `ads_rows` | ADS 产出行数 |

各指标的比值应与上游真值一致；`verify_lake.py` 会逐项比对。

## 测试

```bash
python -m pytest -q
```

12 个用例覆盖：生成器确定性、ODS 完整性、重复推送的存在、DWD 未去重、
维表丢历史、金额扇出倍数、ADS 为空、月末归属错误、merge 语义、分区裁剪。
