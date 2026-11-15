"""lakeside —— 内部湖仓的批式 ELT 管线。

数据先过准入层再进可消费 ODS：
    landing（原始证据，只追加）
      → 契约校验 + 逐行准入（admission/）
      → ODS（仅由 accepted 行确定性重建）
      → DWD → DWS → ADS

准入层对每张源表维护可演进契约，并对每个批次给出逐行/批次级结论：
    accepted     放行；held 暂缓（可恢复的迟到引用）；quarantined 隔离（源数据错误）。
所有结论都带原始证据、契约版本与恢复条件；重跑幂等，已接受批次不会被重复计入。

执行引擎有两个实现：
    engine=duckdb   本地湖仓（默认）。用 DuckDB 文件模拟 lakehouse，
                    分区用目录、表格式用 Parquet、MERGE 用 DuckDB 语法。
                    起服务只要几秒，适合离线验证与录屏。
    engine=spark    生产实现。PySpark + Iceberg，配置在 configs/pipeline.spark.yaml。

两条引擎共享同一批 SQL 逻辑与同一套断言脚本，
因此本地验证通过的改动可以直接推到生产配置。

当前状态：准入层 / ODS / DWD 可用，DWS 与 ADS 仍是草稿。
"""

__version__ = "0.7.0"
