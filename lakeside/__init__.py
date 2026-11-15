"""lakeside —— 内部湖仓的批式 ELT 管线。

分层：ODS → DWD → DWS → ADS

执行引擎有两个实现：
    engine=duckdb   本地湖仓（默认）。用 DuckDB 文件模拟 lakehouse，
                    分区用目录、表格式用 Parquet、MERGE 用 DuckDB 语法。
                    起服务只要几秒，适合离线验证与录屏。
    engine=spark    生产实现。PySpark + Iceberg，配置在 configs/pipeline.spark.yaml。

两条引擎共享同一批 SQL 逻辑与同一套断言脚本，
因此本地验证通过的改动可以直接推到生产配置。

当前状态：ODS / DWD / DWS / ADS 全链路可用。DWS 提供采购单粒度的
履约时效快照（dws_po_fulfillment，按 as_of_date 分区），ADS 提供
风险明细与供应商月度汇总；口径见各 SQL 头注释。
"""

__version__ = "0.6.1"
