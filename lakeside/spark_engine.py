"""生产实现骨架：PySpark + Iceberg。

本地验证用 DuckDB（engine.py），生产用这个。
两者共享 sql/ 下的同一批语句，差异只在执行与写入方式。

依赖见 requirements-spark.txt，不在默认安装里——
CI 与本地验证都不需要起 Spark 集群。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import settings

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


class SparkIcebergLake:
    """Iceberg 湖的 Spark 实现。

    未在当前版本接入：容器编排下起 Spark 需要 ~2GB 内存，
    而本地验证不需要分布式执行。
    """

    def __init__(self, catalog: str = "local", warehouse: str | None = None) -> None:
        # 延迟导入，避免没装 pyspark 时 import lakeside 就失败
        from pyspark.sql import SparkSession  # noqa: PLC0415

        warehouse = warehouse or str(settings.lake_root / "_warehouse")
        self.spark = (
            SparkSession.builder.appName("lakeside")
            .config(
                "spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # 分区裁剪与动态分区覆盖的开关
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )

    def run_sql_file(self, path: Path, target: str) -> None:
        sql = path.read_text(encoding="utf-8")
        self.spark.sql(f"CREATE OR REPLACE TEMP VIEW _src AS {sql}")
        self.spark.sql(f"INSERT OVERWRITE {target} SELECT * FROM _src")

    def merge(self, target: str, source_view: str, keys: list[str], update_cols: list[str]) -> None:
        """Iceberg 的 MERGE INTO。

        与 DuckDB 版本语义一致：匹配则更新、不匹配则插入。
        """
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        sets = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        cols = [c for c in self.spark.table(target).columns]
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join(f"s.{c}" for c in cols)
        self.spark.sql(
            f"""
            MERGE INTO {target} t
            USING {source_view} s
            ON {on}
            WHEN MATCHED THEN UPDATE SET {sets}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
        )

    def build_all(self) -> dict[str, Any]:
        """按层执行。这个是生产入口，本地不用。

        供应商维度走 SCD2 时态模型（与 DuckDB 路径共享同一份 SQL）：
            changelog 摄入（按 record_hash 去重）→ SCD2 版本表与隔离区全量重写。
        Iceberg 用 INSERT OVERWRITE 整表替换，等价于本地的原子 rewrite，
        不需要人工清理旧分区；兼容视图 dwd_dim_supplier 由元数据持久化。
        """
        layers = [
            # 供应商时态维度：先 changelog，再版本表/隔离区
            ("dwd", "supplier_changelog"),
            ("dwd", "dim_supplier_scd2"),
            ("dwd", "dim_supplier_reject"),
            ("dwd", "fact_purchase_order"),
            ("dwd", "fact_goods_receipt"),
            ("dwd", "fact_settlement"),
            ("dws", "dws_po_summary"),
            ("ads", "ads_supplier_monthly"),
        ]
        done = []
        for layer, name in layers:
            sql_path = SQL_DIR / layer / f"{name}.sql"
            target = f"local.{layer}.{name}"
            self.run_sql_file(sql_path, target)
            done.append(target)
        return {"built": done}

    def stop(self) -> None:
        self.spark.stop()
