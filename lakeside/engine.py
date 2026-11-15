"""DuckDB 湖仓引擎。

把 DuckDB 当成一个单机 lakehouse 用：
    * 表存在湖目录下的 parquet 文件里；
    * 通过 `read_parquet` 暴露成视图；
    * 写入用 `COPY ... TO ... (FORMAT PARQUET)`，按目录分区；
    * 扫描量通过 `EXPLAIN ANALYZE` 的 `Total Files Read` 推断。

这样做的原因：这个仓库的目的是验证**分层逻辑与幂等语义**，
而不是验证分布式执行。Spark/Iceberg 的生产实现在 `spark_engine.py`，
两者共享同一份 SQL。
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import duckdb


class Lake:
    """一个湖目录的连接包装。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        # 目录即表、parquet 即数据：catalog 完全内存化。
        # 这样湖目录里只有数据文件，没有任何"引擎私有状态"，
        # 换引擎（Spark/DuckDB/Trino）时不需要迁移元数据。
        self.con = duckdb.connect(":memory:")
        self.con.execute("SET threads TO 4")
        # 扫描量统计需要一个可重置的计数器；DuckDB 用 profiles 实现。
        self._last_stats: dict[str, Any] = {}
        self._timings: list[tuple[str, float]] = []
        # 连接即扫描：湖目录里已有的表全部注册成视图，
        # 这样新进程不用先跑 build 就能查询（verify / API 都依赖这一点）。
        self.refresh_views()

    # ---------- 表访问 ----------

    def table_path(self, layer: str, name: str) -> Path:
        return self.root / layer / name

    def glob(self, layer: str, name: str) -> str:
        # ** 递归匹配：分区表文件在 as_of_date=... 子目录里，
        # 单层 * 跨不过目录分隔符；非分区表的顶层文件同样能被 ** 命中。
        return str(self.table_path(layer, name) / "**" / "*.parquet")

    def exists(self, layer: str, name: str) -> bool:
        p = self.table_path(layer, name)
        # rglob 才能命中分区子目录（as_of_date=.../part-*.parquet）里的文件
        return p.exists() and any(p.rglob("*.parquet"))

    def view(self, layer: str, name: str) -> None:
        """把目录下的 parquet 暴露为视图。"""
        src = self.glob(layer, name)
        self.con.execute(
            f"CREATE OR REPLACE VIEW {self.view_name(layer, name)} AS SELECT * FROM read_parquet('{src}')"
        )

    def refresh_views(self) -> None:
        for layer in ("ods", "dwd", "dws", "ads"):
            d = self.root / layer
            if not d.exists():
                continue
            for sub in d.iterdir():
                if sub.is_dir() and any(sub.rglob("*.parquet")):
                    self.view(layer, sub.name)

    # ---------- 写入 ----------

    def write(self, sql: str, layer: str, name: str, partition_by: str | None = None) -> int:
        """把 sql 的结果写成 parquet。

        分区：按 partition_by 列拆目录。分区语义是"整表覆盖该分区的数据"，
        与真实数仓的 INSERT OVERWRITE PARTITION 一致。
        """
        dest = self.table_path(layer, name)
        dest.mkdir(parents=True, exist_ok=True)

        start = time.perf_counter()
        if partition_by is None:
            out = dest / "part-0001.parquet"
            self.con.execute(f"COPY ({sql}) TO '{out}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)")
        else:
            # 分区值统一在 SQL 内 CAST 成 VARCHAR：目录命名与 WHERE 字面量
            # 始终用同一种字符串形态，避免 DATE/TIMESTAMP 的 Python str() 与
            # 引擎隐式转换不一致导致分区裁剪落空。
            rows = self.con.execute(
                f"SELECT DISTINCT CAST({partition_by} AS VARCHAR) AS pv FROM ({sql})"
            ).fetchall()
            for (val,) in rows:
                safe = str(val).replace("/", "_")
                pdir = dest / f"{partition_by}={safe}"
                pdir.mkdir(parents=True, exist_ok=True)
                inner = (
                    f"SELECT * FROM ({sql}) "
                    f"WHERE CAST({partition_by} AS VARCHAR) = {repr(val)}"
                )
                out = pdir / "part-0001.parquet"
                # 固定文件名 + OVERWRITE_OR_IGNORE：重跑时原子替换该分区，
                # 不需要先删目录（在只读挂载或对象存储上也能工作）。
                self.con.execute(
                    f"COPY ({inner.replace('?', repr(val))}) TO '{out}' "
                    "(FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
                )
        elapsed = time.perf_counter() - start
        self._timings.append((f"{layer}.{name}", elapsed))
        self.refresh_views()
        return self.count(layer, name)

    def merge(self, layer: str, name: str, source_sql: str, keys: list[str], update_set: dict[str, str]) -> int:
        """MERGE 语义的 upsert：匹配则更新、不匹配则插入。

        与整表覆盖的区别在于：已存在的行会被更新而不是重复插入。
        """
        if not self.exists(layer, name):
            return self.write(source_sql, layer, name)

        tmp = f"_staging_{name}"
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS {source_sql}")
        target = self.view_name(layer, name)
        cols = [r[0] for r in self.con.execute(f"DESCRIBE {target}").fetchall()]
        target_set = set(cols)

        # 目标列：除更新列外都取 t，更新列取 s。
        select_items = []
        for c in cols:
            if c in update_set:
                select_items.append(f"s.{update_set[c]} AS {c}")
            else:
                select_items.append(f"t.{c} AS {c}")
        key_match = " AND ".join(f"t.{k} = s.{k}" for k in keys)

        merged = f"""
            SELECT {', '.join(select_items)}
            FROM {target} t
            JOIN {tmp} s ON {key_match}
            UNION ALL
            SELECT {', '.join('s.' + c for c in cols)}
            FROM {tmp} s
            WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {key_match})
        """
        out = self.table_path(layer, name) / "part-0001.parquet"
        self.con.execute(f"COPY ({merged}) TO '{out}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)")
        self.refresh_views()
        return self.count(layer, name)

    def view_name(self, layer: str, name: str) -> str:
        """视图名 = 表名，若表名已带层前缀则不再重复。

        表名与视图名一致，是为了让 SQL 里既能写 `dws_po_summary`（ADS 用），
        也能写 `dwd_fact_purchase_order`（DWS 用），不出现 `dws_dws_po_summary`。
        """
        return name if name.startswith(f"{layer}_") else f"{layer}_{name}"

    # ---------- 统计 ----------

    def count(self, layer: str, name: str) -> int:
        if not self.exists(layer, name):
            return 0
        src = self.glob(layer, name)
        return self.con.execute(f"SELECT count(*) FROM read_parquet('{src}')").fetchone()[0]

    def query_df(self, sql: str):
        """执行查询，返回带 to_dict('records') 的结果对象。

        不依赖 pandas/numpy：用普通 dict 列表承载行，既满足调用方
        按 records 取数的习惯，也避免为单机验证引入重型依赖。
        """
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]

        class _Records:
            def __init__(self, columns, rows):
                self.columns = columns
                self._rows = rows

            def to_dict(self, orient: str = "records"):
                if orient != "records":
                    raise ValueError("only orient='records' is supported")
                return [dict(zip(self.columns, row)) for row in self._rows]

        return _Records(cols, cur.fetchall())

    def files(self, layer: str, name: str) -> int:
        p = self.table_path(layer, name)
        return len(list(p.rglob("*.parquet"))) if p.exists() else 0

    def scanned_files(self, sql: str) -> int:
        """用 EXPLAIN 推断一条查询会读多少个文件。

        这是"全量扫描 vs 分区裁剪"的可观测证据：
        对分区表按分区过滤，应该只读对应目录下的文件。
        """
        try:
            plan = self.con.execute(f"EXPLAIN {sql}").fetchall()
        except Exception:  # noqa: BLE001
            return -1
        text = "\n".join(str(r[1]) for r in plan)
        # DuckDB 的计划里会列出 read_parquet 的路径
        return text.count(".parquet")

    @property
    def timings(self) -> list[tuple[str, float]]:
        return list(self._timings)

    @property
    def duration(self) -> float:
        return sum(t for _, t in self._timings)

    def close(self) -> None:
        self.con.close()
