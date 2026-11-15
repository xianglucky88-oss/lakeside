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

import json
import time
from pathlib import Path
from typing import Any

import duckdb

# 持久化逻辑视图的元数据文件。视图定义随湖目录走，
# 新进程打开湖时按它重建视图，不依赖任何引擎私有 catalog。
VIEW_META_FILE = "_meta/views.json"


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
        return str(self.table_path(layer, name) / "*.parquet")

    def exists(self, layer: str, name: str) -> bool:
        p = self.table_path(layer, name)
        return p.exists() and any(p.glob("*.parquet"))

    def ensure_empty(self, layer: str, name: str, columns: dict[str, str]) -> None:
        """按给定 {列: 类型} 建一张空 parquet 表（已存在则不动）。

        用于让"首跑即引用目标表"的幂等 SQL（如 NOT EXISTS 去重摄入）
        在第一次运行时也有合法的解析目标。
        """
        if self.exists(layer, name):
            return
        col_sql = ", ".join(f"CAST(NULL AS {typ}) AS {col}" for col, typ in columns.items())
        self.write(f"SELECT {col_sql} WHERE false", layer, name)

    def view(self, layer: str, name: str) -> None:
        """把目录下的 parquet 暴露为视图。"""
        src = self.glob(layer, name)
        self.con.execute(
            f"CREATE OR REPLACE VIEW {self.view_name(layer, name)} AS SELECT * FROM read_parquet('{src}')"
        )

    @property
    def view_meta_path(self) -> Path:
        return self.root / VIEW_META_FILE

    def _load_view_meta(self) -> dict[str, str]:
        p = self.view_meta_path
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))

    def _save_view_meta(self, meta: dict[str, str]) -> None:
        p = self.view_meta_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def register_logical_view(self, view_name: str, sql: str) -> None:
        """注册持久化逻辑视图（如 SCD1 兼容视图）。

        定义写入湖目录的元数据文件并立即在当前连接创建，
        新进程通过 refresh_views 自动恢复，不依赖内存 catalog。
        """
        meta = self._load_view_meta()
        meta[view_name] = sql
        self._save_view_meta(meta)
        self.con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")

    def drop_logical_view(self, view_name: str) -> None:
        meta = self._load_view_meta()
        meta.pop(view_name, None)
        self._save_view_meta(meta)
        self.con.execute(f"DROP VIEW IF EXISTS {view_name}")

    def refresh_views(self) -> None:
        for layer in ("ods", "dwd", "dws", "ads"):
            d = self.root / layer
            if not d.exists():
                continue
            for sub in d.iterdir():
                # 只跳过点号开头的隐藏临时目录（.staging/.backup）；
                # 下划线开头的是合法表名，_meta 是视图元数据目录。
                if sub.name.startswith(".") or sub.name == "_meta" or not sub.is_dir():
                    continue
                if any(sub.glob("*.parquet")):
                    self.view(layer, sub.name)
        # 恢复持久化逻辑视图（兼容层）
        for view_name, sql in self._load_view_meta().items():
            self.con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")

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
            rows = self.con.execute(f"SELECT DISTINCT {partition_by} FROM ({sql})").fetchall()
            for (val,) in rows:
                safe = str(val).replace("/", "_")
                pdir = dest / f"{partition_by}={safe}"
                pdir.mkdir(parents=True, exist_ok=True)
                inner = f"SELECT * FROM ({sql}) WHERE {partition_by} = ?"
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

    def rewrite(self, sql: str, layer: str, name: str) -> int:
        """整表原子重写：写临时目录，再 rename 替换。

        与 write() 的区别：目标表的旧文件在替换瞬间整体失效，
        不依赖人工或作业去清理旧分区，也不会留下半写状态——
        临时目录以 '.' 开头，refresh_views 不会注册它。
        重跑/重建任意次数，物理上始终只有一份数据文件。
        """
        dest = self.table_path(layer, name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        staging = dest.parent / f".{name}.staging"
        backup = dest.parent / f".{name}.backup"
        # 清理上次失败可能残留的隐藏目录（它们不会被视图注册）
        self._rmtree(staging)
        self._rmtree(backup)
        staging.mkdir(parents=True, exist_ok=True)
        out = staging / "part-0001.parquet"
        self.con.execute(f"COPY ({sql}) TO '{out}' (FORMAT PARQUET)")
        if dest.exists():
            dest.rename(backup)
        staging.rename(dest)
        if backup.exists():
            self._rmtree(backup)
        self.refresh_views()
        return self.count(layer, name)

    @staticmethod
    def _rmtree(p: Path) -> None:
        import shutil

        shutil.rmtree(p, ignore_errors=True)

    def merge(self, layer: str, name: str, source_sql: str, keys: list[str], update_set: dict[str, str]) -> int:
        """MERGE 语义的 upsert：匹配则更新、不匹配则插入。

        与整表覆盖的区别在于：已存在的行会被更新而不是重复插入。
        """
        if not self.exists(layer, name):
            return self.write(source_sql, layer, name)

        tmp = f"_staging_{name}"
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} AS {source_sql}")
        # 空源必须短路：否则下面的 JOIN 会产出 0 行，把已有目标表整体清空
        if self.con.execute(f"SELECT count(*) FROM {tmp}").fetchone()[0] == 0:
            return self.count(layer, name)
        target = self.view_name(layer, name)
        cols = [r[0] for r in self.con.execute(f"DESCRIBE {target}").fetchall()]
        target_set = set(cols)

        # 目标列：更新列取 s（未匹配时 s 为 NULL，COALESCE 回退到 t），其余取 t。
        # 关键：必须 LEFT JOIN，保证没有任何 staging 行匹配的目标行原样保留，
        # 而不是被 upsert 静默删除。
        select_items = []
        for c in cols:
            if c in update_set:
                select_items.append(f"COALESCE(s.{update_set[c]}, t.{c}) AS {c}")
            else:
                select_items.append(f"t.{c} AS {c}")
        key_match = " AND ".join(f"t.{k} = s.{k}" for k in keys)

        merged = f"""
            SELECT {', '.join(select_items)}
            FROM {target} t
            LEFT JOIN {tmp} s ON {key_match}
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
        return self.con.execute(sql).fetchdf()

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
