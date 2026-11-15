"""管线编排。

等价于 Dagster 的 asset 图，但用纯 Python 实现，避免起 Dagster daemon。
生产配置里 `dagster/` 目录有一份等价的 Definitions（可选）。

执行顺序：
    sample -> ods -> dwd -> dws -> ads

每个 asset 记录：行数、耗时、扫描文件数。这些数字是断言脚本与录屏的输入。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .config import settings
from .engine import Lake
from .generator import generate
from .temporal import (
    build_supplier_dim,
    count_unresolved_facts,
    ingest_supplier_changelog,
    temporal_invariants,
)

QUERY_DIR = Path(__file__).resolve().parent.parent / "sql"


@dataclass
class AssetResult:
    name: str
    rows: int
    seconds: float
    files_scanned: int = -1
    extra: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "rows": self.rows,
            "seconds": round(self.seconds, 3),
            "files_scanned": self.files_scanned,
        }
        if self.extra:
            d["extra"] = {k: round(v, 6) for k, v in self.extra.items()}
        return d


def _read_sql(kind: str, name: str) -> str:
    p = QUERY_DIR / kind / f"{name}.sql"
    if not p.exists():
        raise FileNotFoundError(f"missing SQL: {p}")
    return p.read_text(encoding="utf-8")


class Pipeline:
    """分层管线。所有方法幂等，可重复调用。"""

    def __init__(self, lake: Lake | None = None) -> None:
        settings.ensure()
        self.lake = lake or Lake(settings.lake_root)
        self.results: list[AssetResult] = []
        self.notes: list[str] = []
        self.temporal_report: dict | None = None

    # ---------- ODS ----------

    def build_ods(self, regenerate: bool = True) -> AssetResult:
        start = time.perf_counter()
        counts: dict[str, int] = {}
        if regenerate:
            counts = generate(settings, n_suppliers=60, n_po=400)
        self.lake.refresh_views()
        for t in ("supplier", "purchase_order", "goods_receipt", "settlement"):
            counts.setdefault(t, self.lake.count("ods", t))
        res = AssetResult(
            name="ods",
            rows=sum(counts.values()),
            seconds=time.perf_counter() - start,
            extra={k: float(v) for k, v in counts.items()},
        )
        self.results.append(res)
        return res

    # ---------- DWD ----------

    def build_dwd(self) -> list[AssetResult]:
        out = []
        # 供应商时态维度：
        # 1) 变更只追加进 changelog（内容寻址去重，乱序/重放幂等）；
        # 2) 从 changelog 全量原子重建 SCD2 版本表与隔离区；
        # 3) 注册 SCD1 兼容视图 dwd_dim_supplier，老读法不改 SQL。
        start = time.perf_counter()
        new_rows = ingest_supplier_changelog(self.lake)
        report = build_supplier_dim(self.lake)
        report.changelog_new_rows = new_rows
        self.temporal_report = report.to_dict()
        scd2_asset = AssetResult(
            name="dwd.dim_supplier_scd2",
            rows=report.version_rows,
            seconds=time.perf_counter() - start,
            extra={
                "changelog_rows": float(report.changelog_rows),
                "rejected_rows": float(report.rejected_rows),
                "unresolved_facts": 0.0,
            },
        )
        out.append(scd2_asset)
        self.results.append(scd2_asset)
        # 事实表：直接覆盖。采购事实在 SQL 内做一次点时态关联解析 supplier_sk。
        for name in ("fact_purchase_order", "fact_goods_receipt", "fact_settlement"):
            out.append(self._build_asset("dwd", name, keys=[], merge=False))
        # 事实构建完才能统计未解析数，回填到报告与 asset 指标
        report.unresolved_facts = count_unresolved_facts(self.lake)
        self.temporal_report = report.to_dict()
        scd2_asset.extra["unresolved_facts"] = float(report.unresolved_facts)
        return out

    # ---------- DWS ----------

    def build_dws(self) -> list[AssetResult]:
        # 这一层是草稿。
        return [self._build_asset("dws", "dws_po_summary", keys=["po_id"], merge=False)]

    # ---------- ADS ----------

    def build_ads(self) -> list[AssetResult]:
        # 空模板，产出 0 行。出题点在这里。
        return [self._build_asset("ads", "ads_supplier_monthly", keys=["month", "supplier_id"], merge=False)]

    # ---------- 组合 ----------

    def run_all(self, regenerate: bool = True) -> dict:
        t0 = time.perf_counter()
        self.build_ods(regenerate=regenerate)
        self.build_dwd()
        self.build_dws()
        self.build_ads()
        total = time.perf_counter() - t0
        return {
            "logical_date": settings.logical_date,
            "total_seconds": round(total, 3),
            "assets": [r.to_dict() for r in self.results],
            "temporal_dimension": self.temporal_report,
            "note": "dws/ads 为草稿状态，金额口径尚未定稿",
        }

    # ---------- 内部 ----------

    def _build_asset(self, layer: str, name: str, keys: list[str], merge: bool) -> AssetResult:
        sql = _read_sql(layer, name)
        start = time.perf_counter()
        files_before = self.lake.files(layer, name)
        if merge and keys:
            rows = self.lake.merge(layer, name, sql, keys=keys, update_set={})
        else:
            rows = self.lake.write(sql, layer, name)
        elapsed = time.perf_counter() - start
        res = AssetResult(
            name=f"{layer}.{name}",
            rows=rows,
            seconds=elapsed,
            files_scanned=self.lake.scanned_files(sql) if layer == "dwd" else -1,
            extra={"files_before": float(files_before), "files_after": float(self.lake.files(layer, name))},
        )
        self.results.append(res)
        return res

    # ---------- 校验 ----------

    def invariants(self) -> dict[str, float]:
        """计算金额守恒相关的原始数字。

        这些是 verify_lake.py 与 Feedback 都要引用的证据。
        """
        lk = self.lake
        out: dict[str, float] = {}

        # ODS 真值：按业务主键去重后的采购金额
        out["ods_po_amount_cents"] = float(
            lk.con.execute(
                "SELECT coalesce(sum(amount_cents),0) FROM "
                "(SELECT po_id, max(amount_cents) AS amount_cents FROM ods_purchase_order WHERE status='approved' GROUP BY po_id)"
            ).fetchone()[0]
        )
        out["ods_gr_amount_cents"] = float(
            lk.con.execute("SELECT coalesce(sum(received_cents),0) FROM ods_goods_receipt").fetchone()[0]
        )
        out["ods_st_amount_cents"] = float(
            lk.con.execute("SELECT coalesce(sum(settled_cents),0) FROM ods_settlement").fetchone()[0]
        )

        # DWD 结果
        out["dwd_po_rows"] = float(lk.count("dwd", "fact_purchase_order"))
        out["dwd_po_distinct"] = float(
            lk.con.execute("SELECT count(DISTINCT po_id) FROM dwd_fact_purchase_order").fetchone()[0]
        )
        out["dwd_po_amount_cents"] = float(
            lk.con.execute(
                "SELECT coalesce(sum(amount_cents),0) FROM dwd_fact_purchase_order WHERE status='approved'"
            ).fetchone()[0]
        )

        # DWS 结果
        if lk.exists("dws", "dws_po_summary"):
            out["dws_po_amount_cents"] = float(
                lk.con.execute("SELECT coalesce(sum(po_amount_cents),0) FROM dws_po_summary").fetchone()[0]
            )
            out["dws_received_cents"] = float(
                lk.con.execute("SELECT coalesce(sum(received_cents),0) FROM dws_po_summary").fetchone()[0]
            )
            out["dws_settled_cents"] = float(
                lk.con.execute("SELECT coalesce(sum(settled_cents),0) FROM dws_po_summary").fetchone()[0]
            )
            out["dws_rows"] = float(lk.count("dws", "dws_po_summary"))

        # ADS 结果
        out["ads_rows"] = float(lk.count("ads", "ads_supplier_monthly"))
        if lk.exists("ads", "ads_supplier_monthly"):
            out["ads_po_amount_cents"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(po_amount_cents),0) FROM ads_supplier_monthly"
                ).fetchone()[0]
            )
            out["ads_settled_cents"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(settled_cents),0) FROM ads_supplier_monthly"
                ).fetchone()[0]
            )

        # 供应商时态维度（SCD2）不变式：重叠/断档/多当前版本/事实多重命中都应为 0
        out.update(temporal_invariants(lk))
        return out

    def month_boundary_check(self) -> list[dict]:
        """检查结算日的月末归属问题。

        取 UTC 22:00 之后的结算记录，看它们的 settle_date 与
        biz_date_local 是否一致。
        """
        if not self.lake.exists("dwd", "fact_settlement"):
            return []
        df = self.lake.query_df(
            """
            SELECT s.settlement_id, s.settled_at_utc,
                   substr(s.settled_at_utc, 12, 2) AS utc_hour,
                   s.settle_date AS dwd_date,
                   o.biz_date_local AS true_local_date
            FROM dwd_fact_settlement s
            JOIN ods_settlement o USING (settlement_id)
            WHERE CAST(substr(s.settled_at_utc, 12, 2) AS INTEGER) >= 16
            ORDER BY s.settled_at_utc
            LIMIT 50
            """
        )
        return df.to_dict("records")

    def close(self) -> None:
        self.lake.close()


def build(lake_root: str | None = None, logical_date: str | None = None) -> Pipeline:
    """构造管线。便于测试里指向临时目录。"""
    global settings
    if lake_root:
        settings.lake_root = Path(lake_root)
        settings.ensure()
    if logical_date:
        settings.logical_date = logical_date
    return Pipeline()
