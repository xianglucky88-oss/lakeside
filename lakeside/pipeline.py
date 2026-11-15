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
from typing import Any, Callable

from .config import settings
from .engine import Lake
from .generator import generate

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


def _render(sql: str) -> str:
    """把口径占位符替换为本次运行的逻辑日期与时区偏移。

    同一份 SQL 被 DuckDB 与 Spark 两个引擎共享，运行参数只能在执行期注入；
    逻辑日期来自 settings，保证同一快照重跑结果完全一致。
    """
    return (
        sql.replace("{{LOGICAL_DATE}}", settings.logical_date)
        .replace("{{SRC_TZ_OFFSET_HOURS}}", str(settings.source_tz_offset_hours))
    )


class Pipeline:
    """分层管线。所有方法幂等，可重复调用。"""

    def __init__(self, lake: Lake | None = None) -> None:
        settings.ensure()
        self.lake = lake or Lake(settings.lake_root)
        self.results: list[AssetResult] = []
        self.notes: list[str] = []

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
        # 维表：整表覆盖写入。
        out.append(
            self._build_asset("dwd", "dim_supplier", keys=["supplier_id"], merge=False)
        )
        # 事实表：去重/清洗后整表覆盖；隔离区依赖采购单、入库、结算三张事实，最后构建。
        for name in ("fact_purchase_order", "fact_goods_receipt", "fact_settlement", "reject_event"):
            out.append(self._build_asset("dwd", name, keys=[], merge=False))
        return out

    # ---------- DWS ----------

    def build_dws(self) -> list[AssetResult]:
        out = [
            # 对账用精简汇总，无扇出
            self._build_asset("dws", "dws_po_summary", keys=["po_id"], merge=False),
            # 履约时效主宽表：按 as_of_date 做快照分区
            self._build_asset(
                "dws", "dws_po_fulfillment", keys=["po_id"], merge=False, partition_by="as_of_date"
            ),
        ]
        return out

    # ---------- ADS ----------

    def build_ads(self) -> list[AssetResult]:
        return [
            self._build_asset(
                "ads", "ads_po_fulfillment_risk",
                keys=["po_id"], merge=False, partition_by="as_of_date",
            ),
            self._build_asset("ads", "ads_supplier_monthly",
                              keys=["month", "supplier_id"], merge=False),
        ]

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
            "note": "采购单粒度履约时效产品：快照分区 as_of_date，金额无扇出",
        }

    # ---------- 内部 ----------

    def _build_asset(
        self,
        layer: str,
        name: str,
        keys: list[str],
        merge: bool,
        partition_by: str | None = None,
    ) -> AssetResult:
        sql = _render(_read_sql(layer, name))
        start = time.perf_counter()
        files_before = self.lake.files(layer, name)
        if merge and keys:
            rows = self.lake.merge(layer, name, sql, keys=keys, update_set={})
        else:
            rows = self.lake.write(sql, layer, name, partition_by=partition_by)
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
        """计算金额守恒与履约口径相关的原始数字。

        这些是 verify_lake.py 与测试都要引用的证据。所有"真值"都在 ODS 上
        按业务主键与版本口径独立计算，避免拿被测结果自证。
        """
        lk = self.lake
        out: dict[str, float] = {}

        # ODS 真值：每个 po_id 取 version 最大一行（重新批准按最新版本），
        # 同版本重复推送只计一次，再对 approved 单据求和。
        out["ods_po_amount_cents"] = float(
            lk.con.execute(
                """
                SELECT coalesce(sum(amount_cents), 0)
                FROM (
                    SELECT po_id, amount_cents, status,
                           row_number() OVER (
                               PARTITION BY po_id ORDER BY version DESC, ingested_at DESC
                           ) AS rn
                    FROM ods_purchase_order
                )
                WHERE rn = 1 AND status = 'approved'
                """
            ).fetchone()[0]
        )
        # ODS 入库真值：剔除撤销单；孤儿入库（无有效采购单）单独计量
        out["ods_gr_amount_cents"] = float(
            lk.con.execute(
                """
                SELECT coalesce(sum(gr.received_cents), 0)
                FROM ods_goods_receipt gr
                JOIN (
                    SELECT po_id, status,
                           row_number() OVER (
                               PARTITION BY po_id ORDER BY version DESC, ingested_at DESC
                           ) AS rn
                    FROM ods_purchase_order
                ) po ON gr.po_id = po.po_id AND po.rn = 1
                WHERE gr.gr_status = 'valid' AND po.status = 'approved'
                """
            ).fetchone()[0]
        )
        out["ods_gr_rejected_cents"] = float(
            lk.con.execute(
                """
                SELECT coalesce(sum(gr.received_cents), 0)
                FROM ods_goods_receipt gr
                LEFT JOIN (
                    SELECT po_id, status,
                           row_number() OVER (
                               PARTITION BY po_id ORDER BY version DESC, ingested_at DESC
                           ) AS rn
                    FROM ods_purchase_order
                ) po ON gr.po_id = po.po_id AND po.rn = 1
                WHERE gr.gr_status = 'valid'
                  AND (po.po_id IS NULL OR po.status = 'cancelled')
                """
            ).fetchone()[0]
        )
        # ODS 结算真值：只统计同币种、归属有效采购单的结算
        out["ods_st_amount_cents"] = float(
            lk.con.execute(
                """
                SELECT coalesce(sum(st.settled_cents), 0)
                FROM ods_settlement st
                JOIN (
                    SELECT po_id, currency, status,
                           row_number() OVER (
                               PARTITION BY po_id ORDER BY version DESC, ingested_at DESC
                           ) AS rn
                    FROM ods_purchase_order
                ) po ON st.po_id = po.po_id AND po.rn = 1
                WHERE po.status = 'approved' AND st.currency = po.currency
                """
            ).fetchone()[0]
        )
        out["ods_st_rejected_cents"] = float(
            lk.con.execute(
                """
                SELECT coalesce(sum(st.settled_cents), 0)
                FROM ods_settlement st
                LEFT JOIN (
                    SELECT po_id, currency, status,
                           row_number() OVER (
                               PARTITION BY po_id ORDER BY version DESC, ingested_at DESC
                           ) AS rn
                    FROM ods_purchase_order
                ) po ON st.po_id = po.po_id AND po.rn = 1
                WHERE po.po_id IS NULL
                   OR po.status = 'cancelled'
                   OR st.currency <> po.currency
                """
            ).fetchone()[0]
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
        if lk.exists("dwd", "reject_event"):
            out["dwd_reject_rows"] = float(lk.count("dwd", "reject_event"))
            out["dwd_reject_ccy_cnt"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM dwd_reject_event WHERE reject_reason='CURRENCY_MISMATCH'"
                ).fetchone()[0]
            )
            out["dwd_reject_orphan_cnt"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM dwd_reject_event WHERE reject_reason='ORPHAN_EVENT'"
                ).fetchone()[0]
            )

        # DWS 结果
        if lk.exists("dws", "dws_po_summary"):
            out["dws_po_amount_cents"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(po_amount_cents),0) FROM dws_po_summary "
                    "WHERE po_id IN (SELECT po_id FROM dwd_fact_purchase_order WHERE status='approved')"
                ).fetchone()[0]
            )
            out["dws_received_cents"] = float(
                lk.con.execute("SELECT coalesce(sum(received_cents),0) FROM dws_po_summary").fetchone()[0]
            )
            out["dws_settled_cents"] = float(
                lk.con.execute("SELECT coalesce(sum(settled_cents),0) FROM dws_po_summary").fetchone()[0]
            )
            out["dws_rows"] = float(lk.count("dws", "dws_po_summary"))

        # 履约宽表：状态分布与风险计数
        if lk.exists("dws", "dws_po_fulfillment"):
            out["dws_fulfillment_rows"] = float(lk.count("dws", "dws_po_fulfillment"))
            out["dws_fulfillment_po_amount"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(po_amount_cents),0) FROM dws_po_fulfillment"
                ).fetchone()[0]
            )
            out["dws_fulfillment_received"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(received_cents),0) FROM dws_po_fulfillment"
                ).fetchone()[0]
            )
            out["dws_fulfillment_settled"] = float(
                lk.con.execute(
                    "SELECT coalesce(sum(settled_cents),0) FROM dws_po_fulfillment"
                ).fetchone()[0]
            )
            status_rows = lk.con.execute(
                "SELECT fulfillment_status, count(*) FROM dws_po_fulfillment GROUP BY 1"
            ).fetchall()
            for st_name, cnt in status_rows:
                out[f"dws_status_{st_name.lower()}"] = float(cnt)
            out["dws_overdue_cnt"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM dws_po_fulfillment WHERE overdue_days IS NOT NULL"
                ).fetchone()[0]
            )
            out["dws_intransit_cnt"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM dws_po_fulfillment WHERE in_transit_days IS NOT NULL"
                ).fetchone()[0]
            )
            # 在途（未到期）订单绝不能出现逾期天数——这是"未结束订单不被误判逾期"的护栏
            out["dws_intransit_wrongly_overdue"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM dws_po_fulfillment "
                    "WHERE fulfillment_status='IN_TRANSIT' AND overdue_days IS NOT NULL"
                ).fetchone()[0]
            )

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
        if lk.exists("ads", "ads_po_fulfillment_risk"):
            out["ads_risk_rows"] = float(lk.count("ads", "ads_po_fulfillment_risk"))
            out["ads_risk_flagged"] = float(
                lk.con.execute(
                    "SELECT count(*) FROM ads_po_fulfillment_risk WHERE risk_codes <> ''"
                ).fetchone()[0]
            )
        return out

    def month_boundary_check(self) -> list[dict]:
        """检查结算日的本地归属。

        取 UTC 16:00 之后的结算记录（+8 时区会跨日），比对 DWD 按偏移
        换算的 settle_date 与源系统自带 biz_date_local，口径正确时应为 0 差异。
        """
        if not self.lake.exists("dwd", "fact_settlement"):
            return []
        cur = self.lake.con.execute(
            """
            SELECT s.settlement_id, s.settled_at_utc,
                   substr(s.settled_at_utc, 12, 2) AS utc_hour,
                   s.settle_date AS dwd_date,
                   o.biz_date_local AS true_local_date
            FROM dwd_fact_settlement s
            JOIN (
                SELECT settlement_id, biz_date_local,
                       row_number() OVER (PARTITION BY settlement_id ORDER BY ingested_at) AS rn
                FROM ods_settlement
            ) o ON s.settlement_id = o.settlement_id AND o.rn = 1
            WHERE CAST(substr(s.settled_at_utc, 12, 2) AS INTEGER) >= 16
            ORDER BY s.settled_at_utc
            LIMIT 50
            """
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def risk_summary(self) -> dict[str, Any]:
        """履约风险分布：状态机、风险码、隔离事件、三类异常分开计数。"""
        lk = self.lake
        if not lk.exists("dws", "dws_po_fulfillment"):
            return {}
        statuses = {
            r[0]: int(r[1])
            for r in lk.con.execute(
                "SELECT fulfillment_status, count(*) FROM dws_po_fulfillment GROUP BY 1"
            ).fetchall()
        }
        risk_codes = (
            "IN_TRANSIT_OVERDUE", "SPLIT_DELAY", "OVER_RECEIVED",
            "SETTLE_EXCEEDS_RECEIPT", "CURRENCY_MISMATCH", "MISSING_EXPECTED_DATE",
        )
        code_counts = {}
        for code in risk_codes:
            code_counts[code] = int(
                lk.con.execute(
                    "SELECT count(*) FROM dws_po_fulfillment "
                    "WHERE risk_codes LIKE ?",
                    [f"%{code}%"],
                ).fetchone()[0]
            )
        return {
            "as_of_date": settings.logical_date,
            "status_counts": statuses,
            "risk_code_counts": code_counts,
            "unsettled_po": int(lk.con.execute(
                "SELECT count(*) FROM dws_po_fulfillment WHERE unsettled_flag = 1"
            ).fetchone()[0]),
            "no_receipt_po": int(lk.con.execute(
                "SELECT count(*) FROM dws_po_fulfillment WHERE no_receipt_flag = 1"
            ).fetchone()[0]),
            "data_issue_po": int(lk.con.execute(
                "SELECT count(*) FROM dws_po_fulfillment WHERE data_issue_codes <> ''"
            ).fetchone()[0]),
            "rejected_events": (
                {
                    r[0]: int(r[1])
                    for r in lk.con.execute(
                        "SELECT reject_reason, count(*) FROM dwd_reject_event GROUP BY 1"
                    ).fetchall()
                }
                if lk.exists("dwd", "reject_event") else {}
            ),
        }

    def po_evidence(self, po_id: str) -> dict[str, Any]:
        """返回单个采购单的风险结论与全部业务事实依据，供人工复核。"""
        lk = self.lake
        if not lk.exists("ads", "ads_po_fulfillment_risk"):
            return {}
        cur = lk.con.execute(
            "SELECT * FROM ads_po_fulfillment_risk WHERE po_id = ? ORDER BY as_of_date DESC",
            [po_id],
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        if not rows:
            return {"po_id": po_id, "found": False}
        head = rows[0]
        head["found"] = True
        head["snapshot_count"] = len(rows)
        return head

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
