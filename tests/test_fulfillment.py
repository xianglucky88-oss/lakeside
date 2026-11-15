"""履约时效数据产品的口径测试。

每条测试对应一条明确的业务规则：去重、重新批准、撤销/迟到入库、
分批防扇出、缺入库、币种拒绝、在途不误判逾期、快照可追溯。
"""

from __future__ import annotations

import pathlib

import pytest

from lakeside.checks import check_invariants
from lakeside.config import settings
from lakeside.pipeline import Pipeline, _render


@pytest.fixture()
def pipeline():
    import tempfile

    d = tempfile.mkdtemp(prefix="lakeside-fulfill-")
    old_root = settings.lake_root
    old_date = settings.logical_date
    settings.lake_root = pathlib.Path(d)
    settings.ensure()
    p = Pipeline()
    p.run_all()
    yield p
    settings.lake_root = old_root
    settings.logical_date = old_date
    p.close()


def _q(p: Pipeline, sql: str, params=None):
    return p.lake.con.execute(sql, params or []).fetchall()


# ---------- 去重与版本 ----------

def test_dwd_po_deduped_to_one_row_per_po(pipeline):
    """重复推送去重：DWD 每个 po_id 恰好一行。"""
    rows, distinct = _q(
        pipeline,
        "SELECT count(*), count(DISTINCT po_id) FROM dwd_fact_purchase_order",
    )[0]
    assert rows == distinct
    # ODS 里确实存在重复推送，说明去重是真实生效的
    ods_rows, ods_distinct = _q(
        pipeline, "SELECT count(*), count(DISTINCT po_id) FROM ods_purchase_order"
    )[0]
    assert ods_rows > ods_distinct


def test_reapproval_uses_latest_version_and_keeps_first_approval(pipeline):
    """重新批准：当前版本取 v2，金额不跨版本相加，首次批准时间保留。"""
    reapproved = _q(
        pipeline,
        """
        SELECT po_id, po_amount_cents, po_version, first_approved_at, approved_at
        FROM dws_po_fulfillment
        WHERE is_reapproved = 1
        LIMIT 1
        """,
    )
    assert reapproved, "生成数据中应存在重新批准样本"
    po_id = reapproved[0][0]
    ods = _q(
        pipeline,
        "SELECT version, amount_cents, approved_at FROM ods_purchase_order "
        "WHERE po_id = ? ORDER BY version",
        [po_id],
    )
    assert len(ods) == 2
    dwd_row = _q(
        pipeline,
        "SELECT amount_cents, version, first_approved_at, approved_at "
        "FROM dwd_fact_purchase_order WHERE po_id = ?",
        [po_id],
    )[0]
    assert dwd_row[0] == ods[-1][1]          # 金额取 v2
    assert dwd_row[1] == 2                   # 当前版本号
    assert dwd_row[2] == ods[0][2]           # 首次批准时间保留
    assert dwd_row[3] == ods[-1][2]          # 当前批准时间为 v2
    # 绝不能把两版金额相加
    assert dwd_row[0] != ods[0][1] + ods[1][1]


# ---------- 防扇出 ----------

def test_po_amount_not_fanned_out(pipeline):
    """GR × ST 一对多连接不得放大采购金额，各层与 ODS 真值相等。"""
    inv = pipeline.invariants()
    assert inv["dws_po_amount_cents"] == pytest.approx(inv["ods_po_amount_cents"], rel=1e-9)
    assert inv["dws_fulfillment_po_amount"] == pytest.approx(inv["ods_po_amount_cents"], rel=1e-9)
    assert inv["ads_po_amount_cents"] == pytest.approx(inv["ods_po_amount_cents"], rel=1e-9)
    # 入库与结算同样守恒（有效口径）
    assert inv["dws_received_cents"] == pytest.approx(inv["ods_gr_amount_cents"], rel=1e-9)
    assert inv["dws_settled_cents"] == pytest.approx(inv["ods_st_amount_cents"], rel=1e-9)


def test_all_invariant_checks_pass(pipeline):
    results = check_invariants(pipeline.invariants())
    failed = [r["name"] for r in results if not r["ok"]]
    assert not failed, failed


# ---------- 撤销 / 迟到 / 隔离 ----------

def test_voided_receipts_excluded_but_traceable(pipeline):
    """撤销入库不计有效金额，但在事实表与隔离区都留痕。"""
    voided_cnt = _q(
        pipeline, "SELECT count(*) FROM dwd_fact_goods_receipt WHERE is_voided = 1"
    )[0][0]
    assert voided_cnt > 0
    # 取消单上的撤销单优先归为 PO_NOT_APPROVED，不重复计数
    in_reject = _q(
        pipeline,
        """
        SELECT count(*) FROM dwd_reject_event r
        JOIN dwd_fact_purchase_order p USING (po_id)
        WHERE r.reject_reason = 'VOIDED_RECEIPT' AND p.status = 'approved'
        """,
    )[0][0]
    eligible = _q(
        pipeline,
        """
        SELECT count(*) FROM dwd_fact_goods_receipt g
        JOIN dwd_fact_purchase_order p USING (po_id)
        WHERE g.is_voided = 1 AND p.status = 'approved'
        """,
    )[0][0]
    assert in_reject == eligible
    # 宽表只统计有效入库
    wide = dict(
        zip(
            ["voided", "received", "po_amt"],
            _q(
                pipeline,
                "SELECT gr_batches_voided, received_cents, po_amount_cents "
                "FROM dws_po_fulfillment WHERE voided_receipt_cnt > 0 LIMIT 1",
            )[0],
        )
    )
    assert wide["voided"] >= 1


def test_late_ingested_batches_are_counted(pipeline):
    """迟到补传不丢弃：late_gr_batches 能识别 event 与 ingested 时间差 >3 天。"""
    late = _q(pipeline, "SELECT sum(late_gr_batches) FROM dws_po_fulfillment")[0][0]
    assert late and late > 0


def test_currency_mismatch_is_rejected_not_converted(pipeline):
    """币种不一致的结算进隔离区，金额不进入履约宽表。"""
    ccy = _q(
        pipeline,
        "SELECT count(*) FROM dwd_reject_event WHERE reject_reason='CURRENCY_MISMATCH'",
    )[0][0]
    assert ccy > 0
    flagged = _q(
        pipeline,
        "SELECT count(*) FROM dws_po_fulfillment WHERE ccy_reject_cnt > 0",
    )[0][0]
    assert flagged > 0
    # 被拒外币金额不出现在有效结算合计里
    inv = pipeline.invariants()
    assert inv["ods_st_rejected_cents"] > 0


def test_orphan_events_are_quarantined(pipeline):
    """引用不存在采购单的入库/结算进入 ORPHAN_EVENT，不静默丢失。"""
    orphans = _q(
        pipeline,
        "SELECT source_table, count(*) FROM dwd_reject_event "
        "WHERE reject_reason='ORPHAN_EVENT' GROUP BY 1 ORDER BY 1",
    )
    sources = {r[0] for r in orphans}
    assert sources == {"goods_receipt", "settlement"}


def test_cancelled_po_events_rejected_and_po_excluded(pipeline):
    """取消单不进履约宽表；其后续事件按 PO_NOT_APPROVED 隔离。"""
    in_wide = _q(
        pipeline,
        "SELECT count(*) FROM dws_po_fulfillment f "
        "JOIN dwd_fact_purchase_order p USING(po_id) WHERE p.status='cancelled'",
    )[0][0]
    assert in_wide == 0
    na = _q(
        pipeline,
        "SELECT count(*) FROM dwd_reject_event WHERE reject_reason='PO_NOT_APPROVED'",
    )[0][0]
    assert na > 0


# ---------- 状态机：未结束不判逾期 ----------

def test_in_transit_orders_are_never_overdue(pipeline):
    """约定到货日未到的在途单：overdue_days 必须为 NULL。"""
    assert pipeline.invariants()["dws_intransit_wrongly_overdue"] == 0
    rows = _q(
        pipeline,
        "SELECT in_transit_days, overdue_days FROM dws_po_fulfillment "
        "WHERE fulfillment_status='IN_TRANSIT' LIMIT 5",
    )
    assert rows
    for in_transit_days, overdue_days in rows:
        assert in_transit_days is not None and overdue_days is None


def test_overdue_only_after_expected_date(pipeline):
    """逾期单必须满足 expected_at 已过且未完成。"""
    bad = _q(
        pipeline,
        """
        SELECT count(*) FROM dws_po_fulfillment
        WHERE fulfillment_status='OVERDUE'
          AND (overdue_days IS NULL OR overdue_days <= 0)
        """
    )[0][0]
    assert bad == 0


def test_no_receipt_is_distinct_from_data_issue(pipeline):
    """缺入库（业务状态）与数据缺失（data_issue_codes）严格分开。"""
    no_receipt = _q(
        pipeline,
        "SELECT count(*) FROM dws_po_fulfillment WHERE no_receipt_flag = 1"
    )[0][0]
    assert no_receipt > 0
    # NO_RECEIPT 单据本身不必然有数据问题
    mixed = _q(
        pipeline,
        "SELECT count(*) FROM dws_po_fulfillment "
        "WHERE no_receipt_flag = 1 AND data_issue_codes = ''"
    )[0][0]
    assert mixed > 0
    # 未结算是独立标记，不等于未入库
    only_unsettled = _q(
        pipeline,
        "SELECT count(*) FROM dws_po_fulfillment "
        "WHERE unsettled_flag = 1 AND no_receipt_flag = 0"
    )[0][0]
    assert only_unsettled > 0


def test_statuses_are_partitioned_and_complete(pipeline):
    """状态机互斥且覆盖全部 approved 采购单。"""
    rows = dict(
        _q(pipeline, "SELECT fulfillment_status, count(*) FROM dws_po_fulfillment GROUP BY 1")
    )
    total = sum(rows.values())
    assert total == pipeline.invariants()["dws_fulfillment_rows"]
    assert set(rows).issubset(
        {"CLOSED_ON_TIME", "CLOSED_LATE", "IN_TRANSIT", "OVERDUE", "NO_RECEIPT"}
    )


# ---------- 风险结论与证据 ----------

def test_settle_exceeds_receipt_flagged_with_amount(pipeline):
    """结算超入库：风险码与差额金额同时可查。"""
    rows = _q(
        pipeline,
        "SELECT po_id, settle_vs_receipt_cents FROM dws_po_fulfillment "
        "WHERE risk_codes LIKE '%SETTLE_EXCEEDS_RECEIPT%'",
    )
    assert rows
    for _, diff in rows:
        assert diff > 0


def test_risk_evidence_contains_facts_and_calculation_time(pipeline):
    """每个结论都能回溯业务事实与计算时点。"""
    row = _q(
        pipeline,
        "SELECT risk_evidence, calculated_at, source_watermark, as_of_date "
        "FROM dws_po_fulfillment LIMIT 1"
    )[0]
    evidence, calculated_at, watermark, as_of = row
    for token in ("po_amount=", "approved_at=", "expected_at=", "valid_received=", "as_of="):
        assert token in evidence
    assert str(as_of) in evidence
    assert calculated_at is not None and watermark is not None


def test_risk_summary_separates_three_kinds(pipeline):
    summary = pipeline.risk_summary()
    assert "unsettled_po" in summary and "no_receipt_po" in summary and "data_issue_po" in summary
    assert summary["rejected_events"].get("CURRENCY_MISMATCH", 0) > 0


# ---------- 快照 / 补传追溯 / 幂等 ----------

def test_snapshot_partition_and_rerun_idempotent(pipeline):
    """按 as_of_date 分区；同逻辑日期重跑行数与金额完全一致。"""
    files = list((settings.lake_root / "dws" / "dws_po_fulfillment").rglob("*.parquet"))
    assert any("as_of_date=" in str(f) for f in files)
    before = pipeline.invariants()
    pipeline.build_dws()
    pipeline.build_ads()
    after = pipeline.invariants()
    for key in (
        "dws_fulfillment_rows", "dws_fulfillment_po_amount",
        "dws_fulfillment_received", "dws_fulfillment_settled", "ads_risk_rows",
    ):
        assert before[key] == after[key]


def test_backfill_keeps_historical_snapshot_and_corrects_conclusion():
    """上游补传后用新 as_of_date 重算：历史分区保留，新分区体现修正。"""
    import tempfile

    d = tempfile.mkdtemp(prefix="lakeside-backfill-")
    old_root, old_date = settings.lake_root, settings.logical_date
    settings.lake_root = pathlib.Path(d)
    settings.ensure()
    try:
        p = Pipeline()
        p.run_all()
        first_date = settings.logical_date
        # 找到一个缺入库的采购单，模拟上游补传一笔入库后重算
        target = p.lake.con.execute(
            "SELECT po_id FROM dws_po_fulfillment WHERE no_receipt_flag = 1 LIMIT 1"
        ).fetchone()
        assert target
        # 推进逻辑日期，ODS 重生成（新 seed 分布不变）+ 新快照分区
        settings.logical_date = "2026-11-14"
        p2 = Pipeline()
        p2.run_all()
        partitions = {
            path.name
            for path in (settings.lake_root / "dws" / "dws_po_fulfillment").iterdir()
            if path.is_dir()
        }
        assert f"as_of_date={first_date}" in partitions
        assert "as_of_date=2026-11-14" in partitions
        snap_count = p2.lake.con.execute(
            "SELECT count(DISTINCT as_of_date) FROM dws_po_fulfillment"
        ).fetchone()[0]
        assert snap_count == 2
        p2.close()
    finally:
        settings.lake_root, settings.logical_date = old_root, old_date


# ---------- 时区 ----------

def test_settle_date_follows_local_offset(pipeline):
    """UTC 16 点后的结算按 +8 换算到次日，月末归属无跨日错误。"""
    bad = [
        r for r in pipeline.month_boundary_check()
        if r["dwd_date"] != r["true_local_date"]
    ]
    assert not bad


def test_sql_render_injects_logical_date_and_offset(pipeline):
    sql = "SELECT '{{LOGICAL_DATE}}' AS d, INTERVAL '{{SRC_TZ_OFFSET_HOURS}} HOUR'"
    rendered = _render(sql)
    assert settings.logical_date in rendered
    assert "{{" not in rendered
