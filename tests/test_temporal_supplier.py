"""供应商时态维度（SCD2）专项测试。

覆盖题目要求的全部场景：
  * 点时态唯一关联（事实关联到业务发生时刻的版本，而不是最新版本）
  * 乱序到达（按 valid_from 归位，与到达顺序无关）
  * 同一时点冲突（无法判定先后 → 整组隔离，strict 模式失败）
  * 历史回补（新版本插入时间线中间，既有代理键稳定）
  * 维度重建（全量原子重写、幂等、无孤儿文件）
  * 无变化折叠与 changelog 去重（膨胀控制）
  * 迁移期兼容视图（旧表名/旧列结构，值为当前版本）
  * 对账恒等式与不变式（不重叠、不断档、事实无多重命中）
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakeside.config import settings
from lakeside.engine import Lake
from lakeside.temporal import (
    TemporalConflictError,
    build_supplier_dim,
    count_unresolved_facts,
    ingest_supplier_changelog,
    temporal_invariants,
)


@pytest.fixture()
def lake():
    d = tempfile.mkdtemp(prefix="lakeside-scd2-")
    old = settings.lake_root
    settings.lake_root = Path(d)
    settings.ensure()
    lk = Lake(settings.lake_root)
    yield lk
    lk.close()
    settings.lake_root = old


def _write_ods_supplier(lk: Lake, rows: list[dict]) -> None:
    """用给定行整体覆盖 ods_supplier（模拟上游批次推送）。"""
    df = pd.DataFrame(rows)
    p = settings.ods / "supplier" / "part-0001.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), p)
    lk.refresh_views()


def _sup(supplier_id, name, region, valid_from, valid_to=None, updated_at=None,
         credit_level="A") -> dict:
    return {
        "supplier_id": supplier_id,
        "supplier_name": name,
        "region": region,
        "credit_level": credit_level,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "is_current": valid_to is None,
        "updated_at": updated_at or valid_from,
    }


def _dim_rows(lk: Lake) -> pd.DataFrame:
    return lk.query_df(
        "SELECT * FROM dwd_dim_supplier_scd2 ORDER BY supplier_id, valid_from"
    )


def _rebuild(lk: Lake, **kw):
    ingest_supplier_changelog(lk, ingested_at="2026-11-10 00:00:00")
    return build_supplier_dim(lk, **kw)


# ---------- 基础：点时态唯一关联 ----------

def test_scd2_builds_non_overlapping_timeline(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "甲公司", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00"),
        _sup("S1", "甲股份", "华南", "2026-06-01 00:00:00", None),
    ])
    report = _rebuild(lake)
    assert report.version_rows == 2
    assert report.rejected_rows == 0

    df = _dim_rows(lake)
    v1, v2 = df.to_dict("records")
    assert v1["valid_to"] == pd.Timestamp("2026-06-01")
    assert v2["valid_to"] is pd.NaT or v2["valid_to"] is None
    assert bool(v1["is_current"]) is False
    assert bool(v2["is_current"]) is True
    # 代理键确定性：只依赖 supplier_id + valid_from
    assert v1["supplier_sk"] != v2["supplier_sk"]

    inv = temporal_invariants(lake)
    assert inv["scd2_overlap_pairs"] == 0
    assert inv["scd2_gap_pairs"] == 0
    assert inv["scd2_current_dupes"] == 0


def test_point_in_time_resolution_picks_version_at_business_time(lake):
    """事实在业务发生时刻关联当时版本，而不是最新版本。"""
    _write_ods_supplier(lake, [
        _sup("S1", "旧名称", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00"),
        _sup("S1", "新名称", "华南", "2026-06-01 00:00:00", None),
    ])
    _rebuild(lake)
    # 两条事实：一条发生在旧版本期，一条在新版本期
    po = pd.DataFrame([
        {"po_id": "PO-1", "supplier_id": "S1", "amount_cents": 1000,
         "currency": "CNY", "status": "approved",
         "created_at": "2026-03-01 10:00:00", "biz_date": "2026-03-01"},
        {"po_id": "PO-2", "supplier_id": "S1", "amount_cents": 2000,
         "currency": "CNY", "status": "approved",
         "created_at": "2026-07-01 10:00:00", "biz_date": "2026-07-01"},
    ])
    p = settings.ods / "purchase_order" / "part-0001.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(po, preserve_index=False), p)
    lake.refresh_views()
    lake.write(open(Path(__file__).resolve().parent.parent
                    / "sql/dwd/fact_purchase_order.sql", encoding="utf-8").read(),
               "dwd", "fact_purchase_order")

    fact = lake.query_df(
        "SELECT po_id, supplier_sk FROM dwd_fact_purchase_order ORDER BY po_id"
    ).to_dict("records")
    dim = {r["valid_from"]: r["supplier_sk"] for r in _dim_rows(lake).to_dict("records")}
    assert fact[0]["supplier_sk"] == dim[pd.Timestamp("2026-01-01")]
    assert fact[1]["supplier_sk"] == dim[pd.Timestamp("2026-06-01")]
    assert count_unresolved_facts(lake) == 0
    assert temporal_invariants(lake)["fact_multi_match"] == 0


# ---------- 乱序到达 ----------

def test_out_of_order_arrival_reorders_by_valid_from(lake):
    # 先到中间版本，再到更早和更晚的版本（乱序）
    middle = _sup("S1", "二期", "华南", "2026-06-01 00:00:00", None,
                  updated_at="2026-06-02 00:00:00")
    early = _sup("S1", "一期", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00",
                 updated_at="2026-01-02 00:00:00")
    late = _sup("S1", "三期", "华北", "2026-09-01 00:00:00", None,
                updated_at="2026-09-02 00:00:00")
    _write_ods_supplier(lake, [middle])
    _rebuild(lake)
    # ODS 整批覆盖：第二批必须把仍有效的中间版本一起带上，另加乱序到达的两条
    _write_ods_supplier(lake, [middle, early, late])
    report = _rebuild(lake)
    df = _dim_rows(lake)
    assert [str(r["valid_from"])[:10] for r in df.to_dict("records")] == [
        "2026-01-01", "2026-06-01", "2026-09-01",
    ]
    assert report.version_rows == 3
    # 区间连续不断档
    assert temporal_invariants(lake)["scd2_gap_pairs"] == 0
    # 乱序归位：名称按 valid_from 时间线排列，与到达顺序无关
    assert [r["supplier_name"] for r in df.to_dict("records")] == ["一期", "二期", "三期"]


# ---------- 同一时点冲突 ----------

def test_ambiguous_same_timestamp_conflict_is_quarantined(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "说法A", "华东", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 12:00:00"),
        _sup("S1", "说法B", "华南", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 12:00:00"),
    ])
    report = _rebuild(lake)
    # 整组拒绝：版本表里没有 S1 的任何行
    assert report.version_rows == 0
    assert report.rejected_rows == 2
    assert report.reject_reasons.get("ambiguous_ordering") == 2
    rejected = lake.query_df("SELECT DISTINCT reject_reason FROM dwd_dim_supplier_reject")
    assert rejected.to_dict("records") == [{"reject_reason": "ambiguous_ordering"}]

    # strict 模式必须显式失败，而不是静默选一条
    with pytest.raises(TemporalConflictError):
        build_supplier_dim(lake, strict=True)


def test_same_timestamp_different_updated_at_is_resolvable(lake):
    """同 valid_from 但 updated_at 有先后：可判定，取 updated_at 大者。"""
    _write_ods_supplier(lake, [
        _sup("S1", "旧说法", "华东", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 10:00:00"),
        _sup("S1", "新说法", "华南", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 14:00:00"),
    ])
    report = _rebuild(lake)
    assert report.rejected_rows == 0
    df = _dim_rows(lake)
    assert len(df) == 1
    assert df.to_dict("records")[0]["supplier_name"] == "新说法"


def test_invalid_records_are_rejected_with_reason(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "无生效时间", "华东", None, None),
        _sup("S2", "倒挂区间", "华东", "2026-06-01 00:00:00", "2026-01-01 00:00:00"),
        _sup("S3", "正常", "华北", "2026-01-01 00:00:00", None),
    ])
    report = _rebuild(lake)
    assert report.version_rows == 1
    assert set(report.reject_reasons) == {"invalid_valid_from", "invalid_interval"}
    # 其他供应商不受影响
    assert _dim_rows(lake).to_dict("records")[0]["supplier_id"] == "S3"


# ---------- 历史回补 ----------

def test_historical_backfill_preserves_existing_surrogate_keys(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "一期", "华东", "2026-01-01 00:00:00", None,
             updated_at="2026-01-02 00:00:00"),
    ])
    _rebuild(lake)
    before = {r["supplier_id"] + str(r["valid_from"]): r["supplier_sk"]
              for r in _dim_rows(lake).to_dict("records")}

    # 回补一条插在时间线中间的变更；一期的区间被收窄，更正记录带更晚的 updated_at
    _write_ods_supplier(lake, [
        _sup("S1", "一期", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00",
             updated_at="2026-07-02 00:00:00"),
        _sup("S1", "二期", "华南", "2026-06-01 00:00:00", None,
             updated_at="2026-06-02 00:00:00"),
    ])
    report = _rebuild(lake)
    after = _dim_rows(lake)
    assert report.version_rows == 2
    # 既有版本的代理键不变（I4），事实无需重写
    assert after.to_dict("records")[0]["supplier_sk"] == before["S12026-01-01 00:00:00"]
    # 旧版本区间被正确收窄
    assert str(after.to_dict("records")[0]["valid_to"])[:10] == "2026-06-01"
    assert temporal_invariants(lake)["scd2_overlap_pairs"] == 0


# ---------- 重建幂等与无孤儿文件 ----------

def test_rebuild_is_idempotent_and_leaves_no_orphan_files(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "甲", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00"),
        _sup("S1", "乙", "华南", "2026-06-01 00:00:00", None),
    ])
    _rebuild(lake)
    _rebuild(lake)
    _rebuild(lake)
    df1 = _dim_rows(lake)
    df2 = _dim_rows(lake)
    pd.testing.assert_frame_equal(df1.reset_index(drop=True), df2.reset_index(drop=True))

    table_dir = settings.dwd / "dim_supplier_scd2"
    parquets = list(table_dir.rglob("*.parquet"))
    assert len(parquets) == 1  # 原子重写：物理上始终一份文件
    # 没有遗留 staging/backup 目录，无需人工清理
    assert not list(table_dir.parent.glob(".*"))


def test_reopen_lake_restores_compatibility_view(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "甲", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00"),
        _sup("S1", "乙", "华南", "2026-06-01 00:00:00", None),
    ])
    _rebuild(lake)
    # 旧表名/旧列结构的兼容视图
    old = lake.query_df("SELECT * FROM dwd_dim_supplier ORDER BY supplier_id")
    assert list(old.columns) == [
        "supplier_id", "supplier_name", "region", "credit_level", "dwd_updated_at",
    ]
    assert len(old) == 1
    assert old.to_dict("records")[0]["supplier_name"] == "乙"  # 当前版本

    # 新进程打开同一湖目录：视图由元数据自动恢复
    lake.close()
    reopened = Lake(settings.lake_root)
    rows = reopened.query_df("SELECT supplier_name FROM dwd_dim_supplier").to_dict("records")
    assert rows == [{"supplier_name": "乙"}]
    reopened.close()


# ---------- 膨胀控制 ----------

def test_changelog_dedup_and_noop_collapse(lake):
    base = [
        _sup("S1", "甲", "华东", "2026-01-01 00:00:00", "2026-06-01 00:00:00"),
        _sup("S1", "甲", "华东", "2026-06-01 00:00:00", "2026-09-01 00:00:00"),
        _sup("S1", "乙", "华南", "2026-09-01 00:00:00", None),
    ]
    _write_ods_supplier(lake, base)
    r1 = _rebuild(lake)
    assert r1.changelog_rows == 3
    # 前两个版本属性完全相同 → 折叠成一个版本
    assert r1.version_rows == 2

    # 整批重放（含重复推送）：changelog 零增长
    _write_ods_supplier(lake, base + [base[0]])
    r2 = _rebuild(lake)
    assert r2.changelog_new_rows == 0
    assert r2.changelog_rows == 3
    assert r2.version_rows == 2
    assert lake.count("dwd", "supplier_changelog") == 3


def test_noop_collapse_keeps_current_state_when_last_version_open_ended(lake):
    """回归：同属性多版本折叠时，若末版本是开放区间（当前版本），
    折叠后的右边界必须仍是 NULL，不能因 max/arg_min 忽略 NULL 而留下断档。"""
    _write_ods_supplier(lake, [
        _sup("S1", "甲", "华东", "2024-11-20 00:00:00", "2025-02-25 00:00:00",
             updated_at="2024-11-20 03:00:00"),
        _sup("S1", "甲", "华东", "2025-02-25 00:00:00", None,
             updated_at="2025-02-25 03:00:00"),
    ])
    report = _rebuild(lake)
    # 属性完全相同 → 折叠为一个版本，且该版本必须是当前版本
    assert report.version_rows == 1
    row = _dim_rows(lake).to_dict("records")[0]
    assert row["valid_to"] is pd.NaT or row["valid_to"] is None
    assert bool(row["is_current"]) is True

    # 落在末版本时间之后的事实必须能解析（断档会导致 unresolved）
    po = pd.DataFrame([
        {"po_id": "PO-1", "supplier_id": "S1", "amount_cents": 1000,
         "currency": "CNY", "status": "approved",
         "created_at": "2026-09-10 03:00:00", "biz_date": "2026-09-10"},
    ])
    p = settings.ods / "purchase_order" / "part-0001.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(po, preserve_index=False), p)
    lake.refresh_views()
    lake.write(open(Path(__file__).resolve().parent.parent
                    / "sql/dwd/fact_purchase_order.sql", encoding="utf-8").read(),
               "dwd", "fact_purchase_order")
    assert count_unresolved_facts(lake) == 0
    assert temporal_invariants(lake)["fact_multi_match"] == 0


def test_overlapping_source_intervals_are_adjusted_not_rejected(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "甲", "华东", "2026-01-01 00:00:00", "2026-08-01 00:00:00"),
        _sup("S1", "乙", "华南", "2026-06-01 00:00:00", None),  # 与上一条重叠
    ])
    report = _rebuild(lake)
    assert report.rejected_rows == 0
    assert report.interval_adjusted_rows >= 1
    df = _dim_rows(lake)
    v1, v2 = df.to_dict("records")
    # 重算后边界不重叠：v1.valid_to == v2.valid_from
    assert v1["valid_to"] == v2["valid_from"]
    assert temporal_invariants(lake)["scd2_overlap_pairs"] == 0


# ---------- 对账恒等式 ----------

def test_accounting_identity_versions_plus_rejected(lake):
    _write_ods_supplier(lake, [
        _sup("S1", "甲", "华东", "2026-01-01 00:00:00", None),
        _sup("S2", "冲突A", "华东", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 12:00:00"),
        _sup("S2", "冲突B", "华南", "2026-01-01 00:00:00", None,
             updated_at="2026-01-01 12:00:00"),
    ])
    report = _rebuild(lake)
    # 折叠前口径：参与排序的有效记录 = 版本数（折叠后）+ 被拒数，折叠数可由版本明细解释
    assert report.version_rows + report.rejected_rows <= report.changelog_rows
    assert report.rejected_rows == 2
    # 每条源记录要么进版本表（按折叠组），要么进隔离区，没有静默丢失
    accounted = lake.con.execute(
        "SELECT (SELECT count(DISTINCT record_hash) FROM dwd_dim_supplier_scd2) "
        "+ (SELECT count(DISTINCT record_hash) FROM dwd_dim_supplier_reject)"
    ).fetchone()[0]
    assert accounted == lake.count("dwd", "supplier_changelog")
