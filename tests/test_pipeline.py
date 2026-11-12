"""管线测试。用临时湖目录，互不污染。"""

from __future__ import annotations

import os
import tempfile

import pytest

from lakeside.checks import check_invariants
from lakeside.config import settings
from lakeside.engine import Lake
from lakeside.pipeline import Pipeline


@pytest.fixture()
def lake_root():
    d = tempfile.mkdtemp(prefix="lakeside-test-")
    old = settings.lake_root
    settings.lake_root = __import__("pathlib").Path(d)
    settings.ensure()
    yield settings.lake_root
    settings.lake_root = old


@pytest.fixture()
def pipeline(lake_root):
    p = Pipeline()
    yield p
    p.close()


def test_generator_is_deterministic(lake_root):
    """同一个 seed 生成的数据必须完全一致，否则无法回放与断言。"""
    from lakeside.generator import generate

    a = generate(settings, n_suppliers=10, n_po=20)
    first = {
        t: lake_root.joinpath("ods", t, "part-0001.parquet").read_bytes()
        for t in a
    }
    b = generate(settings, n_suppliers=10, n_po=20)
    assert a == b
    for t in first:
        assert lake_root.joinpath("ods", t, "part-0001.parquet").read_bytes() == first[t]


def test_ods_has_all_four_tables(pipeline):
    res = pipeline.build_ods()
    assert res.rows > 0
    for t in ("supplier", "purchase_order", "goods_receipt", "settlement"):
        assert pipeline.lake.count("ods", t) > 0


def test_purchase_order_has_duplicates_by_design(pipeline):
    """上游重复推送是数据事实；DWD 目前不去重。"""
    pipeline.build_ods()
    rows = pipeline.lake.count("ods", "purchase_order")
    distinct = pipeline.lake.con.execute(
        "SELECT count(DISTINCT po_id) FROM ods_purchase_order"
    ).fetchone()[0]
    assert rows > distinct, "generator should produce duplicate po rows"


def test_dwd_does_not_dedup_po(pipeline):
    """当前实现不去重，测试记录现状；行为变更时需同步更新。"""
    pipeline.run_all()
    inv = pipeline.invariants()
    assert inv["dwd_po_rows"] > inv["dwd_po_distinct"]


def test_supplier_dim_loses_history(pipeline):
    """维表只有当前版本，每条 supplier_id 一行。修复 SCD2 后行数会大于供应商数。"""
    pipeline.build_ods()
    pipeline.build_dwd()
    dim_rows = pipeline.lake.count("dwd", "dim_supplier")
    sup_ids = pipeline.lake.con.execute(
        "SELECT count(DISTINCT supplier_id) FROM ods_supplier"
    ).fetchone()[0]
    assert dim_rows == sup_ids
    # ODS 里有多版本
    ods_rows = pipeline.lake.count("ods", "supplier")
    assert ods_rows > sup_ids


def test_amounts_are_fanned_out(pipeline):
    """DWS 金额被放大，这是 join 扇出的直接证据。"""
    pipeline.run_all()
    inv = pipeline.invariants()
    ratio = inv["dws_po_amount_cents"] / inv["ods_po_amount_cents"]
    assert ratio > 2.0, f"expected fan-out above 2x, got {ratio:.2f}"


def test_ads_is_empty(pipeline):
    pipeline.run_all()
    assert pipeline.lake.count("ads", "ads_supplier_monthly") == 0


def test_month_boundary_mismatch(pipeline):
    """月末深夜结算被记到错误日期。"""
    pipeline.run_all()
    rows = pipeline.month_boundary_check()
    assert rows, "expected some late-night settlements"
    bad = [r for r in rows if r["dwd_date"] != r["true_local_date"]]
    assert bad, "expected date mismatches under current timezone handling"


def test_checks_report_failures(pipeline):
    pipeline.run_all()
    results = check_invariants(pipeline.invariants())
    failed = {r["name"] for r in results if not r["ok"]}
    assert "ads.has_output" in failed
    assert "dws.po_amount_conserved" in failed
    assert "dwd.po_dedup" in failed


def test_rerun_is_idempotent_for_facts(pipeline):
    """事实表的整表覆盖是幂等的；
    DWS 读取的是未去重的 DWD，因此金额仍与真值不同。"""
    pipeline.run_all()
    first = pipeline.lake.count("dwd", "fact_goods_receipt")
    pipeline.run_all()
    second = pipeline.lake.count("dwd", "fact_goods_receipt")
    assert first == second


def test_merge_semantics(pipeline):
    """merge 应为"匹配则更新、不匹配则插入"，而不是整表覆盖。"""
    pipeline.build_ods()
    pipeline.lake.write(
        "SELECT 'A' AS k, 1 AS v", "dwd", "_probe_merge"
    )
    pipeline.lake.merge(
        "dwd",
        "_probe_merge",
        "SELECT 'A' AS k, 2 AS v UNION ALL SELECT 'B' AS k, 9 AS v",
        keys=["k"],
        update_set={"v": "v"},
    )
    rows = pipeline.lake.query_df("SELECT k, v FROM dwd__probe_merge ORDER BY k").to_dict("records")
    assert rows == [{"k": "A", "v": 2}, {"k": "B", "v": 9}], rows


def test_scanned_files_for_partitioned_query(pipeline):
    """分区表按分区过滤应只读一个目录下的文件。"""
    pipeline.build_ods()
    pipeline.lake.write(
        "SELECT biz_date, count(*) AS n FROM ods_purchase_order GROUP BY biz_date",
        "ads",
        "_probe_part",
        partition_by="biz_date",
    )
    total_files = pipeline.lake.files("ads", "_probe_part")
    assert total_files > 5
    scanned = pipeline.lake.scanned_files(
        "SELECT * FROM ads__probe_part WHERE biz_date = (SELECT min(biz_date) FROM ads__probe_part)"
    )
    # 全量扫描会引用全部分区文件；裁剪后应显著更少
    assert scanned < total_files
