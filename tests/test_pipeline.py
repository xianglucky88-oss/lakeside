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
    """同一个 seed 生成的数据必须完全一致，否则无法回放与断言。

    生成器现在只产出落地批次；landing 文件按内容指纹命名，
    两次生成的内容哈希与文件字节必须一致。
    """
    from lakeside.generator import generate
    import hashlib

    def landing_fingerprint() -> dict[str, str]:
        out = {}
        for t in ("supplier", "purchase_order", "goods_receipt", "settlement"):
            d = lake_root / "landing" / t
            for f in sorted(d.glob("*.parquet")):
                out[f"{t}/{f.name}"] = hashlib.sha256(f.read_bytes()).hexdigest()
        return out

    a = generate(settings, n_suppliers=10, n_po=20)
    first = landing_fingerprint()
    b = generate(settings, n_suppliers=10, n_po=20)
    assert a == b
    assert landing_fingerprint() == first


def test_ods_has_all_four_tables(pipeline):
    res = pipeline.build_ods()
    assert res.rows > 0
    for t in ("supplier", "purchase_order", "goods_receipt", "settlement"):
        assert pipeline.lake.count("ods", t) > 0

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
