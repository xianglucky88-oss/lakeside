"""数据生成器。

生成四张 ODS 表，刻意复刻真实数仓里最常见的数据形态：

  * supplier      供应商主数据。同一 supplier_id 有多条历史记录（不同生效区间），
                  正是 SCD2 的输入。
  * purchase_order 采购单。存在**重复行**（同一 po_id 多行，行内容一致），
                  这是 DWD join 扇出的根源。
  * goods_receipt  入库单。一个 po_id 对应多条入库（分批到货），一对多。
  * settlement     结算单。金额按分存储，有时区偏移的日期字段。

所有数据由 seed 决定，可复现。
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REGIONS = ["华东", "华南", "华北", "西南"]
SUPPLIER_PREFIX = "SUP"
CATEGORIES = ["电子元器件", "包装材料", "物流服务", "办公用品"]


def _write(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def generate(settings, n_suppliers: int = 60, n_po: int = 400, n_gr_per_po: int = 2) -> dict[str, int]:
    """生成 ODS 层数据并返回每张表的行数。"""
    rnd = random.Random(settings.seed)
    base_day = datetime.strptime(settings.logical_date, "%Y-%m-%d")
    settings.ensure()
    counts: dict[str, int] = {}

    # ---------- supplier：每个供应商 1~3 条历史版本 ----------
    sup_rows = []
    for i in range(1, n_suppliers + 1):
        sid = f"{SUPPLIER_PREFIX}-{i:04d}"
        versions = rnd.randint(1, 3)
        # 版本按生效时间递增
        start = base_day - timedelta(days=720)
        for v in range(versions):
            span = rnd.randint(90, 240)
            end = start + timedelta(days=span) if v < versions - 1 else None
            # 名称/等级在版本间会变，这正是要保留的历史
            name = f"{rnd.choice(REGIONS)}{rnd.choice(CATEGORIES)}{i:03d}有限公司"
            sup_rows.append(
                {
                    "supplier_id": sid,
                    "supplier_name": name,
                    "region": rnd.choice(REGIONS),
                    "credit_level": rnd.choice(["A", "B", "C"]),
                    # 生效区间用「本地时间」存，这是历史约定
                    "valid_from": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "valid_to": end.strftime("%Y-%m-%d %H:%M:%S") if end else None,
                    "is_current": v == versions - 1,
                    "updated_at": (start + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            start = end if end else start
    _write(
        settings.ods / "supplier" / "part-0001.parquet",
        pa.Table.from_pylist(sup_rows),
    )
    counts["supplier"] = len(sup_rows)

    # ---------- purchase_order：含重复行 ----------
    po_rows = []
    po_ids = []
    for i in range(1, n_po + 1):
        pid = f"PO-{i:05d}"
        po_ids.append(pid)
        sid = f"{SUPPLIER_PREFIX}-{rnd.randint(1, n_suppliers):04d}"
        created = base_day - timedelta(days=rnd.randint(0, 60), hours=rnd.randint(0, 23))
        row = {
            "po_id": pid,
            "supplier_id": sid,
            "amount_cents": rnd.randint(5_000, 900_000),
            "currency": "CNY",
            "status": rnd.choice(["approved", "approved", "approved", "cancelled"]),
            "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
            # 业务日期（应按本地时区解释）
            "biz_date": created.strftime("%Y-%m-%d"),
        }
        po_rows.append(row)
        # 约 12% 的采购单被上游重复推送（内容完全一致）
        if rnd.random() < 0.12:
            po_rows.append(dict(row))
    _write(settings.ods / "purchase_order" / "part-0001.parquet", pa.Table.from_pylist(po_rows))
    counts["purchase_order"] = len(po_rows)

    # ---------- goods_receipt：一对多 ----------
    gr_rows = []
    gr_seq = 1
    for pid in po_ids:
        n = rnd.randint(1, max(1, n_gr_per_po))
        for k in range(n):
            gr_rows.append(
                {
                    "gr_id": f"GR-{gr_seq:06d}",
                    "po_id": pid,
                    "received_qty": rnd.randint(10, 500),
                    "received_cents": rnd.randint(1_000, 200_000),
                    "received_at": (
                        base_day - timedelta(days=rnd.randint(0, 30), hours=rnd.randint(0, 23))
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "warehouse": rnd.choice(["WH-SH-01", "WH-GZ-02", "WH-BJ-03"]),
                }
            )
            gr_seq += 1
    _write(settings.ods / "goods_receipt" / "part-0001.parquet", pa.Table.from_pylist(gr_rows))
    counts["goods_receipt"] = len(gr_rows)

    # ---------- settlement：带时区偏移的日期 ----------
    st_rows = []
    for i in range(1, 260):
        pid = rnd.choice(po_ids)
        amount = rnd.randint(5_000, 500_000)
        # 结算日期以 UTC 存，但业务上要按本地时区归属到日
        utc_dt = base_day - timedelta(days=rnd.randint(0, 45), hours=rnd.randint(0, 23))
        st_rows.append(
            {
                "settlement_id": f"ST-{i:05d}",
                "po_id": pid,
                "settled_cents": amount,
                "settled_at_utc": utc_dt.strftime("%Y-%m-%d %H:%M:%S"),
                # 历史遗留：这张表多带一个本地日期列，与 settled_at_utc 不一定一致
                "biz_date_local": (utc_dt + timedelta(hours=settings.source_tz_offset_hours)).strftime("%Y-%m-%d"),
                "channel": rnd.choice(["bank_transfer", "acceptance_bill", "cash"]),
            }
        )
    _write(settings.ods / "settlement" / "part-0001.parquet", pa.Table.from_pylist(st_rows))
    counts["settlement"] = len(st_rows)

    return counts
