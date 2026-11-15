"""数据生成器。

生成四张 ODS 表，刻意复刻真实数仓里最常见的数据形态：

  * supplier       供应商主数据。同一 supplier_id 有多条历史记录（不同生效区间），
                   正是 SCD2 的输入。
  * purchase_order 采购单。包含三类上游事实：
                     1) 完全一致的重复推送（同一 po_id 同版本两行）；
                     2) 重新批准（同一 po_id 出现 version=2，金额与约定到货日变化）；
                     3) cancelled 单据。
  * goods_receipt  入库单。一个 po_id 对应多条入库（分批到货，一对多），
                   含撤销单（gr_status='voided'）、迟到补传（event 时间早、
                   ingested_at 接近逻辑日期）与孤儿入库。
  * settlement     结算单。金额按分存储，UTC 时间戳，含币种不一致与孤儿结算。

所有事件表带 ingested_at（进入 ODS 的时点），用于区分"业务事件时间"与
"数据可见时间"，是双时态快照与补传追溯的基础。数据由 seed 决定，可复现。
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
WAREHOUSES = ["WH-SH-01", "WH-GZ-02", "WH-BJ-03"]
FOREIGN_CCY = ["USD", "EUR"]


def _write(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


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

    # ---------- purchase_order：重复推送 / 重新批准 / 取消 ----------
    po_rows: list[dict] = []
    po_ids: list[str] = []
    po_info: dict[str, dict] = {}
    for i in range(1, n_po + 1):
        pid = f"PO-{i:05d}"
        po_ids.append(pid)
        sid = f"{SUPPLIER_PREFIX}-{rnd.randint(1, n_suppliers):04d}"
        created = base_day - timedelta(days=rnd.randint(0, 60), hours=rnd.randint(0, 23))
        status = rnd.choice(["approved", "approved", "approved", "cancelled"])
        amount = rnd.randint(5_000, 900_000)
        expected = created + timedelta(days=rnd.randint(5, 30), hours=rnd.randint(0, 23))
        ingested = min(base_day, created + timedelta(hours=rnd.randint(1, 36)))
        row = {
            "po_id": pid,
            "supplier_id": sid,
            "amount_cents": amount,
            "currency": "CNY",
            "status": status,
            "version": 1,
            "created_at": _fmt(created),
            # 业务日期（应按本地时区解释）
            "biz_date": created.strftime("%Y-%m-%d"),
            "approved_at": _fmt(created),
            "expected_at": _fmt(expected),
            "ingested_at": _fmt(ingested),
        }
        po_rows.append(row)
        po_info[pid] = {
            "status": status,
            "amount": amount,
            "approved_at": created,
            "expected_at": expected,
        }
        roll = rnd.random()
        if roll < 0.12:
            # 约 12% 的采购单被上游重复推送（内容完全一致，含版本号）
            po_rows.append(dict(row))
        elif roll < 0.20 and status == "approved":
            # 约 8% 被重新批准：version=2，金额与约定到货日都可能调整。
            # 新批准时间晚于首次批准，时效口径以最新一次批准重新起算。
            reapproved = created + timedelta(days=rnd.randint(1, 10), hours=rnd.randint(0, 23))
            new_expected = expected + timedelta(days=rnd.randint(3, 15))
            new_amount = max(1_000, int(amount * rnd.uniform(0.8, 1.2)))
            po_rows.append(
                {
                    **row,
                    "amount_cents": new_amount,
                    "version": 2,
                    "approved_at": _fmt(reapproved),
                    "expected_at": _fmt(new_expected),
                    "ingested_at": _fmt(min(base_day, reapproved + timedelta(hours=rnd.randint(1, 24)))),
                }
            )
            po_info[pid].update(
                amount=new_amount, approved_at=reapproved, expected_at=new_expected, reapproved=True
            )
    _write(
        settings.ods / "purchase_order" / "part-0001.parquet",
        pa.Table.from_pylist(po_rows),
    )
    counts["purchase_order"] = len(po_rows)

    # ---------- goods_receipt：分批 / 撤销 / 迟到补传 / 缺入库 / 孤儿 ----------
    gr_rows = []
    gr_seq = 1
    approved_pids = [p for p in po_ids if po_info[p]["status"] == "approved"]

    def _gr(pid: str, cents: int, when: datetime, status_flag: str, late: bool) -> None:
        nonlocal gr_seq
        if late:
            # 业务事件发生在很久以前，数据却在逻辑日期前 1 天内才进湖
            ing = base_day - timedelta(hours=rnd.randint(1, 20))
        else:
            ing = min(base_day, when + timedelta(hours=rnd.randint(1, 36)))
        gr_rows.append(
            {
                "gr_id": f"GR-{gr_seq:06d}",
                "po_id": pid,
                "received_qty": rnd.randint(10, 500),
                "received_cents": cents,
                "received_at": _fmt(when),
                "warehouse": rnd.choice(WAREHOUSES),
                "gr_status": status_flag,
                "ingested_at": _fmt(ing),
            }
        )
        gr_seq += 1

    for pid in po_ids:
        info = po_info[pid]
        # 取消单大多数没有入库；少量混入在途入库，用于验证拒绝规则
        if info["status"] == "cancelled" and rnd.random() < 0.7:
            continue
        if info["status"] == "approved" and rnd.random() < 0.10:
            # 约 10% 的有效采购单完全没有入库（缺入库 / 未到货场景）
            continue
        n_batches = rnd.randint(1, max(1, n_gr_per_po) + 1)
        start = info["approved_at"]
        # 入库金额围绕采购金额分布，刻意制造欠交、足额与超交
        target_ratio = rnd.uniform(0.35, 1.25)
        total_cents = max(n_batches * 100, int(info["amount"] * target_ratio))
        share = total_cents // n_batches
        cursor = start
        for k in range(n_batches):
            cursor = cursor + timedelta(days=rnd.randint(1, 12), hours=rnd.randint(0, 23))
            when = min(cursor, base_day - timedelta(hours=1))
            cents = share if k < n_batches - 1 else max(100, total_cents - share * (n_batches - 1))
            is_voided = rnd.random() < 0.05
            late = rnd.random() < 0.08
            _gr(pid, cents, when, "voided" if is_voided else "valid", late)
            if is_voided:
                # 撤销后通常补一笔替代入库
                replacement_when = min(when + timedelta(days=rnd.randint(1, 4)), base_day - timedelta(hours=1))
                _gr(pid, cents, replacement_when, "valid", late)

    # 孤儿入库：po_id 在采购单表里不存在，必须进入隔离区而不是静默丢弃
    for _ in range(2):
        when = base_day - timedelta(days=rnd.randint(2, 20))
        _gr("PO-000000", rnd.randint(1_000, 50_000), when, "valid", False)
    _write(
        settings.ods / "goods_receipt" / "part-0001.parquet",
        pa.Table.from_pylist(gr_rows),
    )
    counts["goods_receipt"] = len(gr_rows)

    # ---------- settlement：UTC 时间 / 币种不一致 / 超入库结算 / 孤儿 ----------
    st_rows = []
    # 明确制造若干笔"结算超过实际入库"的风险样本
    over_settle_pids = rnd.sample(approved_pids, k=min(8, len(approved_pids)))
    received_by_po: dict[str, int] = {}
    for r in gr_rows:
        if r["gr_status"] == "valid" and r["po_id"] in po_info and po_info[r["po_id"]]["status"] == "approved":
            received_by_po[r["po_id"]] = received_by_po.get(r["po_id"], 0) + r["received_cents"]

    def _st(seq: int, pid: str, cents: int, ccy: str) -> None:
        utc_dt = base_day - timedelta(days=rnd.randint(0, 45), hours=rnd.randint(0, 23))
        local_dt = utc_dt + timedelta(hours=settings.source_tz_offset_hours)
        st_rows.append(
            {
                "settlement_id": f"ST-{seq:05d}",
                "po_id": pid,
                "settled_cents": cents,
                "currency": ccy,
                "settled_at_utc": _fmt(utc_dt),
                # 历史遗留：本地日期列，口径正确时应与 UTC 换算结果一致
                "biz_date_local": local_dt.strftime("%Y-%m-%d"),
                "ingested_at": _fmt(min(base_day, local_dt + timedelta(hours=rnd.randint(1, 24)))),
                "channel": rnd.choice(["bank_transfer", "acceptance_bill", "cash"]),
            }
        )

    seq = 1
    for pid in over_settle_pids:
        # 结算额 = 已入库额 * 1.2，保证触发"结算超入库"
        _st(seq, pid, int(received_by_po.get(pid, 10_000) * 1.2), "CNY")
        seq += 1
    for _ in range(260):
        pid = rnd.choice(approved_pids)
        ccy = rnd.choice(FOREIGN_CCY) if rnd.random() < 0.06 else "CNY"
        _st(seq, pid, rnd.randint(5_000, 500_000), ccy)
        seq += 1
    # 孤儿结算：同样必须隔离
    for _ in range(2):
        _st(seq, "PO-000000", rnd.randint(5_000, 50_000), "CNY")
        seq += 1
    _write(
        settings.ods / "settlement" / "part-0001.parquet",
        pa.Table.from_pylist(st_rows),
    )
    counts["settlement"] = len(st_rows)

    return counts
