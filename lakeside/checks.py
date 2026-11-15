"""验证脚本的库函数版本。

`scripts/verify_lake.py` 是给专家在终端跑的可执行版本；
这里提供可被单测直接调用的断言函数，避免逻辑重复。
"""

from __future__ import annotations

from typing import Any


def check_invariants(inv: dict[str, float], tolerance: float = 0.01) -> list[dict[str, Any]]:
    """检查金额守恒。返回每条检查的结果。"""
    out: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        out.append({"name": name, "ok": ok, "detail": detail})

    # 1. DWD 采购单应去重
    rows = inv.get("dwd_po_rows", 0)
    distinct = inv.get("dwd_po_distinct", 0)
    add(
        "dwd.po_dedup",
        rows == distinct,
        f"行数 {rows:.0f} / 去重 {distinct:.0f}（差值 {rows - distinct:.0f}）",
    )

    # 2. DWD 采购金额应与 ODS 去重后一致
    ods_po = inv.get("ods_po_amount_cents", 0)
    dwd_po = inv.get("dwd_po_amount_cents", 0)
    if ods_po:
        ratio = dwd_po / ods_po
        add(
            "dwd.po_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"DWD {dwd_po:,.0f} / ODS {ods_po:,.0f} = {ratio:.4f}",
        )

    # 3. DWS 采购金额守恒（无扇出：比值必须等于 1，而不是被批次数放大）
    dws_po = inv.get("dws_po_amount_cents", 0)
    if ods_po and dws_po:
        ratio = dws_po / ods_po
        add(
            "dws.po_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"DWS {dws_po:,.0f} / ODS {ods_po:,.0f} = {ratio:.4f}",
        )

    # 4. DWS 入库金额守恒（剔除撤销/取消/孤儿后的有效入库）
    ods_gr = inv.get("ods_gr_amount_cents", 0)
    dws_gr = inv.get("dws_received_cents", 0)
    if ods_gr and dws_gr:
        ratio = dws_gr / ods_gr
        add(
            "dws.gr_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"DWS {dws_gr:,.0f} / ODS有效入库 {ods_gr:,.0f} = {ratio:.4f}（隔离 {inv.get('ods_gr_rejected_cents', 0):,.0f}）",
        )

    # 5. DWS 结算金额守恒（同币种有效结算）
    ods_st = inv.get("ods_st_amount_cents", 0)
    dws_st = inv.get("dws_settled_cents", 0)
    if ods_st and dws_st:
        ratio = dws_st / ods_st
        add(
            "dws.st_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"DWS {dws_st:,.0f} / ODS同币种结算 {ods_st:,.0f} = {ratio:.4f}（隔离 {inv.get('ods_st_rejected_cents', 0):,.0f}）",
        )

    # 6. ADS 应有产出
    add("ads.has_output", inv.get("ads_rows", 0) > 0, f"ADS 行数 {inv.get('ads_rows', 0):.0f}")

    # 7. ADS 金额守恒
    ads_po = inv.get("ads_po_amount_cents", 0)
    if ods_po and ads_po:
        ratio = ads_po / ods_po
        add(
            "ads.po_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"ADS {ads_po:,.0f} / ODS {ods_po:,.0f} = {ratio:.4f}",
        )

    # 8. 履约明细行数 = approved 采购单去重数，且金额守恒
    fr_rows = inv.get("dws_fulfillment_rows", 0)
    if fr_rows:
        # 履约表只收 approved 单；用金额守恒间接校验覆盖完整性
        f_po = inv.get("dws_fulfillment_po_amount", 0)
        ratio = f_po / ods_po if ods_po else 0
        add(
            "fulfillment.po_amount_conserved",
            bool(ods_po) and abs(ratio - 1) <= tolerance,
            f"履约宽表采购额 {f_po:,.0f} / ODS {ods_po:,.0f} = {ratio:.4f}，行数 {fr_rows:.0f}",
        )
        add(
            "fulfillment.received_conserved",
            abs(inv.get("dws_fulfillment_received", 0) - dws_gr) <= max(1.0, dws_gr * tolerance),
            f"履约宽表入库 {inv.get('dws_fulfillment_received', 0):,.0f} / 汇总表 {dws_gr:,.0f}",
        )
        add(
            "fulfillment.settled_conserved",
            abs(inv.get("dws_fulfillment_settled", 0) - dws_st) <= max(1.0, dws_st * tolerance),
            f"履约宽表结算 {inv.get('dws_fulfillment_settled', 0):,.0f} / 汇总表 {dws_st:,.0f}",
        )

    # 9. 在途订单不得被误判逾期（核心护栏：未结束订单没有 overdue_days）
    wrongly = inv.get("dws_intransit_wrongly_overdue", 0)
    add(
        "fulfillment.no_overdue_for_in_transit",
        wrongly == 0,
        f"在途却带逾期天数的订单 {wrongly:.0f} 条（必须为 0）",
    )

    # 10. 隔离区必须捕获币种不一致与孤儿事件
    add(
        "reject.currency_mismatch_captured",
        inv.get("dwd_reject_ccy_cnt", 0) > 0,
        f"币种不一致隔离 {inv.get('dwd_reject_ccy_cnt', 0):.0f} 条",
    )
    add(
        "reject.orphan_event_captured",
        inv.get("dwd_reject_orphan_cnt", 0) > 0,
        f"孤儿事件隔离 {inv.get('dwd_reject_orphan_cnt', 0):.0f} 条",
    )

    return out
