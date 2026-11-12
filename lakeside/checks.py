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

    # 3. DWS 采购金额守恒
    dws_po = inv.get("dws_po_amount_cents", 0)
    if ods_po and dws_po:
        ratio = dws_po / ods_po
        add(
            "dws.po_amount_conserved",
            abs(ratio - 1) <= tolerance,
            f"DWS {dws_po:,.0f} / ODS {ods_po:,.0f} = {ratio:.4f}",
        )

    # 4. DWS 入库金额守恒
    ods_gr = inv.get("ods_gr_amount_cents", 0)
    dws_gr = inv.get("dws_received_cents", 0)
    if ods_gr and dws_gr:
        ratio = dws_gr / ods_gr
        add(
            "dws.gr_amount_conserved",
            abs(ratio - 1) <= 0.05,
            f"DWS {dws_gr:,.0f} / ODS {ods_gr:,.0f} = {ratio:.4f}",
        )

    # 5. DWS 结算金额守恒
    ods_st = inv.get("ods_st_amount_cents", 0)
    dws_st = inv.get("dws_settled_cents", 0)
    if ods_st and dws_st:
        ratio = dws_st / ods_st
        add(
            "dws.st_amount_conserved",
            abs(ratio - 1) <= 0.05,
            f"DWS {dws_st:,.0f} / ODS {ods_st:,.0f} = {ratio:.4f}",
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

    return out
