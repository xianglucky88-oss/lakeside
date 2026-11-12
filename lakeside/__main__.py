"""命令行入口。

    python -m lakeside build               全量重建
    python -m lakeside build --layer dwd   只重建某层（含上游）
    python -m lakeside verify              打印守恒指标
    python -m lakeside explain <sql_file>  打印扫描计划
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import settings
from .pipeline import Pipeline, _read_sql


def cmd_build(args) -> int:
    p = Pipeline()
    if args.layer in (None, "all"):
        res = p.run_all()
    else:
        order = ["ods", "dwd", "dws", "ads"]
        idx = order.index(args.layer)
        if idx >= 1:
            p.build_ods(regenerate=not args.keep_ods)
        if idx >= 2:
            p.build_dwd()
        if idx >= 3:
            p.build_dws()
        if idx >= 4:
            p.build_ads()
        res = {
            "logical_date": settings.logical_date,
            "assets": [r.to_dict() for r in p.results],
        }
    print(json.dumps(res, ensure_ascii=False, indent=2))
    p.close()
    return 0


def cmd_verify(args) -> int:
    p = Pipeline()
    inv = p.invariants()
    print(json.dumps(inv, ensure_ascii=False, indent=2))

    problems = []
    if inv.get("dwd_po_rows", 0) > inv.get("dwd_po_distinct", 0):
        problems.append(
            f"DWD 采购单行数 {inv['dwd_po_rows']:.0f} / 去重 po_id {inv['dwd_po_distinct']:.0f}"
        )
    if inv.get("ods_po_amount_cents") and inv.get("dws_po_amount_cents"):
        ratio = inv["dws_po_amount_cents"] / inv["ods_po_amount_cents"]
        if abs(ratio - 1) > 0.01:
            problems.append(f"DWS 采购金额与 ODS 真值之比 = {ratio:.4f}")
    if inv.get("ods_gr_amount_cents") and inv.get("dws_received_cents"):
        ratio = inv["dws_received_cents"] / inv["ods_gr_amount_cents"]
        if abs(ratio - 1) > 0.05:
            problems.append(f"DWS 入库金额与 ODS 真值之比 = {ratio:.4f}")
    if inv.get("ads_rows", 0) == 0:
        problems.append("ADS 产出为空")

    if problems:
        print("\n== 守恒检查：发现问题 ==")
        for i, p_ in enumerate(problems, 1):
            print(f"  {i}. {p_}")
    else:
        print("\n== 守恒检查：全部通过 ==")
    p.close()
    return 0 if not problems else 1


def cmd_explain(args) -> int:
    path = Path(args.sql_file)
    if not path.exists():
        print(f"no such file: {path}", file=sys.stderr)
        return 1
    sql = path.read_text(encoding="utf-8")
    p = Pipeline()
    rows = p.lake.con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
    for r in rows:
        print(r[-1] if isinstance(r, tuple) else r)
    p.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakeside")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="构建管线")
    b.add_argument("--layer", choices=["all", "ods", "dwd", "dws", "ads"], default="all")
    b.add_argument("--keep-ods", action="store_true", help="不重新生成 ODS 数据")
    b.set_defaults(func=cmd_build)

    v = sub.add_parser("verify", help="打印守恒指标并判断")
    v.set_defaults(func=cmd_verify)

    e = sub.add_parser("explain", help="打印某条 SQL 的执行计划")
    e.add_argument("sql_file")
    e.set_defaults(func=cmd_explain)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
