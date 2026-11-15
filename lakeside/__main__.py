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
from .admission.gate import AdmissionGate
from .admission import models as M


def _gate() -> AdmissionGate:
    return AdmissionGate(
        settings.lake_root, tz_offset_hours=settings.source_tz_offset_hours
    )


def cmd_admission(args) -> int:
    """打印准入台账：批次结论 + 逐表 accepted/held/quarantined。"""
    gate = _gate()
    batches = gate.read_batches()
    if args.table:
        batches = [b for b in batches if b["table_name"] == args.table]
    out = {
        "batches": batches,
        "row_ledger": {
            t: _count_states(gate, t) for t in gate.registry.tables()
            if not args.table or t == args.table
        },
        "open_decisions": len(
            gate.open_decisions(args.table) if args.table else gate.open_decisions()
        ),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    return 0


def _count_states(gate: AdmissionGate, table: str) -> dict[str, int]:
    states: dict[str, int] = {}
    for r in gate.read_ledger(table):
        states[r["state"]] = states.get(r["state"], 0) + 1
    return states


def cmd_quarantine(args) -> int:
    """列出仍处于 held / quarantined 的行及其证据与恢复条件。"""
    gate = _gate()
    rows = gate.open_decisions(args.table)
    if not args.json:
        if not rows:
            print("没有待处理的暂缓/隔离记录。")
            return 0
        for r in rows:
            print(
                f"[{r['state']:<11}] {r['table_name']} key={r['natural_key_value']} "
                f"code={r['error_code']} contract=v{r['contract_version']}\n"
                f"    证据: {r['source_file']}#L{r['source_row_number']} hash={r['content_hash'][:12]}\n"
                f"    原因: {r['error_detail']}\n"
                f"    恢复: {r['recovery_condition']}"
            )
        return 1 if any(r["state"] == M.QUARANTINED for r in rows) else 0
    print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_reprocess(args) -> int:
    """对一张表的 held/quarantined 行做确定性重处理。"""
    gate = _gate()
    result = gate.reprocess_quarantine(args.table)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    report = result.get("report")
    if report and report["status"] == M.BATCH_DUPLICATE:
        print("（批次内容与首次接入一致：维持原结论，未重复计入下游）")
    return 0


def cmd_contracts(args) -> int:
    """查看已注册契约的版本与字段。"""
    gate = _gate()
    out = {}
    for table in gate.registry.tables():
        if args.table and table != args.table:
            continue
        out[table] = [
            {
                "version": c.version,
                "natural_key": c.natural_key,
                "uniqueness_key": c.uniqueness_key,
                "fields": [
                    {"name": f.name, "type": f.type, "required": f.required,
                     "tz": f.tz_semantics}
                    for f in c.fields
                ],
            }
            for c in gate.registry.all_versions(table)
        ]
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


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

    a = sub.add_parser("admission", help="查看准入批次与逐表结论")
    a.add_argument("--table", choices=["supplier", "purchase_order", "goods_receipt", "settlement"])
    a.set_defaults(func=cmd_admission)

    q = sub.add_parser("quarantine", help="列出暂缓/隔离记录、证据与恢复条件")
    q.add_argument("--table", choices=["supplier", "purchase_order", "goods_receipt", "settlement"])
    q.add_argument("--json", action="store_true")
    q.set_defaults(func=cmd_quarantine)

    r = sub.add_parser("reprocess", help="重处理一张表的暂缓/隔离行")
    r.add_argument("table", choices=["supplier", "purchase_order", "goods_receipt", "settlement"])
    r.set_defaults(func=cmd_reprocess)

    c = sub.add_parser("contracts", help="查看契约版本与字段定义")
    c.add_argument("--table", choices=["supplier", "purchase_order", "goods_receipt", "settlement"])
    c.set_defaults(func=cmd_contracts)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
