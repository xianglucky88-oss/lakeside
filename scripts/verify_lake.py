#!/usr/bin/env python3
"""verify_lake.py —— 湖仓金额守恒与结构断言。

这是本仓库最核心的"客观证据"工具：
每一项都输出具体数字，而不是"通过/失败"两个字，
所以在 90 秒录屏里能直接把差异念出来。

用法:
    python3 scripts/verify_lake.py                 # 需要先 build
    python3 scripts/verify_lake.py --build         # 先构建再检查
    python3 scripts/verify_lake.py --json          # 机器可读

退出码: 0 全部通过；1 有问题。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lakeside.checks import check_invariants  # noqa: E402
from lakeside.pipeline import Pipeline  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="先跑一次全量构建")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--tolerance", type=float, default=0.01)
    args = ap.parse_args()

    p = Pipeline()
    if args.build:
        p.run_all()

    inv = p.invariants()
    results = check_invariants(inv, tolerance=args.tolerance)
    month = p.month_boundary_check()
    mismatches = [r for r in month if r["dwd_date"] != r["true_local_date"]]
    risks = p.risk_summary()

    if args.json:
        print(
            json.dumps(
                {
                    "invariants": inv,
                    "checks": results,
                    "risks": risks,
                    "month_boundary": {
                        "checked": len(month),
                        "mismatch": len(mismatches),
                        "samples": mismatches[:5],
                    },
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
    else:
        print("== 关键指标 ==")
        for k, v in inv.items():
            print(f"  {k:36} {v:>18,.0f}")
        print("\n== 守恒断言 ==")
        for r in results:
            print(f"  [{'PASS' if r['ok'] else 'FAIL'}] {r['name']}: {r['detail']}")
        if risks:
            print("\n== 履约状态分布（as_of={}） ==".format(risks.get("as_of_date")))
            for k, v in sorted(risks.get("status_counts", {}).items()):
                print(f"  {k:16} {v:>6}")
            print("  -- 风险码 --")
            for k, v in sorted(risks.get("risk_code_counts", {}).items()):
                print(f"  {k:24} {v:>6}")
            print(
                "  未结算 {unsettled_po} ｜ 未入库 {no_receipt_po} ｜ 数据缺失 {data_issue_po}".format(
                    **risks
                )
            )
            print("  -- 隔离事件 --")
            for k, v in sorted(risks.get("rejected_events", {}).items()):
                print(f"  {k:24} {v:>6}")
        print(f"\n== 月末归属 ==\n  抽查 {len(month)} 条深夜结算，跨日错误 {len(mismatches)} 条")
        for r in mismatches[:3]:
            print(
                f"    {r['settlement_id']}  UTC {r['settled_at_utc']} -> "
                f"按 UTC 记 {r['dwd_date']}，实际本地 {r['true_local_date']}"
            )
        failed = sum(1 for r in results if not r["ok"])
        print(f"\n结果: {len(results) - failed}/{len(results)} 通过（月末不一致 {len(mismatches)} 条）")

    p.close()
    failed = sum(1 for r in results if not r["ok"]) + (1 if mismatches else 0)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
