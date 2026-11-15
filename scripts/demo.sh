#!/bin/sh
# demo.sh —— 90 秒内录完的固定演示。
#
#   1. 全量构建（landing 落地 → 准入校验/隔离 → ODS → DWD → DWS → ADS）
#   2. 打印各层目录结构（landing / contracts / quarantine / ods …）
#   3. 准入台账：批次结论与逐表 accepted/held/quarantined
#   4. 注入异常批次：类型漂移、时区不一致、迟到引用，观察隔离与暂缓
#   5. 确定性重处理：父记录到达后 held 自动放行；坏数据重跑不会混入
#   6. 重跑整个管线，验证已接受批次不会被重复计入
set -eu
cd "$(dirname "$0")/.."
export LAKE_ROOT="${LAKE_ROOT:-/tmp/lakeside-demo}"
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"

say() { printf '\n=== %s ===\n' "$1"; }

say "1. 全量构建"
rm -rf "$LAKE_ROOT"
python3 -m lakeside build

say "2. 目录结构（landing / contracts / quarantine / 分层表）"
find "$LAKE_ROOT" -name "*.parquet" -o -name "*.json" | sed "s|$LAKE_ROOT/||" | sort | head -25

say "3. 准入台账（批次与逐行结论）"
python3 -m lakeside admission

say "4. 注入异常批次（类型漂移 / 时区错误 / 迟到的入库引用）"
python3 - <<'PY'
from lakeside.config import settings
from lakeside.admission.gate import AdmissionGate

gate = AdmissionGate(settings.lake_root, tz_offset_hours=settings.source_tz_offset_hours)

# 类型漂移 + 引用了不存在的供应商：逐行隔离，不影响同表其他行。
gate.ingest(
    "purchase_order",
    [
        {
            "po_id": "PO-ANOM-1", "supplier_id": "SUP-9999",
            "amount_cents": "not-a-number", "currency": "CNY", "status": "approved",
            "created_at": "2026-10-01 10:00:00", "biz_date": "2026-10-01",
        }
    ],
    "source://purchase_order/anomaly.parquet",
)
# 时区不一致：UTC 时刻与本地业务日对不上 → 隔离。
gate.ingest(
    "settlement",
    [
        {
            "settlement_id": "ST-ANOM-1", "po_id": "PO-00001", "settled_cents": 100,
            "settled_at_utc": "2026-10-03 12:00:00", "biz_date_local": "2026-10-04",
            "channel": "cash",
        }
    ],
    "source://settlement/anomaly.parquet",
)
# 入库先于采购单到达 → held（可恢复的短暂迟到）。
gate.ingest(
    "goods_receipt",
    [
        {
            "gr_id": "GR-ANOM-1", "po_id": "PO-LATE-1", "received_qty": 1,
            "received_cents": 10, "received_at": "2026-10-02 09:00:00",
            "warehouse": "WH-SH-01",
        }
    ],
    "source://goods_receipt/early.parquet",
)

print("未决结论：")
for r in gate.open_decisions():
    print(f"  [{r['state']:<11}] {r['table_name']} {r['natural_key_value']} {r['error_code']}")
PY

say "5. 隔离工作台（证据 + 契约版本 + 恢复条件）"
python3 -m lakeside quarantine || true

say "6. 迟到父记录到达 → held 自动级联放行；随后重跑验证幂等"
python3 - <<'PY'
from lakeside.config import settings
from lakeside.admission.gate import AdmissionGate

gate = AdmissionGate(settings.lake_root, tz_offset_hours=settings.source_tz_offset_hours)
# 先补供应商，再补被引用的采购单：级联重处理解除 GR-ANOM-1 的暂缓。
gate.ingest(
    "supplier",
    [{
        "supplier_id": "SUP-0001", "supplier_name": "演示供应商", "region": "华东",
        "credit_level": "A", "valid_from": "2024-01-01 00:00:00", "valid_to": None,
        "is_current": True, "updated_at": "2024-01-01 03:00:00",
    }],
    "source://supplier/fix.parquet",
)
rep = gate.ingest(
    "purchase_order",
    [{
        "po_id": "PO-LATE-1", "supplier_id": "SUP-0001", "amount_cents": 1000,
        "currency": "CNY", "status": "approved",
        "created_at": "2026-10-01 10:00:00", "biz_date": "2026-10-01",
    }],
    "source://purchase_order/late-parent.parquet",
)
print("级联重处理的下游表：", rep.get("cascaded"))
print("GR-ANOM-1 当前状态：",
      next(r["state"] for r in gate.read_ledger("goods_receipt") if r["natural_key_value"] == "GR-ANOM-1"))
# 对仍隔离的行做确定性重处理：未修复 → 维持隔离，不混入 ODS。
again = gate.reprocess_quarantine("settlement")
print("未修复结算单重处理结果：", again["report"]["status"],
      "（隔离行数仍为", again["report"]["rows_quarantined"], "）")
PY

say "7. 整管线重跑：ODS 行数不变，历史批次不重复计入"
python3 -m lakeside build --keep-ods >/dev/null
python3 -m lakeside admission | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('逐表行台账：', d['row_ledger'])
print('未决结论数：', d['open_decisions'])
"
echo
