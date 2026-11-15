"""准入层的状态、错误码与物理存储 schema。

四类结论：
    accepted     放行，可进入可消费 ODS；
    held         暂缓。属于可恢复的短暂迟到（引用的父记录尚未到达），
                 父记录到达后重跑即自动放行；
    quarantined  隔离。不可修复的源数据错误（类型漂移、时间格式异常、
                 时区不一致、同键冲突……），源系统修复并明确重放前永不混入事实层；
    superseded   已被同键的更正版本替换（仅出现在行台账里，保留血缘）。

批次级结论：
    accepted     全部行放行或部分隔离（有效数据不被异常记录阻塞）；
    partial      有行被隔离/暂缓但至少一行放行；
    held         破坏性契约变更或批次内无任何可放行行；
    duplicate    内容与已处理批次完全相同，整批跳过，不重复计入下游；
    unavailable  源表/源文件不可用（局部故障，不影响其他表）。
"""

from __future__ import annotations

import pyarrow as pa

# 行级状态
ACCEPTED = "accepted"
HELD = "held"
QUARANTINED = "quarantined"
SUPERSEDED = "superseded"

ROW_STATES = (ACCEPTED, HELD, QUARANTINED, SUPERSEDED)

# 批次级状态
BATCH_ACCEPTED = "accepted"
BATCH_PARTIAL = "partial"
BATCH_HELD = "held"
BATCH_DUPLICATE = "duplicate"
BATCH_UNAVAILABLE = "unavailable"

# 错误码 → 默认处置（held / quarantined）
ERR_SCHEMA_BREAKING = "SCHEMA_BREAKING"          # 批次级 hold
ERR_MISSING_FIELD = "MISSING_FIELD"              # 隔离
ERR_TYPE_DRIFT = "TYPE_DRIFT"                    # 隔离
ERR_UNKNOWN_FIELD = "UNKNOWN_FIELD"              # 契约演进处理；演进失败则批次 hold
ERR_BAD_TIMESTAMP = "BAD_TIMESTAMP"              # 隔离（时间格式异常）
ERR_TIMEZONE_MISMATCH = "TIMEZONE_MISMATCH"      # 隔离（时区不一致）
ERR_DUPLICATE_KEY = "DUPLICATE_KEY"              # 同批同键内容冲突 → 隔离
ERR_MISSING_REFERENCE = "MISSING_REFERENCE"      # 父记录未到 → 暂缓
ERR_DANGLING_REFERENCE = "DANGLING_REFERENCE"    # 父键已被隔离/不可达 → 隔离
ERR_REQUIRED_NULL = "REQUIRED_NULL"              # 隔离

# 每种错误的默认处置与恢复条件文案。
ERROR_POLICY: dict[str, dict[str, str]] = {
    ERR_MISSING_FIELD: {
        "disposition": QUARANTINED,
        "recovery": "源系统补齐字段后以同业务键重放该记录",
    },
    ERR_TYPE_DRIFT: {
        "disposition": QUARANTINED,
        "recovery": "源系统按契约类型修正后重放；若确需变更类型，走契约破坏性变更迁移",
    },
    ERR_BAD_TIMESTAMP: {
        "disposition": QUARANTINED,
        "recovery": "源系统修正为合法时间格式后重放",
    },
    ERR_TIMEZONE_MISMATCH: {
        "disposition": QUARANTINED,
        "recovery": "源系统对齐 UTC 时刻与本地业务日后重放",
    },
    ERR_DUPLICATE_KEY: {
        "disposition": QUARANTINED,
        "recovery": "源系统确认正确版本后重放；完全重复的推送已由幂等逻辑折叠",
    },
    ERR_MISSING_REFERENCE: {
        "disposition": HELD,
        "recovery": "等待被引用的父记录到达后重跑准入；超过 SLA 未到转人工",
    },
    ERR_DANGLING_REFERENCE: {
        "disposition": QUARANTINED,
        "recovery": "父记录修复并放行后，以同业务键重放该记录",
    },
    ERR_REQUIRED_NULL: {
        "disposition": QUARANTINED,
        "recovery": "源系统补齐必填值后重放",
    },
}

# ------------------------------------------------------------------
# 物理 schema。台账用扁平列 + payload JSON（原始证据逐字保留）。
# ------------------------------------------------------------------

LEDGER_SCHEMA = pa.schema(
    [
        pa.field("row_uid", pa.string()),          # 表名 + 自然键 + 内容指纹
        pa.field("table_name", pa.string()),
        pa.field("natural_key_value", pa.string()),
        pa.field("uniqueness_key_json", pa.string()),
        pa.field("batch_id", pa.string()),
        pa.field("contract_version", pa.string()),
        pa.field("state", pa.string()),
        pa.field("error_code", pa.string()),
        pa.field("error_detail", pa.string()),
        pa.field("recovery_condition", pa.string()),
        pa.field("raw_payload_json", pa.string()),  # 原始证据，永不改写
        pa.field("source_file", pa.string()),
        pa.field("source_row_number", pa.int64()),
        pa.field("content_hash", pa.string()),
        pa.field("first_seen_batch", pa.string()),
        pa.field("last_updated_batch", pa.string()),
        pa.field("superseded_by", pa.string()),
        pa.field("admitted_at", pa.string()),
    ]
)

BATCH_SCHEMA = pa.schema(
    [
        pa.field("batch_id", pa.string()),
        pa.field("table_name", pa.string()),
        pa.field("source_file", pa.string()),
        pa.field("content_hash", pa.string()),
        pa.field("status", pa.string()),
        pa.field("contract_version", pa.string()),
        pa.field("rows_received", pa.int64()),
        pa.field("rows_accepted", pa.int64()),
        pa.field("rows_held", pa.int64()),
        pa.field("rows_quarantined", pa.int64()),
        pa.field("rows_duplicate", pa.int64()),
        pa.field("schema_changes_json", pa.string()),
        pa.field("error_detail", pa.string()),
        pa.field("reprocess_of", pa.string()),
        pa.field("ingested_at", pa.string()),
    ]
)
