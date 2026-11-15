"""ODS 准入与隔离层测试。

每个用例用独立临时湖目录，覆盖需求里的每一类场景：
  1. 正常批次放行 + 同内容重跑幂等（不重复计入下游）
  2. 可兼容字段扩展（minor 自动升版，旧批次仍可消费）
  3. 破坏性字段变更（整批 hold，无一行进 ODS，迁移后可重放）
  4. 同一业务键的更正（superseded 血缘）与乱序迟到（隔离，事实层不回退）
  5. 采购单/入库到达顺序颠倒（held → 父记录到达后自动放行）
  6. 不可达引用（父键被隔离）→ 子记录 quarantine
  7. 时区不一致 / 时间格式异常 / 类型漂移 / 必填缺失 → 隔离且带恢复条件
  8. 局部源表不可用（其他表照常）
  9. 同批重复推送折叠 + 同键冲突隔离，有效行不被阻塞
 10. 源系统修复后的确定性重处理
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from lakeside.admission import AdmissionGate, ContractBreaking, models as M
from lakeside.admission.contracts import (
    ContractRegistry,
    FieldSpec,
    TableContract,
    classify_field_change,
)


# ----------------------------------------------------------------------
# 构造器
# ----------------------------------------------------------------------

def supplier_row(sid="SUP-0001", **over) -> dict:
    base = {
        "supplier_id": sid,
        "supplier_name": "华东电子001有限公司",
        "region": "华东",
        "credit_level": "A",
        "valid_from": "2024-01-01 00:00:00",
        "valid_to": None,
        "is_current": True,
        "updated_at": "2024-01-01 03:00:00",
    }
    base.update(over)
    return base


def po_row(pid="PO-00001", sid="SUP-0001", **over) -> dict:
    base = {
        "po_id": pid,
        "supplier_id": sid,
        "amount_cents": 100000,
        "currency": "CNY",
        "status": "approved",
        "created_at": "2026-10-01 10:00:00",
        "biz_date": "2026-10-01",
    }
    base.update(over)
    return base


def gr_row(gid="GR-000001", pid="PO-00001", **over) -> dict:
    base = {
        "gr_id": gid,
        "po_id": pid,
        "received_qty": 20,
        "received_cents": 50000,
        "received_at": "2026-10-02 09:00:00",
        "warehouse": "WH-SH-01",
    }
    base.update(over)
    return base


def st_row(stid="ST-00001", pid="PO-00001", **over) -> dict:
    base = {
        "settlement_id": stid,
        "po_id": pid,
        "settled_cents": 40000,
        "settled_at_utc": "2026-10-03 12:00:00",
        "biz_date_local": "2026-10-03",
        "channel": "bank_transfer",
    }
    base.update(over)
    return base


@pytest.fixture()
def gate(tmp_path) -> AdmissionGate:
    return AdmissionGate(tmp_path, tz_offset_hours=8)


def states(gate: AdmissionGate, table: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in gate.read_ledger(table):
        out[r["state"]] = out.get(r["state"], 0) + 1
    return out


def ods_rows(gate: AdmissionGate, table: str) -> list[dict]:
    import pyarrow.parquet as pq

    p = gate.root / "ods" / table / "part-0001.parquet"
    return pq.read_table(p).to_pylist() if p.exists() else []


# ----------------------------------------------------------------------
# 1. 正常放行 + 重跑幂等
# ----------------------------------------------------------------------

def test_clean_batch_accepted_and_idempotent(gate):
    r1 = gate.ingest("supplier", [supplier_row()], "src://supplier/b1")
    assert r1["status"] == M.BATCH_ACCEPTED
    assert r1["rows_accepted"] == 1
    assert len(ods_rows(gate, "supplier")) == 1

    # 同内容重跑：duplicate，台账与 ODS 不翻倍。
    r2 = gate.ingest("supplier", [supplier_row()], "src://supplier/b1-retry")
    assert r2["status"] == M.BATCH_DUPLICATE
    assert states(gate, "supplier") == {"accepted": 1}
    assert len(ods_rows(gate, "supplier")) == 1
    # 批次注册表里只有一条真实批次（没有为重复尝试追加记录）。
    assert len([b for b in gate.read_batches() if b["table_name"] == "supplier"]) == 1


def test_every_decision_carries_evidence_contract_and_recovery(gate):
    gate.ingest("supplier", [supplier_row()], "src://supplier/b1")
    row = gate.read_ledger("supplier")[0]
    assert row["contract_version"] == "1.0"
    assert json.loads(row["raw_payload_json"])["supplier_id"] == "SUP-0001"
    assert row["source_file"] == "src://supplier/b1"
    assert row["source_row_number"] == 1
    assert len(row["content_hash"]) == 64
    assert (gate.landing / "supplier").exists()


# ----------------------------------------------------------------------
# 2. 可兼容字段扩展
# ----------------------------------------------------------------------

def test_compatible_field_addition_auto_evolves_minor(gate):
    gate.ingest("supplier", [supplier_row()], "src://supplier/b1")
    assert gate.registry.latest("supplier").version == "1.0"

    # 新可空字段 + 新的唯一键（不同 valid_from），模拟下一期的供应商版本。
    evolved_rows = [
        supplier_row(
            valid_from="2025-01-01 00:00:00",
            updated_at="2025-01-01 03:00:00",
            **{"tier_code": "T1"},
        )
    ]
    report = gate.ingest("supplier", evolved_rows, "src://supplier/b2")
    # 新可空字段 → minor 升版，批次照常放行，旧批次仍在 ODS。
    assert gate.registry.latest("supplier").version == "1.1"
    assert report["status"] == M.BATCH_ACCEPTED
    changes = json.loads(report["schema_changes_json"])
    assert changes and changes[0]["change"] == "compatible"
    ods = ods_rows(gate, "supplier")
    assert len(ods) == 2  # 不同 valid_from 唯一键，两行共存
    # 旧台账行保留首次准入的契约版本，新行记录升版后的版本。
    versions = {r["contract_version"] for r in gate.read_ledger("supplier")}
    assert versions == {"1.0", "1.1"}


def test_classify_change_rules():
    old = FieldSpec("amount_cents", "int", True)
    assert classify_field_change(old, FieldSpec("amount_cents", "long", True)) == "compatible"
    assert classify_field_change(old, FieldSpec("amount_cents", "string", True)) == "breaking"
    assert classify_field_change(old, None) == "breaking"
    assert classify_field_change(None, FieldSpec("note", "string", False)) == "compatible"
    assert classify_field_change(None, FieldSpec("note", "string", True)) == "breaking"


# ----------------------------------------------------------------------
# 3. 破坏性字段变更：整批 hold
# ----------------------------------------------------------------------

def test_breaking_schema_holds_whole_batch_and_blocks_ods(gate):
    gate.ingest("purchase_order", [po_row()], "src://po/b1")
    before = len(ods_rows(gate, "purchase_order"))

    # 删除契约字段 + 类型不兼容改写，模拟破坏性变更批次。
    broken = [{"po_id": "PO-00002", "amount_cents": "not-a-number", "currency": "CNY"}]
    report = gate.ingest("purchase_order", broken, "src://po/b2-broken")
    assert report["status"] == M.BATCH_HELD
    assert report["rows_accepted"] == 0
    assert M.ERR_SCHEMA_BREAKING in report["error_detail"]
    # ODS 仍是破坏前的快照，没有任何坏行混入。
    assert len(ods_rows(gate, "purchase_order")) == before

    # 契约版本停留在 1.0，未被自动升版。
    assert gate.registry.latest("purchase_order").version == "1.0"


def test_breaking_batch_can_replay_after_contract_migration(gate, tmp_path):
    gate.ingest("supplier", [supplier_row()], "src://sup/b1")
    gate.ingest("purchase_order", [po_row()], "src://po/b1")
    broken = [{"po_id": "PO-00002", "amount_cents": "100"}]  # 缺字段 + 类型漂移
    held = gate.ingest("purchase_order", broken, "src://po/bad")
    assert held["status"] == M.BATCH_HELD

    # 显式迁移契约（新增可空字段场景用 minor；此处演示注册新主版本后重放合规数据）。
    current = gate.registry.latest("purchase_order")
    migrated = TableContract(
        table="purchase_order",
        version="2.0",
        natural_key=current.natural_key,
        uniqueness_key=list(current.uniqueness_key),
        fields=tuple(
            FieldSpec(f.name, "string" if f.name == "amount_cents" else f.type,
                      f.required, f.tz_semantics)
            for f in current.fields
        ),
        references=current.references,
        ordering_fields=current.ordering_fields,
    )
    gate.registry.register(migrated)
    # 迁移后合规数据：字段齐全，仅 amount_cents 按新契约以字符串承载。
    fixed = [po_row("PO-00002", amount_cents="100")]
    report = gate.ingest("purchase_order", fixed, "src://po/replayed")
    assert report["contract_version"] == "2.0"
    assert report["rows_accepted"] >= 1
    assert any(r["po_id"] == "PO-00002" for r in ods_rows(gate, "purchase_order"))


# ----------------------------------------------------------------------
# 4. 更正与重放 / 乱序迟到
# ----------------------------------------------------------------------

def test_correction_supersedes_with_lineage(gate):
    gate.ingest("supplier", [supplier_row(updated_at="2024-01-01 03:00:00")], "b1")
    # 同自然键、同 valid_from 的更正版本（updated_at 更新）。
    corrected = supplier_row(supplier_name="新名称", updated_at="2024-02-01 03:00:00")
    gate.ingest("supplier", [corrected], "b2")

    ledger = gate.read_ledger("supplier")
    accepted = [r for r in ledger if r["state"] == M.ACCEPTED]
    superseded = [r for r in ledger if r["state"] == M.SUPERSEDED]
    assert len(accepted) == 1 and len(superseded) == 1
    assert json.loads(accepted[0]["raw_payload_json"])["supplier_name"] == "新名称"
    assert superseded[0]["superseded_by"] == accepted[0]["row_uid"]
    # ODS 每个唯一键只有最新一行，不会双倍计数。
    assert len(ods_rows(gate, "supplier")) == 1


def test_out_of_order_late_version_is_quarantined_not_rolled_back(gate):
    gate.ingest("supplier", [supplier_row()], "sup")  # 父记录先就位
    # 新版本先到
    gate.ingest(
        "purchase_order",
        [po_row(amount_cents=200000, created_at="2026-10-05 10:00:00", biz_date="2026-10-05")],
        "newer",
    )
    # 旧版本后到（乱序/迟到）
    late = gate.ingest(
        "purchase_order",
        [po_row(amount_cents=100000, created_at="2026-10-01 10:00:00", biz_date="2026-10-01")],
        "late-older",
    )
    assert late["status"] == M.BATCH_PARTIAL
    st = states(gate, "purchase_order")
    assert st.get("quarantined") == 1 and st.get("accepted") == 1
    ods = ods_rows(gate, "purchase_order")
    assert len(ods) == 1 and ods[0]["amount_cents"] == 200000  # 事实层未回退
    bad = gate.open_decisions("purchase_order")[0]
    assert bad["error_code"] == M.ERR_DUPLICATE_KEY and bad["recovery_condition"]


# ----------------------------------------------------------------------
# 5. 到达顺序颠倒：held 后自动放行
# ----------------------------------------------------------------------

def test_referential_hold_then_auto_release_on_parent_arrival(gate):
    # 供应商主数据通常先到；采购单与入库的到达顺序颠倒。
    gate.ingest("supplier", [supplier_row()], "src://sup/first")
    # 入库先到，采购单还没到 → held（短暂迟到，可恢复）。
    first = gate.ingest("goods_receipt", [gr_row()], "src://gr/early")
    assert first["status"] == M.BATCH_HELD
    assert states(gate, "goods_receipt") == {"held": 1}
    assert ods_rows(gate, "goods_receipt") == []  # 暂缓行进不了 ODS

    # 采购单到达；级联重处理应自动解除暂缓。
    gate.ingest("purchase_order", [po_row()], "src://po/now")
    assert states(gate, "goods_receipt") == {"accepted": 1}
    ods = ods_rows(gate, "goods_receipt")
    assert len(ods) == 1 and ods[0]["gr_id"] == "GR-000001"


def test_dangling_reference_to_quarantined_parent_is_quarantined(gate):
    gate.ingest("supplier", [supplier_row()], "sup")
    # 采购单因类型错误被隔离。
    gate.ingest("purchase_order", [po_row(amount_cents="bad")], "src://po/bad")
    # 入库引用它：父记录不可达 → 子记录隔离（而非无限期暂缓）。
    rep = gate.ingest("goods_receipt", [gr_row()], "src://gr/child")
    assert rep["rows_quarantined"] == 1
    child = gate.read_ledger("goods_receipt")[0]
    assert child["error_code"] == M.ERR_DANGLING_REFERENCE
    assert child["recovery_condition"]


# ----------------------------------------------------------------------
# 6. 时间格式 / 时区 / 类型 / 必填
# ----------------------------------------------------------------------

def test_bad_timestamp_and_timezone_mismatch_quarantined(gate):
    gate.ingest("supplier", [supplier_row()], "sup")
    gate.ingest("purchase_order", [po_row()], "po")
    bad_ts = st_row("ST-1", settled_at_utc="not-a-timestamp")
    bad_tz = st_row("ST-2", settled_at_utc="2026-10-03 12:00:00", biz_date_local="2026-10-04")
    rep = gate.ingest("settlement", [bad_ts, bad_tz, st_row("ST-3")], "st-batch")
    # 三条里两条隔离、一条放行：有效数据不被异常记录阻塞。
    assert rep["rows_quarantined"] == 2 and rep["rows_accepted"] == 1
    codes = {r["error_code"] for r in gate.open_decisions("settlement")}
    assert codes == {M.ERR_BAD_TIMESTAMP, M.ERR_TIMEZONE_MISMATCH}
    assert len(ods_rows(gate, "settlement")) == 1


def test_type_drift_and_required_null_quarantined_with_recovery(gate):
    rep = gate.ingest(
        "supplier",
        [
            supplier_row("SUP-1", credit_level=123),       # 类型漂移
            supplier_row("SUP-2", supplier_name=None),      # 必填缺失
            supplier_row("SUP-3"),                          # 正常
        ],
        "mixed",
    )
    assert rep["status"] == M.BATCH_PARTIAL
    assert rep["rows_accepted"] == 1 and rep["rows_quarantined"] == 2
    for r in gate.open_decisions("supplier"):
        assert r["recovery_condition"] and r["raw_payload_json"]
    assert len(ods_rows(gate, "supplier")) == 1


# ----------------------------------------------------------------------
# 7. 局部源表不可用
# ----------------------------------------------------------------------

def test_partial_source_unavailability_does_not_block_others(gate):
    rep = gate.ingest(
        "settlement", [], "src://settlement/down",
        unavailable=True, unavailable_detail="连接超时",
    )
    assert rep["status"] == M.BATCH_UNAVAILABLE
    # 其他表照常准入。
    ok = gate.ingest("supplier", [supplier_row()], "src://supplier/ok")
    assert ok["status"] == M.BATCH_ACCEPTED
    # 故障恢复后同表可正常接入，台账没有脏数据。
    gate.ingest("supplier", [supplier_row()], "src://supplier/ok2")
    gate.ingest("purchase_order", [po_row()], "src://po/ok")
    gate.ingest("settlement", [st_row()], "src://settlement/recovered")
    assert states(gate, "settlement").get("accepted") == 1


# ----------------------------------------------------------------------
# 8. 同批重复折叠 + 同键冲突
# ----------------------------------------------------------------------

def test_duplicate_delivery_collapsed_and_conflict_quarantined(gate):
    gate.ingest("supplier", [supplier_row()], "sup")
    rows = [
        po_row("PO-1", amount_cents=100),
        dict(po_row("PO-1", amount_cents=100)),   # 完全重复 → 折叠
        po_row("PO-2", amount_cents=200),         # 正常
    ]
    rep = gate.ingest("purchase_order", rows, "po")
    assert rep["rows_duplicate"] == 1
    assert rep["rows_accepted"] == 2
    assert len(ods_rows(gate, "purchase_order")) == 2

    # 同键内容冲突：首条先到先得被接受，冲突的第二条隔离（无法确定真值），
    # ODS 里该键只有一行，不会因为冲突产生双倍计数。
    conflict = gate.ingest(
        "purchase_order",
        [po_row("PO-3", amount_cents=300), po_row("PO-3", amount_cents=301)],
        "po-conflict",
    )
    assert conflict["rows_quarantined"] >= 1
    po3 = [r for r in ods_rows(gate, "purchase_order") if r["po_id"] == "PO-3"]
    assert len(po3) == 1 and po3[0]["amount_cents"] == 300
    bad = next(
        r for r in gate.open_decisions("purchase_order") if r["error_code"] == M.ERR_DUPLICATE_KEY
    )
    assert bad["recovery_condition"]


# ----------------------------------------------------------------------
# 9. 确定性重处理
# ----------------------------------------------------------------------

def test_deterministic_reprocessing_after_source_fix(gate):
    gate.ingest("supplier", [supplier_row()], "sup")
    gate.ingest("purchase_order", [po_row()], "po")
    # 时区错误先被隔离。
    gate.ingest(
        "settlement",
        [st_row(biz_date_local="2026-10-04")],
        "st-bad",
    )
    assert states(gate, "settlement") == {"quarantined": 1}

    # 重放未修复的原始证据：仍然隔离，不会悄悄混入。
    again = gate.reprocess_quarantine("settlement")
    assert again["replayed"] == 1
    assert states(gate, "settlement") == {"quarantined": 1}
    assert ods_rows(gate, "settlement") == []

    # 源系统修复后以同业务键重放正确内容 → 放行；ODS 恰好一行，无重复计入。
    fixed = gate.ingest("settlement", [st_row()], "st-fixed")
    assert fixed["rows_accepted"] == 1
    assert len(ods_rows(gate, "settlement")) == 1


def test_replay_report_reflects_current_ledger_state(gate):
    """重放仍不合规的批次时，批次报告必须维持隔离结论，不能误报 accepted。"""
    gate.ingest("supplier", [supplier_row()], "sup")
    gate.ingest("purchase_order", [po_row()], "po")
    bad = gate.ingest(
        "settlement", [st_row(biz_date_local="2026-10-04")], "st-bad"
    )
    assert bad["status"] == M.BATCH_PARTIAL and bad["rows_quarantined"] == 1

    result = gate.reprocess_quarantine("settlement")
    report = result["report"]
    # 内容未修复：仍是同一批次、同一隔离结论与计数。
    assert report["batch_id"] == bad["batch_id"]
    assert report["rows_quarantined"] == 1 and report["rows_accepted"] == 0
    assert report["status"] == M.BATCH_PARTIAL
    # 批次注册表没有被追加重复批次。
    same = [b for b in gate.read_batches() if b["batch_id"] == bad["batch_id"]]
    assert len(same) == 1
    assert ods_rows(gate, "settlement") == []


def test_rebuild_ods_is_deterministic(gate):
    gate.ingest("supplier", [supplier_row(f"SUP-{i:04d}") for i in range(5)], "b1")
    first = (gate.root / "ods" / "supplier" / "part-0001.parquet").read_bytes()
    gate.rebuild_ods("supplier")
    second = (gate.root / "ods" / "supplier" / "part-0001.parquet").read_bytes()
    assert first == second


# ----------------------------------------------------------------------
# 10. 规范化时间列与契约版本追溯
# ----------------------------------------------------------------------

def test_ods_carries_canonical_utc_and_lineage_columns(gate):
    gate.ingest("goods_receipt", [gr_row()], "gr") if False else None
    gate.ingest("supplier", [supplier_row()], "sup")
    gate.ingest("purchase_order", [po_row()], "po")
    gate.ingest("goods_receipt", [gr_row()], "gr")
    rec = ods_rows(gate, "goods_receipt")[0]
    # 本地时间 2026-10-02 09:00 (UTC+8) → UTC 2026-10-02 01:00
    assert rec["received_at_utc"] == "2026-10-02 01:00:00"
    assert rec["contract_version"] == "1.0"
    assert rec["admission_batch_id"] and rec["row_uid"] and rec["content_hash"]


def test_contract_registry_preserves_all_versions(gate):
    gate.ingest("supplier", [supplier_row()], "b1")
    gate.ingest("supplier", [supplier_row(**{"tier_code": "T1"})], "b2")
    versions = [c.version for c in gate.registry.all_versions("supplier")]
    assert versions == ["1.0", "1.1"]
    # 历史台账行仍记录首次准入时的契约版本。
    row = gate.read_ledger("supplier")[0]
    assert row["contract_version"] == "1.0"
