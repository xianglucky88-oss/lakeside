"""ODS 数据准入与隔离引擎。

数据流（每个批次、每张表独立执行）：

    源文件/记录
      → 1. 落 landing（只追加、内容寻址、永不改写，作为原始证据）
      → 2. 契约校验：兼容扩展自动 minor 升版；破坏性变更整批 hold
      → 3. 逐行校验：必填/类型/时间格式/时区一致性/同键冲突/跨表引用
      → 4. 行台账合并：幂等折叠、更正替换、乱序（迟到旧版本）拒绝
      → 5. 结论：accepted / held / quarantined，全部带证据、契约版本、恢复条件
      → 6. 由已接受台账行确定性重建可消费 ODS（异常行永远进不来）

关键不变量：
  * 重跑同一批次 → duplicate，台账与 ODS 字节级不变，下游不会被重复计入；
  * 少量异常只隔离对应行，同批有效行照常放行；
  * 隔离/暂缓行只存在 quarantine 区与台账里，ODS 重建永远读不到它们；
  * 所有结论可追溯到 landing 原始文件、行号、内容指纹与契约版本。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import (
    ContractBreaking,
    ContractRegistry,
    FieldSpec,
    TableContract,
    initial_contracts,
)
from . import models as M

# 逻辑类型 → pyarrow 类型（用于推断实测 schema 与重建 ODS）
_PA_TYPES = {
    "string": pa.string(),
    "int": pa.int32(),
    "long": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.string(),
    "date": pa.string(),
}

_TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _content_hash(rows: list[dict[str, Any]]) -> str:
    blob = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)
    return _sha256_text(blob)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in _TS_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_date(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


class AdmissionGate:
    """单湖目录的准入引擎。无引擎私有状态，换进程可直接重开。"""

    def __init__(
        self,
        lake_root: Path,
        tz_offset_hours: int = 8,
        registry: ContractRegistry | None = None,
    ) -> None:
        self.root = Path(lake_root)
        self.landing = self.root / "landing"
        self.quarantine = self.root / "quarantine"
        self.registry = registry or ContractRegistry(self.root)
        self.tz_offset = tz_offset_hours
        for d in (self.landing, self.quarantine):
            d.mkdir(parents=True, exist_ok=True)
        self._bootstrap_contracts()

    # ------------------------------------------------------------------
    # 初始化与台账读写
    # ------------------------------------------------------------------

    def _bootstrap_contracts(self) -> None:
        for c in initial_contracts():
            if not self.registry.has(c.table):
                self.registry.register(c)

    def _ledger_path(self, table: str) -> Path:
        return self.quarantine / "row_ledger" / f"table={table}" / "part-0001.parquet"

    def _batch_path(self) -> Path:
        return self.quarantine / "batch_registry" / "part-0001.parquet"

    def _read_parquet(self, path: Path, schema: pa.Schema) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        return pq.read_table(path).to_pylist()

    def _write_parquet(self, path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # 缺失列补 None，未知列剔除，保证 schema 始终受管。
        clean = [{k: r.get(k) for k in schema.names} for r in rows]
        pq.write_table(pa.Table.from_pylist(clean, schema=schema), path)

    def read_ledger(self, table: str) -> list[dict[str, Any]]:
        return self._read_parquet(self._ledger_path(table), M.LEDGER_SCHEMA)

    def write_ledger(self, table: str, rows: list[dict[str, Any]]) -> None:
        self._write_parquet(self._ledger_path(table), rows, M.LEDGER_SCHEMA)

    def read_batches(self) -> list[dict[str, Any]]:
        return self._read_parquet(self._batch_path(), M.BATCH_SCHEMA)

    def write_batches(self, rows: list[dict[str, Any]]) -> None:
        self._write_parquet(self._batch_path(), rows, M.BATCH_SCHEMA)

    # ------------------------------------------------------------------
    # 对外查询
    # ------------------------------------------------------------------

    def open_decisions(self, table: str | None = None) -> list[dict[str, Any]]:
        """仍处于 held / quarantined 的行（恢复工作台的输入）。"""
        tables = [table] if table else self.registry.tables()
        out: list[dict[str, Any]] = []
        for t in tables:
            for r in self.read_ledger(t):
                if r["state"] in (M.HELD, M.QUARANTINED):
                    out.append(r)
        return out

    def batch_report(self, batch_id: str) -> dict[str, Any] | None:
        for b in self.read_batches():
            if b["batch_id"] == batch_id:
                return b
        return None

    # ------------------------------------------------------------------
    # 主入口：接入一个批次
    # ------------------------------------------------------------------

    def ingest(
        self,
        table: str,
        rows: list[dict[str, Any]],
        source_file: str,
        *,
        reprocess_of: str | None = None,
        unavailable: bool = False,
        unavailable_detail: str = "",
        allow_replay: bool = False,
    ) -> dict[str, Any]:
        """把一个源批次接入湖。返回批次级报告（同时落 batch_registry）。

        allow_replay: 内部重放（父记录到达级联 / 人工恢复）时置 True，
            允许对已存在的 held/quarantined 批次重新跑准入逻辑；
            普通重跑仍走幂等 duplicate 短路，绝不重复计入下游。
        """
        batches = self.read_batches()
        hash_value = _content_hash(rows)
        batch_id = f"{table}-{hash_value[:16]}"
        ingested_at = _now_iso()

        # 局部源表不可用：单独记录，不影响其他表，也不污染台账。
        if unavailable:
            report = self._new_batch(
                batch_id, table, source_file, hash_value, M.BATCH_UNAVAILABLE,
                "-", len(rows), 0, 0, 0, 0, "[]",
                unavailable_detail or "源表/源文件不可用", reprocess_of, ingested_at,
            )
            batches.append(report)
            self.write_batches(batches)
            return report

        # 幂等：内容完全相同的批次只处理一次（重跑不重复计入下游）。
        prior = next(
            (b for b in batches if b["batch_id"] == batch_id and b["status"] != M.BATCH_UNAVAILABLE),
            None,
        )
        if prior is not None and not allow_replay:
            # 直接返回首次结论；不新增台账行、不改 ODS，因此历史批次不会被重复计入。
            report = dict(prior)
            report["status"] = M.BATCH_DUPLICATE if prior["status"] != M.BATCH_HELD else M.BATCH_HELD
            return report
        replay = prior is not None and allow_replay

        # 1. 原始证据落 landing（只追加；文件名即内容指纹，天然去重）。
        # landing 必须逐字保留源值，混合类型列统一按字符串物理存储，
        # 真实类型由契约在准入阶段判定，而不是由 parquet 推断。
        landing_path = self.landing / table / f"{batch_id}.parquet"
        if not landing_path.exists():
            landing_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(self._landing_table(rows), landing_path)

        # 2. 实测 schema 与契约对比。
        try:
            observed = self._observe_fields(table, rows)
            contract, evolved, schema_changes = self.registry.evolve(table, observed)
        except ContractBreaking as exc:
            # 破坏性变更：整批 hold，没有任何一行进入 ODS。
            report = self._new_batch(
                batch_id, table, source_file, hash_value, M.BATCH_HELD,
                self.registry.latest(table).version, len(rows), 0, 0, 0, 0,
                json.dumps(exc.changes, ensure_ascii=False),
                f"{M.ERR_SCHEMA_BREAKING}: {exc}", reprocess_of, ingested_at,
            )
            batches.append(report)
            self.write_batches(batches)
            return report

        # 3+4. 逐行校验并合并进行台账。
        ledger = self.read_ledger(table)
        counts = self._validate_and_merge(
            table, contract, rows, batch_id, source_file, ledger
        )

        if replay:
            # 重放时增量计数只反映本次状态迁移（已登记行不会重复计数），
            # 批次报告必须按台账里属于该批次的行的**当前状态**重新汇总，
            # 否则一个仍被隔离的行会被误报成 accepted。
            owned = [r for r in ledger if r["first_seen_batch"] == batch_id]
            counts = {
                "accepted": sum(
                    1 for r in owned if r["state"] in (M.ACCEPTED, M.SUPERSEDED)
                ),
                "held": sum(1 for r in owned if r["state"] == M.HELD),
                "quarantined": sum(1 for r in owned if r["state"] == M.QUARANTINED),
                "duplicate": prior["rows_duplicate"] if prior else 0,
            }

        # 5. 批次级结论：有效行不被少量异常阻塞。
        #    held 仅表示存在「可恢复的等待」（引用迟到）；纯硬错误批次记 partial，
        #    因为它已有明确隔离结论而非无限期等待。
        if counts["accepted"] > 0 and (counts["held"] or counts["quarantined"]):
            status = M.BATCH_PARTIAL
        elif counts["accepted"] == 0 and counts["held"] > 0:
            status = M.BATCH_HELD
        elif counts["accepted"] == 0 and counts["quarantined"] > 0:
            status = M.BATCH_PARTIAL
        else:
            status = M.BATCH_ACCEPTED

        self.write_ledger(table, ledger)

        # 6. 父表到达可能解除子表暂缓 → 级联重处理受影响的下游表。
        cascaded = self._cascade_reprocess(table, batch_id)

        report = self._new_batch(
            batch_id, table, source_file, hash_value, status,
            contract.version, len(rows), counts["accepted"], counts["held"],
            counts["quarantined"], counts["duplicate"],
            json.dumps(schema_changes, ensure_ascii=False), "", reprocess_of, ingested_at,
        )
        report["cascaded"] = cascaded
        if replay and prior is not None:
            # 重放：原地更新首次批次的结论，保留同一 batch_id 与血缘，不新增重复批次。
            prior.update(report)
            report = prior
        else:
            batches.append(report)
        self.write_batches(batches)

        # 7. 由台账确定性重建可消费 ODS。
        for t in [table, *cascaded]:
            self.rebuild_ods(t)
        return report

    # ------------------------------------------------------------------
    # 契约 schema 推断
    # ------------------------------------------------------------------

    def _observe_fields(self, table: str, rows: list[dict[str, Any]]) -> tuple[FieldSpec, ...]:
        """从批次数据推断实测字段；类型与必填性沿用已注册契约的语义。"""
        current = self.registry.latest(table)
        names: list[str] = []
        for r in rows:
            for k in r:
                if k not in names:
                    names.append(k)
        out: list[FieldSpec] = []
        for name in names:
            old = current.field(name)
            if old is not None:
                # 已登记字段沿用契约语义；必填性只能由契约变更显式调整。
                required = old.required
                logical_type = old.type
                tz = old.tz_semantics
            else:
                # 新字段默认按可空处理（可兼容扩展）；
                # 即使批次内恰好没有空值，也不把它推断成必填。
                required = False
                logical_type = self._infer_type(rows, name)
                tz = (
                    "utc" if name.endswith("_utc") else
                    "local" if name in ("biz_date", "biz_date_local") or name.endswith("_at") else None
                )
            out.append(
                FieldSpec(name=name, type=logical_type, required=required, tz_semantics=tz)
            )
        return tuple(out)

    @staticmethod
    def _infer_type(rows: list[dict[str, Any]], name: str) -> str:
        for r in rows:
            v = r.get(name)
            if v is None:
                continue
            if isinstance(v, bool):
                return "boolean"
            if isinstance(v, int):
                return "long"
            if isinstance(v, float):
                return "double"
            return "string"
        return "string"

    # ------------------------------------------------------------------
    # 逐行校验 + 台账合并
    # ------------------------------------------------------------------

    def _validate_and_merge(
        self,
        table: str,
        contract: TableContract,
        rows: list[dict[str, Any]],
        batch_id: str,
        source_file: str,
        ledger: list[dict[str, Any]],
    ) -> dict[str, int]:
        counts = {"accepted": 0, "held": 0, "quarantined": 0, "duplicate": 0}
        # 同批内按唯一键检测：完全一致 → 折叠；同键冲突 → 隔离。
        seen_uk: dict[str, dict[str, Any]] = {}
        index_by_uid = {r["row_uid"]: i for i, r in enumerate(ledger)}
        index_by_uk: dict[str, list[int]] = {}
        for i, r in enumerate(ledger):
            index_by_uk.setdefault(r["uniqueness_key_json"], []).append(i)

        for line_no, raw in enumerate(rows, start=1):
            payload = dict(raw)
            hash_value = _content_hash(payload)
            nk_value = str(payload.get(contract.natural_key, ""))
            uk = {k: payload.get(k) for k in contract.uniqueness_key}
            uk_json = json.dumps(uk, ensure_ascii=False, sort_keys=True, default=str)
            # row_uid 纳入唯一键：同一自然键的不同版本（如供应商 SCD2）不会互相冲撞。
            row_uid = f"{table}:{uk_json}:{hash_value[:12]}"

            entry = self._base_entry(
                table, contract, payload, batch_id, source_file, line_no,
                hash_value, nk_value, uk_json, row_uid,
            )

            # ---- 行内校验：必填 / 类型 / 时间格式 / 时区 ----
            row_error = self._validate_row(contract, payload)
            if row_error is not None:
                code, detail = row_error
                self._finalize_bad(
                    entry, code, detail, ledger, index_by_uid, index_by_uk, counts
                )
                continue

            # ---- 同批重复 / 冲突 ----
            if uk_json in seen_uk:
                prev_hash = seen_uk[uk_json]["_hash"]
                if prev_hash == hash_value:
                    counts["duplicate"] += 1  # 完全重复推送：折叠，不产生新台账行
                    continue
                self._finalize_bad(
                    entry, M.ERR_DUPLICATE_KEY,
                    f"唯一键 {uk_json} 在同一批次内出现内容不一致的多条记录",
                    ledger, index_by_uid, index_by_uk, counts,
                )
                continue
            seen_uk[uk_json] = {"_hash": hash_value}

            # ---- 跨表引用（迟到的父记录 → held；不可达 → quarantine）----
            ref_error = self._check_references(contract, payload)
            if ref_error is not None:
                code, detail = ref_error
                entry["state"] = M.ERROR_POLICY[code]["disposition"]
                entry["error_code"] = code
                entry["error_detail"] = detail
                entry["recovery_condition"] = M.ERROR_POLICY[code]["recovery"]
                self._upsert_ledger(entry, ledger, index_by_uid, index_by_uk, counts, contract)
                continue

            # ---- 干净行：与历史台账做幂等/更正/乱序合并 ----
            entry["state"] = M.ACCEPTED
            entry["admitted_at"] = _now_iso()
            self._upsert_ledger(entry, ledger, index_by_uid, index_by_uk, counts, contract)

        return counts

    def _validate_row(
        self, contract: TableContract, payload: dict[str, Any]
    ) -> tuple[str, str] | None:
        for f in contract.fields:
            value = payload.get(f.name)
            if value is None or value == "":
                if f.required:
                    return M.ERR_REQUIRED_NULL, f"必填字段 {f.name} 为空"
                continue
            if not self._type_ok(f, value):
                return (
                    M.ERR_TYPE_DRIFT,
                    f"字段 {f.name} 期望 {f.type}，实际值 {value!r}（{type(value).__name__}）",
                )
            if f.type == "timestamp" and _parse_ts(value) is None:
                return M.ERR_BAD_TIMESTAMP, f"字段 {f.name} 时间格式无法解析: {value!r}"
            if f.type == "date" and _parse_date(value) is None:
                return M.ERR_BAD_TIMESTAMP, f"字段 {f.name} 日期格式无法解析: {value!r}"

        # 时区一致性：UTC 时刻按表偏移换算出的本地日必须等于源给的本地日。
        for check in contract.timezone_checks:
            utc_value = payload.get(check.utc_column)
            local_value = payload.get(check.local_column)
            utc_dt = _parse_ts(utc_value)
            local_dt = _parse_date(local_value)
            if utc_dt is None or local_dt is None:
                continue  # 格式问题已在上面拦截
            shifted = utc_dt + timedelta(hours=self.tz_offset)
            if shifted.strftime("%Y-%m-%d") != local_dt.strftime("%Y-%m-%d"):
                return (
                    M.ERR_TIMEZONE_MISMATCH,
                    f"{check.utc_column}={utc_value} 按 UTC+{self.tz_offset} 换算应为 "
                    f"{shifted.strftime('%Y-%m-%d')}，但 {check.local_column}={local_value}",
                )
        return None

    @staticmethod
    def _type_ok(f: FieldSpec, value: Any) -> bool:
        if f.type in ("string",):
            return isinstance(value, str)
        if f.type in ("int", "long"):
            if isinstance(value, bool):
                return False
            return isinstance(value, int)
        if f.type == "double":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if f.type == "boolean":
            return isinstance(value, bool)
        # timestamp / date 在本湖以字符串承载
        return isinstance(value, str)

    def _check_references(
        self, contract: TableContract, payload: dict[str, Any]
    ) -> tuple[str, str] | None:
        for ref in contract.references:
            fk = payload.get(ref.column)
            if fk is None:
                continue
            parent_rows = self.read_ledger(ref.references_table)
            parents = [r for r in parent_rows if r["natural_key_value"] == str(fk)]
            if not parents:
                return (
                    M.ERR_MISSING_REFERENCE,
                    f"{ref.column}={fk} 引用的 {ref.references_table}.{ref.references_column} 尚未到达",
                )
            if not any(r["state"] in (M.ACCEPTED, M.SUPERSEDED) for r in parents):
                # 父记录存在但处于 held/quarantine：子记录不可能被修复前放行。
                return (
                    M.ERR_DANGLING_REFERENCE,
                    f"{ref.column}={fk} 引用的父记录处于 "
                    f"{sorted({r['state'] for r in parents})} 状态",
                )
        return None

    def _upsert_ledger(
        self,
        entry: dict[str, Any],
        ledger: list[dict[str, Any]],
        index_by_uid: dict[str, int],
        index_by_uk: dict[str, list[int]],
        counts: dict[str, int],
        contract: TableContract,
    ) -> None:
        """把一条结论合并进行台账，实现幂等、更正与乱序保护。"""
        # 完全相同内容（row_uid 相同）：幂等返回，不新增、不重复计数。
        if entry["row_uid"] in index_by_uid:
            existing = ledger[index_by_uid[entry["row_uid"]]]
            if existing["state"] in (M.HELD, M.QUARANTINED) and entry["state"] == M.ACCEPTED:
                # 之前暂缓/隔离的同一内容现在满足条件 → 结论升级。
                existing["state"] = M.ACCEPTED
                existing["error_code"] = ""
                existing["error_detail"] = ""
                existing["recovery_condition"] = ""
                existing["admitted_at"] = entry["admitted_at"]
                existing["last_updated_batch"] = entry["batch_id"]
                counts["accepted"] += 1
            elif existing["state"] == entry["state"]:
                # 结论未变（如仍暂缓）：刷新证据批次，不重复计数，状态不回退。
                existing["last_updated_batch"] = entry["batch_id"]
            return

        same_key_idx = index_by_uk.get(entry["uniqueness_key_json"], [])
        accepted_idx = [
            i for i in same_key_idx if ledger[i]["state"] in (M.ACCEPTED, M.SUPERSEDED)
        ]

        if entry["state"] == M.ACCEPTED and accepted_idx:
            # 同唯一键已有放行版本：判断是更正还是乱序迟到的旧版本。
            current_idx = self._latest_accepted(ledger, accepted_idx, contract)
            current = ledger[current_idx]
            if self._is_stale(entry, current, contract):
                # 采购单/入库/结算到达顺序颠倒：旧版本不覆盖新版本，隔离并留证。
                entry["state"] = M.QUARANTINED
                entry["error_code"] = M.ERR_DUPLICATE_KEY
                entry["error_detail"] = (
                    f"迟到的旧版本（排序字段 {contract.ordering_fields} 不新于已接受版本 "
                    f"batch={current['batch_id']}），拒绝回写以防事实层回退"
                )
                entry["recovery_condition"] = M.ERROR_POLICY[M.ERR_DUPLICATE_KEY]["recovery"]
                self._append(entry, ledger, index_by_uid, index_by_uk)
                counts["quarantined"] += 1
                return
            # 更正/重放：旧 accepted 行标记 superseded，保留血缘；新行 accepted。
            current["state"] = M.SUPERSEDED
            current["superseded_by"] = entry["row_uid"]
            current["last_updated_batch"] = entry["batch_id"]

        self._append(entry, ledger, index_by_uid, index_by_uk)
        counts[{"accepted": "accepted", "held": "held",
                "quarantined": "quarantined"}[entry["state"]]] += 1

    @staticmethod
    def _latest_accepted(
        ledger: list[dict[str, Any]], idxs: list[int], contract: TableContract
    ) -> int:
        def sort_key(i: int) -> tuple:
            payload = json.loads(ledger[i]["raw_payload_json"])
            order = tuple(str(payload.get(f, "")) for f in contract.ordering_fields)
            return order + (ledger[i]["admitted_at"],)

        return max(idxs, key=sort_key)

    def _is_stale(
        self, entry: dict[str, Any], current: dict[str, Any], contract: TableContract
    ) -> bool:
        if not contract.ordering_fields:
            return False
        new_payload = json.loads(entry["raw_payload_json"])
        old_payload = json.loads(current["raw_payload_json"])
        for f in contract.ordering_fields:
            nv, ov = new_payload.get(f), old_payload.get(f)
            if nv is None or ov is None:
                continue
            if str(nv) > str(ov):
                return False
            if str(nv) < str(ov):
                return True
        # 排序字段完全相同且内容不同：保守视为冲突，交给调用方按同键冲突处理。
        return True

    @staticmethod
    def _append(
        entry: dict[str, Any],
        ledger: list[dict[str, Any]],
        index_by_uid: dict[str, int],
        index_by_uk: dict[str, list[int]],
    ) -> None:
        idx = len(ledger)
        ledger.append(entry)
        index_by_uid[entry["row_uid"]] = idx
        index_by_uk.setdefault(entry["uniqueness_key_json"], []).append(idx)

    def _finalize_bad(
        self,
        entry: dict[str, Any],
        code: str,
        detail: str,
        ledger: list[dict[str, Any]],
        index_by_uid: dict[str, int],
        index_by_uk: dict[str, list[int]],
        counts: dict[str, int],
    ) -> None:
        disposition = M.ERROR_POLICY[code]["disposition"]
        entry["state"] = disposition
        entry["error_code"] = code
        entry["error_detail"] = detail
        entry["recovery_condition"] = M.ERROR_POLICY[code]["recovery"]
        # 同内容重放：若之前已登记过相同坏行，刷新批次信息但不重复计数。
        if entry["row_uid"] in index_by_uid:
            existing = ledger[index_by_uid[entry["row_uid"]]]
            existing["last_updated_batch"] = entry["batch_id"]
            return
        self._append(entry, ledger, index_by_uid, index_by_uk)
        counts[{"held": "held", "quarantined": "quarantined"}[disposition]] += 1

    @staticmethod
    def _base_entry(
        table: str,
        contract: TableContract,
        payload: dict[str, Any],
        batch_id: str,
        source_file: str,
        line_no: int,
        hash_value: str,
        nk_value: str,
        uk_json: str,
        row_uid: str,
    ) -> dict[str, Any]:
        return {
            "row_uid": row_uid,
            "table_name": table,
            "natural_key_value": nk_value,
            "uniqueness_key_json": uk_json,
            "batch_id": batch_id,
            "contract_version": contract.version,
            "state": "",
            "error_code": "",
            "error_detail": "",
            "recovery_condition": "",
            "raw_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
            "source_file": source_file,
            "source_row_number": line_no,
            "content_hash": hash_value,
            "first_seen_batch": batch_id,
            "last_updated_batch": batch_id,
            "superseded_by": "",
            "admitted_at": "",
        }

    # ------------------------------------------------------------------
    # 级联重处理（父记录到达后自动解除子表暂缓）
    # ------------------------------------------------------------------

    def _cascade_reprocess(self, parent_table: str, parent_batch: str) -> list[str]:
        dependents = {
            "supplier": ["purchase_order"],
            "purchase_order": ["goods_receipt", "settlement"],
        }
        out: list[str] = []
        for child in dependents.get(parent_table, []):
            open_rows = [
                r for r in self.read_ledger(child)
                if r["state"] == M.HELD and r["error_code"] == M.ERR_MISSING_REFERENCE
            ]
            if not open_rows:
                continue
            # 用暂缓行的原始证据重建一个重放批次，重新走完整准入逻辑。
            replay_rows = [json.loads(r["raw_payload_json"]) for r in open_rows]
            if not replay_rows:
                continue
            source = f"replay:held:{child}:{parent_batch}"
            self.ingest(
                child, replay_rows, source,
                reprocess_of=parent_batch, allow_replay=True,
            )
            out.append(child)
        return out

    def reprocess_quarantine(self, table: str) -> dict[str, Any]:
        """源系统修复后的确定性重放入口。

        重放当前 held/quarantined 行的原始证据：
          * 已满足条件（父记录到达 / 内容修正后满足契约）→ 放行；
          * 仍不满足 → 保持隔离，绝不悄悄混入；
          * 已被更正版本取代 → 维持 superseded。
        """
        open_rows = [r for r in self.read_ledger(table) if r["state"] in (M.HELD, M.QUARANTINED)]
        if not open_rows:
            return {"table": table, "replayed": 0, "report": None}
        replay_rows = [json.loads(r["raw_payload_json"]) for r in open_rows]
        report = self.ingest(
            table, replay_rows, f"replay:recovery:{table}:{_now_iso()}",
            reprocess_of=open_rows[0]["first_seen_batch"], allow_replay=True,
        )
        return {"table": table, "replayed": len(replay_rows), "report": report}

    # ------------------------------------------------------------------
    # 由台账确定性重建可消费 ODS
    # ------------------------------------------------------------------

    def rebuild_ods(self, table: str) -> int:
        """只从 accepted 台账行重建 ODS 表；held/quarantine 物理上不可达。"""
        contract = self.registry.latest(table)
        ledger = [r for r in self.read_ledger(table) if r["state"] == M.ACCEPTED]
        records: list[dict[str, Any]] = []
        for r in ledger:
            payload = json.loads(r["raw_payload_json"])
            rec = {f.name: self._coerce(f, payload.get(f.name)) for f in contract.fields}
            # 准入元数据：血缘、契约版本、证据指针。
            rec["admission_batch_id"] = r["batch_id"]
            rec["contract_version"] = contract.version
            rec["row_uid"] = r["row_uid"]
            rec["content_hash"] = r["content_hash"]
            # 规范化时间列（原值保留，新增 *_utc 规范化列，不改原列）。
            for f in contract.fields:
                if f.type == "timestamp" and f.tz_semantics in ("local", "naive") and rec[f.name]:
                    dt = _parse_ts(rec[f.name])
                    if dt is not None:
                        if f.tz_semantics == "local":
                            dt_utc = dt - timedelta(hours=self.tz_offset)
                        else:
                            dt_utc = dt.replace(tzinfo=timezone.utc)
                        rec[f"{f.name}_utc"] = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
            records.append(rec)

        records.sort(key=lambda r: tuple(str(r.get(k, "")) for k in contract.uniqueness_key))
        schema = pa.schema(
            [pa.field(f.name, _PA_TYPES[f.type]) for f in contract.fields]
            + [
                pa.field("admission_batch_id", pa.string()),
                pa.field("contract_version", pa.string()),
                pa.field("row_uid", pa.string()),
                pa.field("content_hash", pa.string()),
            ]
            + [
                pa.field(f"{f.name}_utc", pa.string())
                for f in contract.fields
                if f.type == "timestamp" and f.tz_semantics in ("local", "naive")
            ]
        )
        out = self.root / "ods" / table / "part-0001.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        # 规范化缺失的 *_utc 列，保证所有记录对齐同一 schema。
        for rec in records:
            for f in contract.fields:
                if f.type == "timestamp" and f.tz_semantics in ("local", "naive"):
                    rec.setdefault(f"{f.name}_utc", None)
        pq.write_table(pa.Table.from_pylist(records, schema=schema), out)
        return len(records)

    @staticmethod
    def _landing_table(rows: list[dict[str, Any]]) -> pa.Table:
        """构造 landing 物理表：所有列按字符串保留，None 保持空值。

        这样混合类型（同一列既有 int 又有 str）不会让 parquet 推断失败，
        且原始证据的字面值被完整保留，可随时逐字回放。
        """
        names: list[str] = []
        for r in rows:
            for k in r:
                if k not in names:
                    names.append(k)
        cols: dict[str, list[str | None]] = {name: [] for name in names}
        for r in rows:
            for name in names:
                v = r.get(name)
                cols[name].append(None if v is None else str(v))
        schema = pa.schema([pa.field(n, pa.string()) for n in names])
        return pa.Table.from_pylist([dict(zip(names, [cols[n][i] for n in names])) for i in range(len(rows))], schema=schema)

    @staticmethod
    def _coerce(f: FieldSpec, value: Any) -> Any:
        """按契约逻辑类型把台账里的原始值收敛成可写 parquet 的物理值。

        能转则转（字符串数字 → 数值），不能转保留 None 而不是让整表写入失败——
        真正不合规的行进不了这里，这里只做防御性收敛。
        """
        if value is None:
            return None
        try:
            if f.type in ("int",):
                return int(value)
            if f.type == "long":
                return int(value)
            if f.type == "double":
                return float(value)
            if f.type == "boolean":
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ("true", "1", "yes")
            return str(value)
        except (TypeError, ValueError):
            return None

    def rebuild_all_ods(self) -> dict[str, int]:
        return {t: self.rebuild_ods(t) for t in self.registry.tables()}

    # ------------------------------------------------------------------

    @staticmethod
    def _new_batch(
        batch_id: str, table: str, source_file: str, hash_value: str, status: str,
        contract_version: str, received: int, accepted: int, held: int,
        quarantined: int, duplicate: int, schema_changes_json: str,
        error_detail: str, reprocess_of: str | None, ingested_at: str,
    ) -> dict[str, Any]:
        return {
            "batch_id": batch_id,
            "table_name": table,
            "source_file": source_file,
            "content_hash": hash_value,
            "status": status,
            "contract_version": contract_version,
            "rows_received": received,
            "rows_accepted": accepted,
            "rows_held": held,
            "rows_quarantined": quarantined,
            "rows_duplicate": duplicate,
            "schema_changes_json": schema_changes_json,
            "error_detail": error_detail,
            "reprocess_of": reprocess_of or "",
            "ingested_at": ingested_at,
        }
