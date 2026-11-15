"""可演进的数据契约（Data Contract）。

每张源表的契约描述：
  * 字段名、逻辑类型、是否必填；
  * 业务主键（natural_key，决定一条记录身份）与唯一键（uniqueness_key，决定去重粒度）；
  * 跨表引用（references）；
  * 时间字段的时区语义（local / utc / date / naive）；
  * 时区一致性校验组（timezone_checks）。

契约带语义化版本号。字段变更分成两类：
  * 可兼容扩展（新增可空字段、类型拓宽）→ minor +1，新契约自动生效，旧批次仍可消费；
  * 破坏性变更（删除/重命名字段、必填字段缺失、类型收窄/不兼容转换）→ major +1，
    批次整体 hold，不允许任何行进入 ODS，必须显式迁移后才能重放。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

# 逻辑类型名与 DuckDB / pyarrow 的映射在 engine 层完成，契约只持有逻辑类型。
SUPPORTED_TYPES = {
    "string",
    "int",
    "long",
    "double",
    "boolean",
    "timestamp",  # 带时间语义的字符串，时区由 tz_semantics 决定
    "date",
}

# 允许的类型拓宽方向（左 → 右 是兼容的）。
WIDENING = {
    "int": {"long", "double"},
    "long": {"double"},
    "date": {"timestamp"},
}

TIMEZONE_SEMANTICS = {"local", "utc", "date", "naive"}


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool = True
    # 时间字段的时区语义；非时间字段为 None。
    tz_semantics: str | None = None
    description: str = ""

    def __post_init__(self) -> None:
        if self.type not in SUPPORTED_TYPES:
            raise ValueError(f"不支持的逻辑类型: {self.type}")
        if self.tz_semantics is not None and self.tz_semantics not in TIMEZONE_SEMANTICS:
            raise ValueError(f"不支持的时区语义: {self.tz_semantics}")


@dataclass(frozen=True)
class ReferenceSpec:
    """跨表引用：本表字段引用目标表的业务主键。"""

    column: str
    references_table: str
    references_column: str


@dataclass(frozen=True)
class TimezoneCheck:
    """时区一致性校验：utc 列按偏移换算后必须等于 local 列。"""

    utc_column: str
    local_column: str
    offset_hours_column: str | None = None  # None 时用表级默认偏移


@dataclass(frozen=True)
class TableContract:
    table: str
    version: str
    natural_key: str
    uniqueness_key: list[str]
    fields: tuple[FieldSpec, ...]
    references: tuple[ReferenceSpec, ...] = ()
    timezone_checks: tuple[TimezoneCheck, ...] = ()
    # 更正排序字段：同键新版本必须在这些列上不小于旧版本（通常是 updated_at / *_at）。
    ordering_fields: tuple[str, ...] = ()
    description: str = ""

    def field(self, name: str) -> FieldSpec | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    @property
    def field_names(self) -> set[str]:
        return {f.name for f in self.fields}

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TableContract":
        return cls(
            table=d["table"],
            version=d["version"],
            natural_key=d["natural_key"],
            uniqueness_key=list(d["uniqueness_key"]),
            fields=tuple(FieldSpec(**f) for f in d["fields"]),
            references=tuple(ReferenceSpec(**r) for r in d.get("references", ())),
            timezone_checks=tuple(TimezoneCheck(**t) for t in d.get("timezone_checks", ())),
            ordering_fields=tuple(d.get("ordering_fields", ())),
            description=d.get("description", ""),
        )


def parse_version(version: str) -> tuple[int, int]:
    major, minor = version.split(".")
    return int(major), int(minor)


def bump_version(version: str, breaking: bool) -> str:
    major, minor = parse_version(version)
    return f"{major + 1}.0" if breaking else f"{major}.{minor + 1}"


def classify_field_change(
    old: FieldSpec | None, new: FieldSpec | None
) -> str:
    """返回 'compatible' / 'breaking' / 'none'。"""
    if old is None and new is not None:
        # 新增可空字段是兼容扩展；新增必填字段意味着旧数据无法满足 → 破坏。
        return "compatible" if not new.required else "breaking"
    if new is None and old is not None:
        return "breaking"  # 删除字段
    if old is None or new is None:
        return "none"
    if old.name != new.name:
        return "breaking"
    # description 只是文档，不构成 schema 变更。
    schema_equal = (
        old.type == new.type
        and old.required == new.required
        and old.tz_semantics == new.tz_semantics
    )
    if schema_equal:
        return "none"
    if old.type == new.type:
        type_ok = True
    else:
        type_ok = new.type in WIDENING.get(old.type, set())
    required_ok = not (new.required and not old.required)  # 可空变必填是破坏
    tz_ok = old.tz_semantics == new.tz_semantics
    return "compatible" if (type_ok and required_ok and tz_ok) else "breaking"


def diff_contracts(
    old: TableContract, observed_fields: tuple[FieldSpec, ...]
) -> tuple[bool, list[dict[str, str]]]:
    """把批次实测 schema 与已注册契约对比。

    返回 (是否破坏性, 变更明细)。
    """
    old_map = {f.name: f for f in old.fields}
    new_map = {f.name: f for f in observed_fields}
    changes: list[dict[str, str]] = []
    breaking = False
    for name in sorted(set(old_map) | set(new_map)):
        kind = classify_field_change(old_map.get(name), new_map.get(name))
        if kind == "none":
            continue
        if kind == "breaking":
            breaking = True
        changes.append(
            {
                "field": name,
                "change": kind,
                "old_type": old_map[name].type if name in old_map else "-",
                "new_type": new_map[name].type if name in new_map else "-",
            }
        )
    return breaking, changes


class ContractRegistry:
    """契约注册中心。一个湖目录一份，历史版本全部保留。"""

    def __init__(self, root: Path) -> None:
        self.dir = root / "contracts"
        self.dir.mkdir(parents=True, exist_ok=True)

    def path(self, table: str) -> Path:
        return self.dir / f"{table}.json"

    def has(self, table: str) -> bool:
        return self.path(table).exists()

    def latest(self, table: str) -> TableContract:
        p = self.path(table)
        if not p.exists():
            raise KeyError(f"契约不存在: {table}")
        doc = json.loads(p.read_text(encoding="utf-8"))
        # 文件里保存全部版本，取最后一个（版本单调递增）。
        return TableContract.from_dict(doc["versions"][-1])

    def all_versions(self, table: str) -> list[TableContract]:
        doc = json.loads(self.path(table).read_text(encoding="utf-8"))
        return [TableContract.from_dict(v) for v in doc["versions"]]

    def register(self, contract: TableContract) -> TableContract:
        """注册新版本。同表同版本号重复注册是幂等的。"""
        p = self.path(contract.table)
        if p.exists():
            doc = json.loads(p.read_text(encoding="utf-8"))
            versions = doc["versions"]
            existing = {v["version"] for v in versions}
            if contract.version in existing:
                return TableContract.from_dict(
                    next(v for v in versions if v["version"] == contract.version)
                )
            versions.append(contract.to_dict())
        else:
            doc = {"table": contract.table, "versions": [contract.to_dict()]}
        p.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        return contract

    def evolve(
        self, table: str, observed_fields: tuple[FieldSpec, ...]
    ) -> tuple[TableContract, bool, list[dict[str, str]]]:
        """按实测 schema 演进契约。

        兼容扩展 → 自动 minor 升版并注册；
        破坏性变更 → 抛 ContractBreaking，调用方必须 hold 整个批次。
        """
        current = self.latest(table)
        breaking, changes = diff_contracts(current, observed_fields)
        if not changes:
            return current, False, []
        if breaking:
            raise ContractBreaking(table, current.version, changes)
        evolved = TableContract(
            table=current.table,
            version=bump_version(current.version, breaking=False),
            natural_key=current.natural_key,
            uniqueness_key=list(current.uniqueness_key),
            fields=tuple(sorted(observed_fields, key=lambda f: f.name)),
            references=current.references,
            timezone_checks=current.timezone_checks,
            ordering_fields=current.ordering_fields,
            description=current.description,
        )
        self.register(evolved)
        return evolved, True, changes

    def tables(self) -> list[str]:
        return sorted(p.stem for p in self.dir.glob("*.json"))


class ContractBreaking(Exception):
    """破坏性 schema 变更：批次必须整体 hold。"""

    def __init__(self, table: str, version: str, changes: list[dict[str, str]]) -> None:
        self.table = table
        self.version = version
        self.changes = changes
        super().__init__(
            f"表 {table} 契约 v{version} 出现破坏性变更: "
            + ", ".join(f"{c['field']}({c['change']})" for c in changes)
        )


# ------------------------------------------------------------------
# 初始契约（v1.0）。字段与 generator 产出的四张源表一一对应。
# ------------------------------------------------------------------

def initial_contracts() -> list[TableContract]:
    return [
        TableContract(
            table="supplier",
            version="1.0",
            natural_key="supplier_id",
            uniqueness_key=["supplier_id", "valid_from"],
            fields=tuple(
                FieldSpec(*f)
                for f in (
                    ("supplier_id", "string", True, None, "供应商业务主键"),
                    ("supplier_name", "string", True),
                    ("region", "string", True),
                    ("credit_level", "string", True),
                    ("valid_from", "timestamp", True, "local", "生效开始（本地时区）"),
                    ("valid_to", "timestamp", False, "local", "生效结束（本地时区）"),
                    ("is_current", "boolean", True),
                    ("updated_at", "timestamp", True, "naive"),
                )
            ),
            ordering_fields=("updated_at",),
            description="供应商主数据，同一主键多版本（SCD2 输入）",
        ),
        TableContract(
            table="purchase_order",
            version="1.0",
            natural_key="po_id",
            uniqueness_key=["po_id"],
            fields=tuple(
                FieldSpec(*f)
                for f in (
                    ("po_id", "string", True, None, "采购单业务主键"),
                    ("supplier_id", "string", True),
                    ("amount_cents", "long", True),
                    ("currency", "string", True),
                    ("status", "string", True),
                    ("created_at", "timestamp", True, "local"),
                    ("biz_date", "date", True, "local"),
                )
            ),
            references=(ReferenceSpec("supplier_id", "supplier", "supplier_id"),),
            ordering_fields=("created_at",),
            description="采购单",
        ),
        TableContract(
            table="goods_receipt",
            version="1.0",
            natural_key="gr_id",
            uniqueness_key=["gr_id"],
            fields=tuple(
                FieldSpec(*f)
                for f in (
                    ("gr_id", "string", True, None, "入库单业务主键"),
                    ("po_id", "string", True),
                    ("received_qty", "int", True),
                    ("received_cents", "long", True),
                    ("received_at", "timestamp", True, "local"),
                    ("warehouse", "string", True),
                )
            ),
            references=(ReferenceSpec("po_id", "purchase_order", "po_id"),),
            ordering_fields=("received_at",),
            description="入库单，一个采购单可分多批入库",
        ),
        TableContract(
            table="settlement",
            version="1.0",
            natural_key="settlement_id",
            uniqueness_key=["settlement_id"],
            fields=tuple(
                FieldSpec(*f)
                for f in (
                    ("settlement_id", "string", True, None, "结算单业务主键"),
                    ("po_id", "string", True),
                    ("settled_cents", "long", True),
                    ("settled_at_utc", "timestamp", True, "utc", "结算时刻（UTC）"),
                    ("biz_date_local", "date", True, "local", "源系统给出的本地业务日"),
                    ("channel", "string", True),
                )
            ),
            references=(ReferenceSpec("po_id", "purchase_order", "po_id"),),
            timezone_checks=(
                TimezoneCheck("settled_at_utc", "biz_date_local"),
            ),
            ordering_fields=("settled_at_utc",),
            description="结算单，UTC 时间与本地业务日必须一致",
        ),
    ]
