"""配置与路径约定。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    """管线运行配置。"""

    # 本地湖仓根目录。分层以子目录体现：ods/ dwd/ dws/ ads/
    lake_root: Path = field(default_factory=lambda: Path(_env("LAKE_ROOT", "lake")))
    # 逻辑日期。所有增量作业都以它为基准，便于回放。
    logical_date: str = field(default_factory=lambda: _env("LOGICAL_DATE", "2026-11-10"))
    # 种子，决定示例数据的分布。
    seed: int = field(default_factory=lambda: int(_env("SEED", "20261112")))
    # 源数据的时区偏移（小时）。
    source_tz_offset_hours: int = field(default_factory=lambda: int(_env("SRC_TZ_OFFSET", "8")))

    @property
    def ods(self) -> Path:
        return self.lake_root / "ods"

    @property
    def landing(self) -> Path:
        return self.lake_root / "landing"

    @property
    def quarantine(self) -> Path:
        return self.lake_root / "quarantine"

    @property
    def dwd(self) -> Path:
        return self.lake_root / "dwd"

    @property
    def dws(self) -> Path:
        return self.lake_root / "dws"

    @property
    def ads(self) -> Path:
        return self.lake_root / "ads"

    def ensure(self) -> None:
        for p in (self.landing, self.quarantine, self.ods, self.dwd, self.dws, self.ads):
            p.mkdir(parents=True, exist_ok=True)


settings = Settings()

# ODS 四张源表
ODS_TABLES = ("supplier", "purchase_order", "goods_receipt", "settlement")

# 业务主键定义。用于去重与 SCD2。
NATURAL_KEYS = {
    "supplier": "supplier_id",
    "purchase_order": "po_id",
    "goods_receipt": "gr_id",
    "settlement": "settlement_id",
}
