"""供应商时态维度（SCD2）的编排与校验。

设计见 docs/temporal_supplier_design.md。核心约定：

* changelog 是只追加的变更事实，按 record_hash 内容寻址去重；
* SCD2 版本表与隔离区都是 changelog 的纯函数，每次全量、原子重算，
  因此乱序到达、历史回补、整维重建都不需要人工清理旧分区；
* 无法判定先后的记录进隔离区并显式计数，strict 模式下让作业失败。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .config import settings

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "dwd"

# 版本表/隔离区/日志/兼容视图的固定命名
CHANGELOG = ("dwd", "supplier_changelog")
SCD2_TABLE = ("dwd", "dim_supplier_scd2")
REJECT_TABLE = ("dwd", "dim_supplier_reject")
CURRENT_VIEW = "dwd_dim_supplier"

# changelog 的物理 schema，首次摄入时据此建空表，
# 让 NOT EXISTS 去重 SQL 在第一批数据到达时也能解析。
CHANGELOG_COLUMNS = {
    "supplier_id": "VARCHAR",
    "supplier_name": "VARCHAR",
    "region": "VARCHAR",
    "credit_level": "VARCHAR",
    "valid_from": "TIMESTAMP",
    "valid_to": "TIMESTAMP",
    "updated_at": "VARCHAR",
    "record_hash": "VARCHAR",
    "ingested_at": "TIMESTAMP",
}


def _sql(name: str, **params: str) -> str:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    return text.format(**params)


@dataclass
class TemporalReport:
    """一次时态维度构建的可观测指标。"""

    changelog_rows: int = 0
    changelog_new_rows: int = 0
    version_rows: int = 0
    rejected_rows: int = 0
    ambiguous_groups: int = 0
    invalid_rows: int = 0
    interval_adjusted_rows: int = 0
    unresolved_facts: int = 0
    versions_per_supplier_max: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {k: (dict(v) if isinstance(v, dict) else v) for k, v in self.__dict__.items()}


class TemporalConflictError(RuntimeError):
    """strict 模式下存在被拒绝（无法判定先后）的源记录。"""


def ingest_supplier_changelog(lake, ingested_at: str | None = None) -> int:
    """把 ods_supplier 的新内容追加进变更日志，返回新增行数。

    去重键是 record_hash：重复推送与整批重放零增长。
    """
    ingested_at = ingested_at or f"{settings.logical_date} 00:00:00"
    lake.ensure_empty(*CHANGELOG, columns=CHANGELOG_COLUMNS)
    sql = _sql("supplier_changelog_ingest.sql", ingested_at=ingested_at)
    before = lake.count(*CHANGELOG)
    lake.merge(
        CHANGELOG[0],
        CHANGELOG[1],
        sql,
        keys=["record_hash"],
        update_set={},
    )
    return lake.count(*CHANGELOG) - before


def build_supplier_dim(lake, strict: bool = False) -> TemporalReport:
    """从 changelog 全量重建 SCD2 版本表与隔离区，返回构建报告。

    整表原子重写（临时目录 + rename），不依赖人工清理旧分区。
    strict=True 时若存在被拒绝记录则抛 TemporalConflictError。
    """
    report = TemporalReport()
    if not lake.exists(*CHANGELOG):
        # 没有日志时先建空表，保证下游视图与 join 始终有定义
        ingest_supplier_changelog(lake)

    report.changelog_rows = lake.count(*CHANGELOG)
    rejected_at = f"{settings.logical_date} 00:00:00"

    reject_sql = _sql("dim_supplier_reject.sql", rejected_at=rejected_at)
    report.rejected_rows = lake.rewrite(reject_sql, *REJECT_TABLE)
    scd_sql = _sql("dim_supplier_scd2.sql")
    report.version_rows = lake.rewrite(scd_sql, *SCD2_TABLE)
    _register_current_view(lake)

    con = lake.con
    if report.rejected_rows:
        rows = con.execute(
            "SELECT reject_reason, count(*) FROM dwd_dim_supplier_reject GROUP BY 1"
        ).fetchall()
        report.reject_reasons = dict(rows)
        report.ambiguous_groups = con.execute(
            "SELECT count(DISTINCT (supplier_id, valid_from, updated_at)) "
            "FROM dwd_dim_supplier_reject WHERE reject_reason = 'ambiguous_ordering'"
        ).fetchone()[0]
        report.invalid_rows = con.execute(
            "SELECT count(*) FROM dwd_dim_supplier_reject "
            "WHERE reject_reason IN ('invalid_valid_from', 'invalid_interval')"
        ).fetchone()[0]

    report.interval_adjusted_rows = con.execute(
        "SELECT count(*) FROM dwd_dim_supplier_scd2 WHERE interval_adjusted"
    ).fetchone()[0]
    report.versions_per_supplier_max = con.execute(
        "SELECT coalesce(max(c), 0) FROM ("
        "SELECT count(*) AS c FROM dwd_dim_supplier_scd2 GROUP BY supplier_id)"
    ).fetchone()[0]

    if strict and report.rejected_rows:
        raise TemporalConflictError(
            f"{report.rejected_rows} 条源记录无法判定先后，已隔离：{report.reject_reasons}"
        )
    return report


def _register_current_view(lake) -> None:
    """注册 SCD1 兼容视图（旧表名、旧列结构，值为当前版本）。"""
    sql = (SQL_DIR / "dim_supplier_current_view.sql").read_text(encoding="utf-8")
    lake.register_logical_view(CURRENT_VIEW, sql)


def count_unresolved_facts(lake) -> int:
    """统计未关联到任何供应商版本的事实行数。"""
    if not lake.exists("dwd", "fact_purchase_order"):
        return 0
    return lake.con.execute(
        "SELECT count(*) FROM dwd_fact_purchase_order WHERE supplier_sk IS NULL"
    ).fetchone()[0]


def temporal_invariants(lake) -> dict[str, float]:
    """时态模型不变式的原始数字（供断言与 verify 脚本引用）。"""
    out: dict[str, float] = {}
    con = lake.con
    if lake.exists(*SCD2_TABLE):
        # I1/I2：重叠区间数与断档区间数都必须为 0
        out["scd2_overlap_pairs"] = float(
            con.execute(
                """
                SELECT count(*) FROM dwd_dim_supplier_scd2 a
                JOIN dwd_dim_supplier_scd2 b
                  ON a.supplier_id = b.supplier_id AND a.version_seq < b.version_seq
                WHERE a.valid_to > b.valid_from
                """
            ).fetchone()[0]
        )
        out["scd2_gap_pairs"] = float(
            con.execute(
                """
                SELECT count(*) FROM dwd_dim_supplier_scd2 a
                JOIN dwd_dim_supplier_scd2 b
                  ON a.supplier_id = b.supplier_id
                 AND b.version_seq = a.version_seq + 1
                WHERE a.valid_to IS DISTINCT FROM b.valid_from
                """
            ).fetchone()[0]
        )
        # I3：同一供应商同时刻 is_current 只能有一个
        out["scd2_current_dupes"] = float(
            con.execute(
                "SELECT count(*) FROM ("
                "SELECT supplier_id FROM dwd_dim_supplier_scd2 WHERE is_current "
                "GROUP BY supplier_id HAVING count(*) > 1)"
            ).fetchone()[0]
        )
        # I3 对事实的保证：点时态 join 命中超过一个版本的"事实行"数（必须为 0）。
        # 给每条物理事实行编号后统计其命中版本数，避免上游重复推送的同 po_id 行
        # 被聚合误判；一条事实行命中 0 个版本计入 fact_unresolved，不算多版本命中。
        if lake.exists("dwd", "fact_purchase_order"):
            out["fact_multi_match"] = float(
                con.execute(
                    """
                    WITH numbered AS (
                        SELECT *, row_number() OVER () AS rid
                        FROM dwd_fact_purchase_order
                    ),
                    matched AS (
                        SELECT n.rid, count(d.supplier_sk) AS hit
                        FROM numbered n
                        LEFT JOIN dwd_dim_supplier_scd2 d
                          ON n.supplier_id = d.supplier_id
                         AND CAST(n.created_at AS TIMESTAMP) >= d.valid_from
                         AND CAST(n.created_at AS TIMESTAMP)
                             < COALESCE(d.valid_to, TIMESTAMP '9999-12-31 00:00:00')
                        GROUP BY n.rid
                    )
                    SELECT count(*) FROM matched WHERE hit > 1
                    """
                ).fetchone()[0]
            )
            out["fact_unresolved"] = float(count_unresolved_facts(lake))
        out["scd2_versions"] = float(lake.count(*SCD2_TABLE))
    if lake.exists(*REJECT_TABLE):
        out["scd2_rejected"] = float(lake.count(*REJECT_TABLE))
    return out
