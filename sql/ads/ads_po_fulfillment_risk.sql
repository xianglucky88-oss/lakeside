-- ADS：采购单履约风险明细（分析主表）
--
-- 面向采购分析人员，按日期、仓库、供应商、状态筛选在途时长、到货完成度与
-- 异常金额。数据直接取 dws_po_fulfillment 的指定快照分区，
-- 不在本层做任何跨表 JOIN，因此口径与 DWS 完全一致、无二次扇出。
--
-- 三类含义严格分开，不混为一种异常：
--   未结算   -> unsettled_flag=1（业务尚未发生，不是数据问题）；
--   未入库   -> no_receipt_flag=1 / fulfillment_status='NO_RECEIPT'；
--   数据缺失 -> data_issue_codes 非空（如 MISSING_EXPECTED_DATE / ORPHAN_EVENT）。
--
-- 分区：as_of_date。补传后重算只覆盖对应快照分区，历史结论留痕可追溯。

SELECT
    as_of_date,
    biz_date,
    expected_date,
    supplier_id,
    supplier_name,
    region,
    warehouse,
    po_id,
    po_version,
    is_reapproved,
    currency,
    fulfillment_status,
    risk_codes,
    data_issue_codes,
    po_amount_cents,
    received_cents,
    settled_cents,
    completion_rate,
    in_transit_days,
    fulfillment_days,
    expected_delay_days,
    overdue_days,
    gr_batches_valid,
    gr_batches_voided,
    late_gr_batches,
    gr_warehouse_count,
    settlements,
    ccy_reject_cnt,
    orphan_event_cnt,
    voided_receipt_cnt,
    unsettled_flag,
    no_receipt_flag,
    settle_vs_receipt_cents,
    approved_at,
    first_approved_at,
    first_received_at,
    last_received_at,
    first_settled_at,
    last_settled_at,
    source_watermark,
    calculated_at,
    risk_evidence
FROM dws_po_fulfillment
