-- ADS：供应商月度履约汇总
--
-- 面向采购管理与财务对账。月份按采购单本地业务日 biz_date 归属；
-- 取 dws_po_fulfillment 的当前快照（as_of_date = {{LOGICAL_DATE}}），
-- 指标全部来自已按 po_id 预聚合的宽表，按供应商可加、不放大。
--
-- 金额口径：
--   po_amount  = approved 采购单当前版本金额（重新批准不重复计）；
--   received   = 有效入库金额（撤销单、取消单归属事件不计）；
--   settled    = 同币种有效结算金额（币种不一致事件被隔离，不计入）。
-- 状态与风险计数与明细 ads_po_fulfillment_risk 的 risk_codes 完全同源。

WITH base AS (
    SELECT * FROM dws_po_fulfillment
)
SELECT
    substr(biz_date, 1, 7) AS month,
    supplier_id,
    any_value(supplier_name) AS supplier_name,
    any_value(region) AS region,
    count(*) AS po_cnt,
    sum(CASE WHEN fulfillment_status = 'CLOSED_ON_TIME' THEN 1 ELSE 0 END) AS closed_on_time_cnt,
    sum(CASE WHEN fulfillment_status = 'CLOSED_LATE' THEN 1 ELSE 0 END) AS closed_late_cnt,
    sum(CASE WHEN fulfillment_status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_cnt,
    sum(CASE WHEN fulfillment_status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_cnt,
    sum(CASE WHEN fulfillment_status = 'NO_RECEIPT' THEN 1 ELSE 0 END) AS no_receipt_cnt,
    sum(CASE WHEN risk_codes LIKE '%SETTLE_EXCEEDS_RECEIPT%' THEN 1 ELSE 0 END) AS settle_exceed_cnt,
    sum(CASE WHEN risk_codes LIKE '%OVER_RECEIVED%' THEN 1 ELSE 0 END) AS over_received_cnt,
    sum(CASE WHEN risk_codes LIKE '%SPLIT_DELAY%' THEN 1 ELSE 0 END) AS split_delay_cnt,
    sum(CASE WHEN data_issue_codes <> '' THEN 1 ELSE 0 END) AS data_issue_po_cnt,
    CAST(sum(po_amount_cents) AS BIGINT) AS po_amount_cents,
    CAST(sum(received_cents) AS BIGINT) AS received_cents,
    CAST(sum(settled_cents) AS BIGINT) AS settled_cents,
    CAST(sum(CASE WHEN settle_vs_receipt_cents > 0 THEN settle_vs_receipt_cents ELSE 0 END) AS BIGINT)
        AS over_settled_cents,
    round(
        CASE WHEN sum(po_amount_cents) > 0
             THEN sum(received_cents) * 1.0 / sum(po_amount_cents) END,
        4
    ) AS completion_rate,
    round(
        CASE WHEN sum(received_cents) > 0
             THEN sum(settled_cents) * 1.0 / sum(received_cents) END,
        4
    ) AS settle_rate,
    CAST('{{LOGICAL_DATE}}' AS DATE) AS as_of_date
FROM base
GROUP BY substr(biz_date, 1, 7), supplier_id
