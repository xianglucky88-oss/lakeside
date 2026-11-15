-- DWD：履约事件隔离区
--
-- 凡是"不能安全计入采购单履约金额"的事件都落到这里，并写明拒绝原因，
-- 而不是静默丢弃或强行并入。宽表只统计 reject_reason 之外的有效事件。
--
-- reject_reason 取值：
--   CURRENCY_MISMATCH  结算币种与采购单币种不一致，无汇率口径时拒绝折算；
--   ORPHAN_EVENT       入库/结算引用的 po_id 在采购单事实中不存在；
--   PO_NOT_APPROVED    采购单已取消，后续入库/结算不具备履约归属；
--   VOIDED_RECEIPT     撤销入库（同时在入库事实保留 is_voided 标记）。
--
-- 每条记录带 ingested_at，补传修正后重跑本表会自动刷新，可追溯。

SELECT
    'goods_receipt' AS source_table,
    gr_id AS event_id,
    gr.po_id,
    gr.received_cents AS event_cents,
    CAST(NULL AS VARCHAR) AS currency,
    CASE
        WHEN po.po_id IS NULL THEN 'ORPHAN_EVENT'
        WHEN po.status = 'cancelled' THEN 'PO_NOT_APPROVED'
        WHEN gr.gr_status = 'voided' THEN 'VOIDED_RECEIPT'
    END AS reject_reason,
    gr.received_at AS event_time,
    gr.ingested_at
FROM dwd_fact_goods_receipt gr
LEFT JOIN dwd_fact_purchase_order po ON gr.po_id = po.po_id
WHERE po.po_id IS NULL
   OR po.status = 'cancelled'
   OR gr.gr_status = 'voided'

UNION ALL

SELECT
    'settlement' AS source_table,
    st.settlement_id AS event_id,
    st.po_id,
    st.settled_cents AS event_cents,
    st.currency,
    CASE
        WHEN po.po_id IS NULL THEN 'ORPHAN_EVENT'
        WHEN po.status = 'cancelled' THEN 'PO_NOT_APPROVED'
        WHEN st.currency <> po.currency THEN 'CURRENCY_MISMATCH'
    END AS reject_reason,
    st.settled_at_utc AS event_time,
    st.ingested_at
FROM dwd_fact_settlement st
LEFT JOIN dwd_fact_purchase_order po ON st.po_id = po.po_id
WHERE po.po_id IS NULL
   OR po.status = 'cancelled'
   OR st.currency <> po.currency
