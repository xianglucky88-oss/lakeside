-- DWS：采购-入库-结算 汇总宽表
--
-- 粒度：po_id 一行（采购单全部状态，cancelled 单据的入库/结算按隔离规则计 0）。
-- 与 dws_po_fulfillment 的区别：本表只做金额守恒对账用的精简汇总；
-- 履约时效、状态机与风险码在 dws_po_fulfillment。
--
-- 防扇出：入库与结算先各自按 po_id 聚合，再与采购单一对一连接，
--         不允许 GR × ST 直接 JOIN（那会把采购金额放大 batches×settlements 倍）。

WITH gr_agg AS (
    SELECT
        gr.po_id,
        count_if(gr.is_voided = 0) AS gr_batches,
        sum(CASE WHEN gr.is_voided = 0 THEN gr.received_cents ELSE 0 END) AS received_cents
    FROM dwd_fact_goods_receipt gr
    JOIN dwd_fact_purchase_order po ON gr.po_id = po.po_id AND po.status = 'approved'
    GROUP BY gr.po_id
),
st_agg AS (
    SELECT
        st.po_id,
        count(*) AS settlements,
        sum(st.settled_cents) AS settled_cents
    FROM dwd_fact_settlement st
    JOIN dwd_fact_purchase_order po ON st.po_id = po.po_id
    WHERE po.status = 'approved' AND st.currency = po.currency
    GROUP BY st.po_id
)
SELECT
    po.po_id,
    po.supplier_id,
    ds.supplier_name,
    ds.region,
    po.biz_date,
    po.amount_cents                AS po_amount_cents,
    CAST(coalesce(gr.received_cents, 0) AS BIGINT) AS received_cents,
    CAST(coalesce(st.settled_cents, 0) AS BIGINT)  AS settled_cents,
    CAST(coalesce(gr.gr_batches, 0) AS BIGINT)     AS gr_batches,
    CAST(coalesce(st.settlements, 0) AS BIGINT)    AS settlements
FROM dwd_fact_purchase_order po
LEFT JOIN dwd_dim_supplier ds ON po.supplier_id = ds.supplier_id
LEFT JOIN gr_agg gr ON po.po_id = gr.po_id
LEFT JOIN st_agg st ON po.po_id = st.po_id
