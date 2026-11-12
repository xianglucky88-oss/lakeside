-- DWS：采购-入库-结算 汇总宽表（草稿）
--
-- 粒度：po_id 一行
-- 指标：采购金额、已入库金额、已结算金额、入库批次、结算笔数
-- 维度：供应商、区域、业务日期

SELECT
    po.po_id,
    po.supplier_id,
    ds.supplier_name,
    ds.region,
    po.biz_date,
    sum(po.amount_cents)                              AS po_amount_cents,
    sum(gr.received_cents)                            AS received_cents,
    sum(st.settled_cents)                             AS settled_cents,
    count(DISTINCT gr.gr_id)                          AS gr_batches,
    count(DISTINCT st.settlement_id)                  AS settlements
FROM dwd_fact_purchase_order po
LEFT JOIN dwd_dim_supplier ds        ON po.supplier_id = ds.supplier_id
LEFT JOIN dwd_fact_goods_receipt gr  ON po.po_id = gr.po_id
LEFT JOIN dwd_fact_settlement st     ON po.po_id = st.po_id
GROUP BY po.po_id, po.supplier_id, ds.supplier_name, ds.region, po.biz_date
