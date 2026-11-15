-- DWD：采购单事实
--
-- 在事实构建时做一次点时态关联，把供应商代理键 supplier_sk 固化进事实行：
--   * I1/I3（版本区间连续且互不重叠）保证每条事实至多命中一个版本，
--     不会因区间重叠产生一对多放大；
--   * 区间非等值比较只在本层支付一次，下游 DWS/ADS 全部退化为 supplier_sk 等值 join；
--   * 命中 0 行（供应商缺失或事实早于首个版本）时 supplier_sk 为 NULL，
--     由构建报告的 unresolved_facts 显式计数，不丢事实、不静默补值。

SELECT
    po.po_id,
    po.supplier_id,
    d.supplier_sk,
    po.amount_cents,
    po.currency,
    po.status,
    po.created_at,
    po.biz_date
FROM ods_purchase_order po
LEFT JOIN dwd_dim_supplier_scd2 d
    ON po.supplier_id = d.supplier_id
   AND CAST(po.created_at AS TIMESTAMP) >= d.valid_from
   AND CAST(po.created_at AS TIMESTAMP) < COALESCE(d.valid_to, TIMESTAMP '9999-12-31 00:00:00')
