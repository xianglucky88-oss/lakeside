-- DWD：采购单事实

SELECT
    po_id,
    supplier_id,
    amount_cents,
    currency,
    status,
    created_at,
    biz_date
FROM ods_purchase_order
