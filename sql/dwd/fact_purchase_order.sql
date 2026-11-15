-- DWD：采购单事实
--
-- 粒度：po_id 一行。
-- 归属规则：
--   1. 完全一致的重复推送（同 po_id、同 version）按 ingested_at 保留最早一条，
--      其余丢弃，保证下游任何聚合都不会把同一采购单算两次；
--   2. 同一 po_id 被重新批准会出现多条 version，取 version 最大的一条为当前
--      业务事实；首次批准时间保留在 first_approved_at，最新批准时间放在
--      approved_at，时效一律以最新一次批准重新起算；
--   3. 保留 status，cancelled 采购单不进入履约宽表，但可被单独统计；
--   4. 采购金额取当前版本金额，不跨版本相加（相加等于把变更当新订单）。

WITH ranked AS (
    SELECT
        po_id,
        supplier_id,
        amount_cents,
        currency,
        status,
        version,
        created_at,
        biz_date,
        approved_at,
        expected_at,
        ingested_at,
        row_number() OVER (
            PARTITION BY po_id, version
            ORDER BY ingested_at ASC, approved_at ASC, amount_cents ASC
        ) AS dup_rn,
        first_value(approved_at) OVER (
            PARTITION BY po_id ORDER BY version ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_approved_at
    FROM ods_purchase_order
),
dedup AS (
    SELECT * FROM ranked WHERE dup_rn = 1
),
current_version AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY po_id ORDER BY version DESC, ingested_at DESC) AS ver_rn
    FROM dedup
)
SELECT
    po_id,
    supplier_id,
    amount_cents,
    currency,
    status,
    version,
    created_at,
    biz_date,
    first_approved_at,
    approved_at,
    expected_at,
    ingested_at
FROM current_version
WHERE ver_rn = 1
