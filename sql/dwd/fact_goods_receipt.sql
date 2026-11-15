-- DWD：入库事实
--
-- 客观事实：一个 po_id 可以对应多条入库单（分批到货），这是一对多。
-- 因此下游任何「采购单 join 入库单」的汇总都必须先在本层之外按 po_id
-- 预聚合，否则金额会按入库笔数放大。
--
-- 清洗规则：
--   1. gr_status='voided' 的撤销单剔除出有效金额，保留在本层并打标
--      is_voided=1，便于追溯撤销轨迹，其金额不参与任何履约指标；
--   2. received_at 是业务事件时间；ingested_at 是数据进湖时间，
--      迟到补传（event 早、ingested 晚）不被丢弃，快照按计算时点可见性重算；
--   3. 重复 gr_id 按 ingested_at 保留最早一条。

WITH ranked AS (
    SELECT
        gr_id,
        po_id,
        received_qty,
        received_cents,
        received_at,
        warehouse,
        gr_status,
        ingested_at,
        row_number() OVER (PARTITION BY gr_id ORDER BY ingested_at ASC) AS dup_rn
    FROM ods_goods_receipt
)
SELECT
    gr_id,
    po_id,
    received_qty,
    received_cents,
    received_at,
    warehouse,
    gr_status,
    CASE WHEN gr_status = 'voided' THEN 1 ELSE 0 END AS is_voided,
    ingested_at
FROM ranked
WHERE dup_rn = 1
