-- DWD：入库事实
--
-- 客观事实：一个 po_id 可以对应多条入库单（分批到货），这是一对多。
-- 因此下游任何「采购单 join 入库单」的汇总都必须先聚合，
-- 否则金额会按入库笔数放大。

SELECT
    gr_id,
    po_id,
    received_qty,
    received_cents,
    received_at,
    warehouse
FROM ods_goods_receipt
