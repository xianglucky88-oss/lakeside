-- ADS：供应商结算月报
--
-- 面向财务对账。口径：
--   月度采购金额   = 该月所有 approved 采购单金额之和（去重后）
--   月度入库金额   = 该月入库明细金额之和
--   月度结算金额   = 该月结算金额之和（按本地业务日归属）
--   结算率         = 结算金额 / 采购金额
--
-- 当前状态：TODO —— 文件是空模板。
-- 出题时要求选手按上述口径补齐，并用 scripts/verify_lake.py 的金额守恒断言验证。

SELECT
    NULL::VARCHAR AS month,
    NULL::VARCHAR AS supplier_id,
    NULL::BIGINT  AS po_amount_cents,
    NULL::BIGINT  AS received_cents,
    NULL::BIGINT  AS settled_cents,
    NULL::DOUBLE  AS settle_rate
WHERE false
