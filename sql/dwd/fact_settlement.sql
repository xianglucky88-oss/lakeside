-- DWD：结算事实
--
-- settled_at_utc 是 UTC 时间；settle_date 由它截取得出。

SELECT
    settlement_id,
    po_id,
    settled_cents,
    settled_at_utc,
    substr(settled_at_utc, 1, 10) AS settle_date
FROM ods_settlement
