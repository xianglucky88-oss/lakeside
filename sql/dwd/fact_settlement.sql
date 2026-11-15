-- DWD：结算事实
--
-- settled_at_utc 是 UTC 时间；本地业务日按源系统固定时区偏移换算，
-- 不能直接 substr UTC 时间戳——UTC 16:00 之后的结算在 +8 时区已跨日。
-- 偏移量由配置渲染：{{SRC_TZ_OFFSET_HOURS}}。
--
-- 清洗规则：
--   1. 保留 currency 与 po 币种，供宽表做币种一致性判定；
--   2. biz_date_local 是源系统自带本地日期，与换算结果交叉校验；
--   3. 重复 settlement_id 按 ingested_at 保留最早一条。

WITH ranked AS (
    SELECT
        settlement_id,
        po_id,
        settled_cents,
        currency,
        settled_at_utc,
        biz_date_local,
        ingested_at,
        row_number() OVER (PARTITION BY settlement_id ORDER BY ingested_at ASC) AS dup_rn
    FROM ods_settlement
)
SELECT
    settlement_id,
    po_id,
    settled_cents,
    currency,
    settled_at_utc,
    substr(
        CAST(CAST(settled_at_utc AS TIMESTAMP) + INTERVAL '{{SRC_TZ_OFFSET_HOURS}} HOUR' AS VARCHAR),
        1,
        10
    ) AS settle_date,
    biz_date_local,
    ingested_at
FROM ranked
WHERE dup_rn = 1
