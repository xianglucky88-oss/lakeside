-- DWD：供应商变更日志摄入（append-only + 内容寻址去重）
--
-- 源（ods_supplier）按批次整体覆盖，可能重放整批数据；
-- 日志只追加"没见过的内容"，去重键是 record_hash：
-- 同一供应商、同属性、同生效区间、同 updated_at 的重复推送只存一份。
-- ingested_at 取批次逻辑日期，保证回放时确定。

WITH src AS (
    SELECT DISTINCT
        supplier_id,
        supplier_name,
        region,
        credit_level,
        CAST(valid_from AS TIMESTAMP) AS valid_from,
        CAST(valid_to   AS TIMESTAMP) AS valid_to,
        updated_at
    FROM ods_supplier
),
src_hash AS (
    SELECT
        *,
        md5(
            concat_ws(
                '|',
                supplier_id,
                supplier_name,
                region,
                credit_level,
                COALESCE(CAST(valid_from AS VARCHAR), ''),
                COALESCE(CAST(valid_to AS VARCHAR), ''),
                COALESCE(updated_at, '')
            )
        ) AS record_hash
    FROM src
)
SELECT
    supplier_id,
    supplier_name,
    region,
    credit_level,
    valid_from,
    valid_to,
    updated_at,
    record_hash,
    CAST('{ingested_at}' AS TIMESTAMP)                     AS ingested_at
FROM src_hash s
WHERE NOT EXISTS (
    SELECT 1 FROM dwd_supplier_changelog c
    WHERE c.record_hash = s.record_hash
)
