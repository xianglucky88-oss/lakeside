-- DWD：供应商 SCD2 隔离区
--
-- 只收"无法判定先后/无法构成时间线"的硬问题，每条保留原始内容与原因，
-- 供上游更正后下一次重建自动纳入。软处理（区间重叠裁剪、无变化折叠）不进这里。

WITH typed AS (
    SELECT
        supplier_id,
        supplier_name,
        region,
        credit_level,
        TRY_CAST(valid_from AS TIMESTAMP) AS vf,
        TRY_CAST(valid_to   AS TIMESTAMP) AS vt,
        COALESCE(TRY_CAST(updated_at AS TIMESTAMP), TIMESTAMP '1970-01-01 00:00:00')
            AS updated_at_ts,
        updated_at,
        record_hash,
        ingested_at
    FROM dwd_supplier_changelog
),
hard_invalid AS (
    SELECT *,
        CASE
            WHEN vf IS NULL THEN 'invalid_valid_from'
            WHEN vt IS NOT NULL AND vt <= vf THEN 'invalid_interval'
        END AS reject_reason
    FROM typed
    WHERE vf IS NULL OR (vt IS NOT NULL AND vt <= vf)
),
candidates AS (
    SELECT * FROM typed
    WHERE vf IS NOT NULL AND (vt IS NULL OR vt > vf)
),
ambiguous AS (
    SELECT
        c.*,
        'ambiguous_ordering' AS reject_reason
    FROM candidates c
    WHERE EXISTS (
        SELECT 1
        FROM candidates k
        WHERE k.supplier_id = c.supplier_id
          AND k.vf = c.vf
          AND k.updated_at_ts = c.updated_at_ts
          AND k.record_hash <> c.record_hash
    )
)
SELECT
    supplier_id, supplier_name, region, credit_level,
    CAST(vf AS VARCHAR) AS valid_from,
    CAST(vt AS VARCHAR) AS valid_to,
    updated_at, record_hash, ingested_at,
    reject_reason,
    CAST('{rejected_at}' AS TIMESTAMP) AS rejected_at
FROM hard_invalid
UNION ALL
SELECT
    supplier_id, supplier_name, region, credit_level,
    CAST(vf AS VARCHAR), CAST(vt AS VARCHAR),
    updated_at, record_hash, ingested_at,
    reject_reason,
    CAST('{rejected_at}' AS TIMESTAMP) AS rejected_at
FROM ambiguous
ORDER BY supplier_id, reject_reason, record_hash
