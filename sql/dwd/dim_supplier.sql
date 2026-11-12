-- DWD：供应商维表
--
-- 每个 supplier_id 一行，取 updated_at 最新的一条。

SELECT
    supplier_id,
    supplier_name,
    region,
    credit_level,
    updated_at AS dwd_updated_at
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY supplier_id ORDER BY updated_at DESC) AS rn
    FROM ods_supplier
) t
WHERE rn = 1
