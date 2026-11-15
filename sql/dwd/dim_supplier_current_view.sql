-- DWD：供应商维表兼容视图（SCD1 读法）
--
-- 迁移期保留旧表名 dwd_dim_supplier 的逻辑视图，列结构与旧 SCD1 表完全一致，
-- 每个 supplier_id 一行，取 SCD2 表的当前版本。老 SQL 无需改动即可继续读取，
-- 结果明确等于"最新属性"；需要历史口径的新作业直接读 dwd_dim_supplier_scd2。

SELECT
    supplier_id,
    supplier_name,
    region,
    credit_level,
    updated_at AS dwd_updated_at
FROM dwd_dim_supplier_scd2
WHERE is_current
