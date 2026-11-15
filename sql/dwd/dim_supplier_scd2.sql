-- DWD：供应商 SCD2 版本表重建（产出）
--
-- 输入：dwd_supplier_changelog（append-only 的变更事实）
-- 输出：每个 supplier_id 一串连续、互不重叠的半开区间 [valid_from, valid_to)
--
-- 处理链：
--   1. 硬校验：valid_from 缺失/不可解析、valid_to <= valid_from 的记录剔除；
--   2. 歧义检测：同 supplier_id + 同 valid_from + 同 updated_at 但内容不同，整组剔除
--      （没有任何信号能判定先后，拒绝猜测，见隔离区 SQL）；
--   3. 确定性排序：valid_from, updated_at, record_hash（平局键保证重放字节级一致）；
--   4. 相邻取界：valid_to = 下一版本 valid_from，源端 valid_to 仅作审计；
--   5. 无变化折叠：相邻且业务属性完全相同的版本合并，抑制版本表膨胀。
-- 整条链路是 changelog 的纯函数，乱序到达与历史回补在全量重建时自然归位。

WITH typed AS (
    SELECT
        supplier_id,
        supplier_name,
        region,
        credit_level,
        TRY_CAST(valid_from AS TIMESTAMP) AS valid_from,
        TRY_CAST(valid_to   AS TIMESTAMP) AS valid_to,
        -- updated_at 是源端 VARCHAR：转时间戳参与排序，无法解析时用最低值垫底
        COALESCE(TRY_CAST(updated_at AS TIMESTAMP), TIMESTAMP '1970-01-01 00:00:00')
            AS updated_at_ts,
        updated_at,
        record_hash,
        ingested_at
    FROM dwd_supplier_changelog
),
valid_rows AS (
    SELECT *
    FROM typed
    WHERE valid_from IS NOT NULL
      AND (valid_to IS NULL OR valid_to > valid_from)
),
ambiguous_keys AS (
    -- 同自然键、同生效时刻、同记录时刻，但属性（含源 valid_to）不一致
    SELECT supplier_id, valid_from, updated_at_ts
    FROM valid_rows
    GROUP BY supplier_id, valid_from, updated_at_ts
    HAVING count(DISTINCT record_hash) > 1
),
ordered AS (
    SELECT
        v.*,
        row_number() OVER (
            PARTITION BY v.supplier_id
            ORDER BY v.valid_from, v.updated_at_ts, v.record_hash
        ) AS ord
    FROM valid_rows v
    WHERE NOT EXISTS (
        SELECT 1 FROM ambiguous_keys k
        WHERE k.supplier_id = v.supplier_id
          AND k.valid_from = v.valid_from
          AND k.updated_at_ts = v.updated_at_ts
    )
),
point_dedup AS (
    -- 同一供应商、同一 valid_from 的多条可排序记录：只留 updated_at 最新的一条，
    -- 保证时间线上每个生效时刻只对应一个版本（同点冲突已在 ambiguous_keys 整组剔除）。
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY supplier_id, valid_from
                ORDER BY updated_at_ts DESC, record_hash
            ) AS point_rn
        FROM ordered
    )
    WHERE point_rn = 1
),
reindexed AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY supplier_id ORDER BY valid_from
        ) AS ord
    FROM point_dedup
),
with_bounds AS (
    SELECT
        reindexed.*,
        lead(valid_from) OVER (
            PARTITION BY supplier_id ORDER BY ord
        ) AS next_valid_from
    FROM reindexed
),
spanned AS (
    SELECT
        *,
        next_valid_from AS valid_to_calc,
        -- 源区间与重算边界不一致即标记（含重叠被裁剪、相邻被拼接）
        (
            source_valid_to IS NOT NULL
            AND next_valid_from IS NOT NULL
            AND source_valid_to <> next_valid_from
        ) AS interval_adjusted
    FROM (
        SELECT
            *,
            valid_to AS source_valid_to
        FROM with_bounds
    ) b
),
lagged AS (
    SELECT
        *,
        COALESCE(
            supplier_name IS DISTINCT FROM lag(supplier_name) OVER (PARTITION BY supplier_id ORDER BY ord)
            OR region        IS DISTINCT FROM lag(region)        OVER (PARTITION BY supplier_id ORDER BY ord)
            OR credit_level  IS DISTINCT FROM lag(credit_level)  OVER (PARTITION BY supplier_id ORDER BY ord),
            true
        ) AS payload_changes
    FROM spanned
),
islands AS (
    -- 相邻且业务属性相同 → 同一折叠组
    SELECT
        *,
        sum(CASE WHEN payload_changes THEN 1 ELSE 0 END) OVER (
            PARTITION BY supplier_id
            ORDER BY ord
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS grp
    FROM lagged
),
collapsed AS (
    SELECT
        supplier_id,
        min(ord) AS first_ord,
        min(valid_from) AS valid_from,
        -- 折叠组的右边界取组内"最后一个版本"的边界。
        -- 不能用 max()/arg_min()：两者都忽略 NULL，而末版本为当前版本时
        -- 其 valid_to_calc 恰为 NULL，会错误地丢掉当前状态、留下断档时间线。
        -- 显式规则：组内含 NULL 边界（覆盖到当前）则整组右边界为 NULL，
        -- 否则取最大 ord（最后版本）的边界。
        CASE
            WHEN bool_or(valid_to_calc IS NULL) THEN NULL
            ELSE arg_min(valid_to_calc, -ord)
        END AS valid_to,
        -- 折叠组取首个版本的属性（组内属性一致）
        arg_min(supplier_name, ord) AS supplier_name,
        arg_min(region, ord)       AS region,
        arg_min(credit_level, ord) AS credit_level,
        arg_min(source_valid_to, ord) AS source_valid_to,
        bool_or(interval_adjusted) AS interval_adjusted,
        arg_min(record_hash, ord)  AS record_hash,
        arg_min(updated_at, ord)   AS updated_at,
        min(ingested_at)           AS ingested_at,
        count(*) AS folded_versions
    FROM islands
    GROUP BY supplier_id, grp
)
SELECT
    md5(concat_ws('|', supplier_id, strftime(valid_from, '%Y-%m-%d %H:%M:%S'))) AS supplier_sk,
    supplier_id,
    supplier_name,
    region,
    credit_level,
    valid_from,
    valid_to,
    (valid_to IS NULL) AS is_current,
    row_number() OVER (PARTITION BY supplier_id ORDER BY valid_from) AS version_seq,
    source_valid_to,
    interval_adjusted,
    record_hash,
    updated_at,
    ingested_at
FROM collapsed
ORDER BY supplier_id, valid_from
