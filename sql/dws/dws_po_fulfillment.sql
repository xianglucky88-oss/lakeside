-- DWS：采购单履约时效宽表（快照）
--
-- 粒度：每个"当前版本为 approved"的采购单一行；cancelled 单据不进入本表。
-- 快照：as_of_date = {{LOGICAL_DATE}}。重跑同一逻辑日期结果幂等；
--       上游补传后用新的 as_of_date 重算，历史分区保留，结论可追溯修正。
--
-- 防扇出：入库、结算、隔离事件先各自按 po_id 预聚合，再与采购单一对一连接，
--         采购金额永远只取采购单自身一行，不会被入库笔数 × 结算笔数放大。
--
-- 状态机（互斥）：
--   CLOSED_ON_TIME   完成度 >= 99% 且末次到货不晚于约定到货日；
--   CLOSED_LATE      完成度 >= 99% 但末次到货晚于约定到货日；
--   IN_TRANSIT       未完成且约定到货日未到（在途，不得判逾期）；
--   OVERDUE          未完成且已过约定到货日；
--   NO_RECEIPT       截至快照时点没有任何有效入库（缺入库，独立于数据缺失）。
--
-- 风险码 risk_codes（可多选，逗号分隔；与"未结算/未入库"业务状态分开）：
--   IN_TRANSIT_OVERDUE     已审批未到货且逾期；
--   SPLIT_DELAY            分批收货（>=2 批有效入库）且整体逾期；
--   OVER_RECEIVED          有效入库金额超过采购金额 1%（超交）；
--   SETTLE_EXCEEDS_RECEIPT 同币种有效结算金额超过有效入库金额 1%；
--   CURRENCY_MISMATCH      存在币种不一致被拒绝的结算；
--   MISSING_EXPECTED_DATE  缺约定到货日，无法做逾期判定（数据缺失，不臆测）。

WITH po AS (
    SELECT *
    FROM dwd_fact_purchase_order
    WHERE status = 'approved'
),
gr_all AS (
    SELECT
        gr.po_id,
        count_if(gr.is_voided = 0)                                  AS gr_batches_valid,
        count_if(gr.is_voided = 1)                                  AS gr_batches_voided,
        sum(CASE WHEN gr.is_voided = 0 THEN gr.received_cents ELSE 0 END) AS received_cents,
        min(CASE WHEN gr.is_voided = 0 THEN gr.received_at END)     AS first_received_at,
        max(CASE WHEN gr.is_voided = 0 THEN gr.received_at END)     AS last_received_at,
        count(DISTINCT CASE WHEN gr.is_voided = 0 THEN gr.warehouse END) AS gr_warehouse_count,
        count_if(
            gr.is_voided = 0
            AND CAST(gr.ingested_at AS TIMESTAMP)
                > CAST(gr.received_at AS TIMESTAMP) + INTERVAL 3 DAY
        )                                                            AS late_gr_batches,
        max(gr.ingested_at)                                          AS gr_watermark
    FROM dwd_fact_goods_receipt gr
    JOIN po ON gr.po_id = po.po_id
    GROUP BY gr.po_id
),
wh_ranked AS (
    SELECT
        gr.po_id,
        gr.warehouse,
        row_number() OVER (
            PARTITION BY gr.po_id
            ORDER BY sum(gr.received_cents) DESC, min(gr.received_at) ASC, gr.warehouse ASC
        ) AS rn
    FROM dwd_fact_goods_receipt gr
    JOIN po ON gr.po_id = po.po_id
    WHERE gr.is_voided = 0
    GROUP BY gr.po_id, gr.warehouse
),
gr_wh AS (
    SELECT po_id, warehouse AS primary_warehouse FROM wh_ranked WHERE rn = 1
),
st_valid AS (
    SELECT
        st.po_id,
        count(*)                               AS settlements,
        sum(st.settled_cents)                  AS settled_cents,
        min(st.settled_at_utc)                 AS first_settled_at,
        max(st.settled_at_utc)                 AS last_settled_at,
        max(st.ingested_at)                    AS st_watermark
    FROM dwd_fact_settlement st
    JOIN po ON st.po_id = po.po_id AND st.currency = po.currency
    GROUP BY st.po_id
),
rej AS (
    SELECT
        po_id,
        count_if(reject_reason = 'CURRENCY_MISMATCH') AS ccy_reject_cnt,
        count_if(reject_reason = 'ORPHAN_EVENT')      AS orphan_cnt,
        count_if(reject_reason = 'PO_NOT_APPROVED')   AS not_approved_cnt,
        count_if(reject_reason = 'VOIDED_RECEIPT')    AS voided_cnt,
        max(ingested_at)                               AS rej_watermark
    FROM dwd_reject_event
    GROUP BY po_id
)
SELECT
    po.po_id,
    po.supplier_id,
    ds.supplier_name,
    ds.region,
    gr_wh.primary_warehouse                                  AS warehouse,
    po.biz_date,
    substr(po.expected_at, 1, 10)                            AS expected_date,
    po.approved_at,
    po.first_approved_at,
    CAST('{{LOGICAL_DATE}}' AS DATE)                         AS as_of_date,
    po.amount_cents                                          AS po_amount_cents,
    po.currency,
    po.version                                               AS po_version,
    CASE WHEN po.version > 1 THEN 1 ELSE 0 END               AS is_reapproved,
    CAST(coalesce(g.gr_batches_valid, 0) AS BIGINT)          AS gr_batches_valid,
    CAST(coalesce(g.gr_batches_voided, 0) AS BIGINT)         AS gr_batches_voided,
    CAST(coalesce(g.gr_warehouse_count, 0) AS BIGINT)        AS gr_warehouse_count,
    CAST(coalesce(g.received_cents, 0) AS BIGINT)            AS received_cents,
    g.first_received_at,
    g.last_received_at,
    CAST(coalesce(g.late_gr_batches, 0) AS BIGINT)           AS late_gr_batches,
    CAST(coalesce(s.settlements, 0) AS BIGINT)               AS settlements,
    CAST(coalesce(s.settled_cents, 0) AS BIGINT)            AS settled_cents,
    s.first_settled_at,
    s.last_settled_at,
    CAST(coalesce(r.ccy_reject_cnt, 0) AS BIGINT)            AS ccy_reject_cnt,
    CAST(coalesce(r.orphan_cnt, 0) AS BIGINT)               AS orphan_event_cnt,
    CAST(coalesce(r.voided_cnt, 0) AS BIGINT)               AS voided_receipt_cnt,
    CASE WHEN coalesce(s.settlements, 0) = 0 THEN 1 ELSE 0 END AS unsettled_flag,
    CASE WHEN coalesce(g.gr_batches_valid, 0) = 0 THEN 1 ELSE 0 END AS no_receipt_flag,
    round(
        CASE WHEN po.amount_cents > 0
             THEN coalesce(g.received_cents, 0) * 1.0 / po.amount_cents
             ELSE NULL END,
        4
    )                                                        AS completion_rate,
    -- 在途时长：未完成订单 = 快照日 - 最新批准日；已完成则冻结为履约时长
    CASE
        WHEN coalesce(g.received_cents, 0) * 1.0 < po.amount_cents * 0.99
        THEN date_diff('day', CAST(po.approved_at AS TIMESTAMP), CAST('{{LOGICAL_DATE}}' AS TIMESTAMP))
    END                                                      AS in_transit_days,
    -- 履约时长：末次有效到货 - 最新批准日，仅完成时有值
    CASE
        WHEN coalesce(g.received_cents, 0) * 1.0 >= po.amount_cents * 0.99
        THEN date_diff('day', CAST(po.approved_at AS TIMESTAMP), CAST(g.last_received_at AS TIMESTAMP))
    END                                                      AS fulfillment_days,
    -- 到货偏差：末次到货相对约定到货日，正=延迟，负=提前，仅完成时有值
    CASE
        WHEN coalesce(g.received_cents, 0) * 1.0 >= po.amount_cents * 0.99 AND po.expected_at IS NOT NULL
        THEN date_diff('day', CAST(po.expected_at AS TIMESTAMP), CAST(g.last_received_at AS TIMESTAMP))
    END                                                      AS expected_delay_days,
    -- 逾期天数：只有"未完成且约定到货日已过"才有值，在途订单保持 NULL
    CASE
        WHEN coalesce(g.received_cents, 0) * 1.0 < po.amount_cents * 0.99
             AND po.expected_at IS NOT NULL
             AND CAST(po.expected_at AS TIMESTAMP) < CAST('{{LOGICAL_DATE}}' AS TIMESTAMP)
        THEN date_diff('day', CAST(po.expected_at AS TIMESTAMP), CAST('{{LOGICAL_DATE}}' AS TIMESTAMP))
    END                                                      AS overdue_days,
    -- 结算-入库差额：正=结算超过实际入库
    CAST(coalesce(s.settled_cents, 0) - coalesce(g.received_cents, 0) AS BIGINT)
        AS settle_vs_receipt_cents,
    -- 状态机
    CASE
        WHEN po.expected_at IS NULL THEN 'NO_RECEIPT'
        WHEN coalesce(g.gr_batches_valid, 0) = 0 THEN 'NO_RECEIPT'
        WHEN coalesce(g.received_cents, 0) * 1.0 >= po.amount_cents * 0.99
             AND CAST(g.last_received_at AS TIMESTAMP) <= CAST(po.expected_at AS TIMESTAMP)
            THEN 'CLOSED_ON_TIME'
        WHEN coalesce(g.received_cents, 0) * 1.0 >= po.amount_cents * 0.99
            THEN 'CLOSED_LATE'
        WHEN CAST(po.expected_at AS TIMESTAMP) >= CAST('{{LOGICAL_DATE}}' AS TIMESTAMP)
            THEN 'IN_TRANSIT'
        ELSE 'OVERDUE'
    END                                                      AS fulfillment_status,
    -- 风险码（可多选）
    trim(both ',' FROM concat(
        CASE
            WHEN coalesce(g.received_cents, 0) * 1.0 < po.amount_cents * 0.99
                 AND po.expected_at IS NOT NULL
                 AND CAST(po.expected_at AS TIMESTAMP) < CAST('{{LOGICAL_DATE}}' AS TIMESTAMP)
            THEN 'IN_TRANSIT_OVERDUE,' ELSE ''
        END,
        CASE
            WHEN coalesce(g.gr_batches_valid, 0) >= 2
                 AND coalesce(g.received_cents, 0) * 1.0 < po.amount_cents * 0.99
                 AND po.expected_at IS NOT NULL
                 AND CAST(po.expected_at AS TIMESTAMP) < CAST('{{LOGICAL_DATE}}' AS TIMESTAMP)
            THEN 'SPLIT_DELAY,' ELSE ''
        END,
        CASE WHEN coalesce(g.received_cents, 0) > po.amount_cents * 1.01
             THEN 'OVER_RECEIVED,' ELSE '' END,
        CASE WHEN coalesce(s.settled_cents, 0) > coalesce(g.received_cents, 0) * 1.01
             THEN 'SETTLE_EXCEEDS_RECEIPT,' ELSE '' END,
        CASE WHEN coalesce(r.ccy_reject_cnt, 0) > 0 THEN 'CURRENCY_MISMATCH,' ELSE '' END,
        CASE WHEN po.expected_at IS NULL THEN 'MISSING_EXPECTED_DATE,' ELSE '' END
    ))                                                       AS risk_codes,
    -- 数据问题码：只描述数据质量，不与未结算/未入库业务状态混用
    trim(both ',' FROM concat(
        CASE WHEN po.expected_at IS NULL THEN 'MISSING_EXPECTED_DATE,' ELSE '' END,
        CASE WHEN coalesce(r.ccy_reject_cnt, 0) > 0 THEN 'CURRENCY_MISMATCH,' ELSE '' END,
        CASE WHEN coalesce(r.orphan_cnt, 0) > 0 THEN 'ORPHAN_EVENT,' ELSE '' END,
        CASE WHEN coalesce(r.voided_cnt, 0) > 0 THEN 'VOIDED_RECEIPT,' ELSE '' END
    ))                                                       AS data_issue_codes,
    -- 风险结论的业务事实依据（可人工复核的一行证据）
    'po_amount=' || po.amount_cents || po.currency
        || ';approved_at=' || po.approved_at
        || ';expected_at=' || coalesce(po.expected_at, 'NULL')
        || ';valid_received=' || coalesce(g.received_cents, 0)
        || ';valid_batches=' || coalesce(g.gr_batches_valid, 0)
        || ';voided_batches=' || coalesce(g.gr_batches_voided, 0)
        || ';late_ingested_batches=' || coalesce(g.late_gr_batches, 0)
        || ';valid_settled=' || coalesce(s.settled_cents, 0)
        || ';ccy_rejected=' || coalesce(r.ccy_reject_cnt, 0)
        || ';as_of=' || '{{LOGICAL_DATE}}'                  AS risk_evidence,
    -- 源数据水位：本单参与计算的事件中最晚进湖时间，证明结论基于哪些补传
    greatest(
        coalesce(CAST(po.ingested_at AS TIMESTAMP), CAST('1970-01-01' AS TIMESTAMP)),
        coalesce(CAST(g.gr_watermark AS TIMESTAMP), CAST('1970-01-01' AS TIMESTAMP)),
        coalesce(CAST(s.st_watermark AS TIMESTAMP), CAST('1970-01-01' AS TIMESTAMP)),
        coalesce(CAST(r.rej_watermark AS TIMESTAMP), CAST('1970-01-01' AS TIMESTAMP))
    )                                                        AS source_watermark,
    CAST('{{LOGICAL_DATE}} 23:59:59' AS TIMESTAMP)           AS calculated_at
FROM po
LEFT JOIN dwd_dim_supplier ds ON po.supplier_id = ds.supplier_id
LEFT JOIN gr_all g ON po.po_id = g.po_id
LEFT JOIN gr_wh ON po.po_id = gr_wh.po_id
LEFT JOIN st_valid s ON po.po_id = s.po_id
LEFT JOIN rej r ON po.po_id = r.po_id
