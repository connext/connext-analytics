WITH raw_6hr_all AS (
SELECT 
    DATE_TRUNC('hour', to_timestamp(i.origin_timestamp))
    - (EXTRACT(hour FROM to_timestamp(i.origin_timestamp))::int % 6) * INTERVAL '1 hour' AS six_hour_interval,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS volume,
    COUNT(i.id) as total_intents,
    COUNT(CASE
        WHEN (
            CAST(i.hub_settlement_enqueued_timestamp AS FLOAT) - i.origin_timestamp) < 21600
        THEN i.id
        ELSE NULL
    END) as settled_in_6_hrs
FROM public.intents i
WHERE i.status = 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
GROUP BY 1, 2, 3, 4, 5
)
, final_6hr_all AS (
SELECT
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    AVG(settled_in_6_hrs) AS avg_intents_settled_in_6_hrs,
    AVG(total_intents) AS avg_intents_in_6_hrs,
    -- settlement rate
    ROUND(AVG(settled_in_6_hrs * 100.0 / NULLIF(total_intents, 0)), 2) AS daily_avg_settlement_rate_6h
FROM raw_6hr_all
GROUP BY 1, 2, 3, 4
)

-- 24 hr all
, raw_24hr_all AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS volume,
    COUNT(i.id) as total_intents,
    COUNT(CASE
        WHEN (
            CAST(i.hub_settlement_enqueued_timestamp AS FLOAT) - i.origin_timestamp) < 86400
        THEN i.id
        ELSE NULL
    END) as settled_in_24_hrs
FROM public.intents i
WHERE i.status = 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
GROUP BY 1, 2, 3, 4, 5
)
, final_24hr_all AS (
SELECT 
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    AVG(settled_in_24_hrs) AS avg_intents_settled_in_24_hrs,
    AVG(total_intents) AS avg_intents_in_24_hrs,
    -- settlement rate
    ROUND(AVG(settled_in_24_hrs * 100.0 / NULLIF(total_intents, 0)), 2) AS daily_avg_settlement_rate_24h
FROM raw_24hr_all
GROUP BY 1, 2, 3, 4
)
, final_all AS (
SELECT 
    COALESCE(final_6hr_all.from_chain_id, final_24hr_all.from_chain_id) AS from_chain_id,
    COALESCE(final_6hr_all.from_asset_address, final_24hr_all.from_asset_address) AS from_asset_address,
    COALESCE(final_6hr_all.to_chain_id, final_24hr_all.to_chain_id) AS to_chain_id,
    COALESCE(final_6hr_all.to_asset_address, final_24hr_all.to_asset_address) AS to_asset_address,
    final_6hr_all.avg_intents_settled_in_6_hrs,
    final_24hr_all.avg_intents_settled_in_24_hrs,
    final_6hr_all.avg_intents_in_6_hrs,
    final_24hr_all.avg_intents_in_24_hrs,
    final_6hr_all.daily_avg_settlement_rate_6h,
    final_24hr_all.daily_avg_settlement_rate_24h
FROM final_6hr_all
FULL OUTER JOIN final_24hr_all ON 
    final_6hr_all.from_chain_id = final_24hr_all.from_chain_id
    AND final_6hr_all.from_asset_address = final_24hr_all.from_asset_address
    AND final_6hr_all.to_chain_id = final_24hr_all.to_chain_id
    AND final_6hr_all.to_asset_address = final_24hr_all.to_asset_address
)

-- Market Maker 1hr
, raw_1hr_mm AS (
SELECT
    DATE_TRUNC('hour', to_timestamp(i.origin_timestamp))
    - (EXTRACT(hour FROM to_timestamp(i.origin_timestamp))::int % 1) * INTERVAL '1 hour' AS one_hour_interval,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    -- 1hr -> when invoice enters the queue and till hub processes the invoice
    COUNT(CASE
        WHEN ((i.hub_settlement_enqueued_timestamp::float - inv.hub_invoice_enqueued_timestamp::float)) < 3600
        THEN inv.id
        ELSE NULL
    END) as settled_in_1_hr,
    COUNT(i.id) as total_intents
FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status IN ('DISPATCHED', 'SETTLED')
AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
GROUP BY 1,2,3,4,5
)
, final_1hr_mm AS (
SELECT
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    AVG(settled_in_1_hr) AS avg_intents_settled_in_1_hr,
    AVG(total_intents) AS avg_intents_in_1_hr,
    -- settlement rate
    ROUND(AVG(settled_in_1_hr * 100.0 / NULLIF(total_intents, 0)), 2) AS mm_daily_avg_settlement_rate_1h_percentage
FROM raw_1hr_mm
GROUP BY 1, 2, 3, 4
)

-- Market Maker 3hr
, raw_3hr_mm AS (
SELECT
    DATE_TRUNC('hour', to_timestamp(i.origin_timestamp))
    - (EXTRACT(hour FROM to_timestamp(i.origin_timestamp))::int % 1) * INTERVAL '1 hour' AS one_hour_interval,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    -- 3hr
    COUNT(CASE
        WHEN ((i.hub_settlement_enqueued_timestamp::float - inv.hub_invoice_enqueued_timestamp::float)) < 10800
        THEN inv.id
        ELSE NULL
    END) as settled_in_3_hr,
    COUNT(i.id) as total_intents
FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status IN ('DISPATCHED', 'SETTLED')
AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
GROUP BY 1,2,3,4,5
)
, final_3hr_mm AS (
SELECT
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    -- settlement rate
    AVG(settled_in_3_hr) AS avg_intents_settled_in_3_hr,
    AVG(total_intents) AS avg_intents_in_3_hr,
    ROUND(AVG(settled_in_3_hr * 100.0 / NULLIF(total_intents, 0)), 2) AS mm_daily_avg_settlement_rate_3h_percentage
FROM raw_3hr_mm
GROUP BY 1, 2, 3, 4
)

-- Market Maker 1hr and 3hr
, mm_final AS (
SELECT 
    COALESCE(final_1hr_mm.from_chain_id, final_3hr_mm.from_chain_id) AS from_chain_id,
    COALESCE(final_1hr_mm.from_asset_address, final_3hr_mm.from_asset_address) AS from_asset_address,
    COALESCE(final_1hr_mm.to_chain_id, final_3hr_mm.to_chain_id) AS to_chain_id,
    COALESCE(final_1hr_mm.to_asset_address, final_3hr_mm.to_asset_address) AS to_asset_address,
    final_1hr_mm.avg_intents_settled_in_1_hr AS mm_avg_intents_settled_in_1_hr,
    final_3hr_mm.avg_intents_settled_in_3_hr AS mm_avg_intents_settled_in_3_hr,
    final_1hr_mm.avg_intents_in_1_hr AS mm_avg_intents_in_1_hr,
    final_3hr_mm.avg_intents_in_3_hr AS mm_avg_intents_in_3_hr,
    final_1hr_mm.mm_daily_avg_settlement_rate_1h_percentage,
    final_3hr_mm.mm_daily_avg_settlement_rate_3h_percentage
FROM final_1hr_mm
FULL OUTER JOIN final_3hr_mm ON
    final_1hr_mm.from_chain_id = final_3hr_mm.from_chain_id
    AND final_1hr_mm.from_asset_address = final_3hr_mm.from_asset_address
    AND final_1hr_mm.to_chain_id = final_3hr_mm.to_chain_id
    AND final_1hr_mm.to_asset_address = final_3hr_mm.to_asset_address
)

SELECT 
    COALESCE(final_all.from_chain_id, mm_final.from_chain_id) AS from_chain_id,
    COALESCE(final_all.from_asset_address, mm_final.from_asset_address) AS from_asset_address,
    COALESCE(final_all.to_chain_id, mm_final.to_chain_id) AS to_chain_id,
    COALESCE(final_all.to_asset_address, mm_final.to_asset_address) AS to_asset_address,
    -- settled
    final_all.avg_intents_settled_in_6_hrs,
    final_all.avg_intents_settled_in_24_hrs,
    mm_final.mm_avg_intents_settled_in_1_hr,
    mm_final.mm_avg_intents_settled_in_3_hr,
    
    -- avg total
    final_all.avg_intents_in_6_hrs,
    final_all.avg_intents_in_24_hrs,
    mm_final.mm_avg_intents_in_1_hr,
    mm_final.mm_avg_intents_in_3_hr,

    -- rate
    final_all.daily_avg_settlement_rate_6h,
    final_all.daily_avg_settlement_rate_24h,
    mm_final.mm_daily_avg_settlement_rate_1h_percentage,
    mm_final.mm_daily_avg_settlement_rate_3h_percentage
FROM final_all
FULL OUTER JOIN mm_final 
    ON 
    final_all.from_chain_id = mm_final.from_chain_id
    AND final_all.from_asset_address = mm_final.from_asset_address
    AND final_all.to_chain_id = mm_final.to_chain_id
    AND final_all.to_asset_address = mm_final.to_asset_address