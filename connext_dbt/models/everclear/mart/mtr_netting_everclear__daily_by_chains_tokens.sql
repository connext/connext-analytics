WITH netted AS (
SELECT
    i.updated_at,
    DATE_TRUNC(i.origin_timestamp, DAY) AS day,
    i.from_chain_id,
    i.from_chain_name,
    i.from_asset_address,
    i.from_asset_symbol,
    i.to_chain_id,
    i.to_chain_name,
    i.to_asset_address,
    i.to_asset_symbol,
    AVG(i.from_asset_price) AS avg_from_asset_price,
    SUM(i.from_asset_amount) AS netting_volume,
    SUM(i.from_asset_amount_usd) AS netting_volume_usd,
    -- higher accuracy measure used to calculate avg time in hrs
    AVG(
        CAST(TIMESTAMP_DIFF(i.settlement_timestamp, i.origin_timestamp, SECOND) AS FLOAT64) / 3600
    ) AS netting_avg_time_in_hrs,
    SUM(i.fee_value * i.from_asset_amount_usd) AS netting_protocol_revenue_usd,
    COUNT(i.id) AS netting_total_intents,
    AVG(i.from_asset_amount_usd) AS netting_avg_intent_size_usd


FROM {{ref('cln_intents')}} i
LEFT JOIN {{ref('cln_invoices')}} inv
    ON i.id = inv.id
WHERE inv.id IS NULL
    AND i.status = 'SETTLED_AND_COMPLETED'
    AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM netted