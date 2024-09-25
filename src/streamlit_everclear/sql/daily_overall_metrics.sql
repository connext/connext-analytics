--1. Clearing_Volume
-- 6. Total_rebalaicing fee
-- 9. Avg. intent size
-- 12. Total_Protocol_Revenue: Fee for the protocol
-- 13. Total_Rebalaicing_Fee: Rebalaicing fee for the protocol
WITH raw AS (
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    0.0001 AS fee_value,
    i.origin_amount::FLOAT AS origin_amount,
    CASE 
        WHEN inv.id IS NOT NULL 
        THEN (CAST(inv.hub_invoice_amount AS FLOAT) - CAST(i.settlement_amount AS FLOAT)) 
        ELSE 0 
    END AS discounts_by_mm
FROM public.intents i
LEFT JOIN public.invoices inv ON i.id = inv.id
WHERE i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
)

SELECT
    day,
    from_chain_id,
    from_asset_address,
    to_chain_id,
    to_asset_address,
    SUM(origin_amount::float) AS clearing_volume,
    SUM(fee_value * origin_amount) AS protocol_revenue,
    (SUM(discounts_by_mm) + SUM(fee_value * origin_amount)) AS rebalancing_fee,
    AVG(origin_amount::float) AS avg_intent_size
FROM raw
GROUP BY 1,2,3,4,5