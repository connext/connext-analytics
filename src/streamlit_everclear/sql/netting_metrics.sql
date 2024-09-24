-- 3. Netting_Volume
-- 11. Netting_Time
SELECT
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) as day,
    CAST(i.origin_origin AS INTEGER) AS from_chain_id,
    i.origin_input_asset AS from_asset_address,
    CAST(i.settlement_domain AS INTEGER) AS to_chain_id,
    i.settlement_asset AS to_asset_address,
    SUM(i.origin_amount::float) AS netting_volume,
    AVG(
        (i.settlement_timestamp::FLOAT- i.origin_timestamp::FLOAT) / 3600
    ) AS avg_netting_time_in_hrs

FROM public.intents i
LEFT JOIN public.invoices inv 
    ON i.id = inv.id
WHERE inv.id IS NULL
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED'
GROUP BY 1,2,3,4,5
