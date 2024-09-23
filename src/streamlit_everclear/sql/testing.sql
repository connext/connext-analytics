SELECT
    -- # added netted flag: inv.id IS NULL
    -- CASE WHEN inv.id IS NULL THEN 'netted' ELSE 'settled' END AS netted_flag,
    -- i.status,
    -- i.hub_status,
    -- to_timestamp(i.origin_timestamp) as origin_timestamp,
    -- to_timestamp(i.hub_added_timestamp) as hub_added_timestamp,
    -- to_timestamp(i.hub_settlement_enqueued_timestamp) as hub_enqueued_timestamp,
    -- to_timestamp(i.settlement_timestamp) as settlement_timestamp,

    COUNT(i.id) AS intent_count,
    COUNT(DISTINCT i.id) AS distinct_intent_count,
    COUNT(inv.id) AS invoice_count,
    COUNT(DISTINCT inv.id) AS distinct_invoice_count


FROM public.intents i
LEFT JOIN public.invoices inv 
ON i.id = inv.id
WHERE i.hub_status != 'DISPATCHED_UNSUPPORTED'





-- -- look at invoices for this data
-- {
--   "day": "2024-09-18 00:00:00+00",
--   "from_chain_id": 56,
--   "from_asset_address": "0x00000000000000000000000055d398326f99059ff775485246999027b3197955",
--   "to_chain_id": 42161,
--   "to_asset_address": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
--   "volume_settled_by_mm": 1000000000000000000,
--   "total_invoices_by_mm": "1",
--   "avg_settlement_time_in_hrs_by_mm": 68.48555555555555
-- }

WITH raw AS (
SELECT
*,

    (
        (i.settlement_timestamp::FLOAT- i.origin_timestamp::FLOAT) / 3600
    ) AS avg_time_in_hrs

FROM public.intents i
INNER JOIN public.invoices inv
ON i.id = inv.id
AND i.status= 'SETTLED_AND_COMPLETED' AND i.hub_status != 'DISPATCHED_UNSUPPORTED')

SELECT * FROM raw
ORDER BY avg_time_in_hrs DESC