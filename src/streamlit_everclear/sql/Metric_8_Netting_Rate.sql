-- - **Metric 8: Netting_Rate**
--   - Category: OKRs
--   - Description: Percentage of transactions netted within 24 hours
--   - Target: KR2: 60% netted within 24h
--   - Property: by chains; by assets
-- SQL-> hub_intent_table -> settled / dispatched and no hub invoice for the itent id
-- Settled event is emmitted 
-- when hub intent is settled that message is sent to destination, settled on destination then received the money

SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS netted_count,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
    END) AS count_of_intents_within_1h,
-- Calculating the percentage of invoices netted within 24 hour
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS netting_rate_1h_percentage,
    -- Calculating the percentage of invoices netted within 24 hour
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS netting_rate_24h_percentage
FROM public.intents i
WHERE i.settlement_status = 'SETTLED' 
  AND i.origin_ttl = 0
GROUP BY 1;