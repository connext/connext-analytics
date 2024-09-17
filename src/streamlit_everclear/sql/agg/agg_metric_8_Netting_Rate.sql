-- - **Metric 8: Netting_Rate**
--   - Category: OKRs
--   - Description: Percentage of transactions netted within 24 hours
--   - Target: KR2: 60% netted within 24h
--   - Property: by chains; by assets
-- SQL-> hub_intent_table -> settled / dispatched and no hub invoice for the itent id
-- Settled event is emmitted 
-- when hub intent is settled that message is sent to destination, settled on destination then received the money

WITH raw AS (
SELECT
    COUNT(i.id) AS total_intents,
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp <= 3600)
        AND i.settlement_status = 'SETTLED' 
        AND CAST(i.origin_ttl AS INTEGER) = 0
        THEN i.id
    END) AS count_of_intents_within_1h,
    COUNT(CASE
        WHEN (i.settlement_timestamp - i.origin_timestamp <= 86400)
        AND i.settlement_status = 'SETTLED' 
        AND CAST(i.origin_ttl AS INTEGER) = 0
        THEN i.id
    END) AS count_of_intents_within_24h
FROM public.intents i
WHERE DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
)

SELECT
    -- # netting rate 1h
    ROUND(count_of_intents_within_1h * 100.0 / total_intents, 2) AS netting_rate_1h_percentage,
    -- # netting rate 24h
    ROUND(count_of_intents_within_24h * 100.0 / total_intents, 2) AS netting_rate_24h_percentage
FROM raw;