-- 1. Settlement Rate 24h
-- Definition: Percentage of intents that were settled within 24 hours, the start time
-- given there are two settrment strategy: direct settlement on intent queue and hub intent based clearing
SELECT
    COUNT(i.id) as total_intent_count,
    COUNT(CASE WHEN i.settlement_status = 'SETTLED' THEN 1 END) as settled_count,
    100 * COUNT(CASE WHEN i.settlement_status = 'SETTLED' THEN 1 END) / NULLIF(COUNT(i.id), 0) as prct_of_settled_count
FROM public.intents i
WHERE DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
