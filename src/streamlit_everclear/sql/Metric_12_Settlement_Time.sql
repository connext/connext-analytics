
-- Metric 12: **Settlement_Time**: Average time taken to settle the intent
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    AVG(i.settlement_timestamp - i.origin_timestamp) / 3600 AS avg_settlement_time
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;