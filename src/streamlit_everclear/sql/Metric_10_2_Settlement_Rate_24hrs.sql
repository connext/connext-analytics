
-- 9.2. 24 hrs
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_intent_count,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) AS count_of_intents_within_24h,
    -- Calculating the percentage of invoices netted within 24 hour
    ROUND(COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 86400 THEN i.id
    END) * 100.0 / COUNT(i.id), 3) AS settlement_rate_percentage
FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1;