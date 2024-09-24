-- Metric 15: **Number of intents**: Number of intents on daily basis
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_intents
FROM public.intents i
GROUP BY 1;