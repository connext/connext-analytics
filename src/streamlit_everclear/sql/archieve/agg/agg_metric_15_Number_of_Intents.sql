-- Metric 15: **Number of intents**: Number of intents on daily basis
SELECT 
    COUNT(i.id) AS total_intents
FROM public.intents i
WHERE DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')