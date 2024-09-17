-- Metric 2: Invoices_1h_Retention_Rate- Percentage of invoices that remain in the system for ~1h
WITH tis AS (
SELECT
    AVG(i.settlement_timestamp - i.origin_timestamp) as time_in_system,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
    END) AS count_of_invoices_within_1h,
    COUNT(id) AS total_count_of_invoices
FROM public.intents i
WHERE DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  >= DATE('{{ from_date }}') AND DATE_TRUNC('day', to_timestamp(i.origin_timestamp))  <= DATE('{{ to_date }}')
)

SELECT
    (time_in_system) / 3600 AS avg_time_in_system_hours,
    total_count_of_invoices,
    count_of_invoices_within_1h,
    100 * count_of_invoices_within_1h / total_count_of_invoices AS retention_rate
FROM tis
