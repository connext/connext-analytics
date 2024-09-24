-- Metric 2: Invoices_1h_Retention_Rate- Percentage of invoices that remain in the system for ~1h
WITH tis AS (
SELECT
    -- calculate time in system for each invoice origin_timestamp till settlement_timestamp
    DATE_TRUNC('hour', to_timestamp(i.settlement_timestamp)) as settlement_hour,
    AVG(i.settlement_timestamp - i.origin_timestamp) as time_in_system,
    COUNT(CASE
        WHEN i.settlement_timestamp - i.origin_timestamp <= 3600 THEN i.id
    END) AS count_of_invoices_within_1h,
    COUNT(id) AS total_count_of_invoices
FROM public.intents i
GROUP BY 1
)
SELECT 
    DATE_TRUNC('day', settlement_hour) as day,
    AVG(time_in_system) / 3600 AS avg_time_in_system_hours,
    SUM(total_count_of_invoices) AS total_count_of_invoices,
    SUM(count_of_invoices_within_1h) AS count_of_invoices_within_1h,
    100 * SUM(count_of_invoices_within_1h) / SUM(total_count_of_invoices) AS retention_rate
FROM tis
GROUP BY 1;
