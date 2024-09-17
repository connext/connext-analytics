--Metric 10: **Settlement_Rate**: Percentage of transactions settled within 6 hours
-- 9.1.6 hrs

WITH raw AS (
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    COUNT(i.id) AS total_invoices,
    COUNT(CASE
        WHEN i.hub_status = 'DISPATCHED' THEN i.id
    END) AS count_of_invoices_completed,
    -- invoices by market makers
    COUNT(CASE
        WHEN i.hub_status = 'DISPATCHED' AND CAST(i.origin_ttl AS INTEGER) > 0 
        THEN i.id
    END) AS count_of_invoices_by_mm,
    -- calculate the percentage of invoices netted within 6 hour by mm
    COUNT(CASE
        WHEN i.hub_status = 'DISPATCHED' AND CAST(i.origin_ttl AS INTEGER) > 0 
        AND i.hub_invoice_enqueued_timestamp - i.origin_timestamp <= 21600
        THEN i.id
    END) AS count_of_invoices_by_mm_netted_within_6h,
    -- calculate the percentage of invoices netted within 24 hour by mm
    COUNT(CASE
        WHEN i.hub_status = 'DISPATCHED' AND CAST(i.origin_ttl AS INTEGER) > 0 
        AND i.hub_invoice_enqueued_timestamp - i.origin_timestamp <= 86400
        THEN i.id
    END) AS count_of_invoices_by_mm_netted_within_24h

FROM public.invoices i
GROUP BY 1
)

SELECT 
    day,
    ROUND(count_of_invoices_by_mm_netted_within_6h * 100.0 / NULLIF(count_of_invoices_by_mm, 0), 2) AS settlement_rate_6h_percentage,
    ROUND(count_of_invoices_by_mm_netted_within_24h * 100.0 / NULLIF(count_of_invoices_by_mm, 0), 2) AS settlement_rate_24h_percentage
FROM raw;