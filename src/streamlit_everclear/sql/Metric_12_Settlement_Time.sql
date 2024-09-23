
-- Metric 12: **Settlement_Time**: Average time taken to settle the intent
SELECT 
    DATE_TRUNC('day', to_timestamp(i.origin_timestamp)) AS day,
    -- 3 values: all, by mm and by netting

    -- overall
    AVG(i.settlement_timestamp - i.origin_timestamp) / 3600 AS overall_avg_settlement_time,
    -- by mm
    AVG(CASE WHEN i.origin_ttl > 0 THEN (i.settlement_timestamp - i.origin_timestamp) / 3600 ELSE NULL END) AS mm_avg_settlement_time,
    -- by netting
    AVG(CASE WHEN i.origin_ttl = 0 THEN (i.settlement_timestamp - i.origin_timestamp) / 3600 ELSE NULL END) AS netting_avg_settlement_time    

FROM public.intents i
WHERE i.settlement_status = 'SETTLED'
GROUP BY 1



-- filter MM by origin intitiators