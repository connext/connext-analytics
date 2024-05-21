SELECT 
    -- create buckets like: using usd_amount col
        -- <10$
        -- 10-100$
        -- 100-1000$
        -- 1000-10000$
        -- >10000$
    DATE(xcall_date) AS `date`,
    CASE 
        WHEN usd_amount < 10 THEN '<10$'
        WHEN usd_amount BETWEEN 10 AND 100 THEN '10-100$'
        WHEN usd_amount BETWEEN 100 AND 1000 THEN '100-1000$'
        WHEN usd_amount BETWEEN 1000 AND 10000 THEN '1000-10000$'
        WHEN usd_amount > 10000 THEN '>10000$'
    END AS amount_bucket,
    COUNT(transfer_id) AS transfer_count,
    SUM(usd_amount) AS volume,

    -- check status col for CompletedFast and CompletedSlow and create bolume for each
    SUM(CASE 
        WHEN status = 'CompletedFast' THEN usd_amount
        ELSE 0
    END) AS fast_volume,
    
    SUM(CASE 
        WHEN status = 'CompletedSlow' THEN usd_amount
        ELSE 0
    END) AS slow_volume,
    
    COUNT(CASE 
        WHEN status = 'CompletedFast' THEN transfer_id
        ELSE NULL
    END) AS fast_transfer_count,
    
    COUNT(CASE 
        WHEN status = 'CompletedSlow' THEN transfer_id
        ELSE NULL
    END) AS slow_transfer_count


FROM `mainnet-bigq.y42_connext_y42_dev.transfers_mapped`
GROUP BY 1, 2
ORDER BY 1, 2
