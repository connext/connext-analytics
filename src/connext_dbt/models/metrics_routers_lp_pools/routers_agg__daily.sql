-- Metrics: TVL, APR, APY
WITH raw AS (
SELECT 
    balance,
    fees_earned, 
    locked, 
    removed, 
    snapshot_time,
    COALESCE(LAG(fees_earned) OVER (ORDER BY snapshot_time), fees_earned) prev_fee,
    fees_earned - COALESCE(LAG(fees_earned) OVER (ORDER BY snapshot_time), fees_earned) AS running_diff
    
FROM (
    SELECT DISTINCT         
        balance AS balance,
        fees_earned AS fees_earned,
        locked AS locked,  
        removed AS removed,
        MAX(snapshot_time) AS snapshot_time

    FROM `mainnet-bigq.y42_connext_y42_dev.routers_assets_balance_hist`
    WHERE asset_canonical_id = "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
      AND asset_domain = "6648936"
      AND router_address = "0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4"
    GROUP BY balance, fees_earned, locked, removed
) AS subquery
ORDER BY snapshot_time)


SELECT 
    FORMAT('%f',balance) AS balance,
    FORMAT('%f',fees_earned) AS fees_earned,
    FORMAT('%f',locked) AS locked,
    FORMAT('%f',removed) AS removed,
    snapshot_time AS snapshot_time,
    FORMAT('%f',running_diff + locked) AS lock_fee,
FROM raw
