-- Metrics: TVL, APR, APY
WITH chains_meta AS (
    SELECT DISTINCT
        domainid,
        chain_name AS chain
    FROM `mainnet-bigq.raw.stg__ninja_connext_prod_chains_tokens_clean` ct
),

tokens_meta AS (
    SELECT DISTINCT
        token_address AS local,
        token_name AS asset
    FROM `mainnet-bigq.stage.connext_tokens` ct
-- WHERE token_address = "0xb368ae21081709d03c00d7dc51737d8abd9384e6"
),

routers_tvl AS (
    SELECT
        date,
        router,
        chain,
        asset,
        SUM(amount) AS amount
    FROM (
        -- Get the amount of tokens locked in the router based on flow
        SELECT
            DATE_TRUNC(TIMESTAMP_SECONDS(r.timestamp), DAY) AS date,
            r.router,
            cm.chain,
            COALESCE(tm.asset, r.asset) AS asset,
            SUM(CASE
                WHEN event = 'Add' THEN CAST(r.amount AS FLOAT64)
                ELSE -CAST(r.amount AS FLOAT64)
            END)
                OVER (
                    PARTITION BY r.asset, r.domain
                    ORDER BY r.timestamp
                ) AS amount
        FROM `mainnet-bigq.public.router_liquidity_events` r
        LEFT JOIN chains_meta cm ON r.domain = cm.domainid
        LEFT JOIN tokens_meta tm ON (r.asset = tm.local)
        ORDER BY 1 DESC
    )
    GROUP BY 1, 2, 3, 4
    ORDER BY 1 DESC
    -- WHERE router = "0x97b9dcb1aa34fe5f12b728d9166ae353d1e7f5c4"
    -- AND ct.asset_symbol='USDT'
    -- AND a.domain = "6648936"
)

SELECT * FROM routers_tvl
WHERE
    date < "2024-05-20"
    AND router = "0x6fd84ba95525c4ccd218f2f16f646a08b4b0a598"
    AND asset = 'WETH'
    AND chain = "Ethereum Mainnet"
ORDER BY 1, 2, 3 DESC




-- Add the snapshot data after cleaning
-- do the above amount as running total so as to keep evrything in sync
-- pull in the fees data from trnsactions and merge on all dates
-- 
