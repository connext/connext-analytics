WITH raw AS (
SELECT

    date,
    router,
    router_name,
    chain,
    asset_group,
    asset,
    price,
    router_dw,
    initial_locked,
    liquidity_locked,
    liquidity_fill_back,
    total_locked,
    total_fee_earned,

    -- running locked liquidity
    SUM(liquidity_locked) OVER (PARTITION BY router, chain, asset_group ORDER BY date) AS running_locked_liquidity,
    -- running liquidity fill back
    SUM(liquidity_fill_back) OVER (PARTITION BY router, chain, asset_group ORDER BY date) AS running_liquidity_fill_back

FROM `mainnet-bigq.y42_connext_y42_dev.router_tvl_liquidity_under_utilizations__hourly`
WHERE
-- asset_group = "WETH"
-- AND chain = "Mode Mainnet" 
-- AND DATE_TRUNC(date, DAY) = "2024-06-10"
router_name = "Dokia"
AND chain = "Arbitrum One"
AND asset_group = "ezETH")


SELECT
    *,
    initial_locked - running_locked_liquidity + running_liquidity_fill_back AS cal_total_locked,
    initial_locked - running_locked_liquidity + running_liquidity_fill_back + total_fee_earned AS cal_balance 
FROM raw 