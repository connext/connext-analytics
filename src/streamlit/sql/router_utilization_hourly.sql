SELECT 
    r.date,
    r.router,
    r.router_name,
    r.chain,
    r.asset,
    r.asset_group,
    r.initial_locked_usd,
    r.router_fee_usd,
    r.liquidity_locked_usd,
    r.liquidity_fill_back_usd, 
    r.total_locked_usd, 
    r.total_fee_earned_usd, 
    r.total_balance_usd,
    r.router_volume_usd

FROM `mainnet-bigq.y42_connext_y42_dev.router_tvl_liquidity_under_utilizations__hourly` r
WHERE r.asset_group IN ("USDC", "USDT", "WETH", "DAI", "ezETH")
-- Testing
-- AND router_name= "Connext" AND chain = "Ethereum Mainnet" AND asset = "WETH"
