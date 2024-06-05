SELECT
    r.date,
    r.router,
    r.router_name,
    r.chain,
    r.asset,
    r.asset_group,
    r.router_fee_usd,
    r.router_volume_usd,
    r.amount_usd,
    r.total_locked_usd,
    r.total_fee_earned_usd,
    r.total_balance_usd
    FROM
        `mainnet-bigq.y42_connext_y42_dev.routers_tvl_agg_daily` r
    WHERE r.asset_group IN ("USDC", "USDT", "WETH", "DAI")