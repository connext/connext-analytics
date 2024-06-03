WITH
    raw AS (
        SELECT
            r.date,
            r.router,
            r.chain,
            r.asset,
            r.router_fee,
            r.total_fee_earned,
            r.amount,
            r.total_locked,
            r.rbh_total_locked,
            r.ctv_total_locked,
            r.total_balance
            -- [ ] Adding USD Later
        FROM
            `mainnet-bigq.y42_connext_y42_dev_metrics.routers_tvl_agg__daily` r
    )
SELECT
    date,
    router AS router_address,
    chain,
    asset,
    router_fee AS daily_fee_earned,
    total_fee_earned AS total_fee_earned,
    amount AS daily_liquidity_added,
    total_locked AS tvl,
    rbh_total_locked AS router_locked_total,
    ctv_total_locked AS calculated_router_locked_total,
    total_balance AS balance
FROM
    raw
ORDER BY
    date DESC,
    router_address,
    chain,
    asset