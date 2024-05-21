-- Table: mainnet-bigq.dune.ad_hoc_ across_bridge_hourly_agg
WITH inflow AS (
    SELECT
        ah.date AS date,
        ah.token_symbol AS asset,
        ah.src_chain AS chain,
        SUM(ah.value_usd) AS inflow
    FROM `mainnet-bigq.dune.ad_hoc_ across_bridge_hourly_agg` AS ah
    WHERE token_symbol IS NOT NULL AND value_usd > 0
    GROUP BY 1, 2, 3
),
outflow AS (
    SELECT
        ah.date AS date,
        ah.token_symbol AS asset,
        ah.dst_chain AS chain,
        SUM(ah.value_usd) AS outflow
    FROM `mainnet-bigq.dune.ad_hoc_ across_bridge_hourly_agg` AS ah
    WHERE token_symbol IS NOT NULL AND value_usd > 0
    GROUP BY 1, 2, 3
),
daily_net_flow AS (
    SELECT
        -- [X] 1. metric by asset -> EOD netting - aggregate cross tab-> chains | assets | metrics(avg)
        
        COALESCE(i.date, o.date) AS date,
        COALESCE(i.chain, o.chain) AS chain,
        COALESCE(i.asset, o.asset) AS asset,
        COALESCE(i.inflow, 0) AS inflow,
        COALESCE(o.outflow, 0) AS outflow,
        COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0) AS net_amount
    FROM inflow i
    FULL OUTER JOIN outflow o ON i.date = o.date AND i.chain = o.chain AND i.asset = o.asset
)


SELECT * FROM daily_net_flow
ORDER BY 1