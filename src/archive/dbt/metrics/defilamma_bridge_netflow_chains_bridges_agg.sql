-- Asset Filter: 
-- [X] 1. metric by asset -> EOD netting - aggregate cross tab-> chains | assets | metrics(avg)
-- [X] 2. metric by chain -> EOD netting - aggregate cross tab-> chains | DATE | metrics(avg)
-- [X] 3. metric by bridge, chain -> EOD netting - aggregate cross tab-> chains | assets | metrics(avg)
-- [ ] 4. Agg: metric by chain -> avg across chain and date

WITH inflow AS (
    SELECT
        TIMESTAMP_SECONDS(d.date) AS date,
        -- bridge
        d.name AS bridge,
        d.symbol AS asset,
        d.chain AS chain,
        SUM(d.usd_value) AS inflow
    FROM
        `mainnet-bigq.raw.stg__cln_source_defilamma_bridges_history_tokens` AS d
    WHERE symbol IS NOT NULL AND usd_value > 0 AND tx_type = "deposit"
    -- date from start of the year
    GROUP BY 1, 2, 3, 4
),

outflow AS (
    SELECT
        TIMESTAMP_SECONDS(d.date) AS date,
        d.name AS bridge,
        d.symbol AS asset,
        d.chain AS chain,
        SUM(d.usd_value) AS outflow
    FROM
        `mainnet-bigq.raw.stg__cln_source_defilamma_bridges_history_tokens` AS d
    WHERE symbol IS NOT NULL AND usd_value > 0 AND tx_type = "withdrawal"
    GROUP BY 1, 2, 3, 4
),

daily_net_flow AS (
    SELECT
        -- [X] 1. metric by asset -> EOD netting - aggregate cross tab-> chains | assets | metrics(avg)

        DATE_TRUNC(COALESCE(i.date, o.date), DAY) AS date,
        COALESCE(i.chain, o.chain) AS chain,
        COALESCE(i.asset, o.asset) AS asset,
        COALESCE(i.bridge, o.bridge) AS bridge,
        SUM(COALESCE(i.inflow, 0)) - SUM(COALESCE(o.outflow, 0)) AS net_amount,
        100
        - ABS(
            (SUM(COALESCE(i.inflow, 0)) - SUM(COALESCE(o.outflow, 0)))
            / NULLIF(
                (SUM(COALESCE(i.inflow, 0)) + SUM(COALESCE(o.outflow, 0))), 0
            )
        )
        * 100 AS percent_netted
    FROM inflow i
    FULL OUTER JOIN
        outflow o
        ON i.date = o.date AND i.chain = o.chain AND i.asset = o.asset
    GROUP BY 1, 2, 3, 4
)

SELECT
    EXTRACT(YEAR FROM date) AS year,
    chain,
    bridge,
    AVG(net_amount) AS avg_net_amount,
    AVG(percent_netted) AS avg_percent_netted
FROM daily_net_flow dnf
INNER JOIN `mainnet-bigq.stage.connext_tokens` AS t
    ON LOWER(dnf.asset) = LOWER(t.token_name)
GROUP BY 1, 2, 3
