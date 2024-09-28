-- models/transfers_net_flows_dynamic.sql
{% macro generate_transfers_net_flows(hour_interval) %}

WITH raw AS (
    SELECT
        TIMESTAMP_SUB(
            TIMESTAMP_TRUNC(rf.date, HOUR),
            INTERVAL MOD(EXTRACT(HOUR FROM rf.date), {{ hour_interval }}) HOUR
        ) AS date,
        rf.from,
        rf.to,
        rf.asset,
        SUM(rf.amount) AS amount,
        COUNT(rf.transfer_id) AS transfers
    FROM `mainnet-bigq`.`metrics`.`raw_transfer_flows` AS rf
    GROUP BY 1, 2, 3, 4
),
inflow AS (
    SELECT
        r.date,
        r.from AS chain,
        r.asset,
        SUM(r.amount) AS inflow
    FROM raw r
    GROUP BY r.date, r.from, r.asset
),
outflow AS (
    SELECT
        r.date,
        r.to AS chain,
        r.asset,
        SUM(r.amount) AS outflow
    FROM raw r
    GROUP BY r.date, r.to, r.asset
)

SELECT
    COALESCE(i.date, o.date) AS date,
    COALESCE(i.chain, o.chain) AS chain,
    COALESCE(i.asset, o.asset) AS asset,
    COALESCE(i.inflow, 0) - COALESCE(o.outflow, 0) AS net_amount
FROM inflow i
FULL OUTER JOIN outflow o ON i.date = o.date AND i.chain = o.chain AND i.asset = o.asset

{% endmacro %}