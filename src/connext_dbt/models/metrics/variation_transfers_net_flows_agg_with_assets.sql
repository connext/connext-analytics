WITH time_buckets AS (
    SELECT * FROM UNNEST([STRUCT(1 AS hours), STRUCT(3 AS hours), STRUCT(6 AS hours), STRUCT(12 AS hours), STRUCT(24 AS hours)])
),
interval_days AS (
    SELECT * FROM UNNEST([STRUCT(7 AS interval_days), STRUCT(15 AS interval_days), STRUCT(30 AS interval_days)])
),
inflow AS (
    SELECT
        rf.to AS chain,
        rf.asset,
        TIMESTAMP_SUB(
            TIMESTAMP_TRUNC(rf.date, HOUR),
            INTERVAL MOD(EXTRACT(HOUR FROM rf.date), tb.hours) HOUR
        ) AS date,
        tb.hours AS time_bucket,
        i.interval_days,
        SUM(rf.amount) AS inflow
    FROM `mainnet-bigq`.`metrics`.`raw_transfer_flows` AS rf
    CROSS JOIN time_buckets AS tb
    CROSS JOIN interval_days AS i
    WHERE TIMESTAMP_TRUNC(rf.date, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL i.interval_days DAY))
    GROUP BY chain, asset, date, time_bucket, interval_days
)

,
outflow AS (
    SELECT
        rf.from AS chain,
        rf.asset,
        TIMESTAMP_SUB(
            TIMESTAMP_TRUNC(rf.date, HOUR),
            INTERVAL MOD(EXTRACT(HOUR FROM rf.date), tb.hours) HOUR
        ) AS date,
        tb.hours AS time_bucket,
        i.interval_days,
        SUM(rf.amount) AS outflow
    FROM `mainnet-bigq`.`metrics`.`raw_transfer_flows` AS rf
    CROSS JOIN time_buckets AS tb
    CROSS JOIN interval_days AS i
    WHERE 
      TIMESTAMP_TRUNC(rf.date, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL i.interval_days DAY))
    GROUP BY chain, asset, date, time_bucket, interval_days
),
combined_flows AS (
    SELECT
        i.chain,
        i.asset,
        i.date,
        i.time_bucket,
        i.interval_days,
        COALESCE(i.inflow, 0) AS inflow,
        COALESCE(o.outflow, 0) AS outflow
    FROM inflow i
    FULL OUTER JOIN outflow o
        ON i.chain = o.chain
        AND i.asset = o.asset
        AND i.date = o.date
        AND i.time_bucket = o.time_bucket
        AND i.interval_days = o.interval_days
)

SELECT
    chain,
    asset,
    interval_days,
    time_bucket,
    AVG(inflow - outflow) AS avg_net_amount,
    STDDEV(inflow - outflow) AS stddev_net_amount,
    MIN(inflow - outflow) AS min_net_amount,
    MAX(inflow - outflow) AS max_net_amount,
    AVG(100 - ABS((inflow - outflow) / NULLIF((inflow + outflow), 0)) * 100) AS avg_percent_netted
FROM combined_flows
WHERE (chain= "Arbitrum One" AND  asset = "weth")
GROUP BY chain, asset, interval_days, time_bucket