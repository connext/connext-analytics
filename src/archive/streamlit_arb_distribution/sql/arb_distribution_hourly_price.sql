WITH arb_daily_price AS (
  SELECT
    DATE_TRUNC (CAST(p.date AS TIMESTAMP), HOUR) AS date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
  WHERE
    CAST(p.date AS TIMESTAMP) >= "2024-06-01"
    AND symbol IN ("ARB")
  GROUP BY
    1,
    2 )

SELECT * FROM arb_daily_price