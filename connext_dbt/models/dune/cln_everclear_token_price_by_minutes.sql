WITH raw AS (
  SELECT
    p.blockchain,
    p.symbol,
    CAST(p.minute AS TIMESTAMP) AS minute,
    AVG(p.price) AS price
  FROM {{ source("dune", "all_everclear_tokens_prices") }} p
  GROUP BY 1, 2, 3
),

min_max_time AS (
  SELECT
    blockchain,
    symbol,
    MIN(minute) AS min_time,
    MAX(minute) AS max_time
  FROM raw
  GROUP BY blockchain, symbol
),

minute_timeseries AS (
  -- Generate a series of minutes between min_time and max_time
  SELECT
    blockchain,
    symbol,
    TIMESTAMP_ADD(min_time, INTERVAL minute_offset MINUTE) AS minute
  FROM min_max_time
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, TIMESTAMP_DIFF(max_time, min_time, MINUTE))) AS minute_offset
),

filled_data AS (
  SELECT
    ts.blockchain,
    ts.symbol,
    ts.minute,
    r.price
  FROM minute_timeseries ts
  LEFT JOIN raw r
    ON ts.blockchain = r.blockchain
    AND ts.symbol = r.symbol
    AND ts.minute = r.minute
)

-- Fill missing price with the previous available price
SELECT
  blockchain,
  symbol,
  minute,
  IFNULL(price, LAST_VALUE(price IGNORE NULLS) OVER (PARTITION BY blockchain, symbol ORDER BY minute)) AS price
FROM filled_data
ORDER BY blockchain, symbol, minute