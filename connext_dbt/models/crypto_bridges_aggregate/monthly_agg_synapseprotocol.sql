WITH daily_price AS (
  SELECT
    DATE_TRUNC (CAST(p.date AS TIMESTAMP), HOUR) AS date,
    p.symbol AS asset,
    AVG(p.average_price) AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` p
  WHERE
    CAST(p.date AS TIMESTAMP) >= "2024-01-01"
  GROUP BY
    1,
    2 )
, raw AS (
  SELECT *, 
   CASE
      WHEN st.from_token_symbol = 'nETH' THEN 'WETH'
      WHEN st.from_token_symbol = 'ETH' THEN 'WETH'
      ELSE st.from_token_symbol
  END
    AS price_group,
  FROM
    `mainnet-bigq.raw.source_synapseprotocol_explorer_transactions` st)
  
, semi_raw AS (
  SELECT *, CAST(r.from_formatted_value AS FLOAT64) * dp.price AS usd_value
  FROM raw r
  LEFT JOIN
    daily_price dp
  ON
    DATE_TRUNC (TIMESTAMP_SECONDS(CAST(r.from_time AS INT64)), HOUR) = dp.date
    AND r.price_group = dp.asset
)


SELECT
  DATE_TRUNC(DATE(TIMESTAMP_SECONDS(CAST(from_time AS INT64))), MONTH) AS date,
  from_token_symbol AS currency_symbol,
  CASE
    WHEN from_chain_id = 1 THEN 'Ethereum'
    WHEN from_chain_id = 10 THEN 'Optimism'
    WHEN from_chain_id = 25 THEN 'Cronos'
    WHEN from_chain_id = 56 THEN 'BNB Chain'
    WHEN from_chain_id = 137 THEN 'Polygon'
    WHEN from_chain_id = 250 THEN 'Fantom'
    WHEN from_chain_id = 288 THEN 'Boba Network'
    WHEN from_chain_id = 1088 THEN 'Metis Andromeda'
    WHEN from_chain_id = 1284 THEN 'Moonbeam'
    WHEN from_chain_id = 1285 THEN 'Moonriver'
    WHEN from_chain_id = 2000 THEN 'DogeChain'
    WHEN from_chain_id = 42161 THEN 'Arbitrum'
    WHEN from_chain_id = 43114 THEN 'Avalanche'
    WHEN from_chain_id = 53935 THEN 'DFK Chain'
    WHEN from_chain_id = 7700 THEN 'Canto'
    WHEN from_chain_id = 81457 THEN 'Base'
    WHEN from_chain_id = 8453 THEN 'Base'
    WHEN from_chain_id = 1313161554 THEN 'Aurora'
    WHEN from_chain_id = 1666600000 THEN 'Harmony'
    WHEN from_chain_id = 8217 THEN 'Klaytn'
    ELSE CAST(from_chain_id AS STRING)
END
  AS source_chain_name,
  CASE
    WHEN to_chain_id = 1 THEN 'Ethereum'
    WHEN to_chain_id = 10 THEN 'Optimism'
    WHEN to_chain_id = 25 THEN 'Cronos'
    WHEN to_chain_id = 56 THEN 'BNB Chain'
    WHEN to_chain_id = 137 THEN 'Polygon'
    WHEN to_chain_id = 250 THEN 'Fantom'
    WHEN to_chain_id = 288 THEN 'Boba Network'
    WHEN to_chain_id = 1088 THEN 'Metis Andromeda'
    WHEN to_chain_id = 1284 THEN 'Moonbeam'
    WHEN to_chain_id = 1285 THEN 'Moonriver'
    WHEN to_chain_id = 2000 THEN 'DogeChain'
    WHEN to_chain_id = 42161 THEN 'Arbitrum'
    WHEN to_chain_id = 43114 THEN 'Avalanche'
    WHEN to_chain_id = 53935 THEN 'DFK Chain'
    WHEN to_chain_id = 7700 THEN 'Canto'
    WHEN to_chain_id = 81457 THEN 'Base'
    WHEN to_chain_id = 8453 THEN 'Base'
    WHEN to_chain_id = 1313161554 THEN 'Aurora'
    WHEN to_chain_id = 1666600000 THEN 'Harmony'
    WHEN to_chain_id = 8217 THEN 'Klaytn'
    ELSE CAST(to_chain_id AS STRING)
END
  AS destination_chain_name,
  COUNT(CASE
      WHEN usd_value >= 10000 THEN from_hash
      ELSE NULL
  END
    ) AS tx_count_10k_txs,
  SUM(CASE
      WHEN usd_value >= 10000 THEN usd_value
      ELSE NULL
  END
    ) AS volume_10k_txs,
  AVG(CASE
      WHEN usd_value >= 10000 THEN usd_value
      ELSE NULL
  END
    ) AS avg_volume_10k_txs,
  AVG(usd_value) AS avg_volume,
  COUNT(from_hash) AS total_txs,
  SUM(usd_value) AS total_volume
FROM
  semi_raw
WHERE
  from_token_symbol IS NOT NULL
  AND EXTRACT(YEAR
  FROM
    DATE(TIMESTAMP_SECONDS(CAST(from_time AS INT64)))) = EXTRACT(YEAR
  FROM
    CURRENT_DATE())
GROUP BY
  1,
  2,
  3,
  4
HAVING
  SUM(usd_value) > 0