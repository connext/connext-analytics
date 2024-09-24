WITH
  relevant_usd_prices AS (
  SELECT
    symbol AS token_name,
    CAST(date AS TIMESTAMP) AS timestamp,
    average_price AS price
  FROM
    `mainnet-bigq.dune.source_hourly_token_pricing_blockchain_eth` ),
  debridge AS (
  SELECT
    transfer_id,
    TIMESTAMP_TRUNC(date, DAY) AS execute_day,
    UNIX_SECONDS(date) join_time,
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
      WHEN from_chain_id = 7565164 THEN 'Solana'
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
      WHEN to_chain_id = 7565164 THEN 'Solana'
      ELSE CAST(to_chain_id AS STRING)
  END
    AS destination_chain_name,
    UPPER(TRIM(from_actual_symbol)) source_token,
    from_actual_value / POW(10, from_actual_symbol_decimal) source_amount,
    UPPER(TRIM(to_symbol)) target_token,
    to_value / POW(10, to_symbol_decimal) target_amount
  FROM
    mainnet-bigq.stage.stg_cln_de_bridge_explorer_transactions__dedup
  WHERE
    LOWER(pre_swap_in_token_symbol) = 'nan'
    AND LOWER(pre_swap_out_token_symbol) = 'nan' ),
  intents_ AS (
  SELECT
    d.transfer_id,
    d.execute_day,
    d.source_chain_name,
    d.destination_chain_name,
    d.target_token,
    d.target_amount,
    fp_t.price AS target_token_price,
    -- to take care of USD variance in token names- replace this with price group later
    ( COALESCE(d.target_amount, 0) * CAST(fp_t.price AS FLOAT64) ) AS value_usd
  FROM
    debridge d
  LEFT JOIN
    relevant_usd_prices fp_t
  ON
    d.target_token = fp_t.token_name
    AND d.execute_day = fp_t.timestamp ),
  final AS (
  SELECT
    "DeBridge" AS bridge,
    i.execute_day AS date,
    i.target_token AS currency_symbol,
    i.source_chain_name,
    i.destination_chain_name,
    COUNT(CASE
        WHEN i.value_usd >= 10000 THEN i.transfer_id
        ELSE NULL
    END
      ) AS tx_count_10k_txs,
    SUM(CASE
        WHEN i.value_usd >= 10000 THEN i.value_usd
        ELSE NULL
    END
      ) AS volume_10k_txs,
    AVG(CASE
        WHEN i.value_usd >= 10000 THEN i.value_usd
        ELSE NULL
    END
      ) AS avg_volume_10k_txs,
    AVG(i.value_usd) AS avg_volume,
    COUNT(i.transfer_id) AS total_txs,
    SUM(i.value_usd) AS total_volume
  FROM
    intents_ i
  GROUP BY
    1,
    2,
    3,
    4,
    5
  HAVING
    SUM(i.value_usd) > 0)
SELECT
  *
FROM
  final