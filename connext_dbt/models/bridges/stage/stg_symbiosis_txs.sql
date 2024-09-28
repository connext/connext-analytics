WITH extracted_routes AS (
  SELECT
    -- Extract the JSON arrays once
    JSON_EXTRACT_ARRAY(tx.from_route) AS from_route_array,
    JSON_EXTRACT_ARRAY(tx.to_route) AS to_route_array,
    -- Pre-calculate the length of the to_route array once
    ARRAY_LENGTH(JSON_EXTRACT_ARRAY(tx.to_route)) AS to_route_length,
    tx.id,
    tx.created_at,
    tx.from_address,
    tx.to_address,
    tx.from_tx_hash,
    tx.success_at,
    tx.from_chain_id,
    from_chain.name AS from_chain_name,
    tx.token_address,
    COALESCE(tx.token_symbol, tx.token_address) AS from_token_symbol,
    tx.from_amount_usd,
    tx.to_tx_hash,
    tx.to_chain_id,
    to_chain.name AS to_chain_name,
    tx.to_amount_usd,
    from_chain.nativeCurrency_symbol AS fee_token_symbol,
    (tx.from_amount_usd - tx.to_amount_usd) AS fee_amount_usd,
    tx.state,
    tx.type,
    tx.event_type
  FROM
    `mainnet-bigq.raw.source_symbiosis_bridge_explorer_transactions` tx
  LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS from_chain
    ON tx.from_chain_id = from_chain.chainId
  LEFT JOIN `mainnet-bigq.raw.source_chainlist_network__chains` AS to_chain
    ON tx.to_chain_id = to_chain.chainId

  -- Keep only successful transfers
  WHERE state = 0
    AND type = 0
    AND event_type IN (1, 3)
)

, semi_cleaned AS (
SELECT DISTINCT
  -- Transfer details
  CAST(id AS STRING) AS id,
  CAST(created_at AS TIMESTAMP) AS from_timestamp,
  from_address,
  to_address,
  from_tx_hash AS from_hash,
  CAST(success_at AS TIMESTAMP) AS to_timestamp,

  -- from_route (first element of from_route_array)
  CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.chain_id') AS INT64) AS from_chain_id,
  CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.amount') AS FLOAT64) AS from_amount,
  JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.symbol') AS from_token_symbol,
  JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.name') AS from_token_name,
  JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.address') AS from_token_address,
  CAST(JSON_EXTRACT_SCALAR(from_route_array[SAFE_OFFSET(0)], '$.token.decimals') AS FLOAT64) AS from_token_decimals,
  
  -- to_route (last element of to_route_array)
  CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.chain_id') AS INT64) AS to_chain_id,
  CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.amount') AS FLOAT64) AS to_amount,
  JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.symbol') AS to_token_symbol,
  JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.name') AS to_token_name,
  JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.address') AS to_token_address,
  CAST(JSON_EXTRACT_SCALAR(to_route_array[SAFE_OFFSET(to_route_length - 1)], '$.token.decimals') AS FLOAT64) AS to_token_decimals,

  -- Existing fields
  from_chain_name,
  from_amount_usd,
  to_tx_hash AS to_hash,
  to_chain_name,
  to_amount_usd,
  fee_token_symbol,
  CAST(NULL AS FLOAT64) AS gas_fee,
  CAST(NULL AS FLOAT64) AS gas_fee_usd,
  fee_amount_usd

FROM extracted_routes
)

SELECT
  id,
  from_timestamp,
  from_address,
  to_address,
  from_hash,
  to_hash,
  to_timestamp,
  from_chain_id,
  from_chain_name,
  from_token_symbol,
  from_token_name,
  from_token_address,
  from_token_decimals,
  to_chain_id,
  to_chain_name,
  to_token_symbol,
  to_token_name,
  to_token_address,
  to_token_decimals,
  from_amount AS from_amount_raw,
  from_amount / POW(10, from_token_decimals) AS from_amount,
  from_amount_usd,
  to_amount AS to_amount_raw,
  to_amount / POW(10, to_token_decimals) AS to_amount,
  to_amount_usd,
  fee_token_symbol,
  fee_amount_usd,
  gas_fee,
  gas_fee_usd
FROM semi_cleaned
WHERE from_amount > 0
AND to_amount > 0
