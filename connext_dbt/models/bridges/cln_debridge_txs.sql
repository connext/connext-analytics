-- steps:
-- pull from stg_debridge_txs
-- Adding pric for hourly data
-- format in needed final format
WITH raw_txs AS (
SELECT
    tx.*,
    DATE_TRUNC(date, HOUR) AS date_hour,
    fts.price_symbol AS from_price_symbol,
    tts.price_symbol AS to_price_symbol,
    fets.price_symbol AS fee_price_symbol,
    pfts.price_symbol AS protocol_fee_price_symbol
FROM {{ ref('stg_debridge_txs') }} AS tx
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  fts ON fts.token_symbol = tx.from_token_symbol -- from token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  tts ON tts.token_symbol = tx.to_token_symbol -- to token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  fets ON fets.token_symbol = tx.fee_token_symbol -- fee token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  pfts ON pfts.token_symbol = tx.protocol_fee_token_symbol -- protocol fee token symbol
)

, semi_raw_tx AS (
-- adding USD amounts based on the price
SELECT
    rt.*,
    -- usd amounts
    rt.from_amount * from_price.average_price AS from_amount_usd,
    rt.to_amount * to_price.average_price AS to_amount_usd,
    rt.gas_fee * fee_price.average_price AS gas_fee_usd,
    rt.protocol_fee_value * protocol_fee_price.average_price AS protocol_fee_value_usd

FROM raw_txs rt
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} from_price
    ON rt.from_price_symbol = from_price.symbol
    AND rt.date_hour = CAST(from_price.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} to_price
    ON rt.to_price_symbol = to_price.symbol
    AND rt.date_hour = CAST(to_price.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} fee_price
    ON rt.fee_price_symbol = fee_price.symbol
    AND rt.date_hour = CAST(fee_price.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} protocol_fee_price
    ON rt.protocol_fee_price_symbol = protocol_fee_price.symbol
    AND rt.date_hour = CAST(protocol_fee_price.date AS TIMESTAMP)
)

-- final table:
SELECT

s.transfer_id AS id,
s.date AS timestamp_in,
NULL AS timestamp_out,
s.from_amount AS amount_in,
s.to_amount AS amount_out,
s.fee_token_symbol AS gas_fee_symbol,
s.gas_fee AS gas_fee,
s.gas_fee_usd AS gas_price_usd,
s.protocol_fee_token_symbol AS relay_fee_symbol,
s.protocol_fee_value AS relay_fee,
s.protocol_fee_value_usd AS relay_fee_price_usd,
s.from_token_symbol AS symbol_in,
s.to_token_symbol AS symbol_out,
s.from_amount_usd AS price_in_usd,
s.to_amount_usd AS price_out_usd,
NULL AS txn_hash_in,
NULL AS txn_hash_out,
s.from_token_address AS token_address_in,
NULL AS token_address_out,
s.from_chain_id AS chain_in,
s.from_chain_name AS chain_in_name,
s.to_chain_id AS chain_out,
s.to_chain_name AS chain_out_name,
'debridge' AS bridge,
NULL AS user_address_in,
s.user_address_out AS user_address_out

FROM semi_raw_tx s