WITH str_raw AS (

SELECT
    r.*,
    CAST(DATE_TRUNC(r.from_timestamp, HOUR) AS TIMESTAMP) AS from_date_hour,
    CAST(DATE_TRUNC(r.to_timestamp, HOUR) AS TIMESTAMP) AS to_date_hour,
    fts.price_symbol AS from_price_symbol,
    tts.price_symbol AS to_price_symbol,
    rts.price_symbol AS relay_fee_price_symbol
FROM {{ ref('stg_synapsois_txs') }} r
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  fts ON fts.token_symbol = r.from_token_symbol -- from token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  tts ON tts.token_symbol = r.to_token_symbol -- to token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  rts ON rts.token_symbol = r.relayer_fee_symbol -- relay fee symbol
)

, semi_raw_tx AS (
-- adding USD amounts based on the price
SELECT
    rt.*,
    -- usd amounts
    CAST(rt.from_amount AS FLOAT64) * from_price.average_price AS from_amount_usd,
    CAST(rt.to_amount AS FLOAT64) * to_price.average_price AS to_amount_usd,
    CAST(rt.relayer_fee AS FLOAT64) * relay_fee_price.average_price AS relay_fee_usd
FROM str_raw rt
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} from_price
    ON rt.from_price_symbol = from_price.symbol
    AND rt.from_date_hour = CAST(from_price.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} to_price
    ON rt.to_price_symbol = to_price.symbol
    AND rt.to_date_hour = CAST(to_price.date AS TIMESTAMP)
LEFT JOIN {{ source('dune', 'source_hourly_token_pricing_blockchain_eth') }} relay_fee_price
    ON rt.relay_fee_price_symbol = relay_fee_price.symbol
    AND rt.from_date_hour = CAST(relay_fee_price.date AS TIMESTAMP)
)


-- final 
SELECT
s.transfer_id AS id,
s.from_timestamp AS timestamp_in,
s.to_timestamp AS timestamp_out,
CAST(s.from_amount AS FLOAT64) AS amount_in,
CAST(s.to_amount AS FLOAT64) AS amount_out,
s.fee_token_symbol AS gas_fee_symbol,
NULL AS gas_fee,
NULL AS gas_price_usd,
s.relayer_fee_symbol AS relay_fee_symbol,
CAST(s.relayer_fee AS FLOAT64) AS relay_fee,
s.relay_fee_usd AS relay_fee_price_usd,
s.from_token_symbol AS symbol_in,
s.to_token_symbol AS symbol_out,
s.from_amount_usd AS price_in_usd,
s.to_amount_usd AS price_out_usd,
s.from_hash AS txn_hash_in,
s.to_hash AS txn_hash_out,
s.from_token_address AS token_address_in,
s.to_token_address AS token_address_out,
s.from_chain_id AS chain_in,
s.from_chain_name AS chain_in_name,
s.to_chain_id AS chain_out,
s.to_chain_name AS chain_out_name,
'synapse' AS bridge,
s.from_address AS user_address_in,
s.to_address AS user_address_out

from semi_raw_tx s