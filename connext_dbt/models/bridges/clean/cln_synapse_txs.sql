WITH str_raw AS (

SELECT
    r.*,
    CAST(DATE_TRUNC(r.from_timestamp, HOUR) AS TIMESTAMP) AS from_date_hour,
    CAST(DATE_TRUNC(r.to_timestamp, HOUR) AS TIMESTAMP) AS to_date_hour,
    fts.price_symbol AS from_price_symbol,
    tts.price_symbol AS to_price_symbol,
    rts.price_symbol AS relay_fee_price_symbol
FROM {{ ref('stg_synapse_txs') }} r
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  fts ON fts.token_symbol = r.from_token_symbol -- from token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  tts ON tts.token_symbol = r.to_token_symbol -- to token symbol
LEFT JOIN {{ ref('list_of_tokens_symbols') }}  rts ON rts.token_symbol = r.relayer_fee_symbol -- relay fee symbol
)

, semi_raw_tx AS (
-- adding USD amounts based on the price
SELECT
    rt.*,
    -- usd amounts
    CAST(rt.from_amount AS FLOAT64) * from_price.price AS from_amount_usd,
    CAST(rt.to_amount AS FLOAT64) * to_price.price AS to_amount_usd,
    CAST(rt.relayer_fee AS FLOAT64) * relay_fee_price.price AS relay_fee_usd
FROM str_raw rt
LEFT JOIN {{ ref('cln_token_prices') }} from_price
    ON rt.from_price_symbol = from_price.symbol
    AND rt.from_date_hour = from_price.date
LEFT JOIN {{ ref('cln_token_prices') }} to_price
    ON rt.to_price_symbol = to_price.symbol
    AND rt.to_date_hour = to_price.date
LEFT JOIN {{ ref('cln_token_prices') }} relay_fee_price
    ON rt.relay_fee_price_symbol = relay_fee_price.symbol
    AND rt.from_date_hour = relay_fee_price.date
)


-- final 
SELECT
    "synapse" AS bridge,
    s.id AS id,
    -- from
    s.from_timestamp AS from_date,
    s.from_hash AS from_txn_hash,
    s.from_chain_id AS chain_in,
    s.from_chain_name AS from_chain_name,
    s.from_address AS from_user_address,
    s.from_token_address AS from_token_address,
    s.from_token_symbol AS from_token_symbol,
    CAST(s.from_amount AS FLOAT64) AS from_amount,
    s.from_amount_usd AS from_amount_usd,
    -- to
    s.to_timestamp AS to_date,
    s.to_hash AS to_txn_hash,
    s.to_address AS to_user_address,
    s.to_chain_id AS to_chain_id,
    s.to_chain_name AS to_chain_name,
    s.to_token_address AS to_token_address,
    s.to_token_symbol AS to_token_symbol,
    CAST(s.to_amount AS FLOAT64) AS to_amount,
    s.to_amount_usd AS to_amount_usd,
    -- fees
    s.fee_token_symbol AS gas_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(NULL AS FLOAT64) AS gas_amount_usd,
    s.relayer_fee_symbol AS relay_symbol,
    CAST(s.relayer_fee AS FLOAT64) AS relay_amount,
    CAST(s.relay_fee_usd AS FLOAT64) AS relay_amount_usd

FROM semi_raw_tx s