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
    CAST(NULL AS string) AS from_user_address,
    CAST(NULL AS string) AS to_token_address,
    -- usd amounts
    rt.from_amount * from_price.price AS from_amount_usd,
    rt.to_amount * to_price.price AS to_amount_usd,
    rt.gas_fee * fee_price.price AS gas_fee_usd,
    rt.protocol_fee_value * protocol_fee_price.price AS protocol_fee_value_usd

FROM raw_txs rt
LEFT JOIN {{ ref('cln_token_prices') }} from_price
    ON rt.from_price_symbol = from_price.symbol
    AND rt.date_hour = from_price.date
LEFT JOIN {{ ref('cln_token_prices') }} to_price
    ON rt.to_price_symbol = to_price.symbol
    AND rt.date_hour = to_price.date
LEFT JOIN {{ ref('cln_token_prices') }} fee_price
    ON rt.fee_price_symbol = fee_price.symbol
    AND rt.date_hour = fee_price.date
LEFT JOIN {{ ref('cln_token_prices') }} protocol_fee_price
    ON rt.protocol_fee_price_symbol = protocol_fee_price.symbol
    AND rt.date_hour = protocol_fee_price.date
)

-- final table:
SELECT
    'debridge' AS bridge,
    s.id AS id,
    -- from
    s.date AS from_date,
    s.from_tx_hash AS from_tx_hash,
    s.from_chain_id AS from_chain_id,
    s.from_chain_name AS from_chain_name,
    s.from_user_address AS from_user_address,
    s.from_token_address AS from_token_address,
    s.from_token_symbol AS from_token_symbol,
    s.from_amount AS from_amount,
    s.from_amount_usd AS from_amount_usd,

    -- to
    CAST(NULL AS TIMESTAMP) AS to_date,
    s.to_tx_hash AS to_tx_hash,
    s.user_address_out AS to_user_address,
    s.to_chain_id AS to_chain_id,
    s.to_chain_name AS to_chain_name,
    s.to_token_address AS to_token_address,
    s.to_token_symbol AS to_token_symbol,
    s.to_amount AS to_amount,
    s.to_amount_usd AS to_amount_usd,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    s.fee_token_symbol AS gas_symbol,
    s.gas_fee AS gas_amount,
    s.gas_fee_usd AS gas_amount_usd,

    -- relay(protocol fee)
    s.protocol_fee_token_symbol AS relay_symbol,
    s.protocol_fee_value AS relay_amount,
    s.protocol_fee_value_usd AS relay_amount_usd

FROM semi_raw_tx s