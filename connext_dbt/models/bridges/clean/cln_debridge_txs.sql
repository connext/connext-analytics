WITH raw_txs AS (
    SELECT
        tx.*,
        fts.price_symbol AS from_price_symbol,
        tts.price_symbol AS to_price_symbol,
        fets.price_symbol AS fee_price_symbol,
        pfts.price_symbol AS protocol_fee_price_symbol,
        DATE_TRUNC(date, HOUR) AS date_hour
    FROM {{ ref('stg_debridge_txs') }} AS tx
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS fts ON tx.from_token_symbol = fts.token_symbol -- from token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS tts ON tx.to_token_symbol = tts.token_symbol -- to token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS fets ON tx.fee_token_symbol = fets.token_symbol -- fee token symbol
    -- protocol fee token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS pfts ON tx.protocol_fee_token_symbol = pfts.token_symbol
),

semi_raw_tx AS (
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

    FROM raw_txs AS rt
    LEFT JOIN {{ ref('cln_token_prices') }} AS from_price
        ON
            rt.from_price_symbol = from_price.symbol
            AND rt.date_hour = from_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS to_price
        ON
            rt.to_price_symbol = to_price.symbol
            AND rt.date_hour = to_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS fee_price
        ON
            rt.fee_price_symbol = fee_price.symbol
            AND rt.date_hour = fee_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS protocol_fee_price
        ON
            rt.protocol_fee_price_symbol = protocol_fee_price.symbol
            AND rt.date_hour = protocol_fee_price.date
)

-- final table:
SELECT
    'debridge' AS bridge,
    s.id,
    -- from
    s.date AS from_date,
    s.from_tx_hash,
    s.from_chain_id,
    s.from_chain_name,
    s.from_user_address,
    s.from_token_address,
    s.from_token_symbol,
    s.from_amount,
    s.from_amount_usd,

    -- to
    s.to_tx_hash,
    s.user_address_out AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,
    s.fee_token_symbol AS gas_symbol,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    s.gas_fee AS gas_amount,
    s.gas_fee_usd AS gas_amount_usd,
    s.protocol_fee_token_symbol AS relay_symbol,

    -- relay(protocol fee)
    s.protocol_fee_value AS relay_amount,
    s.protocol_fee_value_usd AS relay_amount_usd,
    CAST(NULL AS timestamp) AS to_date

FROM semi_raw_tx AS s
