WITH raw_txs AS (
    SELECT
        tx.*,
        fts.price_symbol AS from_price_symbol,
        its.price_symbol AS interim_price_symbol,
        tts.price_symbol AS to_price_symbol,
        fets.price_symbol AS fee_price_symbol,
        pfts.price_symbol AS protocol_fee_price_symbol,
        DATE_TRUNC(date, HOUR) AS date_hour
    FROM {{ ref('stg_debridge_txs') }} AS tx
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS fts ON tx.from_token_symbol = fts.token_symbol -- from token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS its ON tx.interim_symbol = its.token_symbol -- interim token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS tts ON tx.to_token_symbol = tts.token_symbol -- to token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS fets ON tx.gas_token_symbol = fets.token_symbol -- fee token symbol
    -- protocol fee token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS pfts ON tx.relayer_fee_token_symbol = pfts.token_symbol
),

semi_raw_tx AS (
-- adding USD amounts based on the price
    SELECT
        rt.*,
        -- usd amounts
        rt.from_amount * from_price.price AS from_amount_usd,
        rt.interim_amount * interim_price.price AS interim_amount_usd,
        rt.to_amount * to_price.price AS to_amount_usd,
        rt.gas_amount * fee_price.price AS gas_fee_usd,
        rt.relay_fee_amount * protocol_fee_price.price AS relay_fee_amount_usd,

        -- prices
        from_price.price AS from_token_price,
        interim_price.price AS interim_price,
        to_price.price AS to_token_price,
        fee_price.price AS fee_token_price,
        protocol_fee_price.price AS relay_token_price

    FROM raw_txs AS rt
    LEFT JOIN {{ ref('cln_token_prices') }} AS from_price
        ON
            rt.from_price_symbol = from_price.symbol
            AND rt.date_hour = from_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS interim_price
        ON
            rt.interim_price_symbol = interim_price.symbol
            AND rt.date_hour = interim_price.date
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
    s.from_address AS from_user_address,
    s.from_token_address,
    CASE 
        WHEN s.from_token_price IS NOT NULL THEN s.from_token_symbol
        ELSE s.interim_symbol
    END AS from_token_symbol,

    CASE 
        WHEN s.from_token_price IS NOT NULL THEN s.from_amount
        ELSE s.interim_amount
    END AS from_amount,
    CASE 
        WHEN s.from_token_price IS NOT NULL THEN s.from_amount_usd
        ELSE s.interim_amount_usd
    END AS from_amount_usd,
    -- to
    CAST(NULL AS timestamp) AS to_date,
    s.to_tx_hash,
    s.to_address AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    s.gas_token_symbol AS gas_symbol,
    s.gas_amount AS gas_amount,
    s.gas_fee_usd AS gas_amount_usd,
    
    -- relay(protocol fee)
    s.relayer_fee_token_symbol AS relay_symbol,
    s.relay_fee_amount AS relay_amount,
    s.relay_fee_amount_usd AS relay_amount_usd,

    -- prices
    s.from_token_price,
    s.interim_price,
    s.to_token_price,
    s.fee_token_price,
    s.relay_token_price
FROM semi_raw_tx AS s
