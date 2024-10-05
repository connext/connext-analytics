WITH str_raw AS (

    SELECT
        r.*,
        fts.price_symbol AS from_price_symbol,
        tts.price_symbol AS to_price_symbol,
        rts.price_symbol AS relay_fee_price_symbol,
        CAST(DATE_TRUNC(r.from_timestamp, HOUR) AS TIMESTAMP) AS from_date_hour,
        CAST(DATE_TRUNC(r.to_timestamp, HOUR) AS TIMESTAMP) AS to_date_hour
    FROM {{ ref('stg_synapse_txs') }} AS r
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS fts ON r.from_token_symbol = fts.token_symbol -- from token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS tts ON r.to_token_symbol = tts.token_symbol -- to token symbol
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS rts ON r.relayer_fee_symbol = rts.token_symbol -- relay fee symbol
),

semi_raw_tx AS (
-- adding USD amounts based on the price
    SELECT
        rt.*,
        -- usd amounts
        CAST(rt.from_amount AS FLOAT64) * from_price.price AS from_amount_usd,
        CAST(rt.to_amount AS FLOAT64) * to_price.price AS to_amount_usd,
        CAST(rt.relayer_fee AS FLOAT64) * relay_fee_price.price AS relay_fee_usd
    FROM str_raw AS rt
    LEFT JOIN {{ ref('cln_token_prices') }} AS from_price
        ON
            rt.from_price_symbol = from_price.symbol
            AND rt.from_date_hour = from_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS to_price
        ON
            rt.to_price_symbol = to_price.symbol
            AND rt.to_date_hour = to_price.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS relay_fee_price
        ON
            rt.relay_fee_price_symbol = relay_fee_price.symbol
            AND rt.from_date_hour = relay_fee_price.date
)


-- final 
SELECT
    "synapse" AS bridge,
    s.id,
    -- from
    s.from_timestamp AS from_date,
    s.from_hash AS from_tx_hash,
    CAST(s.from_chain_id AS INT64) AS from_chain_id,
    s.from_chain_name AS from_chain_name,
    s.from_address AS from_user_address,
    s.from_token_address AS from_token_address,
    s.from_token_symbol AS from_token_symbol,
    CAST(s.from_amount AS FLOAT64) AS from_amount,
    s.from_amount_usd AS from_amount_usd,
    s.to_timestamp AS to_date,
    -- to
    s.to_hash AS to_tx_hash,
    s.to_address AS to_user_address,
    CAST(s.to_chain_id AS INT64) AS to_chain_id,
    s.to_chain_name AS to_chain_name,
    s.to_token_address AS to_token_address,
    s.to_token_symbol AS to_token_symbol,
    CAST(s.to_amount AS FLOAT64) AS to_amount,
    s.to_amount_usd AS to_amount_usd,

    -- fees
    s.fee_token_symbol AS gas_symbol,
    s.relayer_fee_symbol AS relay_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(NULL AS FLOAT64) AS gas_amount_usd,
    CAST(s.relayer_fee AS FLOAT64) AS relay_amount,
    CAST(s.relay_fee_usd AS FLOAT64) AS relay_amount_usd

FROM semi_raw_tx AS s
