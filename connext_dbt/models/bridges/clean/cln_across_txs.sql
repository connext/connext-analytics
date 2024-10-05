WITH raw_tx AS (
    -- ading pricing data
SELECT
    *, 
    DATE_TRUNC(s.date, HOUR) AS date_hour,
    from_lts.price_symbol AS from_price_group,
    to_lts.price_symbol AS to_price_group,
    gas_lts.price_symbol AS gas_price_group

FROM {{ref('stg_across_txs')}} AS s
-- from
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS from_lts ON s.from_token_symbol = from_lts.token_symbol
-- to
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS to_lts ON s.to_token_symbol = to_lts.token_symbol
-- gas
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS gas_lts ON s.gas_token_symbol = gas_lts.token_symbol
), 

semi_raw_tx AS (
SELECT
    rt.*,
    -- usd amounts
    rt.from_amount * from_price_group_p.price AS from_amount_usd,
    rt.to_amount * to_price_group_p.price AS to_amount_usd,
    rt.gas_amount * gas_price_group_p.price AS gas_amount_usd

FROM raw_tx AS rt
-- from token prices
LEFT JOIN {{ ref('cln_token_prices') }} AS from_price_group_p
    ON rt.from_price_group = from_price_group_p.symbol AND rt.date_hour = from_price_group_p.date
LEFT JOIN {{ ref('cln_token_prices') }} AS to_price_group_p
        ON rt.to_price_group = to_price_group_p.symbol AND rt.date_hour = to_price_group_p.date
LEFT JOIN {{ ref('cln_token_prices') }} AS gas_price_group_p
    ON
        rt.gas_price_group
        = gas_price_group_p.symbol
        AND rt.date_hour = gas_price_group_p.date
)


-- final table
SELECT
    --cal to amount: from - relay amount
    s.bridge AS bridge,
    s.id,
    -- from
    CAST(NULL AS timestamp) AS from_date,
    s.from_hash AS from_tx_hash,
    CAST(s.from_chain_id AS INT64) AS from_chain_id,
    s.from_chain_name,
    s.from_user AS from_user_address,
    s.from_token_address AS from_token_address,
    s.from_token_symbol,
    s.from_amount,
    s.from_amount_usd,

    -- to
    s.to_hash AS to_tx_hash,
    s.date AS to_date,
    s.to_user AS to_user_address,
    CAST(s.to_chain_id AS INT64) AS to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,

    -- gas and fees
    s.gas_token_symbol AS gas_symbol,
    s.gas_amount AS gas_amount,
    s.gas_amount_usd AS gas_amount_usd,

    -- relay(protocol fee)
    s.relayer_fee_token_symbol AS relay_symbol,
    CAST(NULL AS FLOAT64) AS relay_amount,
    s.relay_fee_usd AS relay_amount_usd

FROM semi_raw_tx AS s
