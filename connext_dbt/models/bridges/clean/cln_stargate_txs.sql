WITH raw_tx AS (
    -- ading pricing data
SELECT
    *, 
    DATE_TRUNC(s.from_date, HOUR) AS date_hour,
    from_lts.price_symbol AS from_price_group

FROM {{ref('stg_stargate_txs')}} AS s
-- from
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS from_lts ON s.from_token_symbol = from_lts.token_symbol
),

semi_raw_tx AS (
SELECT
    rt.*,
    
    -- usd amounts
    rt.from_amount * from_price_group_p.price AS from_amount_usd,
    rt.to_amount * from_price_group_p.price AS to_amount_usd,

    -- relay
    -- (rt.from_amount * from_price_group_p.price) - (rt.to_amount * from_price_group_p.price) AS relay_amount_usd,
    CAST(NULL AS FLOAT64) AS relay_amount_usd,

    -- prices
    from_price_group_p.price AS from_token_price

FROM raw_tx AS rt
-- from token prices
LEFT JOIN {{ ref('cln_token_prices') }} AS from_price_group_p
    ON rt.from_price_group = from_price_group_p.symbol AND rt.date_hour = from_price_group_p.date
)

-- final table
SELECT
    s.bridge AS bridge,
    s.id,
    -- from
    s.from_date,
    s.from_hash AS from_tx_hash,
    s.from_chain_id,
    s.from_chain_name,
    s.from_address AS from_user_address,
    s.from_token_address AS from_token_address,
    s.from_token_symbol,
    s.from_amount AS from_amount,
    s.from_amount_usd AS from_amount_usd,

    -- to
    s.to_hash AS to_tx_hash,
    s.to_date,
    s.to_address AS to_user_address,
    CAST(s.to_chain_id AS INT64) AS to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,

    -- gas and fees
    s.gas_token_symbol AS gas_symbol,
    s.gas_amount ,
    CAST(NULL AS FLOAT64) AS gas_amount_usd,

    -- relay(protocol fee)
    s.relay_symbol,
    s.relay_amount,
    s.relay_amount_usd,

    -- prices
    s.from_token_price

FROM semi_raw_tx AS s