-- -- create price group and join on date to hour

WITH raw_tx AS (
    SELECT
        all_bridge.*,
        from_lts.price_symbol AS from_price_group,
        to_lts.price_symbol AS to_price_group,
        from_gas_lts.price_symbol AS from_gas_native_and_relay_fee_native_price_group,
        to_gas_lts.price_symbol AS to_gas_native_price_group,
        relayer_fee_lts.price_symbol AS relayer_fee_token_price_group,
        DATE_TRUNC(all_bridge.date, HOUR) AS date_hour


    FROM {{ ref('stg_all_bridge_txs') }} AS all_bridge
    -- from
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS from_lts ON all_bridge.from_token_symbol = from_lts.token_symbol
    -- to
    LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS to_lts ON all_bridge.to_token_symbol = to_lts.token_symbol
    -- from_gas_native_token+ relayer_fee_native_symbol
    LEFT JOIN
        {{ ref('list_of_tokens_symbols') }} AS from_gas_lts
        ON all_bridge.from_gas_native_token = from_gas_lts.token_symbol
    -- to_gas_native_token
    LEFT JOIN
        {{ ref('list_of_tokens_symbols') }} AS to_gas_lts
        ON all_bridge.to_gas_native_token = to_gas_lts.token_symbol
    -- relayer_fee_token_symbol
    LEFT JOIN
        {{ ref('list_of_tokens_symbols') }} AS relayer_fee_lts
        ON all_bridge.relayer_fee_token_symbol = relayer_fee_lts.token_symbol
),

-- convert the amounts tousd price
semi_raw_tx AS (
    SELECT
        rt.*,

        -- usd amounts
        rt.from_amount * from_price_group_p.price AS from_amount_usd,
        rt.to_amount * to_price_group_p.price AS to_amount_usd,
        -- fees
        rt.from_gas_amount * from_gas_native_and_relay_fee_native_price_group_p.price AS from_gas_amount_usd,
        rt.to_gas_amount * to_gas_native_price_group_p.price AS to_gas_amount_usd,
        rt.from_relayer_fee_in_native
        * from_gas_native_and_relay_fee_native_price_group_p.price AS from_relayer_fee_in_native_usd,
        rt.relayer_fee_in_tokens * relayer_fee_token_price_group_p.price AS relayer_fee_in_tokens_usd,

        -- prices
        from_price_group_p.price AS from_token_price,
        to_price_group_p.price AS to_token_price,
        from_gas_native_and_relay_fee_native_price_group_p.price AS from_gas_token_price,
        to_gas_native_price_group_p.price AS to_gas_token_price,
        relayer_fee_token_price_group_p.price AS relayer_fee_token_price
    FROM raw_tx AS rt
    -- get price for each token column

    LEFT JOIN {{ ref('cln_token_prices') }} AS from_price_group_p
        ON rt.from_price_group = from_price_group_p.symbol AND rt.date_hour = from_price_group_p.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS to_price_group_p
        ON rt.to_price_group = to_price_group_p.symbol AND rt.date_hour = to_price_group_p.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS from_gas_native_and_relay_fee_native_price_group_p
        ON
            rt.from_gas_native_and_relay_fee_native_price_group
            = from_gas_native_and_relay_fee_native_price_group_p.symbol
            AND rt.date_hour = from_gas_native_and_relay_fee_native_price_group_p.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS to_gas_native_price_group_p
        ON
            rt.to_gas_native_price_group = to_gas_native_price_group_p.symbol
            AND rt.date_hour = to_gas_native_price_group_p.date
    LEFT JOIN {{ ref('cln_token_prices') }} AS relayer_fee_token_price_group_p
        ON
            rt.relayer_fee_token_price_group = relayer_fee_token_price_group_p.symbol
            AND rt.date_hour = relayer_fee_token_price_group_p.date
)


-- final table
SELECT
    --cal to amount: from - relay amount
    'all_bridges' AS bridge,
    s.id,
    -- from
    s.date AS from_date,
    s.from_hash AS from_tx_hash,
    CAST(s.from_chain_id AS INT64) AS from_chain_id,
    s.from_chain_name,
    s.from_address AS from_user_address,
    s.from_token_symbol,
    CAST(s.from_amount AS FLOAT64) AS from_amount,
    s.from_amount_usd,
    s.to_hash AS to_tx_hash,

    -- to
    s.to_address AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    s.to_token_symbol,
    CAST(NULL AS string) AS from_token_address,
    CAST(NULL AS timestamp) AS to_date,
    CAST(NULL AS string) AS to_token_address,
    --cal to amount: from - relay amount
    s.to_amount AS to_amount,
    s.to_amount_usd,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    COALESCE(s.from_gas_native_token, s.to_gas_native_token) AS gas_symbol,
    -- COALESCE(s.from_gas_amount, 0) + COALESCE(s.to_gas_amount, 0) AS gas_amount,
    CAST(NULL AS float64) AS gas_amount,
    COALESCE(s.from_gas_amount_usd, 0) + COALESCE(s.to_gas_amount_usd, 0) AS gas_amount_usd,

    -- relay(protocol fee)
    COALESCE(s.from_relayer_fee_native_symbol, s.relayer_fee_token_symbol) AS relay_symbol,
    COALESCE(s.from_relayer_fee_in_native, 0) + COALESCE(s.relayer_fee_in_tokens, 0) AS relay_amount,
    COALESCE(s.from_relayer_fee_in_native_usd, 0) + COALESCE(s.relayer_fee_in_tokens_usd, 0) AS relay_amount_usd,

    -- prices
    s.from_token_price,
    s.to_token_price,
    s.from_gas_token_price,
    s.to_gas_token_price,
    s.relayer_fee_token_price

FROM semi_raw_tx AS s
