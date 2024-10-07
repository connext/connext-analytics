-- final table:
WITH raw_tx AS (
SELECT
    'symbiosis' AS bridge,
    s.id,
    -- from
    s.from_timestamp AS from_date,
    s.from_hash AS from_tx_hash,
    s.from_chain_id,
    s.from_chain_name,
    s.from_address AS from_user_address,
    s.from_token_address,
    s.from_token_symbol,
    IFNULL(s.from_amount, 0) AS from_amount,
    IFNULL(s.from_amount_usd, 0) AS from_amount_usd,

    -- to
    s.to_hash AS to_tx_hash,
    s.to_timestamp AS to_date,
    s.to_address AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    IFNULL(s.to_amount, 0) AS to_amount,
    IFNULL(s.to_amount_usd, 0) AS to_amount_usd,
    -- gas fees
    s.gas_token_symbol AS gas_symbol,
    s.gas_amount  AS gas_amount,
    s.gas_amount_usd  AS gas_amount_usd,
    s.relay_symbol AS relay_symbol,
    s.relay_amount AS relay_amount,
    s.relay_amount_usd AS relay_amount_usd,
    -- price_group
    DATE_TRUNC(s.from_timestamp, HOUR) AS from_date_hour,
    DATE_TRUNC(s.to_timestamp, HOUR) AS to_date_hour,
    from_lts.price_symbol AS from_price_group,
    to_lts.price_symbol AS to_price_group

FROM {{ ref('stg_symbiosis_txs') }} AS s
-- from
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS from_lts ON s.from_token_symbol = from_lts.token_symbol
-- to
LEFT JOIN {{ ref('list_of_tokens_symbols') }} AS to_lts ON s.to_token_symbol = to_lts.token_symbol
)

, semi_cleaned AS (
SELECT
    rt.*,
    -- usd amounts
    rt.from_amount * from_price_group_p.price AS cal_from_amount_usd,
    rt.to_amount * to_price_group_p.price AS cal_to_amount_usd,
    -- prices
    rt.from_amount_usd / rt.from_amount AS from_token_price,
    rt.to_amount_usd / rt.to_amount AS to_token_price,
    from_price_group_p.price AS cal_from_token_price,
    to_price_group_p.price AS cal_to_token_price

FROM raw_tx AS rt
LEFT JOIN {{ ref('cln_token_prices') }} AS from_price_group_p
    ON rt.from_price_group = from_price_group_p.symbol AND rt.from_date_hour = from_price_group_p.date
LEFT JOIN {{ ref('cln_token_prices') }} AS to_price_group_p
        ON rt.to_price_group = to_price_group_p.symbol AND rt.to_date_hour = to_price_group_p.date

)

, final AS (
SELECT 
    sc.bridge,
    sc.id,
    sc.from_date,
    sc.from_tx_hash,
    sc.from_chain_id,
    sc.from_chain_name,
    sc.from_user_address,
    sc.from_token_address,
    sc.from_token_symbol,
    sc.from_amount,
    COALESCE(NULLIF(sc.from_amount_usd, 0), sc.cal_from_amount_usd) AS from_amount_usd,
    sc.to_date,
    sc.to_tx_hash,
    sc.to_chain_id,
    sc.to_chain_name,
    sc.to_user_address,
    sc.to_token_address,
    sc.to_token_symbol,
    sc.to_amount,
    COALESCE(NULLIF(sc.to_amount_usd, 0), sc.cal_to_amount_usd) AS to_amount_usd,
    sc.gas_symbol,
    sc.gas_amount,
    sc.gas_amount_usd,
    sc.relay_symbol,
    sc.relay_amount,
    -- (
    --     COALESCE(NULLIF(sc.from_amount_usd, 0), sc.cal_from_amount_usd)
    --     - COALESCE(NULLIF(sc.to_amount_usd, 0), sc.cal_to_amount_usd)
    -- ) AS relay_amount_usd,
    CAST(NULL AS FLOAT64) AS relay_amount_usd,
    COALESCE(NULLIF(sc.from_token_price, 0), sc.cal_from_token_price) AS from_token_price,
    COALESCE(NULLIF(sc.to_token_price, 0), sc.cal_to_token_price) AS to_token_price

FROM semi_cleaned sc

)

SELECT * FROM final
-- filter out bad routes
WHERE (relay_amount_usd/ from_amount_usd) < 0.8