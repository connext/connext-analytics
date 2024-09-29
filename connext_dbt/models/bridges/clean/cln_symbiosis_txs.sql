-- final table:
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
    s.from_amount,
    s.from_amount_usd,

    -- to
    s.to_hash AS to_tx_hash,
    s.to_address AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    s.to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,
    s.fee_token_symbol AS gas_symbol,

    -- here the fee includes the gas fee and the protocol fee combined
    CAST(NULL AS TIMESTAMP) AS to_date,
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(NULL AS FLOAT64) AS gas_amount_usd,

    -- relay(protocol fee) -> here the fee is taken from the user at source chain
    CAST(from_token_symbol AS STRING) AS relay_symbol,
    CAST(NULL AS FLOAT64) AS relay_amount,
    CAST(fee_amount_usd AS FLOAT64) AS relay_amount_usd

FROM {{ ref('stg_symbiosis_txs') }} AS s
