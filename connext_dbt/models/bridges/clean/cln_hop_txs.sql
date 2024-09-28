
-- final table
SELECT
    'hop' AS bridge,
    s.id AS id,
    
    -- from
    s.from_timestamp AS from_date,
    s.from_hash AS from_tx_hash,
    s.from_chain_id AS from_chain_id,
    s.from_chain_name AS from_chain_name,
    s.from_address AS from_user_address,
    CAST(NULL AS string) AS from_token_address,
    s.from_token_symbol AS from_token_symbol,
    s.from_amount AS from_amount,
    s.from_amount_usd AS from_amount_usd,

    -- to
    s.to_timestamp AS to_date,
    s.to_hash AS to_tx_hash,
    s.to_address AS to_user_address,
    s.to_chain_id AS to_chain_id,
    s.to_chain_name AS to_chain_name,
    CAST(NULL AS string) AS to_token_address,
    s.to_token_symbol AS to_token_symbol,
    s.to_amount AS to_amount,
    s.to_amount_usd AS to_amount_usd,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    CAST(NULL AS STRING) AS gas_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(NULL AS FLOAT64) AS gas_amount_usd,
    
    -- relay(protocol fee)
    s.relayer_fee_symbol AS relay_symbol,
    s.relayer_fee AS relay_amount,
    s.relayer_fee_in_usd AS relay_amount_usd

FROM {{ ref('stg_hop_txs') }} s