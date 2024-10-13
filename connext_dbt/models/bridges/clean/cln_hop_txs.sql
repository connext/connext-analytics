WITH final_tx AS (
-- final table
SELECT
    'hop' AS bridge,
    s.id,

    -- from
    s.from_timestamp AS from_date,
    s.from_hash AS from_tx_hash,
    s.from_chain_id,
    s.from_chain_name,
    s.from_address AS from_user_address,
    CAST(NULL AS string) AS from_token_address,
    s.from_token_symbol,
    s.from_amount,
    s.from_amount_usd,
    s.to_timestamp AS to_date,

    -- to
    s.to_hash AS to_tx_hash,
    s.to_address AS to_user_address,
    s.to_chain_id,
    s.to_chain_name,
    CAST(NULL AS string) AS to_token_address,
    s.to_token_symbol,
    s.to_amount,
    s.to_amount_usd,
    s.relayer_fee_symbol AS relay_symbol,
    s.relayer_fee AS relay_amount,

    -- fees + relay(protocol fee) -> usually gas fee is taken from the user at source chain
    s.relayer_fee_in_usd AS relay_amount_usd,

    -- relay(protocol fee)
    CAST(NULL AS string) AS gas_symbol,
    CAST(NULL AS float64) AS gas_amount,
    CAST(NULL AS float64) AS gas_amount_usd,
    
    -- price
    s.from_amount_usd / s.from_amount AS from_token_price,
    s.to_amount_usd / s.to_amount AS to_token_price

FROM {{ ref('stg_hop_txs') }} AS s
)

SELECT * FROM final_tx
WHERE from_amount_usd > 0 AND to_amount_usd > 0 AND relay_amount_usd > 0