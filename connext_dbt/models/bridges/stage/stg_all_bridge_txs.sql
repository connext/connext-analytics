-- Steps
-- Pull all raw 
WITH raw AS (
    SELECT DISTINCT * EXCEPT (api_url)
    FROM {{ source('raw', 'source_all_bridge_explorer_transfers_v2') }}
    WHERE CAST(from_amount AS FLOAT64) > 0 AND CAST(to_amount AS FLOAT64) > 0
),

semi AS (
    SELECT DISTINCT
        r.id,
        CAST(r.send_transaction_hash AS STRING) AS from_hash,
        CAST(r.receive_transaction_hash AS STRING) AS to_hash,
        r.from_address,
        r.to_address,
        r.from_chain_symbol AS from_chain_name,
        -- from
        -- from_chain_id
        r.to_chain_symbol AS to_chain_name,
        r.from_gas AS from_gas_amount,
        r.to_gas AS to_gas_amount,
        r.relayer_fee_in_native,
        -- to
        -- to_chain_id
        r.relayer_fee_in_tokens,
        TIMESTAMP_MILLIS(CAST(r.timestamp AS INT64)) AS date,
        CASE
            WHEN ft.blockchain = 'ETH' THEN 1
            WHEN ft.blockchain = 'POL' THEN 137
            WHEN ft.blockchain = 'ARB' THEN 42161
            WHEN ft.blockchain = 'AVA' THEN 43114
            WHEN ft.blockchain = 'OPT' THEN 10
            WHEN ft.blockchain = 'BAS' THEN 8453
            WHEN ft.blockchain = 'SOL' THEN NULL
            WHEN ft.blockchain = 'SRB' THEN NULL
            WHEN ft.blockchain = 'BSC' THEN 56
            WHEN ft.blockchain = 'CEL' THEN NULL
            WHEN ft.blockchain = 'TRX' THEN NULL
        END AS from_chain_id,
        COALESCE(ft.symbol, r.from_token_address) AS from_token_symbol,
        -- fees
        -- from gas fee token
        CAST(r.from_amount AS FLOAT64) AS from_amount,
        CASE
            WHEN tt.blockchain = 'ETH' THEN 1
            WHEN tt.blockchain = 'POL' THEN 137
            WHEN tt.blockchain = 'ARB' THEN 42161
            WHEN tt.blockchain = 'AVA' THEN 43114
            WHEN tt.blockchain = 'OPT' THEN 10
            WHEN tt.blockchain = 'BAS' THEN 8453
            WHEN tt.blockchain = 'SOL' THEN NULL
            WHEN tt.blockchain = 'SRB' THEN NULL
            WHEN tt.blockchain = 'BSC' THEN 56
            WHEN tt.blockchain = 'CEL' THEN NULL
            WHEN tt.blockchain = 'TRX' THEN NULL
        END AS to_chain_id,
        -- to_gas fee token
        COALESCE(tt.symbol, r.to_token_address) AS to_token_symbol,
        CAST(r.to_amount AS FLOAT64) AS to_amount,
        -- relay fee
        CASE
            WHEN ft.blockchain = 'ETH' THEN 'ETH'
            WHEN ft.blockchain = 'POL' THEN 'MATIC'
            WHEN ft.blockchain = 'ARB' THEN 'ETH'
            WHEN ft.blockchain = 'AVA' THEN 'AVAX'
            WHEN ft.blockchain = 'OPT' THEN 'ETH'
            WHEN ft.blockchain = 'BAS' THEN 'ETH'
            WHEN ft.blockchain = 'SOL' THEN 'SOL' -- Not included in ChainList as it's not EVM-based
            WHEN ft.blockchain = 'SRB' THEN 'SRB' -- Native token: Not found in EVM-based chains
            WHEN ft.blockchain = 'BSC' THEN 'BNB'
            WHEN ft.blockchain = 'CEL' THEN 'CEL' -- Native token: Not found in EVM-based chains
            WHEN ft.blockchain = 'TRX' THEN 'TRX' -- Not included in ChainList as it's not EVM-based
        END AS from_native_token,
        CASE
            WHEN tt.blockchain = 'ETH' THEN 'ETH'
            WHEN tt.blockchain = 'POL' THEN 'MATIC'
            WHEN tt.blockchain = 'ARB' THEN 'ETH'
            WHEN tt.blockchain = 'AVA' THEN 'AVAX'
            WHEN tt.blockchain = 'OPT' THEN 'ETH'
            WHEN tt.blockchain = 'BAS' THEN 'ETH'
            WHEN tt.blockchain = 'SOL' THEN 'SOL' -- Not included in ChainList as it's not EVM-based
            WHEN tt.blockchain = 'SRB' THEN 'SRB' -- Native token: Not found in EVM-based chains
            WHEN tt.blockchain = 'BSC' THEN 'BNB'
            WHEN tt.blockchain = 'CEL' THEN 'CEL' -- Native token: Not found in EVM-based chains
            WHEN tt.blockchain = 'TRX' THEN 'TRX' -- Not included in ChainList as it's not EVM-based
        END AS to_native_token
    FROM raw AS r
    LEFT JOIN `mainnet-bigq.raw.source_all_bridge_explorer_tokens` AS ft
        ON
            LOWER(r.from_token_address) = LOWER(ft.token_address)
            AND r.from_chain_symbol = ft.blockchain
    LEFT JOIN `mainnet-bigq.raw.source_all_bridge_explorer_tokens`
        AS tt ON LOWER(r.to_token_address) = LOWER(tt.token_address)
    AND r.to_chain_symbol = tt.blockchain
) -- add metadata related to tokens and chains
-- final
-- keep only col names:

SELECT DISTINCT
    s.id,
    s.date,
    
    -- from
    s.from_hash,
    s.from_address,
    CAST(s.from_chain_id AS INT64) AS from_chain_id,
    s.from_token_symbol,
    CAST(s.from_amount AS FLOAT64) AS from_amount,

    -- to
    CAST(s.to_chain_id AS INT64) AS to_chain_id,
    COALESCE(to_chain.name, s.to_chain_name) AS to_chain_name,
    s.to_token_symbol,
    s.to_hash,
    s.to_address,
    COALESCE(from_chain.name, s.from_chain_name) AS from_chain_name,
    CAST(s.to_amount AS FLOAT64) AS to_amount,
    
    -- fees
    s.from_native_token AS from_gas_native_token,
    CAST(s.from_gas_amount AS FLOAT64) AS from_gas_amount,
    s.to_native_token AS to_gas_native_token,
    CAST(s.to_gas_amount AS FLOAT64) AS to_gas_amount,
    
    
    -- protocol fee
    s.from_native_token AS from_relayer_fee_native_symbol,
    CAST(s.relayer_fee_in_native AS FLOAT64) AS from_relayer_fee_in_native,
    COALESCE(s.from_token_symbol, s.to_token_symbol) AS relayer_fee_token_symbol,
    CAST(s.relayer_fee_in_tokens AS FLOAT64) AS relayer_fee_in_tokens
FROM semi AS s
LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS from_chain ON s.from_chain_id = from_chain.chainid
LEFT JOIN {{ source('raw', 'source_chainlist_network__chains') }} AS to_chain ON s.to_chain_id = to_chain.chainid
