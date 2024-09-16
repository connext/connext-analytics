-- Steps
-- Pull all raw 

WITH raw AS (
    (
    SELECT DISTINCT * EXCEPT (_dlt_load_id, _dlt_id)
    FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers`
    )
    UNION ALL
    (
    SELECT DISTINCT *
    FROM `mainnet-bigq.raw.source_all_bridge_explorer_transfers_new`
    )
)

, semi AS (
SELECT
    r.id AS transfer_id,
    TIMESTAMP_MILLIS(CAST(r.timestamp AS INT64)) AS date,
    r.from_address,
    r.to_address,

    -- from
    NULL AS from_chain_id,
    r.from_chain_symbol AS from_chain_name,
    COALESCE(ft.symbol, r.from_token_address) AS from_token_symbol,
    CAST(r.from_amount AS FLOAT64) AS from_amount,

    -- to
    NULL AS to_chain_id,
    r.to_chain_symbol AS to_chain_name,
    COALESCE(tt.symbol, r.to_token_address) AS to_token_symbol,
    CAST(r.from_amount AS FLOAT64) - CAST(r.relayer_fee_in_tokens AS FLOAT64) AS to_amount,
    
    -- fees
    -- from gas fee token
    CASE 
        WHEN ft.blockchain = 'ETH' THEN 'ETH'
        WHEN ft.blockchain = 'POL' THEN 'MATIC'
        WHEN ft.blockchain = 'ARB' THEN 'ETH'
        WHEN ft.blockchain = 'AVA' THEN 'AVAX'
        WHEN ft.blockchain = 'OPT' THEN 'ETH'
        WHEN ft.blockchain = 'BAS' THEN 'ETH'
        WHEN ft.blockchain = 'SOL' THEN 'SOL'  -- Not included in ChainList as it's not EVM-based
        WHEN ft.blockchain = 'SRB' THEN 'SRB'  -- Native token: Not found in EVM-based chains
        WHEN ft.blockchain = 'BSC' THEN 'BNB'
        WHEN ft.blockchain = 'CEL' THEN 'CEL'  -- Native token: Not found in EVM-based chains
        WHEN ft.blockchain = 'TRX' THEN 'TRX'  -- Not included in ChainList as it's not EVM-based
        ELSE NULL
    END AS from_native_token,
    r.from_gas AS from_gas_amount,
    
    -- to_gas fee token
    CASE 
        WHEN tt.blockchain = 'ETH' THEN 'ETH'
        WHEN tt.blockchain = 'POL' THEN 'MATIC'
        WHEN tt.blockchain = 'ARB' THEN 'ETH'
        WHEN tt.blockchain = 'AVA' THEN 'AVAX'
        WHEN tt.blockchain = 'OPT' THEN 'ETH'
        WHEN tt.blockchain = 'BAS' THEN 'ETH'
        WHEN tt.blockchain = 'SOL' THEN 'SOL'  -- Not included in ChainList as it's not EVM-based
        WHEN tt.blockchain = 'SRB' THEN 'SRB'  -- Native token: Not found in EVM-based chains
        WHEN tt.blockchain = 'BSC' THEN 'BNB'
        WHEN tt.blockchain = 'CEL' THEN 'CEL'  -- Native token: Not found in EVM-based chains
        WHEN tt.blockchain = 'TRX' THEN 'TRX'  -- Not included in ChainList as it's not EVM-based
        ELSE NULL
    END AS to_native_token,
    r.to_gas AS to_gas_amount,

    -- relay fee
    r.relayer_fee_in_native,
    r.relayer_fee_in_tokens
    
FROM raw r
LEFT JOIN `mainnet-bigq.raw.source_all_bridge_explorer_tokens` AS ft
ON LOWER(r.from_token_address) = LOWER(ft.token_address) AND r.from_chain_symbol = ft.blockchain

LEFT JOIN `mainnet-bigq.raw.source_all_bridge_explorer_tokens` AS tt
ON LOWER(r.to_token_address) = LOWER(tt.token_address) AND r.to_chain_symbol = tt.blockchain
)

-- add metadata related to tokens and chains


-- final
-- keep only col names:
SELECT 
  s.transfer_id,
  s.date,  
  s.from_address,
  s.to_address,
  -- from
  s.from_chain_id,
  s.from_chain_name,
  s.from_token_symbol,
  s.from_amount,
  
  -- to
  to_chain_id,
  s.to_chain_name,
  s.to_token_symbol,
  s.to_amount,
  
  -- fees
  s.from_native_token,
  s.from_gas_amount,
  s.to_native_token,
  s.to_gas_amount,
  s.from_native_token AS relayer_fee_native_symbol,
  s.relayer_fee_in_native,
  COALESCE(s.from_token_symbol, s.to_token_symbol) AS relayer_fee_token_symbol,
  s.relayer_fee_in_tokens

FROM semi s
