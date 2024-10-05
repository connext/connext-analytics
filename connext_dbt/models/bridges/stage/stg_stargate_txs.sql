-- - from_hash: "0x438583770d4312e04e705143f87b71e815e1bf2809a71c0f25ddf6786f2c0050"
-- - from_date: "2024-09-29 22:48:20.000 UTC"
-- - from_user: "0x471801ab22320f594318ceca39d0946fa6dc4ef9"
-- - from_chain_name: "bnb"
-- - to_chain_id: "184"
-- - to_chain_name: "base"
-- - from_token_symbol: "USDC"
-- - from_amount: "32909232000000000000"
-- - to_amount: "32859868000000000000"

WITH raw AS (
    SELECT
    from_hash,
    from_date,
    from_user,
    from_chain_name,
    to_chain_id,
    to_chain_name,
    from_token_symbol,
    from_amount,
    to_amount
    FROM {{ source('dune', 'stargate_v2_txs_v0') }}
),
evm_chains_token_metadata AS (
    SELECT DISTINCT
      blockchain, symbol, contract_address, decimals
    FROM (
        SELECT symbol, blockchain, contract_address, decimals, rank() over (partition by blockchain, symbol order by contract_address) as rnk
        FROM {{source('dune', 'evm_chains_token_metadata')}}
    )
    WHERE rnk = 1
  )

SELECT
    "stargate_v2" AS bridge,
    CONCAT(r.from_hash, ' _ ', RANK() OVER (PARTITION BY r.from_hash ORDER BY r.from_date)) AS id,

    -- from
    r.from_hash AS from_hash,
    CAST(r.from_date AS TIMESTAMP) AS from_date,
    CASE
        WHEN from_chain_name = 'mantle' THEN 5000
        WHEN from_chain_name = 'bnb' THEN 56
        WHEN from_chain_name = 'base' THEN 8453
        WHEN from_chain_name = 'linea' THEN 59144
        WHEN from_chain_name = 'scroll' THEN 534352
        WHEN from_chain_name = 'polygon' THEN 137
        WHEN from_chain_name = 'arbitrum' THEN 42161
        WHEN from_chain_name = 'ethereum' THEN 1
        WHEN from_chain_name = 'optimism' THEN 10
        WHEN from_chain_name = 'avalanche' THEN 43114
        ELSE NULL
    END AS from_chain_id,
    r.from_chain_name AS from_chain_name,
    CAST(NULL AS STRING) AS from_token_address,
    r.from_user AS from_address,
    r.from_token_symbol AS from_token_symbol,
    CAST(r.from_amount AS FLOAT64) / POWER(10, tm_from.decimals) AS from_amount,
    
    -- to
    CAST(NULL AS STRING) AS to_hash,
    CAST(NULL AS TIMESTAMP) AS to_date,
    CASE
        WHEN to_chain_name = 'mantle' THEN 5000
        WHEN to_chain_name = 'bnb' THEN 56
        WHEN to_chain_name = 'base' THEN 8453
        WHEN to_chain_name = 'linea' THEN 59144
        WHEN to_chain_name = 'scroll' THEN 534352
        WHEN to_chain_name = 'polygon' THEN 137
        WHEN to_chain_name = 'arbitrum' THEN 42161
        WHEN to_chain_name = 'ethereum' THEN 1
        WHEN to_chain_name = 'optimism' THEN 10
        WHEN to_chain_name = 'avalanche' THEN 43114
        ELSE NULL
    END AS to_chain_id,
    r.to_chain_name AS to_chain_name,
    CAST(NULL AS STRING) AS to_address,
    CAST(NULL AS STRING) AS to_token_address,
    CAST(r.from_token_symbol AS STRING) AS to_token_symbol,
    CAST(r.to_amount AS FLOAT64) / POWER(10, tm_from.decimals) AS to_amount,

    -- gas and relay
    CAST(NULL AS STRING) AS gas_token_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(NULL AS STRING) AS relayer_fee_token_symbol,
    CAST(NULL AS FLOAT64) AS relay_fee_amount,
    CAST(NULL AS FLOAT64) AS relay_fee_usd
FROM raw r

-- from token metadata
LEFT JOIN evm_chains_token_metadata AS tm_from
ON r.from_chain_name = tm_from.blockchain AND r.from_token_symbol = tm_from.symbol
