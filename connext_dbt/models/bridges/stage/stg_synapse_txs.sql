WITH evm_chains_token_metadata AS (
    SELECT DISTINCT
        symbol, chain_id, CAST(decimals AS INT64) AS decimals, contract_address
    FROM (
        SELECT
            c.chain_id,
            symbol,
            decimals,
            contract_address,
            ROW_NUMBER() over (partition by blockchain, symbol order by contract_address) as rnk
        FROM {{source('dune', 'evm_chains_token_metadata')}} tm
        INNER JOIN {{ref('chains')}} c
        ON tm.blockchain = c.name
    )
    WHERE rnk = 1
  )

, safe_raw AS (
    SELECT * EXCEPT (_dlt_load_id, _dlt_id)
    FROM {{ source('raw', 'source_synapseprotocol_explorer_transactions') }} AS tx    
)

,raw AS (
    SELECT DISTINCT
        CAST(tx.kappa AS STRING) AS id,
        ROW_NUMBER() OVER (PARTITION BY tx.kappa ORDER BY tx.from_time DESC) AS ranking_kappa,
        -- from
        tx.from_hash,
        TIMESTAMP_SECONDS(CAST(tx.from_time AS INT64)) AS from_timestamp,
        tx.from_chain_id,
        from_chain.name AS from_chain_name,
        tx.from_address,
        tx.from_token_address,
        tx.from_token_symbol,
        CAST(tx.from_value AS FLOAT64) AS from_value,
        CAST(tx.from_formatted_value AS FLOAT64) AS from_amount,

        -- to
        tx.to_hash,
        TIMESTAMP_SECONDS(CAST(tx.to_time AS INT64)) AS to_timestamp,
        tx.to_chain_id,
        to_chain.name AS to_chain_name,
        tx.to_address,
        tx.to_token_address,
        tx.to_token_symbol,
        CAST(tx.to_value AS FLOAT64) AS to_value,
        CAST(tx.to_formatted_value AS FLOAT64) AS to_amount,

        -- fees
        from_chain.fee_token_symbol AS fee_token_symbol,
        CAST(NULL AS FLOAT64) AS gas_amount,
        CAST(NULL AS FLOAT64) AS gas_amount_usd,
        tx.from_token_symbol AS relayer_fee_symbol,
        CAST(NULL AS FLOAT64) AS relay_amount
FROM safe_raw tx
LEFT JOIN {{ref('chains')}} AS from_chain
ON tx.from_chain_id = from_chain.chain_id
LEFT JOIN {{ref('chains')}} AS to_chain
ON tx.to_chain_id = to_chain.chain_id
WHERE CAST(from_formatted_value  AS FLOAT64) != 0 OR CAST(to_formatted_value AS FLOAT64) != 0
)

, semi_raw AS (
SELECT 
    r.id,
    r.from_timestamp,
    r.from_hash,
    r.from_chain_id,
    r.from_chain_name,
    r.from_address,
    r.from_token_address,
    COALESCE(tm_from.symbol, r.from_token_symbol) AS from_token_symbol,
    r.from_amount AS from_amount,
    r.from_value / POWER(10, tm_from.decimals) AS cal_from_amount,
    
    -- to
    r.to_timestamp,
    r.to_hash,
    r.to_chain_id,
    r.to_chain_name,
    r.to_address,
    r.to_token_address,

    COALESCE(tm_to.symbol, r.to_token_symbol) AS to_token_symbol,
    r.to_amount AS to_amount,
    r.to_value / POWER(10, tm_to.decimals) AS cal_to_amount,
    COALESCE(tm_to.decimals, 18) AS to_token_decimals,

    -- fees
    r.fee_token_symbol,
    r.gas_amount,
    r.gas_amount_usd,
    COALESCE(tm_from.symbol, r.from_token_symbol) AS relayer_fee_symbol,
    r.relay_amount

-- filter put filler tokens
FROM raw r
-- from token metadata
INNER JOIN evm_chains_token_metadata AS tm_from
-- using to chain name
ON LOWER(r.from_token_address) = LOWER(tm_from.contract_address) AND r.from_chain_id = tm_from.chain_id
-- to token metadata
INNER JOIN evm_chains_token_metadata AS tm_to
ON LOWER(r.to_token_address) = LOWER(tm_to.contract_address) AND r.to_chain_id = tm_to.chain_id
WHERE r.ranking_kappa = 1
)

SELECT * FROM semi_raw
WHERE from_amount > 0 AND to_amount > 0