-- notes:
-- give there is no calculatipn iwth amounts , the amounts cant be null, there are few irregularity that need to be removed
-- id = '0x9226853c1fef8611e729d8beb8aa000d2b3503c4b075cba61061a7bdb30e31b8

WITH
  across_v3_txs AS (
    SELECT
      *
    FROM
      {{source('dune', 'across_v3_txs_v0')}}
  ),
  across_v2_txs AS (
    SELECT
      *
    FROM
      {{source('dune', 'across_v2_txs_v0')}}
  ),
  evm_chains_token_metadata AS (
    SELECT DISTINCT
      symbol, contract_address, decimals
    FROM (
        SELECT 
          symbol,
          blockchain,
          contract_address,
          decimals,
          rank() over (partition by blockchain, symbol order by contract_address) as rnk
        FROM {{source('dune', 'evm_chains_token_metadata')}}
    )
    WHERE rnk = 1
  ),

raw_v3 AS (
  SELECT
    'across_v3' AS bridge,
    a3.id,
    CAST(a3.date AS timestamp) AS date,
    -- from
    CAST(NULL AS STRING) AS from_hash,
    a3.from_user AS from_user,
    a3.from_chain_id AS from_chain_id,
    a3.from_chain_name AS from_chain_name,
    a3.from_token_address AS from_token_address,
    -- even the from token address are based on the destination chain
    tm_from.symbol AS from_token_symbol,
    SAFE_CAST(a3.from_amount AS FLOAT64) / pow(10, IFNULL(CAST(tm_from.decimals AS INT64), 18)) AS from_amount,
    -- to
    a3.id AS to_hash,
    a3.to_user AS to_user,
    a3.to_chain_id AS to_chain_id,
    a3.to_chain_name AS to_chain_name,
    a3.to_token_address AS to_token_address,
    tm_to.symbol AS to_token_symbol,
    (SAFE_CAST(a3.to_amount AS FLOAT64) / pow(10, IFNULL(CAST(tm_to.decimals AS INT64), 18))) AS to_amount,
    -- fees + protocol
    a3.gas_token_symbol AS gas_token_symbol,
    --TODO: fix the token for native tokens if there is a bug down the line
    CAST(NULL AS FLOAT64) AS gas_amount,
    CAST(tm_from.symbol AS STRING) AS relayer_fee_token_symbol,
    CAST(NULL AS FLOAT64) AS relay_fee_usd

  FROM across_v3_txs a3
  -- from token metadata
  LEFT JOIN evm_chains_token_metadata AS tm_from
  -- using to chain name
  ON a3.from_token_address = tm_from.contract_address
  -- to token metadata
  LEFT JOIN evm_chains_token_metadata AS tm_to
  ON a3.to_token_address = tm_to.contract_address
),

raw_v2 AS (
  SELECT
    'across_v2' AS bridge,
    a2.id,
    CAST(a2.date AS timestamp) AS date,

    -- from
    CAST(NULL AS STRING) AS from_hash,
    a2.from_user,
    a2.from_chain_id,
    a2.from_chain_name,
    a2.from_token_address,
    tm_from.symbol AS from_token_symbol,
    (SAFE_CAST(a2.from_amount AS FLOAT64) / pow(10, IFNULL(CAST(tm_from.decimals AS INT64), 18))) AS from_amount,
    -- to

    a2.id AS to_hash,
    a2.to_user,
    a2.to_chain_id,
    a2.to_chain_name,
    a2.to_token_address,
    tm_to.symbol AS to_token_symbol,
    (
      CAST(a2.from_amount AS FLOAT64) / pow(10, IFNULL(CAST(tm_to.decimals AS INT64), 18)) * 
      ( 1 - CAST(a2.applied_relayer_fee_pct AS FLOAT64) / pow(10,18) - CAST(a2.realized_lp_fee_pct AS FLOAT64) / pow(10,18))
    )  AS to_amount,
    
    -- fees + protocol
    CAST(NULL AS STRING) AS gas_token_symbol,
    CAST(NULL AS FLOAT64) AS gas_amount,
    tm_from.symbol AS relayer_fee_token_symbol,
    CAST(NULL AS FLOAT64)  AS relay_fee_usd
  FROM across_v2_txs a2
  -- from token metadata
  LEFT JOIN evm_chains_token_metadata AS tm_from
  ON a2.from_token_address = tm_from.contract_address
  -- to token metadata
  LEFT JOIN evm_chains_token_metadata AS tm_to
  ON a2.to_token_address = tm_to.contract_address
),

final AS (
  SELECT *
  FROM raw_v3
  UNION ALL
  SELECT *
  FROM raw_v2
)

SELECT
    f.bridge,
    CAST(ROW_NUMBER() OVER (ORDER BY f.date) AS STRING) AS id,
    f.date,
    -- from
    f.from_hash,
    f.from_user,
    CAST(f.from_chain_id AS INTEGER) AS from_chain_id,
    f.from_chain_name,
    f.from_token_address,
    f.from_token_symbol,
    CAST(f.from_amount AS FLOAT64) AS from_amount,
    -- to

    f.to_hash,
    f.to_user,
    CAST(f.to_chain_id AS INTEGER) AS to_chain_id,
    f.to_chain_name,
    f.to_token_address,
    f.to_token_symbol,
    CAST(f.to_amount AS FLOAT64) AS to_amount,

    -- fees + protocol
    
    f.gas_token_symbol,
    CAST(f.gas_amount AS FLOAT64) AS gas_amount,
    f.relayer_fee_token_symbol,
    f.from_amount - f.to_amount AS relay_fee_amount,
    CAST(f.relay_fee_usd AS FLOAT64) AS relay_fee_usd
FROM final f
-- remove irregularities
WHERE (f.from_amount IS NOT NULL) AND (f.to_amount IS NOT NULL)